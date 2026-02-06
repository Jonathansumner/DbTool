import gzip
import json
import time
from io import BytesIO
from pathlib import Path

import humanize

from .config import DBConfig, DumpSettings
from .db import connect
from .ui import console, interrupted, reset_interrupt, ChunkProgress


def restore_table(
    db_cfg: DBConfig,
    dbname: str,
    table_dir: Path,
    settings: DumpSettings,
):
    manifest_path = table_dir / "manifest.json"
    if not manifest_path.exists():
        console.print(f"  [error]no manifest.json in {table_dir}[/]")
        return

    manifest = json.loads(manifest_path.read_text())
    table_name = manifest["table"]
    columns = manifest["columns"]
    compressed = manifest.get("compressed", True)
    chunks_total = manifest["chunks_total"]
    total_rows = manifest["total_rows"]
    chunk_rows = manifest["chunk_rows"]
    dump_mode = manifest.get("dump_mode", "copy")
    has_schema = manifest.get("has_schema", False)

    if total_rows == 0:
        console.print(f"  [dim]skipping {table_name} — empty dump[/]")
        return

    state_file = table_dir / "restore_state.json"
    start_chunk = 0
    if state_file.exists():
        state = json.loads(state_file.read_text())
        start_chunk = state.get("chunks_restored", 0)
        if start_chunk >= chunks_total:
            console.print(f"  [dim]skipping {table_name} — already restored[/]")
            return
        if start_chunk > 0:
            console.print(f"  [info]resuming {table_name} restore from chunk {start_chunk}[/]")

    # pre-restore schema operations
    if start_chunk == 0:
        _pre_restore(db_cfg, dbname, table_dir, table_name, manifest, settings, has_schema)

    # disable indexes if requested
    dropped_indexes = []
    if settings.disable_indexes_on_restore and start_chunk == 0:
        dropped_indexes = _drop_non_pk_indexes(db_cfg, dbname, table_name, manifest.get("schema", "public"))

    col_list = ", ".join(f'"{c}"' for c in columns)
    rows_restored = start_chunk * chunk_rows
    t_start = time.monotonic()

    with ChunkProgress(table_name, "bold green", total_rows, start_chunk, chunks_total, chunk_rows) as prog:
        for chunk_idx in range(start_chunk, chunks_total):
            if interrupted:
                console.print(f"  [warning]⏸ {table_name} paused at chunk {chunk_idx} — resumable[/]")
                break

            # determine chunk file extension
            if dump_mode == "insert":
                ext = ".sql.gz" if compressed else ".sql"
            else:
                ext = ".csv.gz" if compressed else ".csv"
            chunk_file = table_dir / f"{table_name}_chunk_{chunk_idx:06d}{ext}"

            if not chunk_file.exists():
                console.print(f"  [error]missing chunk file: {chunk_file.name}[/]")
                return

            if compressed:
                with gzip.open(chunk_file, "rb") as f:
                    raw_data = f.read()
            else:
                with open(chunk_file, "rb") as f:
                    raw_data = f.read()

            chunk_row_count = _count_rows_for_mode(raw_data, dump_mode)

            for attempt in range(1, settings.max_retries + 1):
                try:
                    conn = connect(db_cfg, dbname)
                    try:
                        with conn.cursor() as cur:
                            if dump_mode == "insert":
                                cur.execute(raw_data.decode("utf-8"))
                            else:
                                copy_sql = f'COPY "{table_name}" ({col_list}) FROM STDIN'
                                cur.copy_expert(copy_sql, BytesIO(raw_data))

                        if settings.use_transactions:
                            conn.commit()
                        else:
                            conn.commit()  # always commit, but semantics differ
                        rows_restored += chunk_row_count
                        break
                    except Exception:
                        conn.rollback()
                        raise
                    finally:
                        conn.close()

                except Exception as e:
                    if attempt < settings.max_retries:
                        wait = settings.retry_backoff ** attempt
                        console.print(f"  [warning]chunk {chunk_idx} attempt {attempt} failed: {e} — retry in {wait}s…[/]")
                        time.sleep(wait)
                    else:
                        console.print(f"  [error]chunk {chunk_idx} failed after {settings.max_retries} attempts: {e}[/]")
                        state_file.write_text(json.dumps({"chunks_restored": chunk_idx}))
                        raise

            state_file.write_text(json.dumps({"chunks_restored": chunk_idx + 1}))

            elapsed = time.monotonic() - t_start
            speed_str = f"{humanize.intcomma(int(rows_restored / elapsed))} rows/s" if elapsed > 0 else ""
            prog.update(rows_restored, chunk_idx + 1, speed_str)

    # rebuild indexes if we dropped them
    if dropped_indexes and not interrupted:
        _rebuild_indexes(db_cfg, dbname, dropped_indexes)

    if not interrupted:
        console.print(
            f"  [success]✓ {table_name}[/] — "
            f"{humanize.intcomma(rows_restored)} rows restored across {chunks_total} chunks"
        )


def _pre_restore(db_cfg, dbname, table_dir, table_name, manifest, settings, has_schema):
    """handle DROP, CREATE, TRUNCATE before data restore."""
    conn = connect(db_cfg, dbname)
    try:
        with conn.cursor() as cur:
            if settings.drop_on_restore:
                cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                conn.commit()
                console.print(f"  [dim]dropped {table_name}[/]")

            if settings.recreate_schema and has_schema:
                schema_file = table_dir / "schema.sql"
                if schema_file.exists():
                    ddl = schema_file.read_text()
                    cur.execute(ddl)
                    conn.commit()
                    console.print(f"  [dim]recreated schema for {table_name}[/]")
                else:
                    console.print(f"  [warning]recreate_schema enabled but no schema.sql found[/]")

            if settings.truncate_before_restore and not settings.drop_on_restore:
                try:
                    cur.execute(f'TRUNCATE TABLE "{table_name}" CASCADE')
                    conn.commit()
                    console.print(f"  [dim]truncated {table_name}[/]")
                except Exception:
                    conn.rollback()
                    console.print(f"  [warning]could not truncate {table_name} — table may not exist[/]")
    finally:
        conn.close()


def _drop_non_pk_indexes(db_cfg, dbname, table_name, schema) -> list[str]:
    """drop non-PK indexes, return their CREATE INDEX statements for rebuild."""
    conn = connect(db_cfg, dbname)
    definitions = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s
                  AND indexname NOT IN (
                      SELECT conname FROM pg_constraint
                      WHERE conrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass
                        AND contype = 'p'
                  )
            """, (schema, table_name, schema, table_name))

            for idx_name, idx_def in cur.fetchall():
                definitions.append(idx_def)
                cur.execute(f'DROP INDEX IF EXISTS "{schema}"."{idx_name}"')
                console.print(f"  [dim]dropped index {idx_name}[/]")

            conn.commit()
    except Exception as e:
        console.print(f"  [warning]failed to drop indexes: {e}[/]")
        conn.rollback()
    finally:
        conn.close()
    return definitions


def _rebuild_indexes(db_cfg, dbname, definitions: list[str]):
    """rebuild previously dropped indexes."""
    conn = connect(db_cfg, dbname)
    try:
        with conn.cursor() as cur:
            for defn in definitions:
                console.print(f"  [dim]rebuilding index…[/]")
                cur.execute(defn)
            conn.commit()
            console.print(f"  [success]rebuilt {len(definitions)} index(es)[/]")
    except Exception as e:
        console.print(f"  [error]failed to rebuild indexes: {e}[/]")
        conn.rollback()
    finally:
        conn.close()


def _count_rows_for_mode(raw_data: bytes, dump_mode: str) -> int:
    if dump_mode == "insert":
        return raw_data.count(b"(") - raw_data.count(b"INSERT")
    count = raw_data.count(b"\n")
    if raw_data and not raw_data.endswith(b"\n"):
        count += 1
    return count