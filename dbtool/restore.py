import gzip
import json
import time
from io import BytesIO
from pathlib import Path

import humanize
import psycopg2

from .config import DBConfig, DumpSettings
from .db import connect
from .ui import console, interrupted, reset_interrupt, ChunkProgress


# ── connection helpers ───────────────────────────────────────────────────────

def _is_connection_error(e: Exception) -> bool:
    """connection drop vs actual data error."""
    if isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError, OSError, ConnectionError)):
        return True
    msg = str(e).lower()
    return any(kw in msg for kw in [
        "connection", "timeout", "refused", "reset", "broken pipe",
        "server closed", "ssl", "eof", "terminated", "could not connect",
    ])


def _safe_close(conn):
    """close a connection that might already be dead."""
    if conn:
        try:
            conn.close()
        except Exception:
            pass


# ── main restore ─────────────────────────────────────────────────────────────

def restore_table(
    db_cfg: DBConfig,
    dbname: str,
    table_dir: Path,
    settings: DumpSettings,
) -> bool:
    """restore a table from dump chunks. returns True if completed, False if stopped."""
    manifest_path = table_dir / "manifest.json"
    if not manifest_path.exists():
        console.print(f"  [error]no manifest.json in {table_dir}[/]")
        return False

    manifest = json.loads(manifest_path.read_text())
    table_name = manifest["table"]
    columns = manifest["columns"]
    compressed = manifest.get("compressed", True)
    chunks_total = manifest["chunks_total"]
    total_rows = manifest["total_rows"]
    chunk_rows = manifest["chunk_rows"]
    dump_mode = manifest.get("dump_mode", "copy")
    has_schema = manifest.get("has_schema", False)
    schema = manifest.get("schema", "public")
    single_file = manifest.get("single_file", False)

    if total_rows == 0:
        console.print(f"  [dim]skipping {table_name} — empty dump[/]")
        return True

    # ── resume state ─────────────────────────────────────────────────────
    state_file = table_dir / "restore_state.json"
    start_chunk = 0
    if state_file.exists():
        state = json.loads(state_file.read_text())
        start_chunk = state.get("chunks_restored", 0)
        if start_chunk >= chunks_total:
            console.print(f"  [dim]skipping {table_name} — already restored[/]")
            return True
        if start_chunk > 0:
            console.print(f"  [info]resuming {table_name} from chunk {start_chunk}/{chunks_total}[/]")

    # ── pre-restore (only on fresh start, not resume) ────────────────────
    if start_chunk == 0:
        try:
            _pre_restore(db_cfg, dbname, table_dir, table_name, manifest, settings, has_schema)
        except Exception as e:
            console.print(f"  [error]pre-restore failed: {e}[/]")
            console.print(f"  [dim]fix connection and re-run dbtool restore[/]")
            return False

        if settings.disable_indexes_on_restore:
            try:
                _drop_and_save_indexes(db_cfg, dbname, table_dir, table_name, schema)
            except Exception as e:
                console.print(f"  [warning]failed to drop indexes: {e} — continuing anyway[/]")

    # ── chunk loop ───────────────────────────────────────────────────────
    col_list = ", ".join(f'"{c}"' for c in columns)
    rows_restored = start_chunk * chunk_rows
    t_start = time.monotonic()
    max_wait = 60

    with ChunkProgress(table_name, "bold green", total_rows, start_chunk, chunks_total, chunk_rows) as prog:
        for chunk_idx in range(start_chunk, chunks_total):
            if interrupted:
                _save_state(state_file, chunk_idx)
                console.print(f"  [warning]⏸ {table_name} paused at chunk {chunk_idx}/{chunks_total} — resumable[/]")
                return False

            # read chunk from disk
            chunk_file = _chunk_path(table_dir, table_name, dump_mode, compressed, chunk_idx, single_file)
            if not chunk_file.exists():
                console.print(f"  [error]missing chunk file: {chunk_file.name}[/]")
                return False

            raw_data = _read_chunk(chunk_file, compressed)
            chunk_row_count = _count_rows(raw_data, dump_mode)

            # try to load this chunk with retries
            success = False
            for attempt in range(1, settings.max_retries + 1):
                conn = None
                try:
                    conn = connect(db_cfg, dbname)
                    with conn.cursor() as cur:
                        if dump_mode == "insert":
                            cur.execute(raw_data.decode("utf-8"))
                        else:
                            cur.copy_expert(
                                f'COPY "{table_name}" ({col_list}) FROM STDIN',
                                BytesIO(raw_data),
                            )
                    conn.commit()
                    success = True
                    break

                except Exception as e:
                    # rollback if possible (connection might be dead)
                    if conn:
                        try:
                            conn.rollback()
                        except Exception:
                            pass

                    if not _is_connection_error(e):
                        # data error — no point retrying
                        console.print(f"  [error]chunk {chunk_idx} data error: {e}[/]")
                        _save_state(state_file, chunk_idx)
                        return False

                    wait = min(settings.retry_backoff ** attempt, max_wait)
                    console.print(
                        f"  [warning]chunk {chunk_idx} attempt {attempt}/{settings.max_retries} "
                        f"failed: {e}[/]"
                    )
                    if attempt < settings.max_retries:
                        console.print(f"  [dim]retrying in {wait}s…[/]")
                        time.sleep(wait)

                finally:
                    _safe_close(conn)

            if not success:
                _save_state(state_file, chunk_idx)
                console.print(
                    f"\n  [error]✗ {table_name} — connection lost at chunk {chunk_idx}/{chunks_total}[/]"
                )
                console.print(f"  [info]progress saved. re-run dbtool restore to resume.[/]")
                return False

            # chunk committed — save progress
            rows_restored += chunk_row_count
            _save_state(state_file, chunk_idx + 1)

            elapsed = time.monotonic() - t_start
            speed = f"{humanize.intcomma(int(rows_restored / elapsed))} rows/s" if elapsed > 0 else ""
            prog.update(rows_restored, chunk_idx + 1, speed)

    # ── rebuild indexes ──────────────────────────────────────────────────
    if not interrupted:
        _rebuild_saved_indexes(db_cfg, dbname, table_dir)
        console.print(
            f"  [success]✓ {table_name}[/] — "
            f"{humanize.intcomma(rows_restored)} rows restored across {chunks_total} chunks"
        )
        return True

    return False


# ── helpers ──────────────────────────────────────────────────────────────────

def _save_state(state_file: Path, chunks_restored: int):
    state_file.write_text(json.dumps({"chunks_restored": chunks_restored}))


def _chunk_path(table_dir, table_name, dump_mode, compressed, chunk_idx, single_file=False) -> Path:
    if dump_mode == "insert":
        ext = ".sql.gz" if compressed else ".sql"
    else:
        ext = ".csv.gz" if compressed else ".csv"
    if single_file:
        return table_dir / f"{table_name}{ext}"
    return table_dir / f"{table_name}_chunk_{chunk_idx:06d}{ext}"


def _read_chunk(path: Path, compressed: bool) -> bytes:
    if compressed:
        with gzip.open(path, "rb") as f:
            return f.read()
    with open(path, "rb") as f:
        return f.read()


def _count_rows(raw_data: bytes, dump_mode: str) -> int:
    if dump_mode == "insert":
        return raw_data.count(b"(") - raw_data.count(b"INSERT")
    count = raw_data.count(b"\n")
    if raw_data and not raw_data.endswith(b"\n"):
        count += 1
    return count


# ── pre-restore ops ──────────────────────────────────────────────────────────

def _pre_restore(db_cfg, dbname, table_dir, table_name, manifest, settings, has_schema):
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
                    cur.execute(schema_file.read_text())
                    conn.commit()
                    console.print(f"  [dim]recreated schema for {table_name}[/]")

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


def _drop_and_save_indexes(db_cfg, dbname, table_dir, table_name, schema):
    """drop non-PK indexes and save CREATE statements to disk."""
    index_file = table_dir / "dropped_indexes.json"
    conn = connect(db_cfg, dbname)
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

            definitions = []
            for idx_name, idx_def in cur.fetchall():
                definitions.append(idx_def)
                cur.execute(f'DROP INDEX IF EXISTS "{schema}"."{idx_name}"')
                console.print(f"  [dim]dropped index {idx_name}[/]")
            conn.commit()

        if definitions:
            index_file.write_text(json.dumps(definitions))
    finally:
        conn.close()


def _rebuild_saved_indexes(db_cfg, dbname, table_dir):
    """rebuild indexes from dropped_indexes.json."""
    index_file = table_dir / "dropped_indexes.json"
    if not index_file.exists():
        return

    definitions = json.loads(index_file.read_text())
    if not definitions:
        return

    console.print(f"  [dim]rebuilding {len(definitions)} index(es)…[/]")
    conn = connect(db_cfg, dbname)
    try:
        with conn.cursor() as cur:
            for defn in definitions:
                cur.execute(defn)
        conn.commit()
        console.print(f"  [success]rebuilt {len(definitions)} index(es)[/]")
        index_file.unlink(missing_ok=True)
    except Exception as e:
        console.print(f"  [error]index rebuild failed: {e}[/]")
        console.print(f"  [dim]indexes saved in dropped_indexes.json — will retry on next restore[/]")
        conn.rollback()
    finally:
        conn.close()