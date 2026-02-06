import gzip
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional

import humanize

from .config import DBConfig, DumpSettings
from .db import TableInfo, connect, get_table_ddl
from .ui import console, interrupted, reset_interrupt, ChunkProgress


@dataclass
class DumpManifest:
    connection_name: str
    database: str
    table: str
    schema: str
    columns: list[str]
    pk_columns: list[str]
    chunk_rows: int
    total_rows: int
    chunks_completed: int
    chunks_total: int
    started_at: str
    finished_at: Optional[str] = None
    compressed: bool = True
    dump_mode: str = "copy"
    has_schema: bool = False

    def chunk_filename(self, idx: int) -> str:
        if self.dump_mode == "insert":
            ext = ".sql.gz" if self.compressed else ".sql"
        else:
            ext = ".csv.gz" if self.compressed else ".csv"
        return f"{self.table}_chunk_{idx:06d}{ext}"


def dump_table(
    db_cfg: DBConfig,
    dbname: str,
    table: TableInfo,
    output_dir: Path,
    settings: DumpSettings,
    resume: bool = True,
):
    table_dir = output_dir / dbname / table.name
    table_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = table_dir / "manifest.json"

    start_chunk = 0
    if resume and manifest_path.exists():
        existing = json.loads(manifest_path.read_text())
        if existing.get("finished_at"):
            console.print(f"  [dim]skipping {table.name} — already completed[/]")
            return
        start_chunk = existing.get("chunks_completed", 0)
        if start_chunk > 0:
            console.print(f"  [info]resuming {table.name} from chunk {start_chunk}[/]")

    # dump schema DDL if enabled
    schema_ddl = None
    has_schema = False
    if settings.dump_schema:
        try:
            schema_ddl = get_table_ddl(db_cfg, dbname, table)
            (table_dir / "schema.sql").write_text(schema_ddl)
            has_schema = True
            console.print(f"  [dim]saved schema.sql for {table.name}[/]")
        except Exception as e:
            console.print(f"  [warning]could not dump schema: {e}[/]")

    # for insert mode: fetch index definitions for baking into last chunk
    index_defs = []
    if settings.dump_mode == "insert" and settings.disable_indexes_on_restore:
        try:
            from .db import get_index_info
            raw_indexes = get_index_info(db_cfg, dbname, table)
            # filter out PK indexes
            conn_tmp = connect(db_cfg, dbname)
            try:
                with conn_tmp.cursor() as cur_tmp:
                    cur_tmp.execute("""
                        SELECT conname FROM pg_constraint
                        WHERE conrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass
                          AND contype = 'p'
                    """, (table.schema, table.name))
                    pk_names = {row[0] for row in cur_tmp.fetchall()}
            finally:
                conn_tmp.close()
            index_defs = [(name, defn) for name, defn, _ in raw_indexes if name not in pk_names]
        except Exception as e:
            console.print(f"  [warning]could not fetch indexes for baking: {e}[/]")

    conn = connect(db_cfg, dbname)
    try:
        with conn.cursor() as cur:
            console.print(f"  [dim]counting rows in {table.name}…[/]", end="")
            cur.execute(f'SELECT count(*) FROM "{table.name}"')
            total_rows = cur.fetchone()[0]
            console.print(f" [info]{humanize.intcomma(total_rows)}[/]")

            if total_rows == 0:
                console.print(f"  [dim]empty table, skipping[/]")
                _write_manifest(manifest_path, _make_manifest(
                    db_cfg, dbname, table, settings, 0, 0, 0,
                    has_schema=has_schema, finished=True,
                ))
                return

            chunk_rows = settings.chunk_rows
            chunks_total = (total_rows + chunk_rows - 1) // chunk_rows
            manifest = _make_manifest(
                db_cfg, dbname, table, settings, total_rows,
                start_chunk, chunks_total, has_schema=has_schema,
            )

            order_clause = ", ".join(f'"{c}"' for c in table.pk_columns) if table.pk_columns else "ctid"
            col_list = ", ".join(f'"{c}"' for c in table.columns)

            with ChunkProgress(table.name, "bold blue", total_rows, start_chunk, chunks_total, chunk_rows) as prog:
                chunk_idx = start_chunk
                rows_dumped = start_chunk * chunk_rows
                t_start = time.monotonic()

                while chunk_idx < chunks_total and not interrupted:
                    offset = chunk_idx * chunk_rows
                    chunk_file = table_dir / manifest.chunk_filename(chunk_idx)

                    # always use COPY for extraction — it's 100x faster
                    copy_sql = f"""COPY (
                        SELECT {col_list} FROM "{table.name}"
                        ORDER BY {order_clause}
                        LIMIT {chunk_rows} OFFSET {offset}
                    ) TO STDOUT"""

                    buf = BytesIO()
                    try:
                        cur.copy_expert(copy_sql, buf)
                    except KeyboardInterrupt:
                        conn.cancel()
                        raise
                    raw = buf.getvalue()

                    chunk_row_count = raw.count(b"\n")
                    if raw and not raw.endswith(b"\n"):
                        chunk_row_count += 1

                    # convert COPY output to full SQL file if insert mode
                    if settings.dump_mode == "insert":
                        is_first = (chunk_idx == 0 and start_chunk == 0)
                        is_last = (chunk_idx == chunks_total - 1)
                        raw = _build_sql_chunk(
                            raw, table, settings, schema_ddl, index_defs,
                            is_first=is_first, is_last=is_last,
                        )

                    if settings.compress:
                        with gzip.open(chunk_file, "wb", compresslevel=settings.compress_level) as f:
                            f.write(raw)
                    else:
                        with open(chunk_file, "wb") as f:
                            f.write(raw)

                    rows_dumped += chunk_row_count
                    chunk_idx += 1
                    manifest.chunks_completed = chunk_idx
                    _write_manifest(manifest_path, manifest)

                    elapsed = time.monotonic() - t_start
                    rps = int(rows_dumped / elapsed) if elapsed > 0 else 0
                    prog.update(rows_dumped, chunk_idx, f"{humanize.intcomma(rps)} rows/s")

            if not interrupted:
                manifest.finished_at = datetime.now().isoformat()
                _write_manifest(manifest_path, manifest)
                total_file_size = sum(
                    f.stat().st_size for f in table_dir.iterdir()
                    if f.suffix in (".gz", ".csv", ".sql")
                )
                console.print(
                    f"  [success]✓ {table.name}[/] — "
                    f"{humanize.intcomma(rows_dumped)} rows, "
                    f"{chunk_idx} chunks, "
                    f"{humanize.naturalsize(total_file_size, binary=True)} on disk"
                )
            else:
                console.print(f"  [warning]⏸ {table.name} paused at chunk {chunk_idx}/{chunks_total} — resumable[/]")

    except KeyboardInterrupt:
        console.print(f"\n  [warning]⏸ {table.name} — force stopped[/]")
        raise
    except Exception as e:
        console.print(f"  [error]✗ {table.name} failed: {e}[/]")
        raise
    finally:
        conn.close()


# ── SQL generation ───────────────────────────────────────────────────────────

def _build_sql_chunk(
    copy_data: bytes,
    table: TableInfo,
    settings: DumpSettings,
    schema_ddl: str | None,
    index_defs: list[tuple[str, str]],
    is_first: bool,
    is_last: bool,
) -> bytes:
    """build a complete, self-contained .sql chunk file.

    chunk 0 gets preamble (DROP, CREATE, TRUNCATE, drop indexes).
    every chunk gets BEGIN/COMMIT if use_transactions is on.
    last chunk gets index rebuilds.
    """
    parts: list[str] = []

    # ── header comment ───────────────────────────────────────────────────
    parts.append(f"-- dbtool dump: {table.name}")
    parts.append(f"-- generated: {datetime.now().isoformat()}")
    if is_first:
        parts.append("-- chunk: 0 (first — includes preamble)")
    if is_last:
        parts.append("-- chunk: last (includes epilogue)")
    parts.append("")

    # ── preamble (first chunk only) ──────────────────────────────────────
    if is_first:
        if settings.drop_on_restore:
            parts.append(f'DROP TABLE IF EXISTS "{table.name}" CASCADE;')
            parts.append("")

        if settings.recreate_schema and schema_ddl:
            parts.append("-- schema")
            parts.append(schema_ddl)
            parts.append("")
        elif settings.drop_on_restore and schema_ddl:
            # if we dropped, we must recreate
            parts.append("-- schema (required after DROP)")
            parts.append(schema_ddl)
            parts.append("")

        if settings.truncate_before_restore and not settings.drop_on_restore:
            parts.append(f'TRUNCATE TABLE "{table.name}" CASCADE;')
            parts.append("")

        if settings.disable_indexes_on_restore and index_defs:
            parts.append("-- drop indexes for faster bulk load")
            for idx_name, _ in index_defs:
                parts.append(f'DROP INDEX IF EXISTS "{idx_name}";')
            parts.append("")

    # ── transaction open ─────────────────────────────────────────────────
    if settings.use_transactions:
        parts.append("BEGIN;")
        parts.append("")

    # ── data ─────────────────────────────────────────────────────────────
    inserts = _copy_to_inserts(copy_data, table.name, table.columns, settings.insert_batch_size)
    parts.append(inserts)

    # ── transaction close ────────────────────────────────────────────────
    if settings.use_transactions:
        parts.append("COMMIT;")
        parts.append("")

    # ── epilogue (last chunk only) ───────────────────────────────────────
    if is_last and settings.disable_indexes_on_restore and index_defs:
        parts.append("-- rebuild indexes")
        for _, idx_defn in index_defs:
            parts.append(f"{idx_defn};")
        parts.append("")

    return "\n".join(parts).encode("utf-8")


def _copy_to_inserts(raw: bytes, table_name: str, columns: list[str], batch_size: int) -> str:
    """convert COPY tab-delimited output to batched INSERT statements."""
    if not raw:
        return ""

    cols_quoted = ", ".join(f'"{c}"' for c in columns)
    header = f'INSERT INTO "{table_name}" ({cols_quoted}) VALUES'

    lines = raw.split(b"\n")
    if lines and lines[-1] == b"":
        lines = lines[:-1]

    output_parts = []
    batch = []

    for line in lines:
        fields = line.split(b"\t")
        values = []
        for f in fields:
            if f == b"\\N":
                values.append("NULL")
            else:
                s = f.replace(b"\\\\", b"\x00") \
                     .replace(b"\\n", b"\n") \
                     .replace(b"\\r", b"\r") \
                     .replace(b"\\t", b"\t") \
                     .replace(b"\x00", b"\\")
                text = s.decode("utf-8", errors="replace")
                text = text.replace("'", "''")
                values.append(f"'{text}'")

        batch.append("  (" + ", ".join(values) + ")")

        if len(batch) >= batch_size:
            output_parts.append(header + "\n" + ",\n".join(batch) + ";")
            batch = []

    if batch:
        output_parts.append(header + "\n" + ",\n".join(batch) + ";")

    return "\n\n".join(output_parts)


# ── manifest helpers ─────────────────────────────────────────────────────────

def _make_manifest(
    db_cfg, dbname, table, settings: DumpSettings, total_rows,
    chunks_completed, chunks_total, has_schema=False, finished=False,
) -> DumpManifest:
    now = datetime.now().isoformat()
    return DumpManifest(
        connection_name=db_cfg.name, database=dbname,
        table=table.name, schema=table.schema,
        columns=table.columns, pk_columns=table.pk_columns,
        chunk_rows=settings.chunk_rows, total_rows=total_rows,
        chunks_completed=chunks_completed, chunks_total=chunks_total,
        started_at=now,
        finished_at=now if finished else None,
        compressed=settings.compress,
        dump_mode=settings.dump_mode,
        has_schema=has_schema,
    )


def _write_manifest(path: Path, manifest: DumpManifest):
    path.write_text(json.dumps(asdict(manifest), indent=2))