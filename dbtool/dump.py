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
    single_file: bool = False

    def chunk_filename(self, idx: int) -> str:
        if self.single_file:
            if self.dump_mode == "insert":
                return f"{self.table}{'.sql.gz' if self.compressed else '.sql'}"
            return f"{self.table}{'.csv.gz' if self.compressed else '.csv'}"
        if self.dump_mode == "insert":
            ext = ".sql.gz" if self.compressed else ".sql"
        else:
            ext = ".csv.gz" if self.compressed else ".csv"
        return f"{self.table}_chunk_{idx:06d}{ext}"


class _ChunkSplitter:
    """file-like object that receives streaming COPY output and splits into chunk files.

    psycopg2's copy_expert streams data into this via write() calls.
    we count newlines (rows) and flush to a new chunk file every chunk_rows rows.
    """

    def __init__(
        self,
        table_dir: Path,
        table: TableInfo,
        manifest: DumpManifest,
        settings: DumpSettings,
        schema_ddl: str | None,
        index_defs: list[tuple[str, str]],
        chunks_total: int,
        start_chunk: int,
        progress: ChunkProgress,
    ):
        self.table_dir = table_dir
        self.table = table
        self.manifest = manifest
        self.settings = settings
        self.schema_ddl = schema_ddl
        self.index_defs = index_defs
        self.chunks_total = chunks_total
        self.start_chunk = start_chunk
        self.progress = progress

        self.chunk_rows = settings.chunk_rows
        self.chunk_idx = 0           # current chunk being built
        self.row_count = 0           # rows in current chunk
        self.total_rows_seen = 0     # total rows streamed so far
        self.skip_rows = start_chunk * settings.chunk_rows  # rows to skip on resume
        self.buf = BytesIO()
        self.t_start = time.monotonic()

        # track whether we've seen any data at all
        self._leftover = b""

    def write(self, data: bytes) -> int:
        """called by copy_expert with chunks of COPY output."""
        # prepend any leftover from the last call (partial line)
        data = self._leftover + data
        self._leftover = b""

        # if data doesn't end with newline, the last piece is a partial row
        if data and not data.endswith(b"\n"):
            last_nl = data.rfind(b"\n")
            if last_nl == -1:
                # entire chunk is a partial row — buffer it
                self._leftover = data
                return len(data)
            self._leftover = data[last_nl + 1:]
            data = data[:last_nl + 1]

        # process complete rows
        pos = 0
        while pos < len(data):
            if interrupted:
                return len(data)

            # how many more rows until this chunk is full?
            rows_remaining = self.chunk_rows - self.row_count
            # find that many newlines
            end = pos
            found = 0
            while found < rows_remaining and end < len(data):
                nl = data.find(b"\n", end)
                if nl == -1:
                    break
                end = nl + 1
                found += 1

            if found == 0:
                # no complete rows left
                break

            # are we still in the skip zone (resume)?
            if self.total_rows_seen + found <= self.skip_rows:
                self.total_rows_seen += found
                self.row_count += found
                pos = end
                if self.row_count >= self.chunk_rows:
                    # skip this chunk entirely
                    self.chunk_idx += 1
                    self.row_count = 0
                    self.buf = BytesIO()
                continue

            # if we're partially in the skip zone, trim
            if self.total_rows_seen < self.skip_rows:
                skip_here = self.skip_rows - self.total_rows_seen
                # skip skip_here rows from start of this segment
                skip_end = pos
                for _ in range(skip_here):
                    nl = data.find(b"\n", skip_end)
                    skip_end = nl + 1
                self.total_rows_seen += skip_here
                self.row_count += skip_here
                found -= skip_here
                pos = skip_end

            # write rows to buffer
            segment = data[pos:end]
            self.buf.write(segment)
            self.row_count += found
            self.total_rows_seen += found
            pos = end

            # chunk full?
            if self.row_count >= self.chunk_rows:
                self._flush_chunk()

        return len(data) + len(self._leftover)

    def _flush_chunk(self):
        """write the buffered chunk to disk."""
        if self.chunk_idx < self.start_chunk:
            # skip — already on disk from previous run
            self.chunk_idx += 1
            self.row_count = 0
            self.buf = BytesIO()
            return

        raw = self.buf.getvalue()
        if not raw:
            self.chunk_idx += 1
            self.row_count = 0
            self.buf = BytesIO()
            return

        chunk_file = self.table_dir / self.manifest.chunk_filename(self.chunk_idx)

        # insert mode: convert COPY data to SQL
        if self.settings.dump_mode == "insert":
            is_first = (self.chunk_idx == 0)
            is_last = (self.chunk_idx == self.chunks_total - 1)
            raw = _build_sql_chunk(
                raw, self.table, self.settings, self.schema_ddl,
                self.index_defs, is_first=is_first, is_last=is_last,
            )

        # write to disk
        if self.settings.compress:
            with gzip.open(chunk_file, "wb", compresslevel=self.settings.compress_level) as f:
                f.write(raw)
        else:
            with open(chunk_file, "wb") as f:
                f.write(raw)

        # update manifest + progress
        self.chunk_idx += 1
        self.manifest.chunks_completed = self.chunk_idx
        _write_manifest(self.table_dir / "manifest.json", self.manifest)

        elapsed = time.monotonic() - self.t_start
        written_rows = self.total_rows_seen - self.skip_rows
        rps = int(written_rows / elapsed) if elapsed > 0 else 0
        self.progress.update(self.total_rows_seen, self.chunk_idx, f"{humanize.intcomma(rps)} rows/s")

        # reset for next chunk
        self.row_count = 0
        self.buf = BytesIO()

    def flush_remaining(self):
        """flush any remaining buffered data as the final chunk."""
        # handle leftover partial line (shouldn't happen with COPY, but be safe)
        if self._leftover:
            self.buf.write(self._leftover)
            self.row_count += 1
            self.total_rows_seen += 1
            self._leftover = b""

        if self.row_count > 0:
            # mark as last chunk for insert mode epilogue
            self.chunks_total = self.chunk_idx + 1
            self.manifest.chunks_total = self.chunks_total
            self._flush_chunk()


class _SingleFileWriter:
    """file-like that receives streaming COPY output and writes to one compressed file."""

    def __init__(self, out_path: Path, compress: bool, compress_level: int,
                 total_rows_est: int, table_name: str, progress: ChunkProgress):
        self.total_rows_est = total_rows_est
        self.table_name = table_name
        self.progress = progress
        self.total_rows = 0
        self.t_start = time.monotonic()

        if compress:
            self._fh = gzip.open(out_path, "wb", compresslevel=compress_level)
        else:
            self._fh = open(out_path, "wb")

    def write(self, data: bytes) -> int:
        self._fh.write(data)
        self.total_rows += data.count(b"\n")
        elapsed = time.monotonic() - self.t_start
        rps = int(self.total_rows / elapsed) if elapsed > 0 else 0
        self.progress.update(self.total_rows, 1, f"{humanize.intcomma(rps)} rows/s")
        return len(data)

    def close(self):
        self._fh.close()


def dump_table(
    db_cfg: DBConfig,
    dbname: str,
    table: TableInfo,
    output_dir: Path,
    settings: DumpSettings,
    resume: bool = True,
):
    safe_conn_name = db_cfg.name.replace(" ", "_").replace("(", "").replace(")", "")
    table_dir = output_dir / safe_conn_name / dbname / table.name
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
            # use pg_stat estimate — instant, avoids full table scan
            cur.execute("""
                SELECT n_live_tup FROM pg_stat_user_tables
                WHERE schemaname = %s AND relname = %s
            """, (table.schema, table.name))
            row = cur.fetchone()
            total_rows = max(row[0] if row else 0, table.row_estimate)
            if total_rows == 0:
                # fallback: maybe stats are stale, try a quick count
                cur.execute(f'SELECT count(*) FROM "{table.name}"')
                total_rows = cur.fetchone()[0]
            console.print(f"  [dim]{table.name}[/] ~{humanize.intcomma(total_rows)} rows (estimate)")

            if total_rows == 0:
                console.print(f"  [dim]empty table, skipping[/]")
                _write_manifest(manifest_path, _make_manifest(
                    db_cfg, dbname, table, settings, 0, 0, 0,
                    has_schema=has_schema, finished=True,
                ))
                return

            # ── single-file mode ──────────────────────────────────────
            if settings.single_file:
                ext = ".csv.gz" if settings.compress else ".csv"
                out_file = table_dir / f"{table.name}{ext}"

                if out_file.exists() and manifest_path.exists():
                    existing = json.loads(manifest_path.read_text())
                    if existing.get("finished_at"):
                        console.print(f"  [dim]skipping {table.name} — already completed[/]")
                        return

                manifest = _make_manifest(
                    db_cfg, dbname, table, settings, total_rows,
                    0, 1, has_schema=has_schema,
                )
                _write_manifest(manifest_path, manifest)

                copy_sql = f'COPY "{table.name}" TO STDOUT'

                with ChunkProgress(table.name, "bold blue", total_rows, 0, 1, total_rows) as prog:
                    writer = _SingleFileWriter(
                        out_file, settings.compress, settings.compress_level,
                        total_rows, table.name, prog,
                    )
                    try:
                        cur.copy_expert(copy_sql, writer)
                    except KeyboardInterrupt:
                        conn.cancel()
                        writer.close()
                        raise
                    writer.close()
                    rows_dumped = writer.total_rows

                if not interrupted:
                    manifest.chunks_completed = 1
                    manifest.total_rows = rows_dumped
                    manifest.finished_at = datetime.now().isoformat()
                    _write_manifest(manifest_path, manifest)
                    fsize = out_file.stat().st_size
                    console.print(
                        f"  [success]✓ {table.name}[/] — "
                        f"{humanize.intcomma(rows_dumped)} rows, "
                        f"single file, "
                        f"{humanize.naturalsize(fsize, binary=True)} on disk"
                    )
                else:
                    console.print(f"  [warning]⏸ {table.name} — interrupted[/]")
                return

            # ── chunked mode ─────────────────────────────────────────
            chunk_rows = settings.chunk_rows
            chunks_total = (total_rows + chunk_rows - 1) // chunk_rows
            manifest = _make_manifest(
                db_cfg, dbname, table, settings, total_rows,
                start_chunk, chunks_total, has_schema=has_schema,
            )
            _write_manifest(manifest_path, manifest)

            # raw COPY — fastest possible path, pure sequential heap scan
            # no ORDER BY, no subquery overhead
            copy_sql = f'COPY "{table.name}" TO STDOUT'

            with ChunkProgress(table.name, "bold blue", total_rows, start_chunk, chunks_total, chunk_rows) as prog:
                splitter = _ChunkSplitter(
                    table_dir=table_dir,
                    table=table,
                    manifest=manifest,
                    settings=settings,
                    schema_ddl=schema_ddl,
                    index_defs=index_defs,
                    chunks_total=chunks_total,
                    start_chunk=start_chunk,
                    progress=prog,
                )

                try:
                    cur.copy_expert(copy_sql, splitter)
                except KeyboardInterrupt:
                    conn.cancel()
                    raise

                # flush the last partial chunk
                if not interrupted:
                    splitter.flush_remaining()

                chunk_idx = splitter.chunk_idx
                rows_dumped = splitter.total_rows_seen

            if not interrupted:
                manifest.chunks_completed = chunk_idx
                manifest.chunks_total = chunk_idx
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
    """build a complete, self-contained .sql chunk file."""
    parts: list[str] = []

    parts.append(f"-- dbtool dump: {table.name}")
    parts.append(f"-- generated: {datetime.now().isoformat()}")
    if is_first:
        parts.append("-- chunk: 0 (first — includes preamble)")
    if is_last:
        parts.append("-- chunk: last (includes epilogue)")
    parts.append("")

    if is_first:
        if settings.drop_on_restore:
            parts.append(f'DROP TABLE IF EXISTS "{table.name}" CASCADE;')
            parts.append("")
        if settings.recreate_schema and schema_ddl:
            parts.append("-- schema")
            parts.append(schema_ddl)
            parts.append("")
        elif settings.drop_on_restore and schema_ddl:
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

    if settings.use_transactions:
        parts.append("BEGIN;")
        parts.append("")

    inserts = _copy_to_inserts(copy_data, table.name, table.columns, settings.insert_batch_size)
    parts.append(inserts)

    if settings.use_transactions:
        parts.append("COMMIT;")
        parts.append("")

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
        single_file=settings.single_file,
    )


def _write_manifest(path: Path, manifest: DumpManifest):
    path.write_text(json.dumps(asdict(manifest), indent=2))