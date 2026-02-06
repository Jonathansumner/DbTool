from dataclasses import dataclass

import humanize
import psycopg2

from .config import DBConfig


def connect(db_cfg: DBConfig, dbname: str):
    return psycopg2.connect(
        host=db_cfg.host,
        port=db_cfg.port,
        user=db_cfg.user,
        password=db_cfg.password,
        dbname=dbname,
        connect_timeout=10,
        options="-c statement_timeout=0",
    )


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool
    default: str | None
    is_pk: bool


@dataclass
class TableInfo:
    schema: str
    name: str
    row_estimate: int
    size_bytes: int
    total_size_bytes: int
    columns: list[str]
    pk_columns: list[str]

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.name}" if self.schema != "public" else self.name

    @property
    def display_size(self) -> str:
        return humanize.naturalsize(self.size_bytes, binary=True)

    @property
    def display_total_size(self) -> str:
        return humanize.naturalsize(self.total_size_bytes, binary=True)

    @property
    def display_rows(self) -> str:
        return humanize.intcomma(self.row_estimate)


def get_tables(db_cfg: DBConfig, dbname: str) -> list[TableInfo]:
    conn = connect(db_cfg, dbname)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    schemaname, relname, n_live_tup,
                    pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname)),
                    pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname))
                FROM pg_stat_user_tables
                ORDER BY pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname)) DESC
            """)
            rows = cur.fetchall()

            tables = []
            for schema, name, est_rows, size_bytes, total_size in rows:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, name))
                columns = [r[0] for r in cur.fetchall()]

                cur.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass
                      AND i.indisprimary
                    ORDER BY array_position(i.indkey, a.attnum)
                """, (schema, name))
                pk_cols = [r[0] for r in cur.fetchall()]

                tables.append(TableInfo(
                    schema=schema, name=name,
                    row_estimate=est_rows or 0,
                    size_bytes=size_bytes or 0,
                    total_size_bytes=total_size or 0,
                    columns=columns,
                    pk_columns=pk_cols,
                ))
            return tables
    finally:
        conn.close()


def get_column_details(db_cfg: DBConfig, dbname: str, table: TableInfo) -> list[ColumnInfo]:
    conn = connect(db_cfg, dbname)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale
                FROM information_schema.columns c
                WHERE c.table_schema = %s AND c.table_name = %s
                ORDER BY c.ordinal_position
            """, (table.schema, table.name))

            cols = []
            for name, dtype, nullable, default, char_len, num_prec, num_scale in cur.fetchall():
                # build a readable type string
                if char_len:
                    dtype = f"{dtype}({char_len})"
                elif dtype == "numeric" and num_prec:
                    dtype = f"numeric({num_prec},{num_scale or 0})"

                cols.append(ColumnInfo(
                    name=name,
                    data_type=dtype,
                    nullable=nullable == "YES",
                    default=default,
                    is_pk=name in table.pk_columns,
                ))
            return cols
    finally:
        conn.close()


def get_index_info(db_cfg: DBConfig, dbname: str, table: TableInfo) -> list[tuple[str, str, bool]]:
    """returns (index_name, definition, is_unique) tuples."""
    conn = connect(db_cfg, dbname)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname, indexdef,
                       (SELECT indisunique FROM pg_index WHERE indexrelid = (quote_ident(%s) || '.' || quote_ident(indexname))::regclass)
                FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s
                ORDER BY indexname
            """, (table.schema, table.schema, table.name))
            return [(name, defn, unique or False) for name, defn, unique in cur.fetchall()]
    finally:
        conn.close()


def get_table_ddl(db_cfg: DBConfig, dbname: str, table: TableInfo) -> str:
    """extract full DDL for a table: CREATE TABLE + constraints + indexes."""
    conn = connect(db_cfg, dbname)
    try:
        with conn.cursor() as cur:
            # column definitions
            cur.execute("""
                SELECT
                    a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    a.attnotnull,
                    pg_get_expr(d.adbin, d.adrelid)
                FROM pg_attribute a
                LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                WHERE a.attrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass
                  AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """, (table.schema, table.name))

            col_defs = []
            for name, dtype, notnull, default in cur.fetchall():
                parts = [f'    "{name}" {dtype}']
                if default:
                    parts.append(f"DEFAULT {default}")
                if notnull:
                    parts.append("NOT NULL")
                col_defs.append(" ".join(parts))

            # primary key
            pk_clause = ""
            if table.pk_columns:
                pk_cols = ", ".join(f'"{c}"' for c in table.pk_columns)
                pk_clause = f",\n    PRIMARY KEY ({pk_cols})"

            schema_prefix = f'"{table.schema}".' if table.schema != "public" else ""
            ddl = f'CREATE TABLE IF NOT EXISTS {schema_prefix}"{table.name}" (\n'
            ddl += ",\n".join(col_defs)
            ddl += pk_clause
            ddl += "\n);\n"

            # indexes (excluding primary key)
            cur.execute("""
                SELECT indexdef FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s
                  AND indexname NOT IN (
                      SELECT conname FROM pg_constraint
                      WHERE conrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass
                        AND contype = 'p'
                  )
                ORDER BY indexname
            """, (table.schema, table.name, table.schema, table.name))

            for (indexdef,) in cur.fetchall():
                ddl += f"{indexdef};\n"

            return ddl
    finally:
        conn.close()