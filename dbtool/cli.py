import argparse
import json
import signal
import subprocess
import threading
import time
from dataclasses import asdict, fields
from pathlib import Path

import humanize
from InquirerPy import inquirer
from InquirerPy.separator import Separator
from rich import box
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table

from .config import (
    DBConfig, DumpSettings, load_config, save_config, get_connections,
    get_settings, save_settings,
)
from .db import connect, get_tables, get_column_details, get_index_info
from .dump import dump_table
from .restore import restore_table
from .k8s import (
    require_tools, get_current_context, list_contexts, switch_context,
    get_current_namespace, list_namespaces, switch_namespace,
    list_pods, kube_cp_to_pod, kube_cp_from_pod, kube_mkdir, kube_exec,
    kube_psql, kube_psql_pipe, kube_write_file, kube_read_file,
)
from .ui import console, interrupted, reset_interrupt, _signal_handler, TransferProgress


# ── menu wrappers ────────────────────────────────────────────────────────────

_KB_SELECT = {"answer": [{"key": "right"}], "skip": [{"key": "left"}]}
_KB_CHECK  = {"toggle": [{"key": "right"}], "skip": [{"key": "left"}]}


def _rearm():
    signal.signal(signal.SIGINT, _signal_handler)


def _select(message, choices, **kwargs):
    result = inquirer.select(
        message=message, choices=choices,
        keybindings=_KB_SELECT, mandatory=False, **kwargs,
    ).execute()
    _rearm()
    return result


def _checkbox(message, choices, **kwargs):
    result = inquirer.checkbox(
        message=message, choices=choices,
        keybindings=_KB_CHECK, mandatory=False, **kwargs,
    ).execute()
    _rearm()
    return result


# ── formatting helpers ───────────────────────────────────────────────────────

def _pad(text: str, width: int) -> str:
    return f"{text:<{width}}"


def _table_line(t, mw: int) -> str:
    """format a table entry: name  rows  size  pk"""
    return (f"{_pad(t.name, mw)}  "
            f"{t.display_rows:>12} rows  "
            f"{t.display_total_size:>10}  "
            f"pk: {', '.join(t.pk_columns) if t.pk_columns else '—'}")


def _settings_line(name: str, val, desc: str, mw: int) -> str:
    """format a settings entry: name  value  description"""
    if isinstance(val, bool):
        vs = f"{'✓ on' if val else '✗ off':>10}"
    else:
        vs = f"{str(val):>10}"
    return f"{_pad(name, mw)}  {vs}    {desc}"


def _conn_line(c: DBConfig, mw: int) -> str:
    return f"{_pad(c.name, mw)}  {c.user}@{c.host}:{c.port}  [{', '.join(c.databases)}]"


def _tables_header(tables) -> str:
    total = sum(t.total_size_bytes for t in tables)
    return (f"[info]{len(tables)} tables[/], "
            f"[yellow]{humanize.naturalsize(total, binary=True)}[/] total")


# ── connection helpers ───────────────────────────────────────────────────────

def add_connection_interactive() -> DBConfig:
    console.print("\n[header]➕ add new database connection[/]\n")
    name = inquirer.text(message="Connection name:", default="").execute()
    host = inquirer.text(message="Host:", default="localhost").execute()
    port = inquirer.number(message="Port:", default=6543, min_allowed=1, max_allowed=65535).execute()
    user = inquirer.text(message="Username:", default="alexandria").execute()
    password = inquirer.secret(message="Password:", default="alexandria123").execute()
    dbs_raw = inquirer.text(message="Databases (comma-separated):", default="index,cache").execute()
    databases = [d.strip() for d in dbs_raw.split(",") if d.strip()]
    _rearm()
    return DBConfig(name=name, host=host, port=int(port), user=user, password=password, databases=databases)


def select_connection(cfg: dict) -> DBConfig | None:
    connections = get_connections(cfg)

    if not connections:
        console.print("[info]no connections configured yet.[/]")
        db = add_connection_interactive()
        cfg.setdefault("connections", []).append(asdict(db))
        save_config(cfg)
        return db

    mw = max(len(c.name) for c in connections)
    choices = [{"name": _conn_line(c, mw), "value": c.name} for c in connections]
    choices.append(Separator())
    choices.append({"name": "➕ Add new connection", "value": "__add__"})

    selected = _select("Select connection:", choices)
    if selected is None:
        return None

    if selected == "__add__":
        db = add_connection_interactive()
        cfg.setdefault("connections", []).append(asdict(db))
        save_config(cfg)
        return db

    return next(c for c in connections if c.name == selected)


def select_database(db_cfg: DBConfig) -> str | None:
    if len(db_cfg.databases) == 1:
        return db_cfg.databases[0]
    return _select("Select database:", db_cfg.databases)


def select_tables(db_cfg, dbname):
    with console.status("[info]fetching table info…[/]"):
        tables = get_tables(db_cfg, dbname)

    if not tables:
        console.print("[warning]no tables found.[/]")
        return []

    mw = max(len(t.name) for t in tables)
    console.print(_tables_header(tables))

    choices = [{"name": "★ All tables", "value": "__all__"}, Separator()]
    for t in tables:
        choices.append({"name": _table_line(t, mw), "value": t.name})

    selected = _checkbox("Select tables (space=toggle, enter=confirm):", choices)
    if selected is None:
        return []
    if not selected:
        console.print("[warning]no tables selected — use space to toggle.[/]")
        return []
    if "__all__" in selected:
        return tables
    return [t for t in tables if t.name in selected]


# ── flows ────────────────────────────────────────────────────────────────────

def flow_browse(cfg: dict):
    console.print(Panel("[header]🔬 BROWSE[/]", expand=False))

    db_cfg = select_connection(cfg)
    if not db_cfg:
        return
    dbname = select_database(db_cfg)
    if not dbname:
        return

    with console.status("[info]fetching table info…[/]"):
        tables = get_tables(db_cfg, dbname)

    if not tables:
        console.print("[warning]no tables found.[/]")
        return

    mw = max(len(t.name) for t in tables)
    console.print(_tables_header(tables))

    while True:
        choices = [{"name": _table_line(t, mw), "value": t.name} for t in tables]
        choices.append(Separator())
        choices.append({"name": "← Back", "value": "__back__"})

        try:
            selected = _select(f"Inspect table ({dbname}):", choices)
        except KeyboardInterrupt:
            break

        if selected is None or selected == "__back__":
            break

        table = next(t for t in tables if t.name == selected)
        _inspect_table(db_cfg, dbname, table)


def _inspect_table(db_cfg: DBConfig, dbname: str, table):
    console.print()
    console.print(f"[header]{table.name}[/] — {table.display_rows} rows, "
                  f"data: {table.display_size}, total: {table.display_total_size}")

    with console.status("[dim]fetching column details…[/]"):
        columns = get_column_details(db_cfg, dbname, table)

    col_tbl = Table(title=f"Columns — {table.name}", box=box.ROUNDED, show_lines=False)
    col_tbl.add_column("#", style="dim", width=4)
    col_tbl.add_column("Column", style="cyan")
    col_tbl.add_column("Type", style="green")
    col_tbl.add_column("Nullable", justify="center")
    col_tbl.add_column("Default", style="dim")
    col_tbl.add_column("PK", justify="center", style="yellow")

    for i, c in enumerate(columns):
        col_tbl.add_row(
            str(i + 1), c.name, c.data_type,
            "✓" if c.nullable else "✗",
            c.default or "", "🔑" if c.is_pk else "",
        )
    console.print(col_tbl)

    with console.status("[dim]fetching indexes…[/]"):
        indexes = get_index_info(db_cfg, dbname, table)

    if indexes:
        idx_tbl = Table(title=f"Indexes — {table.name}", box=box.ROUNDED, show_lines=False)
        idx_tbl.add_column("Name", style="cyan")
        idx_tbl.add_column("Unique", justify="center")
        idx_tbl.add_column("Definition", style="dim")
        for name, defn, unique in indexes:
            idx_tbl.add_row(name, "✓" if unique else "", defn)
        console.print(idx_tbl)
    else:
        console.print("[dim]no indexes[/]")


def flow_dump(cfg: dict):
    console.print(Panel("[header]📦 DUMP[/]", expand=False))

    db_cfg = select_connection(cfg)
    if not db_cfg:
        return
    dbname = select_database(db_cfg)
    if not dbname:
        return
    tables = select_tables(db_cfg, dbname)
    if not tables:
        return

    settings = get_settings(cfg)
    output_dir = Path(settings.dump_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    console.print()
    mode_info = f"mode: {settings.dump_mode}"
    if settings.dump_schema:
        mode_info += " + schema"
    console.print(f"[info]dumping {len(tables)} table(s) from {db_cfg.name}/{dbname}[/]")
    console.print(f"[dim]chunks: {humanize.intcomma(settings.chunk_rows)} rows | "
                  f"compress: {settings.compress} | {mode_info} | output: {output_dir}[/]")
    console.print()

    reset_interrupt()
    for table in tables:
        if interrupted:
            break
        dump_table(db_cfg, dbname, table, output_dir, settings)

    if not interrupted:
        console.print()
        console.print("[success]✅ dump complete![/]")
        total_size = sum(f.stat().st_size for f in output_dir.rglob("*") if f.is_file())
        console.print(f"[info]total output: {humanize.naturalsize(total_size, binary=True)} in {output_dir}[/]")


def flow_restore(cfg: dict):
    console.print(Panel("[header]📥 RESTORE[/]", expand=False))

    settings = get_settings(cfg)
    dump_dir = Path(settings.dump_dir)

    if not dump_dir.exists():
        console.print(f"[error]dump dir not found: {dump_dir}[/]")
        console.print("[dim]set dump_dir in ⚙ Settings[/]")
        return

    # scan: dump_dir / {conn_name} / {dbname} / {table}
    available_conns = []
    for conn_dir in sorted(dump_dir.iterdir()):
        if not conn_dir.is_dir():
            continue
        dbs = []
        for db_dir in sorted(conn_dir.iterdir()):
            if not db_dir.is_dir():
                continue
            tables = [t.name for t in db_dir.iterdir()
                      if t.is_dir() and (t / "manifest.json").exists()]
            if tables:
                dbs.append((db_dir.name, len(tables)))
        if dbs:
            total_tables = sum(n for _, n in dbs)
            db_names = ", ".join(d for d, _ in dbs)
            available_conns.append((conn_dir.name, dbs, total_tables, db_names))

    if not available_conns:
        console.print(f"[warning]no dumps found in {dump_dir}[/]")
        return

    # select connection
    conn_choices = [
        {"name": f"{_pad(name, 25)}  {total}t across {db_str}", "value": name}
        for name, _, total, db_str in available_conns
    ]
    selected_conn = _select("Select source connection:", conn_choices)
    if selected_conn is None:
        return

    conn_entry = next(c for c in available_conns if c[0] == selected_conn)
    conn_dump_dir = dump_dir / selected_conn

    # select database
    db_list = conn_entry[1]
    if len(db_list) == 1:
        selected_db = db_list[0][0]
    else:
        db_choices = [
            {"name": f"{_pad(name, 20)}  {count} tables", "value": name}
            for name, count in db_list
        ]
        selected_db = _select("Select database:", db_choices)
        if selected_db is None:
            return

    db_dump_dir = conn_dump_dir / selected_db
    available_tables = []
    for t_dir in sorted(db_dump_dir.iterdir()):
        if t_dir.is_dir() and (t_dir / "manifest.json").exists():
            m = json.loads((t_dir / "manifest.json").read_text())
            available_tables.append((t_dir, m))

    info = Table(title=f"Dumps — {selected_conn} / {selected_db}", box=box.ROUNDED)
    info.add_column("Table", style="cyan")
    info.add_column("Rows", justify="right", style="green")
    info.add_column("Chunks", justify="right")
    info.add_column("Mode", style="dim")
    info.add_column("Status", style="yellow")
    info.add_column("Size", justify="right", style="blue")

    for t_dir, m in available_tables:
        dump_size = sum(f.stat().st_size for f in t_dir.iterdir() if f.suffix in (".gz", ".csv", ".sql"))
        status = _restore_status(t_dir, m)
        info.add_row(
            m["table"], humanize.intcomma(m["total_rows"]),
            str(m["chunks_total"]), m.get("dump_mode", "copy"),
            status, humanize.naturalsize(dump_size, binary=True),
        )
    console.print(info)

    table_choices = [{"name": "★ All tables", "value": "__all__"}, Separator()]
    mw = max(len(m["table"]) for _, m in available_tables)
    for t_dir, m in available_tables:
        rows = humanize.intcomma(m["total_rows"])
        table_choices.append({
            "name": f"{_pad(m['table'], mw)}  {rows:>12} rows",
            "value": m["table"],
        })

    selected = _checkbox("Select tables to restore:", table_choices)
    if not selected:
        return
    if "__all__" in selected:
        restore_tables = available_tables
    else:
        restore_tables = [(d, m) for d, m in available_tables if m["table"] in selected]

    if not restore_tables:
        return

    console.print("\n[header]select restore target:[/]")
    db_cfg = select_connection(cfg)
    if not db_cfg:
        return
    dbname = select_database(db_cfg)
    if not dbname:
        return

    console.print()
    console.print(f"[info]restoring {len(restore_tables)} table(s) to {db_cfg.name}/{dbname}[/]")
    console.print(f"[dim]truncate: {settings.truncate_before_restore} | "
                  f"drop: {settings.drop_on_restore} | "
                  f"recreate schema: {settings.recreate_schema} | "
                  f"disable indexes: {settings.disable_indexes_on_restore}[/]")
    console.print()

    reset_interrupt()
    for t_dir, m in restore_tables:
        if interrupted:
            break
        restore_table(db_cfg, dbname, t_dir, settings)

    if not interrupted:
        console.print()
        console.print("[success]✅ restore complete![/]")


def flow_connections(cfg: dict):
    console.print(Panel("[header]🔌 CONNECTIONS[/]", expand=False))

    while True:
        connections = get_connections(cfg)
        if connections:
            tbl = Table(box=box.ROUNDED, show_lines=False)
            tbl.add_column("#", style="dim", width=3)
            tbl.add_column("Name", style="cyan")
            tbl.add_column("Host:Port", style="green")
            tbl.add_column("User", style="yellow")
            tbl.add_column("Databases")
            for i, c in enumerate(connections):
                tbl.add_row(str(i + 1), c.name, f"{c.host}:{c.port}", c.user, ", ".join(c.databases))
            console.print(tbl)
        else:
            console.print("[dim]no connections configured.[/]")

        action = _select("Action:", [
            "➕ Add connection",
            "🗑  Remove connection",
            "🔍 Test connection",
            Separator(),
            "← Back",
        ])

        if action is None or action == "← Back":
            break
        elif action == "➕ Add connection":
            db = add_connection_interactive()
            cfg.setdefault("connections", []).append(asdict(db))
            save_config(cfg)
            console.print(f"[success]added: {db.display}[/]")
        elif action == "🗑  Remove connection":
            if not connections:
                continue
            name = _select("Remove which?", [c.name for c in connections])
            if name is None:
                continue
            cfg["connections"] = [c for c in cfg["connections"] if c["name"] != name]
            save_config(cfg)
            console.print(f"[success]removed: {name}[/]")
        elif action == "🔍 Test connection":
            if not connections:
                continue
            name = _select("Test which?", [c.name for c in connections])
            if name is None:
                continue
            db = next(c for c in connections if c.name == name)
            for dbname in db.databases:
                try:
                    conn = connect(db, dbname)
                    conn.close()
                    console.print(f"  [success]✓ {db.name}/{dbname} — ok[/]")
                except Exception as e:
                    console.print(f"  [error]✗ {db.name}/{dbname} — {e}[/]")


# ── settings flow ────────────────────────────────────────────────────────────

def flow_settings(cfg: dict):
    console.print(Panel("[header]⚙  SETTINGS[/]", expand=False))
    settings = get_settings(cfg)
    descs = DumpSettings.descriptions()
    mw = max(len(f.name) for f in fields(settings))

    while True:
        choices = []
        for f in fields(settings):
            val = getattr(settings, f.name)
            desc = descs.get(f.name, "")
            choices.append({
                "name": _settings_line(f.name, val, desc, mw),
                "value": f.name,
            })
        choices.append(Separator())
        choices.append({"name": "← Back", "value": "__back__"})

        selected = _select("Settings (enter to edit):", choices)
        if selected is None or selected == "__back__":
            break

        _edit_setting(settings, selected, descs)
        save_settings(cfg, settings)
        console.print("[success]saved.[/]")


def _edit_setting(settings: DumpSettings, name: str, descs: dict):
    current = getattr(settings, name)
    desc = descs.get(name, "")

    if isinstance(current, bool):
        new_val = not current
        tag = "✓ on" if new_val else "✗ off"
        console.print(f"  [info]{name}[/] → {tag}")
        setattr(settings, name, new_val)

    elif name == "dump_mode":
        val = _select(f"{name}:", [
            {"name": "copy    — native COPY TO/FROM (fast)", "value": "copy"},
            {"name": "insert  — INSERT statements (portable SQL)", "value": "insert"},
        ])
        if val:
            setattr(settings, name, val)

    elif name == "dump_dir":
        val = inquirer.text(message=f"{name} ({desc}):", default=current).execute()
        _rearm()
        if val:
            setattr(settings, name, val)

    elif isinstance(current, int):
        val = inquirer.number(
            message=f"{name} ({desc}):", default=current,
            min_allowed=1 if name != "retry_backoff" else 0,
        ).execute()
        _rearm()
        setattr(settings, name, int(val))

    elif isinstance(current, str):
        val = inquirer.text(message=f"{name} ({desc}):", default=current).execute()
        _rearm()
        if val:
            setattr(settings, name, val)


# ── inspect flow ─────────────────────────────────────────────────────────────

def flow_inspect(cfg: dict):
    console.print(Panel("[header]🔍 INSPECT DUMPS[/]", expand=False))

    settings = get_settings(cfg)
    dump_dir = Path(settings.dump_dir)

    if not dump_dir.exists():
        console.print(f"[error]dump dir not found: {dump_dir}[/]")
        console.print("[dim]set dump_dir in ⚙ Settings[/]")
        return

    while True:
        # collect all dump entries
        all_dumps = []
        for conn_dir in sorted(dump_dir.iterdir()):
            if not conn_dir.is_dir():
                continue
            for db_dir in sorted(conn_dir.iterdir()):
                if not db_dir.is_dir():
                    continue

                console.print(f"\n[header]{conn_dir.name} / {db_dir.name}[/]")

                tbl = Table(box=box.ROUNDED)
                tbl.add_column("Table", style="cyan")
                tbl.add_column("Rows", justify="right", style="green")
                tbl.add_column("Chunks", justify="right")
                tbl.add_column("Mode", style="dim")
                tbl.add_column("Schema", justify="center")
                tbl.add_column("Size", justify="right", style="blue")
                tbl.add_column("Status", style="yellow")

                has_rows = False
                for t_dir in sorted(db_dir.iterdir()):
                    mf = t_dir / "manifest.json"
                    if not mf.exists():
                        continue
                    m = json.loads(mf.read_text())
                    dump_size = sum(f.stat().st_size for f in t_dir.iterdir() if f.suffix in (".gz", ".csv", ".sql"))
                    status = _restore_status(t_dir, m)
                    has_schema = "✓" if m.get("has_schema") or (t_dir / "schema.sql").exists() else ""
                    tbl.add_row(
                        m["table"], humanize.intcomma(m["total_rows"]),
                        f"{m['chunks_completed']}/{m['chunks_total']}",
                        m.get("dump_mode", "copy"), has_schema,
                        humanize.naturalsize(dump_size, binary=True), status,
                    )
                    all_dumps.append((t_dir, m, conn_dir.name, db_dir.name))
                    has_rows = True
                if has_rows:
                    console.print(tbl)

        if not all_dumps:
            console.print(f"[warning]no dumps found in {dump_dir}[/]")
            return

        action = _select("Action:", [
            {"name": "🗑  Delete dumps", "value": "delete"},
            Separator(),
            {"name": "← Back", "value": "back"},
        ])

        if action is None or action == "back":
            break

        if action == "delete":
            _inspect_delete(all_dumps)


def _inspect_delete(all_dumps: list):
    """let the user select dumps to delete."""
    mw = max(len(f"{conn}/{db}/{m['table']}") for _, m, conn, db in all_dumps)
    choices = [{"name": "★ All dumps", "value": "__all__"}, Separator()]
    for t_dir, m, conn, db in all_dumps:
        label = f"{conn}/{db}/{m['table']}"
        dump_size = sum(f.stat().st_size for f in t_dir.iterdir() if f.suffix in (".gz", ".csv", ".sql"))
        choices.append({
            "name": (f"{_pad(label, mw)}  "
                     f"{humanize.intcomma(m['total_rows']):>12} rows  "
                     f"{humanize.naturalsize(dump_size, binary=True):>10}"),
            "value": str(t_dir),
        })

    selected = _checkbox("Select dumps to delete:", choices)
    if not selected:
        return

    if "__all__" in selected:
        targets = [(t_dir, m, conn, db) for t_dir, m, conn, db in all_dumps]
    else:
        targets = [(t_dir, m, conn, db) for t_dir, m, conn, db in all_dumps if str(t_dir) in selected]

    if not targets:
        return

    total_size = sum(
        sum(f.stat().st_size for f in t_dir.rglob("*") if f.is_file())
        for t_dir, _, _, _ in targets
    )

    confirm = _select(
        f"Delete {len(targets)} dump(s) ({humanize.naturalsize(total_size, binary=True)})?",
        [
            {"name": "Yes — delete permanently", "value": True},
            {"name": "No — cancel", "value": False},
        ],
    )
    if not confirm:
        console.print("[dim]cancelled[/]")
        return

    import shutil
    for t_dir, m, conn, db in targets:
        shutil.rmtree(t_dir)
        console.print(f"  [dim]deleted {conn}/{db}/{m['table']}[/]")

        # clean up empty parent dirs
        db_dir = t_dir.parent
        if db_dir.exists() and not any(db_dir.iterdir()):
            db_dir.rmdir()
            conn_dir = db_dir.parent
            if conn_dir.exists() and not any(conn_dir.iterdir()):
                conn_dir.rmdir()

    console.print(f"[success]✓ deleted {len(targets)} dump(s) ({humanize.naturalsize(total_size, binary=True)})[/]")


def _restore_status(t_dir: Path, m: dict) -> str:
    rs = t_dir / "restore_state.json"
    if rs.exists():
        rstate = json.loads(rs.read_text())
        restored = rstate.get("chunks_restored", 0)
        if restored >= m["chunks_total"]:
            return "✓ restored"
        return f"↻ restored {restored}/{m['chunks_total']}"
    if m.get("finished_at"):
        return "✓ dumped"
    return f"⏸ {m['chunks_completed']}/{m['chunks_total']}"


# ── k8s flow ─────────────────────────────────────────────────────────────────

def flow_k8s(cfg: dict):
    console.print(Panel("[header]☸  KUBERNETES TRANSFER[/]", expand=False))

    if not require_tools():
        return

    ctx = get_current_context()
    ns = get_current_namespace()
    console.print(f"[info]context:[/] {ctx or '?'}  [info]namespace:[/] {ns}")

    while True:
        action = _select("K8s action:", [
            {"name": f"📋 Switch context    (current: {ctx or '?'})", "value": "ctx"},
            {"name": f"📂 Switch namespace  (current: {ns})", "value": "ns"},
            {"name": "📤 Copy dumps TO pod", "value": "to_pod"},
            {"name": "📥 Copy dumps FROM pod", "value": "from_pod"},
            {"name": "🚀 Full restore on pod (transfer + restore)", "value": "full_restore"},
            {"name": "🔄 Restore on pod (dumps already there)", "value": "pod_restore"},
            Separator(),
            {"name": "← Back", "value": "back"},
        ])

        if action is None or action == "back":
            break

        if action == "ctx":
            ctx = _k8s_switch_context()
            ns = get_current_namespace()
        elif action == "ns":
            ns = _k8s_switch_namespace() or ns
        elif action == "to_pod":
            _k8s_copy_to_pod(cfg, ns)
        elif action == "from_pod":
            _k8s_copy_from_pod(cfg, ns)
        elif action == "full_restore":
            _k8s_full_restore(cfg, ns)
        elif action == "pod_restore":
            _k8s_pod_restore(cfg, ns)


def _k8s_switch_context() -> str | None:
    contexts = list_contexts()
    if not contexts:
        console.print("[warning]no contexts found[/]")
        return None

    choices = []
    for name, current in contexts:
        marker = "► " if current else "  "
        choices.append({"name": f"{marker}{name}", "value": name})

    selected = _select("Switch context:", choices)
    if selected:
        if switch_context(selected):
            console.print(f"[success]switched to: {selected}[/]")
            return selected
    return get_current_context()


def _k8s_switch_namespace() -> str | None:
    namespaces = list_namespaces()
    if not namespaces:
        console.print("[warning]no namespaces found[/]")
        return None

    current = get_current_namespace()
    choices = []
    for ns in namespaces:
        marker = "► " if ns == current else "  "
        choices.append({"name": f"{marker}{ns}", "value": ns})

    selected = _select("Switch namespace:", choices)
    if selected:
        if switch_namespace(selected):
            console.print(f"[success]switched to: {selected}[/]")
            return selected
    return None


def _k8s_select_pod(namespace: str) -> str | None:
    with console.status("[info]listing pods…[/]"):
        pods = list_pods(namespace)

    if not pods:
        console.print("[warning]no pods found[/]")
        return None

    choices = []
    for p in pods:
        status = "✓" if p["ready"] else "✗"
        choices.append({
            "name": f"{status} {_pad(p['name'], 50)}  {p['status']}",
            "value": p["name"],
        })

    return _select("Select pod:", choices)


def _k8s_scan_local_dumps(dump_dir: Path) -> list[tuple[str, str, list[tuple[Path, dict]]]]:
    """scan local dump dir, returns [(conn_name, db_name, [(table_dir, manifest)])]."""
    results = []
    for conn_dir in sorted(dump_dir.iterdir()):
        if not conn_dir.is_dir():
            continue
        for db_dir in sorted(conn_dir.iterdir()):
            if not db_dir.is_dir():
                continue
            tables = []
            for t_dir in sorted(db_dir.iterdir()):
                manifest_path = t_dir / "manifest.json"
                if t_dir.is_dir() and manifest_path.exists():
                    m = json.loads(manifest_path.read_text())
                    tables.append((t_dir, m))
            if tables:
                results.append((conn_dir.name, db_dir.name, tables))
    return results


def _dir_size(d: Path) -> int:
    return sum(f.stat().st_size for f in d.rglob("*") if f.is_file())


def _k8s_select_dump_tables(dump_dir: Path) -> list[tuple[Path, dict]] | None:
    """let the user pick which table dumps to transfer. returns [(table_dir, manifest)] or None."""
    groups = _k8s_scan_local_dumps(dump_dir)
    if not groups:
        console.print(f"[warning]no dumps found in {dump_dir}[/]")
        return None

    if len(groups) == 1:
        conn_name, db_name, tables = groups[0]
    else:
        group_choices = [
            {"name": f"{conn}/{db}  ({len(tbls)} tables)", "value": i}
            for i, (conn, db, tbls) in enumerate(groups)
        ]
        idx = _select("Select dump source:", group_choices)
        if idx is None:
            return None
        conn_name, db_name, tables = groups[idx]

    info = Table(title=f"Dumps — {conn_name}/{db_name}", box=box.ROUNDED)
    info.add_column("Table", style="cyan")
    info.add_column("Rows", justify="right", style="green")
    info.add_column("Chunks", justify="right")
    info.add_column("Size", justify="right", style="blue")
    for t_dir, m in tables:
        info.add_row(
            m["table"], humanize.intcomma(m["total_rows"]),
            str(m["chunks_total"]),
            humanize.naturalsize(_dir_size(t_dir), binary=True),
        )
    console.print(info)

    mw = max(len(m["table"]) for _, m in tables)
    choices = [{"name": "★ All tables", "value": "__all__"}, Separator()]
    for t_dir, m in tables:
        choices.append({
            "name": (f"{_pad(m['table'], mw)}  "
                     f"{humanize.intcomma(m['total_rows']):>12} rows  "
                     f"{humanize.naturalsize(_dir_size(t_dir), binary=True):>10}"),
            "value": t_dir.name,
        })

    selected = _checkbox("Select tables to transfer:", choices)
    if not selected:
        return None
    if "__all__" in selected:
        return tables
    return [(t_dir, m) for t_dir, m in tables if t_dir.name in selected]


def _speed(bytes_done: int, t_start: float) -> str:
    elapsed = time.monotonic() - t_start
    if elapsed > 1 and bytes_done > 0:
        return humanize.naturalsize(bytes_done / elapsed, binary=True) + "/s"
    return ""


def _poll_remote_size(pod: str, remote_path: str, namespace: str | None) -> int:
    """quick du -sb on pod, returns bytes or 0 on failure."""
    cmd = ["kubectl", "exec", pod]
    if namespace:
        cmd.extend(["-n", namespace])
    cmd.extend(["--", "du", "-sb", remote_path])
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if r.returncode == 0:
            return int(r.stdout.split()[0])
    except Exception:
        pass
    return 0


def _poll_local_size(local_path: Path) -> int:
    """quick local dir size check."""
    try:
        return sum(f.stat().st_size for f in local_path.rglob("*") if f.is_file())
    except Exception:
        return 0


def _cp_with_progress(
    copy_fn,
    prog: TransferProgress,
    poll_fn,
    table_name: str,
    table_idx: int,
    bytes_before: int,
    table_size: int,
    t_start: float,
    poll_interval: float = 3.0,
) -> bool:
    """run a copy in a thread while polling for progress updates."""
    result = {"ok": False, "error": None}

    def _do_copy():
        try:
            result["ok"] = copy_fn()
        except Exception as e:
            result["error"] = e

    thread = threading.Thread(target=_do_copy, daemon=True)
    thread.start()

    while thread.is_alive():
        thread.join(timeout=poll_interval)
        current = poll_fn()
        # clamp to table size to avoid overshoot from du rounding
        current = min(current, table_size)
        total_now = bytes_before + current
        prog.update(table_name, table_idx, total_now, _speed(total_now, t_start))

    if result["error"]:
        console.print(f"  [error]{result['error']}[/]")
        return False
    return result["ok"]


def _k8s_copy_to_pod(cfg: dict, namespace: str):
    settings = get_settings(cfg)
    dump_dir = Path(settings.dump_dir)

    if not dump_dir.exists():
        console.print(f"[error]dump dir not found: {dump_dir}[/]")
        return

    selected = _k8s_select_dump_tables(dump_dir)
    if not selected:
        return

    pod = _k8s_select_pod(namespace)
    if not pod:
        return

    remote_path = inquirer.text(message="Remote path on pod:", default="/tmp/dbtool_dumps").execute()
    _rearm()

    table_sizes = [(t_dir, m, _dir_size(t_dir)) for t_dir, m in selected]
    total_bytes = sum(s for _, _, s in table_sizes)

    console.print(
        f"\n[info]{len(selected)} table(s) "
        f"({humanize.naturalsize(total_bytes, binary=True)}) → {pod}[/]\n"
    )

    reset_interrupt()
    bytes_done = 0
    t_start = time.monotonic()

    with TransferProgress(len(selected), total_bytes) as prog:
        for i, (t_dir, m, tsize) in enumerate(table_sizes):
            if interrupted:
                console.print(f"\n[warning]⏸ stopped at table {i}/{len(selected)}[/]")
                return

            rel = t_dir.relative_to(dump_dir)
            remote_table = f"{remote_path}/{rel}"
            remote_parent = str(Path(remote_table).parent)

            prog.update(m["table"], i, bytes_done, _speed(bytes_done, t_start))

            if not kube_mkdir(pod, remote_parent, namespace):
                return

            ok = _cp_with_progress(
                copy_fn=lambda td=t_dir, rt=remote_table: kube_cp_to_pod(td, pod, rt, namespace, quiet=True),
                prog=prog,
                poll_fn=lambda rt=remote_table: _poll_remote_size(pod, rt, namespace),
                table_name=m["table"],
                table_idx=i,
                bytes_before=bytes_done,
                table_size=tsize,
                t_start=t_start,
            )

            if not ok:
                console.print(f"\n[error]✗ failed: {m['table']}[/]")
                return

            bytes_done += tsize
            prog.update(m["table"], i + 1, bytes_done, _speed(bytes_done, t_start))

    console.print(
        f"\n[success]✅ {len(selected)} table(s) "
        f"({humanize.naturalsize(total_bytes, binary=True)}) — "
        f"avg {_speed(total_bytes, t_start)}[/]"
    )


def _k8s_list_remote_dumps(pod: str, remote_path: str, namespace: str | None) -> list[tuple[str, int]]:
    """list table directories on a pod. returns [(remote_table_dir, size_bytes)]."""
    cmd = ["kubectl", "exec", pod]
    if namespace:
        cmd.extend(["-n", namespace])
    cmd.extend(["--", "find", remote_path, "-name", "manifest.json", "-type", "f"])
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            return []
        dirs = []
        for line in r.stdout.strip().splitlines():
            line = line.strip()
            if line:
                dirs.append(str(Path(line).parent))

        results = []
        for d in sorted(dirs):
            size_cmd = ["kubectl", "exec", pod]
            if namespace:
                size_cmd.extend(["-n", namespace])
            size_cmd.extend(["--", "du", "-sb", d])
            try:
                sr = subprocess.run(size_cmd, capture_output=True, text=True, timeout=15)
                size = int(sr.stdout.split()[0]) if sr.returncode == 0 else 0
            except Exception:
                size = 0
            results.append((d, size))
        return results
    except Exception:
        return []


def _k8s_copy_from_pod(cfg: dict, namespace: str):
    settings = get_settings(cfg)
    dump_dir = Path(settings.dump_dir)

    pod = _k8s_select_pod(namespace)
    if not pod:
        return

    remote_path = inquirer.text(message="Remote path on pod:", default="/tmp/dbtool_dumps").execute()
    _rearm()

    with console.status("[info]scanning remote dumps…[/]"):
        remote_tables = _k8s_list_remote_dumps(pod, remote_path, namespace)

    if not remote_tables:
        console.print(f"[warning]no table dumps found at {pod}:{remote_path}[/]")
        return

    mw = max(len(Path(d).relative_to(remote_path).as_posix()) for d, _ in remote_tables)
    choices = [{"name": "★ All tables", "value": "__all__"}, Separator()]
    for d, sz in remote_tables:
        rel = Path(d).relative_to(remote_path).as_posix()
        choices.append({
            "name": f"{_pad(rel, mw)}  {humanize.naturalsize(sz, binary=True):>10}",
            "value": d,
        })

    selected = _checkbox("Select tables to copy from pod:", choices)
    if not selected:
        return
    if "__all__" in selected:
        selected_tables = remote_tables
    else:
        selected_tables = [(d, sz) for d, sz in remote_tables if d in selected]

    total_bytes = sum(sz for _, sz in selected_tables)
    dump_dir.mkdir(parents=True, exist_ok=True)

    console.print(
        f"\n[info]{len(selected_tables)} table(s) "
        f"({humanize.naturalsize(total_bytes, binary=True)}) ← {pod}[/]\n"
    )

    reset_interrupt()
    bytes_done = 0
    t_start = time.monotonic()

    with TransferProgress(len(selected_tables), total_bytes) as prog:
        for i, (remote_dir, tsize) in enumerate(selected_tables):
            if interrupted:
                console.print(f"\n[warning]⏸ stopped at table {i}/{len(selected_tables)}[/]")
                return

            rel = Path(remote_dir).relative_to(remote_path)
            local_dest = dump_dir / rel
            local_dest.mkdir(parents=True, exist_ok=True)
            table_name = rel.name

            prog.update(table_name, i, bytes_done, _speed(bytes_done, t_start))

            ok = _cp_with_progress(
                copy_fn=lambda rd=remote_dir, ld=local_dest: kube_cp_from_pod(pod, rd, ld, namespace, quiet=True),
                prog=prog,
                poll_fn=lambda ld=local_dest: _poll_local_size(ld),
                table_name=table_name,
                table_idx=i,
                bytes_before=bytes_done,
                table_size=tsize,
                t_start=t_start,
            )

            if not ok:
                console.print(f"\n[error]✗ failed: {table_name}[/]")
                return

            bytes_done += tsize
            prog.update(table_name, i + 1, bytes_done, _speed(bytes_done, t_start))

    console.print(
        f"\n[success]✅ {len(selected_tables)} table(s) "
        f"({humanize.naturalsize(total_bytes, binary=True)}) — "
        f"avg {_speed(total_bytes, t_start)}[/]"
    )


def _k8s_full_restore(cfg: dict, namespace: str):
    """full automated restore: transfer → truncate → drop indexes/PKs → COPY → rebuild."""
    settings = get_settings(cfg)
    dump_dir = Path(settings.dump_dir)

    if not dump_dir.exists():
        console.print(f"[error]dump dir not found: {dump_dir}[/]")
        return

    # ── step 1: select tables ────────────────────────────────────────────
    console.print(Panel("[bold]Step 1/7 — Select tables[/]", expand=False))
    selected = _k8s_select_dump_tables(dump_dir)
    if not selected:
        return

    table_names = [m["table"] for _, m in selected]
    table_names_sql = ", ".join(f"'{t}'" for t in table_names)
    table_names_quoted = ", ".join(f'"{t}"' for t in table_names)

    # ── step 2: select pod + db url ──────────────────────────────────────
    console.print(Panel("[bold]Step 2/7 — Select pod & database[/]", expand=False))
    pod = _k8s_select_pod(namespace)
    if not pod:
        return

    db_url_env = inquirer.text(
        message="DB URL env var name on pod:",
        default="INDEX_DB_URL",
    ).execute()
    _rearm()

    # verify connectivity
    with console.status("[info]testing DB connection…[/]"):
        ok, out = kube_psql(pod, db_url_env, "SELECT 1", namespace, flags="-t -A")
    if not ok:
        console.print(f"[error]cannot connect to DB via ${db_url_env}: {out}[/]")
        return
    console.print(f"[success]✓ connected via ${db_url_env}[/]")

    remote_path = inquirer.text(message="Remote path on pod:", default="/tmp/dbtool_dumps").execute()
    _rearm()

    # ── step 3: transfer dumps to pod ────────────────────────────────────
    console.print(Panel("[bold]Step 3/7 — Transfer dumps to pod[/]", expand=False))
    table_sizes = [(t_dir, m, _dir_size(t_dir)) for t_dir, m in selected]
    total_bytes = sum(s for _, _, s in table_sizes)

    console.print(
        f"[info]{len(selected)} table(s) "
        f"({humanize.naturalsize(total_bytes, binary=True)}) → {pod}[/]\n"
    )

    reset_interrupt()
    bytes_done = 0
    t_start = time.monotonic()

    with TransferProgress(len(selected), total_bytes) as prog:
        for i, (t_dir, m, tsize) in enumerate(table_sizes):
            if interrupted:
                console.print(f"\n[warning]⏸ stopped at table {i}/{len(selected)}[/]")
                return

            rel = t_dir.relative_to(dump_dir)
            remote_table = f"{remote_path}/{rel}"
            remote_parent = str(Path(remote_table).parent)

            prog.update(m["table"], i, bytes_done, _speed(bytes_done, t_start))

            if not kube_mkdir(pod, remote_parent, namespace):
                return

            ok = _cp_with_progress(
                copy_fn=lambda td=t_dir, rt=remote_table: kube_cp_to_pod(td, pod, rt, namespace, quiet=True),
                prog=prog,
                poll_fn=lambda rt=remote_table: _poll_remote_size(pod, rt, namespace),
                table_name=m["table"],
                table_idx=i,
                bytes_before=bytes_done,
                table_size=tsize,
                t_start=t_start,
            )

            if not ok:
                console.print(f"\n[error]✗ transfer failed: {m['table']}[/]")
                return

            bytes_done += tsize
            prog.update(m["table"], i + 1, bytes_done, _speed(bytes_done, t_start))

    console.print(f"[success]✓ all dumps transferred[/]\n")

    # ── step 4: save PK + index definitions ──────────────────────────────
    console.print(Panel("[bold]Step 4/7 — Save index & PK definitions[/]", expand=False))

    # save non-PK indexes
    ok, index_sql = kube_psql(pod, db_url_env, f"""
        SELECT 'CREATE INDEX IF NOT EXISTS "' || indexname || '" ON "' || schemaname || '"."' || tablename || '" ' ||
               regexp_replace(indexdef, '^CREATE INDEX [^ ]+ ', '') || ';'
        FROM pg_indexes
        WHERE schemaname = 'public'
        AND tablename IN ({table_names_sql})
        AND indexname NOT IN (
            SELECT conname FROM pg_constraint WHERE contype = 'p'
        )
    """, namespace)
    if not ok:
        console.print(f"[error]failed to read indexes: {index_sql}[/]")
        return

    # save PK definitions
    ok, pk_sql = kube_psql(pod, db_url_env, f"""
        SELECT 'ALTER TABLE "' || r.relname || '" ADD CONSTRAINT "' || c.conname || '" ' || pg_get_constraintdef(c.oid) || ';'
        FROM pg_constraint c
        JOIN pg_class r ON c.conrelid = r.oid
        WHERE c.contype = 'p'
        AND r.relname IN ({table_names_sql})
    """, namespace)
    if not ok:
        console.print(f"[error]failed to read PKs: {pk_sql}[/]")
        return

    # write to pod
    rebuild_sql = ""
    if pk_sql.strip():
        rebuild_sql += "-- primary keys\n" + pk_sql.strip() + "\n\n"
    if index_sql.strip():
        rebuild_sql += "-- indexes\n" + index_sql.strip() + "\n"

    if rebuild_sql.strip():
        if not kube_write_file(pod, "/tmp/rebuild_indexes.sql", rebuild_sql, namespace):
            console.print("[error]failed to write rebuild script[/]")
            return
        idx_count = rebuild_sql.count(";")
        console.print(f"[success]✓ saved {idx_count} index/PK definitions to /tmp/rebuild_indexes.sql[/]")
    else:
        console.print("[dim]no indexes or PKs to save[/]")

    # show what we're about to drop
    if index_sql.strip():
        console.print(f"[dim]indexes to drop:[/]")
        for line in index_sql.strip().splitlines():
            # extract index name from the CREATE INDEX statement
            if "CREATE INDEX" in line:
                parts = line.split('"')
                if len(parts) >= 2:
                    console.print(f"  [dim]• {parts[1]}[/]")
    if pk_sql.strip():
        console.print(f"[dim]PKs to drop:[/]")
        for line in pk_sql.strip().splitlines():
            parts = line.split('"')
            if len(parts) >= 4:
                console.print(f"  [dim]• {parts[3]} on {parts[1]}[/]")

    # ── confirm ──────────────────────────────────────────────────────────
    console.print()
    confirm = _select("This will TRUNCATE all selected tables, drop all indexes/PKs, restore from dumps, then rebuild. Continue?", [
        {"name": "Yes — full restore", "value": True},
        {"name": "No — abort", "value": False},
    ])
    if not confirm:
        console.print("[dim]aborted[/]")
        return

    # ── step 5: truncate + drop indexes/PKs ──────────────────────────────
    console.print(Panel("[bold]Step 5/7 — Truncate & drop indexes[/]", expand=False))

    # kill any existing connections that might hold locks
    with console.status("[info]clearing locks…[/]"):
        kube_psql(pod, db_url_env,
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid != pg_backend_pid() AND state != 'idle'",
            namespace)

    # truncate all at once
    with console.status(f"[info]truncating {len(table_names)} tables…[/]"):
        ok, out = kube_psql(pod, db_url_env, f"TRUNCATE TABLE {table_names_quoted}", namespace)
    if not ok:
        console.print(f"[error]truncate failed: {out}[/]")
        return
    console.print(f"[success]✓ truncated {len(table_names)} tables[/]")

    # drop non-PK indexes
    with console.status("[info]dropping indexes…[/]"):
        ok, drop_idx_sql = kube_psql(pod, db_url_env, f"""
            SELECT 'DROP INDEX IF EXISTS "' || schemaname || '"."' || indexname || '";'
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename IN ({table_names_sql})
            AND indexname NOT IN (
                SELECT conname FROM pg_constraint WHERE contype = 'p'
            )
        """, namespace)
        if ok and drop_idx_sql.strip():
            ok, out = kube_psql_pipe(pod, db_url_env, drop_idx_sql, namespace)
            if not ok:
                console.print(f"[warning]some indexes may not have dropped: {out}[/]")
    console.print("[success]✓ indexes dropped[/]")

    # drop PKs
    with console.status("[info]dropping primary keys…[/]"):
        ok, drop_pk_sql = kube_psql(pod, db_url_env, f"""
            SELECT 'ALTER TABLE "' || r.relname || '" DROP CONSTRAINT "' || c.conname || '";'
            FROM pg_constraint c
            JOIN pg_class r ON c.conrelid = r.oid
            WHERE c.contype = 'p'
            AND r.relname IN ({table_names_sql})
        """, namespace)
        if ok and drop_pk_sql.strip():
            ok, out = kube_psql_pipe(pod, db_url_env, drop_pk_sql, namespace)
            if not ok:
                console.print(f"[warning]some PKs may not have dropped: {out}[/]")
    console.print("[success]✓ primary keys dropped[/]")

    # verify clean
    ok, remaining = kube_psql(pod, db_url_env, f"""
        SELECT count(*) FROM pg_indexes
        WHERE schemaname = 'public'
        AND tablename IN ({table_names_sql})
    """, namespace)
    if ok and remaining.strip() != "0":
        console.print(f"[warning]⚠ {remaining.strip()} indexes still remain — check manually[/]")

    # ── step 6: start restore + poll progress ────────────────────────────
    console.print(Panel("[bold]Step 6/7 — Restore data (COPY)[/]", expand=False))

    # figure out dump path on pod
    first_t_dir, first_m = selected[0]
    rel = first_t_dir.relative_to(dump_dir)
    # e.g. DB-B_dorado/index/messages → parent.parent = DB-B_dorado/index
    dump_base = f"{remote_path}/{rel.parent}"

    restore_script = f"""#!/bin/bash
for dir in {dump_base}/*/; do
  table=$(basename "$dir")
  echo "$(date) starting $table" >> /tmp/restore.log

  # detect dump format from files present
  csv_gz_count=$(find "$dir" -maxdepth 1 -name "*.csv.gz" | wc -l)
  csv_count=$(find "$dir" -maxdepth 1 -name "*.csv" | wc -l)
  sql_gz_count=$(find "$dir" -maxdepth 1 -name "*.sql.gz" | wc -l)
  sql_count=$(find "$dir" -maxdepth 1 -name "*.sql" ! -name "schema.sql" | wc -l)

  if [ "$csv_gz_count" -gt 0 ]; then
    # COPY format, gzipped — concat all chunks into one COPY stream
    (find "$dir" -maxdepth 1 -name "*.csv.gz" | sort | while read f; do gunzip -c "$f"; done) \\
      | psql ${db_url_env} -c "COPY \\"$table\\" FROM STDIN" -q 2>> /tmp/restore.log
  elif [ "$csv_count" -gt 0 ]; then
    (find "$dir" -maxdepth 1 -name "*.csv" | sort | while read f; do cat "$f"; done) \\
      | psql ${db_url_env} -c "COPY \\"$table\\" FROM STDIN" -q 2>> /tmp/restore.log
  elif [ "$sql_gz_count" -gt 0 ]; then
    find "$dir" -maxdepth 1 -name "*.sql.gz" | sort | while read f; do
      gunzip -c "$f" | psql ${db_url_env} -q 2>> /tmp/restore.log
    done
  elif [ "$sql_count" -gt 0 ]; then
    find "$dir" -maxdepth 1 -name "*.sql" ! -name "schema.sql" | sort | while read f; do
      psql ${db_url_env} -q -f "$f" 2>> /tmp/restore.log
    done
  else
    echo "$(date) SKIP $table — no data files found" >> /tmp/restore.log
    continue
  fi

  echo "$(date) done $table" >> /tmp/restore.log
done
echo "$(date) ALL_TABLES_DONE" >> /tmp/restore.log
"""

    if not kube_write_file(pod, "/tmp/restore.sh", restore_script, namespace):
        console.print("[error]failed to write restore script[/]")
        return

    # clear log and start
    kube_exec(pod, ["bash", "-c", "> /tmp/restore.log"], namespace)
    kube_exec(pod, ["bash", "-c", "nohup bash /tmp/restore.sh > /tmp/restore.out 2>&1 &"], namespace)
    console.print("[info]restore started on pod…[/]\n")

    reset_interrupt()

    # poll progress
    total_rows = sum(m["total_rows"] for _, m in selected)
    tables_total = len(selected)
    t_start = time.monotonic()

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.fields[current]}"),
        BarColumn(bar_width=40),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TextColumn("{task.fields[status]}"),
        TextColumn("•"),
        TextColumn("{task.fields[speed]}"),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task(
            "Restoring", total=total_rows, completed=0,
            current="waiting…", status="0/? tables", speed="",
        )

        prev_tuples = 0
        prev_time = time.monotonic()

        while True:
            time.sleep(5)

            # check pg_stat_progress_copy
            ok, copy_out = kube_psql(pod, db_url_env,
                "SELECT relid::regclass, tuples_processed, bytes_processed FROM pg_stat_progress_copy",
                namespace, flags="-t -A")

            # check log for completed tables
            log_content = kube_read_file(pod, "/tmp/restore.log", namespace) or ""
            tables_done = log_content.count(" done ")
            all_done = "ALL_TABLES_DONE" in log_content

            current_table = "finishing…"
            tuples = 0
            bytes_done = 0

            if ok and copy_out.strip():
                # format: table|tuples|bytes
                parts = copy_out.strip().split("|")
                if len(parts) >= 3:
                    current_table = parts[0]
                    tuples = int(parts[1]) if parts[1].isdigit() else 0
                    bytes_done = int(parts[2]) if parts[2].isdigit() else 0

            # estimate total tuples done across completed + in-progress
            # use log to count finished tables and their rows
            completed_rows = 0
            for _, m in selected[:tables_done]:
                completed_rows += m["total_rows"]
            total_tuples_now = completed_rows + tuples

            # speed calc (rows/sec over last interval)
            now = time.monotonic()
            interval = now - prev_time
            if interval > 0 and total_tuples_now > prev_tuples:
                rps = int((total_tuples_now - prev_tuples) / interval)
                speed_str = f"{humanize.intcomma(rps)} rows/s"
            else:
                speed_str = ""
            prev_tuples = total_tuples_now
            prev_time = now

            elapsed = now - t_start
            if bytes_done > 0 and elapsed > 1:
                bps = humanize.naturalsize(bytes_done / elapsed, binary=True)
                if speed_str:
                    speed_str += f" • {bps}/s (current)"

            progress.update(
                task,
                completed=min(total_tuples_now, total_rows),
                current=current_table,
                status=f"{tables_done}/{tables_total} tables",
                speed=speed_str,
            )

            if all_done:
                progress.update(task, completed=total_rows, current="done", status=f"{tables_total}/{tables_total} tables")
                break

            if interrupted:
                console.print("\n[warning]stopped polling — restore continues on pod[/]")
                console.print("[dim]reconnect and check: cat /tmp/restore.log[/]")
                return

    elapsed = time.monotonic() - t_start
    mins = int(elapsed) // 60
    secs = int(elapsed) % 60
    console.print(f"\n[success]✓ all tables restored in {mins}m{secs}s[/]\n")

    # show final log
    log_content = kube_read_file(pod, "/tmp/restore.log", namespace) or ""
    for line in log_content.strip().splitlines():
        console.print(f"  [dim]{line}[/]")
    console.print()

    # ── step 7: rebuild indexes + PKs ────────────────────────────────────
    console.print(Panel("[bold]Step 7/7 — Rebuild indexes & primary keys[/]", expand=False))

    if not rebuild_sql.strip():
        console.print("[dim]nothing to rebuild[/]")
    else:
        rebuild_lines = [l for l in rebuild_sql.strip().splitlines() if l.strip() and not l.startswith("--")]
        for i, line in enumerate(rebuild_lines):
            label = line.strip()[:80]
            with console.status(f"[info]({i+1}/{len(rebuild_lines)}) {label}[/]"):
                ok, out = kube_psql(pod, db_url_env, line.rstrip(";"), namespace, flags="-q", timeout=7200)
                if not ok:
                    console.print(f"  [error]✗ {label}: {out}[/]")
                else:
                    console.print(f"  [success]✓ {label}[/]")

    # verify indexes rebuilt
    ok, idx_count = kube_psql(pod, db_url_env, f"""
        SELECT count(*) FROM pg_indexes
        WHERE schemaname = 'public' AND tablename IN ({table_names_sql})
    """, namespace)
    if ok:
        console.print(f"\n[success]✓ {idx_count.strip()} indexes now on tables[/]")

    # ── cleanup ──────────────────────────────────────────────────────────
    cleanup = _select("Clean up remote dumps?", [
        {"name": "Yes — delete dumps from pod", "value": True},
        {"name": "No — keep them", "value": False},
    ])
    if cleanup:
        with console.status("[info]deleting remote dumps…[/]"):
            kube_exec(pod, ["rm", "-rf", remote_path], namespace)
            kube_exec(pod, ["rm", "-f", "/tmp/restore.sh", "/tmp/restore.log",
                            "/tmp/restore.out", "/tmp/rebuild_indexes.sql"], namespace)
        console.print("[success]✓ cleaned up[/]")

    console.print(f"\n[success]🎉 Full restore complete![/]")


def _k8s_pod_restore(cfg: dict, namespace: str):
    """restore dumps already present on a pod — no file transfer, all via kubectl exec."""
    console.print(Panel("[bold]☸ POD-LOCAL RESTORE[/]", expand=False))

    pod = _k8s_select_pod(namespace)
    if not pod:
        return

    remote_path = inquirer.text(message="Remote path on pod:", default="/tmp/dbtool_dumps").execute()
    _rearm()

    # ── scan for dumps on pod ────────────────────────────────────────────
    with console.status("[info]scanning remote dumps…[/]"):
        remote_tables = _k8s_list_remote_dumps(pod, remote_path, namespace)

    if not remote_tables:
        console.print(f"[warning]no table dumps found at {pod}:{remote_path}[/]")
        return

    # read manifests to get table metadata
    table_info = []  # [(remote_dir, size, manifest_dict)]
    for remote_dir, sz in remote_tables:
        manifest_content = kube_read_file(pod, f"{remote_dir}/manifest.json", namespace)
        if manifest_content:
            try:
                m = json.loads(manifest_content)
                table_info.append((remote_dir, sz, m))
            except json.JSONDecodeError:
                console.print(f"  [warning]bad manifest in {remote_dir}[/]")

    if not table_info:
        console.print("[warning]no valid manifests found[/]")
        return

    # show what's available
    info = Table(title=f"Dumps on {pod}", box=box.ROUNDED)
    info.add_column("Table", style="cyan")
    info.add_column("Rows", justify="right", style="green")
    info.add_column("Chunks", justify="right")
    info.add_column("Mode", style="dim")
    info.add_column("Size", justify="right", style="blue")
    for remote_dir, sz, m in table_info:
        info.add_row(
            m["table"], humanize.intcomma(m["total_rows"]),
            str(m["chunks_total"]), m.get("dump_mode", "copy"),
            humanize.naturalsize(sz, binary=True),
        )
    console.print(info)

    # select tables
    mw = max(len(m["table"]) for _, _, m in table_info)
    choices = [{"name": "★ All tables", "value": "__all__"}, Separator()]
    for remote_dir, sz, m in table_info:
        choices.append({
            "name": (f"{_pad(m['table'], mw)}  "
                     f"{humanize.intcomma(m['total_rows']):>12} rows  "
                     f"{humanize.naturalsize(sz, binary=True):>10}"),
            "value": remote_dir,
        })

    selected = _checkbox("Select tables to restore:", choices)
    if not selected:
        return
    if "__all__" in selected:
        selected_tables = table_info
    else:
        selected_tables = [(d, sz, m) for d, sz, m in table_info if d in selected]

    if not selected_tables:
        return

    # ── DB connection ────────────────────────────────────────────────────
    db_url_env = inquirer.text(
        message="DB URL env var name on pod:",
        default="INDEX_DB_URL",
    ).execute()
    _rearm()

    with console.status("[info]testing DB connection…[/]"):
        ok, out = kube_psql(pod, db_url_env, "SELECT 1", namespace, flags="-t -A")
    if not ok:
        console.print(f"[error]cannot connect to DB via ${db_url_env}: {out}[/]")
        return
    console.print(f"[success]✓ connected via ${db_url_env}[/]")

    table_names = [m["table"] for _, _, m in selected_tables]
    table_names_sql = ", ".join(f"'{t}'" for t in table_names)
    table_names_quoted = ", ".join(f'"{t}"' for t in table_names)

    # ── save PK + index definitions ──────────────────────────────────────
    console.print(Panel("[bold]Step 1/4 — Save index & PK definitions[/]", expand=False))

    ok, index_sql = kube_psql(pod, db_url_env, f"""
        SELECT 'CREATE INDEX IF NOT EXISTS "' || indexname || '" ON "' || schemaname || '"."' || tablename || '" ' ||
               regexp_replace(indexdef, '^CREATE INDEX [^ ]+ ', '') || ';'
        FROM pg_indexes
        WHERE schemaname = 'public'
        AND tablename IN ({table_names_sql})
        AND indexname NOT IN (
            SELECT conname FROM pg_constraint WHERE contype = 'p'
        )
    """, namespace)
    if not ok:
        console.print(f"[error]failed to read indexes: {index_sql}[/]")
        return

    ok, pk_sql = kube_psql(pod, db_url_env, f"""
        SELECT 'ALTER TABLE "' || r.relname || '" ADD CONSTRAINT "' || c.conname || '" ' || pg_get_constraintdef(c.oid) || ';'
        FROM pg_constraint c
        JOIN pg_class r ON c.conrelid = r.oid
        WHERE c.contype = 'p'
        AND r.relname IN ({table_names_sql})
    """, namespace)
    if not ok:
        console.print(f"[error]failed to read PKs: {pk_sql}[/]")
        return

    rebuild_sql = ""
    if pk_sql.strip():
        rebuild_sql += "-- primary keys\n" + pk_sql.strip() + "\n\n"
    if index_sql.strip():
        rebuild_sql += "-- indexes\n" + index_sql.strip() + "\n"

    if rebuild_sql.strip():
        if not kube_write_file(pod, "/tmp/rebuild_indexes.sql", rebuild_sql, namespace):
            console.print("[error]failed to write rebuild script[/]")
            return
        idx_count = rebuild_sql.count(";")
        console.print(f"[success]✓ saved {idx_count} index/PK definitions[/]")
    else:
        console.print("[dim]no indexes or PKs to save[/]")

    # ── confirm ──────────────────────────────────────────────────────────
    console.print()
    confirm = _select(
        "This will TRUNCATE selected tables, drop indexes/PKs, restore via COPY, then rebuild. Continue?",
        [
            {"name": "Yes — restore", "value": True},
            {"name": "No — abort", "value": False},
        ],
    )
    if not confirm:
        console.print("[dim]aborted[/]")
        return

    # ── truncate + drop indexes/PKs ──────────────────────────────────────
    console.print(Panel("[bold]Step 2/4 — Truncate & drop indexes[/]", expand=False))

    with console.status("[info]clearing locks…[/]"):
        kube_psql(pod, db_url_env,
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid != pg_backend_pid() AND state != 'idle'",
            namespace)

    with console.status(f"[info]truncating {len(table_names)} tables…[/]"):
        ok, out = kube_psql(pod, db_url_env, f"TRUNCATE TABLE {table_names_quoted}", namespace)
    if not ok:
        console.print(f"[error]truncate failed: {out}[/]")
        return
    console.print(f"[success]✓ truncated {len(table_names)} tables[/]")

    with console.status("[info]dropping indexes…[/]"):
        ok, drop_idx_sql = kube_psql(pod, db_url_env, f"""
            SELECT 'DROP INDEX IF EXISTS "' || schemaname || '"."' || indexname || '";'
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename IN ({table_names_sql})
            AND indexname NOT IN (
                SELECT conname FROM pg_constraint WHERE contype = 'p'
            )
        """, namespace)
        if ok and drop_idx_sql.strip():
            ok, out = kube_psql_pipe(pod, db_url_env, drop_idx_sql, namespace)
            if not ok:
                console.print(f"[warning]some indexes may not have dropped: {out}[/]")
    console.print("[success]✓ indexes dropped[/]")

    with console.status("[info]dropping primary keys…[/]"):
        ok, drop_pk_sql = kube_psql(pod, db_url_env, f"""
            SELECT 'ALTER TABLE "' || r.relname || '" DROP CONSTRAINT "' || c.conname || '";'
            FROM pg_constraint c
            JOIN pg_class r ON c.conrelid = r.oid
            WHERE c.contype = 'p'
            AND r.relname IN ({table_names_sql})
        """, namespace)
        if ok and drop_pk_sql.strip():
            ok, out = kube_psql_pipe(pod, db_url_env, drop_pk_sql, namespace)
            if not ok:
                console.print(f"[warning]some PKs may not have dropped: {out}[/]")
    console.print("[success]✓ primary keys dropped[/]")

    # ── restore via exec ─────────────────────────────────────────────────
    console.print(Panel("[bold]Step 3/4 — Restore data (COPY via exec)[/]", expand=False))

    # build restore script that iterates selected table dirs
    table_dirs_bash = " ".join(f'"{d}"' for d, _, _ in selected_tables)

    restore_script = f"""#!/bin/bash
for dir in {table_dirs_bash}; do
  table=$(basename "$dir")
  echo "$(date) starting $table" >> /tmp/restore.log

  csv_gz_count=$(find "$dir" -maxdepth 1 -name "*.csv.gz" | wc -l)
  csv_count=$(find "$dir" -maxdepth 1 -name "*.csv" | wc -l)
  sql_gz_count=$(find "$dir" -maxdepth 1 -name "*.sql.gz" | wc -l)
  sql_count=$(find "$dir" -maxdepth 1 -name "*.sql" ! -name "schema.sql" | wc -l)

  if [ "$csv_gz_count" -gt 0 ]; then
    (find "$dir" -maxdepth 1 -name "*.csv.gz" | sort | while read f; do gunzip -c "$f"; done) \\
      | psql ${db_url_env} -c "COPY \\"$table\\" FROM STDIN" -q 2>> /tmp/restore.log
  elif [ "$csv_count" -gt 0 ]; then
    (find "$dir" -maxdepth 1 -name "*.csv" | sort | while read f; do cat "$f"; done) \\
      | psql ${db_url_env} -c "COPY \\"$table\\" FROM STDIN" -q 2>> /tmp/restore.log
  elif [ "$sql_gz_count" -gt 0 ]; then
    find "$dir" -maxdepth 1 -name "*.sql.gz" | sort | while read f; do
      gunzip -c "$f" | psql ${db_url_env} -q 2>> /tmp/restore.log
    done
  elif [ "$sql_count" -gt 0 ]; then
    find "$dir" -maxdepth 1 -name "*.sql" ! -name "schema.sql" | sort | while read f; do
      psql ${db_url_env} -q -f "$f" 2>> /tmp/restore.log
    done
  else
    echo "$(date) SKIP $table — no data files found" >> /tmp/restore.log
    continue
  fi

  echo "$(date) done $table" >> /tmp/restore.log
done
echo "$(date) ALL_TABLES_DONE" >> /tmp/restore.log
"""

    if not kube_write_file(pod, "/tmp/restore.sh", restore_script, namespace):
        console.print("[error]failed to write restore script[/]")
        return

    kube_exec(pod, ["bash", "-c", "> /tmp/restore.log"], namespace)
    kube_exec(pod, ["bash", "-c", "nohup bash /tmp/restore.sh > /tmp/restore.out 2>&1 &"], namespace)
    console.print("[info]restore started on pod…[/]\n")

    reset_interrupt()

    # poll progress
    total_rows = sum(m["total_rows"] for _, _, m in selected_tables)
    tables_total = len(selected_tables)
    t_start = time.monotonic()

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.fields[current]}"),
        BarColumn(bar_width=40),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TextColumn("{task.fields[status]}"),
        TextColumn("•"),
        TextColumn("{task.fields[speed]}"),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task(
            "Restoring", total=total_rows, completed=0,
            current="waiting…", status="0/? tables", speed="",
        )

        prev_tuples = 0
        prev_time = time.monotonic()

        while True:
            time.sleep(5)

            ok, copy_out = kube_psql(pod, db_url_env,
                "SELECT relid::regclass, tuples_processed, bytes_processed FROM pg_stat_progress_copy",
                namespace, flags="-t -A")

            log_content = kube_read_file(pod, "/tmp/restore.log", namespace) or ""
            tables_done = log_content.count(" done ")
            all_done = "ALL_TABLES_DONE" in log_content

            current_table = "finishing…"
            tuples = 0
            bytes_done = 0

            if ok and copy_out.strip():
                parts = copy_out.strip().split("|")
                if len(parts) >= 3:
                    current_table = parts[0]
                    tuples = int(parts[1]) if parts[1].isdigit() else 0
                    bytes_done = int(parts[2]) if parts[2].isdigit() else 0

            completed_rows = 0
            for _, _, m in selected_tables[:tables_done]:
                completed_rows += m["total_rows"]
            total_tuples_now = completed_rows + tuples

            now = time.monotonic()
            interval = now - prev_time
            if interval > 0 and total_tuples_now > prev_tuples:
                rps = int((total_tuples_now - prev_tuples) / interval)
                speed_str = f"{humanize.intcomma(rps)} rows/s"
            else:
                speed_str = ""
            prev_tuples = total_tuples_now
            prev_time = now

            progress.update(
                task,
                completed=min(total_tuples_now, total_rows),
                current=current_table,
                status=f"{tables_done}/{tables_total} tables",
                speed=speed_str,
            )

            if all_done:
                progress.update(task, completed=total_rows, current="done", status=f"{tables_total}/{tables_total} tables")
                break

            if interrupted:
                console.print("\n[warning]stopped polling — restore continues on pod[/]")
                console.print("[dim]reconnect and check: cat /tmp/restore.log[/]")
                return

    elapsed = time.monotonic() - t_start
    mins = int(elapsed) // 60
    secs = int(elapsed) % 60
    console.print(f"\n[success]✓ all tables restored in {mins}m{secs}s[/]\n")

    log_content = kube_read_file(pod, "/tmp/restore.log", namespace) or ""
    for line in log_content.strip().splitlines():
        console.print(f"  [dim]{line}[/]")
    console.print()

    # ── rebuild indexes + PKs ────────────────────────────────────────────
    console.print(Panel("[bold]Step 4/4 — Rebuild indexes & primary keys[/]", expand=False))

    if not rebuild_sql.strip():
        console.print("[dim]nothing to rebuild[/]")
    else:
        rebuild_lines = [l for l in rebuild_sql.strip().splitlines() if l.strip() and not l.startswith("--")]
        for i, line in enumerate(rebuild_lines):
            label = line.strip()[:80]
            with console.status(f"[info]({i+1}/{len(rebuild_lines)}) {label}[/]"):
                ok, out = kube_psql(pod, db_url_env, line.rstrip(";"), namespace, flags="-q", timeout=7200)
                if not ok:
                    console.print(f"  [error]✗ {label}: {out}[/]")
                else:
                    console.print(f"  [success]✓ {label}[/]")

    ok, idx_count = kube_psql(pod, db_url_env, f"""
        SELECT count(*) FROM pg_indexes
        WHERE schemaname = 'public' AND tablename IN ({table_names_sql})
    """, namespace)
    if ok:
        console.print(f"\n[success]✓ {idx_count.strip()} indexes now on tables[/]")

    # ── cleanup ──────────────────────────────────────────────────────────
    cleanup = _select("Clean up remote dumps?", [
        {"name": "Yes — delete dumps from pod", "value": True},
        {"name": "No — keep them", "value": False},
    ])
    if cleanup:
        dirs_to_clean = [d for d, _, _ in selected_tables]
        with console.status("[info]deleting remote dumps…[/]"):
            for d in dirs_to_clean:
                kube_exec(pod, ["rm", "-rf", d], namespace)
            kube_exec(pod, ["rm", "-f", "/tmp/restore.sh", "/tmp/restore.log",
                            "/tmp/restore.out", "/tmp/rebuild_indexes.sql"], namespace)
        console.print("[success]✓ cleaned up[/]")

    console.print(f"\n[success]🎉 Pod-local restore complete![/]")


# ── main ─────────────────────────────────────────────────────────────────────

FLOWS = {
    "browse": flow_browse,
    "dump": flow_dump,
    "restore": flow_restore,
    "inspect": flow_inspect,
    "connections": flow_connections,
    "settings": flow_settings,
    "k8s": flow_k8s,
}


def main():
    parser = argparse.ArgumentParser(
        description="dbtool — chunked PostgreSQL dump & restore",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="  dbtool              interactive mode\n"
               "  dbtool browse       explore tables in a live db\n"
               "  dbtool dump         dump tables\n"
               "  dbtool restore      restore from dump\n"
               "  dbtool inspect      inspect dump files\n"
               "  dbtool connections  manage saved connections\n"
               "  dbtool settings     configure dump/restore settings\n"
               "  dbtool k8s          kubectl cp transfers",
    )
    parser.add_argument("command", nargs="?", choices=list(FLOWS.keys()))
    args = parser.parse_args()

    console.print()
    console.print(Panel(
        "[bold magenta]dbtool[/] — chunked PostgreSQL dump & restore\n"
        "[dim]partitioned files • resumable • transaction-safe[/]",
        box=box.DOUBLE, expand=False,
    ))

    from .config import PROJECT_DIR
    console.print(f"[dim]config: {PROJECT_DIR}[/]")

    cfg = load_config()

    if args.command:
        try:
            FLOWS[args.command](cfg)
        except KeyboardInterrupt:
            console.print("\n[dim]see ya, bud.[/]")
        return

    while True:
        try:
            action = _select("What would you like to do?", [
                {"name": "🔬 Browse tables", "value": "browse"},
                {"name": "📦 Dump tables", "value": "dump"},
                {"name": "📥 Restore tables", "value": "restore"},
                {"name": "🔍 Inspect dumps", "value": "inspect"},
                {"name": "☸  K8s transfer", "value": "k8s"},
                Separator(),
                {"name": "🔌 Connections", "value": "connections"},
                {"name": "⚙  Settings", "value": "settings"},
                Separator(),
                {"name": "👋 Exit", "value": "exit"},
            ])
        except KeyboardInterrupt:
            console.print("\n[dim]see ya, bud.[/]")
            break

        if action is None or action == "exit":
            console.print("[dim]see ya, bud.[/]")
            break

        try:
            FLOWS[action](cfg)
        except KeyboardInterrupt:
            console.print("\n[warning]interrupted[/]")
        except Exception as e:
            console.print(f"\n[error]error: {e}[/]")
