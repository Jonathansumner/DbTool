import argparse
import json
import signal
from dataclasses import asdict, fields
from pathlib import Path

import humanize
from InquirerPy import inquirer
from InquirerPy.separator import Separator
from rich import box
from rich.panel import Panel
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
    list_pods, kube_cp_to_pod, kube_cp_from_pod,
)
from .ui import console, interrupted, reset_interrupt, _signal_handler


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

    available_dbs = []
    for db_dir in sorted(dump_dir.iterdir()):
        if db_dir.is_dir():
            table_names = [
                t.name for t in db_dir.iterdir()
                if t.is_dir() and (t / "manifest.json").exists()
            ]
            if table_names:
                available_dbs.append((db_dir.name, table_names))

    if not available_dbs:
        console.print(f"[warning]no dumps found in {dump_dir}[/]")
        return

    db_choices = [
        {"name": f"{_pad(name, 20)}  {len(t)} tables", "value": name}
        for name, t in available_dbs
    ]
    selected_db = _select("Select database to restore:", db_choices)
    if selected_db is None:
        return

    db_dump_dir = dump_dir / selected_db
    available_tables = []
    for t_dir in sorted(db_dump_dir.iterdir()):
        if t_dir.is_dir() and (t_dir / "manifest.json").exists():
            m = json.loads((t_dir / "manifest.json").read_text())
            available_tables.append((t_dir, m))

    info = Table(title=f"Dumps — {selected_db}", box=box.ROUNDED)
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

    for db_dir in sorted(dump_dir.iterdir()):
        if not db_dir.is_dir():
            continue
        console.print(f"\n[header]database: {db_dir.name}[/]")

        tbl = Table(box=box.ROUNDED)
        tbl.add_column("Table", style="cyan")
        tbl.add_column("Rows", justify="right", style="green")
        tbl.add_column("Chunks", justify="right")
        tbl.add_column("Mode", style="dim")
        tbl.add_column("Schema", justify="center")
        tbl.add_column("Size", justify="right", style="blue")
        tbl.add_column("Status", style="yellow")

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
        console.print(tbl)


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


def _k8s_copy_to_pod(cfg: dict, namespace: str):
    settings = get_settings(cfg)
    dump_dir = Path(settings.dump_dir)

    if not dump_dir.exists():
        console.print(f"[error]dump dir not found: {dump_dir}[/]")
        return

    pod = _k8s_select_pod(namespace)
    if not pod:
        return

    remote_path = inquirer.text(message="Remote path on pod:", default="/tmp/dbtool_dumps").execute()
    _rearm()

    console.print(f"\n[info]copying {dump_dir} → {pod}:{remote_path}[/]")
    if kube_cp_to_pod(dump_dir, pod, remote_path, namespace):
        console.print("[success]✅ transfer complete[/]")
    else:
        console.print("[error]transfer failed[/]")


def _k8s_copy_from_pod(cfg: dict, namespace: str):
    settings = get_settings(cfg)
    dump_dir = Path(settings.dump_dir)

    pod = _k8s_select_pod(namespace)
    if not pod:
        return

    remote_path = inquirer.text(message="Remote path on pod:", default="/tmp/dbtool_dumps").execute()
    _rearm()

    console.print(f"\n[info]copying {pod}:{remote_path} → {dump_dir}[/]")
    dump_dir.mkdir(parents=True, exist_ok=True)
    if kube_cp_from_pod(pod, remote_path, dump_dir, namespace):
        console.print("[success]✅ transfer complete[/]")
    else:
        console.print("[error]transfer failed[/]")


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