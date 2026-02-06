import tomllib
from dataclasses import dataclass, field, asdict
from pathlib import Path

import tomli_w

# everything lives in the project directory
PROJECT_DIR = Path.cwd() / ".dbtool"
CONFIG_FILE = PROJECT_DIR / "config.toml"
DEFAULT_CHUNK_ROWS = 500_000
DEFAULT_DUMP_DIR = str(PROJECT_DIR / "dumps")


@dataclass
class DBConfig:
    name: str
    host: str
    port: int
    user: str
    password: str
    databases: list[str] = field(default_factory=lambda: ["index", "cache"])

    @property
    def display(self) -> str:
        return f"{self.name} → {self.user}@{self.host}:{self.port} [{', '.join(self.databases)}]"

    def dsn(self, dbname: str) -> str:
        return f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/{dbname}"


@dataclass
class DumpSettings:
    """all configurable dump/restore settings, persisted in [settings] section."""
    dump_dir: str = DEFAULT_DUMP_DIR
    chunk_rows: int = DEFAULT_CHUNK_ROWS
    compress: bool = True
    compress_level: int = 6
    dump_mode: str = "copy"
    dump_schema: bool = True
    insert_batch_size: int = 1000
    use_transactions: bool = True
    truncate_before_restore: bool = True
    drop_on_restore: bool = False
    recreate_schema: bool = False
    disable_indexes_on_restore: bool = False
    max_retries: int = 3
    retry_backoff: int = 2

    @staticmethod
    def descriptions() -> dict[str, str]:
        return {
            "dump_dir":         "Directory for dump output/input",
            "chunk_rows":       "Rows per dump/restore chunk",
            "compress":         "Gzip-compress chunk files",
            "compress_level":   "Gzip compression level (1=fast, 9=small)",
            "dump_mode":        "Dump format: copy (fast) or insert (portable SQL)",
            "dump_schema":      "Include CREATE TABLE + indexes DDL in dump",
            "insert_batch_size": "Rows per INSERT batch (only when mode=insert)",
            "use_transactions":  "Wrap each chunk in BEGIN/COMMIT",
            "truncate_before_restore": "TRUNCATE table before restoring",
            "drop_on_restore":  "DROP TABLE IF EXISTS before restore",
            "recreate_schema":  "Recreate table schema from dumped DDL",
            "disable_indexes_on_restore": "Drop indexes before restore, rebuild after",
            "max_retries":      "Max retry attempts for failed chunks",
            "retry_backoff":    "Base seconds for exponential backoff",
        }


def settings_from_dict(d: dict) -> DumpSettings:
    valid = {f.name for f in DumpSettings.__dataclass_fields__.values()}
    return DumpSettings(**{k: v for k, v in d.items() if k in valid})


def load_config() -> dict:
    PROJECT_DIR.mkdir(exist_ok=True)
    if CONFIG_FILE.exists():
        return tomllib.loads(CONFIG_FILE.read_text())
    return {"connections": [], "settings": asdict(DumpSettings())}


def save_config(cfg: dict):
    PROJECT_DIR.mkdir(exist_ok=True)
    clean = {k: v for k, v in cfg.items() if v is not None}
    CONFIG_FILE.write_bytes(tomli_w.dumps(clean).encode())


def get_connections(cfg: dict) -> list[DBConfig]:
    return [DBConfig(**c) for c in cfg.get("connections", [])]


def get_settings(cfg: dict) -> DumpSettings:
    return settings_from_dict(cfg.get("settings", {}))


def save_settings(cfg: dict, settings: DumpSettings):
    cfg["settings"] = asdict(settings)
    save_config(cfg)