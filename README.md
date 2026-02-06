# dbtool

Chunked PostgreSQL dump & restore. Built for moving large tables over dodgy connections (k8s port-forwards, tunnels, etc) without losing progress when things inevitably drop.

## install

```bash
poetry install
```

## usage

```bash
dbtool              # interactive menu
dbtool browse       # poke around a live db
dbtool dump         # dump tables
dbtool restore      # restore from dump
dbtool inspect      # check dump status
dbtool settings     # configure dump/restore behaviour
dbtool connections  # manage db connections
dbtool k8s          # kubectl cp dumps to/from pods
```

Or just run `dbtool` and use the menu. Arrow keys to navigate, enter to select, left arrow to go back.

## how it works

**dump** splits tables into chunks (default 500k rows) using `COPY ... TO STDOUT` with pk ordering. Each chunk gets checkpointed to a manifest, so if it dies mid-dump you pick up where you left off.

**restore** loads each chunk in its own transaction with retry + backoff. Same deal — tracks progress per-chunk, fully resumable.

Two dump modes:
- `copy` — native COPY format, fast as it gets for pg-to-pg
- `insert` — generates self-contained `.sql` files with BEGIN/COMMIT, schema DDL, index management etc. Slower but portable and you can just `psql -f` them

## settings

Everything lives in `.dbtool/config.toml`. The settings menu lets you toggle stuff like:

- dump mode (copy vs insert)
- compression on/off + level
- chunk size
- schema DDL in dumps
- truncate/drop/recreate on restore
- disable indexes during restore (faster bulk loads)
- transaction wrapping
- retry count + backoff

## k8s

The k8s menu uses `kubectl cp` to move dump directories to/from pods. Supports `kubectx`/`kubens` if you have them (falls back to raw kubectl if not). Lets you pick context, namespace, and pod interactively.

## output structure

```
.dbtool/dumps/
└── index/
    ├── blocks/
    │   ├── manifest.json
    │   ├── schema.sql
    │   ├── blocks_chunk_000000.csv.gz
    │   └── ...
    └── transactions/
        └── ...
```

Add `.dbtool/` to your `.gitignore`.