# Apache Iceberg, in the tab

A real Iceberg table, written and queried entirely in the browser: no server, no proxy, no runtime
network access. [PyIceberg](https://py.iceberg.apache.org/) 0.11.1 creates the table and commits
every snapshot, [PyArrow](https://arrow.apache.org/) writes the Parquet, [DuckDB](https://duckdb.org/)
answers the SQL, and all three run as CPython extension modules inside
[Pyodide](https://pyodide.org/) in a Web Worker in the visitor's tab.

The page is at `/iceberg/`. Everything it emits at the root of the site is under `/_iceberg/`.

```
tab
├── page (src/main.ts)              buttons, SQL box, snapshot log, file inspector
└── Web Worker (src/worker.ts)
    └── Pyodide (CPython 3.14, wasm)
        ├── PyIceberg  SqlCatalog on SQLite ── writes metadata JSON, manifest lists, manifests
        ├── PyArrow    Parquet writer/reader
        ├── DuckDB     SQL over the scan PyIceberg planned
        └── /warehouse  the table, in the interpreter's in-memory filesystem
```

## What the demo does

Every button is one ordinary PyIceberg call; nothing is synthesised, and no metadata is written by
this repo's own code:

| button | what actually happens |
| --- | --- |
| Create table | `SqlCatalog.create_table("nyc.trips", …)` — format v2, `00000-….metadata.json` appears |
| Append a batch | 240 generated rows → `table.append(arrow_table)`: a Parquet file, a manifest, a manifest list, a new metadata JSON and an `append` snapshot |
| Upsert | `table.upsert(rows, join_cols=["trip_id"])` — a real MERGE, `overwrite` snapshot, the files holding the matched rows are rewritten |
| Delete | `table.delete(delete_filter="fare < 12")` — copy-on-write delete |
| Evolve schema | `update_schema().add_column("tip", …).rename_column("city", "pickup_city")` — metadata only, no data file is touched |
| Evolve partitioning | `update_spec().add_field("pickup_city", IdentityTransform())` — later files land in `pickup_city_part=…/`, older files keep spec 0 |
| Expire the oldest snapshot | `table.maintenance.expire_snapshots().by_id(…)` — the snapshot leaves the log and time travel to it fails afterwards |

Reading:

- **SQL** — `table.scan(row_filter, snapshot_id).to_duckdb("trips", connection)` is PyIceberg's own
  DuckDB bridge: Iceberg plans the scan and reads the Parquet, DuckDB runs the query.
- **Time travel** — the `as of` dropdown is the snapshot log; picking an entry passes
  `snapshot_id=` to the scan. Because a pre-rename snapshot really is a different schema, the page
  rewrites its default query to that snapshot's column names (and lists them under the SQL box) —
  Iceberg resolves the data by field id, but SQL still has to spell the name that snapshot knew.
- **Scan planning** — the pruning panel shows `len(scan.plan_files())` against the unfiltered scan,
  plus each surviving file's record count and partition tuple. A timestamp filter typically keeps
  4 of 7 files: that is manifest/metrics pruning, done by Iceberg, not by DuckDB.
- **File inspector** — the warehouse directory listing is clickable, and each kind is decoded with
  the upstream reader: `json` for `*.metadata.json`, `pyiceberg.manifest.read_manifest_list` for
  `snap-*.avro`, `ManifestFile.fetch_manifest_entry` for the manifests, and
  `pyarrow.parquet.ParquetFile` for the data files (including the `PARQUET:field_id` per column,
  which is what makes reads survive a rename).

The warehouse is a normal Iceberg table on a normal filesystem layout, so the same directory copied
out of the tab would open in Spark, Trino or `pyiceberg` on a laptop.

## Engine choice

The task suggested `pg_lake` or DuckDB. Evaluated in the order asked:

### 1. pg_lake inside PGlite — not achievable, and not close

[pg_lake](https://github.com/Snowflake-Labs/pg_lake) is not a single Postgres extension that could be
compiled to wasm and loaded into PGlite. From the repository (checked out at `main`):

- **It needs a second process.** `pgduck_server/Makefile` builds `PROGRAM = pgduck_server`, a
  standalone executable linked against `-lduckdb` that "implements the Postgres wire-protocol
  (locally)" and listens on a **unix domain socket** (`/tmp`, port 5332); the extensions talk to it
  as a client. A browser tab cannot `fork`/`exec` a second program and has no unix sockets, and
  PGlite is a single Postgres backend compiled to wasm with no postmaster to spawn anything.
- **It needs a background worker and `shared_preload_libraries`.**
  `pg_lake_engine/src/pgduck/cache_worker.c` calls `RegisterBackgroundWorker()` /
  `BackgroundWorkerInitializeConnection()`, and the docs require
  `shared_preload_libraries = 'pg_extension_base'`. Background workers are separate processes forked
  by the postmaster; PGlite has neither.
- **The build system is a different world.** `duckdb_pglake/` vendors DuckDB and
  `duckdb-postgres` and builds them with CMake + vcpkg (Azure SDK among the ports); DuckDB itself is
  C++17 with `std::thread`, `dlopen`-based extension loading and mmap'd file access. Emscripten can
  build DuckDB (that is what duckdb-wasm is), but it cannot be linked into PGlite's Postgres
  backend: PGlite loads extensions as Emscripten *side modules* built against its own wasm Postgres,
  and there is no toolchain in that project for a C++ dependency of DuckDB's size, let alone for the
  threads and process model pg_lake assumes.

Even ignoring the toolchain, the architecture is the blocker: pg_lake is Postgres *plus a DuckDB
server process*, and a tab cannot host the second process. Rejected.

### 2. duckdb-wasm with the `iceberg` extension — real, but read-only, and redundant here

The extension **does** exist for wasm builds. `HEAD` on the extension repository answers for both
wasm ABIs from DuckDB 1.3 onwards (checked while writing this):

```
https://extensions.duckdb.org/v1.4.1/wasm_eh/iceberg.duckdb_extension.wasm   200
https://extensions.duckdb.org/v1.4.1/wasm_mvp/iceberg.duckdb_extension.wasm  200
https://extensions.duckdb.org/v1.3.2/wasm_eh/iceberg.duckdb_extension.wasm   200
https://extensions.duckdb.org/v1.2.1/wasm_eh/iceberg.duckdb_extension.wasm   404   (pre-1.3: not built)
```

But it is a **reader**: `iceberg_scan`, `iceberg_metadata`, `iceberg_snapshots`. DuckDB cannot create
an Iceberg table, commit a snapshot, evolve a schema or expire snapshots without a catalog it can
write to, so a duckdb-wasm-only demo would have to ship a pre-baked table — which is exactly the
"no hand-rolled metadata, no faked snapshots" line this demo is not allowed to cross. Loading it
would also mean either a runtime fetch from `extensions.duckdb.org` (forbidden: nothing may hit the
network at runtime) or vendoring the extension plus a version-matched duckdb-wasm bundle and copying
the whole warehouse out of Pyodide's filesystem into DuckDB's, for reads the demo already has.
Rejected as ~30 MB of extra payload for a strictly smaller feature set.

### 3. PyIceberg on Pyodide + DuckDB — chosen

Pyodide 314.0.3 ships wasm wheels for `pyiceberg` 0.11.1, `pyarrow` 22.0.0, `duckdb` 1.5.1 and
`sqlalchemy` 2.0.48, so the *writer* side is real upstream code, and DuckDB is in the same
interpreter, which makes `scan().to_duckdb()` free (no copy across VMs). Two things had to be worked
around, both in `python/iceberg_wasm/shims.py`:

- **`pyarrow._s3fs` does not exist** in the Pyodide PyArrow build, and `pyiceberg.io.pyarrow`
  imports it unconditionally. The shim installs a stub module whose `S3FileSystem` raises
  `NotImplementedError` if anyone actually tries to use S3 — nothing in a browser can — so local
  `FileIO` imports and works.
- **Pyodide cannot start threads** (`RuntimeError: can't start new thread`), and PyIceberg schedules
  Parquet writes and manifest reads on a `ThreadPoolExecutor`. The shim replaces
  `pyiceberg.utils.concurrent.ExecutorFactory`'s executor with a synchronous one that runs the work
  on the calling thread. PyIceberg's own functions still do the work; only the scheduling changes.

## Known limitations (honest list)

- **Hidden partitioning by day is not available.** `DayTransform().pyarrow_transform()` in PyIceberg
  0.11.1 delegates to the `pyiceberg_core` Rust extension, which has no Emscripten wheel
  (`NotInstalledError: pyiceberg_core needs to be installed`). The demo therefore starts
  unpartitioned and evolves to `identity(pickup_city)`, which is a real partition spec written by
  real code — the directory layout, per-file partition tuples and spec-per-file behaviour are all
  genuine — but it is not the bucket/day transform showcase. Pruning is still demonstrated (timestamp
  filter against manifest metrics), it is just not *hidden-partition* pruning.
- **The warehouse does not survive a reload.** It lives in Pyodide's in-memory filesystem
  (`/warehouse`). Nothing persists it to OPFS: PyIceberg's committer is fully synchronous, and the
  only synchronous OPFS API needs `createSyncAccessHandle` plumbing that Pyodide's `NATIVEFS`/OPFS
  story does not cover on the write path. Reload and you get a fresh, empty catalog.
- **No delete files (merge-on-read).** PyIceberg's `delete`/`upsert` are copy-on-write, so the demo
  shows rewritten data files, not positional/equality delete files.
- **Compaction is not shown.** `expire_snapshots` is; `rewrite_data_files` does not exist in
  PyIceberg 0.11.1.
- **Big first load.** See below.
- Boot occupies one worker thread for ~25 s; the page stays responsive but every button is disabled
  until the interpreter is up, and commands are serialised (one interpreter, one catalog).

## Boot time and payload

Measured in Chrome (fresh profile) against the assembled production build served from localhost:

| | |
| --- | --- |
| boot (production build) | **24–28 s** to the first usable state (three runs: 24.1 s, 26.4 s, 28.0 s) |
| boot (`npm run dev`) | 26.2 s |
| download on first load | **44 MB** uncompressed: 13 MB Pyodide interpreter (`pyodide.asm.wasm` 9.6 MB, `python_stdlib.zip` 2.5 MB, `pyodide.asm.mjs` 1.2 MB) + 31 MB of wheels |
| page's own JS/HTML | 52 kB (10 kB page, 34 kB worker, 8 kB entry) |
| biggest wheels | pyarrow 10.0 MB, duckdb 8.7 MB, pandas 4.2 MB, numpy 2.9 MB, sqlalchemy 2.0 MB, pyiceberg 0.5 MB |
| after boot | create table ≈ 4 s (first PyIceberg import), append ≈ 0.1–0.4 s, query ≈ 0.03–0.3 s |

Nothing is fetched until the visitor opens `/iceberg/`: the interpreter and the wheels are loaded by
the worker, so the project list at `/` costs nothing. Wheels are served with the lock file as the
resolution, so there is no resolver step and no PyPI — 30 same-origin GETs.

## Running it

```bash
cd iceberg
npm install          # postinstall copies the Pyodide interpreter and downloads the wheels
npm run dev          # http://localhost:5173/iceberg/
npm run typecheck
npm run probe        # drives the whole Python side in Node, no browser (fast feedback loop)
```

From the repo root, for the assembled site:

```bash
npm run build && npm run preview   # http://localhost:4173/iceberg/
```

`scripts/build-runtime.mjs` resolves PyIceberg + SQLAlchemy + PyArrow + DuckDB against Pyodide's
lock file, downloads that closure (30 wheels) into `public/_iceberg/wheels/`, copies the interpreter
into `public/_iceberg/pyodide/`, and writes a reduced `pyodide-lock.json`. Both directories are
gitignored — nothing large is committed — and `--force` rebuilds them.

## Files

| path | what it is |
| --- | --- |
| `iceberg/index.html` | the page (route `/iceberg/`), styles inline |
| `src/main.ts` | UI: steps, SQL box, snapshot log, warehouse tree, file inspector |
| `src/worker.ts` | boots Pyodide, installs the wheels and the Python package, relays commands |
| `src/protocol.ts` | the message and result types shared by both |
| `python/iceberg_wasm/demo.py` | every PyIceberg call the demo makes |
| `python/iceberg_wasm/api.py` | one JSON-in/JSON-out `call(command, payload)` entry point |
| `python/iceberg_wasm/shims.py` | the `pyarrow._s3fs` stub and the synchronous executor |
| `scripts/build-runtime.mjs` | build-time interpreter copy + wheel download |
| `scripts/probe.mjs` | runs the same commands under Node's Pyodide, for development |
