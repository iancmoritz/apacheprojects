# datafusion-wasm

Apache DataFusion 53.1.0 — the real Rust query engine — compiled to `wasm32-unknown-unknown` and
running inside a browser tab as a SQL workbench. Parsing, logical planning, optimization and physical
execution all happen in the visitor's browser; the server only hands over static files. There is no
query API, no proxy and nothing to configure.

DataFusion is **not vendored and not reimplemented**: `crates/workbench` depends on the published
`datafusion` crate from crates.io and exposes a handful of `wasm_bindgen` entry points over a single
`SessionContext`. The wasm module is built by `scripts/build-wasm.mjs` and gitignored, the way
`../airflow/scripts/build-wheels.mjs` generates its wheels.

This directory builds the `/datafusion/` route of the site the repository root assembles (the project
list at `/` is the root's; the dev server here serves it too). Everything it emits at the origin root
is namespaced under `/_datafusion/`, so it cannot collide with another project's files.

```text
page (datafusion/index.html, src/main.ts)
  editor, table list, results, plans          Arrow IPC bytes in, an Arrow table out
                     |  postMessage
                     v
worker (src/worker.ts)  streams /_datafusion/workbench.wasm with progress
                     |
                     v
crates/workbench (Rust -> wasm, src/lib.rs)
  Workbench::new          SessionContext, 1 partition, information_schema on
  registerCsv/Parquet     Arrow CSV / Parquet reader -> MemTable
  sql(sql, max_rows)      optimized logical plan + physical plan + rows as Arrow IPC
                     |
                     v
                Apache DataFusion 53.1.0 (crates.io) + arrow/parquet 58.4
```

## Run it yourself

Requirements: Node `^20.19 || >=22.12` and a Rust toolchain (`rustup`/`cargo`). The build adds the
`wasm32-unknown-unknown` target itself if `rustup` is present, and downloads the matching
`wasm-bindgen` CLI and `wasm-opt` release binaries into `.toolchain/` rather than `cargo install`ing
them (a `cargo install wasm-bindgen-cli` costs more than the whole DataFusion build on a cold
machine). If `cargo` is missing the build fails with the rustup one-liner instead of falling back to
anything prebuilt — there is no prebuilt wasm here.

```bash
git clone https://github.com/iancmoritz/apacheprojects.git
cd apacheprojects/datafusion
npm install
npm run dev     # first run compiles DataFusion for wasm: ~8-10 min cold, seconds afterwards
```

Then open <http://localhost:5173/datafusion/> (or <http://localhost:5173/> for the project list).

The header reports the boot steps: the module streams in with a progress bar, the session starts, and
the two bundled tables are registered — `cities` (a 3.7 kB CSV, 76 rows) and `trips` (a 2.5 MB
Parquet file, 120 000 rows generated at build time by `crates/mkdata`). Then it runs the first example
query on its own, so the page arrives with real results in it.

From there: pick an example, edit the SQL and hit **Run** (or `Ctrl`/`Cmd`+`Enter`), or ask for
**EXPLAIN** / **EXPLAIN ANALYZE** of whatever is in the editor — the plan panes render the operator
tree with the `metrics=[...]` of an `ANALYZE` run picked out. Drag a local `.csv` or `.parquet` file
onto the drop zone and it is registered as a table named after the file; the bytes go from the
`File` straight into the wasm module's memory, and no request leaves the tab.

Static bundle instead of the dev server:

```bash
npm run build     # wasm + data generation, tsc --noEmit, vite build into dist/
npm run preview
```

Other scripts:

| command | what it does |
| --- | --- |
| `npm run assets` | rebuilds `public/_datafusion/workbench.wasm` + `src/generated/` glue and regenerates the example tables; both gitignored |
| `npm run typecheck` | `tsc --noEmit` |

## Size and boot time

Measured on this build (`npm run build`, release profile, `opt-level="z"`, `lto = "fat"`,
`panic = "abort"`, `strip = "debuginfo"`, then `wasm-opt -Oz --strip-debug --strip-dwarf`):

| artifact | raw | gzip | brotli |
| --- | --- | --- | --- |
| `workbench.wasm` (cargo output) | 27.7 MB | | |
| `workbench.wasm` (after `wasm-bindgen`) | 28.3 MB | | |
| **`workbench.wasm` (shipped, after `wasm-opt -Oz`)** | **17.1 MB** | **4.8 MB** | **3.8 MB** |
| `trips.parquet` | 2.5 MB | | |
| `cities.csv` | 3.7 kB | | |
| page + JS bundle + worker | 209 kB | 46 kB | |

So the interesting number is the ~4-5 MB actually on the wire, not the 17 MB the module unpacks to.
Compression is the host's job: Vercel gzips/brotlis `application/wasm` automatically, and
`../vercel.json` marks `/_datafusion/*` immutable so a second visit costs nothing. The module is a
plain file fetched by the worker, never inlined into a JS chunk, so the page is interactive before it
arrives and the progress bar is real bytes.

Boot on a warm cache, measured in Chrome on the assembled production build: **~2.5 s** from page load
to results on screen — dominated by compiling 17 MB of wasm, with registering the 120 000-row Parquet
file taking a few hundred milliseconds. Cold, add however long 4-5 MB takes on the visitor's
connection. Queries over `trips` then run in single-digit to low tens of milliseconds.

## How the hard parts work

- **A synchronous engine out of an async one.** DataFusion is `async` and spawns tasks, so
  `Workbench` owns a current-thread Tokio runtime and `block_on`s every call. That is only acceptable
  because the module lives in a Web Worker: blocking that thread blocks nothing the visitor can see,
  and it avoids `wasm-bindgen-futures` plumbing for an engine that is CPU-bound anyway.
- **No C in the dependency tree.** The obvious build (`datafusion` with default features) pulls
  `zstd-sys`/`lzma-sys` in through Arrow IPC and Parquet compression and needs a working
  `clang --target=wasm32` to compile. Rather than shipping a C cross-compiler expectation into the
  build, the feature set is trimmed: `default-features = false` on both `datafusion` and `parquet`,
  the SQL/expression features enabled explicitly, and Snappy (pure Rust) used for the generated
  Parquet. `cargo tree` for the wasm target has no `*-sys` crate in it, so a plain rustup toolchain
  is the only prerequisite.
- **Results without re-encoding.** `sql()` returns Arrow IPC stream bytes; the worker transfers the
  `ArrayBuffer` to the page, which hands it to `apache-arrow`'s `tableFromIPC`. Nothing is turned
  into JSON on the way, values are formatted from their Arrow type, and the buffer is transferred
  rather than copied. Results are sliced to 1 000 rows *inside* the module, so a `SELECT *` over
  120 000 rows encodes 1 000 of them and still reports the true count.
- **Both plans, and where the time went.** The page runs `sql()` once and gets the optimized logical
  plan (`display_indent()`), the physical plan (`displayable(...).indent(true)`) and separate
  plan/execute timings from `performance.now()` inside the module — so the timing is the engine's, not
  a round trip. `EXPLAIN ANALYZE` is a real DataFusion statement, not a re-render of those strings.
- **`getrandom` on wasm.** DataFusion needs randomness (`uuid`, hash seeds), which `getrandom` 0.3
  will not provide for an unknown-unknown target unless told which backend to use;
  `.cargo/config.toml` sets `--cfg getrandom_backend="wasm_js"`.
- **The module's URL.** `wasm-bindgen` emits glue that resolves `workbench_bg.wasm` relative to
  itself, which Vite would then bundle as a second copy of a 17 MB asset. The build script rewrites
  that to `/_datafusion/workbench.wasm`, assembled at runtime so no bundler resolves it either, and
  the worker streams that URL itself to report progress.

## Limitations

- **Tables are in memory.** Every registered table is a `MemTable`, so a dropped file has to fit in
  the tab's memory (wasm32 caps the module at 4 GB and Chrome will refuse well before that). There is
  no object store, so `CREATE EXTERNAL TABLE ... LOCATION 'https://...'` cannot read anything.
- **One thread.** `target_partitions` is 1: there is no worker pool behind the engine, and a query
  runs on the one worker thread. Plans are therefore simpler than the same query's plan on a server
  (no `RepartitionExec` fan-out), which is arguably easier to read.
- **Trimmed feature set.** No `array_expressions` beyond `nested_expressions`, no
  `avro`/`json`/`compression` codecs, and Parquet reads only support Snappy/uncompressed columns — a
  zstd-compressed Parquet file dropped on the page will report a codec error rather than load.
- **Nothing persists.** Reloading the tab restarts the session; only the bundled tables come back.
- **Statements that write** (`CREATE TABLE AS`, `INSERT`) work against in-memory tables for the life
  of the tab, but there is nowhere for them to go afterwards.
