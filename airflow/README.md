# airflow-wasm

Apache Airflow 3.3.1 running entirely in a browser tab. The API server, scheduler, Dag processor,
worker and the Postgres metadata database are all WebAssembly, all in the client. Nothing Airflow
touches runs on a server -- the server only hands over static files.

Airflow itself is **not vendored**: `apache-airflow-core==3.3.1` and its dependency tree are
installed from PyPI wheels into Pyodide, and this repo only holds the glue that makes those wheels
work without processes, threads, sockets or a real Postgres (`python/airflow_wasm/`).

This directory builds the `/airflow/` route of the site the repository root assembles (the project
list at `/` is the root's; the dev server here serves it too). The page is `airflow/index.html` and
the runtime the service worker answers is mounted at `/_airflow/`, so it does not collide with the
`/airflow/` document the host serves.

```text
page (airflow/index.html, src/main.ts)  service worker (public/sw.js)
  boots the runtime worker                intercepts /_airflow/*
  ticks the scheduler every 2s            hands each request to the page
  hosts the real Airflow UI in an iframe            |
                     |                              |
                     v                              v
       runtime worker (src/worker.ts) -> Pyodide -> airflow_wasm (python/)
                                                      api.py          ASGI, called directly
                                                      dagparse.py     parses dags/ in-process
                                                      orchestrator.py one cooperative tick
                                                      runner.py       runs tasks in-process
                                                      pglite.py       DBAPI over PGlite
                                                                       |
                                                                       v
                                                              PGlite (Postgres in WASM)
```

## Run it yourself

Requirements: Node `^20.19 || >=22.12` and a Chromium/Firefox/Safari with service worker support.

```bash
git clone https://github.com/iancmoritz/apacheprojects.git
cd apacheprojects/airflow
npm install     # also copies the Pyodide runtime into public/pyodide
npm run dev     # first run resolves and downloads ~110 Airflow wheels (several minutes)
```

Then open <http://localhost:5173/> for the project list, or <http://localhost:5173/airflow/> for this
app directly.

The header reports each boot step; the first cold boot in the tab takes ~60-120s (Pyodide, 110
wheels, PGlite, `airflow db migrate`). When it says `Airflow 3.3.1 is up`, the iframe below is the
real Airflow UI talking to the in-tab API server. Two demo Dags (`hello_wasm`, `wasm_etl`) ship
unpaused, so hitting **Trigger** on one runs it in the tab within a couple of scheduler ticks; its
logs are readable in the UI's Logs tab. **Reset database** wipes the metadata DB and re-boots.

Static bundle instead of the dev server:

```bash
npm run build     # asset generation + tsc --noEmit + vite build into dist/
```

`dist/` is the whole application (its page at `dist/airflow/index.html`, everything else at the
origin root, because the service worker has to be served from `/`), so any static host can serve it.
Deployment is driven from the repository root — `../vercel.json` and
[the root README](../README.md#deploy-to-vercel) — which merges this `dist/` into the site.

Other scripts:

| command | what it does |
| --- | --- |
| `npm run assets` | regenerates `public/pyodide` (interpreter) and `public/wheels` (Airflow's wheels + Pyodide lock file); both are gitignored |
| `npm run typecheck` | `tsc --noEmit` |

Wheels are resolved once in Node by `scripts/build-wheels.mjs` and served from this origin, so the
browser never resolves dependencies against PyPI and never hits the Pyodide CDN at boot. Pass
`--force` (`node scripts/build-wheels.mjs --force`) to re-resolve them.

## How the hard parts work

- **Postgres.** `pglite.py` is a DBAPI transport: it buffers the Postgres frontend protocol that
  `pg8000` produces and pushes it through PGlite's synchronous `execProtocolRawSync`. No sockets, no
  `SharedArrayBuffer`, no COOP/COEP headers. SQLAlchemy uses `StaticPool` so all access is serialized
  through the one PGlite instance, and Airflow's async connection is turned off (`asyncpg` and
  `greenlet` cannot be built for Emscripten).
- **No processes, no threads.** `syncasgi.py` drives the ASGI apps to completion synchronously and
  refuses any coroutine that would actually suspend; `patches.py` monkey patches Starlette's
  threadpool calls to run inline. Tasks run through the task SDK's in-process supervisor with its
  socket thread removed, so they execute in this interpreter while still talking to the real
  Execution API.
- **Native dependencies.** `shims.py` installs stub modules for the packages that cannot build for
  Emscripten (`psutil`, `setproctitle`, `greenlet`, `grpcio`, ...) before Airflow is installed, both
  at wheel-resolution time in Node and at boot in the browser.
- **Scheduling.** `orchestrator.py` is one `tick()`: create scheduled runs, run whatever tasks are
  ready, update run state, and heartbeat the scheduler/triggerer/Dag processor rows the UI reads its
  health badges from. The page calls it on an interval.
- **Logs.** `runner.py` points structlog at the same per-attempt log file a real worker would write,
  so the UI's Logs tab reads them locally instead of asking a log server that does not exist.

## Limitations

- **Persistence.** The metadata database is in-memory: PGlite's OPFS-backed datadirs stop answering
  `execProtocolRawSync` once a statement has to touch storage. A datadir can still be asked for with
  `?dataDir=idb://airflow` and the runtime falls back to memory if it fails, so state is lost on
  reload.
- **What can run.** Anything that stays inside the interpreter: `@task`, PythonOperator, the SDK.
  Not Bash/subprocess operators, virtualenv tasks, Celery/Kubernetes executors, or providers needing
  native libraries or raw TCP.
- **One task at a time.** Parallelism is 1 and everything is cooperative, so a long task blocks the
  tick that runs it.
- Task logs also contain the in-tab API server's own log lines, because the API really is running in
  the same interpreter as the task.
