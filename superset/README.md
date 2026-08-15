# superset-wasm

Apache Superset 6.1.0 running entirely in a browser tab. Superset's real Flask application, the
React frontend that ships inside its wheel, and the SQLite metadata and example databases are all in
the client, on Pyodide. Nothing Superset touches runs on a server -- the server only hands over
static files.

Superset itself is **not vendored**: `apache-superset==6.1.0` and its ~130-wheel dependency tree are
installed from PyPI wheels into Pyodide, and this repo only holds the glue that makes those wheels
work without processes, threads, sockets, Celery or a cache backend (`python/superset_wasm/`).

This directory builds the `/superset/` route of the site the repository root assembles (the project
list at `/` is the root's; the dev server here serves it too). The page is `superset/index.html`, and
the runtime the service worker answers is mounted at `/_superset/`, so it collides neither with the
`/superset/` document the host serves nor with anything Airflow owns (`/sw.js`, `/pyodide/`,
`/wheels/`).

```text
page (superset/index.html, src/main.ts)   service worker (public/superset-sw.js)
  boots the runtime worker                  intercepts /_superset/*
  logs in as admin                          serves the interpreter, wheels and the
  hosts the real Superset UI in an iframe      frontend bundle straight from the origin
  keeps the SQLite files in OPFS            forwards everything else to the page
                     |                                    |
                     v                                    v
       runtime worker (src/worker.ts) -> Pyodide -> superset_wasm (python/)
                                                     shims.py      stand-ins for native/process deps
                                                     bootstrap.py  config, db upgrade, init, examples
                                                     webapp.py     boot, login, request entry point
                                                     wsgi.py       WSGI environ from a fetch Request
                                                     wheels.py     build-time wheel/asset surgery
                                                                     |
                                                                     v
                                                          sqlite3 (Pyodide's own build)
```

## Run it yourself

Requirements: Node `^20.19 || >=22.12` and a browser with service worker support (tested in Chrome).

```bash
git clone https://github.com/iancmoritz/apacheprojects.git
cd apacheprojects/superset
npm install     # also copies the Pyodide runtime into public/_superset/pyodide
npm run dev     # first run resolves ~130 wheels and unpacks Superset's frontend (several minutes)
```

Then open <http://localhost:5173/> for the project list, or <http://localhost:5173/superset/> for
this app directly.

The header reports each boot step. The first boot in a tab takes **~75-90s** (Pyodide, 130 wheels,
`superset db upgrade`, `superset init`, admin user, example data); later visits restore the saved
SQLite files and take **~26-30s**. When it says `Superset 6.1.0 is up`, the iframe below is the real
Superset UI, already logged in as `admin`, talking to the Flask app in this tab:

- **Dashboards** -> *Sales Dashboard*: the upstream example dashboard, its charts querying the
  example SQLite database through Superset's own chart data API.
- **SQL Lab**: add a tab against the `examples` database and run real SQL, e.g.
  `SELECT product_line, count(*) AS orders, round(sum(sales)) AS revenue FROM cleaned_sales_data
  GROUP BY product_line ORDER BY revenue DESC`.
- **Charts**: 63 example charts; **Reset database** deletes the saved SQLite files and re-boots from
  scratch.

Static bundle instead of the dev server:

```bash
npm run build     # asset generation + tsc --noEmit + vite build into dist/
```

`dist/` is the whole application (its page at `dist/superset/index.html`, the service worker at the
origin root because it has to be served from `/`, and the runtime under `/_superset/`), so any static
host can serve it. Deployment is driven from the repository root -- `../vercel.json` and
[the root README](../README.md#deploy-to-vercel) -- which merges this `dist/` into the site:

```bash
cd .. && npm run build && npm run preview   # http://localhost:4173/superset/
```

Other scripts:

| command | what it does |
| --- | --- |
| `npm run assets` | regenerates `public/_superset/pyodide` (interpreter + Superset's wheels + lock file) and `public/_superset/static` (Superset's frontend bundle); both are gitignored |
| `npm run typecheck` | `tsc --noEmit` |

Wheels are resolved once in Node by `scripts/build-wheels.mjs` and served from this origin, so the
browser never resolves dependencies against PyPI and never hits the Pyodide CDN at boot. Pass
`--force` (`node scripts/build-wheels.mjs --force`) to rebuild them.

## Payload

The build generates ~116 MB under `public/_superset/` (73 MB interpreter + wheels, 44 MB frontend
bundle), but a cold boot only downloads what it actually needs: **~76 MB** over 279 requests, of
which ~8.5 MB is the slimmed Superset wheel and the rest is the interpreter, the dependency wheels
Superset imports and the frontend chunks the pages visited above load. A second visit downloads
~25 kB: everything else is in the HTTP cache and the databases come back from OPFS.

## How the hard parts work

- **The frontend.** The released `apache_superset` wheel is 103 MB, ~100 MB of which is
  `superset/static/assets` -- a webpack bundle Python never opens. `wheels.py` rewrites the wheel at
  build time: the frontend (plus the example CSVs, which are imported at boot rather than read at
  runtime) is written out as ordinary files under `public/_superset/static`, and the 8.5 MB remainder
  becomes the wheel Pyodide installs. `STATIC_ASSETS_PREFIX = /_superset` makes Superset's Jinja
  templates point at them, and the service worker serves them directly instead of waking Python.
- **No HTTP server.** `wsgi.py` turns an intercepted `Request` into a WSGI `environ` and calls the
  Flask app; `webapp.py` keeps the session cookie jar itself, because the synthetic `Response`
  objects the service worker constructs never reach the browser's cookie store. Requests are
  serialized through one promise tail in the worker: one interpreter, one request at a time.
- **The database.** Pyodide's bundled `sqlite3` rather than PGlite: Superset's metadata schema is
  ordinary SQLAlchemy 1.4, the example data is a normal `to_sql()` load, and SQLite is synchronous
  in-process, so no DBAPI transport is needed. Metadata and examples are two files
  (`superset.db`, `examples.db`) because importing the examples through one connection while the
  metadata session is open deadlocks a single SQLite file.
- **Dependency resolution.** wasm has exactly one build of each native package -- whatever Pyodide
  0.27.7 ships -- so upstream's pins on pandas 2.1, pyarrow 16 and cryptography 46 are relaxed to
  those builds, and `pandas.compat._optional.VERSIONS["sqlalchemy"]` is lowered so pandas accepts the
  SQLAlchemy 1.4 Superset requires. SQLAlchemy 1.4.54 and WTForms-JSON publish no pure-Python wheel,
  so both are repackaged from their sdists at build time.
- **Native and process dependencies.** `shims.py` installs stand-ins before Superset is imported:
  `fcntl` (no-op locking), `resource`, `psutil`, `setproctitle`, `sshtunnel`/`paramiko`, `selenium`
  and `pgsanity`. The ones that stand for a real capability raise an explicit "not available in the
  browser" error when used rather than pretending to work. Superset's `signal.alarm`-based query
  timeout becomes a no-op context manager, since Emscripten has no `SIGALRM`.
- **No Celery, no cache backend.** `CELERY_CONFIG = None` and `GLOBAL_ASYNC_QUERIES` off, so SQL Lab
  runs the query in the request that submitted it; every cache is Flask-Caching's `SimpleCache`, a
  dict in this interpreter, instead of a Redis that does not exist. Rate limiting, CSRF and Talisman
  are off, because the "client" and the "server" are the same tab.

## Limitations

- **One request at a time, and a query blocks the tab.** There is one interpreter and no `SIGALRM`,
  so a slow query cannot be timed out and holds up every other request until it finishes.
- **Only the bundled data.** Databases are reachable only if their driver is in the tab and speaks
  over something other than TCP: SQLite is, everything else (Postgres, MySQL, Trino, ...) is not,
  because a browser cannot open a raw socket. SSH tunnels are out for the same reason.
- **No thumbnails, alerts, reports or scheduled anything.** They need Celery beat and a headless
  Chrome; `selenium` is a stub that raises.
- **Uploads and CSV/Excel import** work only for what Python can parse in-tab; anything wired to a
  subprocess (`pgsanity`'s `ecpg`, for example) raises instead.
- **Persistence is per-origin, not durable.** The two SQLite files are copied to OPFS after the first
  boot and restored on the next visit; clearing site data (or **Reset database**) starts over. Work
  done in a session is only saved when it lands in those files at snapshot time.
- The frontend logs `AG Grid: error #200` from Superset 6.1.0's own bundle when SQL Lab renders its
  results grid. It is upstream's module registration warning, not something this port introduces, and
  the grid renders and sorts normally.
