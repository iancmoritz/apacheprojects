# apacheprojects

Run Apache SWF's projects, all from your browser.

One static site: a project list at `/`, and each project on its own route, running entirely in the
visitor's tab.

| project | route | what it is |
| --- | --- | --- |
| [`airflow/`](airflow/) | `/airflow/` | Apache Airflow 3.3.1 — API server, scheduler, worker and Postgres all WebAssembly in the tab |
| [`echarts/`](echarts/) | `/echarts/` | Apache ECharts 6.1.0 — a live option editor, a dozen presets, brushing, drill-down, a streaming series and 3D on WebGL |
| [`iceberg/`](iceberg/) | `/iceberg/` | Apache Iceberg — PyIceberg writes a real table (metadata, manifests, Parquet) in the tab and DuckDB queries it, snapshots and time travel included |

## Build the site

Requirements: Node `^20.19 || >=22.12`.

```bash
npm install       # nothing to install here; each project installs its own dependencies
npm run build     # builds every project and assembles dist/
npm run preview   # http://localhost:4173/
```

`npm run build` builds each project in its own directory and then runs
[`scripts/assemble.mjs`](scripts/assemble.mjs), which copies the project list (`index.html`,
`projects.js`) and every project's `dist/` into the top-level `dist/`. Projects keep the paths they
build, so Airflow's page lands on `/airflow/`; if two projects ever claim the same file, the assemble
step fails instead of picking a winner.

For day-to-day work on a project, use that project's own dev server (`npm run dev` here forwards to
Airflow's, which also serves the project list at `/`).

## Deploy to Vercel

`vercel.json` in this directory pins everything Vercel needs — build command, output directory
(`dist`) and the headers that matter (`sw.js` must never be cached; wheels and the interpreter are
immutable) — so the deployment is the repository root with no settings to fill in.

From the dashboard:

1. **Add New → Project**, import this repository.
2. Leave **Root Directory** at the repository root and every build setting as detected; they come
   from `vercel.json`.
3. **Deploy.** The build installs each project's dependencies and builds it, which for Airflow means
   downloading the Pyodide interpreter and ~110 wheels, so the first build takes a few minutes and
   needs network access to PyPI and the Pyodide CDN. Nothing is downloaded at runtime — the
   deployment serves the interpreter and every wheel from its own origin.

Or from the CLI, in this directory:

```bash
npm i -g vercel
vercel        # preview deployment
vercel --prod
```

No environment variables, no serverless functions, no build-time secrets. The deployment is ~60 MB of
wasm and wheels, and service workers need HTTPS — which every `*.vercel.app` domain already is.

## Add a project

1. Add a top-level directory that builds a `dist/` of its own, serving its page at its route
   (Airflow's build emits `dist/airflow/index.html`).
2. Add the directory to `PROJECTS` in [`scripts/assemble.mjs`](scripts/assemble.mjs) and, if it needs
   a build of its own, to the root `build` script.
3. Add a card to `PROJECTS` in [`projects.js`](projects.js): `name`, `version`, `href`, `blurb`,
   `pieces` and an `accent` colour.
