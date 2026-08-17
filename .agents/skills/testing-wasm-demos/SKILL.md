---
name: testing-wasm-demos
description: How to run and browser-test the in-tab WebAssembly demos in apacheprojects (/airflow/, /iceberg/) — servers, fresh Chrome profile, boot times, and the schema-evolution behaviour of the Iceberg demo's SQL box.
---

# Testing the in-tab WASM demos (apacheprojects)

## Servers

- Production: `npm run build` at the repo root, then `npm run preview` → http://localhost:4173/
  (project list at `/`, demos at `/airflow/` and `/iceberg/`).
- Dev: `cd iceberg && npm run dev` → http://localhost:5173/iceberg/ (each subproject has its own
  Vite dev server; the root `npm run dev` serves the project list).
- Both preview and dev are Vite processes. `pkill -f vite` kills BOTH — if the user started the
  preview for you, restart it afterwards (`npm run preview` from the repo root) instead of leaving
  the environment broken.

## Browser

- Service workers hang in the session's default Chrome profile. Move `~/.browser_data_dir` aside
  (timestamped backup) and let the browser recreate a fresh profile before testing.
- `wmctrl -r :ACTIVE: -b add,maximized_vert,maximized_horz` may fail (no client list) in this
  environment; the browser tool's screenshots still capture the full app viewport.
- Boot of the Iceberg demo (Pyodide + PyIceberg + PyArrow + DuckDB, ~44 MB from localhost) takes
  roughly 20–30 s; the header badge reports `booted in X s`. Airflow's boot is slower.
- Verify "no runtime network" with
  `Array.from(new Set(performance.getEntriesByType('resource').map(e=>new URL(e.name).origin)))`
  — it should be only the localhost origin. Use the browser tool's console action (not a JS
  snippet) to read actual console log entries.

## Iceberg demo specifics

- Button ids: `#create #append #upsert #delete #evolve-schema #evolve-spec #expire #run #plan`,
  `#as-of` select, `#filter`, `#tree` (warehouse), `#preview` (inspector).
- Tree entries are `#tree button[data-path]`; to pick a specific file kind, query
  `document.querySelectorAll('#tree button')` and filter on `dataset.path` (`snap-*.avro` =
  manifest list, other `*.avro` = manifest, `*.metadata.json`, `*.parquet`).
- Schema-aware default SQL: the column renamed by "Evolve schema" (`city` → `pickup_city`) only
  exists in schema 1, so the page keeps the generated default query in sync with whichever schema
  the selected `as of` snapshot used (`schemaAt()`/`defaultSql()`/`syncSql()` in `src/main.ts`,
  backed by the `schemas` list returned by `state()` in `python/iceberg_wasm/demo.py`). Expect the
  textarea to start as `select city, …`, flip to `pickup_city` after schema evolution, and flip
  back when an older snapshot is picked; the `#schema-hint` div under the box reads
  `columns at this snapshot (schema N): …`. Verified working — but if a regression reintroduces
  `BinderException: Referenced column "pickup_city" not found`, the cause is the default SQL not
  matching the selected snapshot's schema, and a schema-0 compatible query
  (`select city, count(*) … from trips group by 1`) is the workaround for proving time travel.
- The sync only rewrites the textarea when it still holds the page-generated query, so a visitor's
  own SQL survives an `as of` change — worth re-checking after any change to that code path.
- Clearing the SQL textarea: click it, `Control+a`, press `Delete`, then type. Typing without
  clearing concatenates into the existing query.

## Devin Secrets Needed

None — everything runs locally with no auth.
