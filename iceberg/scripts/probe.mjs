/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Drives python/iceberg_wasm through the same `call(command, payload)` entry point the page uses,
// but in Node: the whole demo can be checked without a browser.  Run it with
// `node scripts/probe.mjs`; the wheels come from public/_iceberg (npm run assets).

import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

import { loadPyodide } from "pyodide";

const root = join(import.meta.dirname, "..");
const runtime = join(root, "public", "_iceberg");
const lock = JSON.parse(await readFile(join(runtime, "wheels", "pyodide-lock.json"), "utf8"));

const pyodide = await loadPyodide({
  indexURL: join(runtime, "pyodide") + "/",
  packageBaseUrl: join(runtime, "wheels") + "/",
  lockFileURL: join(runtime, "wheels", "pyodide-lock.json"),
});
console.time("packages");
await pyodide.loadPackage(Object.keys(lock.packages), { messageCallback: () => {} });
console.timeEnd("packages");

pyodide.FS.mkdirTree("/pkg/iceberg_wasm");
const python = join(root, "python", "iceberg_wasm");
for (const name of await readdir(python)) {
  pyodide.FS.writeFile(`/pkg/iceberg_wasm/${name}`, await readFile(join(python, name)));
}
pyodide.runPython(`import sys; sys.path.insert(0, "/pkg")`);
const call = pyodide.runPython(`from iceberg_wasm.api import call; call`);

const run = (command, payload = {}) => {
  const result = JSON.parse(call(command, JSON.stringify(payload)));
  if (!result.ok) throw new Error(`${command}: ${result.error}\n${result.traceback ?? ""}`);
  return result;
};

const show = (label, value) => console.log(`\n== ${label}\n` + JSON.stringify(value, null, 1).slice(0, 1800));

show("create", run("create"));
show("append 1", run("append"));
show("append 2", run("append"));
show("upsert", run("upsert"));
show("delete", run("delete"));
show("evolve schema", run("evolve_schema"));
show("evolve partitioning", run("evolve_partitioning"));
show("append 3", run("append"));

const state = run("state");
show("state", { ...state, snapshots: state.snapshots.map((s) => [s.snapshot_id, s.operation, s.parent_id]) });

show("sql now", run("query", { sql: "select pickup_city, count(*) n, round(avg(fare),2) fare from trips group by 1 order by 1" }));
const first = state.snapshots[0].snapshot_id;
show("sql as of first snapshot", run("query", { sql: "select count(*) as n, min(pickup_ts) as first_pickup from trips", snapshot_id: first }));
show("sql tips (old files read as null)", run("query", { sql: "select count(*) as n, count(tip) as with_tip from trips" }));
show("plan unfiltered", { ...run("plan"), files: undefined });
show("plan pruned", { ...run("plan", { row_filter: "pickup_ts >= '2024-01-07T00:00:00'" }), files: undefined });

const tree = run("tree");
show("tree", { bytes: tree.bytes, files: tree.files.map((f) => `${f.kind} ${f.path} ${f.bytes}`) });
for (const kind of ["metadata", "manifest-list", "manifest", "data"]) {
  const file = tree.files.filter((f) => f.kind === kind).pop();
  if (file) show(`preview ${kind}`, run("preview", { relative: file.path }));
}
show("expire", run("expire"));
