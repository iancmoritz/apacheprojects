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

// The page: buttons that call PyIceberg, a SQL box that calls DuckDB, and three read-only views of
// the table (snapshot log, warehouse directory, decoded file).  All of the interesting work happens
// in the worker; this file only draws what comes back.

import { BootPanel } from "../../design/boot";

import "./iceberg.css";
import type {
  Field,
  Preview,
  PlanResult,
  QueryResult,
  State,
  StepResult,
  TreeResult,
  Unidentified,
  WorkerRequest,
  WorkerResponse,
} from "./protocol";

const worker = new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
const pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
let nextId = 1;

const el = <T extends HTMLElement>(selector: string): T => document.querySelector<T>(selector)!;

const status = el<HTMLSpanElement>("#status");
const bootTime = el<HTMLSpanElement>("#boot-time");
const stepResult = el<HTMLDivElement>("#step-result");
const summary = el<HTMLDivElement>("#summary");
const asOf = el<HTMLSelectElement>("#as-of");
const sqlBox = el<HTMLTextAreaElement>("#sql");

const buttons = {
  create: el<HTMLButtonElement>("#create"),
  append: el<HTMLButtonElement>("#append"),
  upsert: el<HTMLButtonElement>("#upsert"),
  delete: el<HTMLButtonElement>("#delete"),
  evolveSchema: el<HTMLButtonElement>("#evolve-schema"),
  evolveSpec: el<HTMLButtonElement>("#evolve-spec"),
  expire: el<HTMLButtonElement>("#expire"),
  run: el<HTMLButtonElement>("#run"),
  plan: el<HTMLButtonElement>("#plan"),
  reset: el<HTMLButtonElement>("#reset"),
};

const boot = new BootPanel({
  mount: el<HTMLElement>("#boot"),
  title: "Starting Apache Iceberg",
  detail:
    "No server. CPython arrives as Pyodide, then the PyIceberg and DuckDB wheels. PyIceberg then " +
    "writes a real Iceberg table — metadata JSON, manifest lists, manifests and Parquet — into " +
    "this tab's filesystem, with a SQLite catalog; DuckDB answers the SQL.",
  logs: [{ label: "Runtime log" }, { label: "Table activity", collapsed: true }],
});

function say(text: string): void {
  boot.say(text);
}

worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
  const message = event.data;
  if (message.type === "progress") {
    status.textContent = message.text;
    boot.now(message.text);
    say(message.text);
    return;
  }
  const settle = pending.get(message.id);
  if (!settle) return;
  pending.delete(message.id);
  if (message.type === "error") settle.reject(new Error(message.error));
  else settle.resolve(message.result);
};

function send<T>(request: Unidentified<WorkerRequest>): Promise<T> {
  const id = nextId++;
  return new Promise<T>((resolve, reject) => {
    pending.set(id, { resolve: resolve as (value: unknown) => void, reject });
    worker.postMessage({ ...request, id } as WorkerRequest);
  });
}

const command = <T>(name: string, payload: Record<string, unknown> = {}): Promise<T> =>
  send<T>({ type: "call", command: name, payload });

/** Disable every button while Python is busy: one interpreter, one catalog, one thing at a time. */
async function busy<T>(label: string, work: () => Promise<T>): Promise<T | undefined> {
  const previous = Object.values(buttons).map((button) => button.disabled);
  Object.values(buttons).forEach((button) => (button.disabled = true));
  status.textContent = label;
  try {
    return await work();
  } catch (error) {
    const text = (error as Error).message;
    stepResult.innerHTML = `<span class="error">${escape(text)}</span>`;
    boot.log("Table activity").write(`${label}: ${text}`);
    return undefined;
  } finally {
    Object.values(buttons).forEach((button, index) => (button.disabled = previous[index]));
    status.textContent = "idle";
  }
}

function escape(value: unknown): string {
  return String(value ?? "").replace(
    /[&<>"]/g,
    (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[character]!,
  );
}

function table(columns: string[], rows: unknown[][]): string {
  if (!rows.length) return `<div class="hint">no rows</div>`;
  const head = columns.map((name) => `<th>${escape(name)}</th>`).join("");
  const body = rows
    .map((row) => `<tr>${row.map((value) => `<td>${escape(value)}</td>`).join("")}</tr>`)
    .join("");
  return `<table class="grid"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

// -- the SQL box ---------------------------------------------------------------------------------

interface TableSchema {
  schema_id: number;
  fields: Field[];
}

/** The schema a snapshot was written against: an older snapshot predates the rename, and knows the
 *  column by its old name.  Iceberg reads it either way, but the SQL has to say the right name. */
function schemaAt(snapshotId: string): TableSchema | undefined {
  const snapshot = state.snapshots?.find((entry) => entry.snapshot_id === snapshotId);
  const wanted = snapshot?.schema_id ?? state.schema?.schema_id;
  return state.schemas?.find((schema) => schema.schema_id === wanted) ?? state.schema;
}

function defaultSql(schema: TableSchema | undefined): string {
  const city = schema?.fields.find((field) => field.name.endsWith("city"))?.name ?? "city";
  return `select ${city}, count(*) as trips, round(avg(fare), 2) as avg_fare\nfrom trips\ngroup by 1\norder by 1`;
}

// What the page last put in the box, so a query the visitor typed is never overwritten.
let generated = sqlBox.value.trim();

function syncSql(): void {
  const schema = schemaAt(asOf.value);
  if (sqlBox.value.trim() === generated) {
    generated = defaultSql(schema);
    sqlBox.value = generated;
  }
  el("#schema-hint").textContent = schema
    ? `columns at this snapshot (schema ${schema.schema_id}): ${schema.fields.map((field) => field.name).join(", ")}`
    : "";
}

asOf.onchange = () => syncSql();

const bytes = (value: number): string =>
  value < 1024 ? `${value} B` : value < 1024 * 1024 ? `${(value / 1024).toFixed(1)} kB` : `${(value / 1048576).toFixed(1)} MB`;

// -- state --------------------------------------------------------------------------------------

let state: State = { exists: false, warehouse: "/warehouse" };

function renderState(): void {
  const exists = state.exists;
  buttons.create.disabled = exists;
  buttons.append.disabled = !exists;
  buttons.upsert.disabled = !exists || !state.snapshots?.length;
  buttons.delete.disabled = !exists || !state.snapshots?.length;
  buttons.evolveSchema.disabled = !exists;
  buttons.evolveSpec.disabled = !exists;
  buttons.expire.disabled = !exists || (state.snapshots?.length ?? 0) < 2;
  buttons.run.disabled = !exists || !state.snapshots?.length;
  buttons.plan.disabled = buttons.run.disabled;
  buttons.reset.disabled = false;

  if (!exists) {
    summary.textContent = `no table yet — warehouse ${state.warehouse} is empty`;
    el("#schema").innerHTML = "";
    el("#specs").innerHTML = "";
    el("#snapshots").innerHTML = "";
    asOf.innerHTML = `<option value="">current snapshot</option>`;
    syncSql();
    return;
  }

  summary.innerHTML =
    `<code>${escape(state.identifier)}</code> — Iceberg format v${state.format_version}, ` +
    `${state.rows} rows, ${state.snapshots?.length ?? 0} snapshots, ` +
    `schema ${state.schema?.schema_id}, spec ${state.specs?.find((spec) => spec.is_current)?.spec_id}` +
    `<br /><span class="hint">${escape(state.metadata_location)}</span>`;

  el("#schema").innerHTML = table(
    ["field id", "name", "type", "required"],
    (state.schema?.fields ?? []).map((field) => [field.id, field.name, field.type, field.required]),
  );

  el("#specs").innerHTML = table(
    ["spec", "fields", "current"],
    (state.specs ?? []).map((spec) => [
      spec.spec_id,
      spec.fields.map((field) => `${field.transform}(${field.source_id}) as ${field.name}`).join(", ") ||
        "unpartitioned",
      spec.is_current ? "yes" : "",
    ]),
  );

  const snapshots = state.snapshots ?? [];
  el("#snapshots").innerHTML = snapshots
    .map((snapshot) => {
      const when = new Date(snapshot.timestamp_ms).toLocaleTimeString();
      const files = snapshot.summary["total-data-files"] ?? "?";
      const rows = snapshot.summary["total-records"] ?? "?";
      return (
        `<li class="${snapshot.is_current ? "current" : ""}">` +
        `<strong>${escape(snapshot.operation)}</strong> ${escape(snapshot.snapshot_id)}` +
        `${snapshot.is_current ? ' <span class="badge">current</span>' : ""}<br />` +
        `<span class="hint">seq ${snapshot.sequence_number} · ${when} · schema ${snapshot.schema_id} · ` +
        `${rows} rows in ${files} files · parent ${escape(snapshot.parent_id ?? "none")}</span><br />` +
        `<span class="hint">${escape(snapshot.manifest_list)}</span></li>`
      );
    })
    .join("");

  const selected = asOf.value;
  asOf.innerHTML =
    `<option value="">current snapshot</option>` +
    snapshots
      .map(
        (snapshot) =>
          `<option value="${snapshot.snapshot_id}">${escape(snapshot.operation)} · ` +
          `${new Date(snapshot.timestamp_ms).toLocaleTimeString()} · ${snapshot.snapshot_id}</option>`,
      )
      .join("");
  if (snapshots.some((snapshot) => snapshot.snapshot_id === selected)) asOf.value = selected;
  syncSql();
}

async function refresh(): Promise<void> {
  state = await command<State>("state");
  renderState();
  await renderTree();
}

// -- warehouse ----------------------------------------------------------------------------------

async function renderTree(): Promise<void> {
  const tree = await command<TreeResult>("tree");
  el("#tree-meta").textContent = `${tree.root} — ${tree.files.length} files, ${bytes(tree.bytes)}`;
  const list = el<HTMLUListElement>("#tree");
  list.innerHTML = tree.files
    .map(
      (file) =>
        `<li><button type="button" data-path="${escape(file.path)}">` +
        `<span class="kind ${file.kind}">${file.kind}</span>${escape(file.path)} ` +
        `<span class="hint">${bytes(file.bytes)}</span></button></li>`,
    )
    .join("");
  for (const button of list.querySelectorAll<HTMLButtonElement>("button[data-path]")) {
    button.onclick = () => void showPreview(button.dataset.path!);
  }
}

async function showPreview(path: string): Promise<void> {
  const preview = await busy(`reading ${path}`, () => command<Preview>("preview", { relative: path }));
  if (!preview) return;
  el("#preview-meta").innerHTML = `<code>${escape(preview.path)}</code> — ${preview.kind}`;
  const target = el("#preview");
  if (preview.kind === "metadata") {
    target.innerHTML = `<pre>${escape(JSON.stringify(preview.json, null, 2))}</pre>`;
  } else if (preview.entries) {
    const columns = Object.keys(preview.entries[0] ?? {});
    target.innerHTML = `<div class="scroll">${table(
      columns,
      preview.entries.map((entry) =>
        columns.map((column) =>
          typeof entry[column] === "object" && entry[column] !== null
            ? JSON.stringify(entry[column])
            : entry[column],
        ),
      ),
    )}</div>`;
  } else if (preview.kind === "data") {
    target.innerHTML =
      `<div class="hint">${preview.records} rows in ${preview.row_groups} row group(s), ` +
      `${bytes(preview.bytes ?? 0)}, written by ${escape(preview.created_by)}</div>` +
      `<div class="scroll">${table(
        ["column", "parquet type", "iceberg field id"],
        (preview.columns ?? []).map((column) => [column.name, column.type, column.field_id]),
      )}</div>` +
      `<div class="hint" style="margin-top:0.4rem">first rows</div>` +
      `<div class="scroll">${table(preview.head?.columns ?? [], preview.head?.rows ?? [])}</div>`;
  } else {
    target.innerHTML = `<div class="hint">${escape(preview.kind)}, ${bytes(preview.bytes ?? 0)}</div>`;
  }
}

// -- steps --------------------------------------------------------------------------------------

function step(button: HTMLButtonElement, label: string, name: string, payload: Record<string, unknown> = {}): void {
  button.onclick = async () => {
    const result = await busy(label, () => command<StepResult>(name, payload));
    if (!result) return;
    stepResult.textContent = `${result.detail} (${result.ms} ms)`;
    boot.log("Table activity").write(`${name}: ${result.detail}`);
    await refresh();
  };
}

step(buttons.create, "creating the table", "create");
step(buttons.append, "writing Parquet and committing a snapshot", "append");
step(buttons.upsert, "merging rows", "upsert");
step(buttons.delete, "deleting rows", "delete");
step(buttons.evolveSchema, "evolving the schema", "evolve_schema");
step(buttons.evolveSpec, "evolving the partition spec", "evolve_partitioning");
step(buttons.expire, "expiring the oldest snapshot", "expire");

buttons.reset.onclick = async () => {
  if (!(await busy("deleting the warehouse", () => command("reset")))) return;
  stepResult.textContent = "warehouse deleted";
  el("#query-result").innerHTML = "";
  el("#plan-result").innerHTML = "";
  el("#preview").innerHTML = "";
  await refresh();
};

buttons.run.onclick = async () => {
  el("#query-error").textContent = "";
  const payload: Record<string, unknown> = { sql: el<HTMLTextAreaElement>("#sql").value };
  if (asOf.value) payload.snapshot_id = asOf.value;
  const result = await busy("running SQL in DuckDB", () => command<QueryResult>("query", payload));
  if (!result) {
    el("#query-error").textContent = stepResult.textContent ?? "";
    return;
  }
  el("#query-meta").textContent =
    `${result.rows.length} rows in ${result.ms} ms · snapshot ${result.snapshot_id} · ` +
    `schema ${result.schema_id} · ${result.files_scanned} data file(s) scanned`;
  el("#query-result").innerHTML = table(result.columns, result.rows);
};

buttons.plan.onclick = async () => {
  el("#plan-error").textContent = "";
  const payload: Record<string, unknown> = { row_filter: el<HTMLInputElement>("#filter").value };
  if (asOf.value) payload.snapshot_id = asOf.value;
  const result = await busy("planning the scan", () => command<PlanResult>("plan", payload));
  if (!result) {
    el("#plan-error").textContent = stepResult.textContent ?? "";
    return;
  }
  el("#plan-meta").textContent =
    `${result.files_scanned} of ${result.files_total} files survive the filter · ` +
    `${result.rows_matched} rows match · ${result.ms} ms`;
  el("#plan-result").innerHTML = table(
    ["data file", "records", "size", "partition", "spec"],
    result.files.map((file) => [
      file.path,
      file.records,
      bytes(file.bytes),
      JSON.stringify(file.partition),
      file.spec_id,
    ]),
  );
};

// -- boot ---------------------------------------------------------------------------------------

void (async () => {
  const starting = boot.step("Loading CPython, PyIceberg and DuckDB into this tab");
  try {
    const info = await send<{ boot_ms?: number }>({ type: "boot" });
    const took = info.boot_ms ? `${(info.boot_ms / 1000).toFixed(1)} s` : undefined;
    starting.done(took);
    bootTime.textContent = took ? `booted in ${took}` : "booted";
    status.textContent = "idle";
    boot.ready(`PyIceberg and DuckDB are running in this tab${took ? ` · booted in ${took}` : ""}`);
    await refresh();
  } catch (error) {
    status.textContent = "boot failed";
    starting.failed("failed");
    boot.fail(`boot failed: ${(error as Error).message}`);
  }
})();
