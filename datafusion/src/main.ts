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

// The page: an editor, the tables the engine knows about, and whatever the last query returned.  It
// holds no engine state of its own -- every query goes to the worker, and results come back as Arrow
// IPC, which is what this file turns into a table.

import { DataType, tableFromIPC, type Table } from "apache-arrow";

import { BootPanel } from "../../design/boot";

import "./datafusion.css";
import { EXAMPLES } from "./examples";
import type { BootInfo, QueryResults, TableInfo, Unidentified, WorkerRequest, WorkerResponse } from "./protocol";

/** Rows kept out of a result set; the engine slices the rest away before encoding anything. */
const MAX_ROWS = 1_000;

const worker = new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
const pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
let nextId = 1;

const status = element<HTMLSpanElement>("#status");
const engine = element<HTMLSpanElement>("#engine");
const editor = element<HTMLTextAreaElement>("#editor");
const timing = element<HTMLSpanElement>("#timing");
const tablesPane = element<HTMLDivElement>("#tables");
const panes = {
  results: element<HTMLDivElement>("#pane-results"),
  physical: element<HTMLDivElement>("#pane-physical"),
  logical: element<HTMLDivElement>("#pane-logical"),
};

function element<T extends Element>(selector: string): T {
  const found = document.querySelector<T>(selector);
  if (!found) throw new Error(`${selector} is missing from the page`);
  return found;
}

const boot = new BootPanel({
  mount: element<HTMLElement>("#boot"),
  title: "Starting Apache DataFusion",
  detail:
    "Nothing here runs on a server. The Rust query engine is compiled to WebAssembly and is being " +
    "fetched into this tab, along with the CSV and Parquet it registers as tables.",
});

worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
  const message = event.data;
  if (message.type === "progress") {
    status.textContent = message.text;
    boot.progress(message.loaded, message.total);
    if (message.loaded === undefined) {
      boot.now(message.text);
      boot.say(message.text);
    } else {
      // A download reports itself many times over; the line above the steps moves, the log does not.
      status.textContent = `${message.text} — ${megabytes(message.loaded)}`;
      boot.now(`${message.text} — ${megabytes(message.loaded)}`);
    }
    return;
  }
  const settle = pending.get(message.id);
  if (!settle) return;
  pending.delete(message.id);
  if (message.type === "error") settle.reject(new Error(message.error));
  else settle.resolve(message.result);
};

function call<T>(request: Unidentified<WorkerRequest>, transfer: Transferable[] = []): Promise<T> {
  const id = nextId++;
  return new Promise<T>((resolve, reject) => {
    pending.set(id, { resolve: resolve as (value: unknown) => void, reject });
    worker.postMessage({ ...request, id } as WorkerRequest, transfer);
  });
}

function megabytes(bytes: number): string {
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} kB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function milliseconds(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)} s` : `${ms.toFixed(1)} ms`;
}

// -- the sidebar ------------------------------------------------------------------------------

function renderTables(tables: TableInfo[]): void {
  tablesPane.replaceChildren();
  for (const table of tables) {
    const item = document.createElement("div");
    item.className = "table";

    const name = document.createElement("button");
    name.type = "button";
    name.textContent = table.name;
    name.title = `SELECT * FROM ${table.name}`;
    name.addEventListener("click", () => {
      editor.value = `SELECT * FROM ${table.name} LIMIT 100;`;
      void run();
    });
    const rows = document.createElement("span");
    rows.className = "rows";
    rows.textContent = ` ${table.rows.toLocaleString()} rows`;
    name.append(rows);

    const schema = document.createElement("dl");
    for (const column of table.columns) {
      const term = document.createElement("dt");
      term.textContent = column.name;
      const type = document.createElement("dd");
      type.textContent = column.type;
      schema.append(term, type);
    }

    item.append(name, schema);
    tablesPane.append(item);
  }
}

function renderExamples(): void {
  const list = element<HTMLDivElement>("#examples");
  for (const example of EXAMPLES) {
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = example.title;
    button.title = example.sql;
    button.addEventListener("click", () => {
      editor.value = example.sql;
      void run();
    });
    list.append(button);
  }
}

// -- results ----------------------------------------------------------------------------------

/** One cell, formatted from the Arrow value rather than from a string the engine made up. */
function cell(value: unknown, type: DataType): { text: string; numeric: boolean } {
  if (value === null || value === undefined) return { text: "NULL", numeric: false };
  if (DataType.isTimestamp(type) || DataType.isDate(type)) {
    const ms = typeof value === "bigint" ? Number(value) : Number(value);
    return { text: new Date(ms).toISOString().replace("T", " ").replace(".000Z", ""), numeric: false };
  }
  if (typeof value === "bigint") return { text: value.toString(), numeric: true };
  if (typeof value === "number") {
    const text = Number.isInteger(value) ? value.toLocaleString("en-US") : value.toFixed(4).replace(/0+$/, "0");
    return { text, numeric: true };
  }
  if (value instanceof Date) {
    return { text: value.toISOString().replace("T", " ").replace(".000Z", ""), numeric: false };
  }
  if (typeof value === "object") return { text: JSON.stringify(value), numeric: false };
  return { text: String(value), numeric: false };
}

function renderResults(table: Table, results: QueryResults): void {
  const fields = table.schema.fields;
  const grid = document.createElement("table");
  grid.className = "grid";

  const head = document.createElement("thead");
  const headRow = document.createElement("tr");
  for (const field of fields) {
    const cell = document.createElement("th");
    cell.textContent = field.name;
    const type = document.createElement("span");
    type.className = "type";
    type.textContent = String(field.type);
    cell.append(type);
    headRow.append(cell);
  }
  head.append(headRow);

  const body = document.createElement("tbody");
  const columns = fields.map((_, index) => table.getChildAt(index));
  for (let row = 0; row < table.numRows; row++) {
    const line = document.createElement("tr");
    for (let column = 0; column < fields.length; column++) {
      const rendered = cell(columns[column]?.get(row), fields[column].type);
      const td = document.createElement("td");
      td.textContent = rendered.text;
      if (rendered.text === "NULL") td.className = "null";
      else if (rendered.numeric) td.className = "num";
      line.append(td);
    }
    body.append(line);
  }
  grid.append(head, body);

  const summary = document.createElement("p");
  summary.className = "note";
  summary.textContent = results.truncated
    ? `${results.totalRows.toLocaleString()} rows, showing the first ${results.rows.toLocaleString()}`
    : `${results.totalRows.toLocaleString()} row${results.totalRows === 1 ? "" : "s"}`;

  panes.results.replaceChildren(summary, grid);
}

/** Plans are indented text; the operator names and the metrics are what the eye wants first. */
function renderPlan(pane: HTMLDivElement, plan: string): void {
  const block = document.createElement("pre");
  block.className = "plan";
  for (const line of plan.split("\n")) {
    const match = /^(\s*)([A-Za-z_]+(?:Exec|Executor)?)(.*)$/.exec(line);
    if (!match) {
      block.append(`${line}\n`);
      continue;
    }
    const [, indent, operator, rest] = match;
    const name = document.createElement("span");
    name.className = "op";
    name.textContent = operator;
    block.append(indent, name);
    // metrics=[...] at the end of an EXPLAIN ANALYZE line is the interesting half of that line.
    const metrics = /^(.*?)(, metrics=\[.*)$/.exec(rest);
    if (metrics) {
      const dim = document.createElement("span");
      dim.className = "metric";
      dim.textContent = metrics[2];
      block.append(metrics[1], dim, "\n");
    } else {
      block.append(`${rest}\n`);
    }
  }
  pane.replaceChildren(block);
}

function selectPane(name: keyof typeof panes): void {
  for (const button of document.querySelectorAll<HTMLButtonElement>("#tabs button")) {
    button.ariaSelected = String(button.dataset.pane === name);
  }
  for (const [key, pane] of Object.entries(panes)) pane.hidden = key !== name;
}

// -- running queries --------------------------------------------------------------------------

let running = false;

async function run(sql = editor.value): Promise<void> {
  const statement = sql.trim().replace(/;\s*$/, "");
  if (!statement || running) return;
  running = true;
  for (const button of document.querySelectorAll<HTMLButtonElement>(".toolbar button")) button.disabled = true;
  timing.textContent = "running…";

  const started = performance.now();
  try {
    const results = await call<QueryResults>({ type: "query", sql: statement, maxRows: MAX_ROWS });
    renderResults(tableFromIPC(results.ipc), results);
    renderPlan(panes.physical, results.physicalPlan);
    renderPlan(panes.logical, results.logicalPlan);
    timing.textContent =
      `plan ${milliseconds(results.planMs)} · execute ${milliseconds(results.execMs)} · ` +
      `round trip ${milliseconds(performance.now() - started)}`;
    selectPane("results");
  } catch (error) {
    const failure = document.createElement("p");
    failure.className = "error";
    failure.textContent = (error as Error).message;
    panes.results.replaceChildren(failure);
    timing.textContent = "failed";
    selectPane("results");
  } finally {
    running = false;
    for (const button of document.querySelectorAll<HTMLButtonElement>(".toolbar button")) button.disabled = false;
    editor.focus();
  }
}

/** `EXPLAIN`/`EXPLAIN ANALYZE` of whatever is in the editor, without editing the editor. */
function explain(prefix: "EXPLAIN" | "EXPLAIN ANALYZE"): void {
  const statement = editor.value.trim().replace(/;\s*$/, "");
  if (!statement) return;
  void run(`${prefix} ${statement.replace(/^EXPLAIN(\s+ANALYZE)?\s+/i, "")}`);
}

// -- files ------------------------------------------------------------------------------------

/** A table name a SQL parser will accept, derived from a file name. */
function tableName(file: string): string {
  const stem = file.replace(/\.[^.]+$/, "").toLowerCase();
  const cleaned = stem.replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
  return /^[a-z_]/.test(cleaned) ? cleaned : `t_${cleaned}`;
}

async function addFiles(files: Iterable<File>): Promise<void> {
  for (const file of files) {
    const format = /\.(parquet|pq)$/i.test(file.name) ? "parquet" : "csv";
    status.textContent = `registering ${file.name}`;
    try {
      const bytes = await file.arrayBuffer();
      const table = await call<TableInfo>(
        { type: "register", name: tableName(file.name), format, bytes },
        [bytes],
      );
      status.textContent = `${table.name}: ${table.rows.toLocaleString()} rows from ${file.name}`;
      editor.value = `SELECT * FROM ${table.name} LIMIT 100;`;
      await refreshTables();
      await run();
    } catch (error) {
      status.textContent = `${file.name}: ${(error as Error).message}`;
    }
  }
}

let known: TableInfo[] = [];

async function refreshTables(): Promise<void> {
  known = await call<TableInfo[]>({ type: "tables" }).catch(() => known);
  renderTables(known);
}

// -- boot -------------------------------------------------------------------------------------

function wireUp(): void {
  element<HTMLButtonElement>("#run").addEventListener("click", () => void run());
  element<HTMLButtonElement>("#explain").addEventListener("click", () => explain("EXPLAIN"));
  element<HTMLButtonElement>("#analyze").addEventListener("click", () => explain("EXPLAIN ANALYZE"));

  editor.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      void run();
    }
  });

  for (const button of document.querySelectorAll<HTMLButtonElement>("#tabs button")) {
    button.addEventListener("click", () => selectPane(button.dataset.pane as keyof typeof panes));
  }

  const drop = element<HTMLDivElement>("#drop");
  const input = element<HTMLInputElement>("#file");
  input.addEventListener("change", () => {
    if (input.files) void addFiles(input.files);
    input.value = "";
  });
  for (const type of ["dragenter", "dragover"]) {
    drop.addEventListener(type, (event) => {
      event.preventDefault();
      drop.classList.add("over");
    });
  }
  for (const type of ["dragleave", "dragend"]) {
    drop.addEventListener(type, () => drop.classList.remove("over"));
  }
  drop.addEventListener("drop", (event) => {
    event.preventDefault();
    drop.classList.remove("over");
    if (event.dataTransfer?.files.length) void addFiles(event.dataTransfer.files);
  });
  // A file dropped anywhere else would otherwise navigate away from the page.
  document.addEventListener("dragover", (event) => event.preventDefault());
  document.addEventListener("drop", (event) => event.preventDefault());
}

async function main(): Promise<void> {
  wireUp();
  renderExamples();
  editor.value = EXAMPLES[0].sql;

  const starting = boot.step("Fetching the engine and registering the bundled tables");
  const info = await call<BootInfo>({ type: "boot" });
  starting.done(`${info.tables.length} tables`);
  known = info.tables;
  renderTables(known);
  status.textContent = `ready in ${milliseconds(info.ms)}`;
  engine.textContent = `DataFusion ${info.version} · ${megabytes(info.wasmBytes)} of wasm`;
  boot.ready(
    `DataFusion ${info.version} is running in this tab · ${megabytes(info.wasmBytes)} of wasm in ` +
      milliseconds(info.ms),
  );
  await run();
}

void main().catch((error: Error) => {
  status.textContent = "failed to start";
  boot.fail(error.message);
});
