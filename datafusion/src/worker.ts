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

// DataFusion itself, in a Web Worker.  Everything the engine does is synchronous from JavaScript's
// point of view (see crates/workbench/src/lib.rs): a query blocks this thread until it is done,
// which is exactly why it is not the thread that draws the page.

import init, { Workbench } from "./generated/workbench";

import type { BootInfo, QueryResults, TableInfo, WorkerRequest, WorkerResponse } from "./protocol";

/** Where scripts/build-wasm.mjs puts the module and scripts/build-data.mjs puts the tables. */
const WASM_URL = "/_datafusion/workbench.wasm";
const DATA_URL = "/_datafusion/data/";
const BUNDLED_TABLES: { name: string; file: string; format: "csv" | "parquet" }[] = [
  { name: "cities", file: "cities.csv", format: "csv" },
  { name: "trips", file: "trips.parquet", format: "parquet" },
];

let workbench: Workbench | undefined;
let wasmBytes = 0;

function progress(text: string, loaded?: number, total?: number): void {
  self.postMessage({ type: "progress", text, loaded, total } satisfies WorkerResponse);
}

/** Fetch `url`, reporting bytes as they arrive: the module is tens of megabytes. */
async function download(url: string, what: string): Promise<Uint8Array> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: ${response.status} ${response.statusText}`);

  // Compressed on the wire, so Content-Length is the compressed size and the decoded stream can run
  // past it; it is still the only size the browser will tell us about before the end.
  const total = Number(response.headers.get("content-length")) || undefined;
  const reader = response.body?.getReader();
  if (!reader) return new Uint8Array(await response.arrayBuffer());

  const chunks: Uint8Array[] = [];
  let loaded = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    loaded += value.length;
    progress(`downloading ${what}`, loaded, total);
  }

  const bytes = new Uint8Array(loaded);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
}

async function boot(): Promise<BootInfo> {
  const started = performance.now();

  const module = await download(WASM_URL, "the DataFusion engine");
  wasmBytes = module.byteLength;
  progress("compiling the DataFusion engine");
  await init({ module_or_path: module });

  progress("starting a session");
  workbench = new Workbench();

  const tables: TableInfo[] = [];
  for (const table of BUNDLED_TABLES) {
    progress(`registering ${table.name}`);
    const bytes = await download(`${DATA_URL}${table.file}`, table.file);
    tables.push(register(table.name, table.format, bytes));
  }

  return {
    version: Workbench.dataFusionVersion(),
    tables,
    wasmBytes,
    ms: performance.now() - started,
  };
}

function engine(): Workbench {
  if (!workbench) throw new Error("the engine is not up yet");
  return workbench;
}

function register(name: string, format: "csv" | "parquet", bytes: Uint8Array): TableInfo {
  const json = format === "csv" ? engine().registerCsv(name, bytes) : engine().registerParquet(name, bytes);
  return JSON.parse(json) as TableInfo;
}

function query(sql: string, maxRows: number): QueryResults {
  const result = engine().sql(sql, maxRows);
  const results: QueryResults = {
    ipc: result.ipc,
    rows: result.rows,
    totalRows: result.totalRows,
    truncated: result.truncated,
    logicalPlan: result.logicalPlan,
    physicalPlan: result.physicalPlan,
    planMs: result.planMs,
    execMs: result.execMs,
  };
  result.free();
  return results;
}

async function handle(request: WorkerRequest): Promise<unknown> {
  switch (request.type) {
    case "boot":
      return boot();
    case "query":
      return query(request.sql, request.maxRows);
    case "tables":
      return JSON.parse(engine().tables()) as TableInfo[];
    case "register":
      return register(request.name, request.format, new Uint8Array(request.bytes));
  }
}

self.onmessage = async (event: MessageEvent<WorkerRequest>) => {
  const { id } = event.data;
  try {
    const result = await handle(event.data);
    const transfer = isQueryResults(result) ? [result.ipc.buffer as ArrayBuffer] : [];
    self.postMessage({ type: "result", id, result } satisfies WorkerResponse, transfer);
  } catch (error) {
    self.postMessage({ type: "error", id, error: message(error) } satisfies WorkerResponse);
  }
};

function isQueryResults(value: unknown): value is QueryResults {
  return typeof value === "object" && value !== null && "ipc" in value;
}

/** DataFusion's errors arrive as `Error`; a panic in the module arrives as anything at all. */
function message(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}
