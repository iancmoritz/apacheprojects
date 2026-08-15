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

/** Messages between the page and the worker the engine runs in. */
export type WorkerRequest =
  | { id: number; type: "boot" }
  | { id: number; type: "query"; sql: string; maxRows: number }
  | { id: number; type: "tables" }
  | { id: number; type: "register"; name: string; format: TableFormat; bytes: ArrayBuffer };

/** ``Omit`` over a union, so each member keeps its own shape. */
export type Unidentified<T> = T extends { id: number } ? Omit<T, "id"> : never;

export type WorkerResponse =
  | { type: "progress"; text: string; loaded?: number; total?: number }
  | { type: "result"; id: number; result: unknown }
  | { type: "error"; id: number; error: string };

export type TableFormat = "csv" | "parquet";

export interface Column {
  name: string;
  type: string;
  nullable: boolean;
}

export interface TableInfo {
  name: string;
  rows: number;
  columns: Column[];
}

export interface BootInfo {
  /** The DataFusion release compiled into the module. */
  version: string;
  tables: TableInfo[];
  /** Bytes of wasm the browser downloaded, and how long the whole boot took. */
  wasmBytes: number;
  ms: number;
}

export interface QueryResults {
  /** The rows, as an Arrow IPC stream. */
  ipc: Uint8Array;
  rows: number;
  totalRows: number;
  truncated: boolean;
  logicalPlan: string;
  physicalPlan: string;
  planMs: number;
  execMs: number;
}
