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

/** Messages between the page and the Pyodide worker. */
export type WorkerRequest =
  | { id: number; type: "boot" }
  | { id: number; type: "call"; command: string; payload?: Record<string, unknown> };

/** ``Omit`` over a union, so each member keeps its own shape. */
export type Unidentified<T> = T extends { id: number } ? Omit<T, "id"> : never;

export type WorkerResponse =
  | { type: "progress"; text: string }
  | { type: "result"; id: number; result: unknown }
  | { type: "error"; id: number; error: string };

export interface Field {
  id: number;
  name: string;
  type: string;
  required: boolean;
  doc: string | null;
}

export interface Spec {
  spec_id: number;
  fields: { name: string; transform: string; source_id: number }[];
  is_current: boolean;
}

export interface Snapshot {
  snapshot_id: string;
  parent_id: string | null;
  sequence_number: number;
  timestamp_ms: number;
  operation: string | null;
  schema_id: number | null;
  manifest_list: string;
  summary: Record<string, string>;
  is_current: boolean;
}

export interface State {
  exists: boolean;
  warehouse: string;
  identifier?: string;
  location?: string;
  format_version?: number;
  metadata_location?: string;
  schema?: { schema_id: number; fields: Field[] };
  schemas?: { schema_id: number; fields: Field[] }[];
  specs?: Spec[];
  rows?: number;
  batches?: number;
  snapshots?: Snapshot[];
  snapshot_log?: { snapshot_id: string; timestamp_ms: number }[];
}

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  snapshot_id: string;
  schema_id: number;
  files_scanned: number;
  ms: number;
}

export interface PlanFile {
  path: string;
  records: number;
  bytes: number;
  partition: Record<string, unknown>;
  spec_id: number;
}

export interface PlanResult {
  row_filter: string;
  files_scanned: number;
  files_total: number;
  records_scanned: number;
  rows_matched: number;
  files: PlanFile[];
  ms: number;
}

export interface TreeFile {
  path: string;
  bytes: number;
  kind: string;
}

export interface TreeResult {
  root: string;
  files: TreeFile[];
  bytes: number;
}

export interface Preview {
  kind: string;
  path: string;
  ms: number;
  /** metadata files */
  json?: unknown;
  /** manifest lists and manifests */
  entries?: Record<string, unknown>[];
  /** Parquet data files */
  records?: number;
  row_groups?: number;
  bytes?: number;
  created_by?: string | null;
  columns?: { name: string; type: string; field_id: string | null }[];
  head?: { columns: string[]; rows: unknown[][] };
}

export interface StepResult {
  detail: string;
  ms: number;
}
