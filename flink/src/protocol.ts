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

// What the cluster in the JVM sends back.  Java writes these shapes by hand in
// ../java/org/apacheprojects/flinkwasm/console/FlinkConsole.java, because a JSON string is the one
// value CheerpJ converts between the JVM and JavaScript for free.

/** The classpath scripts/build-runtime.mjs produced. */
export interface Manifest {
  flinkVersion: string;
  scalaVersion: string;
  distribution: string;
  source: string;
  classpath: string[];
  jarBytes: number;
}

export interface Failure {
  ok: false;
  error: string;
  trace?: string;
  durationMs?: number;
}

/** The MiniCluster, once its JobManager and TaskManager are up. */
export interface Cluster {
  ok: true;
  flinkVersion: string;
  commit: string;
  javaVersion: string;
  jvm: string;
  cores: number;
  slots: number;
  taskManagers: number;
  stateBackend: string;
  checkpointDir: string;
  bootMs: number;
}

/** One vertex of the JobGraph, as the graph was submitted. */
export interface JobVertex {
  id: string;
  name: string;
  parallelism: number;
  operators: number;
  inputs: number;
}

export interface Submitted {
  ok: true;
  jobId: string;
  jobName: string;
  /** Flink's own `StreamGraph` plan JSON: the same document the dashboard draws. */
  plan: unknown;
  vertices: JobVertex[];
  submitMs: number;
}

/** One response from the JobManager's REST endpoint, relayed out of the JVM. */
export interface RestResponse {
  ok: true;
  status: number;
  contentType: string;
  /** The body, base64: it is often a font or a gzipped bundle, not text. */
  body: string;
}

/** One subtask attempt of a vertex, from the ExecutionGraph. */
export interface Subtask {
  index: number;
  attempt: number;
  state: string;
  startedAt: number;
  /** The exception that killed this attempt, as the JobMaster recorded it. */
  failure: string | null;
}

export interface ExecutionVertex {
  id: string;
  name: string;
  parallelism: number;
  state: string;
  subtasks: Subtask[];
}

export interface Checkpoint {
  id: number;
  status: string;
  triggerTimestamp: number;
  durationMs: number;
  stateSizeBytes: number;
  acknowledged: number;
  subtasks: number;
  path: string | null;
}

export interface Checkpoints {
  completed: number;
  failed: number;
  inProgress: number;
  total: number;
  latest?: Checkpoint;
  history: Checkpoint[];
}

/** One window the job closed, as its sink emitted it. */
export interface Window {
  windowStart: number;
  windowEnd: number;
  word: string;
  count: number;
  emittedAt: number;
}

/** Something worth a line in the page's event log: a late record, a checkpoint, a generator change. */
export interface Note {
  kind: string;
  detail: string;
  at: number;
}

export interface Poll {
  ok: true;
  windows: Window[];
  notes: Note[];
  ingested: number;
  emitted: number;
  late: number;
  watermark: number;
  maxEventTime: number;
  rate: number;
  skewPercent: number;
  now: number;
  jobId?: string;
  jobName?: string;
  state?: string;
  /** The root failure of the job, once it has one. */
  failure?: string | null;
  runningMs?: number;
  graph?: ExecutionVertex[];
  checkpoints?: Checkpoints;
  pollMs: number;
}

export interface JobOptions {
  windowSeconds: number;
  lateness: number;
  outOfOrderness: number;
  checkpointInterval: number;
  parallelism: number;
}
