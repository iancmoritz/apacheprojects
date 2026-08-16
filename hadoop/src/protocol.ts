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
// ../java/org/apacheprojects/hadoopwasm/console/, because a JSON string is the one value CheerpJ
// converts between the JVM and JavaScript for free.

/** The classpath and input files that scripts/build-runtime.mjs produced. */
export interface Manifest {
  hadoopVersion: string;
  source: string;
  classpath: string[];
  jarBytes: number;
  data: { name: string; bytes: number; lines: number }[];
}

export interface Failure {
  ok: false;
  error: string;
  trace?: string;
  durationMs?: number;
}

/** One DataNode, as the NameNode's own `DatanodeInfo` report describes it. */
export interface Datanode {
  name: string;
  uuid: string;
  state: string;
  capacity: number;
  dfsUsed: number;
  remaining: number;
  blockPoolUsed: number;
  xceivers: number;
  lastUpdateMs: number;
}

/** The NameNode's storage: its own totals, and the files it and the DataNode wrote. */
export interface Storage {
  blockPoolId: string;
  clusterId: string;
  filesTotal: number;
  blocksTotal: number;
  capacityUsed: number;
  lastWrittenTransactionId: number;
  transactionsSinceLastCheckpoint: number;
  files: { path: string; bytes: number }[];
}

/**
 * What the run is talking to: a MiniDFSCluster once `waitActive` has returned, or -- in `local` mode
 * -- Hadoop's LocalFileSystem, which has no NameNode, no DataNode and no block storage to report.
 */
export interface Cluster {
  ok: true;
  mode?: "local";
  hadoopVersion: string;
  javaVersion: string;
  uri: string;
  user: string;
  cores: number;
  fileSystem?: string;
  workingDirectory?: string;
  nameNodeAddress?: string;
  safeMode?: boolean;
  datanodes?: Datanode[];
  storage?: Storage;
  blockSize: number;
  replication: number;
  bootMs: number;
}

/** One entry of an `ls`, from `FileStatus`. */
export interface Entry {
  path: string;
  name: string;
  directory: boolean;
  length: number;
  replication: number;
  blockSize: number;
  blocks: number;
  owner: string;
  group: string;
  permission: string;
  modified: number;
}

export interface ShellResult {
  ok: true;
  command: string;
  output: string;
  listing?: Entry[];
  durationMs: number;
}

/** One input split, from `TextInputFormat.getSplits` against the NameNode's block locations. */
export interface Split {
  path: string;
  start: number;
  length: number;
  hosts: string[];
  type: string;
}

export interface Submitted {
  ok: true;
  jobId: string;
  jobName: string;
  trackingUrl: string;
  splits: Split[];
}

export interface CounterGroup {
  name: string;
  id: string;
  counters: { name: string; display: string; value: number }[];
}

export interface TaskReport {
  type: string;
  id: string;
  state: string;
  progress: number;
  startTime: number;
  finishTime: number;
}

/** A WordCount run, as the JVM reported it while the job was going. */
export interface JobResult {
  ok: true;
  submitted: Submitted | Failure;
  timeline: Progress[];
}

/** One command of a run: an HDFS shell command, or the MapReduce job. */
export interface Step {
  command: string;
  result: ShellResult | JobResult | Failure;
}

/**
 * A whole run: the cluster this call started, every command it then executed, and the shutdown that
 * lets the page call in again. See HadoopConsole#session.
 */
export interface Session {
  ok: true;
  cluster: Cluster;
  steps: Step[];
  ms: number;
}

export interface Progress {
  ok: true;
  jobId: string;
  complete: boolean;
  successful: boolean;
  state: string;
  mapProgress: number;
  reduceProgress: number;
  setupProgress: number;
  elapsedMs: number;
  counters: CounterGroup[];
  tasks: TaskReport[];
  failure?: string | null;
  output?: {
    files?: { path: string; bytes: number }[];
    rows?: { word: string; count: number }[];
    rowCount?: number;
    path?: string;
    error?: string;
  };
}
