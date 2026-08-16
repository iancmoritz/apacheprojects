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

// What the driver in the JVM sends back.  Java writes these shapes by hand in
// ../java/org/apacheprojects/sparkwasm/console/SparkConsole.java, because a JSON string is the one
// value CheerpJ converts between the JVM and JavaScript for free.

/** The classpath and dataset that scripts/build-runtime.mjs produced. */
export interface Manifest {
  sparkVersion: string;
  scalaVersion: string;
  distribution: string;
  source: string;
  classpath: string[];
  jarBytes: number;
  data: { path: string; view: string; rows: number; bytes: number };
}

export interface Failure {
  ok: false;
  error: string;
  trace?: string;
  durationMs?: number;
}

/** The driver, once `SparkSession.getOrCreate` has returned. */
export interface Session {
  ok: true;
  version: string;
  scalaVersion: string;
  javaVersion: string;
  master: string;
  cores: number;
  applicationId: string;
  view: string;
  source: string;
  schema: string[];
  bootMs: number;
}

/** One completed stage, as the driver's own listener bus reported it. */
export interface Stage {
  id: number;
  name: string;
  tasks: number;
  durationMs: number;
  executorRunTimeMs: number;
  recordsRead: number;
  recordsWritten: number;
  shuffleWriteBytes: number;
  shuffleReadBytes: number;
  peakMemoryBytes: number;
}

export interface Rows {
  ok: true;
  columns: string[];
  types: string[];
  rows: (string | null)[][];
  truncated: boolean;
  plan: string | null;
  stages: Stage[];
  durationMs: number;
}

export type QueryResult = Rows | Failure;
