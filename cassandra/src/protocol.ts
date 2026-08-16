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

// What the page and a node worker say to each other.  Internode traffic is opaque here: the bytes
// in `frame` are a real serialized Cassandra Message, produced and consumed by Cassandra itself
// (see java/Node.java), and the page only routes them to the address the sender asked for.

/** Cassandra release the jars come from; kept in step with scripts/build-jars.mjs. */
export const CASSANDRA_VERSION = "4.1.12";

/** Sent to a worker. */
export type ToNode =
  | { type: "init"; index: number; nodes: number }
  | { type: "frame"; frame: ArrayBuffer }
  | { type: "cql"; id: string; cl: string; query: string }
  | { type: "pause"; paused: boolean };

/** Sent by a worker. */
export type FromNode =
  | { type: "boot"; index: number; stage: string; ms: number }
  | { type: "frame"; to: string; from: number; frame: ArrayBuffer }
  | { type: "node"; index: number; json: string }
  | { type: "exit"; index: number; code: number }
  | { type: "error"; index: number; error: string };

/** What java/Node.java reports about itself, all of it read out of Cassandra's own state. */
export type NodeStatus = {
  type: "status";
  mode: string;
  hostId: string;
  load: string;
  tokens: number;
  schema: string;
  live: string[];
  unreachable: string[];
  ownership: Record<string, number>;
};

export type NodeReady = { type: "ready"; ms: number };

export type CqlResult = {
  type: "result";
  id: string;
  ms: number;
  columns?: string[];
  rows?: (string | null)[][];
  kind?: string;
  error?: string;
};

export type NodeMessage = NodeStatus | NodeReady | CqlResult;

export const CONSISTENCY_LEVELS = ["ONE", "TWO", "QUORUM", "ALL", "ANY", "LOCAL_QUORUM"] as const;

/** Node n listens on 127.0.0.n:7000, so an address is all the routing information a frame needs. */
export const address = (index: number) => `127.0.0.${index}`;

/** The node an internode address belongs to, from `127.0.0.2:7000` and friends. */
export function nodeOf(hostAndPort: string): number | null {
  const match = /^127\.0\.0\.(\d+)/.exec(hostAndPort);
  return match ? Number(match[1]) : null;
}
