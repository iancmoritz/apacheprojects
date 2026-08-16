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

// The page is the network: it starts one worker per node, routes Cassandra's serialized internode
// messages between them by the address Cassandra addressed them to, and speaks CQL to whichever node
// the visitor picks.  It deliberately keeps no cluster state of its own -- everything in the
// nodetool view comes out of StorageService on the node that reported it.

import {
  CASSANDRA_VERSION,
  CONSISTENCY_LEVELS,
  nodeOf,
  type CqlResult,
  type NodeMessage,
  type NodeStatus,
} from "./protocol";

type Node = {
  index: number;
  worker: Worker;
  state: "booting" | "up" | "paused" | "killed" | "failed";
  stage: string;
  bootMs?: number;
  status?: NodeStatus;
  error?: string;
};

const el = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const stage = el<HTMLSpanElement>("stage");
const countSelect = el<HTMLSelectElement>("count");
const startButton = el<HTMLButtonElement>("start");
const clusterSection = el<HTMLElement>("cluster");
const clusterEmpty = el<HTMLParagraphElement>("clusterEmpty");
const targetSelect = el<HTMLSelectElement>("target");
const clSelect = el<HTMLSelectElement>("cl");
const runButton = el<HTMLButtonElement>("run");
const demoButton = el<HTMLButtonElement>("demo");
const queryInput = el<HTMLTextAreaElement>("query");
const resultBox = el<HTMLDivElement>("result");
const logBox = el<HTMLPreElement>("log");

const nodes = new Map<number, Node>();
const pending = new Map<string, (result: CqlResult) => void>();
let nextId = 0;
let clusterStarted = 0;
let nodeCount = 1;
let peersStarted = false;

function log(line: string, cls?: "err" | "ok") {
  const at = ((Date.now() - (clusterStarted || Date.now())) / 1000).toFixed(1).padStart(6);
  const span = document.createElement("span");
  if (cls) span.className = cls;
  span.textContent = `[${at}s] ${line}\n`;
  logBox.append(span);
  logBox.scrollTop = logBox.scrollHeight;
}

/** How many JVMs to offer.  Three is the point of the demo; less memory means fewer fit. */
function suggestedCount(): number {
  const memory = (navigator as Navigator & { deviceMemory?: number }).deviceMemory;
  const cores = navigator.hardwareConcurrency || 4;
  if ((memory !== undefined && memory < 8) || cores < 4) return 2;
  return 3;
}

for (const n of [1, 2, 3, 4, 5]) {
  const option = document.createElement("option");
  option.value = String(n);
  option.textContent = `${n}`;
  countSelect.append(option);
}
countSelect.value = String(suggestedCount());

for (const level of CONSISTENCY_LEVELS) {
  const option = document.createElement("option");
  option.value = level;
  option.textContent = level;
  clSelect.append(option);
}
clSelect.value = "QUORUM";

function renderCluster() {
  clusterEmpty.hidden = nodes.size > 0;
  for (const old of clusterSection.querySelectorAll(".node")) old.remove();

  for (const node of [...nodes.values()].sort((a, b) => a.index - b.index)) {
    const div = document.createElement("div");
    div.className = "node";

    const title = document.createElement("h3");
    const name = document.createElement("span");
    name.textContent = `node ${node.index} — 127.0.0.${node.index}`;
    const pill = document.createElement("span");
    pill.className = `pill ${node.state === "up" ? "up" : node.state === "booting" ? "" : "down"}`;
    pill.textContent = node.state === "up" ? (node.status?.mode ?? "UP") : node.state;
    title.append(name, pill);

    const controls = document.createElement("div");
    controls.className = "controls";
    if (node.state === "up" || node.state === "paused") {
      const pause = document.createElement("button");
      pause.type = "button";
      pause.textContent = node.state === "paused" ? "Resume" : "Pause";
      pause.onclick = () => setPaused(node, node.state !== "paused");
      const kill = document.createElement("button");
      kill.type = "button";
      kill.textContent = "Kill";
      kill.onclick = () => kill_(node);
      controls.append(pause, kill);
    }
    title.append(controls);
    div.append(title);

    const dl = document.createElement("dl");
    const add = (key: string, value: string) => {
      const dt = document.createElement("dt");
      dt.textContent = key;
      const dd = document.createElement("dd");
      dd.textContent = value;
      dl.append(dt, dd);
    };
    const status = node.status;
    if (node.error) add("error", node.error);
    if (status) {
      const own = status.ownership[`127.0.0.${node.index}`];
      add("host id", status.hostId);
      add("load", status.load);
      add("tokens", String(status.tokens));
      add("owns", own === undefined ? "—" : `${(own * 100).toFixed(1)}%`);
      add("schema", status.schema.slice(0, 8));
      add("sees up", status.live.join(" ") || "—");
      if (status.unreachable.length) add("sees down", status.unreachable.join(" "));
    } else if (node.state === "booting") {
      add("booting", `${node.stage} (${((Date.now() - clusterStarted) / 1000).toFixed(0)}s)`);
    }
    if (node.bootMs) add("booted in", `${(node.bootMs / 1000).toFixed(1)}s`);
    div.append(dl);
    clusterSection.append(div);
  }

  const selected = targetSelect.value;
  targetSelect.replaceChildren();
  for (const node of [...nodes.values()].sort((a, b) => a.index - b.index)) {
    if (node.state !== "up") continue;
    const option = document.createElement("option");
    option.value = String(node.index);
    option.textContent = `node ${node.index}`;
    targetSelect.append(option);
  }
  if (selected && targetSelect.querySelector(`option[value="${selected}"]`)) {
    targetSelect.value = selected;
  }
  const anyUp = targetSelect.options.length > 0;
  const booting = [...nodes.values()].some((node) => node.state === "booting") || !peersStarted;
  runButton.disabled = !anyUp;
  // The demo picks its replication factor from the nodes that are up, so let the cluster finish.
  demoButton.disabled = !anyUp || booting;
}

function handle(node: Node, message: NodeMessage) {
  if (message.type === "ready") {
    node.state = "up";
    node.bootMs = message.ms;
    log(`node ${node.index} is NORMAL after ${(message.ms / 1000).toFixed(1)}s`, "ok");
    stage.textContent = `${[...nodes.values()].filter((n) => n.state === "up").length}/${nodes.size} nodes up`;
    if (node.index === 1) startPeers();
  } else if (message.type === "status") {
    if (node.state === "booting") node.state = "up";
    node.status = message;
  } else if (message.type === "result") {
    const resolve = pending.get(message.id);
    if (resolve) {
      pending.delete(message.id);
      resolve(message);
    }
  }
  renderCluster();
}

function spawn(index: number, total: number): Node {
  const worker = new Worker(new URL("./node.worker.ts", import.meta.url), {
    name: `cassandra-node-${index}`,
  });
  const node: Node = { index, worker, state: "booting", stage: "loading CheerpJ" };

  worker.onmessage = (event: MessageEvent) => {
    const message = event.data;
    if (message.type === "frame") {
      // Cassandra addressed this to 127.0.0.n:7000; deliver it, or drop it exactly as a dropped
      // packet would be, and let the failure detector notice.
      const to = nodeOf(message.to);
      const peer = to === null ? undefined : nodes.get(to);
      if (peer && (peer.state === "up" || peer.state === "booting" || peer.state === "paused")) {
        peer.worker.postMessage({ type: "frame", frame: message.frame }, [message.frame]);
      }
    } else if (message.type === "boot") {
      node.stage = message.stage;
      if (node.state === "booting") stage.textContent = `node ${index}: ${message.stage}`;
      renderCluster();
    } else if (message.type === "node") {
      handle(node, JSON.parse(message.json) as NodeMessage);
    } else if (message.type === "error" || message.type === "exit") {
      node.state = "failed";
      node.error = message.type === "error" ? message.error : `exited with ${message.code}`;
      log(`node ${index} ${node.error}`, "err");
      renderCluster();
    }
  };
  worker.onerror = (event) => {
    node.state = "failed";
    node.error = event.message;
    log(`node ${index} worker error: ${event.message}`, "err");
    renderCluster();
  };

  const verbs = new URLSearchParams(location.search).get("verbs") === "1";
  worker.postMessage({ type: "init", index, nodes: total, verbs });
  return node;
}

function setPaused(node: Node, paused: boolean) {
  node.worker.postMessage({ type: "pause", paused });
  node.state = paused ? "paused" : "up";
  log(`node ${node.index} ${paused ? "paused (stops answering, keeps its data)" : "resumed"}`);
  renderCluster();
}

function kill_(node: Node) {
  node.worker.terminate();
  node.state = "killed";
  node.status = undefined;
  log(`node ${node.index} killed`, "err");
  renderCluster();
}

/** Runs a statement on a node and waits for that node's answer. */
function cql(index: number, cl: string, query: string): Promise<CqlResult> {
  const node = nodes.get(index);
  if (!node || (node.state !== "up" && node.state !== "paused")) {
    return Promise.resolve({ type: "result", id: "", ms: 0, error: `node ${index} is not up` });
  }
  const id = String(++nextId);
  const promise = new Promise<CqlResult>((resolve) => pending.set(id, resolve));
  node.worker.postMessage({ type: "cql", id, cl, query });
  return promise;
}

function renderResult(result: CqlResult) {
  resultBox.replaceChildren();
  if (result.error) return;
  if (!result.columns) return;
  const table = document.createElement("table");
  const head = document.createElement("tr");
  for (const column of result.columns) {
    const th = document.createElement("th");
    th.textContent = column;
    head.append(th);
  }
  table.append(head);
  for (const row of result.rows ?? []) {
    const tr = document.createElement("tr");
    for (const cell of row) {
      const td = document.createElement("td");
      td.textContent = cell ?? "null";
      tr.append(td);
    }
    table.append(tr);
  }
  resultBox.append(table);
}

async function execute(index: number, cl: string, query: string): Promise<CqlResult> {
  log(`node ${index} ${cl} > ${query.replace(/\s+/g, " ").trim()}`);
  const result = await cql(index, cl, query);
  if (result.error) {
    log(`  ${result.error}`, "err");
  } else if (result.columns) {
    log(`  ${result.rows?.length ?? 0} row(s) in ${result.ms}ms`, "ok");
    for (const row of result.rows ?? []) log(`    ${row.map((c) => c ?? "null").join(" | ")}`);
  } else {
    log(`  ${result.kind} in ${result.ms}ms`, "ok");
  }
  renderResult(result);
  return result;
}

/**
 * A new table reaches the other nodes by schema push/pull, not instantly, and a demo that races it
 * just sees `Unknown CF`.  Reading the table on every node is the honest check: it only answers once
 * that node has applied the schema and built the table locally.
 */
async function waitForTable(keyspace: string, table: string, timeoutMs = 120_000) {
  const probe = `SELECT * FROM ${keyspace}.${table} LIMIT 1`;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const up = [...nodes.values()].filter((node) => node.state === "up");
    const seen = await Promise.all(up.map((node) => cql(node.index, "ONE", probe)));
    const missing = up.filter((_, i) => seen[i].error !== undefined).map((node) => node.index);
    if (missing.length === 0) {
      log(`  every node has ${keyspace}.${table}`, "ok");
      return true;
    }
    log(`  node(s) ${missing.join(", ")} have not applied ${keyspace}.${table} yet…`);
    await new Promise((resolve) => setTimeout(resolve, 3000));
  }
  log(`  ${keyspace}.${table} did not reach every node in time`, "err");
  return false;
}

async function demo() {
  demoButton.disabled = true;
  const up = [...nodes.values()].filter((node) => node.state === "up").map((node) => node.index);
  const rf = Math.min(3, up.length);
  const [first, second = first, third = first] = up;
  try {
    await execute(
      first,
      "ONE",
      `CREATE KEYSPACE IF NOT EXISTS demo WITH replication = ` +
        `{'class': 'SimpleStrategy', 'replication_factor': ${rf}}`,
    );
    await execute(first, "ONE", "CREATE TABLE IF NOT EXISTS demo.t (k text PRIMARY KEY, v text)");
    log("waiting for the schema to reach every node…");
    await waitForTable("demo", "t");

    // A worker that is still replaying its own startup can miss the QUORUM deadline; the retry is
    // the same thing you would do with cqlsh, not a way to paper over a failure.
    let write = await execute(first, "QUORUM", "INSERT INTO demo.t (k, v) VALUES ('a', 'written at QUORUM')");
    for (let attempt = 0; write.error && attempt < 3; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 5000));
      write = await execute(first, "QUORUM", "INSERT INTO demo.t (k, v) VALUES ('a', 'written at QUORUM')");
    }
    const all = await execute(second, "ALL", "SELECT k, v FROM demo.t WHERE k = 'a'");
    const one = await execute(third, "ONE", "SELECT k, v FROM demo.t WHERE k = 'a'");

    if (!write.error && (all.rows?.length ?? 0) > 0 && (one.rows?.length ?? 0) > 0) {
      log(`the row was written once on node ${first} at QUORUM and read back from node ${second} ` +
        `at ALL and node ${third} at ONE: ${rf} replicas, chosen by Cassandra's token ring.`, "ok");
      log("now kill a node and try ALL — Cassandra will raise UnavailableException, " +
        "while QUORUM keeps working.");
    } else {
      log("the demo did not complete: read the errors above — they are Cassandra's own, " +
        "usually a node still too busy booting to answer in time. Try again in a minute.", "err");
    }
  } finally {
    demoButton.disabled = false;
  }
}

/**
 * Node 1 is the seed, and a joining node gives up after a 20s shadow gossip round, so the peers only
 * start once the seed is answering gossip — the same "seeds first" rule as a cluster on real hosts,
 * and it matters more here because a JVM in a Web Worker takes minutes to get there.
 */
function startPeers() {
  if (peersStarted) return;
  peersStarted = true;
  if (nodeCount < 2) return;
  log(`seed is up; starting node(s) 2–${nodeCount}`);
  for (let index = 2; index <= nodeCount; index++) nodes.set(index, spawn(index, nodeCount));
  renderCluster();
}

startButton.onclick = () => {
  startButton.disabled = true;
  countSelect.disabled = true;
  nodeCount = Number(countSelect.value);
  clusterStarted = Date.now();
  log(`starting ${nodeCount} Cassandra ${CASSANDRA_VERSION} node(s), one JVM per Web Worker`);
  log("a cold start replays a real Cassandra startup: the seed takes ~2-4 minutes, then the other " +
    "nodes join");
  nodes.set(1, spawn(1, nodeCount));
  renderCluster();
  setInterval(renderCluster, 1000);
};

runButton.onclick = () => {
  void execute(Number(targetSelect.value), clSelect.value, queryInput.value.trim());
};
demoButton.onclick = () => void demo();
el<HTMLButtonElement>("clear").onclick = () => logBox.replaceChildren();

renderCluster();
