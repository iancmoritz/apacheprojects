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

// Boots a real Apache Kafka broker in the tab and drives it from the page.  The JVM is CheerpJ; the
// jars under /_kafka/jars are the ones from the official Kafka 3.9.1 release; every operation below
// is a call into Kafka's own StorageTool / KafkaRaftServer / Admin / KafkaProducer / KafkaConsumer
// through the KafkaBrowser facade in java/.

import type { KafkaBrowserClass } from "./cheerpj";

// CheerpJ's runtime is not redistributable, so it is the one thing this page loads from elsewhere.
// It is a JVM, not a Kafka service: no Kafka bytes and no Kafka traffic ever leave the tab.
const CHEERPJ_LOADER = "https://cjrtnc.leaningtech.com/4.3/loader.js";
// Kafka's storage layer is byte-buffer heavy and CheerpJ's Java 11/17 runtimes mis-handle absolute
// heap ByteBuffer puts (see README); its Java 8 runtime is correct, so that is what we run on.
const JAVA_VERSION = 8;
const ASSETS = "/_kafka";
// A cluster id is a base64 UUID, so it has to be exactly 22 characters.
const CLUSTER_ID = "kAfkAinYourBrowserAAAA";
const LOG_DIR_KEY = "kafka-browser-log-dir";

interface StateResponse {
  ok: boolean;
  error?: string;
  clusterId?: string;
  brokers?: { id: number; host: string; port: number }[];
  topics?: {
    name: string;
    internal: boolean;
    partitions: { partition: number; leader: number; startOffset: number; endOffset: number }[];
  }[];
  groups?: {
    groupId: string;
    state: string;
    offsets: { topic: string; partition: number; committed: number; endOffset: number; lag: number }[];
  }[];
}

const el = <T extends HTMLElement>(selector: string): T => {
  const found = document.querySelector<T>(selector);
  if (!found) throw new Error(`missing element: ${selector}`);
  return found;
};

const bar = el<HTMLElement>("#bar i");
const bootStatus = el("#boot-status");
const logPane = el("#log");
const clusterLine = el("#cluster");
const startButton = el<HTMLButtonElement>("#start");
const resetButton = el<HTMLButtonElement>("#reset");
const burstButton = el<HTMLButtonElement>("#produce-burst");
const topicForm = el<HTMLFormElement>("#topic-form");
const produceForm = el<HTMLFormElement>("#produce-form");
const consumeForm = el<HTMLFormElement>("#consume-form");
const actionButtons = [
  ...topicForm.querySelectorAll<HTMLButtonElement>("button"),
  ...produceForm.querySelectorAll<HTMLButtonElement>("button"),
  ...consumeForm.querySelectorAll<HTMLButtonElement>("button"),
];

let kafka: KafkaBrowserClass | null = null;
let busy = false;
const logLines: string[] = [];

// CheerpJ runs the JVM on the page's own thread and rejects a call made while another is still
// running, so every call into Kafka goes through this queue.
let jvmQueue: Promise<unknown> = Promise.resolve();

function jvm<T>(run: () => Promise<T>): Promise<T> {
  const next = jvmQueue.then(run, run);
  jvmQueue = next.catch(() => undefined);
  return next;
}

function progress(fraction: number, message: string): void {
  bar.style.width = `${Math.round(fraction * 100)}%`;
  bootStatus.textContent = message;
}

function log(line: string): void {
  // CheerpJ prints a "TODO: fsync" line on every flush; it is runtime noise, not Kafka output.
  if (line.startsWith("TODO:")) return;
  logLines.push(line);
  if (logLines.length > 400) logLines.splice(0, logLines.length - 400);
  const atBottom = logPane.scrollTop + logPane.clientHeight >= logPane.scrollHeight - 20;
  logPane.textContent = logLines.join("\n");
  if (atBottom) logPane.scrollTop = logPane.scrollHeight;
}

// Kafka logs through log4j to stdout, which CheerpJ forwards to console.log; tee it into the page.
function teeConsole(): void {
  for (const level of ["log", "info", "warn", "error"] as const) {
    const original = console[level].bind(console);
    console[level] = (...args: unknown[]) => {
      original(...args);
      const text = args.map((a) => (typeof a === "string" ? a : String(a))).join(" ");
      if (text.trim()) log(text);
    };
  }
}

function logDir(): string {
  const stored = localStorage.getItem(LOG_DIR_KEY);
  if (stored) return stored;
  const fresh = `/files/kafka-logs-1`;
  localStorage.setItem(LOG_DIR_KEY, fresh);
  return fresh;
}

// The one honest way to start over: /files is persistent and CheerpJ has no rmdir, so a reset moves
// the broker to a new log directory and reloads.
function resetLogDir(): void {
  const current = logDir();
  const next = Number(current.replace(/^.*-/, "")) + 1;
  localStorage.setItem(LOG_DIR_KEY, `/files/kafka-logs-${next}`);
  location.reload();
}

function serverProperties(dir: string): string {
  return [
    "process.roles=broker,controller",
    "node.id=1",
    "controller.quorum.voters=1@localhost:9093",
    "listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093",
    "advertised.listeners=PLAINTEXT://localhost:9092",
    "inter.broker.listener.name=PLAINTEXT",
    "controller.listener.names=CONTROLLER",
    "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
    `log.dirs=${dir}`,
    "num.network.threads=2",
    "num.io.threads=2",
    "num.partitions=1",
    "default.replication.factor=1",
    "offsets.topic.num.partitions=1",
    "offsets.topic.replication.factor=1",
    "transaction.state.log.num.partitions=1",
    "transaction.state.log.replication.factor=1",
    "transaction.state.log.min.isr=1",
    "group.initial.rebalance.delay.ms=0",
    "log.flush.interval.messages=1",
    "metrics.reporters=",
    "auto.create.topics.enable=true",
    "",
  ].join("\n");
}

function loadScript(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = src;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error(`failed to load ${src}`));
    document.head.append(script);
  });
}

async function classpath(): Promise<string> {
  const manifest = (await (await fetch(`${ASSETS}/jars.json`)).json()) as {
    version: string;
    jars: string[];
    bytes: number;
  };
  log(`Kafka ${manifest.version}: ${manifest.jars.length} jars, ${(manifest.bytes / 1e6).toFixed(1)} MB`);
  return [
    `/app${ASSETS}/kafka-browser-glue.jar`,
    ...manifest.jars.map((jar) => `/app${ASSETS}/jars/${jar}`),
  ].join(":");
}

function parse<T>(json: string): T {
  return JSON.parse(json) as T;
}

async function boot(): Promise<void> {
  startButton.disabled = true;
  teeConsole();
  const started = Date.now();
  const since = () => `${((Date.now() - started) / 1000).toFixed(1)}s`;

  progress(0.05, "loading the CheerpJ JVM…");
  await loadScript(CHEERPJ_LOADER);
  await cheerpjInit({
    version: JAVA_VERSION,
    status: "none",
    natives: {
      // CheerpJ has no msync: writes to a MappedByteBuffer are already in its filesystem, so the
      // force Kafka's index flush asks for is a no-op here (there is no native force0 to call).
      async Java_java_nio_MappedByteBuffer_force0() {},
    },
    javaProperties: [
      // Kafka's whole network layer goes through NIO; this provider is a loopback implementation of
      // it in java/inmem, because a browser cannot open a socket.
      "java.nio.channels.spi.SelectorProvider=inmem.InMemorySelectorProvider",
      `log4j.configuration=file:/app${ASSETS}/log4j.properties`,
      "java.awt.headless=true",
    ],
  });

  progress(0.2, "loading Kafka's jars…");
  const cp = await classpath();
  const lib = await cheerpjRunLibrary(cp);
  const browser = await lib.KafkaBrowser;

  const dir = logDir();
  cheerpjAddStringFile("/str/server.properties", serverProperties(dir));
  log(`log.dirs=${dir}`);

  progress(0.35, "formatting the KRaft metadata log…");
  const formatted = parse<{ ok: boolean; error?: string }>(
    await jvm(() => browser.format("/str/server.properties", CLUSTER_ID)),
  );
  if (!formatted.ok) throw new Error(formatted.error ?? "storage format failed");
  log(`storage formatted in ${since()}`);

  progress(0.6, "starting the broker and controller…");
  const startResult = parse<{ ok: boolean; error?: string }>(
    await jvm(() => browser.start("/str/server.properties")),
  );
  if (!startResult.ok) throw new Error(startResult.error ?? "broker start failed");

  progress(0.9, "waiting for the broker to answer AdminClient…");
  kafka = browser;
  const state = await refresh();
  if (!state?.ok) throw new Error(state?.error ?? "broker did not answer describeCluster");

  const bootTime = since();
  progress(1, `Kafka 3.9.1 is up (${bootTime})`);
  log(`broker up in ${bootTime}`);
  startButton.textContent = "Broker running";
  for (const button of actionButtons) button.disabled = false;
  resetButton.disabled = false;
  setInterval(() => void refresh(), 4000);
}

async function refresh(): Promise<StateResponse | null> {
  if (!kafka || busy) return null;
  const client = kafka;
  let json: string;
  try {
    json = await jvm(() => client.state());
  } catch (error) {
    log(`AdminClient call failed: ${String(error)}`);
    return null;
  }
  const state = parse<StateResponse>(json);
  if (!state.ok) {
    el("#state-age").textContent = `AdminClient error: ${state.error ?? "unknown"}`;
    return state;
  }
  render(state);
  return state;
}

function render(state: StateResponse): void {
  const broker = state.brokers?.[0];
  clusterLine.textContent = broker
    ? `cluster ${state.clusterId} · broker ${broker.id} at ${broker.host}:${broker.port} (in-process)`
    : `cluster ${state.clusterId}`;
  el("#state-age").textContent = `updated ${new Date().toLocaleTimeString()}`;

  const topicRows: string[] = [];
  for (const topic of state.topics ?? []) {
    for (const partition of topic.partitions) {
      topicRows.push(
        `<tr class="${topic.internal ? "internal" : ""}"><td>${escapeHtml(topic.name)}</td>` +
          `<td>${partition.partition}</td><td>${partition.leader}</td>` +
          `<td>${partition.startOffset}</td><td>${partition.endOffset}</td></tr>`,
      );
    }
  }
  fill("#topics", "#topics-empty", topicRows);

  const groupRows: string[] = [];
  for (const group of state.groups ?? []) {
    if (!group.offsets.length) {
      groupRows.push(
        `<tr><td>${escapeHtml(group.groupId)}</td><td>${escapeHtml(group.state)}</td>` +
          `<td colspan="5">no committed offsets</td></tr>`,
      );
    }
    for (const offset of group.offsets) {
      groupRows.push(
        `<tr><td>${escapeHtml(group.groupId)}</td><td>${escapeHtml(group.state)}</td>` +
          `<td>${escapeHtml(offset.topic)}</td><td>${offset.partition}</td>` +
          `<td>${offset.committed}</td><td>${offset.endOffset}</td>` +
          `<td class="${offset.lag > 0 ? "lag-behind" : ""}">${offset.lag}</td></tr>`,
      );
    }
  }
  fill("#groups", "#groups-empty", groupRows);
}

function fill(table: string, empty: string, rows: string[]): void {
  el(`${table} tbody`).innerHTML = rows.join("");
  el(empty).hidden = rows.length > 0;
  el(table).hidden = rows.length === 0;
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"]/g, (c) => `&#${c.charCodeAt(0)};`);
}

/** Serialises page actions: the facade is synchronised in Java, and the UI should say what it is doing. */
async function action<T>(label: string, run: () => Promise<T>): Promise<T | null> {
  if (!kafka || busy) return null;
  busy = true;
  for (const button of actionButtons) button.disabled = true;
  const previous = bootStatus.textContent;
  progress(1, label);
  try {
    return await jvm(run);
  } catch (error) {
    log(`${label} failed: ${String(error)}`);
    return null;
  } finally {
    busy = false;
    for (const button of actionButtons) button.disabled = false;
    bootStatus.textContent = previous;
    await refresh();
  }
}

startButton.addEventListener("click", () => {
  void boot().catch((error: unknown) => {
    progress(1, "boot failed");
    bar.style.background = "#e53e3e";
    log(`boot failed: ${String(error)}`);
    startButton.disabled = false;
  });
});

resetButton.addEventListener("click", resetLogDir);

topicForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const data = new FormData(topicForm);
  const name = String(data.get("name"));
  const partitions = Number(data.get("partitions"));
  void action(`creating topic ${name}…`, async () => {
    const result = parse<{ ok: boolean; message?: string; error?: string }>(
      await kafka!.createTopic(name, partitions),
    );
    log(result.ok ? `createTopics: ${result.message}` : `createTopics failed: ${result.error}`);
  });
});

async function produce(topic: string, key: string, value: string): Promise<void> {
  const result = parse<{ ok: boolean; partition?: number; offset?: number; error?: string }>(
    await kafka!.produce(topic, key, value),
  );
  log(
    result.ok
      ? `produced to ${topic}-${result.partition} at offset ${result.offset}`
      : `produce failed: ${result.error}`,
  );
}

produceForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const data = new FormData(produceForm);
  const topic = String(data.get("topic"));
  void action(`producing to ${topic}…`, () =>
    produce(topic, String(data.get("key")), String(data.get("value"))),
  );
});

burstButton.addEventListener("click", () => {
  const data = new FormData(produceForm);
  const topic = String(data.get("topic"));
  const value = String(data.get("value"));
  void action(`producing 10 records to ${topic}…`, async () => {
    for (let i = 0; i < 10; i++) await produce(topic, `k${i}`, `${value} #${i}`);
  });
});

consumeForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const data = new FormData(consumeForm);
  const group = String(data.get("group"));
  const topic = String(data.get("topic"));
  const max = Number(data.get("max"));
  void action(`polling ${topic} as ${group}…`, async () => {
    const result = parse<{
      ok: boolean;
      error?: string;
      records?: { partition: number; offset: number; key: string | null; value: string }[];
    }>(await kafka!.consume(group, topic, max, 8000));
    if (!result.ok) {
      log(`consume failed: ${result.error}`);
      return;
    }
    const records = result.records ?? [];
    log(`consumed ${records.length} record(s) as ${group} and committed`);
    fill(
      "#records",
      "#records-empty",
      records.map(
        (r) =>
          `<tr><td>${r.partition}</td><td>${r.offset}</td>` +
          `<td>${escapeHtml(r.key ?? "—")}</td><td>${escapeHtml(r.value)}</td></tr>`,
      ),
    );
    if (!records.length) el("#records-empty").textContent = "Poll returned no records (group is caught up).";
  });
});

el("#topics").hidden = true;
el("#groups").hidden = true;
el("#records").hidden = true;
