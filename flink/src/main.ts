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

// The page: boot the JVM, start the MiniCluster, submit the streaming job, then feed it and draw
// whatever it reports back.  The interesting code is on the other side of this file, in ../java.

import { boot, type Driver } from "./driver";
import type { Checkpoint, ExecutionVertex, Note, Poll, Window } from "./protocol";

const element = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;

/**
 * Turns anything thrown into something printable.
 *
 * A JVM exception arrives as a CheerpJ object with no primitive conversion, so plain `String(error)`
 * would itself throw and hide the failure it was meant to report.
 */
function describe(error: unknown): string {
  if (typeof error === "string") return error;
  if (error instanceof Error) return error.stack ? `${error.message}\n${error.stack}` : error.message;
  try {
    const text = String(error);
    if (text && text !== "[object Object]") return text;
  } catch {
    // A JVM object with no primitive conversion; fall through to its own fields.
  }
  try {
    return JSON.stringify(error) ?? "unknown failure";
  } catch {
    return Object.prototype.toString.call(error);
  }
}

const status = element("status");
const facts = element("facts");
const steps = element<HTMLOListElement>("steps");
const log = element<HTMLPreElement>("log");
const jobSection = element("job");
const jobNote = element("job-note");
const plan = element<HTMLPreElement>("plan");
const events = element<HTMLPreElement>("events");
const words = element<HTMLInputElement>("words");
const rate = element<HTMLInputElement>("rate");
const skew = element<HTMLInputElement>("skew");
const inputs = {
  window: element<HTMLInputElement>("window"),
  lateness: element<HTMLInputElement>("lateness"),
  skewness: element<HTMLInputElement>("skewness"),
  interval: element<HTMLInputElement>("interval"),
  parallelism: element<HTMLInputElement>("parallelism"),
};
const buttons = {
  submit: element<HTMLButtonElement>("submit"),
  cancel: element<HTMLButtonElement>("cancel"),
  send: element<HTMLButtonElement>("send"),
  checkpoint: element<HTMLButtonElement>("checkpoint"),
};
const metrics = {
  state: element("m-state"),
  ingested: element("m-ingested"),
  emitted: element("m-emitted"),
  late: element("m-late"),
  watermark: element("m-watermark"),
  lag: element("m-lag"),
  checkpoints: element("m-checkpoints"),
  stateSize: element("m-state-size"),
};
const panels = {
  windows: element("panel-windows"),
  graph: element("panel-graph"),
  plan: element("panel-plan"),
  checkpoints: element("panel-checkpoints"),
  events: element("panel-events"),
  dashboard: element("panel-dashboard"),
} as const;

/** Bytes the browser actually pulled over the wire, from its own resource timings. */
function transferred(): number {
  let bytes = 0;
  for (const entry of performance.getEntriesByType("resource") as PerformanceResourceTiming[]) {
    bytes += entry.transferSize || entry.encodedBodySize || 0;
  }
  return bytes;
}

const mb = (bytes: number) => `${(bytes / 1e6).toFixed(0)} MB`;
const seconds = (ms: number) => `${(ms / 1000).toFixed(1)} s`;
const clock = (ms: number) => (ms > 0 ? new Date(ms).toISOString().slice(11, 23) : "—");

/** One boot step, shown in the page while it runs and timed when it finishes. */
function step(text: string) {
  const item = document.createElement("li");
  item.dataset.state = "running";
  item.textContent = text;
  const timing = document.createElement("span");
  timing.className = "ms";
  item.append(timing);
  steps.append(item);
  const started = performance.now();
  return {
    done(detail?: string) {
      item.dataset.state = "done";
      timing.textContent = ` ${detail ? `${detail}, ` : ""}${seconds(performance.now() - started)}`;
    },
    failed(detail: string) {
      item.dataset.state = "failed";
      timing.textContent = ` ${detail}`;
    },
  };
}

function line(text: string) {
  const atBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 8;
  log.append(`${text}\n`);
  if (atBottom) log.scrollTop = log.scrollHeight;
}

function table(head: string[], rows: (string | number | null)[][], numeric: number[] = []): HTMLTableElement {
  const table = document.createElement("table");
  const header = table.createTHead().insertRow();
  for (const name of head) {
    const cell = document.createElement("th");
    cell.textContent = name;
    header.append(cell);
  }
  const body = table.createTBody();
  for (const row of rows) {
    const tr = body.insertRow();
    row.forEach((value, at) => {
      const cell = tr.insertCell();
      cell.textContent = value === null ? "—" : String(value);
      if (numeric.includes(at)) cell.className = "num";
    });
  }
  return table;
}

// --- the tabs ---------------------------------------------------------------------------------

for (const name of Object.keys(panels) as (keyof typeof panels)[]) {
  element<HTMLButtonElement>(`tab-${name}`).addEventListener("click", () => {
    for (const other of Object.keys(panels) as (keyof typeof panels)[]) {
      const selected = other === name;
      element(`tab-${other}`).setAttribute("aria-selected", String(selected));
      panels[other].hidden = !selected;
    }
  });
}

// --- Flink's own dashboard ---------------------------------------------------------------------

/** Where the service worker mounts the dashboard the JobManager serves. */
const DASHBOARD = "/flink/dashboard/";

/**
 * Puts Flink's web dashboard in the page, served by the JobManager in this tab.
 *
 * <p>The worker cannot call the JVM itself -- CheerpJ runs in this window -- so it forwards each
 * request here over a message port, and this is the end that turns it into `FlinkConsole.restRequest`.
 */
async function serveDashboard(driver: Driver): Promise<void> {
  if (!("serviceWorker" in navigator)) {
    throw new Error("this browser has no service worker, so the dashboard cannot be served");
  }

  navigator.serviceWorker.addEventListener("message", (event: MessageEvent) => {
    const data = event.data as { type?: string; call?: { method: string; target: string; body: string } };
    const port = event.ports[0];
    if (data?.type !== "rest" || !data.call || !port) return;
    const { method, target, body } = data.call;
    driver
      .rest(method, target, body)
      .then((answer) => port.postMessage(answer.ok ? answer : { ok: false, error: answer.trace ?? answer.error }))
      .catch((error: unknown) => port.postMessage({ ok: false, error: describe(error) }));
  });

  const registration = await navigator.serviceWorker.register("/flink/dashboard-sw.js", { scope: DASHBOARD });
  // `navigator.serviceWorker.ready` would never resolve: the worker's scope is narrower than this
  // page's URL, on purpose, so it never controls the page that registers it -- only the frame below.
  const worker = registration.installing ?? registration.waiting ?? registration.active;
  if (!worker) throw new Error("the dashboard service worker did not activate");
  if (worker.state !== "activated") {
    await new Promise<void>((resolve) => {
      worker.addEventListener("statechange", function settled() {
        if (worker.state !== "activated") return;
        worker.removeEventListener("statechange", settled);
        resolve();
      });
    });
  }

  // The worker only asks this window, so it has to be told which client this is.
  await new Promise<void>((resolve, reject) => {
    const channel = new MessageChannel();
    const timer = setTimeout(() => reject(new Error("the service worker did not acknowledge")), 10000);
    channel.port1.onmessage = () => {
      clearTimeout(timer);
      resolve();
    };
    worker.postMessage({ type: "host" }, [channel.port2]);
  });

  const frame = document.createElement("iframe");
  frame.src = `${DASHBOARD}index.html`;
  frame.title = "Apache Flink Web Dashboard";
  panels.dashboard.append(frame);
}

// --- what the job produces --------------------------------------------------------------------

/** The window rows the sink has emitted, newest first; Flink emits one row per key per window. */
const emitted: Window[] = [];
const notes: Note[] = [];

function drawWindows() {
  panels.windows.replaceChildren(
    emitted.length === 0
      ? Object.assign(document.createElement("p"), {
          className: "note",
          textContent:
            "No window has closed yet. Send some words, or turn the generator on: a window only " +
            "fires once the watermark passes its end.",
        })
      : table(
          ["window", "word", "count", "emitted at"],
          emitted
            .slice(0, 200)
            .map((row) => [
              `${clock(row.windowStart)} → ${clock(row.windowEnd)}`,
              row.word,
              row.count,
              clock(row.emittedAt),
            ]),
          [2],
        ),
  );
}

function drawGraph(vertices: ExecutionVertex[]) {
  const rows: (string | number)[][] = [];
  for (const vertex of vertices) {
    for (const subtask of vertex.subtasks) {
      rows.push([
        vertex.name,
        `${subtask.index + 1}/${vertex.parallelism}`,
        subtask.attempt,
        subtask.state,
        clock(subtask.startedAt),
        vertex.id.slice(0, 8),
      ]);
    }
  }
  const rendered = table(["vertex", "subtask", "attempt", "state", "deployed", "id"], rows, [2]);
  for (const row of rendered.tBodies[0].rows) {
    const cell = row.cells[3];
    cell.className = "state";
    cell.dataset.state = cell.textContent ?? "";
  }
  panels.graph.replaceChildren(rendered);
}

function drawCheckpoints(history: Checkpoint[]) {
  panels.checkpoints.replaceChildren(
    history.length === 0
      ? Object.assign(document.createElement("p"), {
          className: "note",
          textContent: "The checkpoint coordinator has not triggered one yet.",
        })
      : table(
          ["id", "status", "triggered", "duration", "state size", "acked", "path"],
          history.map((point) => [
            point.id,
            point.status,
            clock(point.triggerTimestamp),
            `${point.durationMs} ms`,
            `${point.stateSizeBytes.toLocaleString()} B`,
            `${point.acknowledged}/${point.subtasks}`,
            point.path,
          ]),
          [0, 3, 4],
        ),
  );
}

function drawNotes() {
  events.textContent = notes
    .slice(0, 400)
    .map((note) => `${clock(note.at)}  ${note.kind.padEnd(11)} ${note.detail}`)
    .join("\n");
}

/** The last job failure printed, so the poll loop reports each one once. */
let reported = "";

function draw(poll: Poll) {
  metrics.state.textContent = poll.state ?? "—";
  if (poll.failure && poll.failure !== reported) {
    reported = poll.failure;
    jobNote.className = "note error";
    jobNote.textContent = poll.failure.split("\n")[0];
    line(poll.failure);
  }
  metrics.ingested.textContent = poll.ingested.toLocaleString();
  metrics.emitted.textContent = poll.emitted.toLocaleString();
  metrics.late.textContent = poll.late.toLocaleString();
  metrics.watermark.textContent = clock(poll.watermark);
  metrics.lag.textContent =
    poll.watermark > 0 && poll.maxEventTime > 0 ? `${((poll.maxEventTime - poll.watermark) / 1000).toFixed(1)} s` : "—";
  metrics.checkpoints.textContent = String(poll.checkpoints?.completed ?? 0);
  const latest = poll.checkpoints?.latest;
  metrics.stateSize.textContent = latest ? `${latest.stateSizeBytes.toLocaleString()} B` : "—";

  if (poll.windows.length) {
    emitted.unshift(...poll.windows.reverse());
    emitted.length = Math.min(emitted.length, 2000);
    drawWindows();
  }
  if (poll.notes.length) {
    notes.unshift(...poll.notes.reverse());
    notes.length = Math.min(notes.length, 2000);
    drawNotes();
  }
  if (poll.graph) drawGraph(poll.graph);
  if (poll.checkpoints) drawCheckpoints(poll.checkpoints.history);
}

// --- the visitor's controls -------------------------------------------------------------------

function jobOptions() {
  return {
    windowSeconds: Number(inputs.window.value),
    lateness: Number(inputs.lateness.value),
    outOfOrderness: Number(inputs.skewness.value),
    checkpointInterval: Number(inputs.interval.value),
    parallelism: Number(inputs.parallelism.value),
  };
}

function wire(driver: Driver) {
  const submit = async () => {
    buttons.submit.disabled = true;
    jobNote.className = "note";
    jobNote.textContent = "submitting…";
    emitted.length = 0;
    notes.length = 0;
    drawWindows();
    drawNotes();
    const submitted = await driver.submit(jobOptions());
    buttons.submit.disabled = false;
    if (!submitted.ok) {
      jobNote.className = "note error";
      jobNote.textContent = submitted.error;
      line(submitted.trace ?? submitted.error);
      return;
    }
    plan.textContent = JSON.stringify(submitted.plan, null, 2);
    jobNote.textContent = `${submitted.jobName} — job ${submitted.jobId.slice(0, 12)}… submitted in ${
      submitted.submitMs
    } ms`;
  };

  buttons.submit.addEventListener("click", () => void submit());
  buttons.cancel.addEventListener("click", async () => {
    await driver.cancel();
    jobNote.textContent = "cancelled";
  });
  const send = async () => {
    const text = words.value.trim();
    if (!text) return;
    await driver.push(text, 0);
    words.value = "";
  };
  buttons.send.addEventListener("click", () => void send());
  words.addEventListener("keydown", (keys) => {
    if (keys.key === "Enter") void send();
  });
  buttons.checkpoint.addEventListener("click", async () => {
    buttons.checkpoint.disabled = true;
    const result = await driver.checkpoint();
    buttons.checkpoint.disabled = false;
    if (!result.ok) line(result.trace ?? result.error);
  });

  const generator = async () => {
    const events = Number(rate.value);
    const percent = Number(skew.value);
    element("rate-value").textContent = events === 0 ? "off" : `${events}/s`;
    element("skew-value").textContent = `${percent}%`;
    await driver.generator(events, percent);
  };
  rate.addEventListener("change", () => void generator());
  skew.addEventListener("change", () => void generator());

  return submit;
}

// --- boot ------------------------------------------------------------------------------------

async function main() {
  const driver = await boot({
    onLog: line,
    onStep: step,
    onManifest: (loaded) => {
      facts.textContent = `Flink ${loaded.flinkVersion} · ${loaded.classpath.length} jars · ${mb(loaded.jarBytes)}`;
    },
  });

  status.textContent = `Flink ${driver.cluster.flinkVersion} running`;
  facts.textContent =
    `Flink ${driver.cluster.flinkVersion} · ${driver.cluster.taskManagers} TaskManager, ` +
    `${driver.cluster.slots} slots · ${mb(transferred())} downloaded · booted in ${seconds(driver.bootMs)}`;
  step(
    `${driver.cluster.jvm}, Java ${driver.cluster.javaVersion} · hashmap state backend · ` +
      `checkpoints in ${driver.cluster.checkpointDir}`,
  ).done();
  jobSection.hidden = false;

  const submit = wire(driver);
  await submit();
  serveDashboard(driver).catch((error: unknown) => {
    const text = describe(error);
    line(text);
    element("dashboard-note").textContent = `The dashboard could not be served in this tab: ${text.split("\n")[0]}`;
    element("dashboard-note").className = "note error";
  });

  // One poll a second is what the page draws from: the windows the sink emitted, the ExecutionGraph
  // the JobMaster reports, and the checkpoint stats the coordinator keeps.
  for (;;) {
    const poll = await driver.poll();
    if (poll.ok) draw(poll);
    else line(poll.trace ?? poll.error);
    await new Promise((wake) => setTimeout(wake, 1000));
  }
}

main().catch((error) => {
  status.textContent = "failed";
  status.className = "error";
  const text = describe(error);
  line(text);
  const failure = document.createElement("p");
  failure.className = "error note";
  failure.textContent =
    "The Flink runtime did not start in this tab. The stack trace above is what the JVM reported.";
  element("boot").append(failure);
  console.error(error);
});
