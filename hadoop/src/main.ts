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

// The page: boot the JVM, then hand the visitor's commands to Hadoop -- cluster and all, in one call --
// and render what comes back.  The interesting code is on the other side of this file, in ../java.

import { boot, type Driver, type Mode } from "./driver";
import type { Cluster, JobResult, Progress, Session, ShellResult, Split, Step } from "./protocol";

/** Where the page puts things by default; `/user/<user>` is HDFS's own convention. */
const HOME = "/user/browser";

/**
 * The same, in local mode: CheerpJ's filesystem is only writable under `/files`, so Hadoop's
 * LocalFileSystem gets a home there rather than at the root of the tab's virtual disk.
 */
const LOCAL_HOME = "/files/hadoop/local/user/browser";

/** Whether this page load has already had its one run; see {@link run}. */
let ran = false;

const element = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;

/**
 * Turns anything thrown into something printable.
 *
 * A JVM exception arrives as a CheerpJ object with no primitive conversion, so plain `String(error)`
 * would itself throw and hide the failure it was meant to report.
 */
function describe(error: unknown): string {
  if (typeof error === "string") return error;
  if (error instanceof Error) return `${error.name}: ${error.message}`;
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
const work = element("work");
const shellOut = element<HTMLPreElement>("shell-out");
const script = element<HTMLTextAreaElement>("script");
const runGo = element<HTMLButtonElement>("run");
const runNote = element("run-note");
const mode = element<HTMLSelectElement>("mode");
const modeNote = element("mode-note");
const writePath = element<HTMLInputElement>("write-path");
const writeText = element<HTMLTextAreaElement>("write-text");
const clusterTable = element<HTMLTableElement>("cluster");
const storageTable = element<HTMLTableElement>("storage");
const storageSummary = element("storage-summary");
const jobNote = element("job-note");
const jobBars = element("job-bars");
const panels = {
  counters: element("panel-counters"),
  splits: element("panel-splits"),
  output: element("panel-output"),
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
const bytes = (value: number) =>
  value >= 1e9
    ? `${(value / 1e9).toFixed(1)} GB`
    : value >= 1e6
      ? `${(value / 1e6).toFixed(1)} MB`
      : value >= 1e3
        ? `${(value / 1e3).toFixed(1)} kB`
        : `${value} B`;

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
  const atBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 4;
  log.append(`${text}\n`);
  if (atBottom) log.scrollTop = log.scrollHeight;
}

/** Appends to the shell transcript, keeping it scrolled to the prompt. */
function echo(text: string, className?: string) {
  const atBottom = shellOut.scrollTop + shellOut.clientHeight >= shellOut.scrollHeight - 4;
  const span = document.createElement("span");
  if (className) span.className = className;
  span.textContent = `${text}\n`;
  shellOut.append(span);
  if (atBottom) shellOut.scrollTop = shellOut.scrollHeight;
}

function table(columns: string[], rows: string[][], numeric: number[] = []): HTMLElement {
  const table = document.createElement("table");
  const head = table.insertRow();
  columns.forEach((column, i) => {
    const cell = document.createElement("th");
    cell.textContent = column;
    if (numeric.includes(i)) cell.className = "num";
    head.append(cell);
  });
  for (const row of rows) {
    const line = table.insertRow();
    row.forEach((value, i) => {
      const cell = line.insertCell();
      cell.textContent = value;
      if (numeric.includes(i)) cell.className = "num";
    });
  }
  return table;
}

function show(tab: keyof typeof panels) {
  for (const name of Object.keys(panels) as (keyof typeof panels)[]) {
    panels[name].hidden = name !== tab;
    element(`tab-${name}`).setAttribute("aria-selected", String(name === tab));
  }
}

/** The NameNode's and DataNode's own report of themselves, or the local filesystem's. */
function renderCluster(cluster: Cluster) {
  const storage = cluster.storage;
  const rows: string[][] = [["Filesystem", cluster.uri]];
  if (storage) {
    rows.push(
      ["NameNode", `${cluster.nameNodeAddress} (safe mode ${cluster.safeMode ? "on" : "off"})`],
      ["Cluster / block pool", `${storage.clusterId} / ${storage.blockPoolId}`],
      ["Namespace", `${storage.filesTotal} files, ${storage.blocksTotal} blocks`],
      [
        "Edit log",
        `txid ${storage.lastWrittenTransactionId}, ${storage.transactionsSinceLastCheckpoint} since checkpoint`,
      ],
    );
  } else {
    rows.push(
      ["FileSystem class", cluster.fileSystem ?? ""],
      ["Working directory", cluster.workingDirectory ?? ""],
      ["NameNode", "none: this run is Hadoop's LocalFileSystem, not HDFS"],
    );
  }
  rows.push(["Default block size", bytes(cluster.blockSize)], ["Replication", String(cluster.replication)]);
  for (const node of cluster.datanodes ?? []) {
    rows.push([
      `DataNode ${node.name}`,
      `${node.state}, ${bytes(node.dfsUsed)} used of ${bytes(node.capacity)}, ${node.xceivers} xceivers`,
    ]);
    rows.push(["DataNode uuid", node.uuid]);
  }
  clusterTable.replaceChildren();
  for (const [name, value] of rows) {
    const row = clusterTable.insertRow();
    row.insertCell().textContent = name;
    row.insertCell().textContent = value;
  }
  storageSummary.textContent = storage
    ? `NameNode and DataNode storage (${storage.files.length} files on the virtual disk)`
    : "No NameNode or DataNode storage: this run used Hadoop's LocalFileSystem";
  storageTable.replaceChildren(
    storage
      ? table(
          ["path", "bytes"],
          storage.files.map((file) => [file.path, String(file.bytes)]),
          [1],
        )
      : document.createElement("span"),
  );
}

function splitTable(splits: Split[], progress: Progress | null): HTMLElement {
  const wrapper = document.createElement("div");
  wrapper.append(
    Object.assign(document.createElement("p"), {
      className: "note",
      textContent: `${splits.length} input split${splits.length === 1 ? "" : "s"}, computed by TextInputFormat from the NameNode's block locations`,
    }),
    table(
      ["path", "start", "length", "hosts"],
      splits.map((split) => [split.path, String(split.start), String(split.length), split.hosts.join(", ")]),
      [1, 2],
    ),
  );
  const tasks = progress?.tasks ?? [];
  if (tasks.length) {
    wrapper.append(
      Object.assign(document.createElement("p"), {
        className: "note",
        textContent: "Task reports, from Job.getTaskReports",
      }),
      table(
        ["task", "type", "state", "progress"],
        tasks.map((task) => [task.id, task.type, task.state, `${task.progress}%`]),
        [3],
      ),
    );
  }
  return wrapper;
}

function counterTables(progress: Progress): HTMLElement {
  const wrapper = document.createElement("div");
  if (progress.counters.length === 0) {
    wrapper.append(
      Object.assign(document.createElement("p"), {
        className: "note",
        textContent: "No counters yet: the job runner has not started a task.",
      }),
    );
  }
  for (const group of progress.counters) {
    wrapper.append(
      Object.assign(document.createElement("p"), { className: "note", textContent: group.name }),
      table(
        ["counter", "value"],
        group.counters.map((counter) => [counter.display, counter.value.toLocaleString()]),
        [1],
      ),
    );
  }
  return wrapper;
}

function outputTable(progress: Progress): HTMLElement {
  const wrapper = document.createElement("div");
  const output = progress.output;
  if (!output) {
    wrapper.append(
      Object.assign(document.createElement("p"), {
        className: "note",
        textContent: "The job has not finished writing its output.",
      }),
    );
    return wrapper;
  }
  if (output.error) {
    wrapper.append(Object.assign(document.createElement("p"), { className: "error", textContent: output.error }));
    return wrapper;
  }
  const rows = [...(output.rows ?? [])].sort((left, right) => right.count - left.count);
  wrapper.append(
    Object.assign(document.createElement("p"), {
      className: "note",
      textContent:
        `${output.rowCount ?? 0} words in ${output.path}: ` +
        (output.files ?? []).map((file) => `${file.path} (${file.bytes} bytes)`).join(", "),
    }),
    table(
      ["word", "count"],
      rows.map((row) => [row.word, String(row.count)]),
      [1],
    ),
  );
  return wrapper;
}

function bar(id: string, percent: number) {
  element(`bar-${id}`).style.width = `${percent}%`;
  element(`pct-${id}`).textContent = `${percent}%`;
}

/** Renders one WordCount run: the last progress Hadoop reported, and the splits it computed. */
function renderJob(job: JobResult) {
  jobBars.hidden = false;
  const last = job.timeline[job.timeline.length - 1];
  const splits = job.submitted.ok ? job.submitted.splits : [];
  if (!job.submitted.ok) {
    jobNote.className = "error";
    jobNote.textContent = ` ${job.submitted.error}`;
    return;
  }
  if (!last) return;
  bar("setup", last.setupProgress);
  bar("map", last.mapProgress);
  bar("reduce", last.reduceProgress);
  panels.counters.replaceChildren(counterTables(last));
  panels.splits.replaceChildren(splitTable(splits, last));
  panels.output.replaceChildren(outputTable(last));
  jobNote.className = last.successful ? "note" : "error";
  jobNote.textContent =
    ` ${last.jobId} ${last.state} in ${seconds(last.elapsedMs)}, ` +
    `${job.timeline.length} progress reports`;
  show(last.successful ? "output" : "counters");
}

/** True when this step's result is a WordCount run rather than a shell command's. */
const isJob = (step: Step): step is Step & { result: JobResult } =>
  step.result.ok && "timeline" in step.result;

/** Puts a run's transcript into the page, in the order the JVM executed it. */
function renderSteps(session: Session) {
  for (const step of session.steps) {
    if (isJob(step)) {
      const submitted = step.result.submitted;
      echo(`$ hadoop jar wordcount ${step.command.replace(/^wordcount\s+/, "")}`, "cmd");
      echo(
        submitted.ok
          ? `submitted ${submitted.jobId} with ${submitted.splits.length} split${submitted.splits.length === 1 ? "" : "s"}`
          : submitted.error,
        submitted.ok ? undefined : "err",
      );
      const last = step.result.timeline[step.result.timeline.length - 1];
      if (last) {
        echo(
          `${last.jobId} ${last.state} in ${seconds(last.elapsedMs)}, ` +
            `${last.output?.rowCount ?? 0} distinct words`,
        );
      }
      renderJob(step.result);
      continue;
    }
    echo(`$ hdfs dfs -${step.command}`, "cmd");
    if (!step.result.ok) echo(step.result.error, "err");
    else if ((step.result as ShellResult).output) echo((step.result as ShellResult).output);
  }
}

/**
 * Starts a cluster, runs the visitor's commands against it and stops it, in one call into the JVM.
 *
 * The tab is frozen while Hadoop works -- CheerpJ runs the JVM on the page's thread -- so the page
 * says so before it calls, and only has something to draw once the whole run is back.
 */
async function run(driver: Driver) {
  runGo.disabled = true;
  runNote.className = "note";
  const hdfs = mode.value === "hdfs";
  runNote.textContent = hdfs
    ? "starting the NameNode and DataNode; the tab will not respond until it is done…"
    : "running Hadoop over CheerpJ's filesystem; the tab will not respond until it is done…";
  const running = step(hdfs ? "Starting HDFS and running the commands" : "Running the commands");
  try {
    const session = await driver.run(
      script.value,
      writePath.value.trim(),
      writeText.value,
      mode.value as Mode,
    );
    if (!session.ok) {
      running.failed(session.error);
      runNote.className = "error";
      runNote.textContent = session.error;
      line(session.trace ?? session.error);
      return;
    }
    running.done(`${session.steps.length} commands, boot ${seconds(session.cluster.bootMs)}`);
    renderCluster(session.cluster);
    facts.innerHTML =
      `boot ${seconds(session.cluster.bootMs)} &middot; run ${seconds(session.ms)} &middot; ` +
      `${mb(transferred())} downloaded<br />` +
      `JVM ${session.cluster.javaVersion} (CheerpJ) &middot; ` +
      `${(session.cluster.datanodes ?? []).length} DataNode &middot; user ${session.cluster.user}`;
    status.textContent = `Hadoop ${session.cluster.hadoopVersion} ran in this tab`;
    runNote.textContent = `${session.steps.length} commands in ${seconds(session.ms)}`;
    renderSteps(session);
  } catch (error) {
    running.failed(describe(error));
    runNote.className = "error";
    runNote.textContent = describe(error);
  } finally {
    // One run per page load: the cluster is still up, and CheerpJ will not take another call while it
    // is (see the driver), so the way to run again is a fresh JVM.
    ran = true;
    runGo.textContent = "Reload to run again";
    runGo.disabled = false;
  }
}

/**
 * What the page offers to run first, per mode: HDFS's own conventions over Hadoop's own files, and --
 * in local mode -- the same files through the MapReduce job, which is the mode it finishes in.
 */
function preset(driver: Driver, which: Mode): string {
  if (which === "local") {
    return [
      `mkdir -p ${LOCAL_HOME}/input`,
      ...driver.manifest.data.map(
        (file) => `put ${driver.dataPath}/${file.name} ${LOCAL_HOME}/input/${file.name}`,
      ),
      `ls -R ${LOCAL_HOME}/input`,
      `rm -r -f ${LOCAL_HOME}/wordcount`,
      `wordcount ${LOCAL_HOME}/input ${LOCAL_HOME}/wordcount`,
      `cat ${LOCAL_HOME}/wordcount/part-r-00000`,
    ].join("\n");
  }
  const puts = driver.manifest.data.map(
    (file) => `put ${driver.dataPath}/${file.name} ${HOME}/input/${file.name}`,
  );
  return [
    `mkdir -p ${HOME}/input`,
    ...puts,
    `ls -R ${HOME}`,
    `blocks ${HOME}/input/${driver.manifest.data[0]?.name ?? "typed.txt"}`,
    `cat ${HOME}/input/typed.txt`,
    "df",
  ].join("\n");
}

async function main() {
  const hooks = step("Fetching the classpath manifest");
  let driver: Driver;
  try {
    driver = await boot({
      onLog: line,
      onStep: step,
      onManifest: (manifest) => hooks.done(`${manifest.classpath.length} jars, ${mb(manifest.jarBytes)}`),
    });
  } catch (error) {
    status.textContent = "failed";
    hooks.failed(describe(error));
    line(describe(error));
    return;
  }
  status.textContent = `JVM ready in ${seconds(driver.jvmMs)}`;
  facts.innerHTML = `JVM ${seconds(driver.jvmMs)} &middot; ${mb(transferred())} downloaded`;
  work.hidden = false;
  script.value = preset(driver, mode.value as Mode);
  writePath.value = `${mode.value === "local" ? LOCAL_HOME : HOME}/input/typed.txt`;
  writeText.value = "the quick brown fox jumps over the lazy dog\nthe dog barks and the fox runs\n";

  mode.addEventListener("change", () => {
    script.value = preset(driver, mode.value as Mode);
    writePath.value = `${mode.value === "local" ? LOCAL_HOME : HOME}/input/typed.txt`;
    modeNote.textContent =
      mode.value === "hdfs"
        ? "A real NameNode and DataNode in this tab. wordcount freezes the JVM in this mode."
        : "Hadoop's LocalFileSystem over CheerpJ's filesystem: no daemons, and wordcount finishes.";
  });
  runGo.addEventListener("click", () => (ran ? location.reload() : void run(driver)));
  for (const tab of Object.keys(panels) as (keyof typeof panels)[]) {
    element(`tab-${tab}`).addEventListener("click", () => show(tab));
  }
}

void main();
