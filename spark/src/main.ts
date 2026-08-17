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

// The page: boot the JVM, start the driver, then hand whatever the visitor types to Spark and render
// what comes back.  The interesting code is on the other side of this file, in ../java.

import { BootPanel } from "../../design/boot";

import { boot, type Driver } from "./driver";
import { type Rows, type Session } from "./protocol";

import "./spark.css";

const SAMPLES: [string, string][] = [
  [
    "Biggest classes in Spark SQL",
    "select class, package, bytes\nfrom spark_classes\nwhere jar like 'spark-sql_%'\norder by bytes desc\nlimit 20",
  ],
  [
    "Compiled bytes per jar (shuffle)",
    "select jar, count(*) as classes, sum(bytes) as bytes, round(avg(bytes)) as avg_bytes\nfrom spark_classes\ngroup by jar\norder by bytes desc",
  ],
  [
    "Catalyst's largest packages",
    "select package, count(*) as classes, sum(bytes) as bytes\nfrom spark_classes\nwhere package like 'org.apache.spark.sql.catalyst%'\ngroup by package\nhaving count(*) > 20\norder by bytes desc\nlimit 15",
  ],
  [
    "How wide is the expression tree?",
    "select count(*) as expressions, round(avg(bytes), 1) as avg_bytes, max(bytes) as biggest\nfrom spark_classes\nwhere package = 'org.apache.spark.sql.catalyst.expressions'",
  ],
  [
    "Physical operators, ranked",
    "select class, bytes,\n       rank() over (order by bytes desc) as rank\nfrom spark_classes\nwhere package = 'org.apache.spark.sql.execution'\n  and class like '%Exec'\norder by rank\nlimit 15",
  ],
  ["Anything you like", "select spark_partition_id() as partition, count(*) as rows_seen\nfrom spark_classes\ngroup by 1"],
];

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
const consoleSection = element("query");
const sql = element<HTMLTextAreaElement>("sql");
const samples = element<HTMLSelectElement>("samples");
const schema = element("schema");
const run = element<HTMLButtonElement>("run");
const note = element("result-note");
const plan = element<HTMLPreElement>("plan");
const panels = {
  rows: element("panel-rows"),
  plan: element("panel-plan"),
  stages: element("panel-stages"),
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

const bootPanel = new BootPanel({
  mount: element("boot"),
  title: "Starting Apache Spark",
  detail:
    "Nothing here runs on a server. A real Spark 3.5.9 driver — Catalyst, the DAG scheduler, the " +
    "block manager and its shuffle — starts in this tab on a JVM compiled to WebAssembly, reading " +
    "its own jars off this origin. The dataset is one row per class in Spark's jars, so the queries " +
    "below are queries about the Spark that is answering them.",
  logs: [{ label: "Runtime log" }, { label: "JVM stdout", collapsed: true }],
});

const step = (text: string) => bootPanel.step(text);

/** The JVM's own stdout, kept in a pane of its own so the boot steps stay readable. */
function line(text: string) {
  bootPanel.log("JVM stdout").write(text);
}

function table(columns: string[], types: string[], rows: (string | null)[][]): HTMLElement {
  const table = document.createElement("table");
  table.className = "grid";
  const head = table.insertRow();
  columns.forEach((column, i) => {
    const cell = document.createElement("th");
    cell.append(column, document.createElement("br"));
    const type = document.createElement("small");
    type.textContent = types[i] ?? "";
    cell.append(type);
    head.append(cell);
  });
  for (const row of rows) {
    const line = table.insertRow();
    for (const value of row) {
      const cell = line.insertCell();
      cell.textContent = value ?? "NULL";
      if (value === null) cell.className = "null";
      else if (/^-?\d+(\.\d+)?$/.test(value)) cell.className = "num";
    }
  }
  return table;
}

function stageTable(result: Rows): HTMLElement {
  if (result.stages.length === 0) {
    const empty = document.createElement("p");
    empty.className = "note";
    empty.textContent =
      "No stages: Catalyst answered this from local relations or metadata without submitting a job.";
    return empty;
  }
  const columns = ["stage", "tasks", "wall ms", "executor ms", "records read", "shuffle write", "shuffle read", "peak memory"];
  const rows = result.stages.map((stage) => [
    `${stage.id} — ${stage.name}`,
    String(stage.tasks),
    String(stage.durationMs),
    String(stage.executorRunTimeMs),
    String(stage.recordsRead),
    String(stage.shuffleWriteBytes),
    String(stage.shuffleReadBytes),
    String(stage.peakMemoryBytes),
  ]);
  return table(columns, columns.map(() => ""), rows);
}

function show(tab: keyof typeof panels) {
  for (const name of Object.keys(panels) as (keyof typeof panels)[]) {
    panels[name].hidden = name !== tab;
    element(`tab-${name}`).setAttribute("aria-selected", String(name === tab));
  }
}

function render(result: Rows) {
  panels.rows.replaceChildren(table(result.columns, result.types, result.rows));
  plan.textContent = result.plan ?? "(no plan)";
  panels.stages.replaceChildren(stageTable(result));
  const tasks = result.stages.reduce((total, stage) => total + stage.tasks, 0);
  note.className = "note";
  note.textContent =
    `${result.rows.length} row${result.rows.length === 1 ? "" : "s"}${result.truncated ? " (truncated)" : ""} in ` +
    `${seconds(result.durationMs)} — ${result.stages.length} stage${result.stages.length === 1 ? "" : "s"}, ` +
    `${tasks} task${tasks === 1 ? "" : "s"}`;
}

async function execute(driver: Driver) {
  const statement = sql.value.trim().replace(/;\s*$/, "");
  if (!statement) return;
  run.disabled = true;
  note.className = "note";
  note.textContent = "running in the tab…";
  const started = performance.now();
  try {
    const result = await driver.query(statement);
    if (!result.ok) {
      note.className = "error";
      note.textContent = result.error;
      panels.rows.replaceChildren(Object.assign(document.createElement("pre"), { textContent: result.trace ?? "" }));
      plan.textContent = "";
      panels.stages.replaceChildren();
      show("rows");
      return;
    }
    render(result);
    show("rows");
  } catch (error) {
    note.className = "error";
    note.textContent = describe(error);
  } finally {
    run.disabled = false;
    bootPanel.say(`> ${statement.replace(/\n/g, " ")}  (${seconds(performance.now() - started)})`);
  }
}

function ready(session: Session, bootMs: number) {
  status.textContent = `Spark ${session.version} on ${session.master}`;
  facts.innerHTML =
    `boot ${seconds(bootMs)} &middot; ${mb(transferred())} downloaded<br />` +
    `Scala ${session.scalaVersion} &middot; JVM ${session.javaVersion} (CheerpJ) &middot; ${session.cores} core${session.cores === 1 ? "" : "s"}`;
  schema.textContent = `${session.view} (${session.schema.join(", ")})`;
  bootPanel.ready(
    `Spark ${session.version} is running in this tab · ${mb(transferred())} of jars in ${seconds(bootMs)}`,
  );
  consoleSection.hidden = false;
  sql.value = SAMPLES[0][1];
  for (const [name, statement] of SAMPLES) {
    const option = document.createElement("option");
    option.textContent = name;
    option.value = statement;
    samples.append(option);
  }
  samples.addEventListener("change", () => {
    sql.value = samples.value;
    sql.focus();
  });
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
    hooks.failed("failed");
    bootPanel.fail(describe(error));
    return;
  }
  ready(driver.session, driver.bootMs);
  run.addEventListener("click", () => void execute(driver));
  sql.addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") void execute(driver);
  });
  for (const tab of Object.keys(panels) as (keyof typeof panels)[]) {
    element(`tab-${tab}`).addEventListener("click", () => show(tab));
  }
  await execute(driver);
}

void main();
