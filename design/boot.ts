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

// Every project on this site spends its first half-minute pulling a runtime into the tab -- wheels,
// jars, an interpreter, a wasm module -- and that wait is the demo, not an interruption of it.  So
// all four pages show the same panel: what is happening now, the steps behind it, the bytes and
// seconds it is costing, and the runtime's own log.
//
// This module owns the panel's markup and its live numbers; design/system.css owns how it looks.

/** One step of a boot, shown while it runs and timed when it settles. */
export interface BootStep {
  done(detail?: string): void;
  failed(detail: string): void;
}

/** A named pane of log lines inside the panel. */
export interface BootLog {
  /** Appends a line, keeping the pane pinned to the bottom unless the reader scrolled up. */
  write(text: string): void;
  /** The `<pre>` itself: a runtime that writes into an element by id (CheerpJ) needs this. */
  element: HTMLPreElement;
}

export interface BootLogOptions {
  /** How the pane is labelled, e.g. "Runtime log". */
  label: string;
  /** Set as the element's id, for runtimes that find their output pane that way. */
  id?: string;
  /** Panes start open -- the loading is worth watching -- unless a project says otherwise. */
  collapsed?: boolean;
}

export interface BootPanelOptions {
  /** The panel is appended here. */
  mount: HTMLElement;
  /** What is starting: "Apache Spark 3.5.9". */
  title: string;
  /** One honest sentence about what the wait buys. */
  detail: string;
  /** Extra log panes beyond the default one, in the order they should appear. */
  logs?: BootLogOptions[];
}

const SECOND = 1000;

const seconds = (ms: number): string => `${(ms / SECOND).toFixed(1)} s`;

const megabytes = (bytes: number): string => `${(bytes / 1e6).toFixed(1)} MB`;

/** Bytes this tab actually pulled over the wire, from the browser's own resource timings. */
function transferred(): number {
  let bytes = 0;
  for (const entry of performance.getEntriesByType("resource") as PerformanceResourceTiming[]) {
    bytes += entry.transferSize || entry.encodedBodySize || 0;
  }
  return bytes;
}

function element<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  className?: string,
  text?: string,
): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

class LogPane implements BootLog {
  readonly element: HTMLPreElement;
  readonly container: HTMLDetailsElement;
  private readonly count: HTMLSpanElement;
  private lines = 0;

  constructor(options: BootLogOptions) {
    const details = element("details", "boot__log");
    details.open = !options.collapsed;
    const summary = element("summary");
    summary.append(element("span", undefined, options.label));
    this.count = element("span", "boot__count", "0 lines");
    summary.append(this.count);
    this.element = element("pre", "boot__lines");
    if (options.id) this.element.id = options.id;
    this.element.setAttribute("aria-label", options.label);
    details.append(summary, this.element);
    this.container = details;
  }

  write(text: string): void {
    // Only follow the tail if the reader is already at it: scrolling back to read a line should not
    // be undone by the next one.
    const pinned = this.element.scrollTop + this.element.clientHeight >= this.element.scrollHeight - 8;
    for (const line of text.split("\n")) {
      this.element.append(`${line}\n`);
      this.lines += 1;
    }
    this.count.textContent = `${this.lines} line${this.lines === 1 ? "" : "s"}`;
    if (pinned) this.element.scrollTop = this.element.scrollHeight;
  }
}

export class BootPanel {
  readonly element: HTMLElement;

  private readonly titleNode: HTMLElement;
  private readonly detailNode: HTMLElement;
  private readonly nowNode: HTMLElement;
  private readonly stepList: HTMLOListElement;
  private readonly bar: HTMLElement;
  private readonly elapsedNode: HTMLElement;
  private readonly bytesNode: HTMLElement;
  private readonly logs = new Map<string, LogPane>();
  private readonly started = performance.now();
  private ticker: number | undefined;
  private settled = false;

  constructor(options: BootPanelOptions) {
    this.element = element("section", "boot");
    this.element.dataset.state = "running";
    this.element.dataset.progress = "indeterminate";
    this.element.setAttribute("aria-live", "polite");

    const head = element("div", "boot__head");
    this.titleNode = element("h2", "boot__title", options.title);
    this.detailNode = element("p", "boot__detail", options.detail);
    const counters = element("div", "boot__counters");
    this.elapsedNode = element("span", undefined, "0.0 s");
    this.bytesNode = element("span");
    counters.append(this.elapsedNode, this.bytesNode);
    head.append(this.titleNode, this.detailNode, counters);

    const meter = element("div", "boot__meter");
    this.bar = element("div", "boot__bar");
    meter.append(this.bar);

    this.nowNode = element("p", "boot__now", "starting");
    this.stepList = element("ol", "boot__steps");

    const logs = element("div", "boot__logs");
    for (const log of options.logs ?? [{ label: "Runtime log" }]) {
      const pane = new LogPane(log);
      this.logs.set(log.label, pane);
      logs.append(pane.container);
    }

    this.element.append(head, meter, this.nowNode, this.stepList, logs);
    options.mount.append(this.element);

    this.tick();
    this.ticker = window.setInterval(() => this.tick(), 200);
  }

  /** The default log pane, or the one with this label. */
  log(label?: string): BootLog {
    const pane = label === undefined ? this.logs.values().next().value : this.logs.get(label);
    if (!pane) throw new Error(`no boot log pane ${label ?? "(default)"}`);
    return pane;
  }

  /** Appends a line to the default log pane. */
  say(text: string): void {
    this.log().write(text);
  }

  /** The single line above the steps: what the runtime is doing right now. */
  now(text: string): void {
    this.nowNode.textContent = text;
  }

  /** Announces a step, and logs it so the transcript is complete. */
  step(text: string): BootStep {
    const item = element("li", "boot__step");
    item.dataset.state = "running";
    const label = element("span", undefined, text);
    const timing = element("span", "boot__timing");
    item.append(label, timing);
    this.stepList.append(item);
    this.now(text);
    this.say(text);
    const started = performance.now();
    return {
      done: (detail?: string) => {
        item.dataset.state = "done";
        const took = seconds(performance.now() - started);
        timing.textContent = detail ? `${detail} · ${took}` : took;
      },
      failed: (detail: string) => {
        item.dataset.state = "failed";
        timing.textContent = detail;
      },
    };
  }

  /**
   * How far along the download is.  Called with no arguments while the size is unknown, which is the
   * honest state for most of these runtimes: the bar travels instead of lying about a percentage.
   */
  progress(loaded?: number, total?: number): void {
    if (loaded === undefined || !total) {
      this.element.dataset.progress = "indeterminate";
      return;
    }
    this.element.dataset.progress = "measured";
    this.bar.style.width = `${Math.min(100, (loaded / total) * 100).toFixed(1)}%`;
  }

  /** Booted: the panel collapses to a line, with the log still a click away. */
  ready(summary: string): void {
    this.settle("done");
    this.titleNode.textContent = summary;
    this.now(summary);
  }

  /** Failed: the panel keeps every step and line, and says what went wrong. */
  fail(message: string): void {
    this.settle("failed");
    this.now("boot failed");
    this.say(message);
    const note = element("p", "boot__note boot__note--error", message);
    this.element.append(note);
  }

  private settle(state: "done" | "failed"): void {
    if (this.settled) return;
    this.settled = true;
    this.tick();
    if (this.ticker !== undefined) window.clearInterval(this.ticker);
    this.ticker = undefined;
    this.element.dataset.state = state;
    this.element.dataset.progress = "measured";
    if (state === "done") this.bar.style.width = "100%";
    // The steps stop being news once it is up; the reader can still open the log.
    this.detailNode.remove();
  }

  private tick(): void {
    this.elapsedNode.textContent = seconds(performance.now() - this.started);
    // Worker fetches are not in this window's resource timings, so the count is only shown once it
    // says something: a stuck "0.0 MB" reads as a broken page rather than a quiet one.
    const bytes = transferred();
    this.bytesNode.textContent = bytes > 1e5 ? `${megabytes(bytes)} downloaded` : "";
  }
}
