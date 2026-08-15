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

// The playground: the editor on the left owns the option, this file owns the one chart instance on
// the right and everything around it -- which chunks an option needs before it can be drawn, the
// frame stepper, the stream driver, drill-down, the theme (a chart is bound to its theme, so
// switching rebuilds the instance) and the two exporters.

import type { EChartsCoreOption, EChartsType } from "echarts/core";
import { drilldown, startStream, type Stream } from "./behaviours";
import { createEditor, evaluate, type Frames } from "./editor";
import { PRESETS, preset, type Preset } from "./presets";
import { BUNDLE_LABELS, bundlesFor, bundlesForSource, echarts, echartsVersion, isLoaded, load } from "./runtime";
import { echartsTheme, exportBackground, type ThemeName } from "./theme";

const RENDER_DEBOUNCE_MS = 350;
const EDITED_HINT = "Your own option -- it is drawn as you type, and any chunk a series type needs is fetched first.";
const FRAME_INTERVAL_MS = 2_200;

const gallery = document.querySelector<HTMLDivElement>("#gallery")!;
const editorHost = document.querySelector<HTMLDivElement>("#editor")!;
const errorBox = document.querySelector<HTMLPreElement>("#error")!;
const chartHost = document.querySelector<HTMLDivElement>("#chart")!;
const controls = document.querySelector<HTMLDivElement>("#controls")!;
const hint = document.querySelector<HTMLSpanElement>("#hint")!;
const stats = document.querySelector<HTMLSpanElement>("#stats")!;

const editor = createEditor(editorHost);

let chart: EChartsType;
let theme: ThemeName = matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
let active: Preset = preset(location.hash.slice(1) || "line");
let frames: Frames = [];
let frame = 0;
let stream: Stream | null = null;
let autoplay: number | null = null;
let drilled: EChartsCoreOption | null = null;
let lastRenderMs = 0;
let selected: number | null = null;

function button(label: string, onClick: () => void): HTMLButtonElement {
  const element = document.createElement("button");
  element.type = "button";
  element.textContent = label;
  element.addEventListener("click", onClick);
  return element;
}

function showStats(): void {
  const loaded = (["extras", "geo", "gl", "svg"] as const).filter(isLoaded);
  stats.textContent = [
    `${frames.length > 1 ? `frame ${frame + 1}/${frames.length} · ` : ""}render ${lastRenderMs.toFixed(1)} ms`,
    `chunks: ${loaded.length ? loaded.join(", ") : "core only"}`,
    stream ? `streaming · ${stream.samples()} samples` : selected !== null ? `brush: ${selected} points` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

function fail(error: unknown): void {
  errorBox.textContent = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

/** Draw an option, timing what ECharts spends on it. */
function draw(option: EChartsCoreOption, notMerge = true): void {
  const started = performance.now();
  try {
    chart.setOption(option, { notMerge });
  } catch (error) {
    fail(error);
    return;
  }
  lastRenderMs = performance.now() - started;
  // An option with a brush still needs the brush cursor picked before a drag selects anything, and
  // making the visitor find the toolbox first is a poor first impression.
  if (option.brush) {
    chart.dispatchAction({
      type: "takeGlobalCursor",
      key: "brush",
      brushOption: { brushType: "rect", brushMode: "single" },
    });
  }
  showStats();
}

function stopStream(): void {
  stream?.stop();
  stream = null;
}

function stopAutoplay(): void {
  if (autoplay !== null) window.clearInterval(autoplay);
  autoplay = null;
}

function buildChart(): void {
  chart?.dispose();
  chart = echarts.init(chartHost, echartsTheme(theme), { renderer: "canvas" });

  chart.on("click", (params) => {
    if (!active.drilldown || drilled) return;
    const point = params as { name?: string; value?: unknown; seriesType?: string };
    if (point.seriesType !== "bar" || !point.name) return;
    const total = Number(point.value);
    if (!Number.isFinite(total)) return;
    drilled = drilldown(point.name, total);
    draw(drilled);
    buildControls();
  });

  chart.on("brushSelected", (params) => {
    // ECharts also fires this with no areas when a brush is cleared (and once per draw of an option
    // that has a brush at all), which is not a selection of zero points.
    const event = params as { batch?: { areas?: unknown[]; selected?: { dataIndex?: number[] }[] }[] };
    const batch = event.batch?.[0];
    selected = batch?.areas?.length
      ? (batch.selected ?? []).reduce((count, series) => count + (series.dataIndex?.length ?? 0), 0)
      : null;
    showStats();
  });
}

function buildControls(): void {
  controls.replaceChildren();

  if (frames.length > 1) {
    const step = (delta: number) => {
      frame = (frame + delta + frames.length) % frames.length;
      draw(frames[frame]);
      buildControls();
    };
    controls.append(button("\u25c0 Previous frame", () => step(-1)), button("Next frame \u25b6", () => step(1)));
    controls.append(
      button(autoplay === null ? "Auto-play" : "Pause", () => {
        if (autoplay === null) autoplay = window.setInterval(() => step(1), FRAME_INTERVAL_MS);
        else stopAutoplay();
        buildControls();
      }),
    );
  }

  if (active.live) {
    controls.append(
      button(stream ? "Stop stream" : "Start stream", () => {
        if (stream) stopStream();
        else stream = startStream(chart, () => showStats());
        buildControls();
        showStats();
      }),
    );
  }

  if (active.drilldown) {
    const back = button("\u2190 Back", () => {
      drilled = null;
      draw(frames[frame]);
      buildControls();
    });
    back.disabled = !drilled;
    controls.append(back);
  }

  const grow = document.createElement("span");
  grow.className = "grow";
  controls.append(grow, button("Re-run", () => void render(editor.value())));
}

/** Fetch whatever the option needs that is not in the initial bundle, then draw it. */
async function render(source: string): Promise<void> {
  stopStream();
  stopAutoplay();
  selected = null;
  drilled = null;

  let parsed: Frames;
  try {
    parsed = evaluate(source);
  } catch (error) {
    fail(error);
    return;
  }
  errorBox.textContent = "";

  const needed = [...new Set(parsed.flatMap(bundlesFor))].filter((bundle) => !isLoaded(bundle));
  for (const bundle of needed) {
    stats.textContent = `fetching the ${BUNDLE_LABELS[bundle]} chunk\u2026`;
    try {
      await load(bundle);
    } catch (error) {
      fail(error);
      return;
    }
  }
  // A chart instance snapshots the registered series types and pipeline stages when it is created,
  // so a chunk that arrives later is only visible to a fresh instance.
  if (needed.length) buildChart();

  frames = parsed;
  frame = 0;
  draw(frames[0]);
  buildControls();
}

let debounce = 0;
function scheduleRender(source: string): void {
  window.clearTimeout(debounce);
  debounce = window.setTimeout(() => void render(source), RENDER_DEBOUNCE_MS);
}

function selectPreset(next: Preset): void {
  active = next;
  history.replaceState(null, "", `#${next.id}`);
  hint.textContent = next.hint;
  for (const element of gallery.querySelectorAll("button")) {
    element.setAttribute("aria-pressed", String(element.dataset.preset === next.id));
  }
  editor.set(next.code);
  void render(next.code);
}

function buildGallery(): void {
  for (const group of [...new Set(PRESETS.map((entry) => entry.group))]) {
    const heading = document.createElement("h2");
    heading.textContent = group;
    const row = document.createElement("div");
    row.className = "row";

    for (const entry of PRESETS.filter((candidate) => candidate.group === group)) {
      const element = button(entry.label, () => selectPreset(entry));
      element.dataset.preset = entry.id;
      element.setAttribute("aria-pressed", "false");
      const bundles = bundlesForSource(entry.code);
      if (bundles.length) {
        const badge = document.createElement("span");
        badge.className = "chunk";
        badge.textContent = `+${bundles.join(" +")}`;
        badge.title = `loads the ${bundles.map((bundle) => BUNDLE_LABELS[bundle]).join(" and ")} chunk on demand`;
        element.append(badge);
      }
      row.append(element);
    }
    gallery.append(heading, row);
  }
}

function download(href: string, name: string): void {
  const link = document.createElement("a");
  link.href = href;
  link.download = name;
  link.click();
}

function current(): EChartsCoreOption {
  return drilled ?? frames[frame] ?? {};
}

async function exportSvg(): Promise<void> {
  await load("svg");
  // The page draws on canvas, so SVG comes from a throwaway instance of the same size holding the
  // same option -- ECharts can only serialise what an SVG-painter instance drew.
  const host = document.createElement("div");
  host.style.cssText = `position:fixed;left:-10000px;width:${chartHost.clientWidth}px;height:${chartHost.clientHeight}px`;
  document.body.append(host);
  const offscreen = echarts.init(host, echartsTheme(theme), { renderer: "svg" });
  try {
    // Serialising happens immediately, so the entrance animation would be caught part-drawn (bars
    // still at zero height, a line clipped short); and an SVG has no canvas behind it to inherit a
    // background from, so the theme's has to go into the option.
    offscreen.setOption(
      { ...current(), animation: false, backgroundColor: exportBackground(theme) },
      { notMerge: true },
    );
    const svg = offscreen.renderToSVGString();
    download(URL.createObjectURL(new Blob([svg], { type: "image/svg+xml" })), `echarts-${active.id}.svg`);
  } finally {
    offscreen.dispose();
    host.remove();
  }
}

function applyTheme(next: ThemeName): void {
  theme = next;
  document.documentElement.dataset.theme = next;
  document.querySelector<HTMLButtonElement>("#theme")!.textContent =
    next === "dark" ? "Light theme" : "Dark theme";
  const running = stream !== null;
  stopStream();
  buildChart();
  draw(current());
  if (running) stream = startStream(chart, () => showStats());
  buildControls();
}

function main(): void {
  document.querySelector<HTMLSpanElement>("#version")!.textContent = `echarts ${echartsVersion}`;
  document.title = `Apache ECharts ${echartsVersion} in the browser`;

  buildGallery();
  buildChart();
  // Never resize inside an ECharts pass: the observer fires while the chart lays itself out.
  let resizeTimer = 0;
  new ResizeObserver(() => {
    window.clearTimeout(resizeTimer);
    resizeTimer = window.setTimeout(() => chart.resize(), 60);
  }).observe(chartHost);
  window.addEventListener("hashchange", () => {
    const next = location.hash.slice(1);
    if (next && next !== active.id) selectPreset(preset(next));
  });

  document.querySelector<HTMLButtonElement>("#theme")!.addEventListener("click", () => {
    applyTheme(theme === "dark" ? "light" : "dark");
  });
  document.querySelector<HTMLButtonElement>("#png")!.addEventListener("click", () => {
    download(
      chart.getDataURL({ type: "png", pixelRatio: 2, backgroundColor: exportBackground(theme) }),
      `echarts-${active.id}.png`,
    );
  });
  document.querySelector<HTMLButtonElement>("#svg")!.addEventListener("click", () => {
    void exportSvg().catch(fail);
  });

  editor.onChange((source) => {
    // The preset's "what to try" no longer describes an option the visitor has rewritten.
    if (source.trim() !== active.code.trim()) hint.textContent = EDITED_HINT;
    scheduleRender(source);
  });
  document.documentElement.dataset.theme = theme;
  document.querySelector<HTMLButtonElement>("#theme")!.textContent =
    theme === "dark" ? "Light theme" : "Dark theme";
  selectPreset(active);
}

main();
