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

// What the first paint costs is decided here: this module registers the charts and components the
// page opens with, and every heavier family (the extra chart types, the world map, WebGL, the SVG
// painter) lives behind a dynamic import in chunks/ that is only fetched once an option asks for it.

import * as echarts from "echarts/core";
import { BarChart, LineChart } from "echarts/charts";
import {
  BrushComponent,
  DataZoomComponent,
  DatasetComponent,
  GraphicComponent,
  GridComponent,
  LegendComponent,
  MarkAreaComponent,
  MarkLineComponent,
  TitleComponent,
  ToolboxComponent,
  TooltipComponent,
  TransformComponent,
} from "echarts/components";
import { LabelLayout, UniversalTransition } from "echarts/features";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([
  LineChart,
  BarChart,
  GridComponent,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
  ToolboxComponent,
  BrushComponent,
  MarkLineComponent,
  MarkAreaComponent,
  DatasetComponent,
  TransformComponent,
  GraphicComponent,
  LabelLayout,
  UniversalTransition,
  CanvasRenderer,
]);

export { echarts };
export const echartsVersion = echarts.version;

/** A family of ECharts modules that is fetched on demand. */
export type Bundle = "extras" | "geo" | "gl" | "svg";

export const BUNDLE_LABELS: Record<Bundle, string> = {
  extras: "extra chart types",
  geo: "geo map",
  gl: "WebGL (echarts-gl)",
  svg: "SVG painter",
};

const loaders: Record<Bundle, () => Promise<unknown>> = {
  extras: () => import("./chunks/extras"),
  geo: () => import("./chunks/geo"),
  gl: () => import("./chunks/gl"),
  svg: () => import("./chunks/svg"),
};

const loading = new Map<Bundle, Promise<unknown>>();

export function isLoaded(bundle: Bundle): boolean {
  return loading.has(bundle);
}

/** Fetch and install a bundle, at most once per page. */
export function load(bundle: Bundle): Promise<unknown> {
  let started = loading.get(bundle);
  if (!started) {
    started = loaders[bundle]();
    loading.set(bundle, started);
  }
  return started;
}

// Series types (and the components they imply) that are not in the first chunk.  Anything typed into
// the editor is looked up here too, so a hand-written `type: "treemap"` loads what it needs as well.
const SERIES_BUNDLES: Record<string, Bundle> = {
  pie: "extras",
  scatter: "extras",
  effectScatter: "extras",
  heatmap: "extras",
  graph: "extras",
  treemap: "extras",
  sunburst: "extras",
  candlestick: "extras",
  boxplot: "extras",
  map: "geo",
  bar3D: "gl",
  line3D: "gl",
  scatter3D: "gl",
  surface: "gl",
};

type LooseOption = Record<string, unknown> & { series?: unknown };

/**
 * The bundles a piece of source *looks* like it needs, for labelling the gallery without evaluating
 * every preset at startup. Rendering never trusts this -- `bundlesFor` reads the real option.
 */
export function bundlesForSource(source: string): Bundle[] {
  const needed = new Set<Bundle>();
  for (const [type, bundle] of Object.entries(SERIES_BUNDLES)) {
    if (new RegExp(`type:\\s*['"]${type}['"]`).test(source)) needed.add(bundle);
  }
  if (/visualMap:/.test(source)) needed.add("extras");
  if (/grid3D:|globe:/.test(source)) needed.add("gl");
  return [...needed];
}

/** Which bundles an option needs before ECharts can draw it. */
export function bundlesFor(option: unknown): Bundle[] {
  const needed = new Set<Bundle>();
  const root = (option ?? {}) as LooseOption;

  const series = Array.isArray(root.series) ? root.series : root.series ? [root.series] : [];
  for (const entry of series) {
    const type = (entry as { type?: unknown } | null)?.type;
    if (typeof type === "string" && SERIES_BUNDLES[type]) needed.add(SERIES_BUNDLES[type]);
  }

  if (root.visualMap) needed.add("extras");
  if (root.geo) needed.add("geo");
  if (root.grid3D || root.globe || root.geo3D) needed.add("gl");

  return [...needed];
}
