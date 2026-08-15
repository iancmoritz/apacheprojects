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

// The gallery.  Every preset is the source the editor loads, so what runs is exactly what is on
// screen -- no hidden option is merged in.  Each one evaluates to an ECharts option, or to an array
// of options the page steps through (that is how the transition preset morphs one chart into
// another), and data is generated in the tab from the seeded `random` in the editor's scope so the
// series are real data rather than a literal pasted in.

/** A gallery entry. `code` is loaded verbatim into the editor. */
export interface Preset {
  id: string;
  label: string;
  group: string;
  /** Prose shown under the chart: what to try. */
  hint: string;
  /** The stream driver appends points to this preset's series. */
  live?: boolean;
  /** Clicking a bar drills into the category. */
  drilldown?: boolean;
  code: string;
}

export const PRESETS: Preset[] = [
  {
    id: "line",
    label: "Line + dataZoom",
    group: "Basics",
    hint: "Scroll or pinch inside the chart to zoom, drag the slider under it to pan, hover for the axis tooltip.",
    code: `{
  title: { text: 'Request latency', subtext: '400 days generated in this tab' },
  tooltip: { trigger: 'axis' },
  legend: { top: 4, right: 8 },
  grid: { left: 56, right: 24, top: 64, bottom: 84 },
  xAxis: {
    type: 'category',
    boundaryGap: false,
    data: Array.from({ length: 400 }, (_, i) =>
      new Date(Date.UTC(2025, 0, 1) + i * 864e5).toISOString().slice(0, 10)),
  },
  yAxis: { type: 'value', name: 'ms' },
  dataZoom: [
    { type: 'inside', start: 55, end: 85 },
    { type: 'slider', bottom: 28, height: 26 },
  ],
  series: ['p50', 'p95'].map((name, s) => {
    const next = random(11 + s);
    let value = 90 + s * 140;
    return {
      name: name,
      type: 'line',
      smooth: true,
      showSymbol: false,
      areaStyle: s === 0 ? { opacity: 0.12 } : undefined,
      data: Array.from({ length: 400 }, () => {
        value = Math.max(20, value + (next() - 0.5) * 22);
        return Math.round(value);
      }),
    };
  }),
}`,
  },
  {
    id: "bar",
    label: "Bar with drill-down",
    group: "Basics",
    drilldown: true,
    hint: "Click any bar to drill into that region's twelve months; the bars morph into the detail and back.",
    code: `{
  title: { text: 'Revenue by region', subtext: 'click a bar to drill down' },
  tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
  grid: { left: 64, right: 24, top: 64, bottom: 48 },
  xAxis: { type: 'category', data: ['EMEA', 'APAC', 'LATAM', 'NA', 'ANZ'] },
  yAxis: { type: 'value', name: 'k$' },
  series: [{
    type: 'bar',
    id: 'total',
    barWidth: '48%',
    universalTransition: { enabled: true, divideShape: 'clone' },
    itemStyle: { borderRadius: [6, 6, 0, 0] },
    label: { show: true, position: 'top' },
    data: (() => {
      const next = random(3);
      return ['EMEA', 'APAC', 'LATAM', 'NA', 'ANZ'].map((name) => ({
        name: name,
        value: Math.round(200 + next() * 800),
        groupId: name,
      }));
    })(),
  }],
}`,
  },
  {
    id: "pie",
    label: "Donut",
    group: "Basics",
    hint: "Click a slice to select it, click the legend to filter, hover for the share.",
    code: `{
  title: { text: 'Where the bytes go', left: 'center' },
  tooltip: { trigger: 'item', formatter: '{b}: {c} MB ({d}%)' },
  legend: { bottom: 8, left: 'center' },
  series: [{
    type: 'pie',
    radius: ['42%', '68%'],
    center: ['50%', '48%'],
    selectedMode: 'multiple',
    itemStyle: { borderWidth: 2, borderColor: 'transparent' },
    label: { formatter: '{b}\\n{d}%' },
    data: [
      { name: 'wasm', value: 41.2 },
      { name: 'wheels', value: 18.9 },
      { name: 'JavaScript', value: 12.4 },
      { name: 'fonts', value: 3.1 },
      { name: 'images', value: 6.7 },
    ],
  }],
}`,
  },
  {
    id: "scatter",
    label: "Scatter + brush",
    group: "Basics",
    hint: "Drag a rectangle over the points -- the brush is already armed; the selected count is reported under the chart, and the toolbox on the right adds a lasso, keep and clear.",
    code: `{
  title: { text: '2 000 samples', subtext: 'brush a region to select points' },
  tooltip: { trigger: 'item', formatter: (p) => p.value[0].toFixed(2) + ', ' + p.value[1].toFixed(2) },
  grid: { left: 56, right: 24, top: 64, bottom: 56 },
  toolbox: { right: 8, feature: { brush: { type: ['rect', 'polygon', 'keep', 'clear'] } } },
  brush: { throttleType: 'debounce', throttleDelay: 120 },
  xAxis: { type: 'value', scale: true },
  yAxis: { type: 'value', scale: true },
  visualMap: {
    dimension: 1, min: -3, max: 3, right: 8, top: 'middle', calculable: true,
    inRange: { color: ['#017cee', '#00b8a9', '#f2a03d', '#c7373b'] },
  },
  series: [{
    type: 'scatter',
    symbolSize: 6,
    itemStyle: { opacity: 0.7 },
    data: (() => {
      const next = random(23);
      const normal = () => Math.sqrt(-2 * Math.log(next() + 1e-9)) * Math.cos(2 * Math.PI * next());
      return Array.from({ length: 2000 }, () => {
        const x = normal();
        return [x, x * 0.6 + normal() * 0.8];
      });
    })(),
  }],
}`,
  },
  {
    id: "heatmap",
    label: "Heatmap",
    group: "More types",
    hint: "Drag the visualMap handles to re-map the colour scale; hover a cell for its value. Loads the extra-charts chunk.",
    code: `{
  title: { text: 'Deploys by hour', subtext: 'hour of day x day of week' },
  tooltip: { position: 'top' },
  grid: { left: 72, right: 96, top: 64, bottom: 56 },
  xAxis: { type: 'category', splitArea: { show: true },
    data: Array.from({ length: 24 }, (_, h) => String(h).padStart(2, '0') + ':00') },
  yAxis: { type: 'category', splitArea: { show: true },
    data: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] },
  visualMap: { min: 0, max: 40, calculable: true, right: 8, top: 'middle',
    inRange: { color: ['#e8eef5', '#017cee', '#7a5cff', '#c7373b'] } },
  series: [{
    type: 'heatmap',
    label: { show: false },
    emphasis: { itemStyle: { shadowBlur: 8, shadowColor: 'rgba(0,0,0,0.3)' } },
    data: (() => {
      const next = random(5);
      const cells = [];
      for (let day = 0; day < 7; day++) {
        for (let hour = 0; hour < 24; hour++) {
          const office = hour > 8 && hour < 19 && day < 5 ? 1 : 0.15;
          cells.push([hour, day, Math.round(next() * 40 * office)]);
        }
      }
      return cells;
    })(),
  }],
}`,
  },
  {
    id: "graph",
    label: "Force graph",
    group: "More types",
    hint: "Drag a node and the whole layout reacts; scroll to zoom, drag the background to pan.",
    code: `{
  title: { text: 'A service mesh, laid out by force' },
  tooltip: {},
  series: [{
    type: 'graph',
    layout: 'force',
    roam: true,
    draggable: true,
    label: { show: true, position: 'right' },
    force: { repulsion: 220, edgeLength: [40, 120], gravity: 0.08 },
    emphasis: { focus: 'adjacency', lineStyle: { width: 4 } },
    lineStyle: { color: 'source', curveness: 0.15, opacity: 0.6 },
    categories: [{ name: 'edge' }, { name: 'service' }, { name: 'store' }],
    data: (() => {
      const next = random(17);
      const names = ['gateway', 'auth', 'accounts', 'billing', 'search', 'catalog', 'orders',
        'shipping', 'events', 'cache', 'postgres', 'blobs', 'workers', 'metrics', 'cron'];
      return names.map((name, i) => ({
        id: String(i), name: name,
        symbolSize: 14 + Math.round(next() * 26),
        category: i === 0 ? 0 : name === 'postgres' || name === 'blobs' || name === 'cache' ? 2 : 1,
      }));
    })(),
    links: (() => {
      const next = random(29);
      const links = [];
      for (let target = 1; target < 15; target++) {
        links.push({ source: String(Math.floor(next() * target)), target: String(target) });
        if (next() > 0.6) links.push({ source: String(target), target: String(10 + Math.floor(next() * 2)) });
      }
      return links;
    })(),
  }],
}`,
  },
  {
    id: "treemap",
    label: "Treemap",
    group: "More types",
    hint: "Click a block to zoom into that subtree, use the breadcrumb at the bottom to come back up.",
    code: `{
  title: { text: 'Bundle, by directory' },
  tooltip: { formatter: (p) => p.name + ': ' + p.value + ' KiB' },
  series: [{
    type: 'treemap',
    roam: true,
    leafDepth: 2,
    upperLabel: { show: true, height: 22 },
    levels: [
      { itemStyle: { borderWidth: 3, borderColor: 'transparent', gapWidth: 3 } },
      { itemStyle: { borderWidth: 2, gapWidth: 2 }, colorSaturation: [0.35, 0.6] },
      { itemStyle: { gapWidth: 1 }, colorSaturation: [0.3, 0.5] },
    ],
    data: (() => {
      const next = random(41);
      const leaf = (name) => ({ name: name, value: 4 + Math.round(next() * 90) });
      const dir = (name, children) => ({ name: name, children: children.map(leaf) });
      return [
        dir('echarts/core', ['echarts.js', 'model', 'view', 'coord', 'scale']),
        dir('echarts/charts', ['line', 'bar', 'pie', 'scatter', 'heatmap', 'graph', 'treemap']),
        dir('echarts/components', ['grid', 'tooltip', 'legend', 'dataZoom', 'brush', 'visualMap']),
        dir('zrender', ['Painter', 'Element', 'graphic', 'animation', 'svg']),
        dir('playground', ['main.ts', 'presets.ts', 'editor.ts', 'behaviours.ts']),
      ];
    })(),
  }],
}`,
  },
  {
    id: "candlestick",
    label: "Candlestick",
    group: "More types",
    hint: "Zoom with the slider; the dashed line is the mean close, the shaded band the drawdown window.",
    code: `(() => {
  const next = random(59);
  const days = 240;
  const bars = [];
  let close = 128;
  for (let i = 0; i < days; i++) {
    const open = close;
    close = Math.max(8, open * (1 + (next() - 0.5) * 0.07));
    bars.push([
      open.toFixed(2), close.toFixed(2),
      (Math.min(open, close) * (1 - next() * 0.03)).toFixed(2),
      (Math.max(open, close) * (1 + next() * 0.03)).toFixed(2),
    ]);
  }
  const mean = bars.reduce((sum, b) => sum + Number(b[1]), 0) / bars.length;
  const window = 5;
  const ma = bars.map((_, i) =>
    i < window ? '-' : (bars.slice(i - window, i).reduce((s, b) => s + Number(b[1]), 0) / window).toFixed(2));
  return {
    title: { text: 'ASF Corp (fictional)', subtext: days + ' generated sessions' },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { top: 4, right: 8 },
    grid: { left: 64, right: 24, top: 64, bottom: 84 },
    xAxis: { type: 'category', data: Array.from({ length: days }, (_, i) =>
      new Date(Date.UTC(2025, 0, 2) + i * 864e5).toISOString().slice(0, 10)) },
    yAxis: { type: 'value', scale: true, name: '$' },
    dataZoom: [{ type: 'inside', start: 60, end: 100 }, { type: 'slider', bottom: 28, height: 26 }],
    series: [
      { name: 'OHLC', type: 'candlestick', data: bars,
        markLine: { symbol: 'none', data: [{ yAxis: mean.toFixed(2), label: { formatter: 'mean' } }] } },
      { name: 'MA5', type: 'line', data: ma, smooth: true, showSymbol: false, lineWidth: 1 },
    ],
  };
})()`,
  },
  {
    id: "geo",
    label: "World map",
    group: "Loads on demand",
    hint: "Scroll to zoom the map, drag to pan, hover a country. The map series and Natural Earth's boundaries are a separate chunk, fetched when you pick this preset.",
    code: `{
  title: { text: 'Something per country', subtext: 'Natural Earth 1:110m, converted at build time' },
  tooltip: { trigger: 'item', formatter: (p) => p.name + ': ' + (p.value == null ? 'no data' : p.value) },
  visualMap: {
    min: 0, max: 100, left: 16, bottom: 24, calculable: true, text: ['high', 'low'],
    inRange: { color: ['#e8eef5', '#00b8a9', '#017cee', '#7a5cff'] },
  },
  series: [{
    type: 'map',
    map: 'world',
    roam: true,
    projection: undefined,
    itemStyle: { borderColor: 'rgba(120,140,160,0.5)', borderWidth: 0.5 },
    emphasis: { label: { show: true }, itemStyle: { areaColor: '#f2a03d' } },
    data: (() => {
      const next = random(83);
      const names = ['United States of America', 'Brazil', 'Germany', 'France', 'Spain', 'Italy',
        'Nigeria', 'Egypt', 'South Africa', 'India', 'China', 'Japan', 'Indonesia', 'Australia',
        'Canada', 'Mexico', 'Argentina', 'Chile', 'Norway', 'Sweden', 'Finland', 'Poland',
        'Ukraine', 'Turkey', 'Saudi Arabia', 'Kenya', 'Ethiopia', 'Vietnam', 'Thailand', 'Kazakhstan'];
      return names.map((name) => ({ name: name, value: Math.round(next() * 100) }));
    })(),
  }],
}`,
  },
  {
    id: "gl",
    label: "3D surface (WebGL)",
    group: "Loads on demand",
    hint: "Drag to orbit, scroll to zoom. Picking this preset fetches echarts-gl, which is the largest chunk on the page -- watch the network panel.",
    code: `{
  title: { text: 'sin(x) * cos(y), on the GPU' },
  tooltip: {},
  visualMap: {
    show: false, dimension: 2, min: -1, max: 1,
    inRange: { color: ['#017cee', '#00b8a9', '#f2a03d', '#c7373b'] },
  },
  xAxis3D: { type: 'value' },
  yAxis3D: { type: 'value' },
  zAxis3D: { type: 'value' },
  grid3D: {
    viewControl: { autoRotate: true, autoRotateSpeed: 8, distance: 190 },
    light: { main: { intensity: 1.2, shadow: true }, ambient: { intensity: 0.3 } },
  },
  series: [{
    type: 'surface',
    wireframe: { show: false },
    equation: {
      x: { step: 0.06, min: -3, max: 3 },
      y: { step: 0.06, min: -3, max: 3 },
      z: (x, y) => Math.sin(x * 2) * Math.cos(y * 2) * (1 - Math.min(1, Math.hypot(x, y) / 3.2)),
    },
  }],
}`,
  },
  {
    id: "stream",
    label: "Live stream",
    group: "Live",
    live: true,
    hint: "Press start: the driver generates a sample every 120 ms in this tab and pushes it into the series, dropping the oldest point.",
    code: `{
  title: { text: 'Two signals, sampled live', subtext: 'generated in the tab, not fetched' },
  tooltip: { trigger: 'axis' },
  legend: { top: 4, right: 8 },
  animation: false,
  grid: { left: 56, right: 24, top: 64, bottom: 48 },
  xAxis: { type: 'category', boundaryGap: false, data: Array.from({ length: 120 }, (_, i) => String(i)) },
  yAxis: { type: 'value', min: 0, max: 100, name: '%' },
  series: [
    { name: 'cpu', type: 'line', smooth: true, showSymbol: false, areaStyle: { opacity: 0.15 },
      data: Array.from({ length: 120 }, (_, i) => Math.round(45 + Math.sin(i / 9) * 18)) },
    { name: 'memory', type: 'line', smooth: true, showSymbol: false,
      data: Array.from({ length: 120 }, (_, i) => Math.round(70 + Math.cos(i / 14) * 8)) },
  ],
}`,
  },
  {
    id: "transition",
    label: "Morphing frames",
    group: "Live",
    hint: "This preset evaluates to an array of options, so the frame buttons under the chart step between them -- ECharts' universalTransition morphs the bars into the slices and into the next dataset.",
    code: `(() => {
  const next = random(97);
  const products = ['wasm', 'wheels', 'js', 'fonts', 'images'];
  const dataset = (seed) => products.map((name) => ({ name: name, value: Math.round(10 + seed() * 90) }));
  const first = dataset(next);
  const second = dataset(next);
  return [
    {
      title: { text: 'Frame 1 - bars' },
      tooltip: {},
      grid: { left: 56, right: 24, top: 64, bottom: 48 },
      xAxis: { type: 'category', data: products },
      yAxis: { type: 'value' },
      series: [{ id: 'shape', type: 'bar', barWidth: '50%', data: first,
        universalTransition: { enabled: true, divideShape: 'clone' },
        itemStyle: { borderRadius: [6, 6, 0, 0] }, label: { show: true, position: 'top' } }],
    },
    {
      title: { text: 'Frame 2 - the same numbers, as a donut' },
      tooltip: { trigger: 'item' },
      series: [{ id: 'shape', type: 'pie', radius: ['40%', '66%'], data: first,
        universalTransition: { enabled: true }, label: { formatter: '{b} {d}%' } }],
    },
    {
      title: { text: 'Frame 3 - a new dataset, morphed into' },
      tooltip: { trigger: 'item' },
      series: [{ id: 'shape', type: 'pie', radius: ['40%', '66%'], roseType: 'radius', data: second,
        universalTransition: { enabled: true }, label: { formatter: '{b} {d}%' } }],
    },
  ];
})()`,
  },
];

export function preset(id: string): Preset {
  const found = PRESETS.find((entry) => entry.id === id);
  if (!found) throw new Error(`no preset ${id}`);
  return found;
}
