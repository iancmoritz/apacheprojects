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

// The two things a static option cannot show on its own: data arriving over time, and a chart that
// answers a click with a different chart.  Both drive the same instance the editor renders into, so
// what they do is visible in the option the chart is holding, not hidden in a parallel state.

import type { EChartsCoreOption, EChartsType } from "echarts/core";
import { random } from "./editor";

const STREAM_INTERVAL_MS = 120;

/** A running stream, appending a generated sample to every series on an interval. */
export interface Stream {
  stop(): void;
  samples(): number;
}

type Numeric = number | string;

/**
 * Push one generated sample per series per tick and drop the oldest, keeping the window the option
 * arrived with. The walk starts from the series' own last value, so editing the option and starting
 * the stream again continues from whatever is on screen.
 */
export function startStream(chart: EChartsType, onSample: (count: number) => void): Stream {
  const option = chart.getOption() as {
    xAxis?: { data?: Numeric[] }[];
    series?: { data?: Numeric[] }[];
  };
  const labels = [...(option.xAxis?.[0]?.data ?? [])].map(Number);
  const series = (option.series ?? []).map((entry) => [...(entry.data ?? [])].map(Number));
  const next = random(Date.now() & 0xffff);
  let samples = 0;

  const timer = window.setInterval(() => {
    samples++;
    labels.push((labels.at(-1) ?? 0) + 1);
    labels.shift();
    for (const values of series) {
      const last = values.at(-1) ?? 50;
      values.push(Math.round(Math.min(100, Math.max(0, last + (next() - 0.5) * 12))));
      values.shift();
    }
    chart.setOption({
      xAxis: [{ data: labels }],
      series: series.map((data) => ({ data })),
    });
    onSample(samples);
  }, STREAM_INTERVAL_MS);

  return {
    stop: () => window.clearInterval(timer),
    samples: () => samples,
  };
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/**
 * The chart to show when a bar is clicked: the clicked category's total, split across twelve months.
 * The series keeps the id and `universalTransition` of the one it replaces, which is what lets
 * ECharts morph the single bar into the twelve rather than redraw.
 */
export function drilldown(name: string, total: number): EChartsCoreOption {
  const next = random([...name].reduce((hash, character) => hash * 31 + character.charCodeAt(0), 7));
  const weights = MONTHS.map(() => 0.5 + next());
  const sum = weights.reduce((a, b) => a + b, 0);

  return {
    title: { text: `${name}, by month`, subtext: "drilled down -- Back returns to the regions" },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    grid: { left: 64, right: 24, top: 64, bottom: 48 },
    xAxis: { type: "category", data: MONTHS },
    yAxis: { type: "value", name: "k$" },
    series: [
      {
        type: "bar",
        id: "total",
        barWidth: "60%",
        universalTransition: { enabled: true, divideShape: "clone" },
        itemStyle: { borderRadius: [4, 4, 0, 0] },
        data: weights.map((weight, index) => ({
          name: MONTHS[index],
          groupId: name,
          value: Math.round((total * weight) / sum),
        })),
      },
    ],
  };
}
