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

// Two registered ECharts themes matching the two page palettes.  A theme is bound when a chart is
// created, so switching one disposes the instance and builds it again -- which is also the honest way
// to show what a theme actually changes, since nothing in the option is touched.

import { echarts } from "./runtime";

export type ThemeName = "light" | "dark";

const PALETTE = [
  "#c7373b",
  "#017cee",
  "#00b8a9",
  "#f2a03d",
  "#7a5cff",
  "#e35d9b",
  "#4caf50",
  "#8d99ae",
];

function theme(ink: string, muted: string, line: string, split: string, page: string): object {
  return {
    color: PALETTE,
    backgroundColor: "transparent",
    textStyle: { fontFamily: "ui-sans-serif, system-ui, sans-serif" },
    title: { textStyle: { color: ink }, subtextStyle: { color: muted } },
    legend: { textStyle: { color: muted } },
    tooltip: {
      backgroundColor: page,
      borderColor: line,
      textStyle: { color: ink },
      axisPointer: { lineStyle: { color: muted }, crossStyle: { color: muted } },
    },
    categoryAxis: axis(ink, muted, line, split),
    valueAxis: axis(ink, muted, line, split),
    logAxis: axis(ink, muted, line, split),
    timeAxis: axis(ink, muted, line, split),
    visualMap: { textStyle: { color: muted } },
    dataZoom: {
      borderColor: line,
      textStyle: { color: muted },
      dataBackground: { lineStyle: { color: muted }, areaStyle: { color: muted } },
      handleStyle: { color: page, borderColor: muted },
    },
    toolbox: { iconStyle: { borderColor: muted } },
    geo: { itemStyle: { areaColor: split, borderColor: line }, emphasis: { itemStyle: { areaColor: "#f2a03d" } } },
  };
}

function axis(ink: string, muted: string, line: string, split: string): object {
  return {
    axisLine: { lineStyle: { color: line } },
    axisTick: { lineStyle: { color: line } },
    axisLabel: { color: muted },
    splitLine: { lineStyle: { color: split } },
    nameTextStyle: { color: ink },
    splitArea: { areaStyle: { color: ["transparent", split] } },
  };
}

echarts.registerTheme("playground-light", theme("#10202e", "#5b6b7a", "#c9d5e1", "#e8eef5", "#ffffff"));
echarts.registerTheme("playground-dark", theme("#eef4fa", "#9fb0c0", "#31414f", "#1e2a36", "#16202c"));

export function echartsTheme(name: ThemeName): string {
  return name === "dark" ? "playground-dark" : "playground-light";
}

/** The background an exported image needs, since the themes themselves are transparent. */
export function exportBackground(name: ThemeName): string {
  return name === "dark" ? "#0e1620" : "#ffffff";
}
