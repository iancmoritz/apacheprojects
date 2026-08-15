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

// Every chart type past the line and bar the page opens with, plus the pieces those presets use.

import * as echarts from "echarts/core";
import {
  BoxplotChart,
  CandlestickChart,
  EffectScatterChart,
  GraphChart,
  HeatmapChart,
  PieChart,
  ScatterChart,
  SunburstChart,
  TreemapChart,
} from "echarts/charts";
import { CalendarComponent, SingleAxisComponent, VisualMapComponent } from "echarts/components";

echarts.use([
  PieChart,
  ScatterChart,
  EffectScatterChart,
  HeatmapChart,
  GraphChart,
  TreemapChart,
  SunburstChart,
  CandlestickChart,
  BoxplotChart,
  VisualMapComponent,
  CalendarComponent,
  SingleAxisComponent,
]);
