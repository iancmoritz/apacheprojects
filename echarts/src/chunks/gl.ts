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

// The 3D series, from the echarts-gl extension pack.  It is by far the biggest thing this page can
// load (claygl and the shaders), which is exactly why it is a chunk of its own: the presets that do
// not need WebGL never fetch it.  echarts-gl registers itself into the same module registry
// `echarts/core` uses, so it does not pull in a second copy of ECharts.

import * as echarts from "echarts/core";
import { Bar3DChart, Line3DChart, Scatter3DChart, SurfaceChart } from "echarts-gl/charts";
import { Grid3DComponent } from "echarts-gl/components";

echarts.use(Grid3DComponent);
echarts.use(SurfaceChart);
echarts.use(Bar3DChart);
echarts.use(Scatter3DChart);
echarts.use(Line3DChart);
