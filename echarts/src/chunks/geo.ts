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

// The map series plus the one map this page registers: Natural Earth's country boundaries, converted
// from the `world-atlas` package's TopoJSON at build time by scripts/build-geo.mjs and served from
// this origin like everything else here.

import * as echarts from "echarts/core";
import { MapChart } from "echarts/charts";
import { GeoComponent } from "echarts/components";
import { VisualMapComponent } from "echarts/components";

echarts.use([MapChart, GeoComponent, VisualMapComponent]);

const response = await fetch(new URL("/_echarts/world.geo.json", import.meta.url));
if (!response.ok) throw new Error(`the world map is missing (${response.status})`);
echarts.registerMap("world", await response.json());

export const MAP_NAME = "world";
