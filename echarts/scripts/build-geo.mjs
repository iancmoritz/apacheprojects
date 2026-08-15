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

// The geo presets need a map ECharts can register, which is a GeoJSON FeatureCollection whose
// feature names are the values the series is keyed by.  Natural Earth's country boundaries ship as
// TopoJSON in the `world-atlas` package, so this converts that once at build time instead of
// vendoring a copy of the map into git or fetching one from a CDN in the visitor's tab.

import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const OUT = join(ROOT, "public", "_echarts", "world.geo.json");

const { feature } = require("topojson-client");
const topology = require("world-atlas/countries-110m.json");

const collection = feature(topology, topology.objects.countries);

// ECharts matches `series.data[].name` against the feature name, and the map is only useful if the
// names are the ones the presets use, so keep just the name and drop everything else.
collection.features = collection.features.map((f) => ({
  type: "Feature",
  properties: { name: f.properties.name },
  geometry: f.geometry,
}));

await mkdir(dirname(OUT), { recursive: true });
await writeFile(OUT, JSON.stringify(collection));

const bytes = JSON.stringify(collection).length;
console.log(`wrote public/_echarts/world.geo.json (${collection.features.length} features, ${(bytes / 1024).toFixed(0)} KiB)`);
