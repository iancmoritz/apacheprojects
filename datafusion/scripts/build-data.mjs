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

// The two tables the page registers at boot.  cities.csv is text and lives in git; trips.parquet is
// a real Parquet file (row groups, dictionary pages, statistics), so it is generated here by
// crates/mkdata instead of being committed.

import { spawnSync } from "node:child_process";
import { mkdir, cp, stat } from "node:fs/promises";
import { join } from "node:path";

const HERE = join(import.meta.dirname, "..");
const OUT = join(HERE, "public", "_datafusion", "data");
const CITIES = join(HERE, "data", "cities.csv");
const TRIPS = join(OUT, "trips.parquet");
const ROWS = "120000";

await mkdir(OUT, { recursive: true });
await cp(CITIES, join(OUT, "cities.csv"));

const result = spawnSync("cargo", ["run", "--release", "-q", "-p", "mkdata", "--", CITIES, TRIPS, ROWS], {
  stdio: "inherit",
  cwd: HERE,
});
if (result.error?.code === "ENOENT") throw new Error("cargo was not found on PATH; see scripts/build-wasm.mjs");
if (result.status !== 0) throw new Error(`mkdata exited ${result.status}`);

const sizes = await Promise.all(
  [join(OUT, "cities.csv"), TRIPS].map(async (path) => `${path.split("/").pop()} ${(await stat(path)).size} B`),
);
console.log(`public/_datafusion/data: ${sizes.join(", ")}`);
