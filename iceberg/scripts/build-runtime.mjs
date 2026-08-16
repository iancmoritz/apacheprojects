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

// Everything Python needs at runtime, resolved here in Node and written into
// public/_iceberg/: the Pyodide interpreter and the wheels for PyIceberg, DuckDB and Arrow.  The
// browser then boots from this origin only -- no CDN, no PyPI, no dependency resolution at boot.
//
// PyIceberg, PyArrow and DuckDB are all part of the Pyodide distribution, so the wheels come from
// the Pyodide release the `pyodide` npm package pins and the dependency closure is read out of its
// lock file; only the packages this app imports are kept (25 of 354).

import { access, cp, mkdir, readdir, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { argv } from "node:process";

const require = createRequire(import.meta.url);
const pyodideDir = dirname(require.resolve("pyodide"));
const { version } = require("pyodide/package.json");
const lock = require("pyodide/pyodide-lock.json");

const root = join(import.meta.dirname, "..");
const runtime = join(root, "public", "_iceberg");
const interpreter = join(runtime, "pyodide");
const wheels = join(runtime, "wheels");
const cdn = `https://cdn.jsdelivr.net/pyodide/v${version}/full/`;

/** The interpreter files Pyodide fetches by URL; the npm distribution ships no wheels. */
const KEEP = /^(pyodide.*\.(m?js|json|wasm|ts)|python_stdlib\.zip)$/;

/** Imported by python/iceberg_wasm/; everything else follows from the lock file's dependencies. */
const PACKAGES = ["pyiceberg", "sqlalchemy", "pyarrow", "duckdb"];

const normalize = (name) => name.toLowerCase().replaceAll("_", "-");
const canonical = new Map(Object.keys(lock.packages).map((name) => [normalize(name), name]));

/** `PACKAGES` plus everything they depend on, as lock file keys. */
function closure(names) {
  const found = new Set();
  const pending = names.map(normalize);
  while (pending.length) {
    const name = pending.pop();
    const key = canonical.get(name);
    if (!key) throw new Error(`${name} is not in the Pyodide ${version} distribution`);
    if (found.has(key)) continue;
    found.add(key);
    pending.push(...lock.packages[key].depends.map(normalize));
  }
  return [...found];
}

async function exists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function download(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: ${response.status}`);
  return Buffer.from(await response.arrayBuffer());
}

const lockPath = join(wheels, "pyodide-lock.json");
if (!argv.includes("--force") && (await exists(lockPath))) {
  console.log("public/_iceberg is already populated; pass --force to rebuild");
  process.exit(0);
}

await rm(runtime, { recursive: true, force: true });
await mkdir(interpreter, { recursive: true });
await mkdir(wheels, { recursive: true });

for (const name of await readdir(pyodideDir)) {
  if (KEEP.test(name)) await cp(join(pyodideDir, name), join(interpreter, name));
}

const keep = closure(PACKAGES);
const packages = {};
let bytes = 0;
for (const key of keep) {
  const pkg = { ...lock.packages[key] };
  const file = pkg.file_name.slice(pkg.file_name.lastIndexOf("/") + 1);
  const content = await download(cdn + file);
  await writeFile(join(wheels, file), content);
  bytes += content.length;
  // Resolved against packageBaseUrl (= /_iceberg/wheels/) by the browser.
  pkg.file_name = file;
  packages[key] = pkg;
}

// The distribution's lock file describes all 354 packages; the browser asks for every package in
// this one by name, so it has to list exactly what we shipped.
await writeFile(lockPath, JSON.stringify({ ...lock, packages }));

console.log(
  `wrote ${keep.length} wheels (${(bytes / 1e6).toFixed(1)} MB) and the interpreter to public/_iceberg`,
);
