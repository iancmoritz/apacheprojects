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

// Resolve Superset's 130-wheel dependency tree once, here in Node, and write the wheels plus a
// Pyodide lock file into public/_superset/wheels.  The browser then installs Superset entirely from
// our own origin: no PyPI, no Pyodide CDN and no dependency resolution at boot.
//
// Three things make Superset harder to resolve than a normal package (see python/superset_wasm/
// wheels.py for the details):
//
//   * upstream pins versions that have no wasm build (pandas 2.1, pyarrow 16, cryptography 46, ...),
//     while wasm has exactly one build of each -- whatever the Pyodide distribution contains;
//   * SQLAlchemy 1.4 and WTForms-JSON publish no pure-Python wheel, so they are repackaged from their
//     sdists (SQLAlchemy's C extensions are optional, WTForms-JSON is pure to begin with);
//   * the released wheel is 103 MB, 100 MB of which is the frontend bundle that Python never opens.
//     That part is written out as ordinary files the SPA fetches over HTTP.
//
// Versions for everything else come from Superset's own requirements/base.txt for the release being
// built, so the tree is the one upstream tests, not whatever PyPI has today.

import { createHash } from "node:crypto";
import { access, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { argv } from "node:process";

import { loadPyodide } from "pyodide";

const require = createRequire(import.meta.url);
const root = join(import.meta.dirname, "..");
const publicDir = join(root, "public", "_superset");
// Wheels sit next to the interpreter: Pyodide 0.27 resolves the file names in a lock file against
// indexURL, so this is the only directory the browser will look in.
const wheelDir = join(publicDir, "pyodide");
const assetDir = join(publicDir, "static", "assets");
// The lock file copy-pyodide.mjs lays down describes the npm distribution; this script replaces it
// with one that also has Superset's 125 wheels in it.
const lockPath = join(wheelDir, "pyodide-lock.json");
const pyodideDir = dirname(require.resolve("pyodide"));
const PYODIDE_CDN = `https://cdn.jsdelivr.net/pyodide/v${require("pyodide/package.json").version}/full/`;

const SUPERSET_VERSION = "6.1.0";
const SUPERSET_CORE_VERSION = "0.1.0";
// Packages whose only distribution is an sdist: name, version, the directory inside the sdist holding
// the importable packages, and those packages.
const REPACKAGE = [
  { name: "SQLAlchemy", version: "1.4.54", tree: "lib", packages: ["sqlalchemy"] },
  { name: "WTForms-JSON", version: "0.3.5", tree: "", packages: ["wtforms_json"] },
];
// Pinned versions with no importable wasm build, replaced by the newest upstream release that has one.
// pygeohash 3.2.2 (the pin) is a C extension with no Python fallback; 2.1.0 is the same library in pure
// Python, with the encode()/decode() signatures superset.utils.pandas_postprocessing.geography uses.
const SUBSTITUTE = ["pygeohash==2.1.0"];
// Standard library modules Pyodide ships separately rather than in python_stdlib.zip.  micropip never
// mentions them, but Superset imports sqlite3 (its metadata database) and its dependency tree imports
// ssl, hashlib and lzma at module scope.
const STDLIB = ["sqlite3", "ssl", "hashlib", "lzma"];
const CONSTRAINTS_URL =
  `https://raw.githubusercontent.com/apache/superset/${SUPERSET_VERSION}/requirements/base.txt`;

const supersetWheel = join(wheelDir, `apache_superset-${SUPERSET_VERSION}-py3-none-any.whl`);
if (!argv.includes("--force") && (await exists(supersetWheel)) && (await exists(lockPath))) {
  console.log("public/_superset/pyodide is already populated; pass --force to rebuild");
  process.exit(0);
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

// Superset's own wheel is 103 MB; cached under node_modules so a rebuild does not re-download it.
const cacheDir = join(root, "node_modules", ".cache", "superset-wheels");

async function pypiFile(name, version, match) {
  const meta = await (await fetch(`https://pypi.org/pypi/${name}/${version}/json`)).json();
  const file = meta.urls.find((entry) => match(entry.filename));
  if (!file) throw new Error(`${name} ${version}: no matching distribution`);
  const cached = join(cacheDir, file.filename);
  if (!(await exists(cached))) {
    await mkdir(cacheDir, { recursive: true });
    await writeFile(cached, await download(file.url));
  }
  return { name: file.filename, bytes: await readFile(cached) };
}

const inputs = [
  await pypiFile("apache-superset", SUPERSET_VERSION, (f) => f.endsWith("-py3-none-any.whl")),
  await pypiFile("apache-superset-core", SUPERSET_CORE_VERSION, (f) =>
    f.endsWith("-py3-none-any.whl"),
  ),
];
const repackage = [];
for (const { name, version, tree, packages } of REPACKAGE) {
  const sdist = await pypiFile(name, version, (f) => f.endsWith(".tar.gz"));
  inputs.push(sdist);
  repackage.push([sdist.name, tree, packages]);
}

const pyodide = await loadPyodide();
await pyodide.loadPackage("micropip");

pyodide.FS.mkdirTree("/in");
for (const file of inputs) pyodide.FS.writeFile(`/in/${file.name}`, file.bytes);
pyodide.FS.writeFile("/in/constraints.txt", await download(CONSTRAINTS_URL));

// The support package is what knows which requirements cannot exist on wasm and how the wheel is
// split; the browser runtime imports the rest of it.
pyodide.FS.mkdirTree("/pkg/superset_wasm");
const pythonDir = join(root, "python", "superset_wasm");
for (const name of await readdir(pythonDir)) {
  pyodide.FS.writeFile(`/pkg/superset_wasm/${name}`, await readFile(join(pythonDir, name)));
}

// public/ is emptied rather than removed and recreated: a running dev server stops serving a public
// directory that disappears underneath it.  copy-pyodide.mjs owns the wheel directory's contents.
await mkdir(assetDir, { recursive: true });
for (const stale of await readdir(assetDir)) {
  await rm(join(assetDir, stale), { recursive: true });
}
// Python writes the frontend bundle straight to disk: 3,700 files is not worth copying twice.
pyodide.FS.mkdirTree("/out");
pyodide.FS.mount(pyodide.FS.filesystems.NODEFS, { root: publicDir }, "/out");

const result = await pyodide.runPythonAsync(`
import json
import sys

sys.path.insert(0, "/pkg")

import micropip
from micropip._compat import REPODATA_PACKAGES

from superset_wasm import wheels

repackaged = [
    wheels.build_pure_wheel(f"/in/{name}", tree, packages, "/in")
    for name, tree, packages in ${JSON.stringify(repackage)}
]
# Installed first and without dependencies: micropip skips a requirement that is already satisfied,
# which is the only way to keep it from replacing SQLAlchemy 1.4 with the 2.x build Pyodide ships.
await micropip.install(["emfs:" + path for path in repackaged], deps=False)

vendored = {wheels._canonical(path.split("/")[-1].split("-")[0]) for path in repackaged}
wheels.relax(REPODATA_PACKAGES, vendored=vendored)
constraints = wheels.constraints("/in/constraints.txt", REPODATA_PACKAGES, vendored=vendored)

await micropip.install(
    ["emfs:/in/apache_superset-${SUPERSET_VERSION}-py3-none-any.whl",
     "emfs:/in/apache_superset_core-${SUPERSET_CORE_VERSION}-py3-none-any.whl"],
    constraints=constraints,
)

# Substitutes go in last, so nothing in the tree can pull the pinned version back in.
await micropip.install(${JSON.stringify(SUBSTITUTE)}, deps=False)

split = wheels.split_superset_wheel(
    "/in/apache_superset-${SUPERSET_VERSION}-py3-none-any.whl", "/out/pyodide", "/out/static/assets"
)
json.dumps({"lock": micropip.freeze(), "installed": sorted(micropip.list()), "split": split})
`);

const { lock: lockJson, installed, split } = JSON.parse(result);
const lock = JSON.parse(lockJson);

// freeze() describes the whole Pyodide distribution; keep only what Superset actually pulled in.
lock.packages = Object.fromEntries(
  [...installed, ...STDLIB].map((name) => [name, lock.packages[name]]),
);

// Dependency edges are dropped: some point at packages that were resolved away (greenlet,
// bottleneck) or at shared libraries Pyodide's loader pulls in by itself.  The browser asks for every
// package in this lock file, so nothing needs resolving there either.
for (const pkg of Object.values(lock.packages)) {
  pkg.depends = [];
}

let downloaded = 0;
for (const pkg of Object.values(lock.packages)) {
  const source = pkg.file_name;
  const file = source.slice(source.lastIndexOf("/") + 1);
  if (!(await exists(join(wheelDir, file)))) {
    // Superset's own wheel was already written by the split above.  The rest come from PyPI, from
    // Pyodide's on-disk distribution, or from the Emscripten filesystem for what we repackaged.
    const bytes = /^https?:\/\//.test(source)
      ? await download(source)
      : source.startsWith("emfs:") || source.startsWith("/")
        ? Buffer.from(pyodide.FS.readFile(source.replace(/^emfs:/, "")))
        : (await exists(join(pyodideDir, file)))
          ? await readFile(join(pyodideDir, file))
          : // The npm distribution carries the common packages only; the rest, including the
            // separately shipped standard library modules, come from the release it was cut from.
            await download(`${PYODIDE_CDN}${file}`);
    await writeFile(join(wheelDir, file), bytes);
  }
  // Rewritten to a bare name so the browser resolves it against packageBaseUrl (= /_superset/wheels/).
  pkg.file_name = file;
  // Re-hashed rather than trusted: Superset's wheel is not the one micropip installed (the frontend
  // was split out of it), and Pyodide's loader rejects a checksum mismatch.
  pkg.sha256 = createHash("sha256").update(await readFile(join(wheelDir, file))).digest("hex");
  downloaded += 1;
}

await writeFile(lockPath, JSON.stringify(lock));
const mb = (bytes) => `${(bytes / 1e6).toFixed(1)} MB`;
console.log(
  `wrote ${downloaded} wheels to public/_superset/pyodide ` +
    `(apache_superset slimmed to ${mb(split.wheel_bytes)}) and ` +
    `${split.assets} frontend files (${mb(split.asset_bytes)}) to public/_superset/static/assets`,
);
