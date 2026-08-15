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

// Builds the deployed site: the project list this directory owns, plus every project's own build
// merged in at the routes that project emits (Airflow's build emits airflow/index.html itself, so
// its page lands on /airflow/).
//
// A project also owns root-level paths -- its service worker has to be served from / to intercept /
// -- so two projects claiming the same file is a real conflict, not something to silently resolve;
// this fails instead.

import { cp, mkdir, readdir, rm, stat } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const OUT = join(ROOT, "dist");

/** Top-level project directories, each of which builds a `dist/` of its own. */
const PROJECTS = ["airflow", "iceberg"];

/** Files the project list itself is made of. */
const SITE_FILES = ["index.html", "projects.js"];

/** Every file under `dir`, as paths relative to it. */
async function walk(dir, prefix = "") {
  const files = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(prefix, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(join(dir, entry.name), path)));
    else files.push(path);
  }
  return files;
}

async function main() {
  await rm(OUT, { recursive: true, force: true });
  await mkdir(OUT, { recursive: true });

  const owner = new Map();

  for (const project of PROJECTS) {
    const built = join(ROOT, project, "dist");
    if (!(await stat(built).catch(() => null))) {
      throw new Error(`${relative(ROOT, built)} is missing -- build ${project} first`);
    }
    for (const file of await walk(built)) {
      const taken = owner.get(file);
      if (taken) throw new Error(`${project} and ${taken} both emit ${file}`);
      owner.set(file, project);
    }
    await cp(built, OUT, { recursive: true });
  }

  for (const file of SITE_FILES) {
    const taken = owner.get(file);
    if (taken) throw new Error(`${taken} emits ${file}, which the project list needs`);
    await cp(join(ROOT, file), join(OUT, file));
  }

  console.log(`assembled ${owner.size + SITE_FILES.length} files into dist/ (${PROJECTS.join(", ")})`);
}

await main();
