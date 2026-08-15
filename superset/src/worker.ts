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

// The whole Superset deployment in one Web Worker: the interpreter, the Flask app, the metadata
// database and the analytics database the examples live in.  Requests arrive from the service worker
// by way of the page (which owns this worker) and are answered by calling Superset's WSGI callable
// directly -- there is no HTTP server anywhere in this project.

import type { BootResult, HttpResult, WorkerRequest, WorkerResponse } from "./protocol";

const PYODIDE_PATH = "/_superset/pyodide/";

//: Where the two SQLite files are kept between visits.  Origin Private File System rather than
//: IndexedDB because a 10 MB write is one streamed file rather than one giant value.
const STORE_DIR = "superset-wasm";

// Bundled at build time so the Python side needs no extra network round trips.
const PYTHON_FILES = import.meta.glob("../python/superset_wasm/*.py", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

let pyodide: any;
let webapp: any;
let booted = false;

function progress(text: string): void {
  self.postMessage({ type: "progress", text } satisfies WorkerResponse);
}

function basename(path: string): string {
  return path.slice(path.lastIndexOf("/") + 1);
}

async function packageNames(): Promise<string[]> {
  const response = await fetch(`${PYODIDE_PATH}pyodide-lock.json`);
  const lock = (await response.json()) as { packages: Record<string, unknown> };
  return Object.keys(lock.packages);
}

async function store(create: boolean): Promise<FileSystemDirectoryHandle | null> {
  try {
    const root = await navigator.storage.getDirectory();
    return await root.getDirectoryHandle(STORE_DIR, { create });
  } catch {
    // No OPFS (or nothing stored yet): initialise from scratch and do not persist.
    return null;
  }
}

async function readDatabases(): Promise<Record<string, Uint8Array>> {
  const directory = await store(false);
  if (!directory) return {};
  const files: Record<string, Uint8Array> = {};
  for await (const name of (directory as any).keys() as AsyncIterable<string>) {
    const handle = await directory.getFileHandle(name);
    files[name] = new Uint8Array(await (await handle.getFile()).arrayBuffer());
  }
  return files;
}

async function writeDatabases(files: Record<string, Uint8Array<ArrayBuffer>>): Promise<void> {
  const directory = await store(true);
  if (!directory) return;
  for (const [name, data] of Object.entries(files)) {
    const handle = await directory.getFileHandle(name, { create: true });
    const writable = await handle.createWritable();
    await writable.write(data);
    await writable.close();
  }
}

async function boot(): Promise<unknown> {
  if (booted) return { alreadyBooted: true };

  progress("loading Python (Pyodide)");
  // Built from location so the bundler treats it as a runtime URL: Pyodide is a copied asset in
  // public/, not part of the module graph.
  const runtime = `${self.location.origin}${PYODIDE_PATH}pyodide.mjs`;
  const { loadPyodide } = await import(/* @vite-ignore */ runtime);
  // scripts/build-wheels.mjs resolved Superset's dependency tree ahead of time, so installation is a
  // series of same-origin GETs with no resolver and no PyPI.
  const wheels = `${self.location.origin}${PYODIDE_PATH}`;
  pyodide = await loadPyodide({
    indexURL: PYODIDE_PATH,
    lockFileURL: `${wheels}pyodide-lock.json`,
    packageBaseUrl: wheels,
  });

  progress("installing the superset_wasm support package");
  pyodide.FS.mkdirTree("/pkg/superset_wasm");
  for (const [path, source] of Object.entries(PYTHON_FILES)) {
    pyodide.FS.writeFile(`/pkg/superset_wasm/${basename(path)}`, source);
  }
  pyodide.runPython('import sys; sys.path.insert(0, "/pkg")');

  progress("installing Superset and its dependencies (this is the slow part)");
  // The lock file *is* the resolution, so ask for everything in it rather than re-running micropip's
  // resolver: ~130 same-origin wheel fetches and no dependency graph to walk.
  await pyodide.loadPackage(await packageNames());

  webapp = pyodide.pyimport("superset_wasm.webapp");

  const stored = await readDatabases();
  const restored = Object.keys(stored).length > 0 && webapp.restore(pyodide.toPy(stored)) === true;
  progress(restored ? "restoring the database from the last visit" : "initialising a fresh database");

  // ``boot`` converts on the Python side, so this is a plain JS object already.
  const info = webapp.boot.callKwargs({ initialised: restored, progress }) as BootResult;
  booted = true;
  const result = { ...info, restored };

  if (!restored) {
    progress("saving the initialised database for the next visit");
    const snapshot = webapp.snapshot() as Record<string, Uint8Array<ArrayBuffer>>;
    await writeDatabases(snapshot);
  }
  return result;
}

async function serve(request: WorkerRequest & { type: "http" }): Promise<HttpResult> {
  // ``webapp.http`` hands back a real JS object (Python-side ``to_js``), including the body as a
  // Uint8Array, so nothing here has to be freed.
  const response = (await webapp.http(
    request.method,
    request.url,
    pyodide.toPy(request.headers),
    pyodide.toPy(new Uint8Array(request.body ?? new ArrayBuffer(0))),
  )) as { status: number; headers: [string, string][]; body: Uint8Array };
  const body = response.body.slice().buffer;
  return { status: response.status, headers: response.headers, body };
}

async function handle(request: WorkerRequest): Promise<unknown> {
  switch (request.type) {
    case "boot":
      return boot();
    case "http":
      return serve(request);
    default: {
      const exhaustive: never = request;
      throw new Error(`unknown request ${JSON.stringify(exhaustive)}`);
    }
  }
}

// One SQLite connection, one interpreter, one thread: requests are answered strictly one at a time,
// which is also what keeps a chart query from interleaving with the session that issued it.
let tail: Promise<void> = Promise.resolve();

self.onmessage = (event: MessageEvent<WorkerRequest>) => {
  const request = event.data;
  tail = tail.then(async () => {
    try {
      const result = await handle(request);
      const message: WorkerResponse = { type: "result", id: request.id, result };
      self.postMessage(message, transferables(result));
    } catch (error) {
      self.postMessage({
        type: "error",
        id: request.id,
        error: String((error as Error)?.message ?? error),
      } satisfies WorkerResponse);
    }
  });
};

function transferables(result: unknown): Transferable[] {
  const body = (result as { body?: unknown })?.body;
  return body instanceof ArrayBuffer ? [body] : [];
}
