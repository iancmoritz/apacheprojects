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

// The whole stack in one Web Worker: CPython (Pyodide), PyIceberg writing a real Iceberg table into
// the interpreter's filesystem, and DuckDB reading it back.  The page only ever sends commands and
// gets JSON, so this file has no idea what the UI looks like.

import type { WorkerRequest, WorkerResponse } from "./protocol";

// Namespaced so nothing collides with the other projects assembled into the same site.
const PYODIDE_PATH = "/_iceberg/pyodide/";
const WHEELS_PATH = "/_iceberg/wheels/";

// Bundled at build time so the Python side needs no extra network round trips.
const PYTHON_FILES = import.meta.glob("../python/iceberg_wasm/*.py", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

let pyodide: any;
let call: ((command: string, payload: string) => string) | undefined;

function progress(text: string): void {
  self.postMessage({ type: "progress", text } satisfies WorkerResponse);
}

function basename(path: string): string {
  return path.slice(path.lastIndexOf("/") + 1);
}

async function packageNames(): Promise<string[]> {
  const response = await fetch(`${WHEELS_PATH}pyodide-lock.json`);
  const lock = (await response.json()) as { packages: Record<string, unknown> };
  return Object.keys(lock.packages);
}

async function boot(): Promise<unknown> {
  if (call) return { alreadyBooted: true };

  const started = performance.now();
  progress("loading Python (Pyodide)");
  // Built from location so the bundler treats it as a runtime URL: Pyodide is a copied asset in
  // public/, not part of the module graph.
  const runtime = `${self.location.origin}${PYODIDE_PATH}pyodide.mjs`;
  const { loadPyodide } = await import(/* @vite-ignore */ runtime);
  const wheels = `${self.location.origin}${WHEELS_PATH}`;
  pyodide = await loadPyodide({
    indexURL: PYODIDE_PATH,
    lockFileURL: `${wheels}pyodide-lock.json`,
    packageBaseUrl: wheels,
  });

  progress("installing the iceberg_wasm support package");
  pyodide.FS.mkdirTree("/pkg/iceberg_wasm");
  for (const [path, source] of Object.entries(PYTHON_FILES)) {
    pyodide.FS.writeFile(`/pkg/iceberg_wasm/${basename(path)}`, source);
  }

  progress("installing PyIceberg, PyArrow and DuckDB (this is the slow part)");
  // scripts/build-runtime.mjs resolved the dependency closure ahead of time, so the lock file *is*
  // the resolution: a series of same-origin GETs with no resolver and no PyPI.
  await pyodide.loadPackage(await packageNames());

  progress("opening the SQLite catalog");
  pyodide.runPython('import sys; sys.path.insert(0, "/pkg")');
  call = pyodide.runPython("from iceberg_wasm.api import call; call") as typeof call;
  const info = JSON.parse(call!("state", "{}")) as Record<string, unknown>;
  return { ...info, boot_ms: Math.round(performance.now() - started) };
}

function command(name: string, payload: Record<string, unknown> = {}): unknown {
  if (!call) throw new Error("the runtime is still booting");
  const result = JSON.parse(call(name, JSON.stringify(payload))) as {
    ok: boolean;
    error?: string;
    traceback?: string;
  };
  if (!result.ok) throw new Error(result.error ?? "unknown Python error");
  return result;
}

async function handle(request: WorkerRequest): Promise<unknown> {
  switch (request.type) {
    case "boot":
      return boot();
    case "call":
      return command(request.command, request.payload);
    default: {
      const exhaustive: never = request;
      throw new Error(`unknown request ${JSON.stringify(exhaustive)}`);
    }
  }
}

// One interpreter, one catalog: commands are answered strictly one at a time.
let tail: Promise<void> = Promise.resolve();

self.onmessage = (event: MessageEvent<WorkerRequest>) => {
  const request = event.data;
  tail = tail.then(async () => {
    try {
      const result = await handle(request);
      self.postMessage({ type: "result", id: request.id, result } satisfies WorkerResponse);
    } catch (error) {
      self.postMessage({
        type: "error",
        id: request.id,
        error: String((error as Error)?.message ?? error),
      } satisfies WorkerResponse);
    }
  });
};
