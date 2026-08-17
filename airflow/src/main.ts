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

// The page owns the runtime worker and lends it to the service worker: a service worker cannot own a
// long-lived worker of its own (it is killed and restarted at the browser's discretion), and the
// Airflow runtime takes a minute to boot, so it has to live where the tab lives.

import { BootPanel } from "../../design/boot";

import "./airflow.css";
import type { HttpResult, Unidentified, WorkerRequest, WorkerResponse } from "./protocol";

// Where the runtime is mounted, not where this page lives: the service worker answers everything
// under it, so it must not collide with a path the static host serves (this page is /airflow/).
const BASE_PATH = "/_airflow/";
const TICK_INTERVAL_MS = 2_000;
const CLAIM_TIMEOUT_MS = 3_000;

const worker = new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
const pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
let nextId = 1;

const status = document.querySelector<HTMLSpanElement>("#status")!;
const frame = document.querySelector<HTMLIFrameElement>("#ui")!;
const heartbeat = document.querySelector<HTMLSpanElement>("#heartbeat")!;

const boot = new BootPanel({
  mount: document.querySelector<HTMLElement>("#boot")!,
  title: "Starting Apache Airflow",
  detail:
    "Nothing here runs on a server. CPython arrives as Pyodide, then the Airflow wheels, then " +
    "Postgres as PGlite — and the API server, scheduler, Dag processor and worker all start in " +
    "this tab.",
  logs: [{ label: "Runtime log" }, { label: "Scheduler", collapsed: true }],
});
const say = (text: string): void => boot.say(text);

worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
  const message = event.data;
  if (message.type === "progress") {
    status.textContent = message.text;
    boot.now(message.text);
    say(message.text);
    return;
  }
  const settle = pending.get(message.id);
  if (!settle) return;
  pending.delete(message.id);
  if (message.type === "error") settle.reject(new Error(message.error));
  else settle.resolve(message.result);
};

function call<T>(request: Unidentified<WorkerRequest>, transfer: Transferable[] = []): Promise<T> {
  const id = nextId++;
  return new Promise<T>((resolve, reject) => {
    pending.set(id, { resolve: resolve as (value: unknown) => void, reject });
    worker.postMessage({ ...request, id } as WorkerRequest, transfer);
  });
}

/** Hand the service worker a port that reaches the runtime worker, relaying messages both ways. */
function connectServiceWorker(target: ServiceWorker): void {
  const channel = new MessageChannel();
  channel.port1.onmessage = async (event) => {
    const { id, method, url, headers, body } = event.data;
    try {
      const result = await call<HttpResult>(
        { type: "http", method, url, headers, body },
        body ? [body] : [],
      );
      channel.port1.postMessage({ id, ...result }, [result.body]);
    } catch (error) {
      channel.port1.postMessage({ id, error: String((error as Error).message) });
    }
  };
  target.postMessage({ type: "runtime-port", port: channel.port2 }, [channel.port2]);
}

async function registerServiceWorker(): Promise<ServiceWorkerRegistration> {
  let registration = await navigator.serviceWorker.register("/sw.js", { scope: "/" });
  // A registration can be left behind with no worker in any state (an interrupted unregister, a
  // killed update).  Registering again does not revive it and `ready` would never settle, so throw
  // it away and start over rather than hanging on the first boot step forever.
  if (!(registration.active || registration.installing || registration.waiting)) {
    await registration.unregister();
    registration = await navigator.serviceWorker.register("/sw.js", { scope: "/" });
  }
  await navigator.serviceWorker.ready;
  // The first visit to this origin loads the page uncontrolled: sw.js claims its clients when it
  // activates, but until that lands a request for /_airflow/ goes to the network and the iframe
  // shows the site's own index.html instead of Airflow.  Wait to be claimed; if the claim never
  // arrives, reload -- a page loaded while a worker is already active is controlled from the start.
  if (!navigator.serviceWorker.controller) {
    const claimed = await new Promise<boolean>((resolve) => {
      navigator.serviceWorker.addEventListener("controllerchange", () => resolve(true), {
        once: true,
      });
      window.setTimeout(() => resolve(false), CLAIM_TIMEOUT_MS);
    });
    if (!claimed) {
      location.reload();
      await new Promise(() => undefined);
    }
  }
  navigator.serviceWorker.addEventListener("message", (event) => {
    if (event.data?.type === "need-port" && navigator.serviceWorker.controller) {
      connectServiceWorker(navigator.serviceWorker.controller);
    }
  });
  const active = registration.active ?? navigator.serviceWorker.controller;
  if (active) connectServiceWorker(active);
  return registration;
}

function startScheduler(): void {
  let running = false;
  window.setInterval(async () => {
    if (running) return;
    running = true;
    try {
      const summary = await call<{ tasks: { task_id: string; state: string }[] }>({ type: "tick" });
      heartbeat.textContent = new Date().toLocaleTimeString();
      for (const task of summary.tasks) {
        boot.log("Scheduler").write(`ran ${task.task_id} -> ${task.state}`);
      }
    } catch (error) {
      boot.log("Scheduler").write(`scheduler tick failed: ${(error as Error).message}`);
    } finally {
      running = false;
    }
  }, TICK_INTERVAL_MS);
}

async function main(): Promise<void> {
  document.querySelector<HTMLButtonElement>("#reset")!.addEventListener("click", async () => {
    // The service worker holds no state worth clearing, and unregistering it here used to race the
    // reload badly enough to leave the origin with a dead registration -- only the database goes.
    const root = await navigator.storage.getDirectory();
    await root.removeEntry("airflow-wasm", { recursive: true }).catch(() => undefined);
    location.reload();
  });

  const interceptor = boot.step("Registering the request interceptor (service worker)");
  await registerServiceWorker();
  interceptor.done();

  const runtime = boot.step("Booting Python, Postgres and Airflow in this tab");
  const info = await call<{ airflow_version: string; dags: { file: string }[] }>({
    type: "boot",
    baseUrl: `${location.origin}${BASE_PATH}`,
    dataDir: new URLSearchParams(location.search).get("dataDir") ?? undefined,
  });
  runtime.done(`${info.dags.length} Dag file(s)`);
  status.textContent = `Airflow ${info.airflow_version} — running in this tab`;

  startScheduler();
  frame.src = BASE_PATH;
  boot.ready(`Airflow ${info.airflow_version} is running in this tab`);
}

void main().catch((error: Error) => {
  status.textContent = "failed to start";
  boot.fail(`boot failed: ${error.message}`);
});
