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
// long-lived worker of its own (it is killed and restarted at the browser's discretion), and booting
// Superset takes long enough that it has to live where the tab lives.

import type {
  BootResult,
  HttpResult,
  Unidentified,
  WorkerRequest,
  WorkerResponse,
} from "./protocol";

// Where the app is mounted, not where this page lives: the service worker answers everything under
// it, and Superset's APPLICATION_ROOT is set to match (this page is /superset/).
const BASE_PATH = "/_superset/";
const SERVICE_WORKER = "/superset-sw.js";

const worker = new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
const pending = new Map<
  number,
  { resolve: (value: unknown) => void; reject: (error: Error) => void }
>();
let nextId = 1;

const log = document.querySelector<HTMLPreElement>("#log")!;
const status = document.querySelector<HTMLSpanElement>("#status")!;
const frame = document.querySelector<HTMLIFrameElement>("#ui")!;
const boot = document.querySelector<HTMLElement>("#boot")!;

function say(text: string): void {
  log.textContent += `${text}\n`;
  log.scrollTop = log.scrollHeight;
}

worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
  const message = event.data;
  if (message.type === "progress") {
    status.textContent = message.text;
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
  const options: RegistrationOptions = { scope: BASE_PATH };
  let registration = await navigator.serviceWorker.register(SERVICE_WORKER, options);
  // A registration can be left behind with no worker in any state (an interrupted unregister, a
  // killed update).  Registering again does not revive it and `ready` would never settle, so throw it
  // away and start over rather than hanging on the first boot step forever.
  if (!(registration.active || registration.installing || registration.waiting)) {
    await registration.unregister();
    registration = await navigator.serviceWorker.register(SERVICE_WORKER, options);
  }
  navigator.serviceWorker.addEventListener("message", (event) => {
    if (event.data?.type === "need-port" && registration.active) {
      connectServiceWorker(registration.active);
    }
  });
  // This page is outside the worker's scope (only /_superset/ is in it, so that Airflow keeps the
  // root registration), so `navigator.serviceWorker.controller` stays null: wait for the
  // registration's own worker instead of for control of this document.
  const active = await new Promise<ServiceWorker>((resolve) => {
    const ready = () => {
      const candidate = registration.active;
      if (candidate?.state === "activated" || candidate?.state === "activating") resolve(candidate);
    };
    const installing = registration.installing ?? registration.waiting;
    installing?.addEventListener("statechange", ready);
    ready();
  });
  connectServiceWorker(active);
  return registration;
}

async function main(): Promise<void> {
  document.querySelector<HTMLButtonElement>("#reset")!.addEventListener("click", async () => {
    // Only the databases go: unregistering the service worker here races the reload badly enough to
    // leave the origin with a dead registration.
    const root = await navigator.storage.getDirectory();
    await root.removeEntry("superset-wasm", { recursive: true }).catch(() => undefined);
    location.reload();
  });

  say("registering the request interceptor (service worker)");
  await registerServiceWorker();

  const started = performance.now();
  const info = await call<BootResult>({ type: "boot" });
  const seconds = ((performance.now() - started) / 1000).toFixed(1);
  say(
    `Superset ${info.version} is up in ${seconds}s ` +
      `(${info.counts.datasets} datasets, ${info.counts.charts} charts, ` +
      `${info.counts.dashboards} dashboards${info.restored ? ", restored from the last visit" : ""})`,
  );
  if (!info.logged_in) say("automatic login failed -- sign in as admin / admin");
  status.textContent = `Superset ${info.version} — running in this tab (booted in ${seconds}s)`;

  boot.classList.add("done");
  // `dashboard` is the app's own route (/superset/dashboard/2/), built outside a request and so
  // without the mount prefix a real one would have picked up from SCRIPT_NAME.
  frame.src = info.dashboard ? `${BASE_PATH.slice(0, -1)}${info.dashboard}` : BASE_PATH;

  const nav = document.querySelector<HTMLElement>("#nav")!;
  nav.hidden = false;
  for (const button of nav.querySelectorAll<HTMLButtonElement>("button[data-path]")) {
    button.addEventListener("click", () => {
      frame.src = `${BASE_PATH.slice(0, -1)}${button.dataset.path}`;
    });
  }
}

void main().catch((error: Error) => {
  status.textContent = "failed to start";
  say(`boot failed: ${error.message}`);
});
