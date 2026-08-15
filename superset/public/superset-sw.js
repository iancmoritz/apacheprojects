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

// Superset's frontend is a stock single-page app that fetches /_superset/api/v1/... over HTTP; it has
// no idea there is no server.  This worker answers those requests from the Pyodide runtime instead of
// the network, forwarding them over a MessagePort to the page, which owns the runtime.
//
// The scope is /_superset/ and not / on purpose: /sw.js already claims the root for Airflow, and a
// second registration on the same scope would replace it.  Superset is configured with
// APPLICATION_ROOT=/_superset, so every URL its frontend builds -- API calls, page navigations,
// asset tags -- carries that prefix and lands inside this scope.

const PREFIX = "/_superset";
// The two things under the prefix that are real files on the static host: the frontend bundle that
// ships in the wheel, and the interpreter with its wheels.  Everything else is the app.
const PASS_THROUGH = [`${PREFIX}/static/assets/`, `${PREFIX}/pyodide/`];
const TIMEOUT_MS = 300_000;

let port = null;
let waiters = [];
const pending = new Map();
let nextId = 1;

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("message", (event) => {
  if (event.data?.type !== "runtime-port") return;
  port = event.data.port;
  port.onmessage = (message) => {
    const { id, ...rest } = message.data;
    const settle = pending.get(id);
    if (!settle) return;
    pending.delete(id);
    settle(rest);
  };
  for (const resolve of waiters.splice(0)) resolve(port);
});

async function runtimePort() {
  if (port) return port;
  // The service worker can be restarted at any time, losing the port; ask the page for a new one.
  const clients = await self.clients.matchAll({ includeUncontrolled: true, type: "window" });
  for (const client of clients) client.postMessage({ type: "need-port" });
  return new Promise((resolve, reject) => {
    waiters.push(resolve);
    setTimeout(() => reject(new Error("the Superset runtime is not connected")), TIMEOUT_MS);
  });
}

async function proxy(request) {
  const target = await runtimePort();
  const body =
    request.method === "GET" || request.method === "HEAD" ? null : await request.arrayBuffer();
  const id = nextId++;
  const answer = new Promise((resolve) => pending.set(id, resolve));

  target.postMessage(
    { id, method: request.method, url: request.url, headers: [...request.headers], body },
    body ? [body] : [],
  );

  const result = await answer;
  if (result.error) {
    return new Response(`Superset runtime error: ${result.error}`, {
      status: 500,
      headers: { "content-type": "text/plain" },
    });
  }
  return new Response(result.status === 204 || result.status === 304 ? null : result.body, {
    status: result.status,
    headers: result.headers,
  });
}

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname !== PREFIX && !url.pathname.startsWith(`${PREFIX}/`)) return;

  // Upstream prefixes APP_ICON with APPLICATION_ROOT server-side and the frontend prefixes it again
  // with STATIC_ASSETS_PREFIX; with one mount for the whole app the two are the same value, so the
  // logo arrives as /_superset/_superset/static/...  Collapse the repeat rather than dropping either
  // setting -- the bundle needs one and every API path needs the other.
  const path = url.pathname.startsWith(`${PREFIX}${PREFIX}/`)
    ? url.pathname.slice(PREFIX.length)
    : url.pathname;
  if (PASS_THROUGH.some((asset) => path.startsWith(asset))) {
    if (path !== url.pathname) event.respondWith(fetch(new URL(path + url.search, url.origin)));
    return;
  }
  event.respondWith(
    proxy(event.request).catch(
      (error) =>
        new Response(`Superset runtime unavailable: ${error.message}`, {
          status: 503,
          headers: { "content-type": "text/plain" },
        }),
    ),
  );
});
