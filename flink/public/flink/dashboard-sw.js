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

// Serves Apache Flink's own web dashboard out of the JobManager running in the page.
//
// The dashboard is an ordinary Angular application that Flink ships inside flink-dist and its
// DispatcherRestEndpoint serves, and it talks to that endpoint over HTTP.  Neither half can reach a
// network here, but both are in the tab: this worker answers every request under /flink/dashboard/ by
// asking the page to relay it into the JVM, where `FlinkConsole.restRequest` connects to the endpoint
// over the in-process loopback and returns what netty wrote back.

/** The prefix the dashboard is mounted under; requests outside it are none of this worker's business. */
const SCOPE = new URL("./dashboard/", self.location).pathname;

/** The page that owns the JVM. Only it can answer, so the iframe below is never asked. */
let host = null;

self.addEventListener("install", () => self.skipWaiting());

self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("message", (event) => {
  if (event.data && event.data.type === "host" && event.source) {
    host = event.source;
    if (event.ports[0]) event.ports[0].postMessage({ ok: true });
  }
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin || !url.pathname.startsWith(SCOPE)) return;
  event.respondWith(serve(event.request, url));
});

async function serve(request, url) {
  // The dashboard's own paths are relative to <base href="./">, so everything it asks for arrives
  // under the scope; what the REST endpoint sees is the same path without it.
  let target = url.pathname.slice(SCOPE.length - 1);
  if (target === "/" || target === "") target = "/index.html";

  try {
    const answer = await relay({
      method: request.method,
      target: target + url.search,
      body: request.method === "GET" || request.method === "HEAD" ? "" : await request.text(),
    });
    if (!answer.ok) {
      return new Response(answer.error, { status: 502, headers: { "Content-Type": "text/plain" } });
    }
    return new Response(bytes(answer.body), {
      status: answer.status,
      headers: { "Content-Type": answer.contentType, "Cache-Control": "no-store" },
    });
  } catch (error) {
    return new Response(String(error && error.stack ? error.stack : error), {
      status: 502,
      headers: { "Content-Type": "text/plain" },
    });
  }
}

/** Asks the page to make one call into the JVM, over a channel opened for this request. */
async function relay(call) {
  if (!host) throw new Error("the Flink page is not open, so there is no JobManager to ask");
  const page = host;

  return new Promise((resolve, reject) => {
    const channel = new MessageChannel();
    const timer = setTimeout(() => reject(new Error(`${call.target}: the page did not answer`)), 120000);
    channel.port1.onmessage = (event) => {
      clearTimeout(timer);
      resolve(event.data);
    };
    page.postMessage({ type: "rest", call }, [channel.port2]);
  });
}

/** The response body arrives base64, because it is as often a font or an image as it is JSON. */
function bytes(base64) {
  const binary = atob(base64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}
