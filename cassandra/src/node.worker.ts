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

// One Cassandra node: a CheerpJ JVM running org.apache.cassandra in this worker, with three native
// methods bridging it to the page.
//
// The awkward part is the direction of control.  Java calls out to JavaScript synchronously
// (`send`, `reply`); JavaScript cannot call *into* Java while a Java call is on the stack, which is
// most of the time here since Cassandra never returns to the caller.  So inbound traffic is pulled,
// not pushed: a Java thread blocks in `poll`, whose native implementation is an async function, and
// CheerpJ suspends only that thread until the promise resolves -- Cassandra's other thread pools
// keep running.

declare function importScripts(...urls: string[]): void;
declare function cheerpjInit(options: Record<string, unknown>): Promise<void>;
declare function cheerpjRunMain(className: string, classpath: string, ...args: string[]): Promise<number>;

// This is a classic worker (importScripts is how CheerpJ's runtime is meant to be loaded into one),
// so it cannot import anything: the loader URL is duplicated from src/runtime.ts on purpose.
importScripts("https://cjrtnc.leaningtech.com/4.3/loader.js");

/** Frames waiting for a Java thread to come and get them, and the thread waiting for one. */
const inbox: Int8Array[] = [];
let waiter: ((frame: Int8Array | null) => void) | null = null;
let paused = false;

function enqueue(frame: Int8Array) {
  if (waiter) {
    const resume = waiter;
    waiter = null;
    resume(frame);
  } else {
    inbox.push(frame);
  }
}

/** A CQL request from the page, framed for java/Node.java's single inbound channel. */
function commandFrame(id: string, cl: string, query: string): Int8Array {
  const body = new TextEncoder().encode(`${id}\n${cl}\n${query}`);
  const frame = new Int8Array(body.length + 1);
  frame[0] = 1;
  frame.set(new Int8Array(body.buffer, body.byteOffset, body.length), 1);
  return frame;
}

let index = 1;

self.onmessage = (event: MessageEvent) => {
  const message = event.data;
  if (message.type === "init") {
    index = message.index;
    void start(message.index, message.nodes, message.verbs === true);
  } else if (message.type === "frame") {
    if (!paused) enqueue(new Int8Array(message.frame));
  } else if (message.type === "cql") {
    enqueue(commandFrame(message.id, message.cl, message.query));
  } else if (message.type === "pause") {
    paused = message.paused;
  }
};

const started = Date.now();
const boot = (stage: string) => postMessage({ type: "boot", index, stage, ms: Date.now() - started });

async function start(node: number, nodes: number, verbs: boolean) {
  await cheerpjInit({
    version: 8,
    status: "none",
    natives: {
      /** Cassandra's outbound sink: a serialized Message for `to`, routed by the page. */
      async Java_Node_send(_lib: unknown, to: string, frame: { length: number; [i: number]: number }) {
        if (paused) return;
        const copy = new Int8Array(frame.length);
        for (let i = 0; i < frame.length; i++) copy[i] = frame[i];
        postMessage({ type: "frame", to, from: node, frame: copy.buffer }, [copy.buffer]);
      },
      /** Blocks one Java thread until there is something to hand to Cassandra. */
      async Java_Node_poll(_lib: unknown, timeoutMs: number) {
        const queued = inbox.shift();
        if (queued) return queued;
        return await new Promise<Int8Array | null>((resolve) => {
          waiter = resolve;
          setTimeout(() => {
            if (waiter === resolve) {
              waiter = null;
              resolve(null);
            }
          }, timeoutMs);
        });
      },
      /** Node state and CQL results, as JSON built in Java. */
      async Java_Node_reply(_lib: unknown, json: string) {
        postMessage({ type: "node", index: node, json });
      },
      /** Boot progress, so the page can show where a two-minute startup has got to. */
      async Java_Node_progress(_lib: unknown, stage: string) {
        boot(stage);
      },
    },
  });
  boot("CheerpJ runtime ready");

  const classpath = await (await fetch("/_cassandra/classpath.txt")).text();
  try {
    const args = [String(node), String(nodes)];
    if (verbs) args.push("verbs");
    const code = await cheerpjRunMain("Node", classpath, ...args);
    postMessage({ type: "exit", index: node, code });
  } catch (error) {
    postMessage({ type: "error", index: node, error: String(error) });
  }
}
