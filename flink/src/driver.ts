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

// Boots CheerpJ's JVM and gets a handle on the Flink MiniCluster inside it.
//
// CheerpJ's library mode is what makes the page interactive: instead of running a main class to
// completion it hands JavaScript an async proxy for the JVM's classes, so the cluster keeps running
// between calls and every word the visitor types is an ordinary call into the same live job.

import type { Cluster, Failure, JobOptions, Manifest, Poll, RestResponse, Submitted } from "./protocol";

/** Where the build script put the jars, relative to the site root. */
const ASSETS = "/_flink";

/**
 * Flink's blob server and netty shuffle service open real sockets, which a tab cannot.  This
 * selector provider answers them with in-process loopback ones; without it the JVM dies in
 * `EPollSelectorProvider` with `UnsatisfiedLinkError: Java_sun_nio_ch_EPoll_eventSize`.
 */
const SELECTOR = "org.apacheprojects.flinkwasm.net.VirtualSelectorProvider";

/** Where Flink may write inside CheerpJ's filesystem: blobs, checkpoints and temp files. */
const WORK_DIR = "/files/flink";

declare function cheerpjInit(options: {
  version?: number;
  status?: "full" | "compact" | "none";
  javaProperties?: string[];
}): Promise<void>;

declare function cheerpjRunLibrary(classPath: string): Promise<CheerpJLibrary>;

interface FlinkConsole {
  startCluster(slots: number, directory: string): Promise<string>;
  submitJob(
    windowSeconds: number,
    lateness: number,
    outOfOrderness: number,
    checkpointInterval: number,
    parallelism: number,
  ): Promise<string>;
  push(text: string, timestamp: number): Promise<string>;
  setGenerator(rate: number, skewPercent: number): Promise<string>;
  checkpointNow(): Promise<string>;
  poll(): Promise<string>;
  cancelJob(): Promise<string>;
  restRequest(method: string, target: string, body: string): Promise<string>;
}

interface CheerpJLibrary {
  org: { apacheprojects: { flinkwasm: { console: { FlinkConsole: Promise<FlinkConsole> } } } };
}

/**
 * Runs calls into the JVM one at a time.
 *
 * CheerpJ's JVM shares the page's thread, and entering it while an earlier call is still inside logs
 * `Java code still running, check for a missing 'await'` and rejects. The page polls on a timer while
 * the visitor types words and drags sliders, so calls do overlap; queuing them behind one promise
 * chain is what keeps a click from landing in the middle of a poll.
 */
function serialize(): <T>(call: () => Promise<T>) => Promise<T> {
  let tail: Promise<unknown> = Promise.resolve();
  return <T>(call: () => Promise<T>): Promise<T> => {
    const next = tail.then(call, call);
    tail = next.catch(() => undefined);
    return next;
  };
}

interface Step {
  done(detail?: string): void;
  failed(detail: string): void;
}

export interface BootOptions {
  onLog(line: string): void;
  onStep(text: string): Step;
  onManifest(manifest: Manifest): void;
}

export interface Driver {
  manifest: Manifest;
  cluster: Cluster;
  bootMs: number;
  submit(options: JobOptions): Promise<Submitted | Failure>;
  push(text: string, timestamp: number): Promise<void>;
  generator(rate: number, skewPercent: number): Promise<void>;
  checkpoint(): Promise<{ ok: true; path: string } | Failure>;
  poll(): Promise<Poll | Failure>;
  cancel(): Promise<void>;
  rest(method: string, target: string, body: string): Promise<RestResponse | Failure>;
}

/**
 * Sends the JVM's stdout to the page.
 *
 * In library mode CheerpJ writes `System.out` to the browser console, which is where log4j puts
 * Flink's own log lines; the page shows them so the boot is something you can watch rather than a
 * spinner, and so a failure inside the cluster is visible without devtools.
 */
function captureOutput(onLog: (line: string) => void) {
  for (const level of ["log", "info", "warn", "error"] as const) {
    const original = console[level].bind(console);
    console[level] = (...args: unknown[]) => {
      original(...args);
      // Only the strings: CheerpJ also logs JVM objects, and converting one to text can throw --
      // which, inside a console call the JVM itself made, would take the boot down with it.
      const text = args.filter((arg) => typeof arg === "string").join(" ");
      // CheerpJ's own progress chatter is not Flink's; the boot steps already cover it.
      if (text.trim() && !text.startsWith("CheerpJ")) onLog(text);
    };
  }
}

export async function boot({ onLog, onStep, onManifest }: BootOptions): Promise<Driver> {
  const started = performance.now();
  const queued = serialize();
  const manifest: Manifest = await fetch(`${ASSETS}/manifest.json`).then((response) => {
    if (!response.ok) {
      throw new Error(
        `${ASSETS}/manifest.json: ${response.status}. Run \`npm run assets\` in flink/ to build the runtime.`,
      );
    }
    return response.json();
  });
  onManifest(manifest);
  captureOutput(onLog);

  const jvm = onStep("Starting the CheerpJ JVM (OpenJDK 8, WebAssembly)");
  await cheerpjInit({
    version: 8,
    status: "none",
    javaProperties: [`java.nio.channels.spi.SelectorProvider=${SELECTOR}`],
  });
  jvm.done();

  // /app is CheerpJ's read-only view of this origin, so the classpath is just the site's own URLs;
  // the JVM fetches the ranges of each jar it needs rather than the whole file.
  const classPath = manifest.classpath.map((jar) => `/app${ASSETS}/jars/${jar}`).join(":");
  const loading = onStep(`Loading Flink ${manifest.flinkVersion} from ${manifest.classpath.length} jars`);
  const library = await cheerpjRunLibrary(classPath);
  const flink = await library.org.apacheprojects.flinkwasm.console.FlinkConsole;
  loading.done();

  const starting = onStep("Starting the MiniCluster: JobManager and TaskManager in this tab");
  const cluster: Cluster | Failure = JSON.parse(await flink.startCluster(2, WORK_DIR));
  if (!cluster.ok) {
    starting.failed("failed");
    const error = new Error(cluster.error);
    error.stack = cluster.trace ?? error.stack;
    throw error;
  }
  starting.done(`${cluster.taskManagers} TaskManager, ${cluster.slots} slots`);

  return {
    manifest,
    cluster,
    bootMs: performance.now() - started,
    submit: async (options: JobOptions) =>
      queued(async () =>
        JSON.parse(
          await flink.submitJob(
            options.windowSeconds,
            options.lateness,
            options.outOfOrderness,
            options.checkpointInterval,
            options.parallelism,
          ),
        ),
      ),
    push: async (text: string, timestamp: number) => {
      await queued(() => flink.push(text, timestamp));
    },
    generator: async (rate: number, skewPercent: number) => {
      await queued(() => flink.setGenerator(rate, skewPercent));
    },
    rest: async (method: string, target: string, body: string) =>
      queued(async () => JSON.parse(await flink.restRequest(method, target, body))),
    checkpoint: async () => queued(async () => JSON.parse(await flink.checkpointNow())),
    poll: async () => queued(async () => JSON.parse(await flink.poll())),
    cancel: async () => {
      await queued(() => flink.cancelJob());
    },
  };
}
