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

// Boots CheerpJ's JVM and gets a handle on the Spark driver inside it.
//
// CheerpJ's library mode is what makes the page interactive: instead of running a main class to
// completion it hands JavaScript an async proxy for the JVM's classes, so every statement the visitor
// types is an ordinary call into the same long-lived driver.

import type { Manifest, QueryResult, Session } from "./protocol";

/** Where the build script put the jars, relative to the site root. */
const ASSETS = "/_spark";

/**
 * Spark's netty RPC and block manager open real sockets, which a tab cannot.  This selector provider
 * answers them with in-process loopback ones; without it the JVM dies in `EPollSelectorProvider` with
 * `UnsatisfiedLinkError: Java_sun_nio_ch_EPoll_eventSize`.
 */
const SELECTOR = "org.apacheprojects.sparkwasm.net.VirtualSelectorProvider";

declare function cheerpjInit(options: {
  version?: number;
  status?: "full" | "compact" | "none";
  javaProperties?: string[];
}): Promise<void>;

declare function cheerpjRunLibrary(classPath: string): Promise<CheerpJLibrary>;

interface CheerpJLibrary {
  org: {
    apacheprojects: {
      sparkwasm: {
        console: {
          SparkConsole: Promise<{
            start(master: string, csvPath: string, view: string): Promise<string>;
            query(sql: string): Promise<string>;
          }>;
        };
      };
    };
  };
}

export interface Driver {
  session: Session;
  bootMs: number;
  query(sql: string): Promise<QueryResult>;
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

/**
 * Sends the JVM's stdout to the page.
 *
 * In library mode CheerpJ writes `System.out` to the browser console, which is where Spark's own log
 * lines end up; the page shows them so the boot is something you can watch rather than a spinner.
 */
function captureOutput(onLog: (line: string) => void) {
  for (const level of ["log", "info", "warn", "error"] as const) {
    const original = console[level].bind(console);
    console[level] = (...args: unknown[]) => {
      original(...args);
      // Only the strings: CheerpJ also logs JVM objects, and converting one to text can throw --
      // which, inside a console call the JVM itself made, would take the boot down with it.
      const text = args.filter((arg) => typeof arg === "string").join(" ");
      // CheerpJ's own progress chatter is not Spark's; the boot steps already cover it.
      if (text.trim() && !text.startsWith("CheerpJ")) onLog(text);
    };
  }
}

export async function boot({ onLog, onStep, onManifest }: BootOptions): Promise<Driver> {
  const started = performance.now();
  const manifest: Manifest = await fetch(`${ASSETS}/manifest.json`).then((response) => {
    if (!response.ok) {
      throw new Error(
        `${ASSETS}/manifest.json: ${response.status}. Run \`npm run assets\` in spark/ to build the runtime.`,
      );
    }
    return response.json();
  });
  onManifest(manifest);
  captureOutput(onLog);

  const jvm = onStep(`Starting the CheerpJ JVM (OpenJDK 8, WebAssembly)`);
  await cheerpjInit({
    version: 8,
    status: "none",
    javaProperties: [`java.nio.channels.spi.SelectorProvider=${SELECTOR}`],
  });
  jvm.done();

  // /app is CheerpJ's read-only view of this origin, so the classpath is just the site's own URLs;
  // the JVM fetches the ranges of each jar it needs rather than the whole file.
  const classPath = manifest.classpath.map((jar) => `/app${ASSETS}/jars/${jar}`).join(":");
  const loading = onStep(`Loading Spark ${manifest.sparkVersion} from ${manifest.classpath.length} jars`);
  const library = await cheerpjRunLibrary(classPath);
  const console_ = await library.org.apacheprojects.sparkwasm.console.SparkConsole;
  loading.done();

  const driver = onStep("Starting the Spark driver in this tab (local[*])");
  const session: Session | (Session & { ok: false }) = JSON.parse(
    await console_.start("local[*]", `/app${ASSETS}/${manifest.data.path}`, manifest.data.view),
  );
  if (!session.ok) {
    driver.failed("failed");
    throw new Error(JSON.stringify(session));
  }
  driver.done(`application ${session.applicationId}`);
  onStep(
    `Registered ${manifest.data.view}: ${manifest.data.rows.toLocaleString()} rows read from ` +
      `${manifest.data.path}`,
  ).done();

  const bootMs = performance.now() - started;
  return {
    session,
    bootMs,
    query: async (sql: string) => JSON.parse(await console_.query(sql)) as QueryResult,
  };
}
