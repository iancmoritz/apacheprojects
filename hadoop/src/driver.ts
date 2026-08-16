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

// Boots CheerpJ's JVM and runs whole Hadoop sessions inside it, one call at a time.
//
// CheerpJ's library mode gives JavaScript an async proxy for the JVM's classes, but it will not
// re-enter Java while the cluster's threads are alive: with MiniDFSCluster running, a second call from
// the page never returns, not even one that only returns a string.  So a run is one call -- start the
// cluster, then execute the visitor's commands -- and a reload is what starts the next one.

import type { Failure, Manifest, Session } from "./protocol";

/** Where the build script put the jars, relative to the site root. */
const ASSETS = "/_hadoop";

/**
 * Hadoop's RPC (`ipc.Server`, `ipc.Client`) and the DataNode's data transfer protocol open real
 * sockets, which a tab cannot.  This selector provider answers them with in-process loopback ones;
 * without it the JVM dies in `EPollSelectorProvider` with `UnsatisfiedLinkError:
 * Java_sun_nio_ch_EPoll_eventSize`.
 */
const SELECTOR = "org.apacheprojects.hadoopwasm.net.VirtualSelectorProvider";

/** CheerpJ's writable filesystem; the NameNode and DataNode keep their storage directories here. */
const STORAGE = "/files/hadoop";

declare function cheerpjInit(options: {
  version?: number;
  status?: "full" | "compact" | "none";
  javaProperties?: string[];
}): Promise<void>;

declare function cheerpjRunLibrary(classPath: string): Promise<CheerpJLibrary>;

interface CheerpJLibrary {
  org: {
    apacheprojects: {
      hadoopwasm: {
        console: {
          HadoopConsole: Promise<{
            session(
              script: string,
              writePath: string,
              writeText: string,
              mode: Mode,
            ): Promise<string>;
            hangs(): Promise<string>;
          }>;
        };
      };
    };
  };
}

/**
 * Which filesystem the run uses.
 *
 * `hdfs` is a real MiniDFSCluster -- a NameNode and a DataNode in this tab. `local` is Hadoop's own
 * LocalFileSystem over CheerpJ's filesystem, with no daemons, which is the only mode MapReduce
 * finishes in: with the cluster up, the map task's read of its input block wedges the JVM.
 */
export type Mode = "hdfs" | "local";

export interface Driver {
  manifest: Manifest;
  /** How long CheerpJ took to have a JVM with Hadoop's jars on its classpath. */
  jvmMs: number;
  /** Where the build script's copies of Hadoop's own text files live, for `put`. */
  dataPath: string;
  /**
   * Starts a cluster, runs the script against it and stops it again.
   *
   * @param writePath a file to write into HDFS before the script runs, or empty for none
   */
  run(script: string, writePath: string, writeText: string, mode: Mode): Promise<Session | Failure>;
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
 * In library mode CheerpJ writes `System.out` to the browser console, which is where the NameNode's
 * and DataNode's own log lines end up; the page shows them so the boot is something you can watch
 * rather than a spinner.
 */
function captureOutput(onLog: (line: string) => void) {
  for (const level of ["log", "info", "warn", "error"] as const) {
    const original = console[level].bind(console);
    console[level] = (...args: unknown[]) => {
      original(...args);
      // Only the strings: CheerpJ also logs JVM objects, and converting one to text can throw --
      // which, inside a console call the JVM itself made, would take the boot down with it.
      const text = args.filter((arg) => typeof arg === "string").join(" ");
      // CheerpJ's own progress chatter is not Hadoop's; the boot steps already cover it.
      if (text.trim() && !text.startsWith("CheerpJ")) onLog(text);
    };
  }
}

export async function boot({ onLog, onStep, onManifest }: BootOptions): Promise<Driver> {
  const started = performance.now();
  const manifest: Manifest = await fetch(`${ASSETS}/manifest.json`).then((response) => {
    if (!response.ok) {
      throw new Error(
        `${ASSETS}/manifest.json: ${response.status}. Run \`npm run assets\` in hadoop/ to build the runtime.`,
      );
    }
    return response.json();
  });
  onManifest(manifest);
  captureOutput(onLog);

  const query = new URLSearchParams(location.search);
  const jvm = onStep("Starting the CheerpJ JVM (OpenJDK 8, WebAssembly)");
  await cheerpjInit({
    version: 8,
    status: "none",
    javaProperties: [
      `java.nio.channels.spi.SelectorProvider=${SELECTOR}`,
      // Jetty (the NameNode's web UI, which MiniDFSCluster starts whether anything can reach it or
      // not) needs a writable temp directory, and CheerpJ's default is the read-only /app mount.
      `java.io.tmpdir=${STORAGE}/tmp`,
      `hadoopwasm.storage=${STORAGE}`,
      "hadoop.home.dir=/files/hadoop-home",
      // A stuck Hadoop thread and a slow one look the same from the page; ?watchdog=<ms> shortens the
      // wait before the JVM dumps every stack into the log below.
      `hadoopwasm.watchdog.ms=${query.get("watchdog") ?? 60000}`,
      ...(query.has("timeout") ? [`hadoopwasm.timeout.ms=${query.get("timeout")}`] : []),
      // ?sample=<ms> asks the running job for its progress and counters that often, which is a
      // timeline for the page but also a call into LocalJobRunner while its task thread holds it.
      ...(query.has("sample") ? [`hadoopwasm.job.sample.ms=${query.get("sample")}`] : []),
      // ?trace=1 records every virtual socket read, write, accept and select to CheerpJ's
      // filesystem, which is how a call that freezes the tab is diagnosed after a reload.
      ...(query.has("trace") ? ["hadoopwasm.trace=true"] : []),
      // ?debug=1 selects the log4j configuration that turns Hadoop's MapReduce, filesystem and HDFS
      // logging up to DEBUG, which is how a Hadoop call that stops making progress says which of its
      // own steps it reached.
      ...(query.has("debug") ? ["log4j.configuration=log4j-debug.properties"] : []),
      // ?unpark=0 turns off the interrupt shim (util/ThreadShim.java), which is how the difference it
      // makes to a Hadoop thread waiting on a lock is measured rather than assumed.
      ...(query.get("unpark") === "0" ? ["hadoopwasm.unpark=false"] : []),
    ],
  });
  jvm.done();

  // /app is CheerpJ's read-only view of this origin, so the classpath is just the site's own URLs;
  // the JVM fetches the ranges of each jar it needs rather than the whole file.
  const classPath = manifest.classpath.map((jar) => `/app${ASSETS}/jars/${jar}`).join(":");
  const loading = onStep(`Loading Hadoop ${manifest.hadoopVersion} from ${manifest.classpath.length} jars`);
  const library = await cheerpjRunLibrary(classPath);
  const console_ = await library.org.apacheprojects.hadoopwasm.console.HadoopConsole;
  loading.done();

  // A call into the console blocks the page until Hadoop answers: CheerpJ runs the JVM on the page's
  // thread, so there is no polling to hide behind. A call that never returns freezes the tab, which is
  // what the JVM-side watchdog is for -- it records the stacks to CheerpJ's filesystem, and a reload
  // reports them.
  //
  // Whatever froze the last visit, if anything did, before this one adds to it.
  const hangs: { log?: string; trace?: string } = JSON.parse(await console_.hangs());
  if (hangs.log) onLog(`the watchdog recorded this before the page was reloaded:\n${hangs.log}`);
  if (hangs.trace) onLog(`the virtual network traced this before the page was reloaded:\n${hangs.trace}`);

  return {
    manifest,
    jvmMs: performance.now() - started,
    dataPath: `/app${ASSETS}/data`,
    run: async (script, writePath, writeText, mode) =>
      JSON.parse(await console_.session(script, writePath, writeText, mode)) as Session | Failure,
  };
}
