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

// Drives java/.../console/Probe.java: the JDK calls Hadoop's daemons make, each run on its own so a
// missing one names itself.  Development only -- vite build never sees ../probe.html.

import type { Manifest } from "./protocol";

declare function cheerpjInit(options: { version?: number; status?: string; javaProperties?: string[] }): Promise<void>;
declare function cheerpjRunLibrary(classPath: string): Promise<{
  org: {
    apacheprojects: {
      hadoopwasm: { console: { Probe: Promise<{ all(directory: string): Promise<string> }> } };
    };
  };
}>;

const out = document.getElementById("out") as HTMLPreElement;
const manifest: Manifest = await fetch("/_hadoop/manifest.json").then((response) => response.json());

await cheerpjInit({
  version: 8,
  status: "none",
  javaProperties: [
    "java.nio.channels.spi.SelectorProvider=org.apacheprojects.hadoopwasm.net.VirtualSelectorProvider",
    "java.io.tmpdir=/files/hadoop/tmp",
    "hadoopwasm.storage=/files/hadoop",
    "hadoopwasm.trace=true",
  ],
});
const library = await cheerpjRunLibrary(
  manifest.classpath.map((jar) => `/app/_hadoop/jars/${jar}`).join(":"),
);
const probe = await library.org.apacheprojects.hadoopwasm.console.Probe;
const results = JSON.parse(await probe.all("/files/hadoop/probe"));
out.textContent = results
  .map((check: { name: string; status: string; detail: string; ms: number }) =>
    `${check.status === "ok" ? "\u2713" : "\u2717"} ${check.name}: ${check.detail} (${check.ms} ms)`,
  )
  .join("\n");
