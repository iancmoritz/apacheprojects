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

// Assembles everything the workers load at runtime into public/_cassandra/:
//
//   * the jars from the official Apache Cassandra binary distribution, minus the ones a browser
//     node can never use (native compressors, the Java driver, the UDF compiler);
//   * patch.jar, the handful of Cassandra classes recompiled for CheerpJ (see java/patch/), which
//     has to come *before* the distribution on the classpath;
//   * boot.jar, the node entry point (java/Node.java).
//
// Both jars must be Java 8 bytecode compiled against a Java 8 class library: CheerpJ runs a Java 8
// JVM, and javac's `-source 8 -target 8` against a newer bootclasspath happily emits Java 9+ method
// signatures (ByteBuffer's covariant overrides, for one) that fail at runtime with confusing
// NoSuchMethodErrors.  So a JDK 8 is downloaded here unless one is already on PATH.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { copyFile, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

const HERE = import.meta.dirname;
const PROJECT = join(HERE, "..");
const CACHE = join(PROJECT, ".cache");
const OUT = join(PROJECT, "public", "_cassandra");

const CASSANDRA = "4.1.12";
const TARBALL = `apache-cassandra-${CASSANDRA}-bin.tar.gz`;
const TARBALL_URL = `https://archive.apache.org/dist/cassandra/${CASSANDRA}/${TARBALL}`;

const JDK8 = "jdk8u462-b08";
const JDK8_URL =
  "https://github.com/adoptium/temurin8-binaries/releases/download/" +
  `${JDK8}/OpenJDK8U-jdk_x64_linux_hotspot_8u462b08.tar.gz`;

// Jars the browser cannot use, or that nothing on the node's path touches.  Dropping them takes the
// download from 52 MB to 33 MB; the classpath is otherwise the distribution's own lib/.
const SKIP = [
  /^zstd-jni-/, // JNI compressor: no native code under CheerpJ, and LZ4 is what we configure
  /^snappy-java-/, // ditto
  /^netty-tcnative-boringssl-static-/, // native TLS, and there are no sockets at all here
  /^cassandra-driver-core-/, // the Java driver, for fqltool/cqlsh; the page speaks to nodes directly
  /^ecj-/, // the Eclipse compiler, only used to compile Java UDFs
];

async function download(url, file) {
  const path = join(CACHE, file);
  if (existsSync(path)) return path;
  await mkdir(CACHE, { recursive: true });
  process.stdout.write(`downloading ${url}\n`);
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: ${response.status}`);
  await writeFile(path, Buffer.from(await response.arrayBuffer()));
  return path;
}

function run(command, args, cwd = PROJECT) {
  execFileSync(command, args, { cwd, stdio: ["ignore", "inherit", "inherit"] });
}

/** A JDK 8 javac: whatever is on PATH if it is version 8, otherwise a downloaded Temurin. */
async function javaHome() {
  const path = process.env.JAVA_HOME_8 ?? "";
  if (path && existsSync(join(path, "bin", "javac"))) return path;
  try {
    const version = execFileSync("javac", ["-version"], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
    if (/^javac 1\.8\./.test(version.trim())) return null; // on PATH already
  } catch {
    // no javac at all; fall through to the download
  }
  const home = join(CACHE, JDK8);
  if (!existsSync(join(home, "bin", "javac"))) {
    const tarball = await download(JDK8_URL, "jdk8.tar.gz");
    await mkdir(home, { recursive: true });
    run("tar", ["xzf", tarball, "-C", home, "--strip-components=1"]);
  }
  return home;
}

/** Every source file under `dir`. */
async function sources(dir) {
  const files = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) files.push(...(await sources(path)));
    else if (entry.name.endsWith(".java")) files.push(path);
  }
  return files;
}

async function main() {
  const tarball = await download(TARBALL_URL, TARBALL);
  const dist = join(CACHE, `apache-cassandra-${CASSANDRA}`);
  if (!existsSync(dist)) run("tar", ["xzf", tarball, "-C", CACHE]);

  await rm(OUT, { recursive: true, force: true });
  await mkdir(join(OUT, "lib"), { recursive: true });

  const jars = (await readdir(join(dist, "lib")))
    .filter((name) => name.endsWith(".jar") && !SKIP.some((skip) => skip.test(name)))
    .sort();
  for (const jar of jars) await copyFile(join(dist, "lib", jar), join(OUT, "lib", jar));

  // CheerpJ mounts the origin at /app/, so these are the paths the worker passes to cheerpjRunMain.
  const classpath = ["/app/_cassandra/patch.jar", "/app/_cassandra/boot.jar"]
    .concat(jars.map((jar) => `/app/_cassandra/lib/${jar}`))
    .join(":");
  await writeFile(join(OUT, "classpath.txt"), classpath);

  const home = await javaHome();
  const javac = home ? join(home, "bin", "javac") : "javac";
  const jar = home ? join(home, "bin", "jar") : "jar";
  const local = jars.map((name) => join(OUT, "lib", name)).join(":");
  const classes = join(CACHE, "classes");
  await rm(classes, { recursive: true, force: true });
  await mkdir(join(classes, "patch"), { recursive: true });
  await mkdir(join(classes, "boot"), { recursive: true });

  const patches = await sources(join(PROJECT, "java", "patch"));
  run(javac, ["-nowarn", "-cp", local, "-d", join(classes, "patch"), ...patches]);
  run(jar, ["cf", join(OUT, "patch.jar"), "-C", join(classes, "patch"), "."]);

  run(javac, [
    "-nowarn",
    "-cp", `${join(OUT, "patch.jar")}:${local}`,
    "-d", join(classes, "boot"),
    join(PROJECT, "java", "Node.java"),
  ]);
  // logback picks this up off the classpath, and boot.jar precedes the distribution's own copy.
  await copyFile(join(PROJECT, "java", "logback.xml"), join(classes, "boot", "logback.xml"));
  run(jar, ["cf", join(OUT, "boot.jar"), "-C", join(classes, "boot"), "."]);

  let bytes = 0;
  for (const name of await readdir(join(OUT, "lib"))) {
    bytes += (await readFile(join(OUT, "lib", name))).length;
  }
  for (const name of ["patch.jar", "boot.jar"]) {
    bytes += (await readFile(join(OUT, name))).length;
  }
  const digest = createHash("sha256").update(await readFile(join(OUT, "patch.jar"))).digest("hex");
  process.stdout.write(
    `built _cassandra: ${jars.length} distribution jars + patch.jar (${digest.slice(0, 12)}) + ` +
      `boot.jar, ${(bytes / 1024 / 1024).toFixed(1)} MiB total\n`,
  );
}

await main();
