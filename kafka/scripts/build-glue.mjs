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

// Compile java/ (the loopback NIO transport plus the KafkaBrowser facade the page calls) into
// public/_kafka/kafka-browser-glue.jar, for Java 8 because that is the CheerpJ runtime Kafka works
// on.  Uses whatever JDK is on PATH; if there is none -- a plain Vercel build image, for instance --
// it downloads a Temurin JDK into .jdk/.  Both the jar and .jdk/ are generated and gitignored.

import { execFileSync } from "node:child_process";
import { access, mkdir, mkdtemp, readdir, rm } from "node:fs/promises";
import { arch, tmpdir, platform } from "node:os";
import { join } from "node:path";
import { writeFile } from "node:fs/promises";

const root = join(import.meta.dirname, "..");
const javaDir = join(root, "java");
const jarsDir = join(root, "public", "_kafka", "jars");
const outJar = join(root, "public", "_kafka", "kafka-browser-glue.jar");
const jdkDir = join(root, ".jdk");

const jars = (await readdir(jarsDir).catch(() => [])).filter((f) => f.endsWith(".jar"));
if (!jars.length) throw new Error("run scripts/fetch-jars.mjs first: public/_kafka/jars is empty");

const { javac, jar } = await toolchain();
const classes = await mkdtemp(join(tmpdir(), "kafka-glue-"));
const sources = (await sourceFiles(javaDir)).sort();
const classpath = jars.map((f) => join(jarsDir, f)).join(":");

execFileSync(javac, ["--release", "8", "-nowarn", "-cp", classpath, "-d", classes, ...sources], {
  stdio: "inherit",
});
await mkdir(join(root, "public", "_kafka"), { recursive: true });
execFileSync(jar, ["--create", "--file", outJar, "-C", classes, "."], { stdio: "inherit" });
await rm(classes, { recursive: true, force: true });
console.log(`built ${outJar} from ${sources.length} sources`);

async function sourceFiles(dir) {
  const out = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...(await sourceFiles(path)));
    else if (entry.name.endsWith(".java")) out.push(path);
  }
  return out;
}

async function toolchain() {
  const local = join(jdkDir, "bin", "javac");
  if (await exists(local)) return { javac: local, jar: join(jdkDir, "bin", "jar") };
  try {
    execFileSync("javac", ["-version"], { stdio: "ignore" });
    return { javac: "javac", jar: "jar" };
  } catch {
    await downloadJdk();
    return { javac: local, jar: join(jdkDir, "bin", "jar") };
  }
}

async function downloadJdk() {
  const os = { linux: "linux", darwin: "mac", win32: "windows" }[platform()];
  const cpu = { x64: "x64", arm64: "aarch64" }[arch()];
  if (!os || !cpu) throw new Error(`no JDK on PATH and no Temurin build for ${platform()}/${arch()}`);
  const url = `https://api.adoptium.net/v3/binary/latest/17/ga/${os}/${cpu}/jdk/hotspot/normal/eclipse`;
  console.log(`no javac on PATH; downloading a JDK from ${url}`);
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
  const work = await mkdtemp(join(tmpdir(), "kafka-jdk-"));
  const archive = join(work, "jdk.tar.gz");
  await writeFile(archive, Buffer.from(await response.arrayBuffer()));
  await rm(jdkDir, { recursive: true, force: true });
  await mkdir(jdkDir, { recursive: true });
  execFileSync("tar", ["-xzf", archive, "-C", jdkDir, "--strip-components", "1"], { stdio: "inherit" });
  await rm(work, { recursive: true, force: true });
}

async function exists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}
