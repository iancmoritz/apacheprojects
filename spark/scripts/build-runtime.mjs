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

// Turns the official Apache Spark distribution into the classpath the tab runs.
//
// Nothing here is a reimplementation of Spark: the jars are the ones the ASF publishes, and the
// only bytecode this script produces are the seven ASM passes in ../java/.../tools, each working
// around one thing CheerpJ's JVM does differently (see ../README.md).  The patched classes are
// merged back into the jar they came from, so the browser never downloads a class twice.
//
// Everything it writes -- .cache/ and public/_spark/ -- is gitignored; run `npm run assets`.

import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createWriteStream } from "node:fs";
import { access, cp, mkdir, mkdtemp, open, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { argv } from "node:process";
import { pipeline } from "node:stream/promises";

const SPARK_VERSION = "3.5.9";
const SCALA_VERSION = "2.12";
const DIST = `spark-${SPARK_VERSION}-bin-hadoop3`;
const MIRROR = `https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${DIST}.tgz`;

/** Downloaded when the machine has no JDK of its own, the way Vercel's build image does not. */
const JDK = "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse";

const root = join(import.meta.dirname, "..");
const cache = join(root, ".cache");
const jars = join(cache, "jars");
const work = join(cache, "work");
const target = join(root, "public", "_spark");

/** The jars Spark needs to plan and run a local query, out of the 250 the distribution ships. */
const KEEP = await readFile(join(root, "scripts", "classpath.txt"), "utf8")
  .then((text) => text.split("\n").map((line) => line.trim()).filter((line) => line && !line.startsWith("#")));

/** Jars whose lambda call sites are rewritten; every Spark closure a task can capture lives here. */
const INDY_JARS = [
  `spark-core_${SCALA_VERSION}-${SPARK_VERSION}.jar`,
  `spark-sql_${SCALA_VERSION}-${SPARK_VERSION}.jar`,
  `spark-catalyst_${SCALA_VERSION}-${SPARK_VERSION}.jar`,
  `spark-sql-api_${SCALA_VERSION}-${SPARK_VERSION}.jar`,
  `spark-common-utils_${SCALA_VERSION}-${SPARK_VERSION}.jar`,
];

const SCALA_LIBRARY = `scala-library-2.12.18.jar`;
const SPARK_CORE = `spark-core_${SCALA_VERSION}-${SPARK_VERSION}.jar`;

/** The JDK this build compiles and patches with; resolved by `jdk()` before anything uses it. */
let tools = { javac: "javac", java: "java", jar: "jar" };

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: "inherit", ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} exited ${result.status}`);
}

async function exists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

/** `javac`, `java` and `jar`: the ones on PATH, or a JDK downloaded once into .cache. */
async function jdk() {
  const onPath = spawnSync("javac", ["-version"], { encoding: "utf8" });
  if (!onPath.error && onPath.status === 0) return { javac: "javac", java: "java", jar: "jar" };

  const home = join(cache, "jdk");
  if (!(await exists(home))) {
    console.log(`no javac on PATH; downloading ${JDK}`);
    const response = await fetch(JDK, { redirect: "follow" });
    if (!response.ok) throw new Error(`${JDK}: ${response.status}`);
    const archive = join(cache, "jdk.tgz");
    await mkdir(cache, { recursive: true });
    await pipeline(response.body, createWriteStream(`${archive}.part`));
    run("mv", [`${archive}.part`, archive]);
    await mkdir(home, { recursive: true });
    run("tar", ["xzf", archive, "-C", home, "--strip-components=1"]);
  }
  const bin = join(home, "bin");
  return { javac: join(bin, "javac"), java: join(bin, "java"), jar: join(bin, "jar") };
}

/** The distribution, downloaded once into .cache and kept there. */
async function distribution() {
  const archive = join(cache, `${DIST}.tgz`);
  if (!(await exists(archive))) {
    console.log(`downloading ${MIRROR}`);
    const response = await fetch(MIRROR);
    if (!response.ok) throw new Error(`${MIRROR}: ${response.status}`);
    await mkdir(cache, { recursive: true });
    await pipeline(response.body, createWriteStream(`${archive}.part`));
    await run("mv", [`${archive}.part`, archive]);
  }
  return archive;
}

/** Extracts just the jars on the classpath. */
async function extract(archive) {
  await rm(jars, { recursive: true, force: true });
  await mkdir(jars, { recursive: true });
  console.log(`extracting ${KEEP.length} jars`);
  run("tar", ["xzf", archive, "-C", jars, "--strip-components=2", ...KEEP.map((jar) => `${DIST}/jars/${jar}`)]);
  const missing = [];
  for (const jar of KEEP) if (!(await exists(join(jars, jar)))) missing.push(jar);
  if (missing.length) throw new Error(`missing from the distribution: ${missing.join(", ")}`);
}

/** Compiles the browser-side Java (the console, the virtual network, the ASM passes). */
async function compile() {
  const classes = join(work, "classes");
  await rm(classes, { recursive: true, force: true });
  await mkdir(classes, { recursive: true });
  const sources = await walk(join(root, "java"), "", ".java");
  console.log(`compiling ${sources.length} java sources`);
  const paths = sources.map((source) => join(root, "java", source));
  run(tools.javac, ["-source", "8", "-target", "8", "-nowarn", "-cp", `${jars}/*`, "-d", classes, ...paths]);
  return classes;
}

/** Runs one of the ASM passes; they all write a jar of the classes they changed. */
function pass(classes, name, output, ...args) {
  run(tools.java, ["-cp", `${classes}:${jars}/*`, `org.apacheprojects.sparkwasm.tools.${name}`, output, ...args]);
  return output;
}

/** Unpacks `jar` over `dir`, so later passes win over earlier ones. A pass may change nothing. */
function layer(dir, jar) {
  unpack(jar, dir);
}

/**
 * Every entry of a zip, read out of its central directory.
 *
 * Doing this here rather than shelling out to `unzip` keeps the build to node, `tar` and the JDK,
 * which is all a machine like Vercel's builder has.
 */
async function entries(path) {
  const handle = await open(path, "r");
  try {
    const { size } = await handle.stat();
    const tailBytes = Math.min(size, 0x10000 + 22);
    const tail = Buffer.alloc(tailBytes);
    await handle.read(tail, 0, tailBytes, size - tailBytes);

    let end = -1;
    for (let at = tail.length - 22; at >= 0; at--) {
      if (tail.readUInt32LE(at) === 0x06054b50) {
        end = at;
        break;
      }
    }
    if (end < 0) throw new Error(`${path}: no end of central directory`);

    let count = tail.readUInt16LE(end + 10);
    let length = tail.readUInt32LE(end + 12);
    let offset = tail.readUInt32LE(end + 16);

    // Zip64, which the bigger Spark jars need: the 32-bit fields above are saturated.
    if (offset === 0xffffffff || length === 0xffffffff || count === 0xffff) {
      for (let at = end - 20; at >= 0; at--) {
        if (tail.readUInt32LE(at) !== 0x07064b50) continue;
        const locator = Number(tail.readBigUInt64LE(at + 8));
        const header = Buffer.alloc(56);
        await handle.read(header, 0, 56, locator);
        if (header.readUInt32LE(0) !== 0x06064b50) throw new Error(`${path}: bad zip64 directory`);
        count = Number(header.readBigUInt64LE(32));
        length = Number(header.readBigUInt64LE(40));
        offset = Number(header.readBigUInt64LE(48));
        break;
      }
    }

    const directory = Buffer.alloc(length);
    await handle.read(directory, 0, length, offset);

    const found = [];
    let at = 0;
    for (let i = 0; i < count && at + 46 <= directory.length; i++) {
      if (directory.readUInt32LE(at) !== 0x02014b50) throw new Error(`${path}: bad directory entry`);
      const nameLength = directory.readUInt16LE(at + 28);
      const extraLength = directory.readUInt16LE(at + 30);
      const commentLength = directory.readUInt16LE(at + 32);
      const name = directory.toString("utf8", at + 46, at + 46 + nameLength);
      found.push({ name, bytes: directory.readUInt32LE(at + 24) });
      at += 46 + nameLength + extraLength + commentLength;
    }
    return found;
  } finally {
    await handle.close();
  }
}

/** Unpacks a jar into `dir` with the JDK's own `jar`, overwriting what is already there. */
function unpack(jar, dir) {
  run(tools.jar, ["xf", jar], { cwd: dir });
}

/**
 * Runs every bytecode pass and returns the directory of patched class files.
 *
 * The order is the point: a class the lambda rewrite produced has to go through the unsafe and wide
 * locals passes again, because those look at bytecode the rewrite has already changed.
 */
async function patch(classes) {
  const overlay = join(work, "overlay");
  const staging = join(work, "staging");
  await rm(overlay, { recursive: true, force: true });
  await rm(staging, { recursive: true, force: true });
  await mkdir(overlay, { recursive: true });
  await mkdir(staging, { recursive: true });

  // The console and the shims themselves: compiled here, not patched from anything.  The ASM passes
  // under tools/ only ever run at build time, so they stay out of what the browser downloads.
  await cp(join(classes, "org", "apacheprojects"), join(overlay, "org", "apacheprojects"), { recursive: true });
  await rm(join(overlay, "org", "apacheprojects", "sparkwasm", "tools"), { recursive: true, force: true });

  const unsafe = pass(classes, "UnsafeShimPatcher", join(staging, "unsafe.jar"), jars);
  const wide = pass(classes, "WideLocalsPatcher", join(staging, "wide.jar"), jars);
  const deserialize = pass(classes, "LambdaDeserializePatcher", join(staging, "deserialize.jar"), join(jars, SCALA_LIBRARY));
  const hook = pass(classes, "ClosureStreamPatcher", join(staging, "hook.jar"), join(jars, SPARK_CORE));
  const clean = pass(classes, "CleanClosurePatcher", join(staging, "clean.jar"), join(jars, SPARK_CORE));
  const indy = pass(classes, "IndyPatcher", join(staging, "indy.jar"), jars, "org/apache/spark/", ...INDY_JARS);

  // The two SparkContext patches change classes the lambda rewrite also has to see.
  const hooked = join(staging, "hooked");
  await mkdir(hooked, { recursive: true });
  layer(hooked, hook);
  layer(hooked, clean);
  run(tools.jar, ["cf", join(staging, "hooked.jar"), "-C", hooked, "."]);
  const indyHooked = pass(classes, "IndyPatcher", join(staging, "indy-hooked.jar"), staging, "org/apache/spark/", "hooked.jar");

  for (const jar of [unsafe, wide, deserialize, indy, join(staging, "hooked.jar"), indyHooked]) layer(overlay, jar);

  // The rewritten classes need the unsafe and wide locals passes over their new bytecode.
  const patched = join(staging, "patched");
  await mkdir(patched, { recursive: true });
  run(tools.jar, ["cf", join(patched, "patched.jar"), "-C", overlay, "."]);
  for (const jar of await readdir(jars)) await cp(join(jars, jar), join(patched, jar));
  const unsafeAgain = pass(classes, "UnsafeShimPatcher", join(staging, "unsafe-again.jar"), patched, "patched.jar");
  layer(overlay, unsafeAgain);
  await rm(join(patched, "patched.jar"), { force: true });
  run(tools.jar, ["cf", join(patched, "patched.jar"), "-C", overlay, "."]);
  const wideAgain = pass(classes, "WideLocalsPatcher", join(staging, "wide-again.jar"), patched, "patched.jar");
  layer(overlay, wideAgain);
  return overlay;
}

/** Every file with `suffix` under `dir`, as jar entry names. */
async function walk(dir, prefix = "", suffix = ".class") {
  const entries = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) entries.push(...(await walk(join(dir, entry.name), path, suffix)));
    else if (entry.name.endsWith(suffix)) entries.push(path);
  }
  return entries;
}

/**
 * Writes the runtime classpath: every jar, with the patched classes merged into the jar that owns
 * them, plus one jar of the classes that belong to no upstream jar (the console and the shims).
 */
async function assemble(overlay) {
  await rm(target, { recursive: true, force: true });
  await mkdir(join(target, "jars"), { recursive: true });

  const patched = new Set(await walk(overlay));

  const owned = new Set();
  const classpath = [];

  for (const jar of KEEP) {
    const mine = (await entries(join(jars, jar))).map((entry) => entry.name).filter((name) => patched.has(name));
    if (mine.length === 0) {
      await cp(join(jars, jar), join(target, "jars", jar));
    } else {
      const scratch = await mkdtemp(join(tmpdir(), "sparkjar-"));
      unpack(join(jars, jar), scratch);
      for (const entry of mine) {
        await cp(join(overlay, entry), join(scratch, entry));
        owned.add(entry);
      }
      run(tools.jar, ["cf", join(target, "jars", jar), "-C", scratch, "."]);
      await rm(scratch, { recursive: true, force: true });
      console.log(`  ${jar}: ${mine.length} patched classes merged`);
    }
    classpath.push(jar);
  }

  // The console, the virtual network, the closure adapters, and the Scala/Spark classes the passes
  // generate: nothing upstream owns these.
  const ours = await mkdtemp(join(tmpdir(), "sparkwasm-"));
  for (const entry of patched) {
    if (owned.has(entry)) continue;
    await mkdir(join(ours, entry.split("/").slice(0, -1).join("/")), { recursive: true });
    await cp(join(overlay, entry), join(ours, entry));
  }
  run(tools.jar, ["cf", join(target, "jars", "sparkwasm.jar"), "-C", ours, "."]);
  await rm(ours, { recursive: true, force: true });
  return ["sparkwasm.jar", ...classpath];
}

/**
 * The bundled dataset: one row per class in Spark's own jars.
 *
 * Real data that comes out of the artifacts the page is running, so a query over it is a query about
 * the Spark in the tab.
 */
async function dataset() {
  const rows = ["jar,package,class,bytes,major_version"];
  for (const jar of KEEP.filter((name) => name.startsWith("spark-"))) {
    for (const { name: entry, bytes } of await entries(join(jars, jar))) {
      if (!entry.endsWith(".class") || entry.includes("$$")) continue;
      const parts = entry.slice(0, -".class".length).split("/");
      const name = parts.pop();
      rows.push([jar.replace(/\.jar$/, ""), parts.join("."), name, bytes, 52].join(","));
    }
  }
  await mkdir(join(target, "data"), { recursive: true });
  const csv = `${rows.join("\n")}\n`;
  await writeFile(join(target, "data", "spark-classes.csv"), csv);
  return { rows: rows.length - 1, bytes: csv.length };
}

async function main() {
  if (!argv.includes("--force") && (await exists(join(target, "manifest.json")))) {
    console.log("public/_spark is already populated; pass --force to rebuild");
    return;
  }
  tools = await jdk();
  const archive = await distribution();
  await extract(archive);
  const classes = await compile();
  const overlay = await patch(classes);
  const classpath = await assemble(overlay);
  const data = await dataset();

  let bytes = 0;
  for (const jar of classpath) bytes += (await stat(join(target, "jars", jar))).size;
  const manifest = {
    sparkVersion: SPARK_VERSION,
    scalaVersion: "2.12.18",
    distribution: DIST,
    source: MIRROR,
    classpath,
    jarBytes: bytes,
    data: { path: "data/spark-classes.csv", view: "spark_classes", ...data },
  };
  await writeFile(join(target, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(
    `wrote ${relative(root, target)}: ${classpath.length} jars, ${(bytes / 1e6).toFixed(0)} MB, ` +
      `${data.rows} data rows (sha ${createHash("sha256").update(JSON.stringify(manifest)).digest("hex").slice(0, 8)})`,
  );
}

await main();
