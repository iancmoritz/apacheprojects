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

// Turns the official Apache Flink distribution into the classpath the tab runs.
//
// Nothing here reimplements Flink: the classes are the ones the ASF publishes.  The script only
//
//   * drops what a browser can never use (the RocksDB, snappy and netty native libraries, which are
//     three quarters of flink-dist's bytes),
//   * lifts flink-rpc-akka.jar out of flink-dist onto the classpath, because Flink's normal way of
//     loading it -- copy the nested jar to a temp dir, open it in a child class loader -- is one
//     more thing to go wrong under CheerpJ, and the fat jar shares no class with flink-dist,
//   * compiles the browser-side Java in ../java (the cluster console and the streaming job),
//   * runs the ASM passes in ../java/.../tools that work around what CheerpJ's JVM does differently.
//
// Everything it writes -- .cache/ and public/_flink/ -- is gitignored; run `npm run assets`.

import { spawnSync } from "node:child_process";
import { createWriteStream } from "node:fs";
import { access, cp, mkdir, mkdtemp, open, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { argv } from "node:process";
import { pipeline } from "node:stream/promises";

const FLINK_VERSION = "1.20.5";
const SCALA_VERSION = "2.12";
const DIST = `flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}`;
const MIRROR = `https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${DIST}.tgz`;

/** The directory the archive unpacks into, which is not named after the binary distribution. */
const PREFIX = `flink-${FLINK_VERSION}`;

/** Downloaded when the machine has no JDK of its own, the way Vercel's build image does not. */
const JDK = "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse";

const root = join(import.meta.dirname, "..");
const cache = join(root, ".cache");
const jars = join(cache, "jars");
const work = join(cache, "work");
const target = join(root, "public", "_flink");

/**
 * The distribution jars the browser gets.
 *
 * flink-dist is the whole runtime -- flink-core, flink-runtime, flink-streaming-java, flink-clients
 * and their shaded dependencies in one jar -- so a DataStream job needs nothing else beyond a
 * logging backend.  The table planner, CEP, the connectors and the Scala API stay behind.
 */
const KEEP = [
  `flink-dist-${FLINK_VERSION}.jar`,
  "log4j-api-2.24.3.jar",
  "log4j-core-2.24.3.jar",
  "log4j-slf4j-impl-2.24.3.jar",
];

const FLINK_DIST = `flink-dist-${FLINK_VERSION}.jar`;

/** Flink's RPC fat jar, nested inside flink-dist; hoisted onto the classpath by `flatten()`. */
const RPC_JAR = "flink-rpc-akka.jar";

/**
 * Entries of flink-dist a browser can never load, dropped before the jar is served.
 *
 * The native libraries alone are 110 MB uncompressed: eleven RocksDB builds for platforms this JVM
 * is not, plus snappy and netty's epoll transports.  The Java classes that would load them stay, so
 * nothing on the configuration or reflection paths changes shape; asking for the RocksDB state
 * backend simply fails the way asking for it on an unsupported platform always has.
 */
const DROP = [
  (name) => /^librocksdbjni.*\.(so|dll|jnilib)$/.test(name),
  (name) => name.startsWith("org/xerial/snappy/native/"),
  (name) => name.startsWith("META-INF/native/"),
  // Monaco: 11 MB of the dashboard's 15 MB, in a chunk only its log and exception viewers load, and
  // the REST endpoint unpacks all of web/ into the virtual filesystem while the cluster starts.
  (name) => name.startsWith("web/assets/vs/"),
];

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
    run("mv", [`${archive}.part`, archive]);
  }
  return archive;
}

/** Extracts just the jars on the classpath. */
async function extract(archive) {
  await rm(jars, { recursive: true, force: true });
  await mkdir(jars, { recursive: true });
  console.log(`extracting ${KEEP.length} jars`);
  run("tar", ["xzf", archive, "-C", jars, "--strip-components=2", ...KEEP.map((jar) => `${PREFIX}/lib/${jar}`)]);
  const missing = [];
  for (const jar of KEEP) if (!(await exists(join(jars, jar)))) missing.push(jar);
  if (missing.length) throw new Error(`missing from the distribution: ${missing.join(", ")}`);
}

/**
 * Rewrites flink-dist without its native libraries, and with its RPC fat jar hoisted out beside it.
 *
 * Both halves of the split are self-contained: the fat jar and flink-dist have no class in common,
 * and the loader that would have unpacked it at run time is replaced by
 * org.apacheprojects.flinkwasm.rpc.FlatPekkoRpcSystemLoader, which just asks the classpath.
 */
async function flatten() {
  const scratch = await mkdtemp(join(tmpdir(), "flinkdist-"));
  const dist = join(jars, FLINK_DIST);
  const before = (await stat(dist)).size;
  run(tools.jar, ["xf", dist], { cwd: scratch });

  await cp(join(scratch, RPC_JAR), join(jars, RPC_JAR));
  await rm(join(scratch, RPC_JAR));

  let dropped = 0;
  for (const name of await walk(scratch, "", "")) {
    if (!DROP.some((matches) => matches(name))) continue;
    dropped += (await stat(join(scratch, name))).size;
    await rm(join(scratch, name));
  }

  await rm(dist);
  run(tools.jar, ["cf", dist, "-C", scratch, "."]);
  await rm(scratch, { recursive: true, force: true });
  const after = (await stat(dist)).size;
  console.log(
    `${FLINK_DIST}: ${(before / 1e6).toFixed(0)} MB -> ${(after / 1e6).toFixed(0)} MB ` +
      `(${(dropped / 1e6).toFixed(0)} MB of native libraries dropped, ${RPC_JAR} hoisted)`,
  );
}

/** Compiles the browser-side Java (the console, the streaming job, the ASM passes). */
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

    // Zip64, which flink-dist needs: the 32-bit fields above are saturated.
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

/** Runs one of the ASM passes; they all write a jar of the classes they changed. */
function pass(classes, name, output, ...args) {
  run(tools.java, ["-cp", `${classes}:${jars}/*`, `org.apacheprojects.flinkwasm.tools.${name}`, output, ...args]);
  return output;
}

/** Runs every bytecode pass and returns the directory of patched class files. */
async function patch(classes) {
  const overlay = join(work, "overlay");
  const staging = join(work, "staging");
  await rm(overlay, { recursive: true, force: true });
  await rm(staging, { recursive: true, force: true });
  await mkdir(overlay, { recursive: true });
  await mkdir(staging, { recursive: true });

  // The console, the job and the shims: compiled here, not patched from anything.  The ASM passes
  // under tools/ only ever run at build time, so they stay out of what the browser downloads.
  await cp(join(classes, "org", "apacheprojects"), join(overlay, "org", "apacheprojects"), { recursive: true });
  await rm(join(overlay, "org", "apacheprojects", "flinkwasm", "tools"), { recursive: true, force: true });
  await rm(join(overlay, "org", "apacheprojects", "flinkwasm", "probe"), { recursive: true, force: true });

  // Sources under java/org/apache/flink shadow a class of the distribution's; `assemble()` merges
  // each one into the jar that owns that name, so Flink loads ours instead.
  await cp(join(classes, "org", "apache"), join(overlay, "org", "apache"), { recursive: true });

  for (const [name, ...args] of PASSES) {
    layer(overlay, pass(classes, name, join(staging, `${name}.jar`), ...args));
  }
  return overlay;
}

/** Unpacks `jar` over `dir`, so later passes win over earlier ones. A pass may change nothing. */
function layer(dir, jar) {
  unpack(jar, dir);
}

/** The ASM passes, in the order they run; see ../README.md for what each one works around. */
const PASSES = [];

/**
 * Writes the runtime classpath: every jar, with the patched classes merged into the jar that owns
 * them, plus one jar of the classes that belong to no upstream jar (the console, the job, the shims).
 */
async function assemble(overlay) {
  await rm(target, { recursive: true, force: true });
  await mkdir(join(target, "jars"), { recursive: true });

  const patched = new Set(await walk(overlay));
  const owned = new Set();
  const classpath = [];

  for (const jar of [...KEEP, RPC_JAR]) {
    const mine = (await entries(join(jars, jar))).map((entry) => entry.name).filter((name) => patched.has(name));
    if (mine.length === 0) {
      await cp(join(jars, jar), join(target, "jars", jar));
    } else {
      const scratch = await mkdtemp(join(tmpdir(), "flinkjar-"));
      unpack(join(jars, jar), scratch);
      for (const entry of mine) {
        await cp(join(overlay, entry), join(scratch, entry));
        owned.add(entry);
      }
      await rm(join(target, "jars", jar), { force: true });
      run(tools.jar, ["cf", join(target, "jars", jar), "-C", scratch, "."]);
      await rm(scratch, { recursive: true, force: true });
      console.log(`  ${jar}: ${mine.length} patched classes merged`);
    }
    classpath.push(jar);
  }

  // The console, the job, the RPC loader and the shims: nothing upstream owns these.
  const ours = await mkdtemp(join(tmpdir(), "flinkwasm-"));
  for (const entry of patched) {
    if (owned.has(entry)) continue;
    await mkdir(join(ours, entry.split("/").slice(0, -1).join("/")), { recursive: true });
    await cp(join(overlay, entry), join(ours, entry));
  }
  await cp(join(root, "conf"), ours, { recursive: true });
  run(tools.jar, ["cf", join(target, "jars", "flinkwasm.jar"), "-C", ours, "."]);
  await rm(ours, { recursive: true, force: true });

  // flinkwasm.jar first: its META-INF/services and log4j2.properties have to win.
  return ["flinkwasm.jar", ...classpath];
}

async function main() {
  if (!argv.includes("--force") && (await exists(join(target, "manifest.json")))) {
    console.log("public/_flink is already populated; pass --force to rebuild");
    return;
  }
  tools = await jdk();
  const archive = await distribution();
  await extract(archive);
  await flatten();
  const classes = await compile();
  const overlay = await patch(classes);
  const classpath = await assemble(overlay);

  let bytes = 0;
  for (const jar of classpath) bytes += (await stat(join(target, "jars", jar))).size;
  const manifest = {
    flinkVersion: FLINK_VERSION,
    scalaVersion: SCALA_VERSION,
    distribution: DIST,
    source: MIRROR,
    classpath,
    jarBytes: bytes,
  };
  await writeFile(join(target, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(`wrote ${relative(root, target)}: ${classpath.length} jars, ${(bytes / 1e6).toFixed(0)} MB`);
}

await main();
