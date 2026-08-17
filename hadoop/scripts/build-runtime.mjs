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

// Turns Apache Hadoop's published jars into the classpath the tab runs.
//
// Nothing here reimplements Hadoop: the jars are the ones the ASF publishes to Maven Central (see
// classpath.txt), and the only bytecode this script produces are the ASM passes in
// ../java/.../tools, each working around one thing CheerpJ's JVM does differently (see ../README.md).
// Patched classes are merged back into the jar they came from, so the browser never downloads a class
// twice.
//
// Everything it writes -- .cache/ and public/_hadoop/ -- is gitignored; run `npm run assets`.

import { spawnSync } from "node:child_process";
import { createWriteStream } from "node:fs";
import { access, cp, mkdir, mkdtemp, open, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, relative } from "node:path";
import { argv } from "node:process";
import { pipeline } from "node:stream/promises";

const HADOOP_VERSION = "3.3.6";

/** Downloaded when the machine has no JDK of its own, the way Vercel's build image does not. */
const JDK = "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse";

const CENTRAL = "https://repo1.maven.org/maven2";

/**
 * The bytecode passes to run over Hadoop's jars, each `[class in ../java/.../tools, ...arguments]`.
 *
 * Populated from what actually broke in the browser rather than from what broke for spark/: Hadoop's
 * bytecode is plain Java 8 and it turned out to need less of this than Spark's Scala does. See
 * ../README.md.
 */
const PASSES = [["HostCallPatcher"]];

/** On the build's classpath (the ASM passes are written against it) but not the browser's. */
const BUILD_ONLY = ["xbean-asm9-shaded-4.23.jar"];

/**
 * The text the page puts into HDFS for WordCount to read, taken out of the jars themselves.
 *
 * Hadoop's own files, so the words the job counts are Hadoop's words: the two default configurations
 * (every property the NameNode and the job runner have, with their documentation) and the release's
 * licence and notice. Four files means four input splits, which is what makes the job's split list
 * worth looking at.
 */
const TEXT = [
  { name: "core-default.xml", jar: "hadoop-client-api-3.3.6.jar", entry: "core-default.xml" },
  { name: "mapred-default.xml", jar: "hadoop-client-api-3.3.6.jar", entry: "mapred-default.xml" },
  { name: "NOTICE.txt", jar: "hadoop-client-api-3.3.6.jar", entry: "META-INF/NOTICE.txt" },
  { name: "LICENSE.txt", jar: "hadoop-client-api-3.3.6.jar", entry: "META-INF/LICENSE.txt", lines: 202 },
];

const root = join(import.meta.dirname, "..");
const cache = join(root, ".cache");
const jars = join(cache, "jars");
const work = join(cache, "work");
const target = join(root, "public", "_hadoop");

/** The jars to fetch, as Maven Central paths. */
const KEEP = await readFile(join(root, "scripts", "classpath.txt"), "utf8").then((text) =>
  text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#")),
);

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

async function download(url, path) {
  console.log(`downloading ${url}`);
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok) throw new Error(`${url}: ${response.status}`);
  await mkdir(join(path, ".."), { recursive: true });
  await pipeline(response.body, createWriteStream(`${path}.part`));
  run("mv", [`${path}.part`, path]);
}

/** `javac`, `java` and `jar`: the ones on PATH, or a JDK downloaded once into .cache. */
async function jdk() {
  const onPath = spawnSync("javac", ["-version"], { encoding: "utf8" });
  if (!onPath.error && onPath.status === 0) return { javac: "javac", java: "java", jar: "jar" };

  const home = join(cache, "jdk");
  if (!(await exists(home))) {
    console.log("no javac on PATH");
    const archive = join(cache, "jdk.tgz");
    await download(JDK, archive);
    await mkdir(home, { recursive: true });
    run("tar", ["xzf", archive, "-C", home, "--strip-components=1"]);
  }
  const bin = join(home, "bin");
  return { javac: join(bin, "javac"), java: join(bin, "java"), jar: join(bin, "jar") };
}

/** Downloads the jars into .cache (kept between builds) and stages them in one flat directory. */
async function collect() {
  await rm(jars, { recursive: true, force: true });
  await mkdir(jars, { recursive: true });
  const names = [];
  for (const path of KEEP) {
    const jar = basename(path);
    const cached = join(cache, "maven", jar);
    if (!(await exists(cached))) await download(`${CENTRAL}/${path}`, cached);
    await cp(cached, join(jars, jar));
    if (!BUILD_ONLY.includes(jar)) names.push(jar);
  }
  console.log(`staged ${names.length} runtime jars`);
  return names;
}

/** Compiles the browser-side Java (the console, the virtual network, the ASM passes). */
async function compile() {
  const classes = join(work, "classes");
  await rm(classes, { recursive: true, force: true });
  await mkdir(classes, { recursive: true });
  const sources = await walk(join(root, "java"), "", ".java");
  console.log(`compiling ${sources.length} java sources`);
  const paths = sources.map((source) => join(root, "java", source));
  // --release 8, not -source/-target 8: it compiles against JDK 8's own API signatures, so a method
  // that only exists on the build machine's JDK 17 is a compile error here instead of a
  // NoSuchMethodError inside CheerpJ's OpenJDK 8.
  run(tools.javac, ["--release", "8", "-nowarn", "-cp", `${jars}/*`, "-d", classes, ...paths]);
  return classes;
}

/** Runs one of the ASM passes; they all write a jar of the classes they changed. */
function pass(classes, name, output, ...args) {
  // Every pass takes the same first two arguments: the jar to write, and the directory of jars to
  // scan (further arguments name individual jars, when a pass only needs some of them).
  run(tools.java, [
    "-cp",
    `${classes}:${jars}/*`,
    `org.apacheprojects.hadoopwasm.tools.${name}`,
    output,
    jars,
    ...args,
  ]);
  return output;
}

/** Unpacks a jar into `dir` with the JDK's own `jar`, overwriting what is already there. */
function unpack(jar, dir) {
  run(tools.jar, ["xf", jar], { cwd: dir });
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

/** Every file with `suffix` under `dir`, as jar entry names. */
async function walk(dir, prefix = "", suffix = ".class") {
  const found = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) found.push(...(await walk(join(dir, entry.name), path, suffix)));
    else if (entry.name.endsWith(suffix)) found.push(path);
  }
  return found;
}

/**
 * Runs the bytecode passes and returns the directory of classes the browser loads instead of the
 * upstream ones.
 *
 * The patched classes are not merged back into the jars they came from: they are put in
 * hadoopwasm.jar, which comes first on the classpath, so the class loader finds them first. That
 * costs nothing (CheerpJ reads jars with range requests, so a shadowed class is never fetched) and
 * saves rezipping 80 MB of shaded jars on every build.
 */
async function patch(classes, names) {
  const overlay = join(work, "overlay");
  const staging = join(work, "staging");
  await rm(overlay, { recursive: true, force: true });
  await rm(staging, { recursive: true, force: true });
  await mkdir(overlay, { recursive: true });
  await mkdir(staging, { recursive: true });

  // The jars are named in classpath order, so where two of them hold the same class the pass patches
  // the copy the browser's class loader would have found.
  for (const [name, ...args] of PASSES) {
    const jar = pass(classes, name, join(staging, `${name}.jar`), ...names, ...args);
    unpack(jar, overlay);
  }

  // Our own classes go in last: org/apache/hadoop/fs/DF replaces Hadoop's outright (see its source),
  // so it must not be overwritten by the patched copy of the class it replaces. The ASM passes under
  // tools/ only ever run at build time, so they stay out of what the browser downloads.
  await cp(join(classes, "org", "apacheprojects"), join(overlay, "org", "apacheprojects"), { recursive: true });
  await rm(join(overlay, "org", "apacheprojects", "hadoopwasm", "tools"), { recursive: true, force: true });
  await cp(join(classes, "org", "apache"), join(overlay, "org", "apache"), { recursive: true, force: true });
  // log4j.properties: Hadoop's jars ship without one, so its own logs would go nowhere.
  await cp(join(root, "resources"), overlay, { recursive: true });
  return overlay;
}

/** Writes the runtime classpath: the patched classes first, then Hadoop's jars untouched. */
async function assemble(names, overlay) {
  await rm(target, { recursive: true, force: true });
  await mkdir(join(target, "jars"), { recursive: true });
  for (const jar of names) await cp(join(jars, jar), join(target, "jars", jar));
  run(tools.jar, ["cf", join(target, "jars", "hadoopwasm.jar"), "-C", overlay, "."]);
  return ["hadoopwasm.jar", ...names];
}

/**
 * The text the page puts into HDFS for WordCount to read.
 *
 * Hadoop's own top-level documents, taken out of the same tarball as the jars, so the words the job
 * counts are the release's words rather than lorem ipsum.  Three files means three input splits, which
 * is what makes the job's split list worth looking at.
 */
async function corpus() {
  const scratch = await mkdtemp(join(tmpdir(), "hadooptext-"));
  await mkdir(join(target, "data"), { recursive: true });
  const files = [];
  for (const { name, jar, entry, lines } of TEXT) {
    run(tools.jar, ["xf", join(jars, jar), entry], { cwd: scratch });
    let text = await readFile(join(scratch, entry), "utf8");
    // LICENSE.txt is 400 KB of bundled third-party licences; the Apache License itself is the first
    // couple of hundred lines, and a wasm JVM counting words in the rest is not a better demo.
    if (lines) text = `${text.split("\n").slice(0, lines).join("\n")}\n`;
    await writeFile(join(target, "data", name), text);
    files.push({ name, bytes: Buffer.byteLength(text), lines: text.split("\n").length - 1 });
  }
  await rm(scratch, { recursive: true, force: true });
  return files;
}

async function main() {
  if (!argv.includes("--force") && (await exists(join(target, "manifest.json")))) {
    console.log("public/_hadoop is already populated; pass --force to rebuild");
    return;
  }
  tools = await jdk();
  const names = await collect();
  const classes = await compile();
  const overlay = await patch(classes, names);
  const classpath = await assemble(names, overlay);
  const data = await corpus();

  let bytes = 0;
  for (const jar of classpath) bytes += (await stat(join(target, "jars", jar))).size;
  const manifest = {
    hadoopVersion: HADOOP_VERSION,
    source: CENTRAL,
    classpath,
    jarBytes: bytes,
    data,
  };
  await writeFile(join(target, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(
    `wrote ${relative(root, target)}: ${classpath.length} jars, ${(bytes / 1e6).toFixed(0)} MB`,
  );
}

await main();
