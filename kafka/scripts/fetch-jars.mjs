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

// Take the official Apache Kafka binary release apart and keep only the jars a single-node KRaft
// broker plus the Java clients actually load: 41 of the release's 120 jars, 32 MB instead of 123 MB.
// Nothing here is vendored into git; public/_kafka/jars/ is generated and gitignored.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { access, copyFile, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { argv } from "node:process";

const VERSION = "3.9.1";
const SCALA = "2.13";
const DIST = `https://archive.apache.org/dist/kafka/${VERSION}/kafka_${SCALA}-${VERSION}.tgz`;

// Everything the broker, the controller, the storage formatter and the clients touch on the way
// through create-topic / produce / consume / describe.  Verified by running the flow with exactly
// this classpath; anything missing shows up immediately as NoClassDefFoundError.
const KEEP = [
  `kafka_${SCALA}-${VERSION}.jar`,
  `kafka-clients-${VERSION}.jar`,
  `kafka-group-coordinator-${VERSION}.jar`,
  `kafka-group-coordinator-api-${VERSION}.jar`,
  `kafka-metadata-${VERSION}.jar`,
  `kafka-raft-${VERSION}.jar`,
  `kafka-server-${VERSION}.jar`,
  `kafka-server-common-${VERSION}.jar`,
  `kafka-storage-${VERSION}.jar`,
  `kafka-storage-api-${VERSION}.jar`,
  `kafka-tools-api-${VERSION}.jar`,
  `kafka-transaction-coordinator-${VERSION}.jar`,
  "scala-library-2.13.15.jar",
  "scala-collection-compat_2.13-2.10.0.jar",
  "scala-java8-compat_2.13-1.0.2.jar",
  "scala-logging_2.13-3.9.5.jar",
  "jackson-annotations-2.16.2.jar",
  "jackson-core-2.16.2.jar",
  "jackson-databind-2.16.2.jar",
  "jackson-datatype-jdk8-2.16.2.jar",
  "jackson-module-scala_2.13-2.16.2.jar",
  "metrics-core-2.2.0.jar",
  "metrics-core-4.1.12.1.jar",
  "slf4j-api-1.7.36.jar",
  "slf4j-reload4j-1.7.36.jar",
  "reload4j-1.2.25.jar",
  "argparse4j-0.7.0.jar",
  "jopt-simple-5.0.4.jar",
  "lz4-java-1.8.0.jar",
  "caffeine-2.9.3.jar",
  "pcollections-4.0.1.jar",
  "jose4j-0.9.4.jar",
  "commons-io-2.14.0.jar",
  "commons-validator-1.7.jar",
  "commons-beanutils-1.9.4.jar",
  "commons-digester-2.1.jar",
  "commons-collections-3.2.2.jar",
  "commons-logging-1.2.jar",
  // KafkaConfig reads ZooKeeper's client config classes even in KRaft mode.
  "zookeeper-3.8.4.jar",
  "zookeeper-jute-3.8.4.jar",
  "audience-annotations-0.12.0.jar",
];

const root = join(import.meta.dirname, "..");
const target = join(root, "public", "_kafka", "jars");
const manifestPath = join(root, "public", "_kafka", "jars.json");

if (!argv.includes("--force") && (await exists(manifestPath))) {
  console.log("public/_kafka/jars is already populated; pass --force to rebuild");
  process.exit(0);
}

async function exists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

console.log(`downloading ${DIST}`);
const response = await fetch(DIST);
if (!response.ok) throw new Error(`${DIST}: HTTP ${response.status}`);
const tarball = Buffer.from(await response.arrayBuffer());
console.log(`  ${(tarball.length / 1e6).toFixed(1)} MB, sha256 ${sha256(tarball).slice(0, 16)}…`);

const work = await mkdtemp(join(tmpdir(), "kafka-dist-"));
const tarPath = join(work, "kafka.tgz");
await writeFile(tarPath, tarball);
execFileSync("tar", ["-xzf", tarPath, "-C", work, `kafka_${SCALA}-${VERSION}/libs`]);
const libs = join(work, `kafka_${SCALA}-${VERSION}`, "libs");

const available = new Set(await readdir(libs));
const missing = KEEP.filter((jar) => !available.has(jar));
if (missing.length) throw new Error(`not in the Kafka ${VERSION} tarball: ${missing.join(", ")}`);

await rm(target, { recursive: true, force: true });
await mkdir(target, { recursive: true });
let bytes = 0;
for (const jar of KEEP) {
  await copyFile(join(libs, jar), join(target, jar));
  bytes += (await readFile(join(target, jar))).length;
}
await rm(work, { recursive: true, force: true });

await writeFile(
  manifestPath,
  `${JSON.stringify({ version: VERSION, scala: SCALA, jars: KEEP, bytes }, null, 2)}\n`,
);
console.log(`kept ${KEEP.length} jars, ${(bytes / 1e6).toFixed(1)} MB, in public/_kafka/jars`);

function sha256(buffer) {
  return createHash("sha256").update(buffer).digest("hex");
}
