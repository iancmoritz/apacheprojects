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

// Builds crates/workbench for wasm32-unknown-unknown and runs wasm-bindgen (and wasm-opt, when it
// can be had) over the result: the JavaScript glue lands in src/generated/ for Vite to bundle, the
// module itself in public/_datafusion/ so it is fetched lazily at its own stable URL rather than
// inlined into a chunk.
//
// The two tools that are not part of a Rust installation are downloaded as release binaries into
// .toolchain/, keyed by version, because `cargo install`ing either of them on a cold machine (which
// is what a Vercel build is) costs more time than the whole DataFusion build.

import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { chmod, cp, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { arch, platform, tmpdir } from "node:os";
import { join } from "node:path";

const HERE = join(import.meta.dirname, "..");
const CRATE = "datafusion-workbench";
const TARGET = "wasm32-unknown-unknown";
const TOOLCHAIN = join(HERE, ".toolchain");
const GLUE_DIR = join(HERE, "src", "generated");
const WASM_DIR = join(HERE, "public", "_datafusion");
const BINARYEN_VERSION = "version_125";

/** The wasm-bindgen CLI has to be the exact version of the crate the module was built against. */
async function wasmBindgenVersion() {
  const lock = await readFile(join(HERE, "Cargo.lock"), "utf8");
  const match = lock.match(/\[\[package\]\]\nname = "wasm-bindgen"\nversion = "([^"]+)"/);
  if (!match) throw new Error("Cargo.lock has no wasm-bindgen entry -- run cargo build first");
  return match[1];
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: "inherit", cwd: HERE, ...options });
  if (result.error?.code === "ENOENT") return { missing: true };
  if (result.status !== 0) throw new Error(`${command} ${args.join(" ")} exited ${result.status}`);
  return { missing: false };
}

function version(command, args = ["--version"]) {
  const result = spawnSync(command, args, { encoding: "utf8", cwd: HERE });
  if (result.status !== 0) return null;
  return `${result.stdout}${result.stderr}`.trim();
}

function requireToolchain() {
  const cargo = version("cargo");
  if (!cargo) {
    throw new Error(
      [
        "cargo was not found on PATH, and this project is Apache DataFusion compiled from source:",
        "there is no prebuilt wasm to fall back to.",
        "",
        "Install a Rust toolchain (https://rustup.rs):",
        "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y",
        `  rustup target add ${TARGET}`,
        "",
        "On Vercel, rustup is already on the build image, so `rustup default stable` in the install",
        "command is enough.",
      ].join("\n"),
    );
  }
  const rustc = version("rustc");
  console.log(`using ${cargo}, ${rustc}`);

  // The target is a component, not something cargo can install on demand.
  const targets = version("rustup", ["target", "list", "--installed"]);
  if (targets === null) {
    console.log(`rustup not found; assuming ${TARGET} is available to this toolchain`);
  } else if (!targets.split(/\s+/).includes(TARGET)) {
    console.log(`installing the ${TARGET} target`);
    run("rustup", ["target", "add", TARGET]);
  }
}

/** Download `url`, unpack the single file `member` out of the tarball, and make it executable. */
async function fetchBinary(url, member, destination) {
  if (existsSync(destination)) return destination;
  console.log(`downloading ${url}`);
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: ${response.status} ${response.statusText}`);
  const archive = join(tmpdir(), `${createHash("sha256").update(url).digest("hex")}.tar.gz`);
  await writeFile(archive, Buffer.from(await response.arrayBuffer()));
  const unpacked = join(tmpdir(), createHash("sha256").update(destination).digest("hex"));
  await rm(unpacked, { recursive: true, force: true });
  await mkdir(unpacked, { recursive: true });
  run("tar", ["xzf", archive, "-C", unpacked, "--strip-components", "1"], { cwd: unpacked });
  await mkdir(join(destination, ".."), { recursive: true });
  await cp(join(unpacked, member), destination);
  await chmod(destination, 0o755);
  await rm(archive, { force: true });
  await rm(unpacked, { recursive: true, force: true });
  return destination;
}

/** The host triple release binaries are published for, or null on a platform without them. */
function hostTriple() {
  const triples = {
    "linux-x64": "x86_64-unknown-linux-musl",
    "linux-arm64": "aarch64-unknown-linux-gnu",
    "darwin-x64": "x86_64-apple-darwin",
    "darwin-arm64": "aarch64-apple-darwin",
  };
  return triples[`${platform()}-${arch()}`] ?? null;
}

/** wasm-bindgen from PATH if it is the right version, otherwise the matching release binary. */
async function wasmBindgen(wanted) {
  const found = version("wasm-bindgen");
  if (found?.includes(wanted)) {
    console.log(`using ${found} from PATH`);
    return "wasm-bindgen";
  }
  const triple = hostTriple();
  if (!triple) {
    throw new Error(
      `no wasm-bindgen ${wanted} on PATH and no release binary for ${platform()}-${arch()}: ` +
        `install it with \`cargo install wasm-bindgen-cli --version ${wanted}\``,
    );
  }
  const name = `wasm-bindgen-${wanted}-${triple}`;
  return fetchBinary(
    `https://github.com/wasm-bindgen/wasm-bindgen/releases/download/${wanted}/${name}.tar.gz`,
    "wasm-bindgen",
    join(TOOLCHAIN, `wasm-bindgen-${wanted}`),
  );
}

/** wasm-opt, or null: it only shrinks the module further, so a platform without it still builds. */
async function wasmOpt() {
  if (version("wasm-opt")) return "wasm-opt";
  const binaryen = {
    "linux-x64": "x86_64-linux",
    "linux-arm64": "aarch64-linux",
    "darwin-x64": "x86_64-macos",
    "darwin-arm64": "arm64-macos",
  }[`${platform()}-${arch()}`];
  if (!binaryen) return null;
  const url = `https://github.com/WebAssembly/binaryen/releases/download/${BINARYEN_VERSION}/binaryen-${BINARYEN_VERSION}-${binaryen}.tar.gz`;
  return fetchBinary(url, join("bin", "wasm-opt"), join(TOOLCHAIN, `wasm-opt-${BINARYEN_VERSION}`))
    .catch((error) => {
      console.log(`wasm-opt unavailable (${error.message}); shipping the wasm-bindgen output`);
      return null;
    });
}

/**
 * Point the generated glue at the module's real URL.
 *
 * wasm-bindgen defaults to `new URL("workbench_bg.wasm", import.meta.url)`, a relative asset
 * reference next to a file that is about to move: Vite would bundle a second copy of the module
 * into the chunk graph, which is the one thing this build wants to avoid.  The replacement is
 * assembled at runtime so that no bundler resolves it either.
 */
async function pointGlueAtPublicUrl(glue, url) {
  const source = await readFile(glue, "utf8");
  const original = /new URL\('workbench_bg\.wasm', import\.meta\.url\)/;
  if (!original.test(source)) {
    throw new Error(`${glue}: no default module URL to rewrite -- did wasm-bindgen change?`);
  }
  const parts = url.split("/").map((part) => JSON.stringify(part));
  await writeFile(glue, source.replace(original, `new URL([${parts}].join('/'), location.href)`));
}

function megabytes(bytes) {
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

async function main() {
  requireToolchain();

  run("cargo", ["build", "--release", "--target", TARGET, "-p", CRATE]);
  const built = join(HERE, "target", TARGET, "release", `${CRATE.replaceAll("-", "_")}.wasm`);
  console.log(`cargo: ${megabytes((await stat(built)).size)} of wasm`);

  const bindgen = await wasmBindgen(await wasmBindgenVersion());
  await rm(GLUE_DIR, { recursive: true, force: true });
  await mkdir(GLUE_DIR, { recursive: true });
  run(bindgen, ["--target", "web", "--out-dir", GLUE_DIR, "--out-name", "workbench", built]);

  const module = join(GLUE_DIR, "workbench_bg.wasm");
  console.log(`wasm-bindgen: ${megabytes((await stat(module)).size)}`);

  const optimizer = await wasmOpt();
  if (optimizer) {
    // -Oz on top of the crate's own opt-level="z", and drop everything the browser never reads.
    run(optimizer, [
      "-Oz",
      "--strip-debug",
      "--strip-dwarf",
      "--strip-producers",
      "--enable-bulk-memory",
      "--enable-nontrapping-float-to-int",
      module,
      "-o",
      module,
    ]);
    console.log(`wasm-opt: ${megabytes((await stat(module)).size)}`);
  }

  // The module is served as a plain file so the page can stream it with a progress bar; only the
  // glue is bundled.  Nothing else in public/_datafusion is ours, so clear just the wasm.
  await mkdir(WASM_DIR, { recursive: true });
  for (const name of await readdir(WASM_DIR).catch(() => [])) {
    if (name.endsWith(".wasm")) await rm(join(WASM_DIR, name));
  }
  await cp(module, join(WASM_DIR, "workbench.wasm"));
  await rm(module);
  await pointGlueAtPublicUrl(join(GLUE_DIR, "workbench.js"), "/_datafusion/workbench.wasm");
  console.log(`public/_datafusion/workbench.wasm: ${megabytes((await stat(join(WASM_DIR, "workbench.wasm"))).size)}`);
}

await main();
