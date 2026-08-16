# spark-wasm

Apache Spark 3.5.9 running entirely in a browser tab. A real Spark driver — `SparkSession`, Catalyst,
the DAG scheduler, `LocalSchedulerBackend`, the block manager and its shuffle — starts on
`local[*]` in the visitor's tab on OpenJDK 8 compiled to WebAssembly ([CheerpJ](https://cheerpj.com/)),
and answers the SQL the visitor types over a bundled CSV. Nothing Spark does runs on a server; the
server only hands over static files.

Spark itself is **not vendored and not reimplemented**: `scripts/build-runtime.mjs` downloads the
official `spark-3.5.9-bin-hadoop3` distribution from `archive.apache.org` and keeps the 89 jars the
driver actually needs. This repo holds only the glue (`java/`, ~4 100 lines): a console class Spark
runs, a virtual `SelectorProvider`, an `Unsafe` shim, and the ASM passes that rewrite bytecode
CheerpJ's JVM cannot execute (below). Everything the browser loads is served from this origin, except
the CheerpJ runtime itself, which is loaded from `cjrtnc.leaningtech.com` because its licence does not
allow redistribution — it is a wasm JVM, not a Spark service, and no query, plan or byte of data
leaves the tab.

This directory builds the `/spark/` route of the site the repository root assembles. The page is
`spark/index.html`, and everything it needs at the origin root is under `/_spark/`, so it cannot
collide with the other projects.

```text
page (spark/index.html, src/main.ts)
  src/driver.ts  cheerpjInit + cheerpjRunLibrary(classpath)
        |
        v
  CheerpJ (OpenJDK 8 in wasm)   jars from /app/_spark/jars/*.jar over HTTP range requests
        |
        v
  org.apacheprojects.sparkwasm.console.SparkConsole   (java/)
    start()  -> SparkSession.builder().master("local[*]")  -> real Spark 3.5.9
    query()  -> spark.sql(...)  ->  rows + EXPLAIN FORMATTED + SparkListener stage/task metrics
        |
        +-- net/     virtual SelectorProvider, Socket, ServerSocket, channels (no TCP in a tab)
        +-- lambda/  closure serialization that does not need LambdaMetafactory
        +-- unsafe/  sun.misc.Unsafe float/double access via int/long bit ops
```

## Run it yourself

Requirements: Node `^20.19 || >=22.12`, a Chromium/Firefox/Safari, ~1 GB of disk and ~2 GB of free
memory in the tab. The build patches Spark's own bytecode, so it needs a JDK; if the machine has no
`javac` on `PATH` — which is the case on Vercel's build image — it downloads Temurin 17 into
`.cache/jdk` and uses that. Beyond node, `tar` and that JDK it shells out to nothing.

```bash
git clone https://github.com/iancmoritz/apacheprojects.git
cd apacheprojects/spark
npm install
npm run dev     # first run downloads Spark (~400 MB) and patches its bytecode (~4 min)
```

Then open <http://localhost:5173/spark/>. The header reports each boot step; when it says
`Spark 3.5.9 on local[*]` the console below is live. Pick a sample query or write your own against
`spark_classes` and hit **Run** (or Ctrl/Cmd+Enter): the **Results** tab is what Spark collected, the
**Physical plan** tab is Spark's own `EXPLAIN FORMATTED`, and **Stages & tasks** is what a
`SparkListener` recorded for that query — stages, task counts, records read, shuffle bytes, peak
memory. The grey panel under the boot steps is the JVM's real stdout/stderr, log4j included.

Static bundle instead of the dev server:

```bash
npm run build     # asset generation + tsc --noEmit + vite build into dist/
```

Other scripts:

| command | what it does |
| --- | --- |
| `npm run assets` | downloads Spark (and a JDK, if needed) and writes the patched classpath, the dataset and the manifest into `public/_spark/` (gitignored); `node scripts/build-runtime.mjs --force` rebuilds |
| `npm run typecheck` | `tsc --noEmit` |

## Measured

On this machine (Chrome 133, Linux, assembled production build served over HTTP, fresh profile):

| | |
| --- | --- |
| boot to a live driver | **67 s** (JVM 1 s, class loading 3 s, `SparkSession` 63 s) |
| first query (13 468-row CSV scan, sort, limit) | **25 s**, 1 stage / 1 task |
| grouped query (shuffle, 2 partitions) | **25 s**, 2 stages / 2 tasks |
| downloaded to boot and run two queries | **163 MB** over 250 requests |
| classpath on disk | 90 jars, 139 MB (89 upstream + `sparkwasm.jar`) |
| dataset | `_spark/data/spark-classes.csv`, 13 468 rows, 1.2 MB |

Nothing is eagerly downloaded: CheerpJ asks for the byte ranges of each jar it needs, so the 163 MB
is what this workload actually touched out of the 139 MB of jars (some ranges are read more than
once), not a full download of the distribution. Warm reloads reuse the HTTP cache.

Spark is slow here for the reason everything in a wasm JVM is slow: no JIT, one core, and Catalyst
generates and loads Java code per query. It is not fast, it is real.

## How the hard parts work

Everything in this list was a hard failure before it was a workaround; all of it is done at build
time by `scripts/build-runtime.mjs` (ASM passes in `java/.../tools/`) or at boot by the classes in
`java/`.

- **`wide` local variables** (`tools/WideLocalsPatcher`). CheerpJ miscompiles methods that use the
  `wide` form of local-variable access (>255 slots), which Spark's generated and hand-written giants
  do: the JVM either threw `ClassCastException: null` with an empty stack trace or
  `ArrayIndexOutOfBoundsException`. The pass recolors local slots so those methods stay under the
  limit.
- **Lambdas** (`tools/IndyPatcher`, `lambda/LambdaAdapterFactory`). CheerpJ's runtime
  `LambdaMetafactory` returns an object whose methods throw, so every Scala closure — meaning every
  Spark task — failed with `NoClassDefFoundError: org/apache/spark/rdd/RDD$$Lambda$2110` or an
  `ArrayIndexOutOfBoundsException` inside the metafactory. The pass rewrites `invokedynamic` call
  sites ahead of time to named adapter classes (generated under
  `org/apacheprojects/sparkwasm/lambda/gen/`, with `anonfun` renamed to `fn` so Spark's
  `ClosureCleaner` does not try to reflect on them).
- **Closure serialization** (`tools/LambdaDeserializePatcher`, `tools/ClosureStreamPatcher`,
  `lambda/SerializedClosure`). The JVM cannot read its own `SerializedLambda` back under CheerpJ, so
  the closure wire format is replaced: `ClosureObjectOutputStream` writes the adapter's identity and
  captured arguments, and Scala's `LambdaDeserialize` bootstrap is patched to rebuild from that.
- **`Unsafe` floats** (`unsafe/UnsafeShim`, `tools/UnsafeShimPatcher`). `Unsafe.putDouble`/`getDouble`
  and friends are unimplemented natives (`UnsatisfiedLinkError: Java_sun_misc_Unsafe_putDouble`), and
  Spark's `UnsafeRow`, `HashAggregate` and Kryo all use them. Calls are rewritten to go through a
  shim that stores the same bits through `putLong`/`getLong`.
- **Sockets** (`net/`). Nothing in a tab can open TCP. `local[*]` needs no RPC, but Netty and the
  block manager still construct selectors on the way up, which failed with
  `UnsatisfiedLinkError: Java_sun_nio_ch_EPoll_eventSize`. A `SelectorProvider` (installed through a
  `javaProperties` entry in `cheerpjInit`) hands out in-process loopback sockets and channels instead.
  The Spark UI is off for the same reason — Jetty needs a real listening socket.
- **Identity and the filesystem.** Hadoop's `UserGroupInformation` throws
  `KerberosAuthException`/`FailedLoginException` with no OS user, so `SPARK_USER=browser` is put into
  the environment map and a remote user is set explicitly. The warehouse lives on CheerpJ's writable
  `/files/`; the jars and the dataset are read from `/app/`, its read-only HTTP-backed mount, with
  `fs.file.impl=RawLocalFileSystem` so Hadoop stops asking for a `.crc` beside every file.
- **Java 8, not 11 or 17.** Java 11 dies immediately with
  `UnsatisfiedLinkError: Java_jdk_internal_misc_Unsafe_fullFence`, so the classpath is the Scala 2.12
  build of Spark on CheerpJ's OpenJDK 8.
- **Range requests.** CheerpJ reads jars by byte range; a host that ignores `Range` prints
  `HTTP server does not support the 'Range' header. CheerpJ cannot run.` Vite's dev server and Vercel
  both answer them, and `../vercel.json` marks `/_spark/*` immutable.

## Limitations

- **One core, one task at a time.** `local[*]` is honest — `Runtime.availableProcessors()` is 1 under
  CheerpJ — so there is no parallelism, and a query holds the tab's JVM until it finishes (the page
  keeps responding; the driver is on CheerpJ's own thread).
- **No Spark UI.** `spark.ui.enabled=false`. Jetty needs a listening socket, which no tab has; the
  Stages & tasks tab is a `SparkListener` rendered by the page instead.
- **Driver only.** No cluster manager, no executors, no `spark-submit`, no external shuffle service,
  nothing that would need RPC between JVMs.
- **SQL and the DataFrame API over the bundled CSV.** Reads through `/app/` and writes under
  `/files/` work; there is no Hive metastore, no Parquet/ORC native codec beyond what the shipped jars
  do in pure Java, and no `spark.read` from another origin.
- **Not persistent.** Reloading the tab starts a new driver from scratch (another ~67 s).
- **Stage names are wrong.** Spark derives them from the closure's call site, and the lambda rewrite
  above replaces those call sites, so stages read `$anonfun$getCallSite$1 at <unknown>:0`. The
  metrics next to them are the real ones.
- **Noise in the JVM log is real.** `TODO: GetMemoryManagers` is CheerpJ reporting an unimplemented
  JVMTI call, and `java.net.SocketException: Operation not permitted (Socket creation failed)` is
  Netty enumerating network interfaces it will never use. Both are harmless and both are printed by
  the real runtime, so they are shown rather than hidden.
