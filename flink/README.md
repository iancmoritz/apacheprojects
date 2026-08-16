# flink-wasm

Apache Flink 1.20.5 running entirely in a browser tab. A real `MiniCluster` — a JobManager with its
dispatcher, scheduler and checkpoint coordinator, and a TaskManager with its own slots, memory manager
and mailbox threads — starts in the visitor's tab on OpenJDK 8 compiled to WebAssembly
([CheerpJ](https://cheerpj.com/)), and runs a DataStream job the visitor feeds by typing words or
turning up a generator: an event-time tumbling window word count with watermarks, keyed state, allowed
lateness, a side output for late records and checkpoints to Flink's heap state backend.

**Flink's own web dashboard runs here too**, served by the JobManager in the tab: the page's service
worker relays every request the dashboard makes into the JVM, where Flink's REST endpoint answers over
an in-process loopback socket. What the dashboard shows — the job graph, subtask states, watermarks,
checkpoint history — is Flink's REST API, not a reimplementation of it.

Flink itself is **not vendored and not reimplemented**: `scripts/build-runtime.mjs` downloads the
official `flink-1.20.5-bin-scala_2.12` distribution from `archive.apache.org` and keeps the 6 jars the
runtime actually needs. This directory holds only the glue (`java/`, ~3 300 lines): the console class
the page calls, the job, a virtual network, an RPC-system loader and one shadowed Flink class.
Everything the browser loads comes from this origin, except the CheerpJ runtime itself, which is loaded
from `cjrtnc.leaningtech.com` because its licence does not allow redistribution — it is a wasm JVM, not
a Flink service, and no event, plan or byte of state leaves the tab.

This directory builds the `/flink/` route of the site the repository root assembles. The page is
`flink/index.html`, the jars are under `/_flink/` and the service worker is `/flink/dashboard-sw.js`
scoped to `/flink/dashboard/`, so nothing it emits can collide with the other projects.

```text
page (flink/index.html, src/main.ts)
  src/driver.ts   cheerpjInit + cheerpjRunLibrary(classpath), one call into the JVM at a time
        |
        v
  CheerpJ (OpenJDK 8 in wasm)    jars from /app/_flink/jars/*.jar over HTTP range requests
        |
        v
  org.apacheprojects.flinkwasm.console.FlinkConsole   (java/)
    startCluster() -> new MiniCluster(...)            -> real Flink 1.20.5, 1 TaskManager, 2 slots
    submitJob()    -> WordCountJob.submit(env, ...)   -> StreamGraph, JobGraph, ExecutionGraph
    push(), setGenerator()   words the visitor types, into the running source
    poll()         -> windows, watermarks, vertex and subtask states, checkpoint stats, JSON plan
    restRequest()  -> Flink's REST endpoint, over the virtual loopback
        |
        +-- job/   the DataStream job and the queue the page feeds it through
        +-- net/   virtual SelectorProvider, SocketImpl, Socket, ServerSocket, channels
        +-- rpc/   an RpcSystemLoader that does not extract a jar out of a jar
        +-- rest/  an HTTP client for the JobManager, for the dashboard's requests
        +-- org/apache/flink/core/memory/MemorySegmentFactory  (shadows the distribution's)
        ^
        |
  public/flink/dashboard-sw.js   answers /flink/dashboard/* by asking the page to call restRequest()
```

## What the job is

`java/.../job/WordCountJob.java` builds this and nothing else does the work:

```java
env.addSource(new LiveSource(), ...)                       // what the page pushes, plus the generator
   .assignTimestampsAndWatermarks(new WallClockWatermarks(outOfOrderness))
   .keyBy(word)
   .process(new SeenSoFar())                               // keyed ValueState, per word
   .keyBy(word)
   .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
   .allowedLateness(Time.seconds(lateness))
   .sideOutputLateData(LATE)                               // the "late records" counter in the page
   .aggregate(new CountWords(), new WindowRow())
   .addSink(new PageSink());                               // a queue the page drains once a second
```

The generator emits a share of its events with timestamps behind the watermark (the *out of order*
slider), so late records are real late records: they arrive after the window they belong to has fired,
are counted again while they are within the allowed lateness, and are routed to the side output after
that. Checkpointing is on at the configured interval, and **Checkpoint now** asks the coordinator for
one; the checkpoints are files under `/files/flink/checkpoints`, and the page shows the size and path
of each.

## Run it yourself

Requirements: Node `^20.19 || >=22.12`, a Chromium (service workers need one, and a *fresh* profile if
yours has one registered from elsewhere), ~1 GB of disk and ~1.5 GB of free memory in the tab. The
build compiles Java, so it needs a JDK; if the machine has no `javac` on `PATH` — which is the case on
Vercel's build image — it downloads Temurin 17 into `.cache/jdk` and uses that. Beyond node and `tar`
it shells out to nothing.

```bash
git clone https://github.com/iancmoritz/apacheprojects.git
cd apacheprojects/flink
npm install
npm run dev     # first run downloads the Flink distribution (~500 MB) and builds the classpath
```

Then open <http://localhost:5173/flink/>. The header reports each boot step; when the metrics row
appears the cluster is up and the job is running. Type words into **Feed the stream** and hit **Send**,
or drag the **generator** slider up, and window rows appear as Flink closes each window. The tabs are
Flink's own data: **ExecutionGraph** is every vertex and subtask with its state, **StreamGraph JSON** is
what `env.getStreamGraph().getStreamingPlanAsJSON()` returns, **Checkpoints** is the checkpoint
coordinator's statistics, **Event log** is the JVM's stdout and stderr (log4j included), and **Web
dashboard** is Flink's dashboard, in an iframe, served by the JobManager in the tab.

Static bundle instead of the dev server:

```bash
npm run build     # asset generation + tsc --noEmit + vite build into dist/
```

Other scripts:

| command | what it does |
| --- | --- |
| `npm run assets` | downloads Flink (and a JDK, if needed), compiles `java/`, writes the classpath and manifest into `public/_flink/` (gitignored); `node scripts/build-runtime.mjs --force` rebuilds |
| `npm run typecheck` | `tsc --noEmit` |

## Measured

On this machine (Chrome 141, Linux, dev server over HTTP, fresh profile):

| | |
| --- | --- |
| boot to a running job | **15.6 s** (JVM 0.2 s, class loading 2.7 s, `MiniCluster.start()` 12.7 s, job submission 2.1 s) |
| first window rows after typing | one window interval (5 s by default) after the words are sent |
| generator, 30 events/s | 1 300 events, 142 window rows, 102 late records and 4 checkpoints in ~45 s |
| checkpoint | ~5 kB of state, under a second |
| downloaded to boot, run the job and open the dashboard | **126 MB** |
| classpath on disk | 6 jars, 81 MB (5 upstream + `flinkwasm.jar`) |

Nothing is eagerly downloaded: CheerpJ asks for the byte ranges of each class it needs out of the
81 MB of jars, so the 126 MB is what this workload touched (some ranges more than once), not a full
download. Warm reloads reuse the HTTP cache.

Flink is slower here than on a machine for the reason everything in a wasm JVM is: no JIT, one core,
and a single JVM doing the JobManager's and the TaskManager's work. It is not fast, it is real.

## How the hard parts work

Everything in this list was a hard failure before it was a workaround.

- **Sockets** (`net/`). A tab cannot open TCP, and `MiniCluster` opens several. Flink's NIO users
  (netty's shuffle service, the REST endpoint) failed in `SelectorProvider`, and the *blob server*
  failed even before that with `Unable to open BLOB Server in specified port range: 0`, because it
  binds a plain `java.net.ServerSocket`. So `net/` provides both halves: a `SelectorProvider`
  (installed through a `javaProperties` entry in `cheerpjInit`) for channels and selectors, and a
  `SocketImpl` installed into `Socket`/`ServerSocket` for the old API. Both hand out in-process
  loopback pipes keyed by port, so Flink's components connect to each other inside the JVM.
- **RPC** (`rpc/FlatRpcSystemLoader`). Flink ships its Pekko RPC system as a jar *inside* flink-dist
  and extracts it to a temp file at startup to load it in a child classloader. Under CheerpJ that is
  slow at best; instead the RPC jar is hoisted onto the classpath by the build and a
  `RpcSystemLoader` service (`conf/META-INF/services/...`) instantiates `PekkoRpcSystem` directly.
  The actor system is `RpcServiceSharing.SHARED` and local: `pekko://flink`, no remote transport.
- **Off-heap memory** (`java/org/apache/flink/core/memory/MemorySegmentFactory.java`). The cluster
  started and the job was scheduled, then every record that crossed the network stack died in
  `NullPointerException at MemorySegment.getInt` → `NonSpanningWrapper.readInt`: CheerpJ cannot do
  `Unsafe` reads against a direct `ByteBuffer`'s address. This class shadows the distribution's (the
  build merges it into flink-dist itself, so Flink loads ours) and routes every
  `allocateUnpooledOffHeapMemory`/`allocateOffHeapUnsafeMemory` call to a heap segment. Flink's
  serializers then take the on-heap path, which is plain array arithmetic. `taskmanager.memory.*` is
  sized for a tab, and off-heap task memory is 0.
- **Watermarks with no clock to speak of.** `WatermarkStrategy.forBoundedOutOfOrderness` only advances
  the watermark when elements arrive, so a visitor who types three words and stops would never see a
  window close. `WallClockWatermarks` emits `max(largest event timestamp, now) - outOfOrderness - 1`,
  which is the same bounded-out-of-orderness rule with the tab's clock as a floor.
- **One JVM, one call at a time.** CheerpJ runs the JVM on its own thread and refuses re-entry
  (`Java code still running, check for a missing 'await'`), and the page has a poll loop, four
  controls and a dashboard all calling in. `src/driver.ts` serializes every call through a promise
  chain, so the dashboard's requests queue behind the poll instead of corrupting it.
- **The dashboard** (`rest/Dashboard.java`, `public/flink/dashboard-sw.js`). Flink's REST endpoint
  serves both the API and the Angular application, and the application is an ordinary browser client
  that expects HTTP. It gets HTTP: the service worker intercepts `/flink/dashboard/*`, the page turns
  each request into `FlinkConsole.restRequest`, and `Dashboard` opens a `java.net.Socket` to
  `cluster.getRestAddress()` — a virtual one, to netty inside this JVM — writes the request and reads
  the response back, which the worker returns to the iframe with Flink's own status and content type.
  The dashboard's `<base href="./">` is why its relative asset and API paths land inside the scope.
- **The state backend and the filesystem.** RocksDB is a native library, so the backend is `hashmap`
  and checkpoint storage is `filesystem` under CheerpJ's writable `/files/`; the jars are read from
  `/app/`, its read-only HTTP-backed mount. The build drops the eleven RocksDB native builds, snappy
  and netty's epoll transports (110 MB uncompressed) from flink-dist, keeping the Java classes that
  would load them so nothing reflective changes shape.
- **Range requests.** CheerpJ reads jars by byte range; a host that ignores `Range` prints
  `HTTP server does not support the 'Range' header. CheerpJ cannot run.` Vite's dev server and Vercel
  both answer them, and `../vercel.json` marks `/_flink/*` immutable.

## Limitations

- **One core.** `Runtime.availableProcessors()` is 1 under CheerpJ, so the parallelism control tops out
  at 2 (the TaskManager's slot count) and everything is really interleaved on one thread. Flink's own
  threads — mailbox, timer service, checkpoint coordinator — all exist and all share it.
- **No RocksDB, no incremental checkpoints, no local recovery.** The heap backend keeps state in the
  tab's memory, so a long-running generator will eventually be limited by the JVM heap.
- **No SQL.** The table planner, CEP, the connectors and the Scala API are not on the classpath; this
  is the DataStream API only. `flink-table-planner-loader` alone is 40 MB and loads its own
  classloader hierarchy, which is a project of its own under CheerpJ.
- **The dashboard is read-only, and two of its views are not there.** Submitting and cancelling jobs
  are disabled (the page owns the job), the flame graph needs thread sampling CheerpJ does not offer,
  and Monaco — 11 MB of the dashboard, loaded only by its log and exception viewers — is dropped from
  the jar, so those two views fail to load. Everything else, including the job graph and the
  checkpoint and watermark views, is the real dashboard against the real REST API.
- **The dashboard is only alive while the page is.** The service worker relays through the page that
  owns the JVM, so `/flink/dashboard/` opened in a tab of its own answers `502` with that explanation.
- **Not persistent.** Reloading the tab starts a new cluster and a new job from scratch; the previous
  checkpoints are still under `/files/flink/checkpoints` in CheerpJ's storage, but nothing restores
  from them.
- **Noise in the event log is real.** `TaskExecutorResourceUtils` warning that
  `taskmanager.memory.flink.size` has no effect for local execution, and CheerpJ's
  `TODO: GetMemoryManagers`, are both printed by the real runtime, so they are shown rather than
  hidden.
