# cassandra-wasm

Apache Cassandra 4.1.12 — the real thing, the jars from the official binary distribution — running as
a **multi-node cluster inside one browser tab**. Each node is a Cassandra JVM in its own Web Worker,
running under [CheerpJ](https://cheerpj.com/docs/) 4.3, with its own commitlog, SSTables and system
keyspaces in that worker's virtual filesystem. The nodes gossip, agree on a token ring, replicate
writes and satisfy reads at the consistency level you ask for. Kill a node and Cassandra tells you
what it can no longer do.

Cassandra is **not reimplemented and not simulated**. `scripts/build-jars.mjs` downloads
`apache-cassandra-4.1.12-bin.tar.gz` from archive.apache.org and puts its jars on the classpath; the
only Java in this directory is a ~350-line entry point (`java/Node.java`) and 16 files under
`java/patch/` that stand in for browser-hostile pieces of the runtime. Gossip, the failure
detector, `StorageProxy`'s write and read paths, replica selection, digest reads, hints, the
partitioner, the memtable, the commitlog, compaction and every consistency-level decision are
Cassandra's own code, unmodified.

This directory builds the `/cassandra/` route of the site the repository root assembles. Everything it
emits at the origin root is namespaced under `/_cassandra/`.

```text
page (cassandra/index.html, src/main.ts)          the "network": routes frames, runs CQL
   |         nodetool view, CQL console, pause/kill
   |  postMessage: {frame} internode bytes, {cql} statements, {node} status/results
   v
worker 1 (src/node.worker.ts)   worker 2   worker 3      one CheerpJ JVM each
   |  three native methods: send / poll / reply
   v
java/Node.java
   DatabaseDescriptor.daemonInitialization, CommitLog.start, Schema.loadFromDisk,
   StorageService.initServer                         a real Cassandra startup
   MessagingService.outboundSink  -> send(to, serialized Message)
   inbound pump: poll() -> Message.serializer.deserialize -> inboundSink
   QueryProcessor.process(statement, QueryOptions.create(CL, ...))
   |
   v
Apache Cassandra 4.1.12 (archive.apache.org) + java/patch/*
```

## Run it yourself

Requirements: Node `^20.19 || >=22.12`. The build downloads the Cassandra distribution (~52 MB) and,
unless a JDK 8 `javac` is already on `PATH` (or `JAVA_HOME_8` is set), a Temurin JDK 8 (~100 MB) into
`.cache/`, which is gitignored.

```bash
npm install
npm run dev       # http://localhost:5173/cassandra/
```

or, from the repository root, the assembled production site:

```bash
npm run build && npm run preview   # http://localhost:4173/cassandra/
```

Then: pick a node count, **Start cluster**, and wait — this is the slow part, see below. Node 1 is the
seed and starts alone; the others join once it is answering gossip (a joining node gives up after a
20 s shadow round, and a JVM in a worker takes minutes to get that far, so starting them all at once
fails with `Unable to gossip with any peers`). When the nodes report `NORMAL`, **Run RF=3 demo**
creates a keyspace with `replication_factor: 3`, waits until every node has actually applied the new
table, writes a row at `QUORUM` on one node and reads it back from the others at `ALL` and `ONE`.
Then kill a node and re-run the read: `ALL` raises `UnavailableException`, `QUORUM` still answers. The
CQL console runs any statement on any node at any consistency level.

Use a fresh Chrome profile if your default one has an old service worker for this origin; three JVMs
want ~4 GB of tab memory.

## Size and boot time

Measured on this machine (Chrome, x86-64 Linux, warm HTTP cache), three nodes:

| | |
| --- | --- |
| served payload | 33 MB of Cassandra jars (`/_cassandra/`), 61 jars, plus ~11 kB of page and worker JS |
| CheerpJ runtime | fetched from `cjrtnc.leaningtech.com` (its licence does not allow rehosting), Java 8 class library loaded lazily |
| one node, cold | ~137-160 s from worker start to `NORMAL` |
| three nodes, cold | seed `NORMAL` at ~160 s, then nodes 2 and 3 join in parallel and are `NORMAL` at ~185 s each: ~6 minutes of wall clock in total |
| CQL statement | tens to a few hundred ms once up; a cross-node `QUORUM` read is a few hundred |

The 33 MB is the distribution's `lib/` minus five jars a browser node cannot use (`zstd-jni` and
`snappy-java` are JNI, `netty-tcnative` is native TLS, `cassandra-driver-core` is only for `cqlsh`
and `fqltool`, `ecj` only compiles Java UDFs), which takes it down from 52 MB. The jars are fetched
by the worker, not the page, so the page paints immediately and shows boot progress reported by
Cassandra itself.

Boot time is Cassandra's, not CheerpJ's overhead alone: it is a real startup — commitlog replay,
system keyspace initialization, schema load, gossip settle (`cassandra.ring_delay_ms` is lowered to
10 s) — interpreted rather than JIT-compiled to native.

## How the hard parts work

**Internode messaging.** Cassandra's `MessagingService` has exactly the seam this needs:
`outboundSink` and `inboundSink`, which the codebase uses itself for in-JVM testing. An
`outboundSink` filter serializes the outgoing `Message` with `Message.serializer` at the version
Cassandra negotiated for that peer and hands the bytes to a native `send(to, frame)`; the page routes
the frame to the worker owning that address and the receiving node deserializes it and drops it into
`inboundSink` on the verb's own stage. So the bytes on the "wire" are Cassandra's real internode
protocol, including digest reads and hints; only TCP is replaced. Returning `false` from the filter
stops Cassandra from also trying to write the message to a socket.

One thing the missing TCP layer takes with it is the connection handshake, which is where Cassandra
normally learns a peer's messaging version — and `MigrationCoordinator` refuses to push or pull schema
to a peer whose version it does not know, so `CREATE TABLE` propagated to nobody and every read
elsewhere failed with `Unknown CF`. Each frame carries the version it was serialized at, so the
receiving node records it (`MessagingService.versions.set`) as the handshake would: that is the whole
fix, and schema push/pull then works untouched.

Sockets do not exist at all here: `MessagingService.listen()` is patched to return immediately under
`-Dcassandra.wasm.no_sockets=true` (CheerpJ has no `ServerSocketChannelImpl` natives, so binding
7000 fails with `UnsatisfiedLinkError`). Nodes are still addressed as `127.0.0.1:7000` …
`127.0.0.N:7000`, which is how the page knows where a frame goes.

**Getting messages *into* a running JVM.** CheerpJ lets Java call JavaScript, and lets JavaScript
call Java — but not while Java is on the stack, which under Cassandra is always. So inbound traffic is
pulled, not pushed: a dedicated Java thread blocks in `poll(timeoutMs)`, whose native implementation
is an `async` function that resolves when the worker receives a frame. CheerpJ suspends only that
thread; Cassandra's other thread pools keep running. Page-originated CQL arrives through the same
channel with a one-byte frame type.

**CQL from the page.** The native protocol (Netty on 9042) is dead in the water for the same reason
gossip's transport is, so the page's statements go through the same `postMessage` channel and are
executed with `QueryProcessor.process(statement, QueryState, QueryOptions.create(CL, …))` — the same
call the native transport makes after decoding a frame, with the consistency level from the console's
dropdown. The coordinator work (replica selection, `StorageProxy`, digest reads, read repair, hints)
is therefore entirely real; what is missing is only the wire protocol between a client and a
coordinator.

**Two bugs that cost the most time.** (1) CheerpJ 4.0–4.3 does not sign-extend single-byte reads from
direct or mapped `ByteBuffer`s: `get()` returns `131` where it must return `-125`. That silently
corrupts every vint Cassandra reads, so SSTable and commitlog deserialization fails in inventive
places far from the cause. `java/patch/.../BrowserBuffers.java` forces every buffer Cassandra
allocates on-heap (`-Dcassandra.wasm.on_heap_buffers=true`), where CheerpJ is correct.
(2) Patches must be compiled by a **JDK 8** `javac`, not `javac -source 8 -target 8` on a newer JDK:
the latter emits Java 9+ signatures (`ByteBuffer`'s covariant overrides, for one) that fail at runtime
as bare `NoSuchMethodError`s. Hence the JDK 8 download in the build script.

**Disk.** Each node gets `/files/node<N>/` in CheerpJ's virtual filesystem (data, commitlog, hints,
saved_caches) and Cassandra is configured with `disk_access_mode: standard`, `heap_buffers` memtables
and `trickle_fsync: false`. `PathUtils`/`BrowserFileChannel` patches fill in the file operations
CheerpJ's `FileChannel` lacks, `SyncUtil` skips `fsync` (`-Dcassandra.skip_sync=true`), and
`HintsCatalog` skips its directory fsync. Data is per-tab and not persisted across reloads: OPFS
persistence via CheerpJ's `IDBFS`/OPFS mounts would work but is not wired up here, and a cluster whose
state survives a reload would also have to survive schema disagreement on restart.

## Limitations

Honest list of what this is not:

- **Slow to start.** ~6 minutes for three nodes, cold, because the seed has to be up before the others
  can join. There is no way around interpreting a Cassandra startup three times over; the page shows
  Cassandra's own progress so it is at least legible.
- **Three JVMs need real hardware.** On a 2-core, 7 GB VM a third worker sometimes dies inside
  CheerpJ (`Cannot read properties of undefined (reading 'cacheRefCount')`) and the page reports it as
  a failed node, which is why the node count is a dropdown: 2 nodes still show replication, and 1 node
  still runs CQL.
- **No client protocol.** Nothing speaks CQL over a socket, so real drivers and `cqlsh` cannot
  connect. The console executes statements in the coordinator directly.
- **No persistence across reloads.** Each start wipes the node directories.
- **Statements only, no bound parameters**, no paging beyond the first 500 rows, and results are
  rendered with each column's `AbstractType.getString`.
- **Single-datacenter, `SimpleSnitch`, `SimpleStrategy`,** 4 tokens per node, `auto_bootstrap: false`.
  Adding a node to a running cluster is not offered: joining requires streaming, which works but takes
  longer than anyone will wait.
- **Compression is LZ4 only** (`zstd`/`snappy` are JNI), and **no UDFs** (`ecj` is not shipped).
- **Off-heap memory, mmap and JNA are unavailable**, so everything runs on-heap; a node is configured
  small (32 MiB memtable space, `concurrent_reads: 2`) and is not a performance sample of anything.
- **`Pause` is not a Cassandra state.** It stops the worker's transport in both directions, which is
  what a partition looks like to the other nodes; `Kill` terminates the worker outright.
- Repair, incremental backup, materialized views, SAI and counters are not exercised and mostly
  untested here.
