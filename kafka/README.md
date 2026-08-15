# kafka-browser

Apache Kafka 3.9.1 running entirely in a browser tab: a single-node **KRaft** cluster
(`process.roles=broker,controller`) started by Kafka's own `KafkaRaftServer`, formatted by Kafka's own
`kafka-storage format`, and driven from the page through Kafka's own `AdminClient`, `KafkaProducer`
and `KafkaConsumer`. There is no server, no proxy and no API call at runtime: the page loads static
files and a JVM in WebAssembly, and everything Kafka does happens in the client.

Kafka is **not reimplemented and not vendored**. The build script downloads the official
`kafka_2.13-3.9.1.tgz` from `archive.apache.org` and keeps the 41 jars (of 120) that this workload
actually loads; the only Java in this repository is ~1 500 lines of glue in `java/`.

```text
page (kafka/index.html, src/main.ts)
  |  every call is a promise into the JVM, serialised through one queue
  v
CheerpJ 4.3, Java 8            <- the JVM, wasm
  |
  v
KafkaBrowser (java/KafkaBrowser.java)      the facade the page calls; returns JSON
  |            |               |
  |            |               +-- Admin / KafkaProducer / KafkaConsumer   (real Kafka clients)
  |            +-- kafka.server.KafkaRaftServer   broker + controller, real threads
  +-- kafka.tools.StorageTool               formats the metadata log with a cluster id
                     |
       +-------------+--------------------------+
       v                                        v
  inmem.* (java/inmem/)                   CheerpJ /files (IndexedDB)
  a loopback SelectorProvider:            log.dirs -- the real Kafka log segments,
  Selector, SocketChannel,                .index/.timeindex, __cluster_metadata
  ServerSocketChannel over byte queues
```

## The three things a browser breaks

### Sockets: a loopback `SelectorProvider`

Nothing in a tab can open TCP, and under CheerpJ the JDK's native NIO entry points are simply absent:

```text
Selector.open()            UnsatisfiedLinkError: Java_sun_nio_ch_EPoll_eventSize
ServerSocketChannel.open() UnsatisfiedLinkError: Java_sun_nio_ch_ServerSocketChannelImpl_initIDs
SocketChannel.open()       UnsatisfiedLinkError: Java_sun_nio_ch_Net_initIDs
```

But the broker and the clients are in one JVM here, and Kafka's entire network layer — the acceptor,
the processors, `NetworkClient`, the Raft client — goes through `java.nio`. So `java/inmem/` is a
complete `SelectorProvider` written in Java, installed with
`-Djava.nio.channels.spi.SelectorProvider=inmem.InMemorySelectorProvider`: `InMemoryNet` is a port
registry, a connection is a pair of bounded byte queues (`InMemoryPipe`), and `InMemorySelector`
computes readiness from those queues and blocks on a monitor instead of `epoll`. Kafka is unmodified
and unaware: it binds `localhost:9092` and `localhost:9093`, accepts, reads and writes real Kafka
protocol bytes, and never learns that no socket exists.

### Disk: CheerpJ's `/files`

`log.dirs` points at `/files/kafka-logs-N`, CheerpJ's IndexedDB-backed writable filesystem, and Kafka
writes its real segments, offset and time indexes and `__cluster_metadata` there. Two adaptations:

- `MappedByteBuffer.force()` has no implementation (`UnsatisfiedLinkError:
  Java_java_nio_MappedByteBuffer_force0`), which stops the KRaft log's first index flush. The page
  supplies that one native as a no-op through CheerpJ's `natives` hook — writes are already in
  CheerpJ's filesystem, there is nothing to msync.
- `FileChannel.transferTo` fails with `Function not implemented`; nothing on this path uses it.

The log directory survives a reload, so the tab really does recover a formatted cluster from disk.
**Reset log dir** moves to a fresh `/files/kafka-logs-N+1` and reloads (CheerpJ has no directory
removal), which is why the button is named after what it does.

### Java 8, not 11 or 17

Under CheerpJ 4.3's Java 11 and 17 runtimes, *absolute* writes into a heap `ByteBuffer` are silently
dropped:

```text
ByteBuffer.allocate(8).putShort(2, (short) 0x0102) -> 00 00 00 00 00 00 00 00
```

Kafka's record batches are built exactly that way, so a formatted metadata log came back as garbage —
`BootstrapDirectory.read()` failed with `Unsupported control record type ABORT at offset 0`, because
the record's 4-byte control key had been written but not stored. The same probe is correct on
CheerpJ's Java 8 runtime, so the page runs `cheerpjInit({ version: 8 })`. The glue is compiled with
`--release 8` for the same reason.

Threads, `ExecutorService`, `wait`/`notify`, `RandomAccessFile` and `FileChannel` all behave, so
Kafka's request handlers, replica manager, Raft io thread and the group coordinator run as they do
anywhere else.

## What the page does

Press **Start the broker**: the header reports each step (JVM, jars, `kafka-storage format`, broker
and controller startup, first `AdminClient` answer). Then

- **Create topic** — `Admin.createTopics`, any partition count.
- **Produce** / **Send 10** — `KafkaProducer.send(...).get()`, and the log reports the partition and
  offset the broker assigned.
- **Poll** — a `KafkaConsumer` in a real consumer group: it joins, is assigned partitions by the group
  coordinator, returns records and commits with `commitSync`. The consumer per group is kept alive
  between polls so it stays a member.
- The right-hand panels are `Admin.describeCluster`, `describeTopics`, `listOffsets(earliest/latest)`,
  `listConsumerGroups` and `listConsumerGroupOffsets` on a 4-second refresh: partitions, leader, log
  start and end offsets, and each group's committed offset and lag. Produce without polling and watch
  the lag column climb.
- **Broker log** is Kafka's own log4j output — CheerpJ forwards stdout to `console.log` and the page
  tees it in.

## Numbers, measured

| | |
| --- | --- |
| download | 32.1 MB of Kafka jars (41 files) + 22 kB of glue, plus the CheerpJ runtime, all lazy: nothing is fetched until **Start the broker** |
| boot | 30–45 s in Chrome 141 on a warm cache: ~12 s `kafka-storage format`, ~15 s broker + controller startup, the rest JVM and class loading |
| create topic | ~1 s · produce ~0.2 s/record · first poll ~3 s (group join), later polls <1 s |

## Run it yourself

Requirements: Node `^20.19 || >=22.12`, and a JDK for the glue (if `javac` is not on `PATH` the build
downloads a Temurin JDK into `.jdk/`).

```bash
git clone https://github.com/iancmoritz/apacheprojects.git
cd apacheprojects/kafka
npm install
npm run dev     # first run downloads the Kafka release (~122 MB) and keeps 32 MB of it
```

Then <http://localhost:5173/kafka/>. `npm run build` writes the static bundle to `dist/` (the page at
`dist/kafka/index.html`, everything else under `dist/_kafka/`, so the root
[`scripts/assemble.mjs`](../scripts/assemble.mjs) can merge it with the other projects).

```text
java/                    the only hand-written Java
  KafkaBrowser.java      format / start / createTopic / produce / consume / state, JSON in and out
  inmem/                 the loopback SelectorProvider and its channels, sockets and byte pipes
scripts/fetch-jars.mjs   downloads the Kafka release, keeps the 41 jars this needs   (gitignored output)
scripts/build-glue.mjs   javac --release 8 -> public/_kafka/kafka-browser-glue.jar   (gitignored output)
src/main.ts              boot sequence, the UI, and the queue in front of the JVM
kafka/index.html         the page
```

## Limits, honestly

- **The CheerpJ runtime comes from `cjrtnc.leaningtech.com`.** It is not redistributable, so it is the
  one asset not served from this origin. It is a JVM, not a Kafka service: no Kafka bytes, no Kafka
  protocol traffic and no cluster state ever leave the tab.
- **One node, one JVM, replication factor 1.** Everything — controller quorum, broker, producer,
  consumer, `AdminClient` — is in the page, connected by the loopback transport. There is nothing to
  connect an external client to, and no test of real replication.
- **`MappedByteBuffer.force` is a no-op** (above). Data reaches CheerpJ's filesystem; there is no
  fsync-level durability guarantee, and a tab killed mid-write can leave the log dir behind — use
  **Reset log dir**.
- **CheerpJ runs the JVM on the page's thread**, so calls into Kafka are serialised: a `Poll` with
  nothing to read holds the queue for up to 8 s and the state panels pause until it returns.
- **The pruned classpath fits this workload.** Kafka Streams, Connect, transactions with RocksDB,
  ZooKeeper mode, SSL/SASL and compression codecs beyond LZ4 are outside it, and asking for them will
  raise `NoClassDefFoundError`.
- **`kafka-storage format` and boot are slow** (~30 s) and there is no shutdown: closing the tab
  stops the broker, which is why the log dir is treated as recoverable rather than clean.
