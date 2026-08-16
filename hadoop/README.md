# hadoop-wasm

Apache Hadoop 3.3.6 running entirely in a browser tab. In **HDFS mode** a real NameNode and a real
DataNode start in the visitor's tab — `MiniDFSCluster`, an fsimage and an edit log, block files under
a storage directory, the DataNode registering and heartbeating over Hadoop's own RPC — and the shell
on the page drives them through Hadoop's `FileSystem` API. In **local mode** Hadoop's own MapReduce
runs the canonical WordCount over Hadoop's `LocalFileSystem`: `LocalJobRunner`, `TextInputFormat`
splits, a real spill/sort/merge, a combiner, and the `Counters` and task reports Hadoop itself
reports. Everything runs on OpenJDK 8 compiled to WebAssembly ([CheerpJ](https://cheerpj.com/)); the
server only hands over static files.

The one thing that does not work is the two together: submit the job with the MiniDFSCluster up and
the JVM freezes. That is measured, not assumed, and it is [written up below](#what-does-not-work) and
on the page. There is no fake filesystem and no JavaScript word count anywhere in here — every path,
block, split, counter and output line on the page is a value returned by Hadoop.

Hadoop itself is **not vendored and not reimplemented**: `scripts/build-runtime.mjs` downloads the
official Hadoop 3.3.6 artifacts from Maven Central (`hadoop-common`, `hadoop-hdfs`,
`hadoop-hdfs-client`, the `hadoop-hdfs` *test* jar for `MiniDFSCluster`, `hadoop-mapreduce-client-*`
and their dependencies) and patches their bytecode where CheerpJ cannot execute it. This directory
holds only the glue (`java/`, ~2 900 lines): the console class the page calls, a virtual socket stack,
three shims, and one ASM pass. The CheerpJ runtime is loaded from `cjrtnc.leaningtech.com` because its
licence does not allow redistribution — it is a wasm JVM, not a Hadoop service, and no byte of the
visitor's data leaves the tab.

This directory builds the `/hadoop/` route of the site the repository root assembles. The page is
`hadoop/index.html`, and everything it needs at the origin root is under `/_hadoop/`, so it cannot
collide with the other projects.

```text
page (hadoop/index.html, src/main.ts)
  src/driver.ts   cheerpjInit + cheerpjRunLibrary(classpath from /_hadoop/manifest.json)
        |
        v
  CheerpJ (OpenJDK 8 in wasm)    jars from /app/_hadoop/jars/*.jar over HTTP range requests
        |
        v
  org.apacheprojects.hadoopwasm.console.HadoopConsole   (java/)
    session(script, write, mode)
      mode=hdfs   new MiniDFSCluster.Builder(conf).numDataNodes(1).build()   -> real NameNode + DataNode
      mode=local  FileSystem.get("file:///")                                -> Hadoop LocalFileSystem
        then      HdfsShell   mkdir/put/ls/cat/rm/df/blocks  -> FileSystem calls
        then      WordCount   Job + LocalJobRunner           -> Counters, splits, TaskReports
        |
        +-- net/    virtual SelectorProvider/Socket/ServerSocket/channels: the NameNode's and
        |           DataNode's RPC and data-transfer protocol over in-process loopback
        +-- nio/    positional FileChannel read/write (CheerpJ has no pwrite64)
        +-- util/   ShellShim (no /bin/sh for stat/permissions), ThreadShim (interrupt + unpark)
        +-- fs/     free space and recursive usage without du(1)
        +-- tools/  HostCallPatcher: the build-time ASM pass that redirects the calls above
```

## Run it yourself

Requirements: Node `^20.19 || >=22.12`, a Chromium/Firefox/Safari, ~1 GB of disk. The build patches
Hadoop's own bytecode, so it needs a JDK; if the machine has no `javac` on `PATH` — which is the case
on Vercel's build image — it downloads Temurin 17 into `.cache/jdk` and uses that.

```bash
git clone https://github.com/iancmoritz/apacheprojects.git
cd apacheprojects/hadoop
npm install
npm run dev     # first run downloads Hadoop's jars (~120 MB) and patches them (~3 min)
```

Then open <http://localhost:5173/hadoop/>. The header reports each boot step; when the last one is
ticked the console is live.

- Press **Run** for the WordCount in **Local** mode: the progress bars, `Counters`, splits/tasks and
  output tabs are all read out of Hadoop's `Job` handle while the job runs.
- Switch the mode to **HDFS** and press **Run** to start a NameNode and a DataNode and replay the
  script: `mkdir`, `put` of Hadoop's own default configuration files, `ls -R`, `blocks` (the block
  IDs, generation stamps and DataNode the NameNode reports for a file), `cat`, `df`. The cluster panel
  shows the NameNode address, safe mode, the namespace's file/block counts, the edit log's
  transaction id and the DataNode's registration. Under `npm run dev` this sometimes freezes partway
  through — Vite serving 130 MB of jars competes with the JVM for the one core CheerpJ schedules on;
  against the built site it completed on every run.

Both modes take one run per page load — see the limitations — so the button turns into **Reload to run
again**.

Static bundle instead of the dev server:

```bash
npm run build     # asset generation + tsc --noEmit + vite build into dist/
```

| command | what it does |
| --- | --- |
| `npm run assets` | downloads Hadoop's jars (and a JDK, if needed) and writes the patched classpath, the corpus and the manifest into `public/_hadoop/` (gitignored); `node scripts/build-runtime.mjs --force` rebuilds |
| `npm run typecheck` | `tsc --noEmit` |

## Measured

On this machine (Chrome 140, Linux, over HTTP, fresh profile):

| | |
| --- | --- |
| boot to a live JVM with the classpath loaded | **4 s** |
| HDFS mode: NameNode + DataNode up, 10 shell commands | **24 s** boot, **30 s** total |
| local mode: 6 shell commands + WordCount over 231 KB / 5 splits | **12 s** total, the job itself **3.1 s** |
| downloaded for a whole local-mode run | **117 MB** |
| downloaded for a whole HDFS run | **133 MB** |
| classpath on disk | 8 jars, 90 MB (7 upstream + `hadoopwasm.jar`) |

Nothing is eagerly downloaded: CheerpJ asks for the byte ranges of each class it needs, so those
figures are what the workload actually touched (some ranges more than once), not a download of the
90 MB of jars. Warm reloads reuse the HTTP cache.

It is slow for the reason everything in a wasm JVM is slow: no JIT and one core. It is not fast, it is
real.

## How the hard parts work

Each item here was a hard failure before it was a workaround. The bytecode rewriting is one ASM pass,
`tools/HostCallPatcher`, run over Hadoop's jars by `scripts/build-runtime.mjs`; the rest is
configuration or a class in `java/`. Hadoop's bytecode is plain Java 8 and needed far less of this
than Spark's Scala did — none of `spark/`'s `wide`-locals, lambda or `Unsafe` passes are used here.

- **Sockets** (`net/`, ~1 000 lines). A tab cannot open TCP, and unlike Spark's `local[*]` an HDFS
  cluster is genuinely distributed software: the DataNode registers with the NameNode over
  `ipc.Client`/`ipc.Server`, and the client writes blocks over the data-transfer protocol, both real
  socket code. `MiniDFSCluster` runs it all in one JVM, so the substitution is at the JDK seam rather
  than inside Hadoop: a `SelectorProvider` (installed through `cheerpjInit`'s `javaProperties`) hands
  out in-process loopback `Socket`s, `ServerSocket`s and selectable channels that hand buffers between
  threads. Hadoop's own RPC, its retry and its wire format run unmodified on top; without it the
  DataNode never registers (`java.net.SocketException: Operation not permitted`, then
  `MiniDFSCluster: No heartbeat from DataNode`). The web UIs are off for the same reason — Jetty needs
  a real listening socket.
- **Positional file I/O** (`nio/ChannelShim`, patched). The NameNode writes its fsimage and edit log
  and the DataNode its block metadata with `FileChannel.write(ByteBuffer, long)`, which CheerpJ has no
  `__syscall_pwrite64` for: startup stopped dead with `Missing import: __syscall_pwrite64`. The pass
  rewrites those call sites to a shim that seeks and writes.
- **No subprocesses** (`util/ShellShim`, `fs/`, `org/apache/hadoop/fs/DF`, patched). Hadoop shells out
  more than its documentation suggests. `RawLocalFileSystem` reads permissions by running `ls -ld`
  (`NoSuchElementException` in `loadPermissionInfoByNonNativeIO`, which killed DataNode startup), `DF`
  parses `df -k` (`IOException: Fewer lines of output than expected`) and `GetSpaceUsed` runs `du`.
  The pass redirects Hadoop's `Shell`/`ProcessBuilder` calls to a shim that answers from Java, `DF` is
  replaced by a class of the same name earlier on the classpath, and `fs.getspaceused.classname` is
  pointed at a walker. CheerpJ's `Unsupported external command: setsid` lines in the log are the same
  story for commands nothing depends on, printed by the real runtime and left visible.
- **An interrupt CheerpJ does not deliver** (`util/ThreadShim`, patched). A thread waiting in an
  untimed `Condition.await()` is not woken by `Thread.interrupt()` under CheerpJ. A one-class probe
  in the browser (`console/Probe`, run from `probe.html` under the dev server; it is not part of the
  built page) pinned it down:

  ```text
  ✗ Thread.interrupt wakes an untimed Condition.await:  interrupted=0, alive=true
  ✓ Thread.interrupt + LockSupport.unpark:              interrupted=1, alive=false
  ```

  That is exactly how `MapTask` ends its `SpillThread`, so no map task could ever finish and the job
  stopped after `Finished spill 0` forever. The pass rewrites `interrupt()` call sites — including
  those whose receiver is a `Thread` *subclass*, which `SpillThread` is, so the pass walks the
  superclass graph of every class in the jars first — to add a best-effort `LockSupport.unpark`. This
  is what makes the WordCount complete.
- **Native code is off.** `libhadoop` does not exist here, so `io.native.lib.available=false`,
  `NativeIO` is never reached (the shims above stand in), checksums and codecs are Hadoop's pure-Java
  paths, and HDFS permission checks are disabled (`dfs.permissions.enabled=false`) because the tab has
  no OS user. `UserGroupInformation` is given one explicitly.
- **Storage and identity.** The fsimage, edit log, block files, MapReduce staging and job output all
  live under `/files/hadoop` on CheerpJ's writable, persistent filesystem; the jars and the corpus are
  read from `/app`, its read-only HTTP-backed mount. Free space is reported from the browser's storage
  quota. One DataNode, replication 1, 1 MB blocks.
- **One call into Java per run.** CheerpJ's library mode never returns from a *second* call into Java
  while the cluster's threads are alive — not even from a method that only returns a string — so a run
  is a single `session()` call that starts the filesystem, replays the visitor's commands and returns
  everything at once, and a page reload is what starts the next run. `MiniDFSCluster.shutdown` hangs
  finalizing the edit log for the same reason, so the page does not pretend to offer one.
- **Watchdog** (`console/Watchdog`). A tab whose JVM is stuck delivers no console output at all, so the
  watchdog dumps every thread's stack past a timeout both to stdout and to a file in CheerpJ's
  persistent filesystem, where the next page load can read it. Most of what is documented here was
  found that way; `?watchdog=<ms>`, `?trace=1` (virtual-network trace) and `?debug=1` (log4j at DEBUG)
  are still wired up on the page.

## What does not work

- **MapReduce and HDFS do not run in the same tab.** This is the one real gap. The job finishes over
  `LocalFileSystem` every time; submit the same job with the MiniDFSCluster up and about five seconds
  later every thread in the JVM stops — the DataNode's and NameNode's own select loops included, which
  the virtual network's trace shows going quiet together — and the tab never comes back (5 of 5
  attempts against the built site, still frozen at 270 s). What the evidence says: HDFS `mkdir`, `put`
  and `ls` all complete in the same run right up to the submit; the last trace lines are the daemons'
  own idle `select waiting` / `accept waiting` rather than anything blocked on a Hadoop lock; every
  wait in the virtual network is bounded, so it is not a Java deadlock; and no watchdog dump is
  delivered because the watchdog thread stops with everything else. It is not the interrupt bug above
  either: it happens identically with that shim off (`?unpark=0`), with the shim never compiled in,
  and with the daemons' handler pools cut to one thread each. It looks like CheerpJ's cooperative
  scheduler giving up on a JVM with this many live threads — the same failure that occasionally ends
  an HDFS-only run under the dev server. So the page ships both modes, labelled, rather than a job
  that pretends to be over HDFS.
- **No web UIs.** The NameNode UI and the job history server need a listening socket, which no tab
  has. The cluster panel and the job panels on the page are Hadoop's own APIs rendered by the page
  instead. (The `airflow/`-style service-worker route would still need the daemon to answer a real
  socket, so it was not attempted.)
- **One run per page load**, and no clean shutdown, for the CheerpJ re-entrancy reason above.
- **One core, no parallelism.** `Runtime.availableProcessors()` is 1, so one map task runs at a time
  and the tab's JVM is busy for the length of a run (the page stays responsive during HDFS work; the
  local job holds it).
- **Not a cluster.** One DataNode, replication 1, no second JVM, no YARN — `mapreduce.framework.name`
  is `local`, so `LocalJobRunner` runs the tasks in-process, which is Hadoop's own code path but not a
  ResourceManager. `hadoop-yarn-*` is on the classpath only because MapReduce's client needs it.
- **Noise in the JVM log is real.** `TODO: GetMemoryManagers` is CheerpJ reporting an unimplemented
  JVMTI call and `Unsupported external command: …` is its process stub; both come from the real
  runtime and are shown rather than hidden.
