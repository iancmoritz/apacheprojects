/*
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

package org.apacheprojects.hadoopwasm.console;

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;
import org.apacheprojects.hadoopwasm.net.Trace;

/**
 * The Hadoop cluster the page talks to.
 *
 * <p>Every entry point is a static method returning a JSON string, because a string is the one value
 * CheerpJ passes between the JVM and JavaScript unchanged. The page makes one call, {@link #session},
 * which boots the cluster and then runs the visitor's commands; everything under it is a thin wrapper
 * over Hadoop's own APIs, and every number the page renders comes back from Hadoop
 * rather than from bookkeeping here. The same methods drive the console from a desktop JDK.
 *
 * <p>Each of those blocks until Hadoop answers, and in a tab that means the page stops repainting for
 * as long as the call runs: CheerpJ runs the JVM on the thread the page is drawn on. Handing the work
 * to a Java thread and polling for the result does not help -- CheerpJ refuses a call from JavaScript
 * while any Java thread is still runnable ("Java code still running, check for a missing 'await'"), so
 * the poll cannot get in. Blocking is therefore the only shape available, and the {@link Watchdog}
 * exists because of it.
 *
 * <p>What runs is a real {@link MiniDFSCluster}: a NameNode with an fsimage and an edit log, and a
 * DataNode with block files, both in this JVM, talking Hadoop's own RPC over the in-process sockets in
 * {@code ../net}. The parts that need an operating system are configured off (see {@link
 * #configuration}).
 */
public final class HadoopConsole {

  /**
   * Where the NameNode and DataNode keep their storage directories.
   *
   * <p>{@code /files} is CheerpJ's writable filesystem ({@code /app} is the read-only HTTP-backed
   * one); the property is what lets the same console run against a desktop JDK, which is how the
   * configuration below was arrived at.
   */
  static final String STORAGE = System.getProperty("hadoopwasm.storage", "/files/hadoop");

  /** Where the NameNode and DataNode keep their storage; wiped on every boot, unlike {@link #STORAGE}. */
  private static final String DFS = STORAGE + "/dfs";

  /** Where the local-filesystem mode keeps its files; Hadoop's {@code LocalFileSystem} owns these. */
  private static final String LOCAL = STORAGE + "/local";

  private static MiniDFSCluster cluster;

  private static FileSystem fs;

  private static JobRun run;

  private HadoopConsole() {}

  /** The cluster, once it is up; the shell and the job runner both go through this. */
  static FileSystem fs() {
    return fs;
  }

  static MiniDFSCluster cluster() {
    return cluster;
  }

  /**
   * Starts the NameNode and DataNode in this tab.
   *
   * @param datanodes how many DataNodes to start (one is what a tab can afford)
   */
  public static String start(int datanodes) {
    long t0 = System.currentTimeMillis();
    Watchdog.enter("starting the cluster");
    try {
      if (cluster != null) {
        return cluster(t0);
      }
      identity();
      temp();
      // Every run formats a new namespace over cleared storage. A tab cannot keep the old one: the
      // cluster the last run left behind was never shut down (see #session), so its storage is
      // half-written, and MiniDFSCluster's own clear of a half-written storage directory fails under
      // CheerpJ -- its mkdir does not see its own delete.
      wipe(new java.io.File(DFS));
      wipe(new java.io.File(STORAGE, "mapred"));
      Configuration conf = configuration();
      // waitSafeMode(false) takes MiniDFSCluster's own eleven-second wait for the DataNode out of the
      // constructor. A tab needs longer than that, and a constructor that gives up calls the shutdown
      // that never returns here (see #stop), so the wait has to be ours instead.
      cluster =
          new MiniDFSCluster.Builder(conf)
              .numDataNodes(datanodes)
              .format(true)
              .waitSafeMode(false)
              .build();
      up();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      return cluster(t0);
    } catch (Throwable t) {
      return Failures.json(t, System.currentTimeMillis() - t0);
    } finally {
      Watchdog.exit();
    }
  }

  /**
   * Waits for the DataNode to have registered and the NameNode to have left safe mode, as {@link
   * MiniDFSCluster#isClusterUp} reports it.
   *
   * @throws java.io.IOException if the cluster is still not up after {@code
   *     hadoopwasm.up.ms} milliseconds
   */
  private static void up() throws java.io.IOException, InterruptedException {
    long deadline = System.currentTimeMillis() + Integer.getInteger("hadoopwasm.up.ms", 180_000);
    while (!cluster.isClusterUp()) {
      if (System.currentTimeMillis() > deadline) {
        throw new java.io.IOException("the DataNode did not register before the deadline");
      }
      Thread.sleep(250L);
    }
  }

  /**
   * The Hadoop configuration a browser tab can actually run.
   *
   * <p>Everything switched off here is switched off because the platform cannot provide it, not to
   * make Hadoop do less work: there is no {@code libhadoop.so} for native checksums or native IO, no
   * OS user to check permissions against, and no way to shell out to {@code du}.
   */
  static Configuration configuration() {
    Configuration conf = new HdfsConfiguration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, DFS);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024 * 1024);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);
    // No OS user and no native stat(2): permission checks would compare against an identity the tab
    // does not have, and ACLs need the extended attributes CheerpJ's filesystem has no room for.
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, false);
    // `du` is a subprocess; the DataNode's replica count is small enough to walk instead.
    conf.set("fs.getspaceused.classname", "org.apacheprojects.hadoopwasm.fs.WalkedSpaceUsed");
    conf.setBoolean("dfs.datanode.dureserved.calculate.rootdir", false);
    // Hadoop's own local job runner, in this JVM: there is no YARN here to submit to.
    conf.set("mapreduce.framework.name", "local");
    conf.set("mapreduce.jobtracker.address", "local");
    conf.set("mapreduce.cluster.local.dir", STORAGE + "/mapred/local");
    conf.set("mapreduce.jobtracker.staging.root.dir", STORAGE + "/mapred/staging");
    // One core, cooperative threads: a heartbeat that gives up while the tab is busy would fail the
    // job for no reason. Overridable because a shorter one is how a stuck read is made to say so.
    int timeout = Integer.getInteger("hadoopwasm.timeout.ms", 15 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, timeout);
    conf.setInt("dfs.datanode.socket.write.timeout", timeout);
    conf.setInt("ipc.client.connect.timeout", timeout);
    conf.setInt("ipc.ping.interval", timeout);
    // Every thread here is scheduled cooperatively on one core, and a cluster's default pools -- two
    // Jetty thread pools, RPC handlers and readers for both daemons -- are sized for a machine with
    // cores to spare. Cut to the minimum that still serves one client.
    conf.setInt("hadoop.http.max.threads", 5);
    conf.setBoolean("dfs.webhdfs.enabled", false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY, 2);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY, 2);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 2);
    conf.setInt("ipc.server.read.threadpool.size", 1);
    return conf;
  }

  /**
   * What the last visit left behind: the {@link Watchdog}'s stacks and, with {@code
   * -Dhadoopwasm.trace}, the virtual network's trace. Both are empty unless something hung.
   *
   * <p>They are read from CheerpJ's filesystem and then forgotten, because a tab that froze delivered
   * neither its console output nor its devtools, and this is the one channel left.
   */
  public static String hangs() {
    String stacks = Watchdog.log();
    String trace = Trace.read();
    Watchdog.forget();
    Trace.forget();
    return Json.object().field("ok", true).field("log", stacks).field("trace", trace).end();
  }

  /**
   * Starts the local-filesystem mode: Hadoop's own {@link org.apache.hadoop.fs.LocalFileSystem} over
   * CheerpJ's writable filesystem, with no NameNode and no DataNode.
   *
   * <p>This mode exists because MapReduce and HDFS cannot yet run in the same tab: with the
   * MiniDFSCluster up, the map task's read of its input block wedges the whole JVM (see the README).
   * The MapReduce here is Hadoop's own {@code LocalJobRunner} either way -- the same {@code Job},
   * splits, counters and task reports -- only the filesystem under it is local rather than HDFS.
   */
  private static String startLocal() {
    long t0 = System.currentTimeMillis();
    try {
      identity();
      temp();
      wipe(new java.io.File(STORAGE, "mapred"));
      new java.io.File(LOCAL).mkdirs();
      Configuration conf = configuration();
      conf.set("fs.defaultFS", "file:///");
      fs = FileSystem.get(new java.net.URI("file:///"), conf);
      fs.setWorkingDirectory(new org.apache.hadoop.fs.Path(LOCAL));
      return Json.object()
          .field("ok", true)
          .field("mode", "local")
          .field("hadoopVersion", VersionInfo.getVersion())
          .field("javaVersion", System.getProperty("java.version"))
          .field("uri", fs.getUri().toString())
          .field("user", UserGroupInformation.getCurrentUser().getUserName())
          .field("cores", Runtime.getRuntime().availableProcessors())
          .field("fileSystem", fs.getClass().getName())
          .field("workingDirectory", fs.getWorkingDirectory().toString())
          .field("blockSize", fs.getDefaultBlockSize())
          .field("replication", fs.getDefaultReplication())
          .field("bootMs", System.currentTimeMillis() - t0)
          .end();
    } catch (Throwable t) {
      return Failures.json(t, System.currentTimeMillis() - t0);
    }
  }

  /**
   * Runs a whole visit inside one call: boot the cluster, then the visitor's commands.
   *
   * <p>CheerpJ will not re-enter Java while the cluster's threads are alive -- after {@link #start} a
   * second call from the page never returns, whatever it does, even one that only returns a string
   * (a call that only returns a string was the measurement) -- so everything the visitor asked for has to happen before
   * this returns, and one page load runs one session. {@link #stop} would be the obvious way out of
   * that, but {@code MiniDFSCluster.shutdown} does not come back either: it stops the IPC servers,
   * removes the block pool and then hangs finalizing the edit log.
   *
   * @param script one HDFS shell command per line; {@code wordcount <in> <out>} runs the MapReduce job
   * @param writePath a file to write into HDFS before the script runs, or empty for none
   * @param writeText what to write there, so a whole file of text can arrive in one call
   */
  public static String session(String script, String writePath, String writeText, String mode) {
    long t0 = System.currentTimeMillis();
    Watchdog.enter("running a session");
    try {
      String boot = "local".equals(mode) ? startLocal() : start(1);
      if (fs == null) {
        return boot;
      }
      StringBuilder steps = new StringBuilder("[");
      boolean first = true;
      if (writePath != null && !writePath.trim().isEmpty()) {
        steps.append(
            Json.object()
                .field("command", "put - " + writePath.trim())
                .raw("result", HdfsShell.write(writePath.trim(), writeText))
                .end());
        first = false;
      }
      for (String line : script.split("\n")) {
        String command = line.trim();
        if (command.isEmpty() || command.startsWith("#")) {
          continue;
        }
        String result;
        try {
          result = command.startsWith("wordcount ") ? job(command) : HdfsShell.run(command);
        } catch (Throwable t) {
          result = Failures.json(t, 0);
        }
        if (!first) {
          steps.append(',');
        }
        first = false;
        steps.append(Json.object().field("command", command).raw("result", result).end());
      }
      steps.append(']');
      return Json.object()
          .field("ok", true)
          .raw("cluster", boot)
          .raw("steps", steps.toString())
          .field("ms", System.currentTimeMillis() - t0)
          .end();
    } catch (Throwable t) {
      return Failures.json(t, System.currentTimeMillis() - t0);
    } finally {
      Watchdog.exit();
    }
  }

  /**
   * Runs WordCount to completion, keeping what Hadoop reported on the way.
   *
   * <p>The page cannot poll a running job -- it cannot call in at all until this returns -- so the
   * progress, counters and task reports are sampled here, out of the same {@code Job} handle the
   * polling version read, and handed over as a timeline once the job is done.
   */
  private static String job(String command) throws Exception {
    String[] words = command.split("\\s+");
    if (words.length != 3) {
      return Failures.message("usage: wordcount <input> <output>");
    }
    run = WordCount.submit(fs, words[1], words[2]);
    int sample = Integer.getInteger("hadoopwasm.job.sample.ms", 0);
    StringBuilder timeline = new StringBuilder("[");
    while (!run.finished()) {
      if (sample > 0) {
        if (timeline.length() > 1) {
          timeline.append(',');
        }
        timeline.append(run.poll());
        Thread.sleep(sample);
      } else {
        Thread.sleep(1000L);
      }
    }
    if (timeline.length() > 1) {
      timeline.append(',');
    }
    timeline.append(run.poll());
    timeline.append(']');
    return Json.object()
        .field("ok", true)
        .raw("submitted", run.submitted())
        .raw("timeline", timeline.toString())
        .end();
  }

  /**
   * Stops the NameNode and DataNode.
   *
   * <p>This works on a desktop JVM and does not come back in a tab -- it gets as far as removing the
   * block pool and then hangs finalizing the edit log -- so {@link #session} does not call it, and a
   * run leaves its cluster behind for the reload to replace. It is kept for the desktop JVM, and
   * because a CheerpJ that finishes this shutdown is all that stands between this page and a cluster
   * it can keep calling.
   */
  public static String stop() {
    try {
      if (cluster != null) {
        cluster.shutdown(true);
      }
      org.apache.hadoop.fs.FileSystem.closeAll();
    } catch (Throwable t) {
      System.out.println("the cluster did not shut down cleanly: " + t);
    } finally {
      cluster = null;
      fs = null;
      run = null;
    }
    return Json.object().field("ok", true).end();
  }

  private static String cluster(long t0) throws Exception {
    NameNode nn = cluster.getNameNode();
    return Json.object()
        .field("ok", true)
        .field("hadoopVersion", VersionInfo.getVersion())
        .field("javaVersion", System.getProperty("java.version"))
        .field("uri", fs.getUri().toString())
        .field("user", UserGroupInformation.getCurrentUser().getUserName())
        .field("cores", Runtime.getRuntime().availableProcessors())
        .field("nameNodeAddress", nn.getNameNodeAddress().toString())
        .field("safeMode", nn.isInSafeMode())
        .raw("datanodes", Datanodes.json(cluster, fs))
        .raw("storage", Datanodes.storage(cluster))
        .field("blockSize", fs.getDefaultBlockSize())
        .field("replication", fs.getDefaultReplication())
        .field("bootMs", System.currentTimeMillis() - t0)
        .end();
  }

  /**
   * Deletes a directory tree, so a reloaded tab starts from an empty namespace.
   *
   * <p>{@code MiniDFSCluster} formats the NameNode itself, but its own clear of the storage directory
   * fails under CheerpJ ({@code Cannot create directory .../current}: the {@code mkdir} that follows
   * its {@code delete} does not see the deletion), so the second visit to the page could never start a
   * cluster. Deleting the tree before Hadoop looks at it avoids the sequence entirely.
   */
  private static void wipe(java.io.File directory) {
    java.io.File[] children = directory.listFiles();
    if (children != null) {
      for (java.io.File child : children) {
        wipe(child);
      }
    }
    directory.delete();
  }

  /**
   * Makes sure {@code java.io.tmpdir} exists.
   *
   * <p>CheerpJ starts with an empty writable filesystem, and Jetty -- which serves the NameNode's web
   * UI, started by {@code MiniDFSCluster} whether anything can reach it or not -- refuses to start
   * when its parent temp directory is not writable, which takes the NameNode down with it.
   */
  private static void temp() {
    new java.io.File(System.getProperty("java.io.tmpdir", "/files/tmp")).mkdirs();
    new java.io.File(STORAGE).mkdirs();
  }

  /**
   * Gives Hadoop an identity without asking the operating system for one.
   *
   * <p>{@code UserGroupInformation} otherwise attempts a JAAS login, which under CheerpJ fails with
   * {@code KerberosAuthException: failure to login} because there is no OS user to find.
   */
  private static void identity() {
    putEnv("HADOOP_USER_NAME", "browser");
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("browser"));
  }

  @SuppressWarnings("unchecked")
  private static void putEnv(String key, String value) {
    try {
      Class<?> environment = Class.forName("java.lang.ProcessEnvironment");
      Field field;
      try {
        field = environment.getDeclaredField("theCaseInsensitiveEnvironment");
        field.setAccessible(true);
        ((Map<String, String>) field.get(null)).put(key, value);
        return;
      } catch (NoSuchFieldException e) {
        field = environment.getDeclaredField("theUnmodifiableEnvironment");
      }
      field.setAccessible(true);
      Object unmodifiable = field.get(null);
      Field delegate = unmodifiable.getClass().getDeclaredField("m");
      delegate.setAccessible(true);
      ((Map<String, String>) delegate.get(unmodifiable)).put(key, value);
    } catch (Exception e) {
      System.out.println(key + " could not be set: " + e);
    }
  }
}
