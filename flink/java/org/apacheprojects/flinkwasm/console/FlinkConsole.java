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

package org.apacheprojects.flinkwasm.console;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsCounts;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apacheprojects.flinkwasm.job.Feed;
import org.apacheprojects.flinkwasm.job.WordCountJob;
import org.apacheprojects.flinkwasm.net.VirtualNet;
import org.apacheprojects.flinkwasm.rest.Dashboard;

/**
 * The Flink cluster the page talks to.
 *
 * <p>Every method here is static, takes and returns strings, and is what JavaScript calls through
 * CheerpJ: a JSON string is the one value that crosses the JVM boundary unchanged. Inside, this is a
 * real {@link MiniCluster} -- a JobManager (dispatcher, resource manager, JobMaster with its
 * scheduler and checkpoint coordinator) and a TaskManager with its own slots, memory manager and
 * shuffle environment, all in this one JVM.
 *
 * <p>What is configured away is only what needs an operating system: the REST endpoint and the web
 * dashboard would have to listen on a socket, the shuffle stays in-process because one TaskManager
 * needs no network channels, and the state backend is the heap one because RocksDB is a native
 * library. The scheduling, the watermarks, the windows, the checkpoints and the state are Flink's.
 */
public final class FlinkConsole {

  /**
   * Everything Flink writes -- blobs, checkpoints, temp files -- goes under here: {@code /files} in
   * the browser, which is the writable half of CheerpJ's filesystem, and a temporary directory when
   * {@link org.apacheprojects.flinkwasm.probe.Probe} runs the same code on an ordinary JVM.
   */
  private static String workDir = "/files/flink";

  private static MiniCluster cluster;

  private static Configuration clusterConfiguration;

  private static JobID jobId;

  private static String planJson;

  private static String jobName;

  private static long submittedAt;

  private static WordCountJob.Options options;

  private FlinkConsole() {}

  /**
   * Starts the MiniCluster.
   *
   * @param slots task slots the single TaskManager offers
   * @param directory where Flink may write: blobs, checkpoints and temporary files
   * @return JSON describing the cluster, or the failure that stopped it
   */
  public static synchronized String startCluster(int slots, String directory) {
    long t0 = System.currentTimeMillis();
    try {
      if (cluster != null && cluster.isRunning()) {
        return cluster(t0);
      }
      // Before Flink constructs its first socket: the blob server binds a java.net ServerSocket,
      // which CheerpJ cannot open, so both it and its clients get in-process pipes instead.
      VirtualNet.installSocketFactories();

      workDir = directory;
      new File(workDir).mkdirs();
      new File(workDir + "/blobs").mkdirs();
      new File(workDir + "/checkpoints").mkdirs();
      new File(workDir + "/tmp").mkdirs();

      clusterConfiguration = configuration(slots);
      MiniClusterConfiguration miniClusterConfiguration =
          new MiniClusterConfiguration.Builder()
              .setConfiguration(clusterConfiguration)
              .setNumTaskManagers(1)
              .setNumSlotsPerTaskManager(slots)
              // One shared, local (never remote) Pekko actor system for the JobManager and the
              // TaskManager: the only RPC arrangement in Flink that opens no socket.
              .setRpcServiceSharing(RpcServiceSharing.SHARED)
              .build();
      cluster = new MiniCluster(miniClusterConfiguration);
      cluster.start();
      return cluster(t0);
    } catch (Throwable t) {
      return failure(t, System.currentTimeMillis() - t0);
    }
  }

  /** The configuration a browser tab can honour; see the class comment for why each entry is here. */
  private static Configuration configuration(int slots) {
    Configuration conf = new Configuration();
    conf.set(CoreOptions.DEFAULT_PARALLELISM, 1);
    conf.set(TaskManagerOptions.NUM_TASK_SLOTS, slots);
    conf.set(TaskManagerOptions.CPU_CORES, 1.0);

    // Memory: sized for a tab, not a machine.  The TaskManager would otherwise try to derive these
    // from the process, and the network and managed pools are the two that allocate up front.
    conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("512m"));
    conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("32m"));
    conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("32m"));
    conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("32m"));
    conf.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.parse("32m"));
    conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("0m"));
    conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("16kb"));
    conf.set(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS, 64);

    // The heap state backend: RocksDB is a native library, and there is no native code here beyond
    // the JVM itself.  Checkpoints land in CheerpJ's writable filesystem, as real files.
    conf.set(StateBackendOptions.STATE_BACKEND, "hashmap");
    conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + workDir + "/checkpoints");
    conf.set(CheckpointingOptions.LOCAL_RECOVERY, false);

    // The REST endpoint (and so the dashboard it serves) is a netty server on a virtual socket, which
    // is reachable from this JVM and from nowhere else; what it cannot do is sample thread stacks,
    // and submitting or cancelling from the dashboard would race the page's own controls.
    conf.set(RestOptions.ENABLE_FLAMEGRAPH, false);
    conf.set(WebOptions.SUBMIT_ENABLE, false);
    conf.set(WebOptions.CANCEL_ENABLE, false);

    // One core, no JIT: every timeout in Flink is short compared to how long this tab takes to do
    // anything, and a slot request or heartbeat that times out fails the job.
    conf.set(RpcOptions.ASK_TIMEOUT_DURATION, Duration.ofMinutes(10));
    conf.set(JobManagerOptions.SLOT_REQUEST_TIMEOUT, Duration.ofMinutes(10));
    conf.set(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, Duration.ofMinutes(1));
    conf.set(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, Duration.ofMinutes(10));

    conf.set(CoreOptions.TMP_DIRS, workDir + "/tmp");
    conf.setString("blob.storage.directory", workDir + "/blobs");
    return conf;
  }

  /**
   * Builds the word count job graph and submits it.
   *
   * @return JSON with the job id, the stream plan the dashboard would draw, and the vertices
   */
  public static synchronized String submitJob(
      int windowSeconds, int lateness, int outOfOrderness, int checkpointInterval, int parallelism) {
    long t0 = System.currentTimeMillis();
    try {
      if (cluster == null || !cluster.isRunning()) {
        return error("the cluster is not running");
      }
      if (jobId != null) {
        cancelJob();
      }
      Feed.reset();
      options =
          new WordCountJob.Options(
              windowSeconds, lateness, outOfOrderness, checkpointInterval, parallelism);

      StreamExecutionEnvironment env = new StreamExecutionEnvironment(clusterConfiguration);
      env.setParallelism(parallelism);
      env.setMaxParallelism(parallelism * 2);
      env.getConfig().setAutoWatermarkInterval(200L);
      env.enableCheckpointing(checkpointInterval * 1000L);
      env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(10));
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
      env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
      // Restarting would silently swallow whatever CheerpJ cannot do; a failure should be visible.
      env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart());

      WordCountJob.define(env, options);

      StreamGraph streamGraph = env.getStreamGraph(false);
      jobName = "windowed word count (" + windowSeconds + "s tumbling)";
      streamGraph.setJobName(jobName);
      planJson = streamGraph.getStreamingPlanAsJSON();
      JobGraph jobGraph = streamGraph.getJobGraph();

      cluster.submitJob(jobGraph).get();
      jobId = jobGraph.getJobID();
      submittedAt = System.currentTimeMillis();

      List<String> vertices = new ArrayList<String>();
      for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
        vertices.add(
            Json.object()
                .field("id", vertex.getID().toString())
                .field("name", vertex.getName())
                .field("parallelism", vertex.getParallelism())
                .field("operators", vertex.getOperatorIDs().size())
                .field("inputs", vertex.getInputs().size())
                .end());
      }

      return Json.object()
          .field("ok", true)
          .field("jobId", jobId.toString())
          .field("jobName", jobName)
          .raw("plan", planJson)
          .raw("vertices", Json.array(vertices))
          .field("submitMs", System.currentTimeMillis() - t0)
          .end();
    } catch (Throwable t) {
      return failure(t, System.currentTimeMillis() - t0);
    }
  }

  /** Queues words for the source; the timestamp is the event time they get. */
  public static synchronized String push(String text, long timestamp) {
    int queued = Feed.push(text, timestamp <= 0 ? System.currentTimeMillis() : timestamp);
    return Json.object().field("ok", true).field("queued", queued).end();
  }

  /** Turns the generator on at {@code rate} events per second, with {@code skew}% out of order. */
  public static synchronized String setGenerator(int rate, int skewPercent) {
    Feed.RATE.set(Math.max(0, rate));
    Feed.SKEW_PERCENT.set(Math.max(0, Math.min(100, skewPercent)));
    Feed.note("generator", rate + " events/s, " + skewPercent + "% out of order");
    return Json.object().field("ok", true).field("rate", rate).field("skewPercent", skewPercent).end();
  }

  /** Asks the checkpoint coordinator for a checkpoint right now. */
  public static synchronized String checkpointNow() {
    try {
      if (cluster == null || jobId == null) {
        return error("no job is running");
      }
      String path = cluster.triggerCheckpoint(jobId).get(10, TimeUnit.MINUTES);
      Feed.note("checkpoint", path);
      return Json.object().field("ok", true).field("path", path).end();
    } catch (Throwable t) {
      return failure(t, 0L);
    }
  }

  /** Everything the page draws once a second: windows, notes, vertices, checkpoints. */
  public static synchronized String poll() {
    long t0 = System.currentTimeMillis();
    try {
      Json out =
          Json.object()
              .field("ok", true)
              .raw("windows", Json.array(Feed.drainWindows()))
              .raw("notes", notes())
              .field("ingested", Feed.INGESTED.get())
              .field("emitted", Feed.EMITTED.get())
              .field("late", Feed.LATE.get())
              .field("watermark", Feed.WATERMARK.get())
              .field("maxEventTime", Feed.MAX_EVENT_TIME.get())
              .field("rate", Feed.RATE.get())
              .field("skewPercent", Feed.SKEW_PERCENT.get())
              .field("now", System.currentTimeMillis());
      if (cluster != null && jobId != null) {
        AccessExecutionGraph graph = cluster.getExecutionGraph(jobId).get(2, TimeUnit.MINUTES);
        out.field("jobId", jobId.toString())
            .field("jobName", jobName)
            .field("state", graph.getState().name())
            .field("runningMs", System.currentTimeMillis() - submittedAt)
            .field(
                "failure",
                graph.getFailureInfo() == null ? null : graph.getFailureInfo().getExceptionAsString())
            .raw("graph", graph(graph))
            .raw("checkpoints", checkpoints(graph.getCheckpointStatsSnapshot()));
      }
      return out.field("pollMs", System.currentTimeMillis() - t0).end();
    } catch (Throwable t) {
      return failure(t, System.currentTimeMillis() - t0);
    }
  }

  /**
   * Sends one request to the JobManager's REST endpoint and returns its response.
   *
   * <p>This is what lets the page serve Flink's real dashboard: the service worker turns every
   * request the dashboard makes into a call here, and the endpoint answering is the same
   * {@code DispatcherRestEndpoint} a cluster on a machine serves {@code :8081} from.
   *
   * @param method the HTTP method
   * @param target the request target the dashboard asked for, path and query
   * @param body a JSON request body, or null
   * @return JSON with the status, the content type and the response body as base64
   */
  public static synchronized String restRequest(String method, String target, String body) {
    try {
      if (cluster == null || !cluster.isRunning()) {
        return error("the cluster is not running");
      }
      Dashboard.note(target);
      Dashboard.Response response =
          Dashboard.request(
              cluster.getRestAddress().get(1, TimeUnit.MINUTES),
              method,
              target,
              body == null || body.isEmpty() ? null : body.getBytes("UTF-8"));
      return Json.object()
          .field("ok", true)
          .field("status", response.status)
          .field("contentType", response.contentType)
          .field("body", response.base64())
          .end();
    } catch (Throwable t) {
      return failure(t, 0L);
    }
  }

  /** Cancels the job, leaving the cluster up so another one can be submitted. */
  public static synchronized String cancelJob() {
    try {
      if (cluster == null || jobId == null) {
        return error("no job is running");
      }
      cluster.cancelJob(jobId).get(2, TimeUnit.MINUTES);
      JobStatus status = cluster.getJobStatus(jobId).get(2, TimeUnit.MINUTES);
      Feed.note("job", "cancelled: " + status);
      jobId = null;
      Feed.RATE.set(0);
      return Json.object().field("ok", true).field("state", status.name()).end();
    } catch (Throwable t) {
      jobId = null;
      return failure(t, 0L);
    }
  }

  /** The ExecutionGraph as the page draws it: vertices, their subtasks and each subtask's state. */
  private static String graph(AccessExecutionGraph graph) {
    List<String> vertices = new ArrayList<String>();
    for (AccessExecutionJobVertex vertex : graph.getVerticesTopologically()) {
      List<String> subtasks = new ArrayList<String>();
      for (AccessExecutionVertex subtask : vertex.getTaskVertices()) {
        AccessExecution execution = subtask.getCurrentExecutionAttempt();
        subtasks.add(
            Json.object()
                .field("index", execution.getParallelSubtaskIndex())
                .field("attempt", execution.getAttemptNumber())
                .field("state", execution.getState().name())
                .field(
                    "startedAt",
                    execution.getStateTimestamp(org.apache.flink.runtime.execution.ExecutionState.DEPLOYING))
                .field(
                    "failure",
                    execution.getFailureInfo().isPresent()
                        ? execution.getFailureInfo().get().getExceptionAsString()
                        : null)
                .end());
      }
      vertices.add(
          Json.object()
              .field("id", vertex.getJobVertexId().toString())
              .field("name", vertex.getName())
              .field("parallelism", vertex.getParallelism())
              .field("state", vertex.getAggregateState().name())
              .raw("subtasks", Json.array(subtasks))
              .end());
    }
    return Json.array(vertices);
  }

  /** What the checkpoint coordinator has done so far, from the same stats the dashboard shows. */
  private static String checkpoints(CheckpointStatsSnapshot snapshot) {
    if (snapshot == null) {
      return "null";
    }
    CheckpointStatsCounts counts = snapshot.getCounts();
    Json out =
        Json.object()
            .field("completed", counts.getNumberOfCompletedCheckpoints())
            .field("failed", counts.getNumberOfFailedCheckpoints())
            .field("inProgress", counts.getNumberOfInProgressCheckpoints())
            .field("total", counts.getTotalNumberOfCheckpoints());
    CompletedCheckpointStats latest = snapshot.getHistory().getLatestCompletedCheckpoint();
    if (latest != null) {
      out.raw("latest", checkpoint(latest));
    }
    List<String> history = new ArrayList<String>();
    for (AbstractCheckpointStats stats : snapshot.getHistory().getCheckpoints()) {
      history.add(checkpoint(stats));
      if (history.size() >= 10) {
        break;
      }
    }
    return out.raw("history", Json.array(history)).end();
  }

  private static String checkpoint(AbstractCheckpointStats stats) {
    return Json.object()
        .field("id", stats.getCheckpointId())
        .field("status", stats.getStatus().name())
        .field("triggerTimestamp", stats.getTriggerTimestamp())
        .field("durationMs", stats.getEndToEndDuration())
        .field("stateSizeBytes", stats.getStateSize())
        .field("acknowledged", stats.getNumberOfAcknowledgedSubtasks())
        .field("subtasks", stats.getNumberOfSubtasks())
        .field(
            "path",
            stats instanceof CompletedCheckpointStats
                ? ((CompletedCheckpointStats) stats).getExternalPath()
                : null)
        .end();
  }

  private static String notes() {
    List<String> notes = new ArrayList<String>();
    for (String note : Feed.drainNotes()) {
      String[] parts = note.split("\u0000", 3);
      notes.add(
          Json.object()
              .field("kind", parts[0])
              .field("detail", parts.length > 1 ? parts[1] : "")
              .field("at", parts.length > 2 ? Long.parseLong(parts[2]) : 0L)
              .end());
    }
    return Json.array(notes);
  }

  /** The cluster as the page reports it in its header. */
  private static String cluster(long t0) throws Exception {
    return Json.object()
        .field("ok", true)
        .field("flinkVersion", org.apache.flink.runtime.util.EnvironmentInformation.getVersion())
        .field("commit", org.apache.flink.runtime.util.EnvironmentInformation.getRevisionInformation().commitId)
        .field("javaVersion", System.getProperty("java.version"))
        .field("jvm", System.getProperty("java.vm.name"))
        .field("cores", Runtime.getRuntime().availableProcessors())
        .field("slots", clusterConfiguration.get(TaskManagerOptions.NUM_TASK_SLOTS))
        .field("taskManagers", 1)
        .field("stateBackend", "hashmap")
        .field("checkpointDir", workDir + "/checkpoints")
        .field("restAddress", cluster.getRestAddress().get(1, TimeUnit.MINUTES).toString())
        .field("bootMs", System.currentTimeMillis() - t0)
        .end();
  }

  private static String error(String message) {
    return Json.object().field("ok", false).field("error", message).end();
  }

  private static String failure(Throwable t, long durationMs) {
    return Json.object()
        .field("ok", false)
        .field("error", String.valueOf(t.getMessage()))
        .field("trace", Json.trace(t))
        .field("durationMs", durationMs)
        .end();
  }
}
