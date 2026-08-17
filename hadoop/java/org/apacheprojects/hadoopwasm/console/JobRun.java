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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A submitted job, and what Hadoop says about it while it runs.
 *
 * <p>The page polls {@link #poll()}; every field it renders is read out of the {@link Job} handle --
 * {@code mapProgress}, {@code reduceProgress}, the {@link JobStatus} state, the {@link Counters} and,
 * once the job is done, the output the reduce task wrote into HDFS.
 */
final class JobRun {

  /** How many lines of the reduce output the page gets. */
  private static final int MAX_ROWS = 2000;

  private final Job job;

  private final List<InputSplit> splits;

  private final Path output;

  private final long startedAt = System.currentTimeMillis();

  private String finalOutput;

  private long finishedAt;

  JobRun(Job job, List<InputSplit> splits, Path output) {
    this.job = job;
    this.splits = splits;
    this.output = output;
  }

  /** A progress fraction as a percentage with one decimal, so the page can render it as a number. */
  private static String percent(float progress) {
    return String.valueOf(Math.round(progress * 1000) / 10.0);
  }

  boolean finished() {
    try {
      return job.isComplete();
    } catch (Exception e) {
      return true;
    }
  }

  /** The job id and the splits Hadoop computed from the NameNode's block locations. */
  String submitted() throws Exception {
    StringBuilder list = new StringBuilder("[");
    for (InputSplit split : splits) {
      if (list.length() > 1) {
        list.append(',');
      }
      FileSplit file = split instanceof FileSplit ? (FileSplit) split : null;
      list.append(
          Json.object()
              .field("path", file == null ? split.toString() : file.getPath().toUri().getPath())
              .field("start", file == null ? 0 : file.getStart())
              .field("length", split.getLength())
              .raw("hosts", Json.strings(java.util.Arrays.asList(split.getLocations())))
              .field("type", split.getClass().getName())
              .end());
    }
    list.append(']');
    return Json.object()
        .field("ok", true)
        .field("jobId", job.getJobID().toString())
        .field("jobName", job.getJobName())
        .field("trackingUrl", String.valueOf(job.getTrackingURL()))
        .raw("splits", list.toString())
        .end();
  }

  String poll() throws Exception {
    boolean complete = job.isComplete();
    JobStatus status = job.getStatus();
    if (complete && finishedAt == 0) {
      finishedAt = System.currentTimeMillis();
    }
    Json json =
        Json.object()
            .field("ok", true)
            .field("jobId", job.getJobID().toString())
            .field("complete", complete)
            .field("successful", complete && job.isSuccessful())
            .field("state", String.valueOf(status.getState()))
            .raw("mapProgress", percent(job.mapProgress()))
            .raw("reduceProgress", percent(job.reduceProgress()))
            .raw("setupProgress", percent(job.setupProgress()))
            .field("elapsedMs", (finishedAt == 0 ? System.currentTimeMillis() : finishedAt) - startedAt)
            .raw("counters", counters())
            .raw("tasks", tasks());
    if (complete) {
      json.field("failure", status.getFailureInfo() == null ? null : status.getFailureInfo());
      json.raw("output", output());
    }
    return json.end();
  }

  /** Hadoop's own counters, group by group, exactly as {@code job.getCounters()} reports them. */
  private String counters() {
    StringBuilder groups = new StringBuilder("[");
    Counters counters;
    try {
      counters = job.getCounters();
    } catch (Exception e) {
      return "[]";
    }
    if (counters == null) {
      return "[]";
    }
    for (CounterGroup group : counters) {
      StringBuilder entries = new StringBuilder("[");
      for (Counter counter : group) {
        if (entries.length() > 1) {
          entries.append(',');
        }
        entries.append(
            Json.object()
                .field("name", counter.getName())
                .field("display", counter.getDisplayName())
                .field("value", counter.getValue())
                .end());
      }
      entries.append(']');
      if (groups.length() > 1) {
        groups.append(',');
      }
      groups.append(
          Json.object()
              .field("name", group.getDisplayName())
              .field("id", group.getName())
              .raw("counters", entries.toString())
              .end());
    }
    return groups.append(']').toString();
  }

  /**
   * The map and reduce task reports.
   *
   * <p>{@code LocalJobRunner} answers this out of its own task tracking, and returns nothing at all
   * for a job that has not started a task yet, so an empty list here is Hadoop's answer rather than a
   * missing feature.
   */
  private String tasks() {
    StringBuilder list = new StringBuilder("[");
    for (TaskType type : new TaskType[] {TaskType.MAP, TaskType.REDUCE}) {
      TaskReport[] reports;
      try {
        reports = job.getTaskReports(type);
      } catch (Exception e) {
        continue;
      }
      for (TaskReport report : reports) {
        if (list.length() > 1) {
          list.append(',');
        }
        list.append(
            Json.object()
                .field("type", type.name())
                .field("id", report.getTaskId())
                .field("state", report.getState())
                .raw("progress", percent(report.getProgress()))
                .field("startTime", report.getStartTime())
                .field("finishTime", report.getFinishTime())
                .end());
      }
    }
    return list.append(']').toString();
  }

  /** The part files the reduce task wrote, read back out of HDFS. */
  private String output() {
    if (finalOutput != null) {
      return finalOutput;
    }
    FileSystem fs = HadoopConsole.fs();
    StringBuilder files = new StringBuilder("[");
    StringBuilder rows = new StringBuilder("[");
    int count = 0;
    try {
      for (FileStatus status : fs.listStatus(output)) {
        if (files.length() > 1) {
          files.append(',');
        }
        files.append(
            Json.object()
                .field("path", status.getPath().toUri().getPath())
                .field("bytes", status.getLen())
                .end());
        if (!status.getPath().getName().startsWith("part-")) {
          continue;
        }
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(fs.open(status.getPath()), "UTF-8"));
        try {
          String line;
          while ((line = reader.readLine()) != null && count < MAX_ROWS) {
            int tab = line.lastIndexOf('\t');
            if (tab < 0) {
              continue;
            }
            if (rows.length() > 1) {
              rows.append(',');
            }
            rows.append(
                Json.object()
                    .field("word", line.substring(0, tab))
                    .field("count", Long.parseLong(line.substring(tab + 1).trim()))
                    .end());
            count++;
          }
        } finally {
          reader.close();
        }
      }
    } catch (Exception e) {
      return Json.object().field("error", String.valueOf(e)).end();
    }
    finalOutput =
        Json.object()
            .raw("files", files.append(']').toString())
            .raw("rows", rows.append(']').toString())
            .field("rowCount", count)
            .field("path", output.toUri().getPath())
            .end();
    return finalOutput;
  }
}
