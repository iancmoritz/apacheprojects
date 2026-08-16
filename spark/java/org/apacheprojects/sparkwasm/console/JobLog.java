/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apacheprojects.sparkwasm.console;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.executor.TaskMetrics;

/**
 * Collects the stage and task facts the page shows next to each result.
 *
 * <p>Spark's own UI is off (its Jetty server cannot bind a socket in the tab), so the scheduler's
 * event bus is where the stage breakdown comes from instead.
 */
public final class JobLog extends SparkListener {

  /** One completed stage, in the order the scheduler finished them. */
  public static final class Stage {
    final int id;
    final String name;
    final int tasks;
    final long durationMs;
    long recordsRead;
    long recordsWritten;
    long shuffleWriteBytes;
    long shuffleReadBytes;
    long executorRunTimeMs;
    long peakMemoryBytes;

    Stage(int id, String name, int tasks, long durationMs) {
      this.id = id;
      this.name = name;
      this.tasks = tasks;
      this.durationMs = durationMs;
    }
  }

  private final List<Stage> stages = new ArrayList<Stage>();

  /** Forgets everything, so the next statement reports only its own stages. */
  public synchronized void reset() {
    stages.clear();
  }

  @Override
  public synchronized void onStageCompleted(SparkListenerStageCompleted event) {
    StageInfo info = event.stageInfo();
    long submitted = info.submissionTime().isDefined() ? (Long) info.submissionTime().get() : 0L;
    long completed = info.completionTime().isDefined() ? (Long) info.completionTime().get() : 0L;
    Stage stage =
        new Stage(
            info.stageId(),
            info.name(),
            info.numTasks(),
            submitted == 0 || completed == 0 ? 0 : completed - submitted);
    TaskMetrics metrics = info.taskMetrics();
    if (metrics != null) {
      stage.recordsRead = metrics.inputMetrics().recordsRead();
      stage.recordsWritten = metrics.outputMetrics().recordsWritten();
      stage.shuffleWriteBytes = metrics.shuffleWriteMetrics().bytesWritten();
      stage.shuffleReadBytes = metrics.shuffleReadMetrics().totalBytesRead();
      stage.executorRunTimeMs = metrics.executorRunTime();
      stage.peakMemoryBytes = metrics.peakExecutionMemory();
    }
    stages.add(stage);
  }

  @Override
  public synchronized void onTaskEnd(SparkListenerTaskEnd event) {
    if (stages.isEmpty() || event.taskMetrics() == null) {
      return;
    }
    Stage stage = stages.get(stages.size() - 1);
    if (stage.id == event.stageId() && stage.recordsRead == 0) {
      stage.recordsRead = event.taskMetrics().inputMetrics().recordsRead();
    }
  }

  /** The stages of the statement that just ran, as a JSON array. */
  synchronized String toJson() {
    StringBuilder array = new StringBuilder("[");
    for (Stage stage : stages) {
      if (array.length() > 1) {
        array.append(',');
      }
      array.append(
          Json.object()
              .field("id", stage.id)
              .field("name", stage.name)
              .field("tasks", stage.tasks)
              .field("durationMs", stage.durationMs)
              .field("executorRunTimeMs", stage.executorRunTimeMs)
              .field("recordsRead", stage.recordsRead)
              .field("recordsWritten", stage.recordsWritten)
              .field("shuffleWriteBytes", stage.shuffleWriteBytes)
              .field("shuffleReadBytes", stage.shuffleReadBytes)
              .field("peakMemoryBytes", stage.peakMemoryBytes)
              .end());
    }
    return array.append(']').toString();
  }
}
