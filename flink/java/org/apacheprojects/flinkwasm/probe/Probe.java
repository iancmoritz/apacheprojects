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

package org.apacheprojects.flinkwasm.probe;

import org.apacheprojects.flinkwasm.console.FlinkConsole;

/**
 * Drives the whole browser-side console from a command line, on an ordinary JDK 8.
 *
 * <p>Iterating on the job inside a wasm JVM is a minute per attempt, so every change is made to work
 * here first -- same classes, same configuration, same calls in the same order the page makes them --
 * and only then in the browser. Whatever this prints is what the page's panels are drawn from.
 *
 * <p>It is a development-time tool: {@code scripts/build-runtime.mjs} leaves this package out of the
 * jar the browser downloads. To run it, compile {@code java/} against {@code public/_flink/jars/*} on
 * a JDK 8 and start this class with the same jars on the classpath.
 */
public final class Probe {

  private Probe() {}

  public static void main(String[] args) throws Exception {
    int seconds = args.length > 0 ? Integer.parseInt(args[0]) : 40;
    String workDir = args.length > 1 ? args[1] : System.getProperty("java.io.tmpdir") + "/flinkwasm";

    System.out.println("startCluster: " + FlinkConsole.startCluster(2, workDir));
    System.out.println("submitJob:    " + FlinkConsole.submitJob(5, 2, 1, 10, 1));
    System.out.println("push:         " + FlinkConsole.push("checkpoint watermark checkpoint", 0L));
    System.out.println("generator:    " + FlinkConsole.setGenerator(20, 10));

    for (int i = 0; i < seconds; i++) {
      Thread.sleep(1000L);
      System.out.println("poll " + i + ": " + FlinkConsole.poll());
      if (i == 5) {
        System.out.println("checkpointNow: " + FlinkConsole.checkpointNow());
      }
    }
    System.out.println("cancelJob:    " + FlinkConsole.cancelJob());
    System.exit(0);
  }
}
