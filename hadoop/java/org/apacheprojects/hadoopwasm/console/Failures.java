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

/**
 * Failures, as the page shows them.
 *
 * <p>The whole stack trace goes to the browser, causes included: when something in here breaks it is
 * usually Hadoop meeting a platform limit, and the frame it broke in is the interesting part.
 */
final class Failures {

  private Failures() {}

  static String json(Throwable t, long durationMs) {
    StringBuilder trace = new StringBuilder(t.getClass().getName() + ": " + t.getMessage());
    for (StackTraceElement frame : t.getStackTrace()) {
      trace.append("\n    at ").append(frame);
    }
    for (Throwable cause = t.getCause(); cause != null; cause = cause.getCause()) {
      trace.append("\n  caused by ").append(cause.getClass().getName()).append(": ");
      trace.append(cause.getMessage());
      for (StackTraceElement frame : cause.getStackTrace()) {
        trace.append("\n    at ").append(frame);
      }
    }
    return Json.object()
        .field("ok", false)
        .field("error", t.getClass().getName() + ": " + t.getMessage())
        .field("trace", trace.toString())
        .field("durationMs", durationMs)
        .end();
  }

  static String message(String error) {
    return Json.object().field("ok", false).field("error", error).end();
  }
}
