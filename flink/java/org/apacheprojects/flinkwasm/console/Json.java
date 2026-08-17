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

/**
 * The smallest JSON writer that will do.
 *
 * <p>Everything the JVM sends the page is a JSON string, because a string is the one value CheerpJ
 * converts between Java and JavaScript for free. Flink ships Jackson, but shaded and only on the
 * paths that need it; writing the handful of shapes the page reads by hand keeps the browser from
 * loading a serializer for them.
 */
public final class Json {

  private final StringBuilder out = new StringBuilder();

  private Json() {}

  public static Json object() {
    Json json = new Json();
    json.out.append('{');
    return json;
  }

  public Json field(String name, String value) {
    separate();
    string(name).out.append(':');
    if (value == null) {
      out.append("null");
    } else {
      string(value);
    }
    return this;
  }

  public Json field(String name, long value) {
    separate();
    string(name).out.append(':').append(value);
    return this;
  }

  public Json field(String name, double value) {
    separate();
    string(name).out.append(':');
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      out.append("null");
    } else {
      out.append(value);
    }
    return this;
  }

  public Json field(String name, boolean value) {
    separate();
    string(name).out.append(':').append(value);
    return this;
  }

  /** Adds a field whose value is already JSON (an object or array built elsewhere). */
  public Json raw(String name, String json) {
    separate();
    string(name).out.append(':').append(json == null ? "null" : json);
    return this;
  }

  public String end() {
    return out.append('}').toString();
  }

  /** Renders already-built JSON values as an array. */
  public static String array(Iterable<String> values) {
    StringBuilder array = new StringBuilder("[");
    for (String value : values) {
      if (array.length() > 1) {
        array.append(',');
      }
      array.append(value == null ? "null" : value);
    }
    return array.append(']').toString();
  }

  /** Renders strings as a JSON array, nulls included. */
  public static String strings(Iterable<String> values) {
    StringBuilder array = new StringBuilder("[");
    for (String value : values) {
      if (array.length() > 1) {
        array.append(',');
      }
      array.append(value == null ? "null" : quote(value));
    }
    return array.append(']').toString();
  }

  public static String quote(String value) {
    StringBuilder quoted = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          quoted.append("\\\"");
          break;
        case '\\':
          quoted.append("\\\\");
          break;
        case '\n':
          quoted.append("\\n");
          break;
        case '\r':
          quoted.append("\\r");
          break;
        case '\t':
          quoted.append("\\t");
          break;
        default:
          if (c < 0x20) {
            quoted.append(String.format("\\u%04x", (int) c));
          } else {
            quoted.append(c);
          }
      }
    }
    return quoted.append('"').toString();
  }

  /** A throwable as the page's error panel wants it: message plus the frames, causes included. */
  public static String trace(Throwable t) {
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
      if (cause.getCause() == cause) {
        break;
      }
    }
    return trace.toString();
  }

  private void separate() {
    if (out.length() > 1) {
      out.append(',');
    }
  }

  private Json string(String value) {
    out.append(quote(value));
    return this;
  }
}
