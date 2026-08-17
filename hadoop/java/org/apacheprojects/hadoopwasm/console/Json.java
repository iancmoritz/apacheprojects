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

package org.apacheprojects.hadoopwasm.console;

/**
 * The smallest JSON writer that will do.
 *
 * <p>The console hands its results to the page as a JSON string because that is the one type CheerpJ
 * converts between Java and JavaScript for free; pulling in a JSON library would mean another jar in
 * the classpath the browser has to fetch.
 */
final class Json {

  private final StringBuilder out = new StringBuilder();

  private Json() {}

  static Json object() {
    Json json = new Json();
    json.out.append('{');
    return json;
  }

  Json field(String name, String value) {
    separate();
    string(name).out.append(':');
    if (value == null) {
      out.append("null");
    } else {
      string(value);
    }
    return this;
  }

  Json field(String name, long value) {
    separate();
    string(name).out.append(':').append(value);
    return this;
  }

  Json field(String name, boolean value) {
    separate();
    string(name).out.append(':').append(value);
    return this;
  }

  /** Adds a field whose value is already JSON (an object or array built elsewhere). */
  Json raw(String name, String json) {
    separate();
    string(name).out.append(':').append(json);
    return this;
  }

  String end() {
    return out.append('}').toString();
  }

  /** Renders {@code values} as a JSON array of strings, nulls included. */
  static String strings(Iterable<String> values) {
    StringBuilder array = new StringBuilder("[");
    for (String value : values) {
      if (array.length() > 1) {
        array.append(',');
      }
      if (value == null) {
        array.append("null");
      } else {
        array.append(quote(value));
      }
    }
    return array.append(']').toString();
  }

  static String quote(String value) {
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
