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

package org.apacheprojects.hadoopwasm.net;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * Writes what the virtual network is doing to a file, for the runs that end with a frozen tab.
 *
 * <p>A frozen tab keeps its console output: the browser never delivers it, and devtools never opens.
 * It does not keep it from CheerpJ's filesystem, which is IndexedDB and survives the reload -- so
 * when the question is "which side of which socket stopped", the answer has to be written down as it
 * happens. Off unless {@code hadoopwasm.trace} is set, because every line is a flushed write.
 */
public final class Trace {

  private static final boolean ON = Boolean.getBoolean("hadoopwasm.trace");

  private static final File FILE =
      new File(System.getProperty("hadoopwasm.storage", "/files/hadoop"), "trace.log");

  private static long started = System.currentTimeMillis();

  /** When each repeating line was last written, so a wait that repeats every 250ms does not fill it. */
  private static final Map<String, Long> lastSaid = new HashMap<>();

  private static final long REPEAT_MILLIS = 5000L;

  private static final long STALLED_MILLIS = 3000L;

  private Trace() {}

  public static boolean on() {
    return ON;
  }

  /** Writes one line from outside this package, so a probe can check the trace is really written. */
  public static void probe(String line) {
    say(line);
  }

  /**
   * Records a wait only once it has lasted {@link #STALLED_MILLIS}, then every {@link
   * #REPEAT_MILLIS}.
   *
   * <p>Every line costs an open, a write, an fsync and a close, so tracing each of the four-times-a-
   * second wakeups of every Hadoop daemon thread slows the tab down by more than the tracing is worth
   * -- and a wait that ends quickly is not what anyone is looking for. A wait that never ends is.
   *
   * @param since when this call started waiting
   */
  static void stalled(long since, String line) {
    if (!ON) return;
    if (System.currentTimeMillis() - since < STALLED_MILLIS) return;
    waiting(line);
  }

  /** Appends a line that repeats at most every {@link #REPEAT_MILLIS}. */
  static synchronized void waiting(String line) {
    if (!ON) return;
    long now = System.currentTimeMillis();
    String key = Thread.currentThread().getName() + ' ' + line;
    Long last = lastSaid.get(key);
    if (last != null && now - last < REPEAT_MILLIS) return;
    lastSaid.put(key, now);
    say(line);
  }

  /**
   * Writes one line, thread and elapsed time included, and closes the file again.
   *
   * <p>Opened and closed per line on purpose. CheerpJ only commits a file to IndexedDB when it is
   * closed, so a stream held open by a tab that then freezes -- the only tab whose trace anyone wants
   * -- writes nothing that survives the reload. The seek is there because {@code
   * FileOutputStream(file, true)} does not append under CheerpJ: it starts at the beginning and
   * overwrites what is already there (see Probe).
   */
  static synchronized void say(String line) {
    if (!ON) return;
    try {
      FILE.getParentFile().mkdirs();
      String text =
          (System.currentTimeMillis() - started)
              + "ms ["
              + Thread.currentThread().getName()
              + "] "
              + line
              + "\n";
      try (RandomAccessFile file = new RandomAccessFile(FILE, "rw")) {
        file.seek(file.length());
        file.write(text.getBytes("UTF-8"));
        file.getFD().sync();
      }
    } catch (IOException e) {
      // Tracing that throws is worse than tracing that stops.
    }
  }

  /**
   * The trace of the run before this one, or empty.
   *
   * <p>Read to the end of the stream rather than to {@code File.length()}: CheerpJ reports the length
   * a file had when it was opened, so a file this JVM has been appending to measures 0 bytes and
   * sizing a buffer from it reads nothing at all.
   */
  public static String read() {
    if (!FILE.isFile()) return "";
    java.io.ByteArrayOutputStream all = new java.io.ByteArrayOutputStream();
    byte[] chunk = new byte[64 * 1024];
    try (java.io.InputStream in = new java.io.FileInputStream(FILE)) {
      for (int n = in.read(chunk); n > 0; n = in.read(chunk)) {
        all.write(chunk, 0, n);
      }
      return new String(all.toByteArray(), "UTF-8");
    } catch (IOException e) {
      return "trace could not be read: " + e;
    }
  }

  public static void forget() {
    FILE.delete();
  }
}
