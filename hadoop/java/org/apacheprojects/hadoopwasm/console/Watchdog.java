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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Records what the JVM is waiting on when a call takes too long.
 *
 * <p>A tab has one thread of execution, and CheerpJ's threads are cooperative: a Hadoop thread that
 * waits forever looks exactly like a frozen browser -- no console, no devtools, no way to ask. This
 * daemon is the way to ask. Past {@link #PATIENCE} it dumps every thread's stack to {@code
 * System.out}, which the page shows in its JVM log, and appends it to {@link #LOG} in CheerpJ's
 * persistent filesystem, which is the copy that survives: a tab frozen hard enough never delivers
 * its console output at all, and the dump is there to be read after a reload ({@link #log()}).
 */
final class Watchdog {

  /** How long a call may run before the dump; long enough that ordinary HDFS work never trips it. */
  private static final long PATIENCE = Long.getLong("hadoopwasm.watchdog.ms", 60_000L);

  private static volatile String call;

  private static volatile long since;

  private static volatile long dumped;

  private static boolean started;

  /** Where dumps are kept, so a reload can read what froze the last one. */
  private static final File LOG = new File(HadoopConsole.STORAGE, "watchdog.log");

  private Watchdog() {}

  /** Records that {@code what} is running, starting the watchdog on the first call. */
  static synchronized void enter(String what) {
    call = what;
    since = System.currentTimeMillis();
    if (!started) {
      started = true;
      Thread thread = new Thread(Watchdog::watch, "hadoopwasm-watchdog");
      thread.setDaemon(true);
      thread.start();
    }
  }

  static void exit() {
    call = null;
  }

  private static void watch() {
    for (; ; ) {
      try {
        Thread.sleep(10_000L);
      } catch (InterruptedException e) {
        return;
      }
      String running = call;
      long started = since;
      if (running == null || System.currentTimeMillis() - started < PATIENCE) {
        continue;
      }
      if (System.currentTimeMillis() - dumped < PATIENCE) {
        continue;
      }
      dumped = System.currentTimeMillis();
      dump(running, System.currentTimeMillis() - started);
    }
  }

  private static void dump(String running, long elapsed) {
    StringBuilder text = new StringBuilder();
    text.append("WATCHDOG ").append(running).append(" has been running for ").append(elapsed / 1000);
    text.append("s; thread dump follows\n");
    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
      Thread thread = entry.getKey();
      text.append('"').append(thread.getName()).append("\" ").append(thread.getState()).append('\n');
      StackTraceElement[] frames = entry.getValue();
      for (int i = 0; i < frames.length && i < 32; i++) {
        text.append("\tat ").append(frames[i]).append('\n');
      }
    }
    System.out.println(text);
    append(text.toString());
  }

  /**
   * Adds one dump to the log, seeking to the end itself.
   *
   * <p>{@code FileOutputStream(file, true)} does not append under CheerpJ -- it overwrites from the
   * beginning of the file (see Probe) -- so a second dump would erase the first.
   */
  private static void append(String text) {
    LOG.getParentFile().mkdirs();
    try (RandomAccessFile file = new RandomAccessFile(LOG, "rw")) {
      file.seek(file.length());
      file.write(text.getBytes(Charset.forName("UTF-8")));
      file.getFD().sync();
    } catch (IOException e) {
      System.out.println("WATCHDOG could not be written to " + LOG + ": " + e);
    }
  }

  /**
   * Every dump this browser has recorded, most recent last; empty when nothing ever hung.
   *
   * <p>Read to the end of the stream, not to {@code File.length()}: CheerpJ reports the length the
   * file had when it was opened, which for a file this JVM appended to is zero.
   */
  static String log() {
    if (!LOG.isFile()) return "";
    java.io.ByteArrayOutputStream all = new java.io.ByteArrayOutputStream();
    byte[] chunk = new byte[64 * 1024];
    try (java.io.InputStream in = new java.io.FileInputStream(LOG)) {
      for (int n = in.read(chunk); n > 0; n = in.read(chunk)) {
        all.write(chunk, 0, n);
      }
      return new String(all.toByteArray(), Charset.forName("UTF-8"));
    } catch (IOException e) {
      return "WATCHDOG log could not be read: " + e;
    }
  }

  static void forget() {
    LOG.delete();
  }
}
