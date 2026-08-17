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

package org.apacheprojects.flinkwasm.job;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The wire between the page and the running job.
 *
 * <p>The JobManager and the TaskManager are objects in the same JVM as this class, so the source
 * function reads the events the visitor types straight out of {@link #PENDING} and the sink writes
 * finished windows back into {@link #WINDOWS}. Nothing here is a Flink concept: it is the "external
 * system" the job is connected to, which in this deployment is a text box.
 *
 * <p>Every field is static because the task that runs the source is deserialized inside the
 * TaskManager from the bytes of the job graph -- an instance field would arrive as a copy, a static
 * one is the same object the page is pushing into.
 */
public final class Feed {

  /** One event on its way into the job: a word and the event time it happened at. */
  public static final class Event {
    public final String word;
    public final long timestamp;

    Event(String word, long timestamp) {
      this.word = word;
      this.timestamp = timestamp;
    }
  }

  /** Words the generator picks from: the vocabulary of Flink's own streaming glossary. */
  private static final String[] VOCABULARY = {
    "watermark", "checkpoint", "keyby", "window", "operator", "subtask", "barrier", "state",
    "savepoint", "backpressure", "sink", "source", "timer", "eventtime", "shuffle", "mailbox",
  };

  /** Bounded so a page pushing faster than the job drains cannot grow the heap without limit. */
  private static final BlockingQueue<Event> PENDING = new ArrayBlockingQueue<Event>(8192);

  /** Finished windows, as JSON rows the page renders; oldest dropped once it is full. */
  private static final Deque<String> WINDOWS = new ArrayDeque<String>();

  /** Notes the operators leave for the page: watermarks, late events, generator changes. */
  private static final Deque<String> NOTES = new ArrayDeque<String>();

  private static final int HISTORY = 500;

  /** Generator rate in events per second; 0 means only what the visitor types. */
  public static final AtomicInteger RATE = new AtomicInteger(0);

  /** Share of generated events emitted with an out-of-order timestamp, in percent. */
  public static final AtomicInteger SKEW_PERCENT = new AtomicInteger(0);

  public static final AtomicLong INGESTED = new AtomicLong();
  public static final AtomicLong EMITTED = new AtomicLong();
  public static final AtomicLong LATE = new AtomicLong();

  /** The watermark the window operator last saw, and the largest event time that reached it. */
  public static final AtomicLong WATERMARK = new AtomicLong(Long.MIN_VALUE);
  public static final AtomicLong MAX_EVENT_TIME = new AtomicLong(Long.MIN_VALUE);

  private static final Random RANDOM = new Random(20250816L);

  private Feed() {}

  /** Queues the words of {@code text} at {@code timestamp}; returns how many were queued. */
  public static int push(String text, long timestamp) {
    int queued = 0;
    for (String word : text.toLowerCase().split("[^\\p{L}\\p{N}_]+")) {
      if (word.isEmpty()) {
        continue;
      }
      if (PENDING.offer(new Event(word, timestamp))) {
        queued++;
      }
    }
    return queued;
  }

  /** Queues one synthesized event, the way the generator does. */
  static void generate(long now) {
    String word = VOCABULARY[RANDOM.nextInt(VOCABULARY.length)];
    int skew = SKEW_PERCENT.get();
    long timestamp = now;
    if (skew > 0 && RANDOM.nextInt(100) < skew) {
      // Deliberately behind the watermark often enough to make the late-event path visible.
      timestamp = now - 1000L - RANDOM.nextInt(20000);
    }
    PENDING.offer(new Event(word, timestamp));
  }

  /** The next event for the source, or null if none arrived within {@code millis}. */
  static Event poll(long millis) throws InterruptedException {
    return PENDING.poll(millis, TimeUnit.MILLISECONDS);
  }

  static int pending() {
    return PENDING.size();
  }

  static void window(String json) {
    add(WINDOWS, json);
  }

  /** Records a note for the page: {@code kind} is what the page groups them by. */
  public static void note(String kind, String detail) {
    add(NOTES, kind + "\u0000" + detail + "\u0000" + System.currentTimeMillis());
  }

  /** Drains the finished windows; the page asks for them once per poll. */
  public static List<String> drainWindows() {
    return drain(WINDOWS);
  }

  public static List<String> drainNotes() {
    return drain(NOTES);
  }

  /** Forgets everything queued, for a job that is being restarted. */
  public static void reset() {
    PENDING.clear();
    synchronized (WINDOWS) {
      WINDOWS.clear();
    }
    synchronized (NOTES) {
      NOTES.clear();
    }
    INGESTED.set(0);
    EMITTED.set(0);
    LATE.set(0);
    WATERMARK.set(Long.MIN_VALUE);
    MAX_EVENT_TIME.set(Long.MIN_VALUE);
  }

  private static void add(Deque<String> queue, String item) {
    synchronized (queue) {
      queue.addLast(item);
      while (queue.size() > HISTORY) {
        queue.removeFirst();
      }
    }
  }

  private static List<String> drain(Deque<String> queue) {
    synchronized (queue) {
      List<String> drained = new ArrayList<String>(queue);
      queue.clear();
      return drained;
    }
  }
}
