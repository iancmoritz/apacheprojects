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

import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apacheprojects.flinkwasm.console.Json;

/**
 * The streaming job the page runs: a windowed word count over what the visitor types.
 *
 * <p>It is an ordinary DataStream program -- a source, a watermark strategy, a {@code keyBy}, a
 * tumbling event-time window with an aggregate and a side output for late data, and a sink -- built
 * here and handed to the {@link org.apache.flink.runtime.minicluster.MiniCluster} in the tab as a
 * {@code JobGraph}. Every element really goes through Flink's network stack (the {@code keyBy} is a
 * shuffle), its watermark generator, its window operator and its keyed state backend.
 *
 * <p>The source and the sink are the only unusual part: instead of a socket or a file they read and
 * write {@link Feed}, static queues in this JVM that JavaScript pushes into and polls.
 */
public final class WordCountJob {

  /** Late elements go here instead of being dropped, so the page can show them. */
  public static final OutputTag<Tuple2<String, Long>> LATE =
      new OutputTag<Tuple2<String, Long>>("late-events", Types.TUPLE(Types.STRING, Types.LONG));

  /** (window start, window end, word, count) -- what a finished window produces. */
  private static final TypeInformation<Tuple4<Long, Long, String, Long>> ROW =
      Types.TUPLE(Types.LONG, Types.LONG, Types.STRING, Types.LONG);

  private WordCountJob() {}

  /** How the visitor configured the job before it was submitted. */
  public static final class Options {
    public final int windowSeconds;
    public final int lateness;
    public final int outOfOrderness;
    public final int checkpointInterval;
    public final int parallelism;

    public Options(
        int windowSeconds,
        int lateness,
        int outOfOrderness,
        int checkpointInterval,
        int parallelism) {
      this.windowSeconds = windowSeconds;
      this.lateness = lateness;
      this.outOfOrderness = outOfOrderness;
      this.checkpointInterval = checkpointInterval;
      this.parallelism = parallelism;
    }
  }

  /** Defines the job on {@code env}; the caller turns the transformations into a job graph. */
  public static void define(StreamExecutionEnvironment env, Options options) {
    DataStream<Tuple2<String, Long>> events =
        env.addSource(new LiveSource(), "typed and generated events", Types.TUPLE(Types.STRING, Types.LONG))
            .setParallelism(1)
            .assignTimestampsAndWatermarks(new WallClockWatermarks(options.outOfOrderness))
            .name("watermarks")
            .setParallelism(1);

    SingleOutputStreamOperator<Tuple4<Long, Long, String, Long>> counted =
        events
            .keyBy(new WordKey())
            .process(new SeenSoFar(), Types.TUPLE(Types.STRING, Types.LONG))
            .name("count keyed state")
            .setParallelism(options.parallelism)
            .keyBy(new WordKey())
            .window(TumblingEventTimeWindows.of(Time.seconds(options.windowSeconds)))
            .allowedLateness(Time.seconds(options.lateness))
            .sideOutputLateData(LATE)
            .aggregate(new CountWords(), new WindowRow(), Types.LONG, Types.LONG, ROW)
            .name("tumbling window " + options.windowSeconds + "s")
            .setParallelism(options.parallelism);

    counted.addSink(new WindowSink()).name("page sink").setParallelism(1);
    counted.getSideOutput(LATE).addSink(new LateSink()).name("late events").setParallelism(1);
  }

  /**
   * Reads the events the page queued, and synthesizes them when the generator is turned on.
   *
   * <p>{@link SourceFunction} is Flink's legacy source interface, and the simplest thing that can
   * block on a queue: the new {@code Source} API wants a split enumerator and a reader, neither of
   * which has anything to enumerate here.
   */
  private static final class LiveSource implements SourceFunction<Tuple2<String, Long>> {

    private static final long serialVersionUID = 1L;

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
      long nextGenerated = System.currentTimeMillis();
      while (running) {
        int rate = Feed.RATE.get();
        long now = System.currentTimeMillis();
        if (rate > 0) {
          long interval = Math.max(1L, 1000L / rate);
          while (nextGenerated <= now) {
            Feed.generate(now);
            nextGenerated += interval;
          }
        } else {
          nextGenerated = now;
        }
        Feed.Event event = Feed.poll(rate > 0 ? 5L : 50L);
        if (event == null) {
          continue;
        }
        // The checkpoint lock is how a legacy source and the checkpoint barrier take turns; without
        // it a barrier could be injected halfway through an emit.
        synchronized (ctx.getCheckpointLock()) {
          ctx.collect(Tuple2.of(event.word, event.timestamp));
        }
        Feed.INGESTED.incrementAndGet();
        long max = Feed.MAX_EVENT_TIME.get();
        if (event.timestamp > max) {
          Feed.MAX_EVENT_TIME.set(event.timestamp);
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  /**
   * Bounded-out-of-orderness watermarks that keep moving while nobody is typing.
   *
   * <p>{@link WatermarkStrategy#forBoundedOutOfOrderness} derives the watermark from the elements it
   * has seen, so a visitor who types five words and stops leaves the watermark a second behind the
   * last word forever, and the window holding those words never closes. The events are stamped from
   * the same clock this generator reads, so once wall-clock time has passed a window's end no on-time
   * element can still arrive for it: advancing on the clock as well as on elements closes the window
   * the visitor is waiting for without inventing anything the stream did not say.
   */
  private static final class WallClockWatermarks implements WatermarkStrategy<Tuple2<String, Long>> {

    private static final long serialVersionUID = 1L;

    private final long outOfOrdernessMillis;

    WallClockWatermarks(int outOfOrdernessSeconds) {
      this.outOfOrdernessMillis = Duration.ofSeconds(outOfOrdernessSeconds).toMillis();
    }

    @Override
    public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(
        WatermarkGeneratorSupplier.Context context) {
      return new ClockOrElements(outOfOrdernessMillis);
    }

    @Override
    public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(
        TimestampAssignerSupplier.Context context) {
      return new EventTimestamps();
    }
  }

  /** The generator behind {@link WallClockWatermarks}. */
  private static final class ClockOrElements implements WatermarkGenerator<Tuple2<String, Long>> {

    private final long outOfOrdernessMillis;
    private long maxTimestamp = Long.MIN_VALUE;
    private long emitted = Long.MIN_VALUE;

    ClockOrElements(long outOfOrdernessMillis) {
      this.outOfOrdernessMillis = outOfOrdernessMillis;
    }

    @Override
    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
      maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      long ahead = Math.max(maxTimestamp, System.currentTimeMillis());
      long watermark = ahead - outOfOrdernessMillis - 1;
      if (watermark > emitted) {
        emitted = watermark;
        output.emitWatermark(new Watermark(watermark));
      }
    }
  }

  /** The event time of an element is the timestamp the page (or the generator) gave it. */
  private static final class EventTimestamps
      implements SerializableTimestampAssigner<Tuple2<String, Long>> {

    private static final long serialVersionUID = 1L;

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
      return element.f1;
    }
  }

  private static final class WordKey implements KeySelector<Tuple2<String, Long>, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String getKey(Tuple2<String, Long> value) {
      return value.f0;
    }
  }

  /**
   * Keyed state between the shuffle and the window: a running total per word.
   *
   * <p>It is here to put something in the state backend that is not the window's own contents, so a
   * checkpoint has keyed state to write and the page has a watermark to read: the timer service this
   * function sees is the operator's, so {@code currentWatermark} is the real one.
   */
  private static final class SeenSoFar
      extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<Long> total;

    @Override
    public void open(Configuration parameters) {
      total = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total", Types.LONG));
    }

    @Override
    public void processElement(
        Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out)
        throws Exception {
      Long seen = total.value();
      total.update(seen == null ? 1L : seen + 1L);
      long watermark = ctx.timerService().currentWatermark();
      Feed.WATERMARK.set(watermark);
      out.collect(value);
    }
  }

  private static final class CountWords
      implements AggregateFunction<Tuple2<String, Long>, Long, Long> {

    private static final long serialVersionUID = 1L;

    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long add(Tuple2<String, Long> value, Long accumulator) {
      return accumulator + 1L;
    }

    @Override
    public Long getResult(Long accumulator) {
      return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
      return a + b;
    }
  }

  /** Turns an aggregate into a row with the window it belongs to. */
  private static final class WindowRow
      extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
          Long, Tuple4<Long, Long, String, Long>, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(
        String key, Context context, Iterable<Long> counts, Collector<Tuple4<Long, Long, String, Long>> out) {
      long count = 0L;
      for (Long value : counts) {
        count += value;
      }
      TimeWindow window = context.window();
      out.collect(Tuple4.of(window.getStart(), window.getEnd(), key, count));
    }
  }

  private static final class WindowSink implements SinkFunction<Tuple4<Long, Long, String, Long>> {

    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(Tuple4<Long, Long, String, Long> row, Context context) {
      Feed.window(
          Json.object()
              .field("windowStart", row.f0)
              .field("windowEnd", row.f1)
              .field("word", row.f2)
              .field("count", row.f3)
              .field("emittedAt", System.currentTimeMillis())
              .end());
      Feed.EMITTED.incrementAndGet();
    }
  }

  private static final class LateSink implements SinkFunction<Tuple2<String, Long>> {

    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(Tuple2<String, Long> element, Context context) {
      Feed.LATE.incrementAndGet();
      Feed.note("late", element.f0 + " at " + element.f1);
    }
  }
}
