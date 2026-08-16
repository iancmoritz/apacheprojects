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

package org.apacheprojects.sparkwasm.console;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

/**
 * The Spark driver the page talks to.
 *
 * <p>Everything the browser needs is two static methods CheerpJ can call from JavaScript, {@link
 * #start} and {@link #query}, each returning a JSON string: strings are the one type that crosses the
 * JVM boundary unchanged, and Spark's own JSON support would mean loading the SQL writer path just to
 * describe a result.
 *
 * <p>The session is an ordinary local Spark driver -- scheduler, block manager, shuffle and Catalyst
 * all real -- with the pieces that need an operating system turned off: no Spark UI (Jetty cannot bind
 * a socket in a tab), warehouse and scratch directories in CheerpJ's writable {@code /files}, and
 * {@code SPARK_USER} set so Hadoop's login does not look for an OS identity.
 */
public final class SparkConsole {

  private static final int MAX_ROWS = 200;

  private static SparkSession spark;

  private static JobLog log;

  private SparkConsole() {}

  /**
   * Starts the driver and registers the bundled CSV as a temporary view.
   *
   * @param master the Spark master URL, normally {@code local[*]}
   * @param csvPath the CSV to register, as a path in CheerpJ's filesystem
   * @param view the name to register it under
   */
  public static synchronized String start(String master, String csvPath, String view) {
    long t0 = System.currentTimeMillis();
    try {
      if (spark != null) {
        return session(t0, view, csvPath);
      }
      identity();
      spark =
          SparkSession.builder()
              .master(master)
              .appName("apacheprojects-spark-wasm")
              .config("spark.ui.enabled", "false")
              .config("spark.driver.host", "127.0.0.1")
              .config("spark.driver.bindAddress", "127.0.0.1")
              .config("spark.sql.shuffle.partitions", "2")
              .config("spark.sql.warehouse.dir", "/files/warehouse")
              // Hadoop's checksummed local filesystem looks for a .crc beside every file it reads,
              // which over CheerpJ's HTTP-backed /app is a 404 per read; the raw one does not.
              .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
              // Wasm has no sendfile and no zero-copy transfer, and compression codecs are the
              // slowest thing in the tab; shuffle blocks here are kilobytes.
              .config("spark.file.transferTo", "false")
              .config("spark.shuffle.compress", "false")
              .config("spark.shuffle.spill.compress", "false")
              .config("spark.broadcast.compress", "false")
              .config("spark.rdd.compress", "false")
              .config("spark.buffer.pageSize", "1m")
              .config("spark.shuffle.file.buffer", "32k")
              // The driver is the executor and it cannot be lost; long timeouts keep the heartbeat
              // thread from failing a task while the tab is busy compiling generated code.
              .config("spark.executor.heartbeatInterval", "3600s")
              .config("spark.network.timeout", "7200s")
              .config("spark.log.level", "WARN")
              .getOrCreate();
      log = new JobLog();
      spark.sparkContext().addSparkListener(log);
      register(csvPath, view);
      return session(t0, view, csvPath);
    } catch (Throwable t) {
      return failure(t, System.currentTimeMillis() - t0);
    }
  }

  /** Runs one SQL statement and returns its rows, physical plan and stages. */
  public static synchronized String query(String sql) {
    long t0 = System.currentTimeMillis();
    if (spark == null) {
      return Json.object().field("ok", false).field("error", "the driver is not started").end();
    }
    try {
      log.reset();
      Dataset<Row> result = spark.sql(sql);
      List<String> columns = new ArrayList<String>();
      List<String> types = new ArrayList<String>();
      for (StructField field : result.schema().fields()) {
        columns.add(field.name());
        types.add(field.dataType().simpleString());
      }
      Row[] rows = (Row[]) result.head(MAX_ROWS + 1);
      boolean truncated = rows.length > MAX_ROWS;
      StringBuilder body = new StringBuilder("[");
      for (int i = 0; i < Math.min(rows.length, MAX_ROWS); i++) {
        if (i > 0) {
          body.append(',');
        }
        body.append(Json.strings(cells(rows[i])));
      }
      body.append(']');
      return Json.object()
          .field("ok", true)
          .raw("columns", Json.strings(columns))
          .raw("types", Json.strings(types))
          .raw("rows", body.toString())
          .field("truncated", truncated)
          .field("plan", plan(sql))
          .raw("stages", log.toJson())
          .field("durationMs", System.currentTimeMillis() - t0)
          .end();
    } catch (Throwable t) {
      return failure(t, System.currentTimeMillis() - t0);
    }
  }

  /** The physical plan Catalyst chose, as {@code EXPLAIN FORMATTED} prints it. */
  private static String plan(String sql) {
    try {
      Row[] explained = (Row[]) spark.sql("explain formatted " + sql).head(1);
      return explained.length == 0 ? null : explained[0].getString(0);
    } catch (Throwable t) {
      return "EXPLAIN failed: " + t;
    }
  }

  private static List<String> cells(Row row) {
    List<String> cells = new ArrayList<String>(row.length());
    for (int i = 0; i < row.length(); i++) {
      Object value = row.get(i);
      cells.add(value == null ? null : String.valueOf(value));
    }
    return cells;
  }

  private static void register(String csvPath, String view) {
    Dataset<Row> table =
        spark.read().option("header", "true").option("inferSchema", "true").csv(csvPath);
    table.createOrReplaceTempView(view);
  }

  private static String session(long t0, String view, String csvPath) {
    Dataset<Row> table = spark.table(view);
    List<String> columns = new ArrayList<String>();
    for (StructField field : table.schema().fields()) {
      columns.add(field.name() + " " + field.dataType().simpleString());
    }
    return Json.object()
        .field("ok", true)
        .field("version", spark.version())
        .field("scalaVersion", scala.util.Properties$.MODULE$.versionNumberString())
        .field("javaVersion", System.getProperty("java.version"))
        .field("master", spark.sparkContext().master())
        .field("cores", Runtime.getRuntime().availableProcessors())
        .field("applicationId", spark.sparkContext().applicationId())
        .field("view", view)
        .field("source", csvPath)
        .raw("schema", Json.strings(columns))
        .field("bootMs", System.currentTimeMillis() - t0)
        .end();
  }

  private static String failure(Throwable t, long durationMs) {
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
        .field("error", String.valueOf(t.getMessage()))
        .field("trace", trace.toString())
        .field("durationMs", durationMs)
        .end();
  }

  /**
   * Gives Hadoop an identity without asking the operating system for one.
   *
   * <p>{@code UserGroupInformation} otherwise tries a JAAS login, which fails under CheerpJ with
   * {@code KerberosAuthException: failure to login}; Spark skips the lookup when {@code SPARK_USER}
   * is set, and the environment is only readable through {@code ProcessEnvironment}'s own maps.
   */
  private static void identity() {
    putEnv("SPARK_USER", "browser");
    org.apache.hadoop.security.UserGroupInformation.setLoginUser(
        org.apache.hadoop.security.UserGroupInformation.createRemoteUser("browser"));
  }

  @SuppressWarnings("unchecked")
  private static void putEnv(String key, String value) {
    try {
      Class<?> environment = Class.forName("java.lang.ProcessEnvironment");
      Field field;
      try {
        field = environment.getDeclaredField("theCaseInsensitiveEnvironment");
        field.setAccessible(true);
        ((Map<String, String>) field.get(null)).put(key, value);
        return;
      } catch (NoSuchFieldException e) {
        field = environment.getDeclaredField("theUnmodifiableEnvironment");
      }
      field.setAccessible(true);
      Object unmodifiable = field.get(null);
      Field delegate = unmodifiable.getClass().getDeclaredField("m");
      delegate.setAccessible(true);
      ((Map<String, String>) delegate.get(unmodifiable)).put(key, value);
    } catch (Exception e) {
      System.out.println("SPARK_USER could not be set: " + e);
    }
  }
}
