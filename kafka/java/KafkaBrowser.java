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

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;

/**
 * The page's entry point into Kafka. Every method here is called from JavaScript through CheerpJ and
 * returns JSON; all the Kafka work is done by Kafka's own classes -- {@code StorageTool} formats the
 * KRaft metadata log, {@code KafkaRaftServer} is the broker/controller, and the topic, produce,
 * consume and lag operations go through {@code Admin}, {@code KafkaProducer} and
 * {@code KafkaConsumer} over the loopback transport in {@code inmem}.
 */
public final class KafkaBrowser {
  private static final String BOOTSTRAP = "localhost:9092";

  private static kafka.server.KafkaRaftServer server;
  private static Admin admin;
  private static KafkaProducer<String, String> producer;
  private static final Map<String, KafkaConsumer<String, String>> CONSUMERS = new LinkedHashMap<>();

  private KafkaBrowser() {}

  /** Runs Kafka's own storage formatter over the config's log.dirs, as kafka-storage.sh format does. */
  public static synchronized String format(String configPath, String clusterId) {
    Exit.setExitProcedure((code, message) -> {
      throw new ExitCalled(code, message);
    });
    Exit.setHaltProcedure((code, message) -> {
      throw new ExitCalled(code, message);
    });
    try {
      kafka.tools.StorageTool.main(
          new String[] {"format", "-t", clusterId, "-c", configPath, "--ignore-formatted"});
      return ok("formatted");
    } catch (ExitCalled e) {
      return e.code == 0 ? ok("formatted") : error("storage format exited " + e.code + ": " + e.message);
    } catch (Throwable t) {
      return error(t);
    } finally {
      Exit.resetExitProcedure();
      Exit.resetHaltProcedure();
    }
  }

  /** Starts the combined broker + controller and returns once it reports itself started. */
  public static synchronized String start(String configPath) {
    try {
      if (server != null) return ok("already running");
      Properties props = new Properties();
      try (InputStream in = new FileInputStream(configPath)) {
        props.load(in);
      }
      kafka.server.KafkaConfig config = kafka.server.KafkaConfig$.MODULE$.fromProps(props, false);
      kafka.server.KafkaRaftServer raftServer = new kafka.server.KafkaRaftServer(config, Time.SYSTEM);
      raftServer.startup();
      server = raftServer;
      return ok("started");
    } catch (Throwable t) {
      return error(t);
    }
  }

  public static synchronized String createTopic(String name, int partitions) {
    try {
      admin().createTopics(Collections.singletonList(new NewTopic(name, partitions, (short) 1))).all().get();
      return ok("created " + name + " with " + partitions + " partition(s)");
    } catch (ExecutionException e) {
      return error(e.getCause());
    } catch (Throwable t) {
      return error(t);
    }
  }

  /** Produces one record and reports the partition and offset the broker assigned it. */
  public static String produce(String topic, String key, String value) {
    try {
      RecordMetadata md = producer()
          .send(new ProducerRecord<>(topic, key == null || key.isEmpty() ? null : key, value)).get();
      return "{\"ok\":true,\"topic\":\"" + esc(md.topic()) + "\",\"partition\":" + md.partition()
          + ",\"offset\":" + md.offset() + "}";
    } catch (ExecutionException e) {
      return error(e.getCause());
    } catch (Throwable t) {
      return error(t);
    }
  }

  /**
   * Polls one consumer group and commits what it read, so the group's committed offsets and lag move
   * exactly as they would against a networked broker. The consumer per group is kept alive between
   * calls, so it stays a member of the group.
   */
  public static String consume(String group, String topic, int max, long timeoutMs) {
    try {
      KafkaConsumer<String, String> consumer = consumer(group, topic);
      StringBuilder sb = new StringBuilder("{\"ok\":true,\"records\":[");
      long deadline = System.currentTimeMillis() + timeoutMs;
      int seen = 0;
      while (seen < max && System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) {
          if (seen++ > 0) sb.append(",");
          sb.append("{\"partition\":").append(r.partition())
              .append(",\"offset\":").append(r.offset())
              .append(",\"timestamp\":").append(r.timestamp())
              .append(",\"key\":").append(r.key() == null ? "null" : "\"" + esc(r.key()) + "\"")
              .append(",\"value\":\"").append(esc(r.value())).append("\"}");
          if (seen >= max) break;
        }
      }
      consumer.commitSync();
      return sb.append("]}").toString();
    } catch (Throwable t) {
      return error(t);
    }
  }

  /**
   * The cluster as Kafka's AdminClient sees it: brokers, topics with their partitions and leaders,
   * log start and end offsets, and every consumer group's committed offset and lag per partition.
   */
  public static String state() {
    try {
      Admin a = admin();
      List<String> names = new ArrayList<>(a.listTopics(
          new org.apache.kafka.clients.admin.ListTopicsOptions().listInternal(true)).names().get());
      Collections.sort(names);
      Map<String, TopicDescription> topics = a.describeTopics(names).allTopicNames().get();

      Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
      Map<TopicPartition, OffsetSpec> earliestSpecs = new HashMap<>();
      for (TopicDescription d : topics.values()) {
        for (TopicPartitionInfo pi : d.partitions()) {
          TopicPartition tp = new TopicPartition(d.name(), pi.partition());
          latestSpecs.put(tp, OffsetSpec.latest());
          earliestSpecs.put(tp, OffsetSpec.earliest());
        }
      }
      Map<TopicPartition, ListOffsetsResultInfo> ends = a.listOffsets(latestSpecs).all().get();
      Map<TopicPartition, ListOffsetsResultInfo> starts = a.listOffsets(earliestSpecs).all().get();

      StringBuilder sb = new StringBuilder("{\"ok\":true,\"clusterId\":\"")
          .append(esc(a.describeCluster().clusterId().get())).append("\",\"brokers\":[");
      boolean first = true;
      for (org.apache.kafka.common.Node n : a.describeCluster().nodes().get()) {
        if (!first) sb.append(",");
        first = false;
        sb.append("{\"id\":").append(n.id()).append(",\"host\":\"").append(esc(n.host()))
            .append("\",\"port\":").append(n.port()).append("}");
      }
      sb.append("],\"topics\":[");
      first = true;
      for (String name : names) {
        if (!first) sb.append(",");
        first = false;
        sb.append("{\"name\":\"").append(esc(name)).append("\",\"internal\":")
            .append(name.startsWith("__")).append(",\"partitions\":[");
        boolean firstPartition = true;
        for (TopicPartitionInfo pi : topics.get(name).partitions()) {
          if (!firstPartition) sb.append(",");
          firstPartition = false;
          TopicPartition tp = new TopicPartition(name, pi.partition());
          sb.append("{\"partition\":").append(pi.partition())
              .append(",\"leader\":").append(pi.leader() == null ? -1 : pi.leader().id())
              .append(",\"replicas\":").append(pi.replicas().size())
              .append(",\"startOffset\":").append(offset(starts.get(tp)))
              .append(",\"endOffset\":").append(offset(ends.get(tp))).append("}");
        }
        sb.append("]}");
      }
      sb.append("],\"groups\":[");
      first = true;
      for (ConsumerGroupListing g : a.listConsumerGroups().valid().get()) {
        if (!first) sb.append(",");
        first = false;
        Map<TopicPartition, OffsetAndMetadata> committed =
            a.listConsumerGroupOffsets(g.groupId()).partitionsToOffsetAndMetadata().get();
        sb.append("{\"groupId\":\"").append(esc(g.groupId())).append("\",\"state\":\"")
            .append(esc(g.state().map(Object::toString).orElse("unknown"))).append("\",\"offsets\":[");
        boolean firstOffset = true;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : committed.entrySet()) {
          if (!firstOffset) sb.append(",");
          firstOffset = false;
          long end = offset(ends.get(e.getKey()));
          sb.append("{\"topic\":\"").append(esc(e.getKey().topic())).append("\",\"partition\":")
              .append(e.getKey().partition()).append(",\"committed\":").append(e.getValue().offset())
              .append(",\"endOffset\":").append(end)
              .append(",\"lag\":").append(end < 0 ? -1 : end - e.getValue().offset()).append("}");
        }
        sb.append("]}");
      }
      return sb.append("]}").toString();
    } catch (Throwable t) {
      return error(t);
    }
  }

  private static long offset(ListOffsetsResultInfo info) {
    return info == null ? -1 : info.offset();
  }

  private static synchronized Admin admin() {
    if (admin == null) {
      Properties p = new Properties();
      p.put("bootstrap.servers", BOOTSTRAP);
      p.put("client.id", "browser-admin");
      p.put("request.timeout.ms", "60000");
      p.put("default.api.timeout.ms", "60000");
      admin = Admin.create(p);
    }
    return admin;
  }

  private static synchronized KafkaProducer<String, String> producer() {
    if (producer == null) {
      Properties p = new Properties();
      p.put("bootstrap.servers", BOOTSTRAP);
      p.put("client.id", "browser-producer");
      p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      p.put("acks", "all");
      p.put("linger.ms", "0");
      producer = new KafkaProducer<>(p);
    }
    return producer;
  }

  private static synchronized KafkaConsumer<String, String> consumer(String group, String topic) {
    String key = group + "\u0000" + topic;
    KafkaConsumer<String, String> consumer = CONSUMERS.get(key);
    if (consumer == null) {
      Properties p = new Properties();
      p.put("bootstrap.servers", BOOTSTRAP);
      p.put("group.id", group);
      p.put("client.id", "browser-consumer-" + group);
      p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      p.put("auto.offset.reset", "earliest");
      p.put("enable.auto.commit", "false");
      consumer = new KafkaConsumer<>(p);
      consumer.subscribe(Collections.singletonList(topic));
      CONSUMERS.put(key, consumer);
    }
    return consumer;
  }

  private static String ok(String message) {
    return "{\"ok\":true,\"message\":\"" + esc(message) + "\"}";
  }

  private static String error(String message) {
    return "{\"ok\":false,\"error\":\"" + esc(message) + "\"}";
  }

  private static String error(Throwable t) {
    return error(t.getClass().getName() + ": " + t.getMessage());
  }

  private static String esc(String s) {
    if (s == null) return "";
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"': sb.append("\\\""); break;
        case '\\': sb.append("\\\\"); break;
        case '\n': sb.append("\\n"); break;
        case '\r': sb.append("\\r"); break;
        case '\t': sb.append("\\t"); break;
        default:
          if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
          else sb.append(c);
      }
    }
    return sb.toString();
  }

  /** Kafka's tools call Exit rather than returning; this carries the code back out. */
  private static final class ExitCalled extends RuntimeException {
    private final int code;
    private final String message;

    ExitCalled(int code, String message) {
      super("exit " + code);
      this.code = code;
      this.message = message;
    }
  }
}
