import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

/** One Cassandra node in a Web Worker; internode messages travel over postMessage. */
public class Node {
  static long t0 = System.currentTimeMillis();

  static native void send(String to, byte[] frame);

  static native byte[] poll(int timeoutMs);

  static native void reply(String json);

  static native void progress(String stage);

  /** `?verbs=1` on the page: log every internode message, which is how you debug this transport. */
  static boolean logVerbs = false;

  static final java.util.concurrent.ExecutorService cqlExecutor =
      java.util.concurrent.Executors.newSingleThreadExecutor();

  static void mark(String what) {
    System.out.println("[node " + (System.currentTimeMillis() - t0) + "ms] " + what);
    try {
      progress(what);
    } catch (Throwable ignored) {
      // boot progress is cosmetic; never let it stop a node from starting
    }
  }

  static void rmdir(File f) {
    File[] kids = f.listFiles();
    if (kids != null) for (File k : kids) rmdir(k);
    f.delete();
  }

  static String yaml(String dir, String addr, String seeds) {
    return String.join(
        "\n",
        "cluster_name: 'browser'",
        "num_tokens: 4",
        "auto_bootstrap: false",
        "partitioner: org.apache.cassandra.dht.Murmur3Partitioner",
        "endpoint_snitch: SimpleSnitch",
        "disk_access_mode: standard",
        "commitlog_compression:",
        "    - class_name: LZ4Compressor",
        "commitlog_sync: periodic",
        "commitlog_sync_period: 10000ms",
        "seed_provider:",
        "  - class_name: org.apache.cassandra.locator.SimpleSeedProvider",
        "    parameters:",
        "      - seeds: \"" + seeds + "\"",
        "data_file_directories:",
        "  - " + dir + "/data",
        "commitlog_directory: " + dir + "/commitlog",
        "hints_directory: " + dir + "/hints",
        "saved_caches_directory: " + dir + "/saved_caches",
        "listen_address: " + addr,
        "rpc_address: " + addr,
        "start_native_transport: false",
        "storage_port: 7000",
        "native_transport_port: 9042",
        "memtable_allocation_type: heap_buffers",
        "concurrent_reads: 2",
        "concurrent_writes: 2",
        "concurrent_counter_writes: 2",
        "concurrent_compactors: 1",
        "memtable_heap_space: 32MiB",
        "memtable_offheap_space: 0MiB",
        "file_cache_size: 16MiB",
        "auto_snapshot: false",
        "commitlog_total_space: 32MiB",
        "commitlog_segment_size: 8MiB",
        "min_free_space_per_drive: 0MiB",
        "max_hints_file_size: 8MiB",
        "key_cache_size: 1MiB",
        "counter_cache_size: 1MiB",
        "index_summary_capacity: 1MiB",
        "trickle_fsync: false",
        "disk_failure_policy: stop",
        "commit_failure_policy: stop",
        "");
  }

  public static void main(String[] args) throws Exception {
    int index = Integer.parseInt(args[0]);
    logVerbs = args.length > 2 && "verbs".equals(args[2]);
    String addr = "127.0.0." + index;
    String base = "/files/node" + index;
    String dir = base;

    if (!"keep".equals(System.getProperty("boot.data", ""))) {
      rmdir(new File(base));
      String[] left = new File(base).list();
      if (left != null && left.length > 0) {
        // CheerpJ's filesystem does not always honour delete(), and a commitlog segment left
        // half-written by an earlier visit stops this node during replay, so start somewhere new.
        dir = base + "-" + System.currentTimeMillis();
        mark(left.length + " files survived wiping " + base + "; starting in " + dir);
      } else {
        mark("wiped " + base);
      }
    }
    new File(dir).mkdirs();
    File conf = new File(dir + "/cassandra.yaml");
    try (FileWriter w = new FileWriter(conf)) {
      w.write(yaml(dir, addr, "127.0.0.1"));
    }

    System.setProperty("cassandra.config", "file://" + conf.getAbsolutePath());
    System.setProperty("cassandra.storagedir", dir);
    System.setProperty("cassandra-foreground", "yes");
    System.setProperty("cassandra.jmx.local.port", "");
    System.setProperty("cassandra.ring_delay_ms", "10000");
    System.setProperty("cassandra.consistent.rangemovement", "false");
    System.setProperty("cassandra.skip_wait_for_gossip_to_settle", "0");
    System.setProperty("cassandra.disable_tcactive_openssl", "true");
    System.setProperty("io.netty.transport.noNative", "true");
    System.setProperty("io.netty.noUnsafe", "false");
    System.setProperty("cassandra.wasm.fs_space_bytes", String.valueOf(1024L * 1024 * 1024));
    System.setProperty("cassandra.require_native_file_hints", "true");
    System.setProperty("cassandra.skip_sync", "true");
    System.setProperty("cassandra.wasm.on_heap_buffers", "true");
    System.setProperty("cassandra.wasm.no_sockets", "true");

    mark("config " + addr);
    DatabaseDescriptor.daemonInitialization();
    mark("daemonInitialization done, broadcast=" + FBUtilities.getBroadcastAddressAndPort());

    org.apache.cassandra.io.util.FileUtils.setFSErrorHandler(
        new org.apache.cassandra.service.DefaultFSErrorHandler());

    mark("commitlog start");
    org.apache.cassandra.db.commitlog.CommitLog.instance.start();
    org.apache.cassandra.db.SystemKeyspace.persistLocalMetadata();
    mark("local metadata persisted");
    StorageService.instance.populateTokenMetadata();
    org.apache.cassandra.schema.Schema.instance.loadFromDisk();
    org.apache.cassandra.db.Keyspace.setInitialized();
    org.apache.cassandra.db.commitlog.CommitLog.instance.recoverSegmentsOnDisk();
    mark("system keyspace up");

    installTransport();

    mark("initServer");
    StorageService.instance.initServer();
    mark("initServer done, mode=" + StorageService.instance.getOperationMode());
    reply("{\"type\":\"ready\",\"ms\":" + (System.currentTimeMillis() - t0) + "}");

    while (true) {
      Thread.sleep(3000);
      reply(statusJson());
    }
  }

  /** nodetool-status-style state, read straight out of Cassandra. */
  static String statusJson() {
    StringBuilder sb = new StringBuilder("{\"type\":\"status\"");
    sb.append(",\"mode\":").append(json(StorageService.instance.getOperationMode()));
    sb.append(",\"hostId\":").append(json(String.valueOf(StorageService.instance.getLocalHostId())));
    sb.append(",\"load\":").append(json(StorageService.instance.getLoadString()));
    sb.append(",\"tokens\":").append(StorageService.instance.getTokens().size());
    sb.append(",\"schema\":").append(json(String.valueOf(StorageService.instance.getSchemaVersion())));
    sb.append(",\"live\":").append(jsonList(StorageService.instance.getLiveNodes()));
    sb.append(",\"unreachable\":").append(jsonList(StorageService.instance.getUnreachableNodes()));
    sb.append(",\"ownership\":{");
    try {
      boolean first = true;
      for (Map.Entry<java.net.InetAddress, Float> e : StorageService.instance.getOwnership().entrySet()) {
        if (!first) sb.append(',');
        first = false;
        sb.append(json(e.getKey().getHostAddress())).append(':').append(e.getValue());
      }
    } catch (Throwable ignored) {
    }
    sb.append("}}");
    return sb.toString();
  }

  static String jsonList(List<String> values) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) sb.append(',');
      sb.append(json(values.get(i)));
    }
    return sb.append(']').toString();
  }

  /** Replace Cassandra's TCP transport with the worker's postMessage transport. */
  static void installTransport() {
    MessagingService.instance().outboundSink.add((message, to) -> {
      try {
        int version = MessagingService.instance().versions.get(to);
        try (DataOutputBuffer out = new DataOutputBuffer(1024)) {
          Message.serializer.serialize(message, out, version);
          if (logVerbs)
            System.out.println("[verb] -> " + to + " " + message.verb() + " " + out.getLength() + "B");
          send(to.getHostAddressAndPort(),
              frame(FBUtilities.getBroadcastAddressAndPort(), version, out.toByteArray()));
        }
      } catch (Throwable t) {
        System.out.println("[node] outbound serialize failed to " + to + ": " + t);
      }
      return false; // Cassandra must not also try to write it to a socket
    });

    Thread pump = new Thread(() -> {
      while (true) {
        try {
          byte[] frame = poll(1000);
          if (frame == null)
            continue;
          if (frame[0] == 0)
            deliver(frame);
          else
            command(frame);
        } catch (Throwable t) {
          System.out.println("[node] inbound pump error: " + t);
        }
      }
    }, "wasm-inbound-pump");
    pump.setDaemon(true);
    pump.start();
    mark("postMessage transport installed");
  }

  /** command frame = [1 byte type=1]"id\nCONSISTENCY\nquery" */
  static void command(byte[] frame) {
    String body = new String(frame, 1, frame.length - 1, StandardCharsets.UTF_8);
    int nl1 = body.indexOf('\n');
    int nl2 = body.indexOf('\n', nl1 + 1);
    String id = body.substring(0, nl1);
    String cl = body.substring(nl1 + 1, nl2);
    String query = body.substring(nl2 + 1);
    cqlExecutor.execute(() -> {
      long start = System.currentTimeMillis();
      try {
        reply(cqlJson(id, cl, query, start));
      } catch (Throwable t) {
        String msg = t.getClass().getSimpleName() + ": " + t.getMessage();
        reply("{\"type\":\"result\",\"id\":" + json(id) + ",\"ms\":"
            + (System.currentTimeMillis() - start) + ",\"error\":" + json(msg) + "}");
      }
    });
  }

  static String cqlJson(String id, String cl, String query, long start) throws Exception {
    ConsistencyLevel level = ConsistencyLevel.valueOf(cl);
    ClientState cs = ClientState.forInternalCalls();
    QueryState qs = new QueryState(cs);
    CQLStatement st = QueryProcessor.parseStatement(query, cs);
    st.validate(cs);
    QueryOptions opts = QueryOptions.create(level, java.util.Collections.emptyList(), false, 500,
        null, ConsistencyLevel.SERIAL, ProtocolVersion.CURRENT, null);
    ResultMessage r = QueryProcessor.instance.process(st, qs, opts,
        Dispatcher.RequestTime.forImmediateExecution());
    long ms = System.currentTimeMillis() - start;

    StringBuilder sb = new StringBuilder("{\"type\":\"result\",\"id\":");
    sb.append(json(id)).append(",\"ms\":").append(ms);
    if (r instanceof ResultMessage.Rows) {
      ResultSet rs = ((ResultMessage.Rows) r).result;
      sb.append(",\"columns\":[");
      for (int i = 0; i < rs.metadata.names.size(); i++) {
        if (i > 0) sb.append(',');
        sb.append(json(rs.metadata.names.get(i).name.toString()));
      }
      sb.append("],\"rows\":[");
      for (int i = 0; i < rs.rows.size(); i++) {
        if (i > 0) sb.append(',');
        sb.append('[');
        List<ByteBuffer> row = rs.rows.get(i);
        for (int c = 0; c < row.size(); c++) {
          if (c > 0) sb.append(',');
          ByteBuffer bb = row.get(c);
          sb.append(bb == null ? "null" : json(rs.metadata.names.get(c).type.getString(bb)));
        }
        sb.append(']');
      }
      sb.append(']');
    } else {
      sb.append(",\"kind\":").append(json(String.valueOf(r.kind)));
    }
    return sb.append('}').toString();
  }

  static String json(String s) {
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '"' || c == '\\') sb.append('\\').append(c);
      else if (c == '\n') sb.append("\\n");
      else if (c == '\r') sb.append("\\r");
      else if (c == '\t') sb.append("\\t");
      else if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
      else sb.append(c);
    }
    return sb.append('"').toString();
  }

  /** frame = [1 byte type=0][1 byte from length][from utf8][4 bytes version BE][payload] */
  static byte[] frame(InetAddressAndPort from, int version, byte[] payload) {
    byte[] addr = from.getHostAddressAndPort().getBytes(StandardCharsets.UTF_8);
    byte[] out = new byte[2 + addr.length + 4 + payload.length];
    out[0] = 0;
    out[1] = (byte) addr.length;
    System.arraycopy(addr, 0, out, 2, addr.length);
    int p = 2 + addr.length;
    out[p] = (byte) (version >>> 24);
    out[p + 1] = (byte) (version >>> 16);
    out[p + 2] = (byte) (version >>> 8);
    out[p + 3] = (byte) version;
    System.arraycopy(payload, 0, out, p + 4, payload.length);
    return out;
  }

  static void deliver(byte[] frame) throws Exception {
    int addrLen = frame[1] & 0xff;
    String from = new String(frame, 2, addrLen, StandardCharsets.UTF_8);
    int p = 2 + addrLen;
    int version = ((frame[p] & 0xff) << 24) | ((frame[p + 1] & 0xff) << 16)
        | ((frame[p + 2] & 0xff) << 8) | (frame[p + 3] & 0xff);
    byte[] payload = new byte[frame.length - p - 4];
    System.arraycopy(frame, p + 4, payload, 0, payload.length);

    InetAddressAndPort fromAddr = InetAddressAndPort.getByName(from);
    // Cassandra normally learns a peer's messaging version from the internode connection
    // handshake, and MigrationCoordinator refuses to push or pull schema to a peer whose version
    // it does not know.  There is no handshake here, so the frame's version header is the handshake.
    MessagingService.instance().versions.set(fromAddr, version);
    Message<?> messageIn;
    try (DataInputBuffer in = new DataInputBuffer(payload)) {
      messageIn = Message.serializer.deserialize(in, fromAddr, version);
    }
    if (logVerbs)
      System.out.println("[verb] <- " + from + " " + messageIn.verb() + " " + payload.length + "B");
    messageIn.header.verb.stage.executor().execute(
        () -> MessagingService.instance().inboundSink.accept(messageIn));
  }
}
