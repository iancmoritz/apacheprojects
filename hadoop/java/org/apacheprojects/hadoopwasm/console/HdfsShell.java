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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * The HDFS commands the page's prompt understands, each one call into {@link FileSystem}.
 *
 * <p>This is deliberately not {@code org.apache.hadoop.fs.FsShell}: {@code FsShell} writes to a
 * process's stdout and exits, neither of which a tab has. The commands themselves are the same
 * {@code FileSystem} calls {@code FsShell} makes -- {@code mkdirs}, {@code create}, {@code open},
 * {@code listStatus}, {@code delete}, {@code rename}, {@code getFileBlockLocations} -- so the
 * namespace, the blocks and the errors are the NameNode's.
 */
final class HdfsShell {

  /** How much of a file {@code cat} will read into the page. */
  private static final int MAX_CAT = 256 * 1024;

  private HdfsShell() {}

  static String run(String line) {
    long t0 = System.currentTimeMillis();
    FileSystem fs = HadoopConsole.fs();
    if (fs == null) {
      return Failures.message("HDFS is not started");
    }
    String[] words = line.trim().split("\\s+");
    if (words.length == 0 || words[0].isEmpty()) {
      return ok(line, "", null, t0);
    }
    // `hdfs dfs -ls /` and `ls /` both work; the page's prompt is chatty about neither.
    List<String> argv = new ArrayList<String>();
    for (String word : words) {
      if (word.equals("hdfs") || word.equals("hadoop") || word.equals("dfs") || word.equals("fs")) {
        continue;
      }
      argv.add(word.startsWith("-") && argv.isEmpty() ? word.substring(1) : word);
    }
    if (argv.isEmpty()) {
      return ok(line, "", null, t0);
    }
    String command = argv.remove(0);
    try {
      return dispatch(fs, line, command, argv, t0);
    } catch (Throwable t) {
      return Failures.json(t, System.currentTimeMillis() - t0);
    }
  }

  private static String dispatch(FileSystem fs, String line, String command, List<String> argv, long t0)
      throws Exception {
    List<String> flags = new ArrayList<String>();
    List<String> args = new ArrayList<String>();
    for (String word : argv) {
      if (word.startsWith("-") && word.length() > 1) {
        flags.add(word.substring(1));
      } else {
        args.add(word);
      }
    }

    if (command.equals("ls")) {
      Path path = path(args.isEmpty() ? "." : args.get(0));
      List<FileStatus> statuses = new ArrayList<FileStatus>();
      collect(fs, path, flags.contains("R"), statuses);
      return ok(line, render(statuses), listing(fs, statuses), t0);
    }
    if (command.equals("mkdir")) {
      require(args, 1, "mkdir <path>");
      Path path = path(args.get(0));
      // The NameNode creates parents itself; -p only changes whether an existing path is an error.
      boolean existed = fs.exists(path);
      if (existed && !flags.contains("p")) {
        return Failures.message("mkdir: " + path + ": File exists");
      }
      boolean made = fs.mkdirs(path);
      return ok(line, made ? "created " + path : "mkdir failed", null, t0);
    }
    if (command.equals("put") || command.equals("copyFromLocal")) {
      require(args, 2, "put <source> <destination>");
      Path source = new Path(args.get(0));
      FileSystem local = local(fs);
      Path destination = path(args.get(1));
      if (fs.isDirectory(destination)) {
        destination = new Path(destination, source.getName());
      }
      FileUtil.copy(local, source, fs, destination, false, true, fs.getConf());
      FileStatus status = fs.getFileStatus(destination);
      return ok(line, "wrote " + destination + " (" + status.getLen() + " bytes)", null, t0);
    }
    if (command.equals("cat") || command.equals("text") || command.equals("head")) {
      require(args, 1, "cat <path>");
      return ok(line, read(fs, path(args.get(0))), null, t0);
    }
    if (command.equals("rm")) {
      require(args, 1, "rm [-r] <path>");
      Path path = path(args.get(0));
      boolean recursive = flags.contains("r") || flags.contains("R");
      if (!fs.exists(path)) {
        return Failures.message("rm: " + path + ": No such file or directory");
      }
      boolean deleted = fs.delete(path, recursive);
      return ok(line, deleted ? "deleted " + path : "rm failed (use -r for a directory)", null, t0);
    }
    if (command.equals("mv")) {
      require(args, 2, "mv <source> <destination>");
      boolean moved = fs.rename(path(args.get(0)), path(args.get(1)));
      return ok(line, moved ? "moved" : "mv failed", null, t0);
    }
    if (command.equals("cp")) {
      require(args, 2, "cp <source> <destination>");
      FileUtil.copy(fs, path(args.get(0)), fs, path(args.get(1)), false, true, fs.getConf());
      return ok(line, "copied", null, t0);
    }
    if (command.equals("touchz")) {
      require(args, 1, "touchz <path>");
      fs.create(path(args.get(0)), false).close();
      return ok(line, "created", null, t0);
    }
    if (command.equals("du")) {
      Path path = path(args.isEmpty() ? "/" : args.get(0));
      ContentSummary summary = fs.getContentSummary(path);
      String text =
          summary.getLength()
              + "\t"
              + summary.getSpaceConsumed()
              + "\t"
              + path
              + "\n"
              + summary.getFileCount()
              + " files, "
              + summary.getDirectoryCount()
              + " directories";
      return ok(line, text, null, t0);
    }
    if (command.equals("df")) {
      FsStatus status = fs.getStatus();
      String text =
          "Filesystem          Size            Used       Available  Use%\n"
              + fs.getUri()
              + "  "
              + status.getCapacity()
              + "  "
              + status.getUsed()
              + "  "
              + status.getRemaining()
              + "  "
              + (status.getCapacity() == 0 ? 0 : 100 * status.getUsed() / status.getCapacity())
              + "%";
      return ok(line, text, null, t0);
    }
    if (command.equals("blocks") || command.equals("locations")) {
      require(args, 1, "blocks <path>");
      Path path = path(args.get(0));
      FileStatus status = fs.getFileStatus(path);
      StringBuilder text = new StringBuilder();
      text.append(path).append(" len=").append(status.getLen());
      text.append(" blockSize=").append(status.getBlockSize());
      text.append(" replication=").append(status.getReplication());
      for (BlockLocation block : fs.getFileBlockLocations(status, 0, Math.max(status.getLen(), 1))) {
        text.append("\n  offset=").append(block.getOffset());
        text.append(" length=").append(block.getLength());
        text.append(" hosts=").append(String.join(",", block.getHosts()));
        text.append(" datanodes=").append(String.join(",", block.getNames()));
      }
      return ok(line, text.toString(), null, t0);
    }
    if (command.equals("help")) {
      return ok(line, help(), null, t0);
    }
    return Failures.message(command + ": unknown command. Try `help`.");
  }

  /**
   * The local filesystem, unchecksummed.
   *
   * <p>The bundled files live on CheerpJ's read-only {@code /app} mount, which is this origin over
   * HTTP; Hadoop's checksummed {@code LocalFileSystem} would ask for a {@code .crc} beside each one,
   * which is a 404 per read. This is not set as {@code fs.file.impl} globally because the DataNode
   * casts the result of {@code FileSystem.getLocal} to {@code LocalFileSystem}.
   */
  private static FileSystem local(FileSystem fs) throws IOException {
    RawLocalFileSystem local = new RawLocalFileSystem();
    local.initialize(URI.create("file:///"), fs.getConf());
    return local;
  }

  /** Writes text typed into the page straight through the DFS client. */
  static String write(String path, String text) {
    long t0 = System.currentTimeMillis();
    FileSystem fs = HadoopConsole.fs();
    if (fs == null) {
      return Failures.message("HDFS is not started");
    }
    try {
      Path target = path(path);
      FSDataOutputStream out = fs.create(target, true);
      PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));
      writer.print(text);
      writer.close();
      FileStatus status = fs.getFileStatus(target);
      return ok("write " + path, "wrote " + target + " (" + status.getLen() + " bytes)", null, t0);
    } catch (Throwable t) {
      return Failures.json(t, System.currentTimeMillis() - t0);
    }
  }

  private static void collect(FileSystem fs, Path path, boolean recursive, List<FileStatus> into)
      throws IOException {
    FileStatus status = fs.getFileStatus(path);
    if (!status.isDirectory()) {
      into.add(status);
      return;
    }
    for (FileStatus child : fs.listStatus(path)) {
      into.add(child);
      if (recursive && child.isDirectory()) {
        collect(fs, child.getPath(), true, into);
      }
    }
  }

  private static String render(List<FileStatus> statuses) {
    if (statuses.isEmpty()) {
      return "";
    }
    StringBuilder text = new StringBuilder("Found " + statuses.size() + " items");
    for (FileStatus status : statuses) {
      FsPermission permission = status.getPermission();
      text.append('\n');
      text.append(status.isDirectory() ? 'd' : '-').append(permission);
      text.append(status.isDirectory() ? "   -" : "   " + status.getReplication());
      text.append(' ').append(status.getOwner()).append(' ').append(status.getGroup());
      text.append(' ').append(status.getLen());
      text.append(' ').append(status.getPath().toUri().getPath());
    }
    return text.toString();
  }

  private static String listing(FileSystem fs, List<FileStatus> statuses) throws IOException {
    StringBuilder out = new StringBuilder("[");
    for (FileStatus status : statuses) {
      if (out.length() > 1) {
        out.append(',');
      }
      int blocks =
          status.isDirectory()
              ? 0
              : fs.getFileBlockLocations(status, 0, Math.max(status.getLen(), 1)).length;
      out.append(
          Json.object()
              .field("path", status.getPath().toUri().getPath())
              .field("name", status.getPath().getName())
              .field("directory", status.isDirectory())
              .field("length", status.getLen())
              .field("replication", status.getReplication())
              .field("blockSize", status.getBlockSize())
              .field("blocks", blocks)
              .field("owner", status.getOwner())
              .field("group", status.getGroup())
              .field("permission", status.getPermission().toString())
              .field("modified", status.getModificationTime())
              .end());
    }
    return out.append(']').toString();
  }

  private static String read(FileSystem fs, Path path) throws IOException {
    FileStatus status = fs.getFileStatus(path);
    int length = (int) Math.min(status.getLen(), MAX_CAT);
    byte[] bytes = new byte[length];
    FSDataInputStream in = fs.open(path);
    try {
      in.readFully(0, bytes);
    } finally {
      in.close();
    }
    String text = new String(bytes, "UTF-8");
    return status.getLen() > MAX_CAT ? text + "\n... truncated at " + MAX_CAT + " bytes" : text;
  }

  private static Path path(String path) {
    if (path.equals(".") || path.isEmpty()) {
      return new Path("/");
    }
    return new Path(path.startsWith("/") || path.contains(":") ? path : "/" + path);
  }

  private static void require(List<String> args, int count, String usage) {
    if (args.size() < count) {
      throw new IllegalArgumentException("usage: " + usage);
    }
  }

  private static String ok(String command, String output, String listing, long t0) {
    Json json = Json.object().field("ok", true).field("command", command).field("output", output);
    if (listing != null) {
      json.raw("listing", listing);
    }
    return json.field("durationMs", System.currentTimeMillis() - t0).end();
  }

  private static String help() {
    return "ls [-R] <path>          list a directory through the NameNode\n"
        + "mkdir [-p] <path>       create a directory\n"
        + "put <local> <path>      copy a bundled file into HDFS\n"
        + "cat <path>              read a file back through the DataNode\n"
        + "rm [-r] <path>          delete\n"
        + "mv <src> <dst>          rename in the namespace\n"
        + "cp <src> <dst>          copy inside HDFS\n"
        + "touchz <path>           create an empty file\n"
        + "du [<path>]             ContentSummary for a path\n"
        + "df                      FsStatus of the cluster\n"
        + "blocks <path>           block locations, offsets and DataNodes";
  }
}
