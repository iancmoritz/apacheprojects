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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

/**
 * The cluster as the NameNode sees it, and the files it wrote.
 *
 * <p>The DataNode report is what {@code hdfs dfsadmin -report} prints, from the same call
 * ({@link DistributedFileSystem#getDataNodeStats()}); the storage section is the NameNode's own
 * counters plus a listing of the fsimage, the edit log and the block files as they exist on CheerpJ's
 * writable filesystem, which is the evidence that this is HDFS and not a shim over something else.
 */
final class Datanodes {

  private Datanodes() {}

  static String json(MiniDFSCluster cluster, FileSystem fs) throws Exception {
    StringBuilder out = new StringBuilder("[");
    DatanodeInfo[] live = ((DistributedFileSystem) fs).getDataNodeStats();
    for (int i = 0; i < live.length; i++) {
      DatanodeInfo node = live[i];
      if (i > 0) {
        out.append(',');
      }
      out.append(
          Json.object()
              .field("name", node.getXferAddr())
              .field("uuid", node.getDatanodeUuid())
              .field("state", String.valueOf(node.getAdminState()))
              .field("capacity", node.getCapacity())
              .field("dfsUsed", node.getDfsUsed())
              .field("remaining", node.getRemaining())
              .field("blockPoolUsed", node.getBlockPoolUsed())
              .field("xceivers", node.getXceiverCount())
              .field("lastUpdateMs", System.currentTimeMillis() - node.getLastUpdate())
              .end());
    }
    return out.append(']').toString();
  }

  static String storage(MiniDFSCluster cluster) {
    FSNamesystem namesystem = cluster.getNamesystem();
    return Json.object()
        .field("blockPoolId", namesystem.getBlockPoolId())
        .field("clusterId", namesystem.getClusterId())
        .field("filesTotal", namesystem.getFilesTotal())
        .field("blocksTotal", namesystem.getBlocksTotal())
        .field("capacityUsed", namesystem.getCapacityUsed())
        .field("lastWrittenTransactionId", namesystem.getLastWrittenTransactionId())
        .field("transactionsSinceLastCheckpoint", namesystem.getTransactionsSinceLastCheckpoint())
        .raw("files", files())
        .end();
  }

  /** The fsimage, edit logs and block files the daemons have written, straight off the disk. */
  private static String files() {
    List<String> found = new ArrayList<String>();
    walk(new File(HadoopConsole.STORAGE), found, 0);
    StringBuilder out = new StringBuilder("[");
    for (String entry : found) {
      if (out.length() > 1) {
        out.append(',');
      }
      out.append(entry);
    }
    return out.append(']').toString();
  }

  private static void walk(File dir, List<String> found, int depth) {
    File[] children = dir.listFiles();
    if (children == null || found.size() > 400) {
      return;
    }
    Arrays.sort(children);
    for (File child : children) {
      if (child.isDirectory()) {
        walk(child, found, depth + 1);
      } else {
        String path = child.getAbsolutePath();
        if (path.startsWith(HadoopConsole.STORAGE)) {
          path = path.substring(HadoopConsole.STORAGE.length());
        }
        found.add(Json.object().field("path", path).field("bytes", child.length()).end());
      }
    }
  }
}
