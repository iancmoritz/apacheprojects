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

package org.apacheprojects.hadoopwasm.fs;

import java.io.File;

/**
 * How much room the storage directory has, on a filesystem that does not know.
 *
 * <p>CheerpJ's virtual filesystem implements no {@code statvfs}: {@link File#getTotalSpace()} and
 * {@link File#getUsableSpace()} both return 0. Hadoop reads that as a full disk, and two things stop
 * dead -- the NameNode puts itself in safe mode ("Resources are low on NN"), and {@code
 * MiniDFSCluster.waitActive} never accepts a DataNode whose reported capacity is zero.
 *
 * <p>So the capacity is a quota this project declares (2 GiB, or {@code hadoopwasm.capacity}) and the
 * used figure is measured by walking the directory, which is cheap for the handful of blocks a tab
 * writes. The numbers the page shows for HDFS capacity are therefore this quota, not a real device;
 * how much the browser will actually store is up to its origin storage limit.
 */
public final class Space {

  private Space() {}

  private static final long NOMINAL =
      Long.getLong("hadoopwasm.capacity", 2L * 1024 * 1024 * 1024);

  /** The device's capacity when it has one, and the declared quota when it does not. */
  public static long capacity(File directory) {
    long total = directory.getTotalSpace();
    return total > 0L ? total : NOMINAL;
  }

  public static long available(File directory) {
    long usable = directory.getUsableSpace();
    if (usable > 0L) {
      return usable;
    }
    long free = capacity(directory) - size(directory);
    return free < 0L ? 0L : free;
  }

  /** The total length of every file under {@code file}, following the directory tree. */
  public static long size(File file) {
    if (file == null || !file.exists()) {
      return 0L;
    }
    if (file.isFile()) {
      return file.length();
    }
    File[] children = file.listFiles();
    if (children == null) {
      return 0L;
    }
    long total = 0L;
    for (File child : children) {
      total += size(child);
    }
    return total;
  }
}
