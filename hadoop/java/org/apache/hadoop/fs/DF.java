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

package org.apache.hadoop.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apacheprojects.hadoopwasm.fs.Space;

/**
 * Free and used space of a directory, from the JDK instead of from {@code df}.
 *
 * <p>This shadows Hadoop's own {@code org.apache.hadoop.fs.DF} -- it comes first on the browser's
 * classpath, so this is the class the NameNode and DataNode load. Hadoop's version forks {@code df
 * -k}; a tab cannot fork, and CheerpJ's stub {@code exec} returns success with no output at all, so
 * Hadoop's parser fails with {@code IOException: Fewer lines of output than expected}. That happens
 * inside {@code NameNodeResourceChecker}, which the NameNode builds in {@code startCommonServices},
 * so with Hadoop's own DF the NameNode dies immediately after its RPC server comes up. The DataNode
 * reads volume capacity through the same class.
 *
 * <p>The answers come from the JDK instead, through {@link Space} -- which also has to make up a
 * capacity, because CheerpJ's filesystem reports none. The API is Hadoop's, including the {@link
 * Shell} supertype it is passed around as.
 */
public class DF extends Shell {

  private final String dirPath;

  private final File dirFile;

  public DF(File path, Configuration conf) throws IOException {
    this(path, 0L);
  }

  public DF(File path, long dfInterval) throws IOException {
    super(dfInterval);
    this.dirPath = path.getCanonicalPath();
    this.dirFile = new File(this.dirPath);
  }

  public String getDirPath() {
    return dirPath;
  }

  /** Hadoop reports the device behind the directory; there is one virtual filesystem here. */
  public String getFilesystem() throws IOException {
    return "cheerpj";
  }

  public long getCapacity() {
    return Space.capacity(dirFile);
  }

  public long getUsed() {
    long used = getCapacity() - getAvailable();
    return used < 0L ? 0L : used;
  }

  public long getAvailable() {
    return Space.available(dirFile);
  }

  public int getPercentUsed() {
    long capacity = getCapacity();
    if (capacity <= 0L) {
      return 0;
    }
    return (int) (getUsed() * 100.0 / capacity);
  }

  /** The mount point: the closest existing ancestor, which is as much as the JDK can say. */
  public String getMount() throws IOException {
    File mount = dirFile;
    while (mount != null && !mount.exists()) {
      mount = mount.getParentFile();
    }
    return mount == null ? "/" : mount.getPath();
  }

  @Override
  public String toString() {
    return "df -k "
        + getMountSafely()
        + "\n"
        + "cheerpj"
        + "\t"
        + getCapacity() / 1024
        + "\t"
        + getUsed() / 1024
        + "\t"
        + getAvailable() / 1024
        + "\t"
        + getPercentUsed()
        + "%\t"
        + getMountSafely();
  }

  private String getMountSafely() {
    try {
      return getMount();
    } catch (IOException failure) {
      return dirPath;
    }
  }

  @Override
  protected String[] getExecString() {
    return new String[] {"true"};
  }

  @Override
  protected void parseExecResult(BufferedReader lines) throws IOException {}

  protected void parseOutput() throws IOException {}
}
