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

package org.apacheprojects.hadoopwasm.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Positional {@link FileChannel} reads and writes, done by seeking.
 *
 * <p>CheerpJ 4.2 has no {@code pwrite64}: {@code channel.write(buffer, position)} logs {@code Missing
 * import: __syscall_pwrite64} and throws {@code IllegalArgumentException}. That is the call the
 * NameNode's edit log makes on every segment it opens ({@code EditLogFileOutputStream.preallocate}
 * grows the file a megabyte at a time through {@code IOUtils.writeFully(FileChannel, ByteBuffer,
 * long)}), so without this the NameNode cannot start a log segment and HDFS never comes up.
 *
 * <p>Seek, transfer, seek back, under the channel's own position lock -- which is what the JDK does
 * on a platform without {@code pwrite}. It is only equivalent because nothing else is using the
 * channel's position concurrently; in this JVM the edit log and the block files each have their own
 * channel and one writer.
 */
public final class ChannelShim {

  private ChannelShim() {}

  public static int write(FileChannel channel, ByteBuffer source, long position) throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("Negative position: " + position);
    }
    synchronized (channel) {
      long resume = channel.position();
      try {
        pad(channel, position);
        channel.position(position);
        return channel.write(source);
      } finally {
        channel.position(resume);
      }
    }
  }

  /**
   * Grows the file to {@code position} with zeros when it is shorter than that.
   *
   * <p>A seek past the end of a file and a write there is a hole on a real filesystem; CheerpJ throws
   * {@code IllegalArgumentException} instead, which is how {@code EditLogFileOutputStream.preallocate}
   * -- a positional write a megabyte past the end -- fails. The bytes it would have written are zeros
   * anyway, so writing them is the same file.
   */
  private static void pad(FileChannel channel, long position) throws IOException {
    long size = channel.size();
    if (size >= position) {
      return;
    }
    channel.position(size);
    ByteBuffer zeros = ByteBuffer.allocate((int) Math.min(position - size, 64 * 1024));
    for (long left = position - size; left > 0; ) {
      zeros.clear();
      zeros.limit((int) Math.min(left, zeros.capacity()));
      left -= channel.write(zeros);
    }
  }

  public static int read(FileChannel channel, ByteBuffer destination, long position)
      throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("Negative position: " + position);
    }
    synchronized (channel) {
      long resume = channel.position();
      try {
        if (position >= channel.size()) {
          return -1;
        }
        channel.position(position);
        return channel.read(destination);
      } finally {
        channel.position(resume);
      }
    }
  }
}
