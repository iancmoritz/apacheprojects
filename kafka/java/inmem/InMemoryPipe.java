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
package inmem;

import java.nio.ByteBuffer;

/** A bounded byte queue: one direction of a loopback connection. */
final class InMemoryPipe {
  private final byte[] buf;
  private int head;
  private int size;
  private boolean writeClosed;

  InMemoryPipe(int capacity) {
    this.buf = new byte[capacity];
  }

  int available() {
    synchronized (InMemoryNet.MONITOR) {
      return size;
    }
  }

  boolean writable() {
    synchronized (InMemoryNet.MONITOR) {
      return !writeClosed && size < buf.length;
    }
  }

  boolean eof() {
    synchronized (InMemoryNet.MONITOR) {
      return writeClosed && size == 0;
    }
  }

  void closeWrite() {
    synchronized (InMemoryNet.MONITOR) {
      writeClosed = true;
      InMemoryNet.MONITOR.notifyAll();
    }
  }

  /** Non-blocking write; returns bytes accepted. */
  int write(ByteBuffer src) {
    synchronized (InMemoryNet.MONITOR) {
      int n = Math.min(src.remaining(), buf.length - size);
      for (int i = 0; i < n; i++) {
        buf[(head + size + i) % buf.length] = src.get();
      }
      size += n;
      if (n > 0) InMemoryNet.MONITOR.notifyAll();
      return n;
    }
  }

  /** Non-blocking read; returns bytes copied, or -1 at end of stream. */
  int read(ByteBuffer dst) {
    synchronized (InMemoryNet.MONITOR) {
      if (size == 0) return writeClosed ? -1 : 0;
      int n = Math.min(dst.remaining(), size);
      for (int i = 0; i < n; i++) {
        dst.put(buf[(head + i) % buf.length]);
      }
      head = (head + n) % buf.length;
      size -= n;
      if (n > 0) InMemoryNet.MONITOR.notifyAll();
      return n;
    }
  }
}
