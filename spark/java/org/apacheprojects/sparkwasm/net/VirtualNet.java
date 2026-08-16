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

package org.apacheprojects.sparkwasm.net;

import java.net.BindException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * The virtual loopback network every channel in this package shares.
 *
 * <p>The browser has no TCP, so a connection here is a pair of in-memory byte pipes between two
 * channels inside the same JVM: the "server" side of a connection is handed to whichever {@link
 * VirtualServerSocketChannel} listens on the port the client asked for. One global monitor guards
 * every mutable piece of state and is what blocking reads, writes, accepts and {@link
 * VirtualSelector#select} wait on, so any state change only has to call {@link #signal()}.
 */
public final class VirtualNet {

  /** Guards all state in this package; also the monitor blocking operations wait on. */
  public static final Object LOCK = new Object();

  /** Bytes each direction of a connection buffers before writes start returning short. */
  static final int PIPE_CAPACITY = 1 << 20;

  private static final Map<Integer, VirtualServerSocketChannel> LISTENERS = new HashMap<>();

  private static int nextEphemeralPort = 45000;

  private VirtualNet() {}

  /** Wakes up every thread blocked in this package: a read, a write, an accept or a select. */
  public static void signal() {
    synchronized (LOCK) {
      LOCK.notifyAll();
    }
  }

  static InetAddress loopback() {
    return InetAddress.getLoopbackAddress();
  }

  /** Claims {@code port} (or an unused ephemeral one when it is 0) for {@code listener}. */
  static int bind(int port, VirtualServerSocketChannel listener) throws BindException {
    synchronized (LOCK) {
      if (port == 0) {
        while (LISTENERS.containsKey(nextEphemeralPort)) nextEphemeralPort++;
        port = nextEphemeralPort++;
      } else if (LISTENERS.containsKey(port)) {
        throw new BindException("Address already in use: port " + port);
      }
      LISTENERS.put(port, listener);
      return port;
    }
  }

  static void unbind(int port, VirtualServerSocketChannel listener) {
    synchronized (LOCK) {
      if (LISTENERS.get(port) == listener) LISTENERS.remove(port);
    }
  }

  static VirtualServerSocketChannel listenerFor(int port) {
    synchronized (LOCK) {
      return LISTENERS.get(port);
    }
  }

  static int ephemeralPort() {
    synchronized (LOCK) {
      return nextEphemeralPort++;
    }
  }

  /**
   * A bounded byte pipe: one channel writes, the peer channel reads. Every method takes {@link
   * #LOCK}, so a pipe is safe to use from the two event loop threads at each end of a connection.
   */
  static final class Pipe {
    private final byte[] buffer = new byte[PIPE_CAPACITY];
    private int head;
    private int size;
    private boolean writeClosed;

    /** Copies as much of {@code src} as fits; returns the number of bytes taken. */
    int write(ByteBuffer src) {
      synchronized (LOCK) {
        if (writeClosed) return 0;
        int n = Math.min(src.remaining(), buffer.length - size);
        for (int i = 0; i < n; i++) {
          buffer[(head + size + i) % buffer.length] = src.get();
        }
        size += n;
        if (n > 0) LOCK.notifyAll();
        return n;
      }
    }

    /** Copies out up to {@code dst.remaining()} bytes; returns -1 once the writer is done. */
    int read(ByteBuffer dst) {
      synchronized (LOCK) {
        if (size == 0) return writeClosed ? -1 : 0;
        int n = Math.min(dst.remaining(), size);
        for (int i = 0; i < n; i++) {
          dst.put(buffer[(head + i) % buffer.length]);
        }
        head = (head + n) % buffer.length;
        size -= n;
        LOCK.notifyAll();
        return n;
      }
    }

    boolean readable() {
      synchronized (LOCK) {
        return size > 0 || writeClosed;
      }
    }

    boolean writable() {
      synchronized (LOCK) {
        return !writeClosed && size < buffer.length;
      }
    }

    void closeWrite() {
      synchronized (LOCK) {
        writeClosed = true;
        LOCK.notifyAll();
      }
    }
  }
}
