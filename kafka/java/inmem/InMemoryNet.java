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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry and shared monitor for the in-JVM loopback network. Everything about readiness
 * changes is signalled through {@link #MONITOR} so selectors can block and be woken up.
 */
public final class InMemoryNet {
  public static final Object MONITOR = new Object();

  private static final Map<Integer, InMemoryServerSocketChannel> LISTENERS = new HashMap<>();
  private static int ephemeral = 40000;

  private InMemoryNet() {}

  public static void bind(int port, InMemoryServerSocketChannel ch) throws IOException {
    synchronized (MONITOR) {
      if (port == 0) throw new IOException("ephemeral bind not supported");
      if (LISTENERS.containsKey(port)) throw new IOException("Address already in use: " + port);
      LISTENERS.put(port, ch);
      MONITOR.notifyAll();
    }
  }

  public static void unbind(int port) {
    synchronized (MONITOR) {
      LISTENERS.remove(port);
      MONITOR.notifyAll();
    }
  }

  public static InMemoryServerSocketChannel listener(int port) {
    synchronized (MONITOR) {
      return LISTENERS.get(port);
    }
  }

  public static int nextEphemeralPort() {
    synchronized (MONITOR) {
      return ++ephemeral;
    }
  }

  public static void signal() {
    synchronized (MONITOR) {
      MONITOR.notifyAll();
    }
  }

  public static int portOf(java.net.SocketAddress sa) throws IOException {
    if (!(sa instanceof InetSocketAddress)) throw new IOException("unsupported address " + sa);
    return ((InetSocketAddress) sa).getPort();
  }
}
