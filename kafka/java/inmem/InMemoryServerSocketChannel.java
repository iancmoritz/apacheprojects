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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Set;

/** A loopback ServerSocketChannel: accept() hands back the peer end of an in-JVM connection. */
public final class InMemoryServerSocketChannel extends ServerSocketChannel {
  private final Deque<InMemorySocketChannel> backlog = new ArrayDeque<>();
  private InetSocketAddress local;
  private ServerSocket serverSocket;

  InMemoryServerSocketChannel(SelectorProvider provider) {
    super(provider);
  }

  void enqueue(InMemorySocketChannel channel) {
    synchronized (InMemoryNet.MONITOR) {
      backlog.addLast(channel);
      InMemoryNet.MONITOR.notifyAll();
    }
  }

  boolean acceptReady() {
    synchronized (InMemoryNet.MONITOR) {
      return !backlog.isEmpty();
    }
  }

  @Override
  public ServerSocketChannel bind(SocketAddress local, int backlogSize) throws IOException {
    int port = InMemoryNet.portOf(local);
    InMemoryNet.bind(port, this);
    this.local = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    return this;
  }

  @Override
  public <T> ServerSocketChannel setOption(SocketOption<T> name, T value) {
    return this;
  }

  @Override
  public <T> T getOption(SocketOption<T> name) {
    return null;
  }

  @Override
  public Set<SocketOption<?>> supportedOptions() {
    return Collections.emptySet();
  }

  @Override
  public synchronized ServerSocket socket() {
    if (serverSocket == null) {
      try {
        serverSocket = new InMemoryServerSocket(this);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return serverSocket;
  }

  @Override
  public SocketChannel accept() throws IOException {
    while (true) {
      synchronized (InMemoryNet.MONITOR) {
        InMemorySocketChannel c = backlog.pollFirst();
        if (c != null) return c;
        if (!isBlocking()) return null;
        try {
          InMemoryNet.MONITOR.wait(50);
        } catch (InterruptedException e) {
          throw new java.nio.channels.ClosedByInterruptException();
        }
      }
    }
  }

  @Override
  public SocketAddress getLocalAddress() {
    return local;
  }

  int localPort() {
    return local == null ? -1 : local.getPort();
  }

  @Override
  protected void implCloseSelectableChannel() {
    if (local != null) InMemoryNet.unbind(local.getPort());
  }

  @Override
  protected void implConfigureBlocking(boolean block) {}
}
