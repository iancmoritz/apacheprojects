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

package org.apacheprojects.flinkwasm.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetBoundException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** A listener on the virtual loopback: {@link #accept()} pops connections queued by connectors. */
public final class VirtualServerSocketChannel extends ServerSocketChannel
    implements VirtualSelectableChannel {

  private static final Set<SocketOption<?>> OPTIONS =
      new HashSet<>(
          Arrays.asList(
              StandardSocketOptions.SO_RCVBUF,
              StandardSocketOptions.SO_REUSEADDR,
              StandardSocketOptions.IP_TOS));

  private final Map<SocketOption<?>, Object> options = new HashMap<>();
  private final Deque<VirtualSocketChannel> pending = new ArrayDeque<>();

  private InetSocketAddress localAddress;
  private int boundPort = -1;
  private ServerSocket socketAdapter;

  VirtualServerSocketChannel(SelectorProvider provider) {
    super(provider);
  }

  @Override
  public ServerSocketChannel bind(SocketAddress local, int backlog) throws IOException {
    if (!isOpen()) throw new ClosedChannelException();
    if (boundPort >= 0) throw new AlreadyBoundException();
    InetSocketAddress isa = (InetSocketAddress) local;
    int requested = isa == null ? 0 : isa.getPort();
    boundPort = VirtualNet.bind(requested, this);
    localAddress =
        new InetSocketAddress(
            isa == null || isa.getAddress() == null ? VirtualNet.loopback() : isa.getAddress(),
            boundPort);
    return this;
  }

  @Override
  public SocketAddress getLocalAddress() {
    return localAddress;
  }

  @Override
  public <T> ServerSocketChannel setOption(SocketOption<T> name, T value) {
    options.put(name, value);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getOption(SocketOption<T> name) {
    Object value = options.get(name);
    if (value != null) return (T) value;
    if (name == StandardSocketOptions.SO_RCVBUF) {
      return (T) Integer.valueOf(VirtualNet.PIPE_CAPACITY);
    }
    if (name == StandardSocketOptions.IP_TOS) return (T) Integer.valueOf(0);
    return (T) Boolean.FALSE;
  }

  @Override
  public Set<SocketOption<?>> supportedOptions() {
    return OPTIONS;
  }

  /** Hands the server end of a freshly created connection to this listener. */
  void enqueue(VirtualSocketChannel accepted) {
    synchronized (VirtualNet.LOCK) {
      pending.add(accepted);
      VirtualNet.LOCK.notifyAll();
    }
  }

  @Override
  public SocketChannel accept() throws IOException {
    if (!isOpen()) throw new ClosedChannelException();
    if (boundPort < 0) throw new NotYetBoundException();
    while (true) {
      synchronized (VirtualNet.LOCK) {
        VirtualSocketChannel accepted = pending.poll();
        if (accepted != null) return accepted;
        if (!isBlocking()) return null;
        try {
          VirtualNet.LOCK.wait(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("interrupted", e);
        }
      }
      if (!isOpen()) throw new ClosedChannelException();
    }
  }

  @Override
  public synchronized ServerSocket socket() {
    if (socketAdapter == null) socketAdapter = VirtualServerSocket.forChannel(this);
    return socketAdapter;
  }

  @Override
  protected void implCloseSelectableChannel() {
    if (boundPort >= 0) VirtualNet.unbind(boundPort, this);
    synchronized (VirtualNet.LOCK) {
      pending.clear();
    }
    VirtualNet.signal();
  }

  @Override
  protected void implConfigureBlocking(boolean block) {}

  @Override
  public int readyOps(int interest) {
    synchronized (VirtualNet.LOCK) {
      boolean acceptable = !pending.isEmpty();
      return (interest & SelectionKey.OP_ACCEPT) != 0 && acceptable ? SelectionKey.OP_ACCEPT : 0;
    }
  }

  @Override
  public String toString() {
    return "VirtualServerSocketChannel[" + localAddress + "]";
  }
}
