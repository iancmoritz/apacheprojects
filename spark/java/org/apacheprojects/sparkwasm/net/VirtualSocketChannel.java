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

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** One end of a virtual connection: reads from one {@link VirtualNet.Pipe} and writes to another. */
public final class VirtualSocketChannel extends SocketChannel implements VirtualSelectableChannel {

  private static final Set<SocketOption<?>> OPTIONS =
      new HashSet<>(
          Arrays.asList(
              StandardSocketOptions.SO_SNDBUF,
              StandardSocketOptions.SO_RCVBUF,
              StandardSocketOptions.SO_KEEPALIVE,
              StandardSocketOptions.SO_REUSEADDR,
              StandardSocketOptions.SO_LINGER,
              StandardSocketOptions.TCP_NODELAY,
              StandardSocketOptions.IP_TOS));

  private final Map<SocketOption<?>, Object> options = new HashMap<>();

  private VirtualNet.Pipe in;
  private VirtualNet.Pipe out;
  private InetSocketAddress localAddress;
  private InetSocketAddress remoteAddress;
  private boolean connected;
  private boolean inputShutdown;
  private boolean outputShutdown;
  private Socket socketAdapter;

  VirtualSocketChannel(SelectorProvider provider) {
    super(provider);
  }

  VirtualSocketChannel(
      SelectorProvider provider,
      VirtualNet.Pipe in,
      VirtualNet.Pipe out,
      InetSocketAddress localAddress,
      InetSocketAddress remoteAddress) {
    super(provider);
    this.in = in;
    this.out = out;
    this.localAddress = localAddress;
    this.remoteAddress = remoteAddress;
    this.connected = true;
  }

  @Override
  public boolean connect(SocketAddress remote) throws IOException {
    if (connected) throw new AlreadyConnectedException();
    InetSocketAddress isa = (InetSocketAddress) remote;
    VirtualServerSocketChannel listener = VirtualNet.listenerFor(isa.getPort());
    if (listener == null) {
      throw new ConnectException("Connection refused: no in-tab listener on port " + isa.getPort());
    }
    VirtualNet.Pipe clientToServer = new VirtualNet.Pipe();
    VirtualNet.Pipe serverToClient = new VirtualNet.Pipe();
    synchronized (VirtualNet.LOCK) {
      this.in = serverToClient;
      this.out = clientToServer;
      if (localAddress == null) {
        localAddress = new InetSocketAddress(VirtualNet.loopback(), VirtualNet.ephemeralPort());
      }
      this.remoteAddress = isa;
      this.connected = true;
    }
    listener.enqueue(
        new VirtualSocketChannel(provider(), clientToServer, serverToClient, isa, localAddress));
    VirtualNet.signal();
    return true;
  }

  @Override
  public boolean finishConnect() {
    return connected;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public boolean isConnectionPending() {
    return false;
  }

  @Override
  public SocketChannel bind(SocketAddress local) {
    InetSocketAddress isa = (InetSocketAddress) local;
    localAddress =
        isa == null
            ? new InetSocketAddress(VirtualNet.loopback(), VirtualNet.ephemeralPort())
            : isa;
    return this;
  }

  @Override
  public SocketAddress getLocalAddress() {
    return localAddress;
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return remoteAddress;
  }

  @Override
  public <T> SocketChannel setOption(SocketOption<T> name, T value) {
    options.put(name, value);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getOption(SocketOption<T> name) {
    Object value = options.get(name);
    if (value != null) return (T) value;
    if (name == StandardSocketOptions.SO_SNDBUF || name == StandardSocketOptions.SO_RCVBUF) {
      return (T) Integer.valueOf(VirtualNet.PIPE_CAPACITY);
    }
    if (name == StandardSocketOptions.SO_LINGER) return (T) Integer.valueOf(-1);
    if (name == StandardSocketOptions.IP_TOS) return (T) Integer.valueOf(0);
    return (T) Boolean.FALSE;
  }

  @Override
  public Set<SocketOption<?>> supportedOptions() {
    return OPTIONS;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    ensureConnected();
    while (true) {
      if (inputShutdown) return -1;
      int n = in.read(dst);
      if (n != 0 || !isBlocking() || !dst.hasRemaining()) return n;
      await();
    }
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    long total = 0;
    for (int i = offset; i < offset + length; i++) {
      if (!dsts[i].hasRemaining()) continue;
      int n = read(dsts[i]);
      if (n < 0) return total > 0 ? total : -1;
      total += n;
      if (dsts[i].hasRemaining()) break;
    }
    return total;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    ensureConnected();
    int written = 0;
    while (true) {
      if (outputShutdown) throw new ClosedChannelException();
      written += out.write(src);
      if (!src.hasRemaining() || !isBlocking()) return written;
      await();
    }
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    long total = 0;
    for (int i = offset; i < offset + length; i++) {
      if (!srcs[i].hasRemaining()) continue;
      total += write(srcs[i]);
      if (srcs[i].hasRemaining()) break;
    }
    return total;
  }

  @Override
  public SocketChannel shutdownInput() {
    inputShutdown = true;
    VirtualNet.signal();
    return this;
  }

  @Override
  public SocketChannel shutdownOutput() {
    outputShutdown = true;
    if (out != null) out.closeWrite();
    return this;
  }

  @Override
  public synchronized Socket socket() {
    if (socketAdapter == null) socketAdapter = VirtualSocket.forChannel(this);
    return socketAdapter;
  }

  @Override
  protected void implCloseSelectableChannel() {
    synchronized (VirtualNet.LOCK) {
      connected = false;
      inputShutdown = true;
      outputShutdown = true;
    }
    if (out != null) out.closeWrite();
    VirtualNet.signal();
  }

  @Override
  protected void implConfigureBlocking(boolean block) {}

  @Override
  public int readyOps(int interest) {
    int ready = 0;
    if ((interest & SelectionKey.OP_CONNECT) != 0 && connected) ready |= SelectionKey.OP_CONNECT;
    if ((interest & SelectionKey.OP_READ) != 0 && ((in != null && in.readable()) || !isOpen())) {
      ready |= SelectionKey.OP_READ;
    }
    if ((interest & SelectionKey.OP_WRITE) != 0 && out != null && out.writable()) {
      ready |= SelectionKey.OP_WRITE;
    }
    return ready;
  }

  private void ensureConnected() throws IOException {
    if (!isOpen()) throw new ClosedChannelException();
    if (!connected) throw new NotYetConnectedException();
  }

  private void await() throws IOException {
    synchronized (VirtualNet.LOCK) {
      try {
        VirtualNet.LOCK.wait(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted", e);
      }
    }
  }

  @Override
  public String toString() {
    return "VirtualSocketChannel[" + localAddress + " -> " + remoteAddress + "]";
  }
}
