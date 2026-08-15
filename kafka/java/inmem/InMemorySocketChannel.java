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
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.Set;

/** A loopback SocketChannel: both endpoints live in this JVM, backed by two byte queues. */
public final class InMemorySocketChannel extends SocketChannel {
  private InMemoryPipe in;
  private InMemoryPipe out;
  private InMemorySocketChannel peer;
  private volatile boolean connected;
  private volatile boolean connectPending;
  private InetSocketAddress remote;
  private InetSocketAddress local;
  private Socket socket;

  InMemorySocketChannel(SelectorProvider provider) {
    super(provider);
  }

  private void wire(InMemorySocketChannel peer, InMemoryPipe in, InMemoryPipe out, InetSocketAddress local,
      InetSocketAddress remote) {
    this.peer = peer;
    this.in = in;
    this.out = out;
    this.local = local;
    this.remote = remote;
    this.connected = true;
    this.connectPending = false;
  }

  // --- readiness, used by the selector ------------------------------------------------------

  boolean readReady() {
    return connected && in != null && (in.available() > 0 || in.eof());
  }

  boolean writeReady() {
    return connected && out != null && out.writable();
  }

  boolean connectReady() {
    return connected;
  }

  // --- SocketChannel -----------------------------------------------------------------------

  @Override
  public SocketChannel bind(SocketAddress local) {
    this.local = (InetSocketAddress) local;
    return this;
  }

  @Override
  public <T> SocketChannel setOption(SocketOption<T> name, T value) {
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
  public SocketChannel shutdownInput() {
    if (in != null) in.closeWrite();
    return this;
  }

  @Override
  public SocketChannel shutdownOutput() {
    if (out != null) out.closeWrite();
    return this;
  }

  @Override
  public synchronized Socket socket() {
    if (socket == null) socket = new InMemorySocket(this);
    return socket;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public boolean isConnectionPending() {
    return connectPending;
  }

  @Override
  public boolean connect(SocketAddress remote) throws IOException {
    if (connected) throw new IOException("already connected");
    int port = InMemoryNet.portOf(remote);
    InMemoryServerSocketChannel listener = InMemoryNet.listener(port);
    if (listener == null) {
      throw new java.net.ConnectException("Connection refused: no in-JVM listener on port " + port);
    }
    InetSocketAddress serverAddr = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    InetSocketAddress clientAddr =
        new InetSocketAddress(InetAddress.getLoopbackAddress(), InMemoryNet.nextEphemeralPort());
    InMemoryPipe toServer = new InMemoryPipe(1 << 20);
    InMemoryPipe toClient = new InMemoryPipe(1 << 20);
    InMemorySocketChannel server = new InMemorySocketChannel(provider());
    wire(server, toClient, toServer, clientAddr, serverAddr);
    server.wire(this, toServer, toClient, serverAddr, clientAddr);
    listener.enqueue(server);
    InMemoryNet.signal();
    return true;
  }

  @Override
  public boolean finishConnect() {
    return connected;
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return remote;
  }

  @Override
  public SocketAddress getLocalAddress() {
    return local;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (!isOpen()) throw new ClosedChannelException();
    if (!connected) throw new java.nio.channels.NotYetConnectedException();
    while (true) {
      int n = in.read(dst);
      if (n != 0 || !isBlocking()) return n;
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
      if (n == 0) break;
    }
    return total;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (!isOpen()) throw new ClosedChannelException();
    if (!connected) throw new java.nio.channels.NotYetConnectedException();
    while (true) {
      int n = out.write(src);
      if (n != 0 || !isBlocking() || !src.hasRemaining()) return n;
      await();
    }
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    long total = 0;
    for (int i = offset; i < offset + length; i++) {
      if (!srcs[i].hasRemaining()) continue;
      int n = write(srcs[i]);
      total += n;
      if (srcs[i].hasRemaining()) break;
    }
    return total;
  }

  private void await() throws IOException {
    synchronized (InMemoryNet.MONITOR) {
      try {
        InMemoryNet.MONITOR.wait(50);
      } catch (InterruptedException e) {
        throw new java.nio.channels.ClosedByInterruptException();
      }
    }
  }

  @Override
  protected void implCloseSelectableChannel() {
    connected = false;
    if (out != null) out.closeWrite();
    if (in != null) in.closeWrite();
    InMemoryNet.signal();
  }

  @Override
  protected void implConfigureBlocking(boolean block) {}
}
