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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.SocketImplFactory;
import java.net.SocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link SocketImpl} over the virtual loopback, so plain {@code java.net} sockets work too.
 *
 * <p>{@link VirtualSelectorProvider} only covers NIO: code that reaches for {@link ServerSocket}
 * directly still lands on the JDK's native implementation, which is how Flink's blob server dies in
 * a tab ({@code Unable to open BLOB Server in specified port range}). Installing this through {@link
 * #install()} routes {@code new ServerSocket(...)} and {@code new Socket(...)} through the same
 * in-process pipes the channels use, so the blob server binds a port, its accept loop parks on a
 * real queue, and a {@code BlobClient} in this JVM can connect to it.
 */
final class VirtualSocketImpl extends SocketImpl {

  private static boolean installed;

  private final Map<Integer, Object> options = new HashMap<>();
  private final SelectorProvider provider = new VirtualSelectorProvider();

  private VirtualServerSocketChannel listener;
  private VirtualSocketChannel channel;
  private InputStream input;
  private OutputStream output;

  /** Makes every {@code java.net} socket in this JVM a virtual one. Idempotent. */
  static synchronized void install() throws IOException {
    if (installed) return;
    installed = true;
    // Anonymous classes, not lambdas: CheerpJ cannot link a lambda call site this early in boot.
    ServerSocket.setSocketFactory(
        new SocketImplFactory() {
          @Override
          public SocketImpl createSocketImpl() {
            return new VirtualSocketImpl();
          }
        });
    Socket.setSocketImplFactory(
        new SocketImplFactory() {
          @Override
          public SocketImpl createSocketImpl() {
            return new VirtualSocketImpl();
          }
        });
  }

  @Override
  protected void create(boolean stream) {
    if (!stream) throw new UnsupportedOperationException("datagram sockets are not virtualised");
  }

  @Override
  protected void bind(InetAddress host, int port) throws IOException {
    listener = (VirtualServerSocketChannel) provider.openServerSocketChannel();
    listener.bind(new InetSocketAddress(host, port), 0);
    this.address = host;
    this.localport = ((InetSocketAddress) listener.getLocalAddress()).getPort();
  }

  @Override
  protected void listen(int backlog) {}

  @Override
  protected void accept(SocketImpl target) throws IOException {
    if (listener == null) throw new SocketException("socket is not bound");
    VirtualSocketImpl accepted = (VirtualSocketImpl) target;
    accepted.channel = (VirtualSocketChannel) listener.accept();
    InetSocketAddress remote = (InetSocketAddress) accepted.channel.getRemoteAddress();
    accepted.address = remote.getAddress();
    accepted.port = remote.getPort();
    accepted.localport = localport;
  }

  @Override
  protected void connect(String host, int port) throws IOException {
    connect(new InetSocketAddress(host, port), 0);
  }

  @Override
  protected void connect(InetAddress address, int port) throws IOException {
    connect(new InetSocketAddress(address, port), 0);
  }

  @Override
  @SuppressWarnings("unused")
  protected void connect(SocketAddress remote, int timeout) throws IOException {
    InetSocketAddress isa = (InetSocketAddress) remote;
    channel = (VirtualSocketChannel) provider.openSocketChannel();
    channel.connect(isa);
    this.address = isa.getAddress();
    this.port = isa.getPort();
    this.localport = ((InetSocketAddress) channel.getLocalAddress()).getPort();
  }

  @Override
  protected synchronized InputStream getInputStream() throws IOException {
    if (channel == null) throw new SocketException("socket is not connected");
    if (input == null) {
      input =
          new InputStream() {
            private final byte[] one = new byte[1];

            @Override
            public int read() throws IOException {
              int n = read(one, 0, 1);
              // A single-byte read returns an unsigned value, which sign extension would corrupt.
              return n < 0 ? -1 : one[0] & 0xff;
            }

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
              if (length == 0) return 0;
              int n = channel.read(ByteBuffer.wrap(bytes, offset, length));
              return n <= 0 ? -1 : n;
            }

            @Override
            public int available() {
              return 0;
            }

            @Override
            public void close() throws IOException {
              channel.shutdownInput();
            }
          };
    }
    return input;
  }

  @Override
  protected synchronized OutputStream getOutputStream() throws IOException {
    if (channel == null) throw new SocketException("socket is not connected");
    if (output == null) {
      output =
          new OutputStream() {
            @Override
            public void write(int value) throws IOException {
              write(new byte[] {(byte) value}, 0, 1);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
              ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
              while (buffer.hasRemaining()) channel.write(buffer);
            }

            @Override
            public void close() throws IOException {
              channel.shutdownOutput();
            }
          };
    }
    return output;
  }

  @Override
  protected int available() {
    return 0;
  }

  @Override
  protected void close() throws IOException {
    if (channel != null) channel.close();
    if (listener != null) listener.close();
  }

  @Override
  protected void sendUrgentData(int data) {
    throw new UnsupportedOperationException("urgent data");
  }

  @Override
  protected void shutdownInput() throws IOException {
    if (channel != null) channel.shutdownInput();
  }

  @Override
  protected void shutdownOutput() throws IOException {
    if (channel != null) channel.shutdownOutput();
  }

  @Override
  protected boolean supportsUrgentData() {
    return false;
  }

  @Override
  public void setOption(int option, Object value) {
    options.put(option, value);
  }

  @Override
  public Object getOption(int option) {
    Object value = options.get(option);
    if (value != null) return value;
    switch (option) {
      case SocketOptions.SO_RCVBUF:
      case SocketOptions.SO_SNDBUF:
        return VirtualNet.PIPE_CAPACITY;
      case SocketOptions.SO_LINGER:
        return -1;
      case SocketOptions.SO_TIMEOUT:
      case SocketOptions.IP_TOS:
        return 0;
      case SocketOptions.SO_BINDADDR:
        return VirtualNet.loopback();
      default:
        return Boolean.FALSE;
    }
  }

  @Override
  public String toString() {
    return "VirtualSocketImpl[" + (listener != null ? listener : channel) + "]";
  }
}
