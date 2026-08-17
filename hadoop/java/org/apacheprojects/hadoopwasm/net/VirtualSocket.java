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

package org.apacheprojects.hadoopwasm.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * The {@link Socket} view of a {@link VirtualSocketChannel}: connecting, the two streams and every
 * accessor.
 *
 * <p>Nothing here may reach the JDK's native socket implementation, which in a tab fails with {@code
 * Operation not permitted} or, worse, with {@code Socket is not connected} from a stream the caller
 * has every reason to expect works. Both matter beyond Netty's channel config: Hadoop's RPC client
 * connects through {@link Socket#connect(SocketAddress, int)}, and the DataNode's block transfer
 * reads and writes through {@link Socket#getInputStream()} and {@link Socket#getOutputStream()}.
 */
final class VirtualSocket extends Socket {

  private final VirtualSocketChannel channel;

  private boolean tcpNoDelay = true;
  private boolean keepAlive;
  private boolean reuseAddress = true;
  private boolean oobInline;
  private int soLinger = -1;
  private int soTimeout;
  private int trafficClass;
  private int sendBufferSize = VirtualNet.PIPE_CAPACITY;
  private int receiveBufferSize = VirtualNet.PIPE_CAPACITY;

  private VirtualSocket(VirtualSocketChannel channel) {
    this.channel = channel;
  }

  static Socket forChannel(VirtualSocketChannel channel) {
    return new VirtualSocket(channel);
  }

  @Override
  public void connect(SocketAddress remote) throws IOException {
    connect(remote, 0);
  }

  @Override
  public void connect(SocketAddress remote, int timeout) throws IOException {
    // The connection is made by handing the listener its end, which either has somewhere to go or
    // does not: there is nothing here for a timeout to wait for.
    channel.connect(remote);
  }

  @Override
  public void bind(SocketAddress local) {
    channel.bind(local);
  }

  @Override
  public InputStream getInputStream() throws IOException {
    ensureUsable();
    return new InputStream() {
      @Override
      public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : one[0] & 0xff;
      }

      @Override
      public int read(byte[] bytes, int offset, int length) throws IOException {
        if (length == 0) return 0;
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
        long deadline = soTimeout > 0 ? System.currentTimeMillis() + soTimeout : 0;
        for (; ; ) {
          int n = channel.read(buffer);
          if (n != 0) return n;
          // A stream read blocks whatever the channel's mode is; Hadoop puts channels it also selects
          // on into non-blocking mode, and those return 0 rather than waiting.
          if (deadline > 0 && System.currentTimeMillis() >= deadline) {
            throw new SocketTimeoutException("Read timed out");
          }
          VirtualNet.pause();
        }
      }

      @Override
      public int available() throws IOException {
        return channel.available();
      }

      @Override
      public void close() throws IOException {
        channel.close();
      }
    };
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    ensureUsable();
    return new OutputStream() {
      @Override
      public void write(int one) throws IOException {
        write(new byte[] {(byte) one}, 0, 1);
      }

      @Override
      public void write(byte[] bytes, int offset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
        while (buffer.hasRemaining()) {
          if (channel.write(buffer) == 0) VirtualNet.pause();
        }
      }

      @Override
      public void close() throws IOException {
        channel.close();
      }
    };
  }

  private void ensureUsable() throws SocketException {
    if (!channel.isOpen()) throw new SocketException("Socket is closed");
    if (!channel.isConnected()) throw new SocketException("Socket is not connected");
  }

  @Override
  public SocketChannel getChannel() {
    return channel;
  }

  @Override
  public boolean isConnected() {
    return channel.isConnected();
  }

  @Override
  public boolean isBound() {
    return channel.getLocalAddress() != null;
  }

  @Override
  public boolean isClosed() {
    return !channel.isOpen();
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  @Override
  public void shutdownInput() {
    channel.shutdownInput();
  }

  @Override
  public void shutdownOutput() {
    channel.shutdownOutput();
  }

  @Override
  public SocketAddress getLocalSocketAddress() {
    return channel.getLocalAddress();
  }

  @Override
  public SocketAddress getRemoteSocketAddress() {
    return channel.getRemoteAddress();
  }

  @Override
  public InetAddress getInetAddress() {
    InetSocketAddress remote = (InetSocketAddress) channel.getRemoteAddress();
    return remote == null ? null : remote.getAddress();
  }

  @Override
  public InetAddress getLocalAddress() {
    InetSocketAddress local = (InetSocketAddress) channel.getLocalAddress();
    return local == null ? VirtualNet.loopback() : local.getAddress();
  }

  @Override
  public int getPort() {
    InetSocketAddress remote = (InetSocketAddress) channel.getRemoteAddress();
    return remote == null ? 0 : remote.getPort();
  }

  @Override
  public int getLocalPort() {
    InetSocketAddress local = (InetSocketAddress) channel.getLocalAddress();
    return local == null ? -1 : local.getPort();
  }

  @Override
  public void setTcpNoDelay(boolean on) {
    tcpNoDelay = on;
  }

  @Override
  public boolean getTcpNoDelay() {
    return tcpNoDelay;
  }

  @Override
  public void setKeepAlive(boolean on) {
    keepAlive = on;
  }

  @Override
  public boolean getKeepAlive() {
    return keepAlive;
  }

  @Override
  public void setReuseAddress(boolean on) {
    reuseAddress = on;
  }

  @Override
  public boolean getReuseAddress() {
    return reuseAddress;
  }

  @Override
  public void setOOBInline(boolean on) {
    oobInline = on;
  }

  @Override
  public boolean getOOBInline() {
    return oobInline;
  }

  @Override
  public void setSoLinger(boolean on, int linger) {
    soLinger = on ? linger : -1;
  }

  @Override
  public int getSoLinger() {
    return soLinger;
  }

  @Override
  public void setSoTimeout(int timeout) {
    soTimeout = timeout;
  }

  @Override
  public int getSoTimeout() {
    return soTimeout;
  }

  @Override
  public void setTrafficClass(int tc) {
    trafficClass = tc;
  }

  @Override
  public int getTrafficClass() {
    return trafficClass;
  }

  @Override
  public void setSendBufferSize(int size) {
    sendBufferSize = size;
  }

  @Override
  public int getSendBufferSize() {
    return sendBufferSize;
  }

  @Override
  public void setReceiveBufferSize(int size) {
    receiveBufferSize = size;
  }

  @Override
  public int getReceiveBufferSize() {
    return receiveBufferSize;
  }

  @Override
  public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {}

  @Override
  public String toString() {
    return "VirtualSocket[" + channel + "]";
  }
}
