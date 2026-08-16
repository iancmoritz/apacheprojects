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
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;

/**
 * The {@link ServerSocket} view of a {@link VirtualServerSocketChannel}, for Netty's channel config.
 *
 * <p>As with {@link VirtualSocket}, every accessor is overridden so no call reaches the JDK's native
 * socket implementation.
 */
final class VirtualServerSocket extends ServerSocket {

  private final VirtualServerSocketChannel channel;

  private boolean reuseAddress = true;
  private int soTimeout;
  private int receiveBufferSize = VirtualNet.PIPE_CAPACITY;

  private VirtualServerSocket(VirtualServerSocketChannel channel) throws IOException {
    this.channel = channel;
  }

  static ServerSocket forChannel(VirtualServerSocketChannel channel) {
    try {
      return new VirtualServerSocket(channel);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public ServerSocketChannel getChannel() {
    return channel;
  }

  @Override
  public Socket accept() throws IOException {
    return channel.accept().socket();
  }

  @Override
  public void bind(SocketAddress endpoint, int backlog) throws IOException {
    channel.bind(endpoint, backlog);
  }

  @Override
  public void bind(SocketAddress endpoint) throws IOException {
    channel.bind(endpoint, 0);
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
  public SocketAddress getLocalSocketAddress() {
    return channel.getLocalAddress();
  }

  @Override
  public InetAddress getInetAddress() {
    InetSocketAddress local = (InetSocketAddress) channel.getLocalAddress();
    return local == null ? null : local.getAddress();
  }

  @Override
  public int getLocalPort() {
    InetSocketAddress local = (InetSocketAddress) channel.getLocalAddress();
    return local == null ? -1 : local.getPort();
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
  public void setSoTimeout(int timeout) {
    soTimeout = timeout;
  }

  @Override
  public int getSoTimeout() {
    return soTimeout;
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
    return "VirtualServerSocket[" + channel + "]";
  }
}
