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
import java.nio.channels.SocketChannel;

/**
 * Minimal {@link Socket} facade over an in-JVM channel. Every method Kafka's network layer calls is
 * overridden, so the inherited SocketImpl (which needs native sockets) is never touched.
 */
final class InMemorySocket extends Socket {
  private final InMemorySocketChannel channel;
  private int sendBuffer = 1 << 17;
  private int receiveBuffer = 1 << 17;

  InMemorySocket(InMemorySocketChannel channel) {
    this.channel = channel;
  }

  private InetSocketAddress remote() {
    return (InetSocketAddress) channel.getRemoteAddress();
  }

  private InetSocketAddress local() {
    return (InetSocketAddress) channel.getLocalAddress();
  }

  @Override
  public SocketChannel getChannel() {
    return channel;
  }

  @Override
  public InetAddress getInetAddress() {
    InetSocketAddress r = remote();
    return r == null ? null : r.getAddress();
  }

  @Override
  public InetAddress getLocalAddress() {
    InetSocketAddress l = local();
    return l == null ? InetAddress.getLoopbackAddress() : l.getAddress();
  }

  @Override
  public int getPort() {
    InetSocketAddress r = remote();
    return r == null ? 0 : r.getPort();
  }

  @Override
  public int getLocalPort() {
    InetSocketAddress l = local();
    return l == null ? 0 : l.getPort();
  }

  @Override
  public SocketAddress getRemoteSocketAddress() {
    return remote();
  }

  @Override
  public SocketAddress getLocalSocketAddress() {
    return local();
  }

  @Override
  public boolean isConnected() {
    return channel.isConnected();
  }

  @Override
  public boolean isClosed() {
    return !channel.isOpen();
  }

  @Override
  public boolean isBound() {
    return true;
  }

  @Override
  public void setTcpNoDelay(boolean on) {}

  @Override
  public boolean getTcpNoDelay() {
    return true;
  }

  @Override
  public void setKeepAlive(boolean on) {}

  @Override
  public boolean getKeepAlive() {
    return true;
  }

  @Override
  public void setSoLinger(boolean on, int linger) {}

  @Override
  public int getSoLinger() {
    return -1;
  }

  @Override
  public void setSoTimeout(int timeout) {}

  @Override
  public int getSoTimeout() {
    return 0;
  }

  @Override
  public void setSendBufferSize(int size) {
    sendBuffer = size;
  }

  @Override
  public int getSendBufferSize() {
    return sendBuffer;
  }

  @Override
  public void setReceiveBufferSize(int size) {
    receiveBuffer = size;
  }

  @Override
  public int getReceiveBufferSize() {
    return receiveBuffer;
  }

  @Override
  public void setReuseAddress(boolean on) {}

  @Override
  public boolean getReuseAddress() {
    return true;
  }

  @Override
  public void shutdownInput() throws IOException {
    channel.shutdownInput();
  }

  @Override
  public void shutdownOutput() throws IOException {
    channel.shutdownOutput();
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  @Override
  public String toString() {
    return "InMemorySocket[" + local() + " -> " + remote() + "]";
  }
}
