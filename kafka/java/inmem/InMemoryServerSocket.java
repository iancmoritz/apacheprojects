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
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;

/** Minimal {@link ServerSocket} facade over an in-JVM listening channel. */
final class InMemoryServerSocket extends ServerSocket {
  private final InMemoryServerSocketChannel channel;
  private int receiveBuffer = 1 << 17;

  InMemoryServerSocket(InMemoryServerSocketChannel channel) throws java.io.IOException {
    this.channel = channel;
  }

  @Override
  public ServerSocketChannel getChannel() {
    return channel;
  }

  @Override
  public void bind(SocketAddress endpoint, int backlog) throws IOException {
    channel.bind(endpoint, backlog);
  }

  @Override
  public void bind(SocketAddress endpoint) throws IOException {
    channel.bind(endpoint, 50);
  }

  @Override
  public boolean isBound() {
    return channel.getLocalAddress() != null;
  }

  @Override
  public int getLocalPort() {
    return channel.localPort();
  }

  @Override
  public SocketAddress getLocalSocketAddress() {
    return channel.getLocalAddress();
  }

  @Override
  public InetAddress getInetAddress() {
    return InetAddress.getLoopbackAddress();
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
  public void setSoTimeout(int timeout) {}

  @Override
  public int getSoTimeout() {
    return 0;
  }

  @Override
  public boolean isClosed() {
    return !channel.isOpen();
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }
}
