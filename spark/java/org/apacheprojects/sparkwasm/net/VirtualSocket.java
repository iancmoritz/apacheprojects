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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

/**
 * The {@link Socket} view of a {@link VirtualSocketChannel}, which Netty's channel config needs.
 *
 * <p>Every accessor is overridden so nothing reaches the JDK's native socket implementation: option
 * values are remembered in fields and addresses come from the channel.
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
