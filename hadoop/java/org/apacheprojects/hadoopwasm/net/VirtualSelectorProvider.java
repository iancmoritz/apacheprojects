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
import java.net.ProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;

/**
 * The NIO provider that replaces the JDK's epoll one under CheerpJ.
 *
 * <p>CheerpJ ships no native networking, so {@code sun.nio.ch.EPollSelectorProvider} dies with
 * {@code UnsatisfiedLinkError: Java_sun_nio_ch_EPoll_eventSize} as soon as Netty opens an event
 * loop. Selecting this provider with {@code -Djava.nio.channels.spi.SelectorProvider} gives Spark's
 * RPC and shuffle stack channels that talk over in-process pipes instead of real sockets, which is
 * all Spark needs in {@code local} mode where every endpoint lives in this one JVM.
 */
public final class VirtualSelectorProvider extends SelectorProvider {

  /** Called reflectively by the JDK; must stay public and no-arg. */
  public VirtualSelectorProvider() {}

  @Override
  public AbstractSelector openSelector() {
    return new VirtualSelector(this);
  }

  @Override
  public SocketChannel openSocketChannel() {
    return new VirtualSocketChannel(this);
  }

  @Override
  public ServerSocketChannel openServerSocketChannel() {
    return new VirtualServerSocketChannel(this);
  }

  @Override
  public DatagramChannel openDatagramChannel() throws IOException {
    throw new IOException("datagram channels are not supported in the browser");
  }

  @Override
  public DatagramChannel openDatagramChannel(ProtocolFamily family) throws IOException {
    throw new IOException("datagram channels are not supported in the browser");
  }

  @Override
  public Pipe openPipe() throws IOException {
    throw new IOException("java.nio.channels.Pipe is not supported in the browser");
  }
}
