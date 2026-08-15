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
import java.net.ProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;

/**
 * SelectorProvider whose channels never touch the operating system: connections are byte queues
 * between two endpoints inside this JVM. Selected with
 * {@code -Djava.nio.channels.spi.SelectorProvider=inmem.InMemorySelectorProvider}.
 */
public final class InMemorySelectorProvider extends SelectorProvider {
  @Override
  public DatagramChannel openDatagramChannel() throws IOException {
    throw new IOException("datagram channels are not supported in-JVM");
  }

  @Override
  public DatagramChannel openDatagramChannel(ProtocolFamily family) throws IOException {
    throw new IOException("datagram channels are not supported in-JVM");
  }

  @Override
  public Pipe openPipe() throws IOException {
    throw new IOException("pipes are not supported in-JVM");
  }

  @Override
  public AbstractSelector openSelector() {
    return new InMemorySelector(this);
  }

  @Override
  public ServerSocketChannel openServerSocketChannel() {
    return new InMemoryServerSocketChannel(this);
  }

  @Override
  public SocketChannel openSocketChannel() {
    return new InMemorySocketChannel(this);
  }
}
