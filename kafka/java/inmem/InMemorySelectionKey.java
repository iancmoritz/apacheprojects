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

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AbstractSelectionKey;

final class InMemorySelectionKey extends AbstractSelectionKey {
  private final InMemorySelector selector;
  private final AbstractSelectableChannel channel;
  private volatile int interestOps;
  private volatile int readyOps;

  InMemorySelectionKey(InMemorySelector selector, AbstractSelectableChannel channel, int ops, Object att) {
    this.selector = selector;
    this.channel = channel;
    this.interestOps = ops;
    attach(att);
  }

  @Override
  public SelectableChannel channel() {
    return channel;
  }

  @Override
  public Selector selector() {
    return selector;
  }

  @Override
  public int interestOps() {
    return interestOps;
  }

  @Override
  public SelectionKey interestOps(int ops) {
    if (!isValid()) throw new java.nio.channels.CancelledKeyException();
    this.interestOps = ops;
    InMemoryNet.signal();
    return this;
  }

  @Override
  public int readyOps() {
    return readyOps;
  }

  void readyOps(int ops) {
    this.readyOps = ops;
  }

  /** Readiness of the underlying in-JVM channel, masked by the current interest set. */
  int computeReadyOps() {
    int ready = 0;
    if (channel instanceof InMemoryServerSocketChannel) {
      if (((InMemoryServerSocketChannel) channel).acceptReady()) ready |= OP_ACCEPT;
    } else if (channel instanceof InMemorySocketChannel) {
      InMemorySocketChannel c = (InMemorySocketChannel) channel;
      if (c.readReady()) ready |= OP_READ;
      if (c.writeReady()) ready |= OP_WRITE;
      if (c.connectReady()) ready |= OP_CONNECT;
    }
    return ready & interestOps;
  }
}
