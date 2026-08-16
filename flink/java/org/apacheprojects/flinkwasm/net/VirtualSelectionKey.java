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

import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AbstractSelectionKey;

/** The registration of one virtual channel with one {@link VirtualSelector}. */
final class VirtualSelectionKey extends AbstractSelectionKey {

  private final VirtualSelector selector;
  private final AbstractSelectableChannel channel;

  private volatile int interestOps;
  private volatile int readyOps;

  VirtualSelectionKey(VirtualSelector selector, AbstractSelectableChannel channel, int interestOps) {
    this.selector = selector;
    this.channel = channel;
    this.interestOps = interestOps;
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
    ensureValid();
    return interestOps;
  }

  @Override
  public SelectionKey interestOps(int ops) {
    ensureValid();
    if ((ops & ~channel.validOps()) != 0) {
      throw new IllegalArgumentException("unsupported ops: " + ops);
    }
    interestOps = ops;
    VirtualNet.signal();
    return this;
  }

  @Override
  public int readyOps() {
    ensureValid();
    return readyOps;
  }

  void readyOps(int ops) {
    readyOps = ops;
  }

  /** Like {@link #interestOps()} but usable on a cancelled key, as selection needs. */
  int interestOpsQuietly() {
    return interestOps;
  }

  /** Like {@link #readyOps()} but usable on a cancelled key, as selection needs. */
  int readyOpsQuietly() {
    return readyOps;
  }

  private void ensureValid() {
    if (!isValid()) throw new CancelledKeyException();
  }

  @Override
  public String toString() {
    return "VirtualSelectionKey[" + channel + " interest=" + interestOps + " ready=" + readyOps + "]";
  }
}
