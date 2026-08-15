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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A Selector over in-JVM channels. Readiness is recomputed by polling the channel state; blocking
 * selects wait on {@link InMemoryNet#MONITOR}, which every state change notifies.
 */
public final class InMemorySelector extends AbstractSelector {
  private final Set<SelectionKey> keys = new HashSet<>();
  private final Set<SelectionKey> selectedKeys = new HashSet<>();
  private volatile boolean wakeupPending;

  InMemorySelector(SelectorProvider provider) {
    super(provider);
  }

  @Override
  protected SelectionKey register(AbstractSelectableChannel ch, int ops, Object att) {
    InMemorySelectionKey key = new InMemorySelectionKey(this, ch, ops, att);
    synchronized (keys) {
      keys.add(key);
    }
    InMemoryNet.signal();
    return key;
  }

  @Override
  public Set<SelectionKey> keys() {
    synchronized (keys) {
      return Collections.unmodifiableSet(new HashSet<>(keys));
    }
  }

  @Override
  public Set<SelectionKey> selectedKeys() {
    return selectedKeys;
  }

  @Override
  public int selectNow() throws IOException {
    return doSelect(0, false);
  }

  @Override
  public int select(long timeout) throws IOException {
    if (timeout < 0) throw new IllegalArgumentException("negative timeout");
    return doSelect(timeout, true);
  }

  @Override
  public int select() throws IOException {
    return doSelect(0, true);
  }

  @Override
  public Selector wakeup() {
    wakeupPending = true;
    InMemoryNet.signal();
    return this;
  }

  private void processDeregisterQueue() {
    Set<SelectionKey> cancelled = cancelledKeys();
    synchronized (cancelled) {
      if (cancelled.isEmpty()) return;
      for (Iterator<SelectionKey> it = cancelled.iterator(); it.hasNext();) {
        SelectionKey k = it.next();
        synchronized (keys) {
          keys.remove(k);
        }
        selectedKeys.remove(k);
        deregister((java.nio.channels.spi.AbstractSelectionKey) k);
        it.remove();
      }
    }
  }

  private int updateReady() {
    int updated = 0;
    synchronized (keys) {
      for (SelectionKey k : keys) {
        InMemorySelectionKey key = (InMemorySelectionKey) k;
        if (!key.isValid()) continue;
        int ready = key.computeReadyOps();
        if (ready != 0) {
          boolean fresh = !selectedKeys.contains(key) || key.readyOps() != ready;
          key.readyOps(ready);
          if (selectedKeys.add(key) || fresh) updated++;
        }
      }
    }
    return updated;
  }

  private int doSelect(long timeout, boolean blocking) throws IOException {
    if (!isOpen()) throw new java.nio.channels.ClosedSelectorException();
    processDeregisterQueue();
    int n = updateReady();
    if (n > 0 || !blocking || consumeWakeup()) return n;

    long deadline = timeout == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeout;
    begin();
    try {
      while (true) {
        synchronized (InMemoryNet.MONITOR) {
          long remaining = deadline == Long.MAX_VALUE ? 50 : deadline - System.currentTimeMillis();
          if (remaining <= 0) break;
          try {
            InMemoryNet.MONITOR.wait(Math.min(remaining, 50));
          } catch (InterruptedException e) {
            break;
          }
        }
        processDeregisterQueue();
        n = updateReady();
        if (n > 0 || consumeWakeup()) break;
        if (deadline != Long.MAX_VALUE && System.currentTimeMillis() >= deadline) break;
      }
    } finally {
      end();
    }
    return n;
  }

  private boolean consumeWakeup() {
    if (!wakeupPending) return false;
    wakeupPending = false;
    return true;
  }

  @Override
  protected void implCloseSelector() {
    InMemoryNet.signal();
    synchronized (keys) {
      for (SelectionKey k : new HashSet<>(keys)) {
        k.cancel();
      }
    }
    processDeregisterQueue();
  }
}
