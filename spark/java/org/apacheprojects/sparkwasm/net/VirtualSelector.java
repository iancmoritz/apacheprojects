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
import java.nio.channels.ClosedSelectorException;
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
 * A selector over virtual channels, in place of the JDK's epoll one.
 *
 * <p>There are no file descriptors to poll, so selection walks the registered keys and asks each
 * channel what it can do right now ({@link VirtualSelectableChannel#readyOps}). A blocking select
 * waits on {@link VirtualNet#LOCK}, which every pipe write, connect, accept, close and {@link
 * #wakeup()} notifies; the wait is also bounded so a selector can never miss a state change.
 */
final class VirtualSelector extends AbstractSelector {

  private static final long MAX_PARK_MILLIS = 100;

  private final Set<SelectionKey> keys = new HashSet<>();
  private final Set<SelectionKey> selectedKeys = new HashSet<>();
  private final Set<SelectionKey> publicKeys = Collections.unmodifiableSet(keys);

  private boolean wakeupPending;

  VirtualSelector(SelectorProvider provider) {
    super(provider);
  }

  @Override
  protected SelectionKey register(AbstractSelectableChannel channel, int ops, Object attachment) {
    if (!isOpen()) throw new ClosedSelectorException();
    VirtualSelectionKey key = new VirtualSelectionKey(this, channel, ops);
    key.attach(attachment);
    synchronized (VirtualNet.LOCK) {
      keys.add(key);
    }
    return key;
  }

  @Override
  public Set<SelectionKey> keys() {
    if (!isOpen()) throw new ClosedSelectorException();
    return publicKeys;
  }

  @Override
  public Set<SelectionKey> selectedKeys() {
    if (!isOpen()) throw new ClosedSelectorException();
    return selectedKeys;
  }

  @Override
  public int selectNow() throws IOException {
    return doSelect(0);
  }

  @Override
  public int select() throws IOException {
    return doSelect(-1);
  }

  @Override
  public int select(long timeout) throws IOException {
    if (timeout < 0) throw new IllegalArgumentException("negative timeout");
    return doSelect(timeout == 0 ? -1 : timeout);
  }

  /** {@code timeout} is millis to wait, 0 for a poll, or -1 to wait until something is ready. */
  private int doSelect(long timeout) throws IOException {
    if (!isOpen()) throw new ClosedSelectorException();
    long deadline = timeout > 0 ? System.currentTimeMillis() + timeout : -1;
    while (true) {
      dropCancelledKeys();
      int ready = pollChannels();
      if (ready > 0) return ready;
      synchronized (VirtualNet.LOCK) {
        if (wakeupPending) {
          wakeupPending = false;
          return 0;
        }
        if (timeout == 0) return 0;
        long park = MAX_PARK_MILLIS;
        if (deadline > 0) {
          long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0) return 0;
          park = Math.min(park, remaining);
        }
        begin();
        try {
          VirtualNet.LOCK.wait(park);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return 0;
        } finally {
          end();
        }
      }
      if (!isOpen()) throw new ClosedSelectorException();
    }
  }

  /** Refreshes ready ops on every key; returns how many keys newly became ready. */
  private int pollChannels() {
    int newlyReady = 0;
    synchronized (VirtualNet.LOCK) {
      for (SelectionKey key : keys) {
        if (!key.isValid()) continue;
        VirtualSelectionKey virtualKey = (VirtualSelectionKey) key;
        int ready =
            ((VirtualSelectableChannel) key.channel()).readyOps(virtualKey.interestOpsQuietly());
        if (ready == 0) continue;
        boolean alreadySelected = selectedKeys.contains(key);
        virtualKey.readyOps(alreadySelected ? virtualKey.readyOpsQuietly() | ready : ready);
        if (!alreadySelected) {
          selectedKeys.add(key);
          newlyReady++;
        }
      }
    }
    return newlyReady;
  }

  private void dropCancelledKeys() {
    Set<SelectionKey> cancelled = cancelledKeys();
    synchronized (cancelled) {
      if (cancelled.isEmpty()) return;
      for (Iterator<SelectionKey> it = cancelled.iterator(); it.hasNext(); ) {
        SelectionKey key = it.next();
        synchronized (VirtualNet.LOCK) {
          keys.remove(key);
          selectedKeys.remove(key);
        }
        deregister((VirtualSelectionKey) key);
        it.remove();
      }
    }
  }

  @Override
  public Selector wakeup() {
    synchronized (VirtualNet.LOCK) {
      wakeupPending = true;
      VirtualNet.LOCK.notifyAll();
    }
    return this;
  }

  @Override
  protected void implCloseSelector() {
    wakeup();
    synchronized (VirtualNet.LOCK) {
      for (SelectionKey key : new HashSet<>(keys)) {
        ((VirtualSelectionKey) key).cancel();
      }
    }
    dropCancelledKeys();
  }
}
