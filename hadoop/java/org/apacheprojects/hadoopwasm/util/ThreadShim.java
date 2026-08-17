/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apacheprojects.hadoopwasm.util;

import java.util.concurrent.locks.LockSupport;

/**
 * Makes {@link Thread#interrupt()} wake a thread that CheerpJ has parked.
 *
 * <p>CheerpJ 4.2 sets the interrupt flag and wakes {@code Object.wait}, but a thread waiting in an
 * untimed {@code Condition.await} -- that is, parked through {@code LockSupport.park}, which is what
 * every {@code java.util.concurrent} lock uses -- is never woken, so the interrupt is not noticed
 * until something else unparks it. The probe page (../../../../../probe.html) shows both halves:
 * "Thread.interrupt wakes an untimed Condition.await" times out with the peer still alive, and the
 * same check with an added {@code unpark} finishes in 300ms.
 *
 * <p>MapReduce cannot finish a map task without this. {@code MapTask.MapOutputBuffer.flush()} ends
 * its spill thread with {@code spillThread.interrupt()} followed by {@code spillThread.join()}, and
 * the spill thread waits on {@code spillReady.await()} -- so on an unpatched CheerpJ the join never
 * returns and the job stops after "Finished spill 0" with the task still in its sort phase.
 *
 * <p>Unparking is safe for the receiving thread either way: {@code LockSupport.park} is specified to
 * return spuriously, so every caller loops on its own condition, and here the loop then sees the
 * interrupt flag that {@code interrupt()} has already set. {@link
 * org.apacheprojects.hadoopwasm.tools.HostCallPatcher} rewrites Hadoop's call sites to this.
 */
public final class ThreadShim {
  private ThreadShim() {}

  /** Whether to add the unpark; {@code -Dhadoopwasm.unpark=false} leaves CheerpJ's behaviour alone. */
  private static final boolean UNPARK = !"false".equals(System.getProperty("hadoopwasm.unpark"));

  /**
   * {@code thread.interrupt()}, then an unpark so a parked thread notices it.
   *
   * <p>CheerpJ's {@code unpark} throws {@code NullPointerException} for a thread it has no parked
   * state for -- one that has already finished, or has never parked -- where the JDK's is specified
   * to do nothing. {@code Task.done} interrupts its finished ping thread on exactly that path, so the
   * unpark is best effort: the interrupt above it is what Hadoop asked for.
   */
  public static void interrupt(Thread thread) {
    thread.interrupt();
    if (!UNPARK) {
      return;
    }
    try {
      LockSupport.unpark(thread);
    } catch (NullPointerException nothingParked) {
      return;
    }
  }
}
