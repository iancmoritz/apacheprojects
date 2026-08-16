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

package org.apacheprojects.hadoopwasm.console;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What the JVM in the tab can actually do, one JDK call at a time.
 *
 * The NameNode and DataNode use a handful of operations that a browser JVM has no obvious way to
 * implement -- positional writes, {@code fsync}, file locks, threads, sockets -- and when one of them
 * misbehaves the failure surfaces deep inside Hadoop, if it surfaces at all. Each check here is the
 * bare JDK call Hadoop makes, run on its own, so the page (../../../../../probe.html, a development
 * tool that is not part of the build) can say which primitive is missing rather than which Hadoop
 * class hung.
 */
public final class Probe {
  private Probe() {}

  private interface Check {
    String run() throws Exception;
  }

  private static final StringBuilder RESULTS = new StringBuilder();

  private static void check(String name, Check check) {
    System.out.println("probe " + name + "...");
    long started = System.currentTimeMillis();
    String status;
    String detail;
    try {
      detail = check.run();
      status = "ok";
    } catch (Throwable failure) {
      status = "failed";
      detail = failure.getClass().getName() + ": " + failure.getMessage();
    }
    if (RESULTS.length() > 0) {
      RESULTS.append(',');
    }
    RESULTS.append(
        Json.object()
            .field("name", name)
            .field("status", status)
            .field("detail", detail)
            .field("ms", System.currentTimeMillis() - started)
            .end());
    System.out.println("probe " + name + ": " + status + " -- " + detail);
  }

  /** Runs every check and returns them as JSON. Each one prints before it runs, so a hang names itself. */
  public static synchronized String all(String directory) {
    RESULTS.setLength(0);
    final File dir = new File(directory);
    dir.mkdirs();

    // A tab that froze took its console output with it, but not its files: whatever the watchdog saw
    // last time is still on disk, and this is the first chance to read it.
    String hung = Watchdog.log();
    if (!hung.isEmpty()) {
      System.out.println("WATCHDOG log from an earlier run of this browser:\n" + hung);
      Watchdog.forget();
    }

    check(
        "File.mkdirs + canWrite",
        () -> dir.isDirectory() + ", writable=" + dir.canWrite());

    check(
        // Whether a file written by an earlier visit is still there, and whether this visit's own
        // write comes back: everything the diagnosis of a frozen tab is built on assumes both.
        "append + fsync survives a reload",
        () -> {
          File file = new File(dir, "durable.log");
          long before = file.length();
          try (java.io.FileOutputStream out = new java.io.FileOutputStream(file, true)) {
            out.write("a line\n".getBytes("UTF-8"));
            out.flush();
            out.getFD().sync();
          }
          return "was " + before + " bytes, now " + file.length();
        });

    check(
        // The edit log and every block file are opened for append; if append starts at the beginning
        // of the file instead of its end, Hadoop's own data is what gets overwritten.
        "FileOutputStream(file, append) really appends",
        () -> {
          File file = new File(dir, "appended");
          file.delete();
          try (java.io.FileOutputStream out = new java.io.FileOutputStream(file)) {
            out.write("AAA".getBytes("UTF-8"));
          }
          try (java.io.FileOutputStream out = new java.io.FileOutputStream(file, true)) {
            out.write("BBB".getBytes("UTF-8"));
          }
          java.io.ByteArrayOutputStream all = new java.io.ByteArrayOutputStream();
          byte[] chunk = new byte[64];
          try (java.io.InputStream in = new java.io.FileInputStream(file)) {
            for (int n = in.read(chunk); n > 0; n = in.read(chunk)) {
              all.write(chunk, 0, n);
            }
          }
          return "content=" + new String(all.toByteArray(), "UTF-8") + ", length()=" + file.length();
        });

    check(
        "-Dhadoopwasm.trace and what the trace file holds",
        () -> {
          org.apacheprojects.hadoopwasm.net.Trace.probe("probe wrote this");
          return "property="
              + System.getProperty("hadoopwasm.trace")
              + ", on="
              + org.apacheprojects.hadoopwasm.net.Trace.on()
              + ", trace holds "
              + org.apacheprojects.hadoopwasm.net.Trace.read().length()
              + " chars";
        });

    check(
        "what is in " + directory,
        () -> {
          String[] names = dir.list();
          return names == null ? "unreadable" : java.util.Arrays.toString(names);
        });

    check(
        "RandomAccessFile write + setLength + seek",
        () -> {
          File file = new File(dir, "raf");
          try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(1024);
            raf.seek(512);
            raf.write("hadoop".getBytes("UTF-8"));
            raf.seek(512);
            byte[] read = new byte[6];
            raf.readFully(read);
            return new String(read, "UTF-8") + ", length=" + raf.length();
          }
        });

    check(
        // What EditLogFileOutputStream.preallocate does to grow the edit log a megabyte at a time.
        "FileChannel.write(buffer, position)",
        () -> {
          File file = new File(dir, "positional");
          try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
              FileChannel channel = raf.getChannel()) {
            ByteBuffer fill = ByteBuffer.wrap(new byte[] {-1, -1, -1, -1, -1, -1, -1, -1});
            int written = channel.write(fill, 4096);
            return "wrote " + written + " at 4096, size=" + channel.size();
          }
        });

    check(
        "FileChannel.read(buffer, position)",
        () -> {
          File file = new File(dir, "positional");
          try (RandomAccessFile raf = new RandomAccessFile(file, "r");
              FileChannel channel = raf.getChannel()) {
            ByteBuffer into = ByteBuffer.allocate(8);
            return "read " + channel.read(into, 4096);
          }
        });

    check(
        "FileChannel.force (fsync)",
        () -> {
          File file = new File(dir, "forced");
          try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
              FileChannel channel = raf.getChannel()) {
            raf.write("x".getBytes("UTF-8"));
            channel.force(true);
            return "forced";
          }
        });

    check(
        // Storage.tryLock, which is how the NameNode claims its storage directory.
        "FileChannel.tryLock",
        () -> {
          File file = new File(dir, "in_use.lock");
          try (RandomAccessFile raf = new RandomAccessFile(file, "rws");
              FileChannel channel = raf.getChannel()) {
            FileLock lock = channel.tryLock();
            String held = lock == null ? "null" : "held";
            if (lock != null) {
              lock.release();
            }
            return held;
          }
        });

    check(
        "Thread.start + join",
        () -> {
          final AtomicInteger counter = new AtomicInteger();
          Thread thread = new Thread(() -> counter.set(41 + 1));
          thread.start();
          thread.join(5000);
          return "counter=" + counter.get() + ", alive=" + thread.isAlive();
        });

    check(
        "Thread daemon + wait/notify",
        () -> {
          final Object monitor = new Object();
          final AtomicInteger state = new AtomicInteger();
          Thread thread =
              new Thread(
                  () -> {
                    synchronized (monitor) {
                      state.set(1);
                      monitor.notifyAll();
                    }
                  });
          thread.setDaemon(true);
          synchronized (monitor) {
            thread.start();
            monitor.wait(5000);
          }
          return "state=" + state.get();
        });

    check(
        // Hadoop's RPC and its block transfer are both one thread waiting on another over a monitor,
        // which is the pattern a cooperatively scheduled JVM is most likely to be unable to run.
        "monitor handoff between two threads, 200 rounds",
        () -> {
          final Object monitor = new Object();
          final AtomicInteger turn = new AtomicInteger();
          Thread peer =
              new Thread(
                  () -> {
                    synchronized (monitor) {
                      while (turn.get() < 400) {
                        if (turn.get() % 2 == 1) {
                          turn.incrementAndGet();
                          monitor.notifyAll();
                        }
                        try {
                          monitor.wait(250);
                        } catch (InterruptedException stop) {
                          return;
                        }
                      }
                    }
                  });
          peer.setDaemon(true);
          peer.start();
          long deadline = System.currentTimeMillis() + 10_000L;
          synchronized (monitor) {
            while (turn.get() < 400 && System.currentTimeMillis() < deadline) {
              if (turn.get() % 2 == 0) {
                turn.incrementAndGet();
                monitor.notifyAll();
              }
              monitor.wait(250);
            }
          }
          return "rounds=" + turn.get() / 2 + " of 200";
        });

    check(
        // MapTask hands its spill off over a ReentrantLock and two Conditions, which is a different
        // mechanism from a monitor: AbstractQueuedSynchronizer parks and unparks threads directly.
        "ReentrantLock + Condition handoff, 200 rounds",
        () -> {
          final java.util.concurrent.locks.ReentrantLock lock =
              new java.util.concurrent.locks.ReentrantLock();
          final java.util.concurrent.locks.Condition changed = lock.newCondition();
          final AtomicInteger turn = new AtomicInteger();
          Thread peer =
              new Thread(
                  () -> {
                    lock.lock();
                    try {
                      while (turn.get() < 400) {
                        if (turn.get() % 2 == 1) {
                          turn.incrementAndGet();
                          changed.signalAll();
                        }
                        changed.await(250, java.util.concurrent.TimeUnit.MILLISECONDS);
                      }
                    } catch (InterruptedException stop) {
                      return;
                    } finally {
                      lock.unlock();
                    }
                  });
          peer.setDaemon(true);
          peer.start();
          long deadline = System.currentTimeMillis() + 10_000L;
          lock.lock();
          try {
            while (turn.get() < 400 && System.currentTimeMillis() < deadline) {
              if (turn.get() % 2 == 0) {
                turn.incrementAndGet();
                changed.signalAll();
              }
              changed.await(250, java.util.concurrent.TimeUnit.MILLISECONDS);
            }
          } finally {
            lock.unlock();
          }
          return "rounds=" + turn.get() / 2 + " of 200";
        });

    check(
        // The same handoff with no timeout on the wait: MapTask.flush() calls spillDone.await(), so if
        // an untimed park never wakes, the map task stops exactly where this check would.
        "Condition.await with no timeout wakes on signal",
        () -> {
          final java.util.concurrent.locks.ReentrantLock lock =
              new java.util.concurrent.locks.ReentrantLock();
          final java.util.concurrent.locks.Condition done = lock.newCondition();
          final AtomicInteger state = new AtomicInteger();
          Thread peer =
              new Thread(
                  () -> {
                    try {
                      Thread.sleep(200L);
                    } catch (InterruptedException stop) {
                      return;
                    }
                    lock.lock();
                    try {
                      state.set(1);
                      done.signalAll();
                    } finally {
                      lock.unlock();
                    }
                  });
          peer.setDaemon(true);
          peer.start();
          lock.lock();
          try {
            while (state.get() == 0) {
              done.await();
            }
          } finally {
            lock.unlock();
          }
          return "state=" + state.get();
        });

    check(
        // How MapTask ends its spill thread: interrupt a thread waiting on a Condition, then join it.
        // If the interrupt does not wake the wait, flush() never returns and the map task stops there.
        "Thread.interrupt wakes an untimed Condition.await",
        () -> {
          final java.util.concurrent.locks.ReentrantLock lock =
              new java.util.concurrent.locks.ReentrantLock();
          final java.util.concurrent.locks.Condition never = lock.newCondition();
          final AtomicInteger state = new AtomicInteger();
          Thread peer =
              new Thread(
                  () -> {
                    lock.lock();
                    try {
                      never.await();
                    } catch (InterruptedException stop) {
                      state.set(1);
                    } finally {
                      lock.unlock();
                    }
                  });
          peer.setDaemon(true);
          peer.start();
          Thread.sleep(300L);
          peer.interrupt();
          peer.join(5000);
          return "interrupted=" + state.get() + ", alive=" + peer.isAlive();
        });

    check(
        // The same interrupt followed by an unpark: if the flag is set but the park is not broken,
        // unparking the thread is enough for AbstractQueuedSynchronizer to notice and throw.
        "Thread.interrupt + LockSupport.unpark wakes an untimed Condition.await",
        () -> {
          final java.util.concurrent.locks.ReentrantLock lock =
              new java.util.concurrent.locks.ReentrantLock();
          final java.util.concurrent.locks.Condition never = lock.newCondition();
          final AtomicInteger state = new AtomicInteger();
          Thread peer =
              new Thread(
                  () -> {
                    lock.lock();
                    try {
                      never.await();
                    } catch (InterruptedException stop) {
                      state.set(1);
                    } finally {
                      lock.unlock();
                    }
                  });
          peer.setDaemon(true);
          peer.start();
          Thread.sleep(300L);
          peer.interrupt();
          java.util.concurrent.locks.LockSupport.unpark(peer);
          peer.join(5000);
          return "interrupted=" + state.get() + ", alive=" + peer.isAlive();
        });

    check(
        // The same, for a thread in Object.wait(), which is how the DataNode's threads are stopped.
        "Thread.interrupt wakes an untimed Object.wait",
        () -> {
          final Object monitor = new Object();
          final AtomicInteger state = new AtomicInteger();
          Thread peer =
              new Thread(
                  () -> {
                    synchronized (monitor) {
                      try {
                        monitor.wait();
                      } catch (InterruptedException stop) {
                        state.set(1);
                      }
                    }
                  });
          peer.setDaemon(true);
          peer.start();
          Thread.sleep(300L);
          peer.interrupt();
          peer.join(5000);
          return "interrupted=" + state.get() + ", alive=" + peer.isAlive();
        });

    check(
        // What a Condition is built on, on its own.
        "LockSupport.park wakes on unpark",
        () -> {
          final Thread waiting = Thread.currentThread();
          final AtomicInteger state = new AtomicInteger();
          Thread peer =
              new Thread(
                  () -> {
                    try {
                      Thread.sleep(200L);
                    } catch (InterruptedException stop) {
                      return;
                    }
                    state.set(1);
                    java.util.concurrent.locks.LockSupport.unpark(waiting);
                  });
          peer.setDaemon(true);
          peer.start();
          long deadline = System.currentTimeMillis() + 10_000L;
          while (state.get() == 0 && System.currentTimeMillis() < deadline) {
            java.util.concurrent.locks.LockSupport.parkNanos(500_000_000L);
          }
          return "state=" + state.get();
        });

    check(
        // LocalJobRunner runs its map tasks on an ExecutorService and collects them with Future.get.
        "ExecutorService + Future.get",
        () -> {
          java.util.concurrent.ExecutorService pool =
              java.util.concurrent.Executors.newFixedThreadPool(1);
          try {
            java.util.concurrent.Future<String> future = pool.submit(() -> "ran on " + Thread.currentThread().getName());
            return future.get(10, java.util.concurrent.TimeUnit.SECONDS);
          } finally {
            pool.shutdownNow();
          }
        });

    check(
        // The block transfer path: a listening channel, a thread accepting on it, and streams over the
        // connection -- exactly what the DataNode and DFSClient do to move a block.
        "virtual socket echo through the DataNode's own path",
        () -> {
          final java.nio.channels.ServerSocketChannel listener =
              java.nio.channels.ServerSocketChannel.open();
          listener.socket().bind(new InetSocketAddress("127.0.0.1", 0), 1);
          final int port = listener.socket().getLocalPort();
          final StringBuilder server = new StringBuilder();
          Thread accepting =
              new Thread(
                  () -> {
                    try (Socket peer = listener.socket().accept()) {
                      int byte_ = peer.getInputStream().read();
                      server.append("read=").append(byte_);
                      peer.getOutputStream().write(byte_ + 1);
                      peer.getOutputStream().flush();
                    } catch (Exception failure) {
                      server.append(failure);
                    }
                  });
          accepting.setDaemon(true);
          accepting.start();
          try (Socket client = java.nio.channels.SocketChannel.open().socket()) {
            client.connect(new InetSocketAddress("127.0.0.1", port), 10_000);
            client.setSoTimeout(10_000);
            client.getOutputStream().write(41);
            client.getOutputStream().flush();
            int answer = client.getInputStream().read();
            accepting.join(5000);
            listener.close();
            return "client got " + answer + ", server " + server;
          }
        });

    check(
        "Selector.open (provider "
            + java.nio.channels.spi.SelectorProvider.provider().getClass().getName()
            + ")",
        () -> {
          Selector selector = Selector.open();
          String name = selector.getClass().getName();
          selector.close();
          return name;
        });

    check(
        // The one thing a tab certainly cannot do; here to prove the fallback is needed, not assumed.
        "ServerSocket bind on 127.0.0.1",
        () -> {
          try (ServerSocket server = new ServerSocket()) {
            server.bind(new InetSocketAddress("127.0.0.1", 0), 1);
            int port = server.getLocalPort();
            try (Socket client = new Socket()) {
              client.connect(new InetSocketAddress("127.0.0.1", port), 2000);
              return "connected to " + port;
            }
          }
        });

    check(
        // Shell.execCommand, which Hadoop uses for df, du, chmod and id when libhadoop is missing.
        "Runtime.exec(\"/bin/sh -c echo\")",
        () -> {
          Process process = new ProcessBuilder("/bin/sh", "-c", "echo hadoop").start();
          int code = process.waitFor();
          return "exit=" + code;
        });

    check(
        // NameNodeResourceChecker asks DF how much room the edit log has left.
        "org.apache.hadoop.fs.DF",
        () -> {
          org.apache.hadoop.fs.DF df =
              new org.apache.hadoop.fs.DF(dir, new org.apache.hadoop.conf.Configuration());
          return "filesystem=" + df.getFilesystem() + ", available=" + df.getAvailable();
        });

    check(
        // What the DataNode uses to report how much of its volume is in use.
        "GetSpaceUsed on the storage directory",
        () -> {
          org.apache.hadoop.fs.GetSpaceUsed used =
              new org.apache.hadoop.fs.GetSpaceUsed.Builder()
                  .setPath(dir)
                  .setConf(new org.apache.hadoop.conf.Configuration())
                  .build();
          return "used=" + used.getUsed() + " by " + used.getClass().getName();
        });

    return "[" + RESULTS + "]";
  }
}
