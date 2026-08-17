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

package org.apache.flink.core.memory;

/**
 * Flink's {@code MemorySegmentFactory}, replaced so that every segment is a heap segment.
 *
 * <p>This class shadows the one in flink-dist: the build merges it into that jar, so all of Flink
 * allocates through it. Every method behaves as upstream except the three that hand out off-heap
 * memory, which hand out heap memory instead.
 *
 * <p>Off-heap segments address raw memory: {@link MemorySegment#getInt(int)} on one calls {@code
 * sun.misc.Unsafe.getInt(null, address)}. CheerpJ implements {@code Unsafe} against Java objects
 * only, so a null reference with an absolute address throws, and the first record that crosses
 * Flink's network stack kills the task with
 *
 * <pre>
 * java.lang.NullPointerException
 *     at org.apache.flink.core.memory.MemorySegment.getInt
 *     at ...NonSpanningWrapper.readInt
 *     at ...SpillingAdaptiveSpanningRecordDeserializer.readNonSpanningRecord
 * </pre>
 *
 * <p>A heap segment holds a {@code byte[]} and reads it with {@code Unsafe.getInt(array, offset)},
 * which CheerpJ does implement. Nothing else changes: heap segments are a first-class case
 * everywhere in Flink, since that is what {@code taskmanager.memory.segment-size} buffers were
 * before off-heap network memory became the default.
 */
public final class MemorySegmentFactory {

  /** Wraps the byte array in a {@link MemorySegment}, as upstream does. */
  public static MemorySegment wrap(byte[] buffer) {
    return new MemorySegment(buffer, null);
  }

  /** A segment over a copy of {@code bytes} between {@code start} and {@code end}. */
  public static MemorySegment wrapCopy(byte[] bytes, int start, int end)
      throws IllegalArgumentException {
    if (end < start || start > bytes.length || end > bytes.length) {
      throw new IllegalArgumentException("invalid range [" + start + ", " + end + ")");
    }
    byte[] copy = new byte[end - start];
    System.arraycopy(bytes, start, copy, 0, copy.length);
    return wrap(copy);
  }

  /** A four byte segment holding {@code value} big endian, as upstream does. */
  public static MemorySegment wrapInt(int value) {
    return wrap(
        new byte[] {
          (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
        });
  }

  public static MemorySegment allocateUnpooledSegment(int size) {
    return allocateUnpooledSegment(size, null);
  }

  public static MemorySegment allocateUnpooledSegment(int size, Object owner) {
    return new MemorySegment(new byte[size], owner);
  }

  /** Heap memory here, not off-heap: see the class comment. */
  public static MemorySegment allocateUnpooledOffHeapMemory(int size) {
    return allocateUnpooledSegment(size, null);
  }

  /** Heap memory here, not off-heap: see the class comment. */
  public static MemorySegment allocateUnpooledOffHeapMemory(int size, Object owner) {
    return allocateUnpooledSegment(size, owner);
  }

  /** Heap memory here, not unsafe off-heap memory: see the class comment. */
  public static MemorySegment allocateOffHeapUnsafeMemory(int size) {
    return allocateUnpooledSegment(size, null);
  }

  /**
   * Heap memory here, not unsafe off-heap memory: see the class comment.
   *
   * <p>{@code cleaner} frees the native allocation upstream makes; there is none to free, and a heap
   * segment's own {@code free()} does not run it, so it is dropped.
   */
  public static MemorySegment allocateOffHeapUnsafeMemory(int size, Object owner, Runnable cleaner) {
    return allocateUnpooledSegment(size, owner);
  }

  /**
   * A segment over a {@link java.nio.ByteBuffer} that already exists.
   *
   * <p>The buffer's own storage has to be kept -- callers, such as the ones mapping a file, read and
   * write through it -- so a heap-backed buffer is wrapped by its array and a direct one still
   * becomes an off-heap segment, which is as good as its accessors are under CheerpJ.
   */
  public static MemorySegment wrapOffHeapMemory(java.nio.ByteBuffer memory) {
    if (memory.hasArray() && memory.arrayOffset() == 0 && memory.capacity() == memory.array().length) {
      return new MemorySegment(memory.array(), null);
    }
    return new MemorySegment(memory, null);
  }

  private MemorySegmentFactory() {}
}
