/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;

public enum BufferType
{
    ON_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocate(size);
        }
    },
    OFF_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            // Browser (CheerpJ) builds: single-byte reads from direct buffers are not
            // sign-extended by the runtime, which corrupts every vint/cell deserialization,
            // so off-heap allocation requests are served on-heap instead.
            return ON_HEAP_ONLY ? ByteBuffer.allocate(size) : ByteBuffer.allocateDirect(size);
        }
    };

    private static final boolean ON_HEAP_ONLY = Boolean.getBoolean("cassandra.wasm.on_heap_buffers");

    public abstract ByteBuffer allocate(int size);

    public static BufferType typeOf(ByteBuffer buffer)
    {
        return buffer.isDirect() ? OFF_HEAP : ON_HEAP;
    }
}
