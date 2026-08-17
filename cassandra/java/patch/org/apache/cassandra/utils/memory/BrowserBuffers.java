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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

/**
 * Buffer allocation for browser (CheerpJ) builds. CheerpJ's direct ByteBuffers do not
 * sign-extend single-byte reads ({@code get()} / {@code get(int)} return 0..255), which
 * corrupts every vint and cell deserialization in Cassandra, so direct allocation requests
 * are served on-heap when {@code cassandra.wasm.on_heap_buffers} is set.
 */
public final class BrowserBuffers
{
    private static final boolean ON_HEAP_ONLY = Boolean.getBoolean("cassandra.wasm.on_heap_buffers");

    private BrowserBuffers()
    {
    }

    public static ByteBuffer allocate(int size)
    {
        return ON_HEAP_ONLY ? ByteBuffer.allocate(size) : ByteBuffer.allocateDirect(size);
    }
}
