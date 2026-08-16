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

package org.apacheprojects.flinkwasm.rpc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemLoader;

/**
 * Loads Flink's Pekko RPC system straight off the classpath.
 *
 * <p>Flink normally keeps that RPC system isolated: {@code flink-rpc-akka.jar} is a fat jar nested
 * inside {@code flink-dist}, and {@code PekkoRpcSystemLoader} copies it into a temp directory at run
 * time to open it in a {@code SubmoduleClassLoader}. Under CheerpJ that is three risky things at once
 * -- reading a 21 MB resource out of a jar being served over HTTP range requests, writing it to the
 * virtual filesystem, and opening it again through a {@code URLClassLoader} of a {@code file:} URL.
 *
 * <p>The build script hoists the fat jar onto the classpath instead (it shares no class with
 * flink-dist, so nothing is shadowed), and this loader constructs the RPC system directly. Its
 * priority is below Flink's own loader, which stays on the classpath as a fallback: if this one ever
 * throws, {@code RpcSystem.load} moves on to the isolating loader and Flink behaves as it does
 * everywhere else.
 */
public final class FlatRpcSystemLoader implements RpcSystemLoader {

  private static final String PEKKO_RPC_SYSTEM = "org.apache.flink.runtime.rpc.pekko.PekkoRpcSystem";

  @Override
  public int getLoadPriority() {
    return -1;
  }

  @Override
  public RpcSystem loadRpcSystem(Configuration configuration) {
    try {
      return (RpcSystem)
          Class.forName(PEKKO_RPC_SYSTEM, true, FlatRpcSystemLoader.class.getClassLoader())
              .getDeclaredConstructor()
              .newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          PEKKO_RPC_SYSTEM + " is not on the classpath; is flink-rpc-akka.jar missing?", e);
    }
  }
}
