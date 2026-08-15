/*!
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

/** The slice of the CheerpJ 4 API this page uses; the runtime defines these on `window`. */
export interface CheerpJInitOptions {
  version?: number;
  status?: "splash" | "none" | "devtools";
  javaProperties?: string[];
  natives?: Record<string, (...args: unknown[]) => unknown>;
}

/** Static methods of our KafkaBrowser facade, as CheerpJ exposes them: every call is a promise. */
export interface KafkaBrowserClass {
  format(configPath: string, clusterId: string): Promise<string>;
  start(configPath: string): Promise<string>;
  createTopic(name: string, partitions: number): Promise<string>;
  produce(topic: string, key: string, value: string): Promise<string>;
  consume(group: string, topic: string, max: number, timeoutMs: number): Promise<string>;
  state(): Promise<string>;
}

export interface CheerpJLibrary {
  KafkaBrowser: Promise<KafkaBrowserClass>;
}

declare global {
  function cheerpjInit(options?: CheerpJInitOptions): Promise<void>;
  function cheerpjRunLibrary(classpath: string): Promise<CheerpJLibrary>;
  function cheerpjAddStringFile(path: string, content: string): void;
}
