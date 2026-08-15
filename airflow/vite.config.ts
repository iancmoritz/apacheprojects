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

import { defineConfig } from "vite";

export default defineConfig({
  build: {
    target: "es2022",
    sourcemap: true,
    // The project list at / and the Airflow app at /airflow/ are two pages of one origin, because
    // the service worker the app installs is only allowed to intercept its own origin.
    rollupOptions: {
      input: { home: "index.html", airflow: "airflow/index.html" },
    },
  },
  worker: { format: "es" },
  // PGlite ships its wasm as an optional dependency graph that Vite's pre-bundler mangles.
  optimizeDeps: { exclude: ["@electric-sql/pglite"] },
});
