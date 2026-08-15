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

import { readFileSync } from "node:fs";
import { defineConfig, type Plugin } from "vite";

// In production the project list at / is a static file the repo root owns and
// ../scripts/assemble.mjs copies next to this build; in `npm run dev` there is no assemble step, so
// serve it from here.
function rootProjectList(): Plugin {
  const files: Record<string, [string, string]> = {
    "/": ["../index.html", "text/html"],
    "/index.html": ["../index.html", "text/html"],
    "/projects.js": ["../projects.js", "text/javascript"],
  };

  return {
    name: "root-project-list",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const entry = files[(req.url ?? "").split("?")[0]];
        if (!entry) return next();
        const [file, type] = entry;
        res.setHeader("Content-Type", type);
        res.end(readFileSync(new URL(file, import.meta.url)));
      });
    },
  };
}

export default defineConfig({
  plugins: [rootProjectList()],
  build: {
    target: "es2022",
    sourcemap: true,
    // Namespaced, like everything else this project emits at the root, so assemble.mjs can merge it.
    assetsDir: "_kafka/assets",
    // The page lives at /kafka/ so that the dev server and the assembled site have identical URLs.
    rollupOptions: { input: { kafka: "kafka/index.html" } },
  },
  // CheerpJ reads the jars with Range requests; Vite serves public/ with Range support already.
  assetsInclude: ["**/*.jar"],
});
