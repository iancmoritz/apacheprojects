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

import { defineConfig, type Plugin } from "vite";

import { serveSiteFiles } from "../design/site-files";

// The project list at / and the design system's stylesheets are the repository root's, served here
// in dev so that this project's dev server serves the whole site (see ../design/site-files.ts).
function siteFiles(): Plugin {
  return {
    name: "site-files",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use(serveSiteFiles);
    },
  };
}

// CheerpJ loads classes by asking for byte ranges of each jar, so the dev server has to answer range
// requests for them; Vite's static middleware does, but only once the type is one it will stream.
function jarRanges(): Plugin {
  return {
    name: "jar-ranges",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        if ((req.url ?? "").endsWith(".jar")) res.setHeader("Content-Type", "application/java-archive");
        next();
      });
    },
  };
}

export default defineConfig({
  plugins: [siteFiles(), jarRanges()],
  build: {
    target: "es2022",
    sourcemap: true,
    // The page lives at /spark/ so that the dev server and the assembled site have identical URLs.
    rollupOptions: { input: { spark: "spark/index.html" } },
  },
});
