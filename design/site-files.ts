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

// The site's own files -- the project list at / and the design system's stylesheets -- belong to the
// repository root, not to any one project.  In production ../scripts/assemble.mjs copies them next to
// the builds; in `npm run dev` there is no assemble step, so every project's dev server serves them
// from here, and all four therefore serve the identical site around the project they are running.

import { readFileSync } from "node:fs";
import type { IncomingMessage, ServerResponse } from "node:http";

/**
 * Request path -> the file that answers it, relative to this directory, and its content type.
 * The production copy of this list is `SITE_FILES` in ../scripts/assemble.mjs, which a Node script
 * cannot import from this TypeScript module.
 */
const FILES: Record<string, [string, string]> = {
  "/": ["../index.html", "text/html"],
  "/index.html": ["../index.html", "text/html"],
  "/projects.js": ["../projects.js", "text/javascript"],
  "/design/tokens.css": ["tokens.css", "text/css"],
  "/design/system.css": ["system.css", "text/css"],
};

export function serveSiteFiles(
  request: IncomingMessage,
  response: ServerResponse,
  next: () => void,
): void {
  const entry = FILES[(request.url ?? "").split("?")[0]];
  if (!entry) return next();
  const [file, type] = entry;
  response.setHeader("Content-Type", type);
  // Read per request: editing the project list or a token should show up on reload, like any source.
  response.end(readFileSync(new URL(file, import.meta.url)));
}
