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

// One entry per top-level project directory.  `href` is the route scripts/assemble.mjs mounts that
// project's build under, so a new project is a new directory plus an entry here.
export const PROJECTS = [
  {
    name: "Apache Airflow",
    version: "3.3.1",
    href: "/airflow/",
    blurb:
      "The API server, scheduler, Dag processor, worker and the Postgres metadata database, " +
      "all WebAssembly in your tab. Trigger a Dag and it really runs here.",
    pieces: ["Pyodide", "PGlite", "Airflow UI"],
    accent: "#017cee",
  },
  {
    name: "Apache Iceberg",
    version: "PyIceberg 0.11.1",
    href: "/iceberg/",
    blurb:
      "A real Iceberg table written into your tab's filesystem: metadata JSON, manifests and " +
      "Parquet you can open file by file. Append, merge, evolve the schema, then query an older " +
      "snapshot with DuckDB.",
    pieces: ["Pyodide", "PyIceberg", "DuckDB"],
    accent: "#1f6feb",
  },
];

const list = document.querySelector("#projects");

for (const project of PROJECTS) {
  const item = document.createElement("li");
  item.className = "card";
  item.style.setProperty("--accent", project.accent);

  const link = document.createElement("a");
  link.href = project.href;

  const title = document.createElement("h2");
  title.textContent = project.name;
  const version = document.createElement("span");
  version.className = "version";
  version.textContent = project.version;
  title.append(version);

  const blurb = document.createElement("p");
  blurb.textContent = project.blurb;

  const pieces = document.createElement("ul");
  pieces.className = "pieces";
  for (const piece of project.pieces) {
    const tag = document.createElement("li");
    tag.textContent = piece;
    pieces.append(tag);
  }

  const open = document.createElement("span");
  open.className = "open";
  open.textContent = "Open in this tab →";

  link.append(title, blurb, pieces, open);
  item.append(link);
  list.append(item);
}
