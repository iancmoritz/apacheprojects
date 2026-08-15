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

// The list the home page renders.  A new project is one entry plus its page under that `href`.

export type Project = {
  name: string;
  version: string;
  href: string;
  blurb: string;
  /** What actually runs in the tab, shown as small tags on the card. */
  pieces: string[];
  accent: string;
};

export const PROJECTS: Project[] = [
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
];
