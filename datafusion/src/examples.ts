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

// Queries over the two bundled tables, in the order a visitor should try them: an aggregation, a
// join across the CSV and the Parquet table, a window function, and the plans for both.

export interface Example {
  title: string;
  sql: string;
}

export const EXAMPLES: Example[] = [
  {
    title: "Busiest cities (aggregate over Parquet)",
    sql: `SELECT city,
       count(*) AS trips,
       round(avg(distance_km), 2) AS avg_km,
       round(sum(fare_usd), 2) AS revenue_usd
FROM trips
GROUP BY city
ORDER BY trips DESC
LIMIT 15;`,
  },
  {
    title: "Revenue by region (join CSV to Parquet)",
    sql: `SELECT c.region,
       count(DISTINCT c.city) AS cities,
       count(*) AS trips,
       round(sum(t.fare_usd) / 1000, 1) AS revenue_k_usd
FROM trips AS t
JOIN cities AS c ON c.city = t.city
GROUP BY c.region
ORDER BY trips DESC;`,
  },
  {
    title: "Monthly ranking (window function)",
    sql: `WITH monthly AS (
  SELECT date_trunc('month', departed_at) AS month,
         city,
         count(*) AS trips
  FROM trips
  GROUP BY 1, 2
),
ranked AS (
  SELECT month,
         city,
         trips,
         rank() OVER (PARTITION BY month ORDER BY trips DESC) AS rank
  FROM monthly
)
SELECT * FROM ranked WHERE rank <= 3 ORDER BY month, rank;`,
  },
  {
    title: "Long trips out of dense cities (filter + subquery)",
    sql: `SELECT t.city,
       count(*) AS long_trips,
       round(max(t.distance_km), 2) AS longest_km,
       round(avg(t.minutes), 1) AS avg_minutes
FROM trips AS t
WHERE t.distance_km > 25
  AND t.city IN (SELECT city FROM cities WHERE population > 15000000)
GROUP BY t.city
ORDER BY long_trips DESC;`,
  },
  {
    title: "EXPLAIN ANALYZE the join",
    sql: `EXPLAIN ANALYZE
SELECT c.country, count(*) AS trips
FROM trips AS t
JOIN cities AS c ON c.city = t.city
GROUP BY c.country
ORDER BY trips DESC
LIMIT 10;`,
  },
  {
    title: "What the session knows (information_schema)",
    sql: `SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;`,
  },
];
