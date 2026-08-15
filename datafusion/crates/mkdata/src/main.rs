// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Writes `trips.parquet`, the Parquet table the page registers at boot.  Generated at build time
//! rather than committed: a real Parquet file (row groups, dictionary pages, statistics) is exactly
//! the kind of binary this repository does not keep in git.
//!
//! Usage: `mkdata <cities.csv> <trips.parquet> [rows]`

use std::env::args;
use std::fs::{read_to_string, File};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

const DEFAULT_ROWS: usize = 120_000;
const ROWS_PER_BATCH: usize = 8_192;
/// 2024-01-01T00:00:00Z, the first departure.
const EPOCH_MS: i64 = 1_704_067_200_000;
const DAY_MS: i64 = 86_400_000;

/// A tiny linear congruential generator: the tables have to be identical on every machine that
/// builds the site, so nothing here may come from the system's randomness.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn unit(&mut self) -> f64 {
        (self.below(1_000_000) as f64) / 1_000_000.0
    }
}

fn cities(path: &str) -> Vec<String> {
    let text = read_to_string(path).unwrap_or_else(|error| panic!("{path}: {error}"));
    text.lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.split(',').next().unwrap_or_default().to_string())
        .collect()
}

fn main() {
    let arguments: Vec<String> = args().skip(1).collect();
    let [cities_csv, trips_parquet, rest @ ..] = arguments.as_slice() else {
        eprintln!("usage: mkdata <cities.csv> <trips.parquet> [rows]");
        std::process::exit(2);
    };
    let rows: usize = rest
        .first()
        .map(|value| value.parse().expect("rows must be a number"))
        .unwrap_or(DEFAULT_ROWS);

    let cities = cities(cities_csv);
    assert!(!cities.is_empty(), "{cities_csv} has no rows");

    let schema = Arc::new(Schema::new(vec![
        Field::new("trip_id", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
        Field::new(
            "departed_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("minutes", DataType::Int32, false),
        Field::new("distance_km", DataType::Float64, false),
        Field::new("fare_usd", DataType::Float64, false),
        Field::new("passengers", DataType::Int32, false),
    ]));

    // zstd is the wrong choice here: the browser side decodes this file with the Parquet reader
    // compiled into the wasm module, which is built without the compression codecs (they are C
    // libraries).  Snappy is pure Rust, so it is the one codec that survives the trip.
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(32 * 1024))
        .build();
    let file = File::create(trips_parquet).unwrap_or_else(|error| panic!("{trips_parquet}: {error}"));
    let mut writer =
        ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties)).expect("parquet writer");

    let mut random = Lcg(0x5EED_1234_ABCD_0001);
    let mut written = 0;
    while written < rows {
        let count = ROWS_PER_BATCH.min(rows - written);
        let mut ids = Vec::with_capacity(count);
        let mut names = Vec::with_capacity(count);
        let mut departures = Vec::with_capacity(count);
        let mut minutes = Vec::with_capacity(count);
        let mut distances = Vec::with_capacity(count);
        let mut fares = Vec::with_capacity(count);
        let mut passengers = Vec::with_capacity(count);

        for offset in 0..count {
            // Bigger cities get more trips: index the city list with a squared draw so the head of
            // the list dominates and GROUP BY has something interesting to say.
            let skewed = random.unit() * random.unit();
            let city = &cities[(skewed * cities.len() as f64) as usize % cities.len()];
            let distance = 0.6 + random.unit() * 34.0;
            let speed = 12.0 + random.unit() * 26.0;
            let duration = (distance / speed * 60.0).max(2.0);

            ids.push((written + offset + 1) as i32);
            names.push(city.clone());
            departures.push(EPOCH_MS + random.below((180 * DAY_MS) as u64) as i64);
            minutes.push(duration.round() as i32);
            distances.push((distance * 100.0).round() / 100.0);
            fares.push(((3.0 + distance * 1.85 + duration * 0.42) * 100.0).round() / 100.0);
            passengers.push(1 + random.below(4) as i32);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(TimestampMillisecondArray::from(departures)),
            Arc::new(Int32Array::from(minutes)),
            Arc::new(Float64Array::from(distances)),
            Arc::new(Float64Array::from(fares)),
            Arc::new(Int32Array::from(passengers)),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch");
        writer.write(&batch).expect("write batch");
        written += count;
    }

    writer.close().expect("close parquet");
    println!("wrote {written} rows to {trips_parquet}");
}
