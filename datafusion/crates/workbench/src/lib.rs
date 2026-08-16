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

//! The whole engine the page talks to: one `SessionContext`, tables held in memory as Arrow record
//! batches, and results handed back as Arrow IPC so the JavaScript side never re-encodes them.
//!
//! Every entry point is synchronous.  DataFusion is async and spawns tasks, so a current-thread
//! Tokio runtime drives each call to completion with `block_on`; that is only allowed because this
//! module runs inside a Web Worker, where blocking the thread blocks nothing the user can see.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use tokio::runtime::{Builder, Runtime};
use wasm_bindgen::prelude::*;

/// How many rows of a CSV are read before its column types are settled.
const CSV_SCHEMA_SAMPLE_ROWS: usize = 1_000;

#[derive(Serialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    nullable: bool,
}

#[derive(Serialize)]
struct Table {
    name: String,
    rows: usize,
    columns: Vec<Column>,
}

fn columns_of(schema: &SchemaRef) -> Vec<Column> {
    schema
        .fields()
        .iter()
        .map(|field| Column {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        })
        .collect()
}

/// Arrow IPC stream bytes for `batches`, which is what the page turns back into an Arrow table.
fn to_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>, JsError> {
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(buffer)
}

/// The first `limit` rows of `batches`, and whether anything was left behind.
fn take_rows(batches: Vec<RecordBatch>, limit: usize) -> (Vec<RecordBatch>, usize, bool) {
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total <= limit {
        return (batches, total, false);
    }
    let mut kept = Vec::new();
    let mut taken = 0;
    for batch in batches {
        if taken >= limit {
            break;
        }
        let room = limit - taken;
        taken += batch.num_rows().min(room);
        kept.push(if batch.num_rows() > room {
            batch.slice(0, room)
        } else {
            batch
        });
    }
    (kept, total, true)
}

/// One query's results: the rows as Arrow IPC, both plans as text, and where the time went.
#[wasm_bindgen(getter_with_clone)]
pub struct QueryResult {
    /// Arrow IPC stream: the schema plus every row this result carries.
    pub ipc: Vec<u8>,
    /// Rows in `ipc`.
    pub rows: usize,
    /// Rows the query produced, which is more than `rows` when `truncated` is set.
    #[wasm_bindgen(js_name = totalRows)]
    pub total_rows: usize,
    pub truncated: bool,
    /// The optimized logical plan, indented.
    #[wasm_bindgen(js_name = logicalPlan)]
    pub logical_plan: String,
    /// The physical plan actually executed, indented.
    #[wasm_bindgen(js_name = physicalPlan)]
    pub physical_plan: String,
    /// Milliseconds spent in parsing, planning and optimizing.
    #[wasm_bindgen(js_name = planMs)]
    pub plan_ms: f64,
    /// Milliseconds spent executing the physical plan and collecting its output.
    #[wasm_bindgen(js_name = execMs)]
    pub exec_ms: f64,
}

#[wasm_bindgen]
pub struct Workbench {
    ctx: SessionContext,
    runtime: Runtime,
    /// Row counts per registered table, so listing tables costs no queries.
    rows: RefCell<BTreeMap<String, usize>>,
}

#[wasm_bindgen]
impl Workbench {
    /// A session with one partition: a browser tab has no threads to spread a query over, and
    /// repartitioning into eight streams on one thread only makes the plans harder to read.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<Workbench, JsError> {
        console_error_panic_hook::set_once();
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_information_schema(true);
        let runtime = Builder::new_current_thread().build()?;
        Ok(Workbench {
            ctx: SessionContext::new_with_config(config),
            runtime,
            rows: RefCell::new(BTreeMap::new()),
        })
    }

    /// The DataFusion release these bindings were built against.
    #[wasm_bindgen(js_name = dataFusionVersion)]
    pub fn datafusion_version() -> String {
        datafusion::DATAFUSION_VERSION.to_string()
    }

    /// Register `bytes` (a whole CSV file) as an in-memory table, inferring its column types.
    #[wasm_bindgen(js_name = registerCsv)]
    pub fn register_csv(&self, name: &str, bytes: &[u8]) -> Result<String, JsError> {
        let format = Format::default().with_header(true);
        let (schema, _) = format.infer_schema(Cursor::new(bytes), Some(CSV_SCHEMA_SAMPLE_ROWS))?;
        let schema: SchemaRef = Arc::new(schema);
        let reader = ReaderBuilder::new(Arc::clone(&schema))
            .with_format(format)
            .build(Cursor::new(bytes))?;
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        self.register(name, schema, batches)
    }

    /// Register `bytes` (a whole Parquet file) as an in-memory table.
    #[wasm_bindgen(js_name = registerParquet)]
    pub fn register_parquet(&self, name: &str, bytes: &[u8]) -> Result<String, JsError> {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))?.build()?;
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        self.register(name, schema, batches)
    }

    /// Every registered table, as JSON.
    pub fn tables(&self) -> Result<String, JsError> {
        let rows = self.rows.borrow();
        let catalog = self
            .ctx
            .catalog("datafusion")
            .ok_or_else(|| JsError::new("no default catalog"))?;
        let schema = catalog
            .schema("public")
            .ok_or_else(|| JsError::new("no public schema"))?;
        let mut tables = Vec::new();
        for name in schema.table_names() {
            let provider = self
                .runtime
                .block_on(schema.table(&name))?
                .ok_or_else(|| JsError::new(&format!("table {name} vanished")))?;
            tables.push(Table {
                columns: columns_of(&provider.schema()),
                rows: rows.get(&name).copied().unwrap_or_default(),
                name,
            });
        }
        Ok(serde_json::to_string(&tables)?)
    }

    /// Plan and run `sql`, keeping at most `max_rows` rows of the result.
    pub fn sql(&self, sql: &str, max_rows: usize) -> Result<QueryResult, JsError> {
        self.runtime.block_on(async {
            let started = now();
            let frame = self.ctx.sql(sql).await?;
            let logical = frame.clone().into_optimized_plan()?;
            let physical = frame.clone().create_physical_plan().await?;
            let planned = now();

            let batches = frame.collect().await?;
            let finished = now();

            let schema: SchemaRef = Arc::new(Schema::from(physical.schema().as_ref().clone()));
            let (kept, total_rows, truncated) = take_rows(batches, max_rows);
            let rows = kept.iter().map(RecordBatch::num_rows).sum();
            Ok(QueryResult {
                ipc: to_ipc(&schema, &kept)?,
                rows,
                total_rows,
                truncated,
                logical_plan: format!("{}", logical.display_indent()),
                physical_plan: format!("{}", displayable(physical.as_ref()).indent(true)),
                plan_ms: planned - started,
                exec_ms: finished - planned,
            })
        })
    }

    fn register(
        &self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<String, JsError> {
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        let table = MemTable::try_new(Arc::clone(&schema), vec![batches])?;
        self.ctx.deregister_table(name)?;
        self.ctx.register_table(name, Arc::new(table))?;
        self.rows.borrow_mut().insert(name.to_string(), rows);
        Ok(serde_json::to_string(&Table {
            name: name.to_string(),
            rows,
            columns: columns_of(&schema),
        })?)
    }
}

/// `performance.now()`, or 0 where the host does not have one.
fn now() -> f64 {
    js_sys::global()
        .dyn_into::<js_sys::Object>()
        .ok()
        .and_then(|global| js_sys::Reflect::get(&global, &JsValue::from_str("performance")).ok())
        .and_then(|performance| {
            js_sys::Reflect::get(&performance, &JsValue::from_str("now"))
                .ok()
                .and_then(|now| now.dyn_into::<js_sys::Function>().ok())
                .and_then(|now| now.call0(&performance).ok())
        })
        .and_then(|value| value.as_f64())
        .unwrap_or_default()
}
