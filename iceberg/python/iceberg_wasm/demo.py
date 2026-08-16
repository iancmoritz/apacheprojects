# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""The demo: one Iceberg table in the interpreter's filesystem, driven by PyIceberg.

Every step here is an ordinary PyIceberg call against a `SqlCatalog` whose warehouse is a directory
and whose catalog database is SQLite.  Nothing about the table is synthesised: the `metadata/*.json`,
the manifest lists, the manifests and the Parquet files are the ones PyIceberg writes, so the
warehouse directory is a table any other Iceberg reader can open.

Queries go to DuckDB through PyIceberg's own `scan().to_duckdb()`, which registers the scan's Arrow
table on a DuckDB connection in this same interpreter.
"""

from __future__ import annotations

import json
import os
import random
import shutil
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.manifest import ManifestFile, read_manifest_list
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

WAREHOUSE = "/warehouse"
NAMESPACE = "nyc"
TABLE = "trips"
IDENTIFIER = f"{NAMESPACE}.{TABLE}"

CITIES = ["Amsterdam", "Berlin", "San Francisco", "Tokyo"]
EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)

SCHEMA_FIELDS = (
    NestedField(1, "trip_id", LongType(), required=True),
    NestedField(2, "pickup_ts", TimestampType(), required=True),
    NestedField(3, "city", StringType(), required=True),
    NestedField(4, "fare", DoubleType(), required=True),
)


def _iceberg_schema():
    from pyiceberg.schema import Schema

    return Schema(*SCHEMA_FIELDS)


class Demo:
    """One catalog, one table, one DuckDB connection, all in this interpreter."""

    def __init__(self, warehouse: str = WAREHOUSE) -> None:
        self.warehouse = warehouse
        self.catalog: SqlCatalog | None = None
        self.batches = 0
        self.log: list[str] = []
        self.duckdb = duckdb.connect()

    # -- lifecycle ---------------------------------------------------------------------------

    def open_catalog(self) -> dict[str, Any]:
        os.makedirs(self.warehouse, exist_ok=True)
        self.catalog = SqlCatalog(
            "browser",
            uri=f"sqlite:///{self.warehouse}/catalog.db",
            warehouse=f"file://{self.warehouse}",
        )
        return {"warehouse": self.warehouse, "catalog": "SqlCatalog (SQLite)"}

    def reset(self) -> dict[str, Any]:
        self.catalog = None
        self.batches = 0
        self.log = []
        shutil.rmtree(self.warehouse, ignore_errors=True)
        return self.open_catalog()

    @property
    def _catalog(self) -> SqlCatalog:
        if self.catalog is None:
            self.open_catalog()
        assert self.catalog is not None
        return self.catalog

    def _table(self):
        return self._catalog.load_table(IDENTIFIER)

    def exists(self) -> bool:
        return self.catalog is not None and self._catalog.table_exists(IDENTIFIER)

    # -- steps -------------------------------------------------------------------------------

    def create_table(self) -> dict[str, Any]:
        catalog = self._catalog
        catalog.create_namespace_if_not_exists(NAMESPACE)
        table = catalog.create_table(
            IDENTIFIER,
            schema=_iceberg_schema(),
            properties={"format-version": "2"},
        )
        return {
            "detail": f"created {IDENTIFIER} (format v{table.format_version}), unpartitioned, "
            f"at {table.location()}",
            "metadata_location": table.metadata_location,
        }

    def append_batch(self, rows: int = 240) -> dict[str, Any]:
        table = self._table()
        batch = self._generate(table, rows)
        table.append(batch)
        self.batches += 1
        table = self._table()
        snapshot = table.current_snapshot()
        return {
            "detail": f"appended {rows} rows -> snapshot {snapshot.snapshot_id} "
            f"({snapshot.summary.additional_properties.get('added-data-files')} data files, "
            f"{snapshot.summary.additional_properties.get('added-files-size')} bytes of Parquet)",
            "snapshot_id": str(snapshot.snapshot_id),
        }

    def upsert_rows(self) -> dict[str, Any]:
        """A real MERGE: PyIceberg matches on `trip_id` and rewrites the files that changed."""
        table = self._table()
        arrow = table.scan(limit=5).to_arrow()
        if arrow.num_rows == 0:
            raise ValueError("nothing to update yet -- append a batch first")
        updated = arrow.set_column(
            arrow.schema.get_field_index("fare"),
            "fare",
            pa.array([round(float(v) + 25.0, 2) for v in arrow.column("fare").to_pylist()], pa.float64()),
        )
        result = table.upsert(updated.cast(table.schema().as_arrow()), join_cols=["trip_id"])
        table = self._table()
        snapshot = table.current_snapshot()
        return {
            "detail": f"upsert matched {result.rows_updated} rows "
            f"(+$25 fare), inserted {result.rows_inserted} -> snapshot {snapshot.snapshot_id}",
            "snapshot_id": str(snapshot.snapshot_id),
        }

    def delete_rows(self, row_filter: str = "fare < 12") -> dict[str, Any]:
        table = self._table()
        before = table.scan().count()
        table.delete(delete_filter=row_filter)
        table = self._table()
        after = table.scan().count()
        snapshot = table.current_snapshot()
        return {
            "detail": f"delete where {row_filter}: {before - after} rows gone "
            f"({snapshot.summary.operation.value} rewrote the files that held them "
            f"-> snapshot {snapshot.snapshot_id})",
            "snapshot_id": str(snapshot.snapshot_id),
        }

    def evolve_schema(self) -> dict[str, Any]:
        """Add a column and rename one.  No data file is touched: schema changes are metadata."""
        table = self._table()
        names = set(table.schema().column_names)
        with table.update_schema() as update:
            if "tip" not in names:
                update.add_column("tip", DoubleType(), "tip in the trip's currency")
            if "city" in names:
                update.rename_column("city", "pickup_city")
        table = self._table()
        return {
            "detail": "schema v%d: added `tip`, renamed `city` -> `pickup_city`; "
            "no data files rewritten" % table.schema().schema_id,
            "schema_id": table.schema().schema_id,
        }

    def evolve_partitioning(self) -> dict[str, Any]:
        """Add a partition field.  Old files keep their old spec and still read."""
        table = self._table()
        column = "pickup_city" if "pickup_city" in table.schema().column_names else "city"
        with table.update_spec() as update:
            update.add_field(column, IdentityTransform(), f"{column}_part")
        table = self._table()
        return {
            "detail": f"partition spec {table.spec().spec_id}: identity({column}); "
            "files written before this keep spec 0 and still read",
            "spec_id": table.spec().spec_id,
        }

    def expire_oldest(self) -> dict[str, Any]:
        table = self._table()
        snapshots = list(table.metadata.snapshots)
        if len(snapshots) < 2:
            raise ValueError("need more than one snapshot to expire the oldest")
        oldest = snapshots[0].snapshot_id
        table.maintenance.expire_snapshots().by_id(oldest).commit()
        table = self._table()
        return {
            "detail": f"expired snapshot {oldest}; {len(table.metadata.snapshots)} snapshots left "
            "(time travel to it now fails, as it should)",
            "expired": str(oldest),
        }

    # -- data --------------------------------------------------------------------------------

    def _generate(self, table: Any, rows: int) -> pa.Table:
        """Deterministic trips for the *current* schema, three days per batch."""
        rng = random.Random(1000 + self.batches)
        start = EPOCH + timedelta(days=3 * self.batches)
        columns: dict[str, list[Any]] = {
            "trip_id": [],
            "pickup_ts": [],
            "city": [],
            "fare": [],
            "tip": [],
        }
        for i in range(rows):
            columns["trip_id"].append(self.batches * 100000 + i)
            columns["pickup_ts"].append(
                (start + timedelta(days=i % 3, minutes=rng.randrange(0, 1440))).replace(tzinfo=None)
            )
            columns["city"].append(CITIES[rng.randrange(len(CITIES))])
            columns["fare"].append(round(rng.uniform(4.0, 90.0), 2))
            columns["tip"].append(round(rng.uniform(0.0, 12.0), 2))

        schema = table.schema().as_arrow()
        arrays = []
        for field in schema:
            source = "city" if field.name == "pickup_city" else field.name
            values = columns.get(source)
            if values is None:
                arrays.append(pa.nulls(rows, field.type))
            else:
                arrays.append(pa.array(values).cast(field.type))
        return pa.Table.from_arrays(arrays, schema=schema)

    # -- reading -----------------------------------------------------------------------------

    def query(
        self, sql: str, snapshot_id: str | int | None = None, row_filter: str = ""
    ) -> dict[str, Any]:
        """Run SQL over the table, optionally as of a snapshot.

        `scan().to_duckdb()` is PyIceberg's own DuckDB bridge: it plans the scan (manifest pruning
        included), reads the Parquet files it selected and registers the result as a DuckDB view.
        The SQL then runs in DuckDB proper, in this interpreter.
        """
        table = self._table()
        as_of = int(snapshot_id) if snapshot_id else None
        scan = table.scan(row_filter=row_filter or AlwaysTrue(), snapshot_id=as_of)
        connection = scan.to_duckdb(TABLE, connection=self.duckdb)
        result = connection.sql(sql)
        current = table.current_snapshot()
        return {
            "columns": list(result.columns),
            "rows": [[_plain(value) for value in row] for row in result.fetchall()],
            "snapshot_id": str(as_of or (current.snapshot_id if current else "")),
            "schema_id": scan.projection().schema_id,
            "files_scanned": len(scan.plan_files()),
        }

    def plan(self, row_filter: str = "", snapshot_id: str | int | None = None) -> dict[str, Any]:
        """What the Iceberg planner does with a filter: which files survive, and why."""
        table = self._table()
        as_of = int(snapshot_id) if snapshot_id else None
        scan = table.scan(row_filter=row_filter or AlwaysTrue(), snapshot_id=as_of)
        selected = scan.plan_files()
        total = table.scan(snapshot_id=as_of).plan_files()
        files = [
            {
                "path": _relative(task.file.file_path, table.location()),
                "records": task.file.record_count,
                "bytes": task.file.file_size_in_bytes,
                "partition": _partition(table, task.file),
                "spec_id": task.file.spec_id,
            }
            for task in selected
        ]
        return {
            "row_filter": row_filter or "true",
            "files_scanned": len(selected),
            "files_total": len(total),
            "records_scanned": sum(f["records"] for f in files),
            "rows_matched": scan.count(),
            "files": files,
        }

    def snapshots(self) -> dict[str, Any]:
        table = self._table()
        current = table.current_snapshot()
        entries = []
        for snapshot in table.metadata.snapshots:
            summary = snapshot.summary
            entries.append(
                {
                    "snapshot_id": str(snapshot.snapshot_id),
                    "parent_id": str(snapshot.parent_snapshot_id) if snapshot.parent_snapshot_id else None,
                    "sequence_number": snapshot.sequence_number,
                    "timestamp_ms": snapshot.timestamp_ms,
                    "operation": summary.operation.value if summary else None,
                    "schema_id": snapshot.schema_id,
                    "manifest_list": _relative(snapshot.manifest_list, table.location()),
                    "summary": dict(summary.additional_properties) if summary else {},
                    "is_current": current is not None and snapshot.snapshot_id == current.snapshot_id,
                }
            )
        return {"snapshots": entries, "snapshot_log": _snapshot_log(table)}

    def state(self) -> dict[str, Any]:
        """Everything the page's panels are drawn from, in one round trip."""
        if not self.exists():
            return {"exists": False, "warehouse": self.warehouse}
        table = self._table()
        snapshot = table.current_snapshot()
        return {
            "exists": True,
            "warehouse": self.warehouse,
            "identifier": IDENTIFIER,
            "location": table.location(),
            "format_version": table.format_version,
            "metadata_location": table.metadata_location,
            "schema": {
                "schema_id": table.schema().schema_id,
                "fields": [
                    {
                        "id": field.field_id,
                        "name": field.name,
                        "type": str(field.field_type),
                        "required": field.required,
                        "doc": field.doc,
                    }
                    for field in table.schema().fields
                ],
            },
            # Every schema the table has ever had, so the page can offer a query that is valid for
            # the snapshot being read: a snapshot older than a rename knows nothing of the new name.
            "schemas": [
                {
                    "schema_id": schema.schema_id,
                    "fields": [
                        {
                            "id": field.field_id,
                            "name": field.name,
                            "type": str(field.field_type),
                            "required": field.required,
                            "doc": field.doc,
                        }
                        for field in schema.fields
                    ],
                }
                for schema in table.metadata.schemas
            ],
            "specs": [
                {
                    "spec_id": spec.spec_id,
                    "fields": [
                        {
                            "name": field.name,
                            "transform": str(field.transform),
                            "source_id": field.source_id,
                        }
                        for field in spec.fields
                    ],
                    "is_current": spec.spec_id == table.spec().spec_id,
                }
                for spec in table.specs().values()
            ],
            "rows": table.scan().count() if snapshot else 0,
            "batches": self.batches,
            **self.snapshots(),
        }

    # -- the warehouse on disk ---------------------------------------------------------------

    def tree(self) -> dict[str, Any]:
        files = []
        for root, _dirs, names in os.walk(self.warehouse):
            for name in sorted(names):
                path = os.path.join(root, name)
                files.append(
                    {
                        "path": os.path.relpath(path, self.warehouse),
                        "bytes": os.path.getsize(path),
                        "kind": _kind(name),
                    }
                )
        files.sort(key=lambda f: f["path"])
        return {"root": self.warehouse, "files": files, "bytes": sum(f["bytes"] for f in files)}

    def preview(self, relative: str) -> dict[str, Any]:
        """Decode one file of the table for the file browser."""
        path = os.path.join(self.warehouse, relative)
        if not os.path.isfile(path):
            raise FileNotFoundError(relative)
        kind = _kind(os.path.basename(path))
        if kind == "metadata":
            with open(path) as handle:
                return {"kind": kind, "path": relative, "json": json.load(handle)}
        if kind == "manifest-list":
            return {"kind": kind, "path": relative, "entries": self._manifest_list(path)}
        if kind == "manifest":
            return {"kind": kind, "path": relative, "entries": self._manifest_entries(path)}
        if kind == "data":
            return {"kind": kind, "path": relative, **_parquet(path)}
        return {"kind": kind, "path": relative, "bytes": os.path.getsize(path)}

    def _manifest_list(self, path: str) -> list[dict[str, Any]]:
        table = self._table()
        entries = []
        for manifest in read_manifest_list(table.io.new_input(path)):
            entries.append(
                {
                    "manifest_path": _relative(manifest.manifest_path, table.location()),
                    "added_files": manifest.added_files_count,
                    "existing_files": manifest.existing_files_count,
                    "deleted_files": manifest.deleted_files_count,
                    "added_rows": manifest.added_rows_count,
                    "spec_id": manifest.partition_spec_id,
                    "sequence_number": manifest.sequence_number,
                    "added_snapshot_id": str(manifest.added_snapshot_id),
                    "content": str(manifest.content),
                    "partitions": [
                        {
                            "lower_bound": _plain(summary.lower_bound),
                            "upper_bound": _plain(summary.upper_bound),
                            "contains_null": summary.contains_null,
                        }
                        for summary in (manifest.partitions or [])
                    ],
                }
            )
        return entries

    def _manifest_entries(self, path: str) -> list[dict[str, Any]]:
        table = self._table()
        manifest = self._find_manifest(table, path)
        entries = []
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False):
            entries.append(
                {
                    "status": str(entry.status),
                    "snapshot_id": str(entry.snapshot_id),
                    "sequence_number": entry.sequence_number,
                    "file_path": _relative(entry.data_file.file_path, table.location()),
                    "content": str(entry.data_file.content),
                    "records": entry.data_file.record_count,
                    "bytes": entry.data_file.file_size_in_bytes,
                    "partition": _partition(table, entry.data_file, spec_id=manifest.partition_spec_id),
                }
            )
        return entries

    def _find_manifest(self, table: Any, path: str) -> ManifestFile:
        wanted = os.path.basename(path)
        for snapshot in table.metadata.snapshots:
            for manifest in read_manifest_list(table.io.new_input(snapshot.manifest_list)):
                if os.path.basename(manifest.manifest_path) == wanted:
                    return manifest
        raise FileNotFoundError(f"{wanted} belongs to no snapshot in this table")


# -- helpers ---------------------------------------------------------------------------------


def _kind(name: str) -> str:
    if name.endswith(".metadata.json"):
        return "metadata"
    if name.startswith("snap-") and name.endswith(".avro"):
        return "manifest-list"
    if name.endswith(".avro"):
        return "manifest"
    if name.endswith(".parquet"):
        return "data"
    if name.endswith(".db"):
        return "catalog"
    return "other"


def _relative(path: str, location: str) -> str:
    for prefix in (location + "/", location, "file://"):
        if path.startswith(prefix):
            return path[len(prefix) :]
    return path


def _plain(value: Any) -> Any:
    """JSON-able version of whatever DuckDB, Arrow or Avro handed back."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    return str(value)


def _partition(table: Any, data_file: Any, spec_id: int | None = None) -> dict[str, Any]:
    """The partition tuple of a data file, named by the spec that wrote it."""
    spec = table.specs().get(spec_id if spec_id is not None else data_file.spec_id)
    if spec is None:
        return {}
    values = data_file.partition
    names = [field.name for field in spec.fields]
    out = {}
    for index, name in enumerate(names):
        value = values[index] if index < len(values) else None
        transform = spec.fields[index].transform
        out[name] = _plain(transform.to_human_string(_source_type(table, spec.fields[index]), value))
    return out


def _source_type(table: Any, field: Any) -> Any:
    return table.schema().find_field(field.source_id).field_type


def _snapshot_log(table: Any) -> list[dict[str, Any]]:
    return [
        {"snapshot_id": str(entry.snapshot_id), "timestamp_ms": entry.timestamp_ms}
        for entry in table.metadata.snapshot_log
    ]


def _parquet(path: str) -> dict[str, Any]:
    handle = pq.ParquetFile(path)
    metadata = handle.metadata
    arrow_schema = handle.schema_arrow
    head = handle.read_row_group(0).slice(0, 8) if metadata.num_row_groups else None
    return {
        "records": metadata.num_rows,
        "row_groups": metadata.num_row_groups,
        "bytes": os.path.getsize(path),
        "created_by": metadata.created_by,
        "columns": [
            {
                "name": field.name,
                "type": str(field.type),
                # The field id is what makes reads survive renames: Iceberg matches on it, not names.
                "field_id": (field.metadata or {}).get(b"PARQUET:field_id", b"").decode() or None,
            }
            for field in arrow_schema
        ],
        "head": {
            "columns": head.column_names if head is not None else [],
            "rows": [
                [_plain(value) for value in row]
                for row in (zip(*[column.to_pylist() for column in head.columns]) if head is not None else [])
            ],
        },
    }
