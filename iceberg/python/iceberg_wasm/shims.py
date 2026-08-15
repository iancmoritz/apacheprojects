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
"""The two things PyIceberg assumes that Emscripten does not have.

Both are about the platform, not about Iceberg: object stores it cannot reach from a tab, and threads
Pyodide does not have.  Nothing here touches how tables are written or read -- the metadata, manifests
and Parquet files are entirely PyIceberg's.
"""

from __future__ import annotations

import sys
import types
from concurrent.futures import Executor, Future
from typing import Any, Callable, Iterable, Iterator


class _SyncExecutor(Executor):
    """An `Executor` that runs everything on the calling thread.

    Pyodide's Python has no threads (`_start_joinable_thread` raises), so PyIceberg's
    `ThreadPoolExecutor` cannot start.  Work is done eagerly on `submit`, which keeps the contract
    the callers use: they submit, then `result()`, and exceptions surface there.
    """

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        future: Future = Future()
        if not future.set_running_or_notify_cancel():  # pragma: no cover - never cancelled
            return future
        try:
            future.set_result(fn(*args, **kwargs))
        except BaseException as error:  # noqa: BLE001 - mirrors ThreadPoolExecutor
            future.set_exception(error)
        return future

    def map(  # type: ignore[override]
        self,
        fn: Callable[..., Any],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[Any]:
        return map(fn, *iterables)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        return None


def _stub_pyarrow_s3() -> None:
    """Stand in for `pyarrow._s3fs`, which Pyodide's PyArrow is built without.

    `pyiceberg.io.pyarrow` imports `S3RetryStrategy` at module scope, so without this the whole
    PyArrow FileIO -- the local filesystem included -- is unimportable.  A tab cannot sign S3
    requests anyway; these names exist only so the import succeeds, and any attempt to construct an
    S3 filesystem fails loudly instead of silently doing nothing.
    """
    if "pyarrow._s3fs" in sys.modules:
        return

    module = types.ModuleType("pyarrow._s3fs")

    class S3RetryStrategy:
        pass

    class AwsDefaultS3RetryStrategy(S3RetryStrategy):
        pass

    class AwsStandardS3RetryStrategy(S3RetryStrategy):
        pass

    class S3FileSystem:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise NotImplementedError("no S3 in the browser: this build of PyArrow has no _s3fs")

    for name, value in {
        "S3RetryStrategy": S3RetryStrategy,
        "AwsDefaultS3RetryStrategy": AwsDefaultS3RetryStrategy,
        "AwsStandardS3RetryStrategy": AwsStandardS3RetryStrategy,
        "S3FileSystem": S3FileSystem,
        "ensure_s3_initialized": lambda: None,
        "finalize_s3": lambda: None,
        "initialize_s3": lambda *a, **k: None,
        "resolve_s3_region": lambda *a, **k: None,
    }.items():
        setattr(module, name, value)

    sys.modules["pyarrow._s3fs"] = module


def install() -> None:
    """Make PyIceberg importable and usable in a single-threaded Emscripten interpreter."""
    _stub_pyarrow_s3()

    from pyiceberg.utils.concurrent import ExecutorFactory

    executor = _SyncExecutor()
    ExecutorFactory._instance = executor
    ExecutorFactory.get_or_create = staticmethod(lambda: executor)  # type: ignore[method-assign]
