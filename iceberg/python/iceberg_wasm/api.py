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
"""The one function the Web Worker calls.

Commands and results are JSON strings, so the only thing crossing the Python/JavaScript boundary is
text -- no proxied Python objects to free, and the same entry point works when the demo is driven
from Node in `scripts/probe.mjs`.
"""

from __future__ import annotations

import json
import time
import traceback
from typing import Any, Callable

from iceberg_wasm.demo import Demo

_demo: Demo | None = None


def demo() -> Demo:
    global _demo
    if _demo is None:
        from iceberg_wasm import shims

        shims.install()
        _demo = Demo()
        _demo.open_catalog()
    return _demo


def _reset(instance: Demo, **_: Any) -> dict[str, Any]:
    return instance.reset()


COMMANDS: dict[str, Callable[..., dict[str, Any]]] = {
    "state": lambda instance, **kwargs: instance.state(),
    "create": lambda instance, **kwargs: instance.create_table(),
    "append": lambda instance, **kwargs: instance.append_batch(**kwargs),
    "upsert": lambda instance, **kwargs: instance.upsert_rows(),
    "delete": lambda instance, **kwargs: instance.delete_rows(**kwargs),
    "evolve_schema": lambda instance, **kwargs: instance.evolve_schema(),
    "evolve_partitioning": lambda instance, **kwargs: instance.evolve_partitioning(),
    "expire": lambda instance, **kwargs: instance.expire_oldest(),
    "query": lambda instance, **kwargs: instance.query(**kwargs),
    "plan": lambda instance, **kwargs: instance.plan(**kwargs),
    "tree": lambda instance, **kwargs: instance.tree(),
    "preview": lambda instance, **kwargs: instance.preview(**kwargs),
    "reset": _reset,
}


def call(command: str, payload: str = "{}") -> str:
    """Run one command; never raise, so the page can show the error instead of dying."""
    started = time.monotonic()
    try:
        handler = COMMANDS[command]
    except KeyError:
        return json.dumps({"ok": False, "error": f"unknown command {command!r}"})

    try:
        result = handler(demo(), **json.loads(payload or "{}"))
        return json.dumps(
            {"ok": True, "command": command, "ms": round((time.monotonic() - started) * 1000), **result}
        )
    except Exception as error:  # noqa: BLE001 - reported to the page verbatim
        return json.dumps(
            {
                "ok": False,
                "command": command,
                "ms": round((time.monotonic() - started) * 1000),
                "error": f"{type(error).__name__}: {error}",
                "traceback": traceback.format_exc(),
            }
        )
