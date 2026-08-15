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
"""
Call the Superset Flask app as a WSGI application, with no server in between.

Superset is a synchronous WSGI app, which is a good fit for a tab: there is no event loop to bridge
and no socket to open -- the service worker hands over a request, this builds the environ Flask
expects and collects what the app writes back.  Requests arrive already prefixed with the mount
point (``/_superset``), which becomes ``SCRIPT_NAME`` so that every URL Superset generates -- and
therefore every URL its React frontend calls -- stays inside the mount.
"""

from __future__ import annotations

import sys
from io import BytesIO
from typing import Any
from urllib.parse import urlsplit

#: Where the app is mounted in the tab.  Must match ``BASE_PATH`` in the TypeScript side.
from superset_wasm.bootstrap import MOUNT

#: Headers WSGI expects without the ``HTTP_`` prefix.
UNPREFIXED = {"CONTENT_TYPE", "CONTENT_LENGTH"}


def environ(method: str, url: str, headers: list[tuple[str, str]], body: bytes) -> dict[str, Any]:
    """The WSGI environ for one request from the tab."""
    parts = urlsplit(url)
    # The prefix stays on PATH_INFO: Superset's own ``AppRootMiddleware`` moves it to SCRIPT_NAME,
    # which is how upstream supports being served from a subdirectory (APPLICATION_ROOT).
    path = parts.path if parts.path.startswith(MOUNT) else f"{MOUNT}{parts.path}"

    result: dict[str, Any] = {
        "REQUEST_METHOD": method.upper(),
        "SCRIPT_NAME": "",
        "PATH_INFO": path,
        "QUERY_STRING": parts.query,
        "SERVER_NAME": parts.hostname or "localhost",
        "SERVER_PORT": str(parts.port or (443 if parts.scheme == "https" else 80)),
        "SERVER_PROTOCOL": "HTTP/1.1",
        "REMOTE_ADDR": "127.0.0.1",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": parts.scheme or "http",
        "wsgi.input": BytesIO(body),
        "wsgi.errors": sys.stderr,
        "wsgi.multithread": False,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
    }
    for name, value in headers:
        key = name.upper().replace("-", "_")
        result[key if key in UNPREFIXED else f"HTTP_{key}"] = value
    result.setdefault("CONTENT_LENGTH", str(len(body)))
    return result


def call(app: Any, method: str, url: str, headers: list[tuple[str, str]], body: bytes) -> dict[str, Any]:
    """Serve one request and return its status, headers and body."""
    captured: dict[str, Any] = {}

    def start_response(status: str, response_headers: list[tuple[str, str]], exc_info: Any = None) -> Any:
        if exc_info:
            raise exc_info[1].with_traceback(exc_info[2])
        captured["status"] = int(status.split(" ", 1)[0])
        captured["headers"] = list(response_headers)
        return lambda chunk: None

    chunks = app(environ(method, url, headers, body), start_response)
    try:
        payload = b"".join(chunks)
    finally:
        close = getattr(chunks, "close", None)
        if close:
            close()
    return {"status": captured["status"], "headers": captured["headers"], "body": payload}
