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
The surface ``worker.ts`` calls: boot the app, then serve requests out of it.

Everything returns plain JS values so the TypeScript side never handles a ``PyProxy``.
"""

from __future__ import annotations

import time
from http.cookies import SimpleCookie
from typing import Any

_app: Any = None

#: Flask's session cookie, kept here rather than in the browser.  A response a service worker builds
#: in JavaScript never reaches the cookie store -- ``Set-Cookie`` on a synthesised ``Response`` is
#: ignored -- so the tab's cookie jar lives on this side of the boundary instead, which keeps
#: Superset's real session-based login working exactly as it does on a server.
_cookies: dict[str, str] = {}


def boot(*, initialised: bool = False, progress: Any = None) -> Any:
    """
    Configure and initialise Superset, and remember the app for :func:`http`.

    ``initialised`` says the database files were restored from a previous boot, in which case the
    migrations, the role sync and the example load are skipped -- they are what makes a cold boot slow.
    """
    global _app  # noqa: PLW0603

    from superset_wasm import bootstrap, shims

    def step(text: str) -> float:
        if progress:
            progress(text)
        return time.monotonic()

    step("installing stand-ins for the modules a browser has no use for")
    shims.install()
    bootstrap.configure()

    started = step("creating the Superset Flask application")
    _app = bootstrap.create_app()
    timings = {"create_app": round(time.monotonic() - started, 1)}

    if not initialised:
        started = step("running the database migrations (superset db upgrade)")
        bootstrap.migrate(_app)
        timings["migrate"] = round(time.monotonic() - started, 1)

        started = step("creating roles and permissions (superset init)")
        bootstrap.init(_app)
        timings["init"] = round(time.monotonic() - started, 1)

        started = step("creating the admin user")
        bootstrap.create_admin(_app)

        started = step("loading the example datasets, charts and dashboards")
        counts = bootstrap.load_examples(_app)
        timings["examples"] = round(time.monotonic() - started, 1)
    else:
        counts = bootstrap.counts(_app)

    step("logging in as admin")
    logged_in = login()

    from importlib.metadata import version

    return _to_js(
        {
            "version": version("apache-superset"),
            "timings": timings,
            "counts": counts,
            "dashboard": bootstrap.first_dashboard(_app),
            "logged_in": logged_in,
        }
    )


def login(username: str = "admin", password: str = "admin") -> bool:
    """
    Post Superset's own login form, so the session in the jar is an authenticated one.

    The visitor could type the same credentials into the login page instead; doing it here is what
    makes the frontend come up already signed in.
    """
    from urllib.parse import urlencode

    from superset_wasm import bootstrap

    body = urlencode({"username": username, "password": password}).encode()
    response = _serve(
        "POST",
        f"http://tab{bootstrap.MOUNT}/login/",
        [("Content-Type", "application/x-www-form-urlencoded")],
        body,
    )
    # A successful login redirects; a failed one re-renders the form with a 200.
    return response["status"] in {301, 302, 303, 307, 308}


def http(method: str, url: str, headers: Any, body: Any) -> Any:
    """Serve one request from the Superset frontend out of the in-tab app."""
    pairs = [(str(name), str(value)) for name, value in headers]
    response = _serve(method, url, pairs, bytes(body) if body else b"")
    return _to_js(
        {
            "status": response["status"],
            "headers": [[name, value] for name, value in response["headers"]],
            "body": response["body"],
        }
    )


def _serve(method: str, url: str, headers: list[tuple[str, str]], body: bytes) -> dict[str, Any]:
    """One request through the app, with the cookie jar applied on the way in and out."""
    from superset_wasm import wsgi

    if _app is None:
        raise RuntimeError("Superset has not booted yet")

    sent = [(name, value) for name, value in headers if name.lower() != "cookie"]
    if _cookies:
        sent.append(("Cookie", "; ".join(f"{name}={value}" for name, value in _cookies.items())))

    response = wsgi.call(_app, method, url, sent, body)

    kept = []
    for name, value in response["headers"]:
        if name.lower() != "set-cookie":
            kept.append((name, value))
            continue
        for key, morsel in SimpleCookie(value).items():
            if morsel.value:
                _cookies[key] = morsel.value
            else:
                _cookies.pop(key, None)
    response["headers"] = kept
    return response


def snapshot() -> Any:
    """The database files, so the page can store them and skip initialisation next time."""
    from superset_wasm import bootstrap

    return _to_js(bootstrap.snapshot())


def restore(files: Any) -> bool:
    """Put database files from a previous boot back into the filesystem."""
    from superset_wasm import bootstrap

    return bootstrap.restore({str(name): bytes(data) for name, data in files.items()})


def _to_js(value: Any) -> Any:
    import js
    from pyodide.ffi import to_js

    return to_js(value, dict_converter=js.Object.fromEntries)
