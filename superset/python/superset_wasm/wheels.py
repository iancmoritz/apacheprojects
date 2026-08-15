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
Make Superset's dependency tree resolvable for Emscripten.  Build time only.

Superset pins upper bounds on packages that only exist as wasm wheels Pyodide built itself
(``pyarrow<19``, ``pandas<2.2``, ``msgpack<1.1``, ``Pillow>=11``, ...).  For CPython those pins pick
a PyPI wheel; for wasm there is exactly one version of each -- the one in the Pyodide distribution --
so a pin that excludes it cannot be satisfied at all, by anything.

``relax()`` therefore installs one policy into micropip's resolver, applied to *every* wheel it
looks at rather than only to Superset's own metadata (the same pins come back through
``apache-superset-core`` and other dependencies):

* a requirement naming a package Pyodide ships is pinned to the version Pyodide ships, extras
  dropped (``pandas[excel]`` would pull in the Rust ``python-calamine``);
* requirements in ``DROP`` are removed, because they cannot exist here at all and Superset only
  reaches them on code paths a tab never takes (SSH tunnels, headless-browser thumbnails, WSGI
  servers, filesystem watchers).

``build_pure_wheel()`` covers the other resolution gap: SQLAlchemy 1.4, which Superset requires and
which publishes no pure-Python wheel.  Its C extensions are optional (``DISABLE_SQLALCHEMY_CEXT``),
so the sdist's ``lib/sqlalchemy`` tree is repackaged into a ``py3-none-any`` wheel as-is.  Nothing is
compiled, patched or vendored: the code is exactly what upstream shipped.
"""

from __future__ import annotations

import base64
import hashlib
import io
import re
import tarfile
import zipfile
from pathlib import Path
from typing import Any

#: Requirements dropped from every wheel's metadata: no wasm build can exist and nothing the browser
#: runtime does needs them.
DROP = {
    # Native, with no wasm build anywhere and no pure fallback:
    "greenlet",  # SQLAlchemy only needs it for its asyncio dialects
    "numexpr",  # optional pandas accelerator
    "bottleneck",  # optional pandas accelerator
    "paramiko",  # SSH tunnels to remote databases
    "sshtunnel",
    "backports-zstd",
    # Published as an sdist only, and unreachable from anything a tab does:
    "odfpy",  # OpenDocument spreadsheet export
    "pgsanity",  # Postgres syntax linting, needs the ecpg binary
    "pygeohash",  # geohash post-processing
    # Pure, but pointless here and a large download:
    "selenium",  # thumbnails and alerts, which drive a real browser
    "playwright",
    "gunicorn",  # there is no socket to serve on
    "watchdog",  # filesystem notifications for dev reload
}

#: Example dashboards kept in the wheel we ship.  Superset discovers examples by directory, so the
#: ones left out simply do not exist as far as ``load_examples`` is concerned; dropping the four
#: largest (deck.gl, world health, misc charts, FCC survey) takes 47 MB off the download.
EXAMPLES = {
    "_shared",
    "usa_births_names",
    "video_game_sales",
    "sales_dashboard",
    "international_sales",
    "featured_charts",
    "slack_dashboard",
}

#: Frontend assets served from the origin.  Everything the SPA loads is here except bitmaps: the
#: country GeoJSON (36 MB) and the gallery screenshots (35 MB of PNG/JPG) only matter to viz types
#: and marketing pages this deployment does not reach, so only small images are kept.
ASSET_SUFFIXES = (".js", ".css", ".woff", ".woff2", ".svg", ".json", ".gif", ".txt", ".ico")
ASSET_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg")
ASSET_IMAGE_MAX_BYTES = 100_000

STATIC_PREFIX = "superset/static/assets/"
#: Upstream's own asset-caching service worker, which ``spa.html`` registers unconditionally.  It sits
#: beside the bundle rather than in it, and is served so the register call does not 404.
STATIC_WORKER = "superset/static/service-worker.js"
EXAMPLES_PREFIX = "superset/examples/"


def _canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def relax(pyodide_packages: dict[str, Any], vendored: set[str] | None = None) -> None:
    """
    Patch micropip so every requirement it reads goes through the policy above.

    ``pyodide_packages`` is micropip's view of the Pyodide lock file (name -> package info);
    ``vendored`` names packages installed beforehand from a wheel we built ourselves (SQLAlchemy 1.4),
    whose requirements are dropped so the resolver cannot replace them with a Pyodide build.
    """
    from micropip._vendored.packaging.src.packaging.requirements import Requirement
    from micropip.transaction import Transaction
    from micropip.wheelinfo import WheelInfo

    vendored = {_canonical(name) for name in vendored or set()}

    provided = {_canonical(name): info["version"] for name, info in pyodide_packages.items()}
    original = WheelInfo.requires

    def requires(self: WheelInfo, extras: set[str]) -> list[Requirement]:
        kept = []
        for requirement in original(self, extras):
            name = _canonical(requirement.name)
            if name in DROP or name in vendored:
                continue
            version = provided.get(name)
            if version is not None and not requirement.specifier.contains(version, prereleases=True):
                requirement = Requirement(f"{name}=={version}")
            elif version is not None and requirement.extras:
                requirement = Requirement(f"{name}=={version}")
            kept.append(requirement)
        return kept

    WheelInfo.requires = requires  # type: ignore[method-assign]

    # With constraints in play micropip re-resolves the dependencies a Pyodide lock entry declares,
    # and those include shared libraries that only exist inside the distribution ("openssl"), which
    # it then goes looking for on PyPI.  Pyodide's own loader pulls them in, so hide them.
    from_lock = Transaction._add_requirement_from_pyodide_lock

    async def add_from_lock(self: Transaction, requirement: Requirement) -> bool:
        constraints, self.constraints = self.constraints, None
        try:
            return await from_lock(self, requirement)
        finally:
            self.constraints = constraints

    Transaction._add_requirement_from_pyodide_lock = add_from_lock  # type: ignore[method-assign]


def constraints(
    requirements: str, pyodide_packages: dict[str, Any], vendored: set[str] | None = None
) -> list[str]:
    """
    Turn Superset's ``requirements/base.txt`` into micropip constraints.

    micropip resolves depth-first without backtracking, so whichever dependency asks for a package
    first decides its version: ``marshmallow-sqlalchemy`` alone is enough to pull in marshmallow 4 and
    dead-end the install, because Superset needs 3.x.  Feeding it upstream's pinned versions removes
    the guesswork and gives the tree Superset is actually tested against.

    Pins for packages that come from the Pyodide distribution, that we repackaged ourselves, or that
    :data:`DROP` removes are left out -- constraints intersect with requirements rather than replacing
    them, so a pin on a version wasm does not have would make the install unsatisfiable.
    """
    skip = set(DROP) | {_canonical(name) for name in vendored or set()}
    skip |= {_canonical(name) for name in pyodide_packages}
    pins = []
    for line in Path(requirements).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "-")):
            continue
        name, separator, version = line.partition("==")
        if not separator:
            continue
        canonical = _canonical(name)
        if canonical in skip:
            continue
        pins.append(f"{canonical}=={version.split(';')[0].strip()}")
    return pins


def patch_metadata(metadata: str) -> str:
    """Apply :data:`DROP` to a METADATA document (used for wheels we rewrite on disk)."""
    lines = []
    for line in metadata.splitlines():
        if line.startswith("Requires-Dist:"):
            requirement = line.split(":", 1)[1].strip()
            if _canonical(re.split(r"[\s\[<>=!;~(]", requirement, maxsplit=1)[0]) in DROP:
                continue
        lines.append(line)
    return "\n".join(lines) + "\n"


def build_pure_wheel(sdist: str, package_dir: str, packages: list[str], out_dir: str) -> str:
    """
    Repackage the pure-Python part of an sdist as a ``py3-none-any`` wheel.

    ``package_dir`` is the directory inside the sdist that holds the importable ``packages``
    (``lib`` for SQLAlchemy, the sdist root for WTForms-JSON); everything that is not Python source or
    package data is left out, which is what dropping SQLAlchemy's optional C extensions amounts to.
    Metadata comes from the sdist's own ``PKG-INFO``.
    """
    keep_suffixes = (".py", ".pyi", ".typed", ".txt", ".json")
    with tarfile.open(sdist) as archive:
        members = {member.name: member for member in archive.getmembers() if member.isfile()}
        root = next(iter(members)).split("/")[0]
        pkg_info = archive.extractfile(members[f"{root}/PKG-INFO"])
        assert pkg_info is not None
        metadata = patch_metadata(pkg_info.read().decode())

        name = version = ""
        for line in metadata.splitlines():
            if line.startswith("Name: "):
                name = line[len("Name: ") :].strip()
            elif line.startswith("Version: "):
                version = line[len("Version: ") :].strip()
            elif not line.strip():
                break
        if not name or not version:
            raise ValueError(f"{sdist}: PKG-INFO has no name/version")

        dist_info = f"{name.replace('-', '_')}-{version}.dist-info"
        records: list[str] = []
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as wheel:

            def add(arcname: str, data: bytes) -> None:
                wheel.writestr(arcname, data)
                digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
                records.append(f"{arcname},sha256={digest.decode()},{len(data)}")

            prefix = "/".join(part for part in (root, package_dir.strip("/")) if part) + "/"
            for member_name, member in members.items():
                if not member_name.startswith(prefix) or not member_name.endswith(keep_suffixes):
                    continue
                arcname = member_name[len(prefix) :]
                if not any(arcname.startswith(f"{package}/") for package in packages):
                    continue
                content = archive.extractfile(member)
                assert content is not None
                add(arcname, content.read())

            add(f"{dist_info}/METADATA", metadata.encode())
            add(
                f"{dist_info}/WHEEL",
                b"Wheel-Version: 1.0\nGenerator: superset_wasm.wheels\n"
                b"Root-Is-Purelib: true\nTag: py3-none-any\n",
            )
            records.append(f"{dist_info}/RECORD,,")
            wheel.writestr(f"{dist_info}/RECORD", "\n".join(records) + "\n")

    filename = f"{name.replace('-', '_').lower()}-{version}-py3-none-any.whl"
    path = Path(out_dir) / filename
    path.write_bytes(buffer.getvalue())
    return str(path)


def _keep_asset(name: str, size: int) -> bool:
    if name.endswith(ASSET_SUFFIXES):
        return True
    return name.endswith(ASSET_IMAGE_SUFFIXES) and size <= ASSET_IMAGE_MAX_BYTES


def split_superset_wheel(wheel: str, wheel_dir: str, asset_dir: str) -> dict[str, Any]:
    """
    Split ``apache_superset``'s 103 MB wheel into the part Pyodide installs and the part the browser
    fetches as ordinary files.

    The frontend bundle is 100 MB of the wheel and Python never reads it -- the SPA asks for it over
    HTTP -- so it is written out as plain files under ``asset_dir`` and removed from the wheel, along
    with the translation catalogues and the example dashboards not in :data:`EXAMPLES`.  The one
    exception is ``manifest.json``: Flask reads it from disk to know which bundles a page needs.
    """
    kept: list[tuple[zipfile.ZipInfo, bytes]] = []
    assets: dict[str, bytes] = {}
    #: Files that belong one level above the bundle, in ``static/`` itself.
    beside: dict[str, bytes] = {}
    with zipfile.ZipFile(wheel) as archive:
        for info in archive.infolist():
            name = info.filename
            if name == STATIC_WORKER:
                beside[Path(name).name] = archive.read(name)
                continue
            if name.startswith(STATIC_PREFIX):
                data = archive.read(name)
                if name.endswith("/manifest.json"):
                    kept.append((info, data))
                if _keep_asset(name, info.file_size):
                    assets[name[len(STATIC_PREFIX) :]] = data
                continue
            if name.startswith(EXAMPLES_PREFIX):
                parts = name.split("/")
                if len(parts) > 3 and parts[2] not in EXAMPLES:
                    continue
            if name.endswith((".po", ".pot")):  # .mo is what Babel actually reads
                continue
            kept.append((info, archive.read(name)))

    names = {info.filename for info, _ in kept}
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as slim:
        for info, data in kept:
            if info.filename.endswith(".dist-info/RECORD"):
                lines = data.decode().splitlines()
                data = (
                    "\n".join(line for line in lines if line.split(",")[0] in names) + "\n"
                ).encode()
            slim.writestr(info, data)

    Path(wheel_dir).mkdir(parents=True, exist_ok=True)
    slim_path = Path(wheel_dir) / Path(wheel).name
    slim_path.write_bytes(buffer.getvalue())

    asset_root = Path(asset_dir)
    for name, data in assets.items():
        path = asset_root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    for name, data in beside.items():
        asset_root.parent.mkdir(parents=True, exist_ok=True)
        (asset_root.parent / name).write_bytes(data)

    return {
        "wheel": str(slim_path),
        "wheel_bytes": slim_path.stat().st_size,
        "assets": len(assets),
        "asset_bytes": sum(len(data) for data in assets.values()),
    }
