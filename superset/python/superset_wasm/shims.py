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
Stand-ins for modules that cannot exist in a browser tab.

``resource`` is a standard library module the Pyodide distribution drops entirely, because Emscripten
has no rlimits; ``psutil`` and ``setproctitle`` have no wasm build at all.  Superset's dependency tree
imports all three at module scope even though nothing in a tab reaches the code that uses them.

Anything that would need the missing capability raises rather than pretending to work.
"""

from __future__ import annotations

import sys
from pathlib import Path

RESOURCE_SOURCE = '''
"""Minimal resource stand-in: Emscripten has no rlimits."""

RLIMIT_AS = 9
RLIMIT_CORE = 4
RLIMIT_CPU = 0
RLIMIT_DATA = 2
RLIMIT_FSIZE = 1
RLIMIT_NOFILE = 7
RLIMIT_NPROC = 6
RLIMIT_STACK = 3
RLIM_INFINITY = -1


class error(OSError):
    pass


def getrlimit(resource):
    return (RLIM_INFINITY, RLIM_INFINITY)


def setrlimit(resource, limits):
    pass


def getrusage(who):
    return (0.0,) * 16
'''

FCNTL_SOURCE = '''
"""Minimal fcntl stand-in: Emscripten's file descriptors have no flags to get or set.

Celery's billiard imports it at module scope to close inherited descriptors and set O_NONBLOCK on
pipes; in a tab there is nothing to inherit and nothing to unblock, so the calls are no-ops.
"""

F_DUPFD = 0
F_GETFD = 1
F_SETFD = 2
F_GETFL = 3
F_SETFL = 4
F_GETLK = 5
F_SETLK = 6
F_SETLKW = 7
F_GETOWN = 9
F_SETOWN = 8
FD_CLOEXEC = 1

LOCK_SH = 1
LOCK_EX = 2
LOCK_NB = 4
LOCK_UN = 8


def fcntl(fd, cmd, arg=0):
    return 0


def ioctl(fd, request, arg=0, mutate_flag=True):
    return 0


def flock(fd, operation):
    return None


def lockf(fd, cmd, len=0, start=0, whence=0):
    return None
'''

PSUTIL_SOURCE = '''
"""Non-functional psutil stand-in: a tab has one process and cannot inspect it."""

import os

version_info = (7, 0, 0)
__version__ = "7.0.0"

POSIX = True
WINDOWS = False
MACOS = False
LINUX = True


class Error(Exception):
    pass


class NoSuchProcess(Error):
    def __init__(self, pid=None, name=None, msg=None):
        super().__init__(msg or f"process no longer exists (pid={pid})")
        self.pid = pid
        self.name = name


class ZombieProcess(NoSuchProcess):
    pass


class AccessDenied(Error):
    pass


class TimeoutExpired(Error):
    def __init__(self, seconds=None, pid=None, name=None):
        super().__init__(f"timeout after {seconds} seconds (pid={pid})")
        self.seconds = seconds
        self.pid = pid
        self.name = name


class Process:
    def __init__(self, pid=None):
        self.pid = os.getpid() if pid is None else pid

    def name(self):
        return "python"

    def cmdline(self):
        return ["python"]

    def username(self):
        return "browser"

    def create_time(self):
        return 0.0

    def is_running(self):
        return self.pid == os.getpid()

    def status(self):
        return "running"

    def children(self, recursive=False):
        return []

    def parent(self):
        return None

    def cwd(self):
        return os.getcwd()

    def environ(self):
        return dict(os.environ)

    def memory_info(self):
        raise NotImplementedError("psutil is not available under Emscripten")

    def cpu_percent(self, interval=None):
        return 0.0

    def terminate(self):
        pass

    def kill(self):
        pass

    def wait(self, timeout=None):
        return 0


def pid_exists(pid):
    return pid == os.getpid()


def process_iter(attrs=None, ad_value=None):
    return iter(())


def pids():
    return [os.getpid()]


def cpu_count(logical=True):
    return 1


def virtual_memory():
    raise NotImplementedError("psutil is not available under Emscripten")
'''

SETPROCTITLE_SOURCE = '''
"""Non-functional setproctitle stand-in: a browser tab has no process title."""

__version__ = "1.3.6"

_title = "python"


def setproctitle(title):
    global _title
    _title = title


def getproctitle():
    return _title


def setthreadtitle(title):
    pass


def getthreadtitle():
    return _title
'''

SSHTUNNEL_SOURCE = '''
"""sshtunnel stand-in: a tab has no sockets, so it can never forward a port over SSH."""

__version__ = "0.4.0"

TUNNEL_TIMEOUT = 10.0
SSH_TIMEOUT = 10.0
DEFAULT_LOGLEVEL = 20
SSH_CONFIG_FILE = "~/.ssh/config"


class BaseSSHTunnelForwarderError(Exception):
    pass


class HandlerSSHTunnelForwarderError(BaseSSHTunnelForwarderError):
    pass


class SSHTunnelForwarder:
    def __init__(self, *args, **kwargs):
        raise BaseSSHTunnelForwarderError(
            "SSH tunnels need TCP sockets, which do not exist under Emscripten"
        )


def open_tunnel(*args, **kwargs):
    raise BaseSSHTunnelForwarderError(
        "SSH tunnels need TCP sockets, which do not exist under Emscripten"
    )
'''

PARAMIKO_SOURCE = '''
"""paramiko stand-in: its dependencies (bcrypt, PyNaCl) have no wasm build, and nothing here can SSH."""

__version__ = "3.5.0"


class SSHException(Exception):
    pass


class AuthenticationException(SSHException):
    pass


class PasswordRequiredException(AuthenticationException):
    pass


class _Key:
    def __init__(self, *args, **kwargs):
        raise SSHException("paramiko is not available under Emscripten")

    @classmethod
    def from_private_key(cls, *args, **kwargs):
        raise SSHException("paramiko is not available under Emscripten")

    @classmethod
    def from_private_key_file(cls, *args, **kwargs):
        raise SSHException("paramiko is not available under Emscripten")


class RSAKey(_Key):
    pass


class DSSKey(_Key):
    pass


class ECDSAKey(_Key):
    pass


class Ed25519Key(_Key):
    pass


class PKey(_Key):
    pass


class SSHClient:
    def __init__(self, *args, **kwargs):
        raise SSHException("paramiko is not available under Emscripten")
'''

_SELENIUM_UNAVAILABLE = '''
"""selenium stand-in: driving a real browser from inside one is not possible."""


class WebDriverException(Exception):
    pass


class TimeoutException(WebDriverException):
    pass


class NoSuchElementException(WebDriverException):
    pass


class StaleElementReferenceException(WebDriverException):
    pass


class SessionNotCreatedException(WebDriverException):
    pass


class _Unavailable:
    def __init__(self, *args, **kwargs):
        raise WebDriverException(
            "selenium cannot run under Emscripten: screenshots, thumbnails and alerts are disabled"
        )
'''

#: package path -> source, for stand-ins that have to be importable as packages
PACKAGES: dict[str, str] = {
    # pgsanity lints Postgres SQL by shelling out to ecpg; the metadata database here is SQLite and
    # there are no subprocesses.  Only superset.sql_validators.postgres imports it, at module scope.
    "pgsanity/__init__.py": "",
    "pgsanity/pgsanity.py": '''
"""pgsanity stand-in: it validates SQL by running the ecpg binary, and a tab has no subprocesses."""


def check_string(sql, add_semicolon=False):
    raise NotImplementedError("pgsanity needs the ecpg binary, which cannot run in a browser")


def check_file(filename=None, add_semicolon=False):
    raise NotImplementedError("pgsanity needs the ecpg binary, which cannot run in a browser")
''',
}

SELENIUM_SOURCES: dict[str, str] = {
    "selenium/__init__.py": '__version__ = "4.28.1"\n',
    "selenium/common/__init__.py": "from .exceptions import *  # noqa: F401,F403\n",
    "selenium/common/exceptions.py": _SELENIUM_UNAVAILABLE,
    "selenium/webdriver/__init__.py": '''
from selenium.common.exceptions import _Unavailable
from selenium.webdriver import chrome, firefox  # noqa: F401


class Chrome(_Unavailable):
    pass


class Firefox(_Unavailable):
    pass


class Remote(_Unavailable):
    pass


class FirefoxProfile(_Unavailable):
    pass


ChromeOptions = chrome.options.Options
FirefoxOptions = firefox.options.Options
''',
    "selenium/webdriver/chrome/__init__.py": "from selenium.webdriver.chrome import options, service  # noqa: F401\n",
    "selenium/webdriver/chrome/options.py": '''
class Options:
    def __init__(self):
        self.arguments = []

    def add_argument(self, argument):
        self.arguments.append(argument)
''',
    "selenium/webdriver/chrome/service.py": "from selenium.webdriver.common.service import Service  # noqa: F401\n",
    "selenium/webdriver/firefox/__init__.py": "from selenium.webdriver.firefox import options, service  # noqa: F401\n",
    "selenium/webdriver/firefox/options.py": "from selenium.webdriver.chrome.options import Options  # noqa: F401\n",
    "selenium/webdriver/firefox/service.py": "from selenium.webdriver.common.service import Service  # noqa: F401\n",
    "selenium/webdriver/common/__init__.py": "",
    "selenium/webdriver/common/by.py": '''
class By:
    ID = "id"
    XPATH = "xpath"
    LINK_TEXT = "link text"
    PARTIAL_LINK_TEXT = "partial link text"
    NAME = "name"
    TAG_NAME = "tag name"
    CLASS_NAME = "class name"
    CSS_SELECTOR = "css selector"
''',
    "selenium/webdriver/common/service.py": '''
from selenium.common.exceptions import _Unavailable


class Service(_Unavailable):
    pass
''',
    "selenium/webdriver/remote/__init__.py": "",
    "selenium/webdriver/remote/webdriver.py": '''
from selenium.common.exceptions import _Unavailable


class WebDriver(_Unavailable):
    pass
''',
    "selenium/webdriver/remote/webelement.py": '''
from selenium.common.exceptions import _Unavailable


class WebElement(_Unavailable):
    pass
''',
    "selenium/webdriver/support/__init__.py": "",
    "selenium/webdriver/support/expected_conditions.py": '''
from selenium.common.exceptions import WebDriverException


def _unavailable(*args, **kwargs):
    def condition(driver):
        raise WebDriverException("selenium cannot run under Emscripten")

    return condition


visibility_of_any_elements_located = _unavailable
visibility_of_element_located = _unavailable
presence_of_element_located = _unavailable
presence_of_all_elements_located = _unavailable
staleness_of = _unavailable
invisibility_of_element = _unavailable
invisibility_of_element_located = _unavailable
element_to_be_clickable = _unavailable
''',
    "selenium/webdriver/support/ui.py": '''
from selenium.common.exceptions import _Unavailable


class WebDriverWait(_Unavailable):
    pass
''',
}

#: module name -> source
MODULES: dict[str, str] = {
    "resource": RESOURCE_SOURCE,
    "fcntl": FCNTL_SOURCE,
    "sshtunnel": SSHTUNNEL_SOURCE,
    "paramiko": PARAMIKO_SOURCE,
    "psutil": PSUTIL_SOURCE,
    "setproctitle": SETPROCTITLE_SOURCE,
}


#: numpy 2 removals that Superset 6.1 still uses (it pins numpy<2; Pyodide ships 2.0.2)
NUMPY_ALIASES = {
    "product": "prod",
    "cumproduct": "cumprod",
    "alltrue": "all",
    "sometrue": "any",
    "round_": "round",
    "in1d": "isin",
    "NaN": "nan",
    "NAN": "nan",
    "Inf": "inf",
    "Infinity": "inf",
    "infty": "inf",
    "float_": "float64",
    "unicode_": "str_",
    "string_": "bytes_",
}


def patch_numpy() -> None:
    """Restore the numpy 1.x aliases Superset uses, on the numpy 2 build Pyodide ships."""
    import numpy

    for old, new in NUMPY_ALIASES.items():
        if not hasattr(numpy, old):
            setattr(numpy, old, getattr(numpy, new))


def patch_pandas() -> None:
    """
    Let pandas talk to SQLAlchemy 1.4, which is the version Superset pins.

    pandas 2.2 (the build Pyodide ships) declares a minimum of SQLAlchemy 2.0 and, because
    ``import_optional_dependency`` is called with ``errors="ignore"``, silently decides SQLAlchemy is
    absent when it finds 1.4.  ``to_sql`` then treats the engine as a raw DBAPI connection and dies
    on ``Engine.cursor``, which is how the example data fails to load.  The code path pandas takes
    with a 1.4 engine works; only the version gate is wrong here.
    """
    from pandas.compat._optional import VERSIONS

    VERSIONS["sqlalchemy"] = "1.4.0"


def patch_timeout() -> None:
    """
    Replace Superset's query timeout with a no-op, because nothing here can interrupt a query.

    ``superset.utils.core.timeout`` is ``SigalrmTimeout`` off Windows: it arms ``signal.alarm``, which
    Emscripten's ``signal`` module does not have, so every query -- including the DDL that loads the
    example data -- fails before it runs.  The alternative Superset ships for Windows uses a
    ``threading.Timer`` to interrupt the main thread, and Pyodide has no threads either.  A query that
    runs too long in a tab blocks the worker until it finishes; see the README.
    """
    from contextlib import nullcontext

    from superset.utils import core

    class NoTimeout(nullcontext):  # type: ignore[type-arg]
        def __init__(self, seconds: int = 1, error_message: str = "Timeout") -> None:
            super().__init__()

    core.timeout = NoTimeout
    core.SigalrmTimeout = NoTimeout


def patch_command_timeout() -> None:
    """
    Patch the one module that imported ``timeout`` by name rather than through its module.

    Importing it pulls in the model layer, which needs a Flask app, so this has to wait until the app
    exists -- unlike :func:`patch_timeout`, which has to happen before it.
    """
    from superset.commands.database import utils as database_utils
    from superset.utils import core

    database_utils.timeout = core.timeout


def install(directory: str = "/shims") -> None:
    """Write the stand-ins to *directory* and put it first on ``sys.path``."""
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)
    for name, source in MODULES.items():
        (target / f"{name}.py").write_text(source)
    for path, source in {**PACKAGES, **SELENIUM_SOURCES}.items():
        file = target / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(source)
    if directory in sys.path:
        sys.path.remove(directory)
    sys.path.insert(0, directory)
    patch_numpy()
    patch_pandas()
