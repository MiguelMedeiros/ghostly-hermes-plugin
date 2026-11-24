"""Shared test setup.

Every test gets its own ``HERMES_HOME`` (the ``env`` fixture), and Hermes caches its locale catalogs per
``(home, lang)``, so without help each test that renders or checks a gateway notice parses the same 17 bundled YAML
catalogs again with the pure-Python parser: about half of the suite's time. The fixture below parses each locale file
once per session, keyed by its path, mtime and size, and hands every caller its own copy. Hermes' per-home caches,
``reset_language_cache`` and per-home overlay files behave as before; only the YAML read of an unchanged file is shared.
"""
import os
import sys
import threading
from pathlib import Path

import pytest

AGENT = Path(os.environ.get("HERMES_AGENT_DIR") or Path.home() / ".hermes" / "hermes-agent")
if str(AGENT) not in sys.path:
    sys.path.insert(0, str(AGENT))


@pytest.fixture(scope="session", autouse=True)
def parse_each_locale_file_once():
    """Memoize ``agent.i18n_layers.parse_locale_file`` for the session (a changed file is parsed again)."""
    try:
        from agent import i18n_layers
    except ImportError:          # no Hermes Agent to test against: the tests themselves report it
        yield
        return
    real = i18n_layers.parse_locale_file
    parsed, lock = {}, threading.Lock()

    def parse_locale_file(path):
        st = os.stat(path)
        key = (os.path.realpath(path), st.st_mtime_ns, st.st_size)
        # Held across the parse, so the adapter's catalog warm-up thread and a t() on the test's thread never both
        # parse the same file (the parse is pure Python, so the lock costs no parallelism).
        with lock:
            hit = parsed.get(key)
            if hit is None:
                hit = parsed[key] = real(path)     # raises as before; failures are not cached
        return dict(hit)         # callers merge into the result (agent.i18n._load_catalog), so never share it

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(i18n_layers, "parse_locale_file", parse_locale_file)
        yield
