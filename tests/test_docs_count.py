"""Doc-drift guard for the README's stated suite size.

business-correctness: the test count printed in README.md must equal the full
suite size, computed so the number is identical in every environment.

Why this does not simply assert ``README == collected``: a test module whose
first statements call ``pytest.importorskip(...)`` at module level (column 0)
fails to *collect at all* when that optional dependency is absent. Here
``tests/test_realism_query_target.py`` gates its whole module on the optional
SQLAlchemy "realism" extra — its 3 tests collect locally (extra installed) but
not on a clean CI runner (extra absent). A guard asserting ``README ==
collected`` would be green in one environment and red in the other, which is
worse than no guard.

So the full count = (everything collected *except* the module-gated files) +
(those files' tests counted from source). The first term is identical with or
without the optional extra, because the gated files are excluded from it in
both cases; the second restores the gated tests to the documented total. The
result is stable across environments and still fails if the real suite size and
the README headline diverge.

Module-gated files are detected automatically, so a future optional-extra
module is handled without editing this guard. The source count assumes such
files use plain ``def test_*`` functions (no parametrization); if that ever
changes the guard fails loudly rather than silently miscounting.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
README = REPO_ROOT / "README.md"
TESTS_DIR = REPO_ROOT / "tests"

# A module-level importorskip sits at column 0 (optionally `name = ` first),
# unlike an in-test importorskip which is indented inside a function body.
_MODULE_LEVEL_IMPORTORSKIP = re.compile(r"^(?:\w+\s*=\s*)?pytest\.importorskip\(", re.MULTILINE)
_TEST_DEF = re.compile(r"^def test_", re.MULTILINE)


def _gated_modules() -> list[Path]:
    return [
        path
        for path in sorted(TESTS_DIR.rglob("test_*.py"))
        if _MODULE_LEVEL_IMPORTORSKIP.search(path.read_text(encoding="utf-8"))
    ]


def _readme_claimed_count() -> int:
    match = re.search(r"The test suite has (\d+) tests", README.read_text(encoding="utf-8"))
    assert match is not None, "could not find the suite-size claim in README.md"
    return int(match.group(1))


def _collected_excluding(gated: list[Path]) -> int:
    ignore_args = [f"--ignore={path}" for path in gated]
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", *ignore_args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    match = re.search(r"(\d+) tests collected", result.stdout)
    assert match is not None, (
        "could not parse the collected count from pytest output:\n"
        f"{result.stdout}\n{result.stderr}"
    )
    return int(match.group(1))


def _structural_test_count(paths: list[Path]) -> int:
    return sum(len(_TEST_DEF.findall(path.read_text(encoding="utf-8"))) for path in paths)


def _full_suite_size() -> int:
    gated = _gated_modules()
    return _collected_excluding(gated) + _structural_test_count(gated)


def test_readme_count_matches_full_suite_size() -> None:
    assert _readme_claimed_count() == _full_suite_size(), (
        "README.md test count is out of sync with the full suite size "
        "(tests collected in this environment plus the optionally-gated tests "
        "counted from source); update the README headline to match."
    )
