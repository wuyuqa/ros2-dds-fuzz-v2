#!/usr/bin/env python3
"""Run offline unittest and pytest contracts: python3 scripts/check_offline.py [pytest args].

Install dependencies with python3 -m pip install -e '.[test]'. With no selectors,
pytest collects all tests/ modules. Arguments such as --collect-only, -k and
--junitxml are passed through. This checks code, not persisted evidence; use
scripts/verify_s1.py REPORT separately for evidence acceptance.

Live DDS/kernel test opt-ins and native manifest probes are rejected before
collection. Existing opt-in skips stay visible via -ra; real ROS imports in this
interpreter fail. This entrypoint is for the audited offline tests, not a sandbox
for arbitrary subprocesses.
"""
import importlib.abc
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]


class OfflineImports(importlib.abc.MetaPathFinder):
    """Reject live ROS imports; tests may still provide fake sys.modules."""

    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"rclpy", "_rclpy_pybind11"}:
            raise RuntimeError(f"offline check forbids real ROS imports: {fullname}")
        return None


def main():
    blocked = [name for name in ("R2F_NATIVE_DDS_TESTS", "R2F_BOUNDARY_KERNEL_TESTS")
               if os.environ.get(name) == "1"]
    blocked += [name for name in ("R2F_FILTER_TRACE_TEST_MANIFEST", "PYTEST_ADDOPTS",
                                  "PYTEST_PLUGINS") if os.environ.get(name)]
    if blocked:
        print("offline check environment error: unset " + ", ".join(blocked)
              + "; live/native opt-ins and implicit pytest configuration are not allowed. "
              "Pass offline pytest selectors explicitly.", file=sys.stderr)
        return 2
    os.chdir(ROOT)
    paths = [str(ROOT / "src"), str(ROOT / "tests"), str(ROOT)]
    sys.path[:0] = paths
    os.environ["PYTHONPATH"] = os.pathsep.join(paths)
    # Unrelated installed pytest plugins must not alter collection or execution.
    os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    try:
        import pytest
    except ImportError as exc:
        print(f"offline check environment error: {exc}; "
              "install with python3 -m pip install -e '.[test]'", file=sys.stderr)
        return 2
    sys.meta_path.insert(0, OfflineImports())
    print("Offline code checks only; no execution/candidate/seal qualification. "
          "Evidence acceptance: python3 scripts/verify_s1.py REPORT", flush=True)
    return int(pytest.main(["-c", str(ROOT / "pyproject.toml"), "-ra", *sys.argv[1:]]))


if __name__ == "__main__":
    raise SystemExit(main())
