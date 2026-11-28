"""Exercise the offline entrypoint in real child interpreters, never ROS/DDS."""
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "scripts" / "check_offline.py"


def run_cli(tmp_path, *args, environment=None, interpreter_args=()):
    env = dict(os.environ)
    for name in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS", "R2F_NATIVE_DDS_TESTS", "R2F_BOUNDARY_KERNEL_TESTS",
                 "R2F_FILTER_TRACE_TEST_MANIFEST"):
        env.pop(name, None)
    env.update(environment or {})
    return subprocess.run(
        [sys.executable, *interpreter_args, str(CLI), *map(str, args)],
        cwd=tmp_path, env=env, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, timeout=60,
    )


def test_both_styles_execute_with_source_layout_and_visible_skip(tmp_path):
    suite = tmp_path / "test_mixed.py"
    suite.write_text('''import unittest
import pytest
import r2f.core.scenario

class LegacyTests(unittest.TestCase):
    def test_unittest(self):
        print("UNITTEST_EXECUTED")
        self.assertEqual(2 + 2, 4)

def test_function():
    print("PYTEST_EXECUTED")
    assert 2 + 2 == 4

@pytest.mark.skip(reason="explicit offline fixture skip")
def test_skipped():
    raise AssertionError("must stay skipped")
''')
    junit = tmp_path / "mixed.xml"
    result = run_cli(tmp_path, suite, "-s", "--junitxml", junit,
                     environment={"PYTHONPATH": str(tmp_path / "absent")})
    assert result.returncode == 0, result.stdout
    assert "UNITTEST_EXECUTED" in result.stdout
    assert "PYTEST_EXECUTED" in result.stdout
    assert "2 passed, 1 skipped" in result.stdout
    assert "explicit offline fixture skip" in result.stdout
    assert junit.is_file()


def test_collection_passthrough_does_not_execute_test_bodies(tmp_path):
    suite = tmp_path / "test_collect.py"
    suite.write_text('''import unittest
class LegacyTests(unittest.TestCase):
    def test_legacy(self):
        raise AssertionError("collection must not execute")
def test_function():
    raise AssertionError("collection must not execute")
''')
    result = run_cli(tmp_path, suite, "--collect-only", "-q")
    assert result.returncode == 0, result.stdout
    assert "LegacyTests::test_legacy" in result.stdout
    assert "::test_function" in result.stdout
    assert "2 tests collected" in result.stdout


# Exit values are pytest's public CLI contract, not wrapper-specific codes.
@pytest.mark.parametrize("source,expected_code,message", [
    ("def test_function():\n    assert False, 'FUNCTION_FAILURE'\n", 1, "FUNCTION_FAILURE"),
    ("import unittest\nclass LegacyTests(unittest.TestCase):\n"
     "    def test_failure(self):\n        self.fail('LEGACY_FAILURE')\n", 1, "LEGACY_FAILURE"),
    ("raise RuntimeError('COLLECTION_ERROR')\n", 2, "COLLECTION_ERROR"),
    ("def test_error():\n    raise RuntimeError('EXECUTION_ERROR')\n", 1, "EXECUTION_ERROR"),
    ("# empty module\n", 5, "no tests ran"),
])
def test_preserves_pytest_failure_exit_codes(tmp_path, source, expected_code, message):
    suite = tmp_path / "test_failure.py"
    suite.write_text(source)
    result = run_cli(tmp_path, suite, "-q")
    assert result.returncode == expected_code, result.stdout
    assert message in result.stdout


def test_missing_pytest_is_a_visible_environment_failure(tmp_path):
    result = run_cli(tmp_path, "--collect-only", interpreter_args=("-S",))
    assert result.returncode == 2, result.stdout
    assert "environment error" in result.stdout
    assert ".[test]" in result.stdout


@pytest.mark.parametrize("name,value", [
    ("R2F_NATIVE_DDS_TESTS", "1"),
    ("R2F_BOUNDARY_KERNEL_TESTS", "1"),
    ("R2F_FILTER_TRACE_TEST_MANIFEST", "/tmp/native-manifest.json"),
    ("PYTEST_ADDOPTS", "-k hidden_selection"),
    ("PYTEST_PLUGINS", "uncontrolled_plugin"),
])
def test_rejects_inherited_live_opt_ins_and_hidden_pytest_configuration(tmp_path, name, value):
    suite = tmp_path / "test_never_run.py"
    suite.write_text("raise RuntimeError('TESTS_MUST_NOT_BE_IMPORTED')\n")
    result = run_cli(tmp_path, suite, environment={name: value})
    assert result.returncode == 2, result.stdout
    assert name in result.stdout
    assert "TESTS_MUST_NOT_BE_IMPORTED" not in result.stdout


def test_real_ros_import_is_blocked_before_module_execution(tmp_path):
    # A tiny available module stands in for an installed ROS package. The import
    # guard must fail without executing it; mocked sys.modules stay usable.
    (tmp_path / "rclpy.py").write_text("raise RuntimeError('ROS_MODULE_EXECUTED')\n")
    suite = tmp_path / "test_ros.py"
    suite.write_text("import rclpy\ndef test_never():\n    pass\n")
    result = run_cli(tmp_path, suite)
    assert result.returncode == 2, result.stdout
    assert "offline check forbids real ROS imports" in result.stdout
    assert "ROS_MODULE_EXECUTED" not in result.stdout


def test_real_ros_import_is_blocked_in_parallel_worker(tmp_path):
    (tmp_path / "rclpy.py").write_text("raise RuntimeError('ROS_MODULE_EXECUTED')\n")
    suite = tmp_path / "test_parallel_ros.py"
    suite.write_text("import rclpy\ndef test_never():\n    pass\n")
    result = run_cli(tmp_path, suite, "-p", "xdist.plugin", "-n", "2", "--dist", "loadfile")
    assert result.returncode != 0, result.stdout
    assert "offline check forbids real ROS imports" in result.stdout
    assert "ROS_MODULE_EXECUTED" not in result.stdout
