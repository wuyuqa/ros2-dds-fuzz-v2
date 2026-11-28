"""Exercise delivery validation through its public CLI and actual isolated git trees."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_delivery.py"


def git(root, *args):
    return subprocess.run(["git", *args], cwd=root, check=True,
                          text=True, capture_output=True).stdout.strip()


@pytest.fixture
def delivery(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-q")
    (source / "value.txt").write_text("before\n")
    git(source, "add", "value.txt")
    git(source, "-c", "user.name=Fixture", "-c", "user.email=fixture@localhost",
        "commit", "-qm", "fixture")
    head = git(source, "rev-parse", "HEAD")
    worker = tmp_path / "worker"
    git(source, "worktree", "add", "--detach", str(worker), head)
    (worker / "value.txt").write_text("after\n")
    patch = tmp_path / "change.patch"
    patch.write_text(git(worker, "diff") + "\n")
    log = tmp_path / "test.log"
    log.write_text("previous test result\n")
    files = [worker / "value.txt", patch, log]
    record = tmp_path / "source-identity.json"
    data = {"worktree": str(worker), "base_head": head,
            "changed_paths": [" M value.txt"],
            "artifacts": [{"path": str(p), "bytes": p.stat().st_size,
                           "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                          for p in files]}
    record.write_text(json.dumps(data))
    return source, worker, patch, record, data


def run_delivery(delivery, *extra):
    source, _, patch, record, _ = delivery
    command = [sys.executable, str(SCRIPT), str(record), "--source", str(source),
               "--patch", str(patch), "--allow-path", "value.txt", *extra]
    return subprocess.run(command, text=True, capture_output=True)


def test_delivery_is_applied_and_checked_without_changing_source_or_worker(delivery):
    source, worker, _, _, _ = delivery
    result = run_delivery(delivery, "--", sys.executable, "-c",
                          "from pathlib import Path; assert Path('value.txt').read_text() == 'after\\n'")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["status"] == "verified"
    assert report["checks"]["returncode"] == 0
    assert (source / "value.txt").read_text() == "before\n"
    assert (worker / "value.txt").read_text() == "after\n"
    assert git(source, "status", "--porcelain") == ""


@pytest.mark.parametrize("fault,code", [
    ("missing_worktree", "worktree_missing"),
    ("corrupt_patch", "artifact_mismatch"),
    ("outside_scope", "write_scope_violation"),
    ("changed_artifact", "artifact_mismatch"),
])
def test_invalid_delivery_has_named_failure(delivery, fault, code):
    _, worker, patch, record, data = delivery
    if fault == "missing_worktree":
        data["worktree"] = str(worker / "absent")
    elif fault == "corrupt_patch":
        patch.write_text("corrupt\n")
    elif fault == "outside_scope":
        (worker / "unexpected.txt").write_text("unexpected")
    else:
        (worker / "value.txt").write_text("tampered\n")
    record.write_text(json.dumps(data))
    result = run_delivery(delivery)
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == code


def test_new_source_requires_new_integration_check(delivery):
    source, _, _, _, _ = delivery
    (source / "other.txt").write_text("new base\n")
    git(source, "add", "other.txt")
    git(source, "-c", "user.name=Fixture", "-c", "user.email=fixture@localhost",
        "commit", "-qm", "source advanced")
    result = run_delivery(delivery)
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == "base_changed_requires_checks"
    result = run_delivery(delivery, "--", sys.executable, "-c", "assert True")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["base_changed"] is True


def test_failed_integration_check_is_not_reported_as_verified(delivery):
    result = run_delivery(delivery, "--", sys.executable, "-c", "raise SystemExit(9)")
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == "integration_check_failed"


def test_patch_must_reproduce_delivered_source_bytes(delivery):
    _, worker, _, record, data = delivery
    (worker / "value.txt").write_text("different delivered bytes\n")
    data["artifacts"][0]["bytes"] = (worker / "value.txt").stat().st_size
    data["artifacts"][0]["sha256"] = hashlib.sha256((worker / "value.txt").read_bytes()).hexdigest()
    record.write_text(json.dumps(data))
    result = run_delivery(delivery)
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == "patch_source_mismatch"


def test_no_check_command_is_explicitly_partial(delivery):
    result = run_delivery(delivery)
    assert result.returncode == 0
    report = json.loads(result.stdout)
    assert report["status"] == "patch_verified_checks_not_run"
    assert report["checks"]["status"] == "not_run"
    assert report["commit_go"] is False
    assert report["runtime_qualified"] is False


def test_changed_base_does_not_hide_mismatched_delivered_source(delivery):
    source, worker, _, record, data = delivery
    (source / "other.txt").write_text("next base\n")
    git(source, "add", "other.txt")
    git(source, "-c", "user.name=Fixture", "-c", "user.email=fixture@localhost",
        "commit", "-qm", "advance")
    (worker / "value.txt").write_text("mismatch\n")
    data["artifacts"][0]["bytes"] = (worker / "value.txt").stat().st_size
    data["artifacts"][0]["sha256"] = hashlib.sha256((worker / "value.txt").read_bytes()).hexdigest()
    record.write_text(json.dumps(data))
    result = run_delivery(delivery, "--", sys.executable, "-c", "assert True")
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == "patch_source_mismatch"


def test_malformed_record_is_a_named_rejection(delivery):
    delivery[3].write_text("[]")
    result = run_delivery(delivery)
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == "delivery_schema_invalid"


def test_check_timeout_has_a_named_result(delivery):
    result = run_delivery(delivery, "--timeout", "0.05", "--", sys.executable,
                          "-c", "import time; time.sleep(2)")
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == "integration_check_timeout"


def rebind_artifacts(record, data):
    for item in data["artifacts"]:
        path = Path(item["path"])
        item.update(bytes=path.stat().st_size,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    record.write_text(json.dumps(data))


@pytest.mark.parametrize("advance", [False, True], ids=["original_base", "advanced_base"])
def test_patch_must_reproduce_executable_mode(delivery, advance):
    source, worker, patch, record, data = delivery
    (worker / "value.txt").chmod(0o755)
    if advance:
        (source / "other.txt").write_text("new base\n")
        git(source, "add", "other.txt")
        git(source, "-c", "user.name=Fixture", "-c", "user.email=fixture@localhost",
            "commit", "-qm", "advance mode fixture")
    # The hash-bound content patch omits chmod. This command passes in its 0644 clone.
    check = "from pathlib import Path; assert Path('value.txt').stat().st_mode & 0o111 == 0"
    result = run_delivery(delivery, "--", sys.executable, "-c", check)
    assert result.returncode != 0, result.stdout
    assert json.loads(result.stdout)["error"] == "patch_source_mode_mismatch"
    # Bind the correct patch, including its mode change, through the public CLI.
    patch.write_text(git(worker, "diff") + "\n")
    rebind_artifacts(record, data)
    result = run_delivery(delivery, "--", sys.executable, "-c",
                          "from pathlib import Path; assert Path('value.txt').stat().st_mode & 0o100")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["status"] == "verified"
    assert report["source_modes"] == {"value.txt": "100755"}
    assert report["patch_sha256"] == hashlib.sha256(patch.read_bytes()).hexdigest()
    assert report["record_sha256"] == hashlib.sha256(record.read_bytes()).hexdigest()


@pytest.mark.parametrize("dangling", [False, True])
def test_worker_symlink_type_is_explicitly_unsupported(delivery, dangling):
    source, worker, _, _, _ = delivery
    path = worker / "value.txt"
    path.unlink()
    path.symlink_to(source / ("absent" if dangling else "value.txt"))
    result = run_delivery(delivery)
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == "source_type_unsupported"


def test_worker_chmod_during_checks_rejected(delivery):
    _, worker, _, _, _ = delivery
    result = run_delivery(delivery, "--", sys.executable, "-c",
                          f"from pathlib import Path; Path({str(worker / 'value.txt')!r}).chmod(0o755)")
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == "worker_changed_during_check"


def test_patch_symlink_type_is_explicitly_unsupported(delivery):
    _, worker, patch, record, data = delivery
    path = worker / "value.txt"
    contents = path.read_text()
    path.unlink()
    path.symlink_to(contents)
    patch.write_text(git(worker, "diff") + "\n")
    path.unlink()
    path.write_text(contents)
    rebind_artifacts(record, data)
    result = run_delivery(delivery)
    assert result.returncode != 0
    assert json.loads(result.stdout)["error"] == "source_type_unsupported"


def test_correct_mode_only_patch_is_verified(delivery):
    _, worker, patch, record, data = delivery
    (worker / "value.txt").write_text("before\n")
    (worker / "value.txt").chmod(0o755)
    patch.write_text(git(worker, "diff") + "\n")
    rebind_artifacts(record, data)
    result = run_delivery(delivery, "--", sys.executable, "-c",
                          "from pathlib import Path; assert Path('value.txt').stat().st_mode & 0o100")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["source_modes"] == {"value.txt": "100755"}
