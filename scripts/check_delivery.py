#!/usr/bin/env python3
"""Validate an existing source-identity delivery and apply it in a disposable clone.

The caller supplies the write scope and optional check command after ``--``.
Results qualify only the checked integration, never commits or DDS execution.
"""
import argparse
import hashlib
import json
import os
import stat
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tempfile


class DeliveryError(Exception):
    def __init__(self, code, detail=""):
        super().__init__(detail)
        self.code = code
        self.detail = str(detail)


def git(root, *args):
    result = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", *args],
                            cwd=root, capture_output=True)
    if result.returncode:
        raise DeliveryError("git_failed", result.stderr.decode(errors="replace"))
    return result.stdout


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def source_mode(path):
    """Git tracks owner execute permission for regular files; reject other types."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(mode):
        raise DeliveryError("source_type_unsupported", path)
    return "100755" if mode & stat.S_IXUSR else "100644"


def safe_relative(value):
    p = PurePosixPath(value)
    if not value or p.is_absolute() or ".." in p.parts or ".git" in p.parts:
        raise DeliveryError("unsafe_path", value)
    return str(p)


def changed_paths(root):
    entries = git(root, "status", "--porcelain=v1", "-z", "--untracked-files=all").split(b"\0")
    paths = set()
    index = 0
    while index < len(entries):
        entry = entries[index]
        index += 1
        if not entry:
            continue
        paths.add(safe_relative(os.fsdecode(entry[3:])))
        if b"R" in entry[:2] or b"C" in entry[:2]:
            paths.add(safe_relative(os.fsdecode(entries[index])))
            index += 1
    return paths


def artifact_records(data):
    artifacts = data.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise DeliveryError("delivery_schema_invalid", "nonempty artifacts required")
    result = {}
    for item in artifacts:
        if not isinstance(item, dict):
            raise DeliveryError("delivery_schema_invalid", "artifact must be an object")
        path = Path(item["path"])
        if not path.is_absolute() or str(path) in result:
            raise DeliveryError("delivery_schema_invalid", "unique absolute artifact paths required")
        if (path.is_symlink() or not path.is_file() or path.stat().st_size != item["bytes"]
                or digest(path) != item["sha256"]):
            raise DeliveryError("artifact_mismatch", path)
        result[str(path)] = item
    return result


def verify(args, command):
    record_bytes = args.record.read_bytes()
    data = json.loads(record_bytes)
    if not isinstance(data, dict):
        raise DeliveryError("delivery_schema_invalid", "record must be an object")
    worker = Path(data["worktree"])
    if not worker.is_dir():
        raise DeliveryError("worktree_missing", worker)
    if not worker.is_absolute() or worker.resolve() != worker:
        raise DeliveryError("worktree_identity_invalid", worker)
    if Path(os.fsdecode(git(worker, "rev-parse", "--show-toplevel")).strip()) != worker:
        raise DeliveryError("worktree_identity_invalid", worker)
    base = data["base_head"]
    if os.fsdecode(git(worker, "rev-parse", "HEAD")).strip() != base:
        raise DeliveryError("worker_base_mismatch", worker)
    source_head = os.fsdecode(git(args.source, "rev-parse", "HEAD")).strip()
    scope = {safe_relative(p) for p in args.allow_path}
    actual = changed_paths(worker)
    entries = data["changed_paths"]
    if not isinstance(entries, list) or any(not isinstance(p, str) or len(p) < 4 for p in entries):
        raise DeliveryError("delivery_schema_invalid", "porcelain changed_paths required")
    declared = {safe_relative(p[3:]) for p in entries}
    if not actual <= scope or not declared <= scope:
        raise DeliveryError("write_scope_violation", sorted((actual | declared) - scope))
    if actual != declared:
        raise DeliveryError("changed_paths_mismatch", sorted(actual ^ declared))
    worker_modes = {name: source_mode(worker / name) for name in declared}
    artifacts = artifact_records(data)
    patch = args.patch.absolute()
    if str(patch) not in artifacts:
        raise DeliveryError("patch_unbound", patch)
    patch_bytes = patch.read_bytes()
    # Git parses the patch; -z makes quoted filenames unambiguous.
    stat = subprocess.run(["git", "apply", "--numstat", "-z", "-"],
                          input=patch_bytes, cwd=args.source, capture_output=True)
    if stat.returncode:
        raise DeliveryError("patch_invalid", stat.stderr.decode(errors="replace"))
    patch_paths = set()
    for entry in stat.stdout.split(b"\0"):
        if entry:
            parts = entry.split(b"\t", 2)
            if len(parts) != 3:
                raise DeliveryError("patch_invalid", "unsupported path record")
            patch_paths.add(safe_relative(os.fsdecode(parts[2])))
    if patch_paths != declared or not patch_paths <= scope:
        raise DeliveryError("patch_scope_mismatch", sorted(patch_paths ^ declared))
    if source_head != base and not command:
        raise DeliveryError("base_changed_requires_checks", f"{base} -> {source_head}")
    with tempfile.TemporaryDirectory(prefix="r2f-delivery-", dir=args.scratch_parent) as scratch:
        clone = Path(scratch) / "integration"
        git(args.source, "clone", "--shared", "--no-checkout", "--", str(args.source.resolve()), str(clone))
        git(clone, "checkout", "--detach", source_head)
        # Apply/check the same already-verified bytes, not a mutable external pathname.
        applied = subprocess.run(["git", "apply", "--whitespace=error", "-"],
                                 input=patch_bytes, cwd=clone, capture_output=True)
        if applied.returncode:
            raise DeliveryError("patch_apply_failed", applied.stderr.decode(errors="replace"))
        # Always compare the patch at its original base with delivered source,
        # even when integration now targets a newer commit.
        original_clone = clone
        if source_head != base:
            original_clone = Path(scratch) / "original-base"
            git(args.source, "clone", "--shared", "--no-checkout", "--", str(args.source.resolve()), str(original_clone))
            git(original_clone, "checkout", "--detach", base)
            original_apply = subprocess.run(["git", "apply", "--whitespace=error", "-"],
                                            input=patch_bytes, cwd=original_clone, capture_output=True)
            if original_apply.returncode:
                raise DeliveryError("patch_original_base_failed", original_apply.stderr.decode(errors="replace"))
        for name in declared:
            original = worker / name
            integrated = original_clone / name
            integrated_mode = source_mode(integrated)
            if worker_modes[name] != integrated_mode:
                raise DeliveryError("patch_source_mode_mismatch", name)
            if worker_modes[name] is not None:
                bound = artifacts.get(str(original))
                if not bound:
                    raise DeliveryError("source_artifact_unbound", original)
                if not integrated.is_file() or integrated.is_symlink() or digest(integrated) != bound["sha256"]:
                    raise DeliveryError("patch_source_mismatch", name)
            elif integrated_mode is not None:
                raise DeliveryError("patch_source_mismatch", name)
        checks = {"status": "not_run"}
        if command:
            env = dict(os.environ)
            env["PYTHONPATH"] = str(clone / "src")
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            checked = subprocess.run(command, cwd=clone, env=env, capture_output=True, text=True, timeout=args.timeout)
            checks = {"command": command, "returncode": checked.returncode,
                      "stdout": checked.stdout, "stderr": checked.stderr}
            if checked.returncode:
                raise DeliveryError("integration_check_failed", json.dumps(checks))
        # The original source/worker/record must still identify this validation.
        if os.fsdecode(git(args.source, "rev-parse", "HEAD")).strip() != source_head:
            raise DeliveryError("source_changed_during_check")
        if args.record.read_bytes() != record_bytes:
            raise DeliveryError("delivery_changed_during_check")
        artifact_records(data)
        if ({name: source_mode(worker / name) for name in declared} != worker_modes
                or changed_paths(worker) != actual
                or os.fsdecode(git(worker, "rev-parse", "HEAD")).strip() != base):
            raise DeliveryError("worker_changed_during_check")
        return {"status": "verified" if command else "patch_verified_checks_not_run",
                "record_sha256": hashlib.sha256(record_bytes).hexdigest(),
                "patch_sha256": hashlib.sha256(patch_bytes).hexdigest(),
                "base_head": base, "source_head": source_head,
                "base_changed": source_head != base, "paths": sorted(patch_paths),
                "source_modes": worker_modes,
                "checks": checks, "scratch_removed_on_exit": True,
                "commit_go": False, "runtime_qualified": False}


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    command = []
    if "--" in argv:
        marker = argv.index("--")
        command, argv = argv[marker + 1:], argv[:marker]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("record", type=Path, help="Existing source-identity.json with bound artifacts")
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--patch", required=True, type=Path)
    parser.add_argument("--allow-path", action="append", required=True)
    parser.add_argument("--scratch-parent", type=Path)
    parser.add_argument("--timeout", type=float, default=600, help="Integration check timeout in seconds")
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    try:
        report = verify(args, command)
    except subprocess.TimeoutExpired as exc:
        report = {"status": "rejected", "error": "integration_check_timeout", "detail": str(exc)}
    except DeliveryError as exc:
        report = {"status": "rejected", "error": exc.code, "detail": exc.detail}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        report = {"status": "rejected", "error": "delivery_input_error", "detail": str(exc)}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report["status"] == "rejected" else 0


if __name__ == "__main__":
    raise SystemExit(main())
