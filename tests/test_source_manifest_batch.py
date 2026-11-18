"""Offline source identity equivalence and bounded Git I/O; no ROS/campaigns.

Temporary repositories use synthetic commit objects/HEAD refs, never a commit in
this workspace. The legacy oracle intentionally keeps its per-file git show.
"""
import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign import identity


def legacy_source_manifest(repo, *, nul_paths=False):
    """Pre-batch implementation; optional NUL listing fixes only path quoting."""
    repo = Path(repo).resolve()
    result = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                            capture_output=True, text=True, timeout=10)
    head = result.stdout.strip() if result.returncode == 0 else None
    files = sorted(path for folder in (repo / "src", repo / "config")
                   for path in folder.rglob("*") if path.is_file()
                   and "__pycache__" not in path.parts and path.suffix != ".pyc")
    suffixes = {".py", ".sh", ".bash"}
    files.extend(path for path in (repo / "scripts").rglob("*")
                 if path.is_file() and path.suffix in suffixes and "__pycache__" not in path.parts)
    files = sorted(set(files))
    digest = hashlib.sha256()
    dirty, tracked = [], []
    if head:
        command = ["git", "-C", str(repo), "ls-tree", "-r", "--name-only"]
        if nul_paths:
            command.append("-z")
        listing = subprocess.run(command + ["HEAD", "--", "src", "config", "scripts"],
                                 capture_output=True, text=True, timeout=10)
        if listing.returncode:
            raise ValueError("cannot enumerate tracked behavior sources at HEAD")
        paths = listing.stdout.split("\0")[:-1] if nul_paths else listing.stdout.splitlines()
        tracked = [p for p in paths if not p.startswith("scripts/") or Path(p).suffix in suffixes]
        files = sorted(set(files) | {repo / p for p in tracked})
    for path in files:
        relative = str(path.relative_to(repo))
        if "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        if not path.is_file():
            dirty.append(relative)
            digest.update(relative.encode() + b"\0<missing>\0")
            continue
        content = path.read_bytes()
        digest.update(relative.encode() + b"\0" + content + b"\0")
        if not head or relative not in tracked:
            dirty.append(relative)
            continue
        committed = subprocess.run(["git", "-C", str(repo), "show", "HEAD:" + relative],
                                   capture_output=True, timeout=10)
        if committed.returncode or committed.stdout != content:
            dirty.append(relative)
    return {"source_repo": str(repo), "head": head, "behavior_tree_sha256": digest.hexdigest(),
            "source_frozen": bool(head) and not dirty, "dirty_behavior_paths": sorted(set(dirty))}


class GitFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="source-batch-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git("init", "-q")
        self.files = {"src/a.py": b"a=1\n", "config/p.json": b"{}\n",
                      "src/space name.py": b"space\n",
                      "scripts/run.py": b"main()\n", "scripts/nested/helper.sh": b"true\n",
                      "scripts/setup.bash": b"export X=1\n", "scripts/README.md": b"ignored\n",
                      "src/__pycache__/ignored.py": b"ignored", "config/ignored.pyc": b"ignored"}
        self.write_files(self.files)
        self.install_head(self.files)

    def git(self, *args, input=None):
        return subprocess.run(["git", "-C", str(self.root), *args], input=input,
                              capture_output=True, check=True, timeout=10).stdout

    def write_files(self, files):
        for relative, content in files.items():
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)

    def install_head(self, files, *, message=b"HEAD fixture"):
        """Construct deterministic objects; no git commit or user config writes."""
        self.git("read-tree", "--empty")
        for relative, content in sorted(files.items()):
            oid = self.git("hash-object", "-w", "--stdin", input=content).strip().decode()
            self.git("update-index", "--add", "--cacheinfo", "100644", oid, relative)
        tree = self.git("write-tree").strip()
        commit = (b"tree " + tree + b"\nauthor Fixture <fixture@localhost> 1 +0000\n"
                  b"committer Fixture <fixture@localhost> 1 +0000\n\n" + message + b"\n")
        oid = self.git("hash-object", "-t", "commit", "-w", "--stdin", input=commit).strip().decode()
        self.git("update-ref", "HEAD", oid)
        return oid

    def assert_equivalent(self, *, nul_paths=False):
        expected = legacy_source_manifest(self.root, nul_paths=nul_paths)
        actual = identity.source_manifest(self.root)
        self.assertEqual(actual.keys(), expected.keys())
        for key in expected:
            with self.subTest(field=key):
                self.assertEqual(actual[key], expected[key])
        return actual


class SourceManifestBatchTests(GitFixture):
    def test_legacy_field_equivalence_across_multiple_repositories(self):
        # Every case has its own real Git repository, including dirty and missing
        # HEAD paths; no mocks supply expected hashes or clean/dirty outcomes.
        cases = ("clean", "modified", "deleted", "untracked", "new_scripts", "ignored", "empty_blob")
        original = self.root
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                self.git("init", "-q")
                self.write_files(self.files)
                self.install_head(self.files)
                if case == "modified":
                    self.write_files({"src/a.py": b"changed\0binary\n"})
                elif case == "deleted":
                    (self.root / "config/p.json").unlink()
                elif case == "untracked":
                    self.write_files({"src/new.py": b"new", "config/new.toml": b"new"})
                elif case == "new_scripts":
                    self.write_files({"scripts/new." + suffix: b"new" for suffix in ("py", "sh", "bash")})
                elif case == "ignored":
                    self.write_files({name: b"irrelevant" for name in
                                      ("docs/a.md", "tests/t.py", "results/raw", "scripts/a.txt",
                                       "scripts/__pycache__/a.py", "src/ignored.pyc")})
                elif case == "empty_blob":
                    self.files["src/empty.py"] = b""
                    self.write_files({"src/empty.py": b""})
                    self.install_head(self.files)
                result = self.assert_equivalent()
                self.assertEqual(result["source_frozen"], case in ("clean", "ignored", "empty_blob"))
        self.root = original

    def test_space_newline_tab_unicode_paths_and_binary_framing(self):
        odd = {"src/a space.py": b"same\0\nblob 123\n", "config/line\nbreak.json": b"\0end\n",
               "scripts/line\nbreak.sh": b"\n", "scripts/tab\t脚本.bash": b"blob\n",
               "src/payload.py": b"", "src/shared.py": b"same\0\nblob 123\n"}
        self.files.update(odd)
        self.write_files(odd)
        self.install_head(self.files)
        self.assertTrue(self.assert_equivalent(nul_paths=True)["source_frozen"])
        self.write_files({"scripts/line\nbreak.sh": b"changed"})
        (self.root / "config/line\nbreak.json").unlink()
        self.write_files({"scripts/new\nentry.py": b"new"})
        result = self.assert_equivalent(nul_paths=True)
        self.assertEqual(result["dirty_behavior_paths"],
                         ["config/line\nbreak.json", "scripts/line\nbreak.sh", "scripts/new\nentry.py"])

    def test_head_only_and_head_content_changes_are_detected(self):
        before = self.assert_equivalent()
        changed = dict(self.files, **{"src/a.py": b"a=2\n"})
        self.install_head(changed)
        after = self.assert_equivalent()
        self.assertNotEqual(before["head"], after["head"])
        self.assertEqual(before["behavior_tree_sha256"], after["behavior_tree_sha256"])
        self.assertEqual(after["dirty_behavior_paths"], ["src/a.py"])
        self.write_files({"src/a.py": b"a=2\n"})
        final = self.assert_equivalent()
        self.assertTrue(final["source_frozen"])
        self.assertNotEqual(before["behavior_tree_sha256"], final["behavior_tree_sha256"])

    def test_metadata_only_head_change_keeps_content_fields_but_changes_head(self):
        before = self.assert_equivalent()
        self.install_head(self.files, message=b"different HEAD, identical behavior tree")
        after = self.assert_equivalent()
        self.assertNotEqual(before["head"], after["head"])
        for key in before.keys() - {"head"}:
            self.assertEqual(before[key], after[key])

    def test_sha256_object_ids_use_the_same_batch_framing(self):
        original = self.root
        with tempfile.TemporaryDirectory() as directory:
            self.root = Path(directory)
            self.git("init", "-q", "--object-format=sha256")
            self.write_files(self.files)
            self.install_head(self.files)
            manifest = self.assert_equivalent()
            self.assertEqual(len(manifest["head"]), 64)
            self.assertTrue(manifest["source_frozen"])
        self.root = original

    def test_file_to_directory_and_symlink_preserve_legacy_detection(self):
        path = self.root / "src/a.py"
        path.unlink()
        path.mkdir()
        self.write_files({"src/a.py/new.py": b"nested"})
        result = self.assert_equivalent()
        self.assertEqual(result["dirty_behavior_paths"], ["src/a.py", "src/a.py/new.py"])
        (path / "new.py").unlink()
        path.rmdir()
        self.write_files({"outside.txt": self.files["src/a.py"]})
        path.symlink_to(self.root / "outside.txt")
        self.assertTrue(self.assert_equivalent()["source_frozen"])
        self.write_files({"outside.txt": b"changed target content"})
        self.assertEqual(self.assert_equivalent()["dirty_behavior_paths"], ["src/a.py"])
        path.unlink()
        path.symlink_to(self.root / "does-not-exist")
        self.assertEqual(self.assert_equivalent()["dirty_behavior_paths"], ["src/a.py"])

    def test_no_head_and_non_git_have_legacy_semantics(self):
        for initialized in (False, True):
            with self.subTest(initialized=initialized), tempfile.TemporaryDirectory() as directory:
                old_root, self.root = self.root, Path(directory)
                if initialized:
                    self.git("init", "-q")
                self.write_files({"src/a.py": b"data", "scripts/new.sh": b"new"})
                manifest = self.assert_equivalent()
                self.assertIsNone(manifest["head"])
                self.assertFalse(manifest["source_frozen"])
                self.assertEqual(manifest["dirty_behavior_paths"], ["scripts/new.sh", "src/a.py"])
                self.root = old_root

    def test_same_size_same_mtime_edit_is_not_cached(self):
        import os
        before = self.assert_equivalent()
        path = self.root / "src/a.py"
        stat = path.stat()
        path.write_bytes(b"b=2\n")
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        after = self.assert_equivalent()
        self.assertNotEqual(before["behavior_tree_sha256"], after["behavior_tree_sha256"])
        self.assertFalse(after["source_frozen"])

    def test_only_four_git_processes_for_many_tracked_files(self):
        extra = {f"src/many/{i}.py": (b"repeated" if i % 2 else str(i).encode()) for i in range(160)}
        self.files.update(extra)
        self.write_files(extra)
        self.install_head(self.files)
        real_run = subprocess.run
        with patch.object(identity.subprocess, "run", wraps=real_run) as run:
            self.assertTrue(identity.source_manifest(self.root)["source_frozen"])
        self.assertEqual(run.call_count, 4)
        commands = [call.args[0][3] for call in run.call_args_list]
        self.assertEqual(commands, ["rev-parse", "ls-tree", "cat-file", "rev-parse"])

    def test_empty_tree_and_all_missing_sources(self):
        self.install_head({})
        self.assert_equivalent()
        for relative in self.files:
            (self.root / relative).unlink()
        self.assertTrue(self.assert_equivalent()["source_frozen"])
        self.install_head(self.files)
        self.assertFalse(self.assert_equivalent()["source_frozen"])

    def test_deleted_tracked_script_and_ignored_tracked_cache(self):
        (self.root / "scripts/nested/helper.sh").unlink()
        (self.root / "src/__pycache__/ignored.py").unlink()
        (self.root / "config/ignored.pyc").unlink()
        manifest = self.assert_equivalent()
        self.assertEqual(manifest["dirty_behavior_paths"], ["scripts/nested/helper.sh"])

    def test_persistent_changes_during_collection_fail_closed(self):
        changes = ("modify", "delete", "add", "new_script")
        real_run = subprocess.run
        for change in changes:
            with self.subTest(change=change):
                def run(args, **kwargs):
                    result = real_run(args, **kwargs)
                    if args[3] == "cat-file":
                        if change == "modify":
                            self.write_files({"src/a.py": b"changed"})
                        elif change == "delete":
                            (self.root / "src/a.py").unlink()
                        else:
                            self.write_files({"scripts/added.sh" if change == "new_script" else "config/added": b"new"})
                    return result
                with patch.object(identity.subprocess, "run", side_effect=run):
                    with self.assertRaisesRegex(ValueError, "behavior sources changed"):
                        identity.source_manifest(self.root)
                self.write_files({"src/a.py": self.files["src/a.py"]})
                for name in ("scripts/added.sh", "config/added"):
                    (self.root / name).unlink(missing_ok=True)

    def test_head_changes_during_batch_fail_closed_and_listing_is_pinned(self):
        before = self.git("rev-parse", "HEAD").strip().decode()
        other = self.install_head(dict(self.files, **{"src/a.py": b"other"}))
        self.git("update-ref", "HEAD", before)
        real_run = subprocess.run
        commands = []
        def run(args, **kwargs):
            commands.append(args)
            result = real_run(args, **kwargs)
            if args[3] == "cat-file":
                real_run(["git", "-C", str(self.root), "update-ref", "HEAD", other], check=True)
            return result
        with patch.object(identity.subprocess, "run", side_effect=run):
            with self.assertRaisesRegex(ValueError, "HEAD changed"):
                identity.source_manifest(self.root)
        self.assertIn(before, commands[1])
        self.assertNotIn("HEAD", commands[1])

    def test_head_appearing_or_disappearing_fails_closed(self):
        head = self.git("rev-parse", "HEAD").strip().decode()
        real_run = subprocess.run
        for appear in (False, True):
            with self.subTest(appear=appear):
                if appear:
                    self.git("update-ref", "-d", "HEAD")
                def run(args, **kwargs):
                    result = real_run(args, **kwargs)
                    if args[3] == "rev-parse":
                        command = (["update-ref", "HEAD", head] if appear else ["update-ref", "-d", "HEAD"])
                        real_run(["git", "-C", str(self.root), *command], check=True)
                    return result
                with patch.object(identity.subprocess, "run", side_effect=run):
                    with self.assertRaisesRegex(ValueError, "HEAD changed"):
                        identity.source_manifest(self.root)
                self.git("update-ref", "HEAD", head)

    def test_git_failures_and_malformed_batch_fail_closed(self):
        real_run = subprocess.run
        for fault in ("listing", "bad_listing", "batch", "truncated", "wrong_oid", "missing", "size", "wrong_kind", "trailing", "final_head"):
            with self.subTest(fault=fault):
                def run(args, **kwargs):
                    result = real_run(args, **kwargs)
                    if args[3] == "ls-tree" and fault == "listing":
                        return subprocess.CompletedProcess(args, 1, b"", b"failure")
                    if args[3] == "ls-tree" and fault == "bad_listing":
                        result.stdout = result.stdout[:-1]
                    if args[3] == "cat-file":
                        if fault == "batch":
                            return subprocess.CompletedProcess(args, 1, b"", b"failure")
                        if fault == "truncated":
                            result.stdout = result.stdout[:-1]
                        elif fault == "wrong_oid":
                            result.stdout = b"0" * 40 + result.stdout[40:]
                        elif fault == "missing":
                            result.stdout = b"0" * 40 + b" missing\n"
                        elif fault == "size":
                            header, body = result.stdout.split(b"\n", 1)
                            result.stdout = header.rsplit(b" ", 1)[0] + b" -1\n" + body
                        elif fault == "wrong_kind":
                            result.stdout = result.stdout.replace(b" blob ", b" tree ", 1)
                        elif fault == "trailing":
                            result.stdout += b"garbage"
                    if args[3] == "rev-parse" and fault == "final_head":
                        run.head_calls += 1
                        if run.head_calls == 2:
                            return subprocess.CompletedProcess(args, 1, "", "failure")
                    return result
                run.head_calls = 0
                with patch.object(identity.subprocess, "run", side_effect=run):
                    with self.assertRaises(ValueError):
                        identity.source_manifest(self.root)

    def test_git_timeouts_and_read_failure_propagate(self):
        real_run = subprocess.run
        for command in ("rev-parse", "ls-tree", "cat-file"):
            with self.subTest(command=command):
                def run(args, **kwargs):
                    if args[3] == command:
                        raise subprocess.TimeoutExpired(args, kwargs["timeout"])
                    return real_run(args, **kwargs)
                with patch.object(identity.subprocess, "run", side_effect=run):
                    with self.assertRaises(subprocess.TimeoutExpired):
                        identity.source_manifest(self.root)
        with patch.object(Path, "read_bytes", side_effect=PermissionError("cannot read source")):
            with self.assertRaises(PermissionError):
                identity.source_manifest(self.root)


if __name__ == "__main__":
    unittest.main()
