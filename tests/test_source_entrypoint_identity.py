"""Offline HEAD fixture; no repository commit, ROS or execution."""
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign.identity import source_manifest


class SourceEntrypointIdentity(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.committed = {"src/a.py": b"a=1\n", "config/a.json": b"{}",
                          "scripts/run_s1.py": b"main()\n", "scripts/nested/helper.sh": b"true\n",
                          "scripts/setup_overlay.bash": b"export X=1\n"}
        for name, content in self.committed.items():
            self.write(name, content)
        def git(args, **kwargs):
            cmd = args[3:]
            if cmd == ["rev-parse", "HEAD"]:
                return subprocess.CompletedProcess(args, 0, "a" * 40 + "\n", "")
            if cmd[0] == "ls-tree":
                self.assertIn("scripts", cmd)
                self.assertIn("-z", cmd)
                entries = (b"100644 blob " + self.oid(content) + b"\t" + name.encode() + b"\0"
                           for name, content in self.committed.items())
                return subprocess.CompletedProcess(args, 0, b"".join(entries), b"")
            if cmd[0] == "cat-file":
                blobs = {self.oid(content): content for content in self.committed.values()}
                data = b"".join(oid + b" blob " + str(len(blobs[oid])).encode() + b"\n"
                                + blobs[oid] + b"\n" for oid in kwargs["input"].splitlines())
                return subprocess.CompletedProcess(args, 0, data, b"")
            raise AssertionError(args)
        self.mock_git = patch("r2f.campaign.identity.subprocess.run", side_effect=git)
        self.mock_git.start()
        self.addCleanup(self.mock_git.stop)

    @staticmethod
    def oid(content):
        import hashlib
        return hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest().encode()

    def write(self, name, content):
        p = self.root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)

    def manifest(self):
        return source_manifest(self.root)

    def test_cli_edit_drifts_and_unfreezes(self):
        before = self.manifest()
        self.assertTrue(before["source_frozen"])
        self.write("scripts/run_s1.py", b"resume_and_budget()\n")
        after = self.manifest()
        self.assertFalse(after["source_frozen"])
        self.assertEqual(after["dirty_behavior_paths"], ["scripts/run_s1.py"])
        self.assertNotEqual(before["behavior_tree_sha256"], after["behavior_tree_sha256"])

    def test_all_supported_scripts_add_delete_and_edit(self):
        before = self.manifest()
        for suffix in ("py", "sh", "bash"):
            with self.subTest(suffix=suffix):
                name = "scripts/new." + suffix
                self.write(name, b"new")
                after = self.manifest()
                self.assertIn(name, after["dirty_behavior_paths"])
                self.assertNotEqual(before["behavior_tree_sha256"], after["behavior_tree_sha256"])
                (self.root / name).unlink()
        (self.root / "scripts/nested/helper.sh").unlink()
        after = self.manifest()
        self.assertIn("scripts/nested/helper.sh", after["dirty_behavior_paths"])
        self.assertFalse(after["source_frozen"])
        self.assertNotEqual(before["behavior_tree_sha256"], after["behavior_tree_sha256"])

    def test_unrelated_changes_do_not_drift(self):
        before = self.manifest()
        for name in ("docs/a.md", "tests/test_a.py", "results/raw.json", "scripts/README.md",
                     "scripts/__pycache__/ignored.py", "scripts/a.pyc"):
            self.write(name, b"not behavior")
        self.assertEqual(before, self.manifest())

    def test_cli_drift_between_plan_and_runtime_aborts_without_runtime_call(self):
        from r2f.campaign.runner import run_exploratory_cycle
        from r2f.campaign.feedback import FeedbackLog
        from r2f.campaign.scheduler import ExploratoryScheduler
        from test_fuzz_feedback_loop import make_corpus, profile
        corpus, feedback = make_corpus(), FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=7, warmup=0)
        calls = []
        def runtime(config, output_dir):
            calls.append(config)
            raise AssertionError("runtime must not run after CLI source drift")
        def hook(payload):
            if payload["phase"] == "planned":
                self.write("scripts/run_s1.py", b"changed_before_submit()")
        result = run_exploratory_cycle(self.root / "output", corpus=corpus,
            feedback=feedback, scheduler=scheduler, profile=profile(), repo=self.root,
            execute=True, runtime=runtime, max_inputs=1, checkpoint_hook=hook)
        self.assertEqual(calls, [])
        self.assertEqual(result["abort"]["reason_code"], "source_repo_HEAD_or_behavior_tree_changed")
        self.assertEqual(result["rows"][0]["execution_stage"], "aborted")
        self.assertEqual(result["rows"][0]["abort_reason"], result["abort"]["reason_code"])
        self.assertFalse(result["rows"][0]["actual_run"])
        self.assertEqual(result["budget"]["actual"], 0)
        self.assertEqual(feedback.events, [])

    def test_listing_failure_fails_closed(self):
        with patch("r2f.campaign.identity.subprocess.run", side_effect=[
                subprocess.CompletedProcess([], 0, "a" * 40, ""),
                subprocess.CompletedProcess([], 1, "", "error")]):
            with self.assertRaisesRegex(ValueError, "enumerate"):
                self.manifest()


if __name__ == "__main__":
    unittest.main()
