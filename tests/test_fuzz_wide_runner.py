"""T-E offline contracts: wide sampling and bounded chains in the runner.

Everything here is planned-only.  No ROS process, namespace, nft rule or
campaign is started; a generated row is an input document, never execution
evidence, a hit or a candidate.

The legacy golden hashes below were captured by running this same fixture on a
``git archive HEAD`` export (HEAD e2268bf) *and* on the working tree, then
comparing them byte for byte.  A mismatch means the T-E opt-in leaked into the
undeclared (legacy) generation path.
"""
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from r2f.campaign.corpus import Corpus, bucket_key
from r2f.campaign.feedback import FeedbackLog
import r2f.campaign.runner as runner_module
from r2f.campaign.runner import run_exploratory_cycle
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core import Registry
from r2f.core.evidence import canonical_hash
from r2f.modules.s1 import fuzz_catalog
from r2f.modules.s1.module import register_exploratory
from r2f.reports.run_report import exploratory_coverage_table

SESSION_SOURCE = {"source_repo": "/fixture/repo", "head": "e2268bf",
                  "behavior_tree_sha256": "f" * 64, "source_frozen": True,
                  "dirty_behavior_paths": []}

CYCLONEDDS = "rmw_cyclonedds_cpp"
FASTRTPS = "rmw_fastrtps_cpp"

#: (surface, rmw) -> (legacy row hash, content, plan, v2 digest)
LEGACY_GOLDENS = {
    ("P01", CYCLONEDDS): (
        "af4350662f28c5710d3dd0a91d066c91c725d9a371748ed26bfc360e89563f7e",
        "425516989654156cc4d6d1ec62b7cd220d85959b6327f0e93e888a3f11dfc4c8",
        "623a3872fd4cb5cd908bf2f0c82875fe7b877da27538d3b2e3d7ea4dbaf29223",
        "fb60747d7bcd063c4fcfba7615192e0093155c2fcd4a5a0150ad80eaf1024361"),
    ("P01", FASTRTPS): (
        "724d87b6462041ac53aff86ebb987d7c54d2efaaa37a8c38b2d2b108fe5614e9",
        "943dca9495d9eb0fcdecd091e58587015922fe598b221d25706c153cf71d1458",
        "7ec4231065a82190659ea8b4eee13af04647cec5a38b2b2d509e9c9954898440",
        "a8a6d0c70cdcee675e0baec315951f9e5cbc71ec28db365b5926a2abd2df0d24"),
    ("P04", CYCLONEDDS): (
        "c3cd4ff9db5f04a4b9037ddf196ddda492fe528f7e1ff029935daaa88143345a",
        "38a52064a5233e6f5bde05add4b3c3e231e532950435a8ec43c857e35ad2f4c7",
        "fcaaab09b7f3ff5bb4b905f90f1160a8df6be3643b3503d4b30d5cb7be0a9a02",
        "24383e5dff4ad2373f6f21653eac5cf769d1e368535bb88611c4e057e2f84a4d"),
    ("P04", FASTRTPS): (
        "fba985eee0d854c2313a257a2b8c6d3309182ea0fac75e791710804c4b592b39",
        "37937ff3a6d07acea392ce51b5f241d4781f5e1b0ee5522bf972578dddae9a4b",
        "671705d54c75d6704dbb3e2169f36a476d27a532255ccdb58eb39902f25ec9e2",
        "990711a70a7adf6158891ff4fe22daace4fc66aa6cad1529d8cf417a418bf2a5"),
    ("E01", CYCLONEDDS): (
        "2c56d8fa2bdb21dacb0ea43e4ec32670bc55b462719dd161310099f8baecaf61",
        "9e851c09b2cfa61f80e8c82fdd1a2effb630594d134de16e61c96ca705de7658",
        "e567c928fac324d67ae58307d96bba731754949410fd73319b92d8d5b3dc7479",
        "0958fe93be635b36032fac7f217d9c4cdd94cfb46d7dde1694e49c5ed03d94a3"),
    ("E01", FASTRTPS): (
        "cc6ac42e6cf3d5e31bd7c7428000b8be98f35f16095e7038a9b58f9acd97ea79",
        "c8027e06564271c1f22878d2777e72480ee0d7cd63d96092571ab676c7feebf3",
        "1b59fcee5dbfde9f0044578bd5c5194b74044b63e8631eb34bafac4763c2e786",
        "7a3169018713a842481393e7d8f50bff319006a3a318beb84e7649fc05ee1335"),
}

SURFACE_TARGET = {
    "P01": "discovery.late_endpoint.fuzz_v1",
    "P02": "builtin.discovery_projection_reconciliation.fuzz_v1",
    "P04": "builtin.discovery_projection_reconciliation.fuzz_v1",
    "E01": "discovery.interruption_recovery.fuzz_v1",
    "W02": "discovery.late_endpoint.fuzz_v1",
}

PATH_FIELDS = ("replay_path", "runtime_config_path", "runtime_result_path")


def registry():
    reg = Registry()
    register_exploratory(reg, include_required=False)
    return reg


def contract(target_id):
    return registry().get_target(target_id).exploration_contract


def parent_config(surface_id):
    return json.loads(json.dumps(
        fuzz_catalog.load_seeds()["seeds"][surface_id]["logical_config"]))


def profile(target_id, surfaces, rmws=(CYCLONEDDS,), **extra):
    data = {
        "schema_version": "r2f.s1-fuzz-profile/v1",
        "profile_id": "golden-fixture",
        "campaign_kind": "exploratory",
        "enabled": False,
        "required_catalog_included": False,
        "target_ids": [target_id],
        "surfaces": list(surfaces),
        "rmws": list(rmws),
        "seed_specs": [],
        "execution_boundary": "offline planning only; no ROS",
    }
    data.update(extra)
    return data


def corpus_for(surface_id, rmw=CYCLONEDDS, *, parent=None, extra=()):
    """Exactly one authored seed per listed surface (deterministic selection)."""
    target_id = SURFACE_TARGET[surface_id]
    contract_ = contract(target_id)
    surface = contract_.surface_by_id[surface_id]
    corpus = Corpus()
    for sid in (surface_id,) + tuple(extra):
        layer = contract_.surface_by_id[sid]
        corpus.add_seed({
            "seed_id": sid, "target_id": target_id, "target_revision": 0,
            "rmw": rmw, "contract_hash": contract_.contract_hash(),
            "surface_id": sid, "boundary_class": layer.boundary_classes[0],
            "mutator_id": layer.mutator_ids[0], "executable": True,
            "final_config": json.loads(json.dumps(parent)) if parent and sid == surface_id
            else parent_config(sid),
        })
    return corpus


def generation_projection(row):
    """Remove only the three temp-dir paths and the derived normalized hash."""
    value = json.loads(json.dumps(row))
    for key in PATH_FIELDS:
        value.pop(key, None)
    value["binding"].pop("normalized_config_sha256", None)
    return value


class CycleHarness(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self._patcher = mock.patch.object(
            runner_module, "source_manifest",
            side_effect=lambda _repo=None: json.loads(json.dumps(SESSION_SOURCE)))
        self._patcher.start()
        self.addCleanup(self._patcher.stop)

    def cycle(self, name, corpus, profile_doc, *, rng_seed=7, max_inputs=1,
              scheduler=None, feedback=None, execute=False, runtime=None,
              run_id=None):
        feedback = feedback if feedback is not None else FeedbackLog()
        scheduler = scheduler or ExploratoryScheduler(
            corpus, feedback, rng_seed=rng_seed, warmup=0,
            sampling_kinds=profile_doc.get("sampling_kinds"),
            sampling_weight=profile_doc.get("sampling_weight"))
        out = self.root / name
        result = run_exploratory_cycle(
            out, corpus=corpus, feedback=feedback, scheduler=scheduler,
            profile=profile_doc, execute=execute, runtime=runtime,
            run_id=run_id or ("wide-" + name), max_inputs=max_inputs)
        return out, result


class LegacyByteIdentity(CycleHarness):
    """No sampling_kinds/chain_length ⇒ old runner bytes, unchanged."""

    def test_six_golden_rows_are_byte_identical_to_head(self):
        for (surface_id, rmw), (row_hash, content, plan, digest) in LEGACY_GOLDENS.items():
            with self.subTest(surface=surface_id, rmw=rmw):
                target_id = SURFACE_TARGET[surface_id]
                corpus = corpus_for(surface_id, rmw)
                _, result = self.cycle(
                    f"legacy-{surface_id}-{rmw}", corpus,
                    profile(target_id, [surface_id], rmws=(rmw,)), run_id="golden")
                row = result["rows"][0]
                self.assertEqual(row["execution_stage"], "planned")
                self.assertIs(row["actual_run"], False)
                self.assertEqual(canonical_hash(generation_projection(row)), row_hash)
                self.assertEqual(row["content_sha256"], content)
                self.assertEqual(row["mutation_plan_sha256"], plan)
                self.assertEqual(row["effective_input_sha256"], digest)
                # No T-E fields appear in the undeclared path.
                self.assertNotIn("sampling_kind", row)
                self.assertNotIn("chain_depth", row)
                self.assertNotIn("fuzz_generation", row)
                self.assertNotIn("chain", row["mutation_plan"])
                self.assertNotIn("fuzz_generation", json.loads(
                    pathlib.Path(row["runtime_config_path"]).read_text()))

    def test_legacy_scheduler_selection_log_stays_unchanged(self):
        corpus = corpus_for("P01", CYCLONEDDS)
        _, result = self.cycle("legacy-selection", corpus,
                               profile(SURFACE_TARGET["P01"], ["P01"]))
        selection = result["selection_log"]["selections"][0]
        self.assertNotIn("sampling_kind", selection)
        self.assertNotIn("sampling", selection)
        self.assertNotIn("sampling", result["selection_log"])


class WideSampling(CycleHarness):
    """All six declared kinds are reachable and correctly recorded."""

    KINDS = ["boundary", "representative", "log_uniform",
             "linear_uniform", "neighbor", "jump"]

    def test_every_supported_kind_can_be_selected_and_recorded(self):
        # E01 supports log_uniform; P01 supports boundary; P02 supports only
        # representative; linear_uniform/neighbor/jump appear on numeric P04.
        cases = {
            "boundary": ("P01", "discovery.late_endpoint.fuzz_v1"),
            "representative": ("P02", "builtin.discovery_projection_reconciliation.fuzz_v1"),
            "log_uniform": ("E01", "discovery.interruption_recovery.fuzz_v1"),
            "linear_uniform": ("P04", "builtin.discovery_projection_reconciliation.fuzz_v1"),
            "neighbor": ("P04", "builtin.discovery_projection_reconciliation.fuzz_v1"),
            "jump": ("P04", "builtin.discovery_projection_reconciliation.fuzz_v1"),
        }
        seen = set()
        for rng_seed in range(60):
            if len(seen) == len(self.KINDS):
                break
            for kind, (surface_id, target_id) in cases.items():
                if kind in seen:
                    continue
                corpus = corpus_for(surface_id)
                doc = profile(target_id, [surface_id], sampling_kinds=self.KINDS)
                _, result = self.cycle(f"kind-{kind}-{rng_seed}", corpus, doc,
                                       rng_seed=rng_seed)
                row = result["rows"][0]
                if row.get("sampling_kind") != kind:
                    continue
                seen.add(kind)
                self.assertEqual(row["chain_depth"], 1)
                self.assertEqual(row["fuzz_generation"]["declared_sampling_kinds"],
                                 self.KINDS)
                receipt = row["mutation_chain"][0]
                self.assertEqual(receipt["parameters"]["sampling_kind"], kind)
                config = json.loads(pathlib.Path(row["runtime_config_path"]).read_text())
                self.assertEqual(config["sampling_kind"], kind)
                self.assertEqual(config["effective_input_sha256"],
                                 row["effective_input_sha256"])
        self.assertEqual(seen, set(self.KINDS))

    def test_unsupported_kinds_are_filtered_not_errors(self):
        # P02 is a two-value enum: only ``representative`` is generatable.
        target_id = SURFACE_TARGET["P02"]
        corpus = corpus_for("P02")
        doc = profile(target_id, ["P02"], sampling_kinds=self.KINDS)
        _, result = self.cycle("filtered", corpus, doc)
        row = result["rows"][0]
        self.assertEqual(row["execution_stage"], "planned")
        self.assertEqual(row["sampling_kind"], "representative")
        self.assertEqual(row["fuzz_generation"]["supported_sampling_kinds"],
                         ["representative"])
        self.assertEqual(sorted(row["fuzz_generation"]["filtered_sampling_kinds"]),
                         sorted([k for k in self.KINDS if k != "representative"]))
        selection = result["selection_log"]["selections"][0]
        self.assertEqual(selection["sampling"]["supported"], ["representative"])

    def test_surface_without_any_declared_kind_is_rejected_not_relabelled(self):
        target_id = SURFACE_TARGET["P02"]
        corpus = corpus_for("P02")
        doc = profile(target_id, ["P02"], sampling_kinds=["jump", "neighbor"])
        _, result = self.cycle("no-kind", corpus, doc)
        row = result["rows"][0]
        self.assertEqual(row["generation_status"], "rejected")
        self.assertTrue(row["reason_code"].startswith(
            "unsupported_capability:no_declared_sampling_kind_for_surface"), row["reason_code"])
        self.assertNotIn("sampling_kind", row)
        self.assertNotIn("mutation_plan_sha256", row)

    def test_sampling_selection_is_deterministic_for_one_seed(self):
        target_id = SURFACE_TARGET["P04"]
        doc = profile(target_id, ["P04"], sampling_kinds=self.KINDS,
                      sampling_weight={"boundary": 0, "representative": 0,
                                       "linear_uniform": 1, "neighbor": 1,
                                       "jump": 2})
        first_corpus = corpus_for("P04")
        _, first = self.cycle("det-a", first_corpus, doc, rng_seed=19)
        second_corpus = corpus_for("P04")
        _, second = self.cycle("det-b", second_corpus, doc, rng_seed=19)
        self.assertEqual(first["rows"][0]["sampling_kind"],
                         second["rows"][0]["sampling_kind"])
        self.assertEqual(first["rows"][0]["effective_input_sha256"],
                         second["rows"][0]["effective_input_sha256"])
        self.assertEqual(first["selection_log"]["selections"][0]["selection_sha256"],
                         second["selection_log"]["selections"][0]["selection_sha256"])

    def test_under_covered_kind_gets_priority(self):
        # All six kinds are supported by P04 with equal weight 1.  After the
        # first selection the used kind may not repeat while a zero-use kind
        # remains, so the deficit-first rule is what drives the second pick.
        corpus = corpus_for("P04")
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=7, warmup=0,
                                         sampling_kinds=self.KINDS)
        first = scheduler.next()
        second = scheduler.next()
        self.assertNotEqual(first["sampling_kind"], second["sampling_kind"])
        counts = {}
        for event in scheduler.selection_log:
            counts[event["sampling_kind"]] = counts.get(event["sampling_kind"], 0) + 1
        self.assertLessEqual(max(counts.values()) - min(counts.values()), 1)

    def test_zero_weight_kind_is_never_chosen_while_positive_kind_is_available(self):
        # P04 supports boundary and jump; boundary weight 0 must lose every
        # time even though both kinds start with zero recorded use.
        corpus = corpus_for("P04")
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(
            corpus, feedback, rng_seed=3, warmup=0,
            sampling_kinds=["boundary", "jump"],
            sampling_weight={"boundary": 0, "jump": 5})
        chosen = {scheduler.next()["sampling_kind"] for _ in range(12)}
        self.assertEqual(chosen, {"jump"})
        selections = scheduler.to_dict()["selections"]
        self.assertFalse(any(e["sampling_kind"] == "boundary" for e in selections))

    def test_all_supported_kinds_zero_weight_is_rejected(self):
        # P02 supports only representative; excluding it must reject rather
        # than substitute a different class.
        target_id = SURFACE_TARGET["P02"]
        corpus = corpus_for("P02")
        doc = profile(target_id, ["P02"], sampling_kinds=self.KINDS,
                      sampling_weight={kind: (0 if kind == "representative" else 1)
                                       for kind in self.KINDS})
        _, result = self.cycle("excluded", corpus, doc)
        row = result["rows"][0]
        self.assertEqual(row["generation_status"], "rejected")
        self.assertTrue(row["reason_code"].startswith(
            "unsupported_capability:all_supported_sampling_kinds_zero_weight"),
            row["reason_code"])

    def test_sampling_selection_log_replays_offline(self):
        from r2f.campaign.scheduler import replay_selection
        corpus = corpus_for("P04")
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(
            corpus, feedback, rng_seed=13, warmup=0,
            sampling_kinds=["representative", "jump"],
            sampling_weight={"representative": 1, "jump": 2})
        selections = scheduler.plan(4)
        replayed = replay_selection(scheduler.to_dict(), corpus, feedback.events)
        self.assertTrue(replayed["valid"])
        self.assertEqual([e["sampling_kind"] for e in replayed["selections"]],
                         [e["sampling_kind"] for e in selections])
        self.assertEqual([e["selection_sha256"] for e in replayed["selections"]],
                         [e["selection_sha256"] for e in selections])


class ChainGeneration(CycleHarness):
    def test_two_and_four_layer_chains_keep_one_identity(self):
        target_id = SURFACE_TARGET["P02"]
        for depth in (2, 4):
            with self.subTest(chain_length=depth):
                corpus = corpus_for("P02")
                doc = profile(target_id, ["P02", "P03", "P04", "P05", "P06"],
                              chain_length=depth)
                _, result = self.cycle(f"chain-{depth}", corpus, doc)
                row = result["rows"][0]
                self.assertEqual(row["execution_stage"], "planned")
                self.assertEqual(row["chain_depth"], depth)
                self.assertEqual(len(row["mutation_chain"]), depth)
                plan = row["mutation_plan"]
                self.assertEqual(plan["chain"]["chain_length"], depth)
                self.assertEqual(plan["chain"]["proposal_order"],
                                 [op["op_id"] for op in plan["operations"]])
                self.assertEqual(plan["chain"]["surface_order"],
                                 [op["surface_id"] for op in plan["operations"]])
                self.assertEqual(len(plan["operations"]), depth)
                capsule = json.loads(pathlib.Path(row["replay_path"]).read_text())
                config = json.loads(pathlib.Path(row["runtime_config_path"]).read_text())
                self.assertEqual(capsule["input"]["effective_input_sha256"],
                                 row["effective_input_sha256"])
                self.assertEqual(config["effective_input_sha256"],
                                 row["effective_input_sha256"])
                self.assertEqual(capsule["input"]["effective_input"]["final_input"],
                                 plan["final_config"])
                self.assertEqual(config["mutation_plan"], plan)
                self.assertEqual(config["final_input"], plan["final_config"])
                self.assertEqual(len(capsule["input"]["effective_input"]["operations"]), depth)

    def test_chain_identity_is_stable_across_runs(self):
        target_id = SURFACE_TARGET["P02"]
        doc = profile(target_id, ["P02", "P03", "P04", "P05", "P06"], chain_length=4)
        _, first = self.cycle("stable-a", corpus_for("P02"), doc)
        _, second = self.cycle("stable-b", corpus_for("P02"), doc)
        self.assertEqual(first["rows"][0]["effective_input_sha256"],
                         second["rows"][0]["effective_input_sha256"])
        self.assertEqual(first["rows"][0]["content_sha256"],
                         second["rows"][0]["content_sha256"])
        self.assertEqual(first["rows"][0]["mutation_plan"],
                         second["rows"][0]["mutation_plan"])

    def test_two_layer_chain_on_endpoint_surfaces(self):
        # P01 + W02 compose; the first layer keeps the sampled class and the
        # second uses the same wide namespace without a foreign boundary label.
        target_id = SURFACE_TARGET["P01"]
        corpus = corpus_for("P01")
        doc = profile(target_id, ["P01", "W02"], sampling_kinds=["representative"],
                      chain_length=2)
        _, result = self.cycle("endpoint-chain", corpus, doc)
        row = result["rows"][0]
        self.assertEqual(row["execution_stage"], "planned", row.get("reason_code"))
        order = row["mutation_plan"]["chain"]["surface_order"]
        self.assertEqual(order[0], row["surface_id"])
        self.assertEqual(set(order), {"P01", "W02"})
        self.assertEqual(row["sampling_kind"], "representative")
        self.assertEqual(len(row["mutation_chain"]), 2)

    def test_uncomposable_chain_is_rejected(self):
        # E01 is the only surface in the recovery contract; depth 2 cannot compose.
        target_id = SURFACE_TARGET["E01"]
        doc = profile(target_id, ["E01"], chain_length=2)
        _, result = self.cycle("bad-chain", corpus_for("E01"), doc)
        row = result["rows"][0]
        self.assertEqual(row["generation_status"], "rejected")
        self.assertTrue(row["reason_code"].startswith(
            "unsupported_capability:chain_length_not_composable"), row["reason_code"])


class ProfileValidation(CycleHarness):
    def assert_profile_rejected(self, extra, pattern):
        # The scheduler validates weight declarations at construction; the
        # runner validates chain length.  Both reject explicitly.
        corpus = corpus_for("P01")
        target_id = SURFACE_TARGET["P01"]
        with self.assertRaisesRegex(ValueError, pattern):
            self.cycle("bad-profile", corpus, profile(target_id, ["P01"], **extra))

    def test_all_zero_weights_are_rejected(self):
        self.assert_profile_rejected(
            {"sampling_kinds": ["boundary", "jump"],
             "sampling_weight": {"boundary": 0, "jump": 0}},
            "must not be all zero")

    def test_negative_and_non_integer_weights_are_rejected(self):
        self.assert_profile_rejected(
            {"sampling_kinds": ["boundary"], "sampling_weight": {"boundary": -1}},
            "nonnegative integers")
        for bad in (1.5, True, "2"):
            with self.subTest(weight=bad):
                corpus = corpus_for("P01")
                with self.assertRaisesRegex(ValueError, "nonnegative integers"):
                    self.cycle("bad-weight", corpus, profile(
                        SURFACE_TARGET["P01"], ["P01"],
                        sampling_kinds=["boundary"],
                        sampling_weight={"boundary": bad}))

    def test_undeclared_weight_kind_and_bad_chain_length_are_rejected(self):
        self.assert_profile_rejected(
            {"sampling_kinds": ["boundary"], "sampling_weight": {"jump": 1}},
            "undeclared kind")
        for depth in (0, 5, "2", True):
            with self.subTest(chain_length=depth):
                corpus = corpus_for("P01")
                with self.assertRaisesRegex(ValueError, "chain_length"):
                    self.cycle("bad-depth", corpus, profile(
                        SURFACE_TARGET["P01"], ["P01"], chain_length=depth))

    def test_fractional_or_unknown_kind_is_rejected(self):
        self.assert_profile_rejected({"sampling_kinds": ["uniform"]},
                                     "unsupported sampling kind")
        self.assert_profile_rejected({"sampling_kinds": ["boundary", "boundary"]},
                                     "duplicates")

    def test_scheduler_config_must_match_profile(self):
        corpus = corpus_for("P01")
        doc = profile(SURFACE_TARGET["P01"], ["P01"], sampling_kinds=["boundary"])
        feedback = FeedbackLog()
        scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=7, warmup=0,
                                         sampling_kinds=["jump"])
        with self.assertRaisesRegex(runner_module.ExploratoryCycleError,
                                    "differs from the profile"):
            self.cycle("mismatch", corpus, doc, scheduler=scheduler, feedback=feedback)


class CorpusBucketKey(unittest.TestCase):
    def test_bucket_key_separates_sampling_kind_and_chain_depth(self):
        base = {"target_id": "t", "boundary_class": "b", "observed_edge": "e"}
        one = dict(base, sampling_kind="boundary", chain_depth=1)
        two = dict(base, sampling_kind="jump", chain_depth=1)
        deep = dict(base, sampling_kind="boundary", chain_depth=2)
        keys = {bucket_key(one), bucket_key(two), bucket_key(deep)}
        self.assertEqual(len(keys), 3)
        self.assertEqual(bucket_key(dict(base)), bucket_key(
            dict(base, sampling_kind=None, chain_depth=None)))

    def test_different_kinds_are_not_folded_into_one_bucket(self):
        corpus = Corpus(bucket_limit=1)
        for index, kind in enumerate(("boundary", "jump", "neighbor")):
            corpus.offer(
                {"target_id": "t", "target_revision": 0, "rmw": "r",
                 "contract_hash": "c", "surface_id": "P01",
                 "boundary_class": "same", "relation_class": "r" + kind,
                 "sampling_kind": kind, "chain_depth": 1,
                 "final_config": {"reader_delay_sec": index + 1},
                 "executable": True},
                executed=True, evidence_complete=True, oracle_verdict="unknown")
        retained = {r["sampling_kind"] for r in corpus.entries.values()}
        self.assertEqual(retained, {"boundary", "jump", "neighbor"})
        self.assertFalse(any(h["reason"] == "bucket_representative_limit"
                             for h in corpus.to_dict()["history"]))

    def test_bucket_limit_still_applies_inside_one_class(self):
        corpus = Corpus(bucket_limit=1)
        for index in range(3):
            corpus.offer(
                {"target_id": "t", "target_revision": 0, "rmw": "r",
                 "contract_hash": "c", "surface_id": "P01",
                 "boundary_class": "same", "relation_class": "r" + str(index),
                 "sampling_kind": "boundary",
                 "chain_depth": 1, "final_config": {"reader_delay_sec": index + 1},
                 "executable": True},
                executed=True, evidence_complete=True, oracle_verdict="unknown")
        self.assertEqual(len(corpus.entries), 1)
        self.assertTrue(any(h["reason"] == "bucket_representative_limit"
                            for h in corpus.to_dict()["history"]))


class CoverageTable(unittest.TestCase):
    def row(self, **fields):
        base = {"target_id": "t1", "surface_id": "P01", "sampling_kind": "boundary",
                "chain_depth": 1, "execution_stage": "planned", "actual_run": False,
                "generation_status": "generated",
                "mutation_plan": {"operations": []},
                "mutation_receipt": {"hit": False}}
        base.update(fields)
        return base

    def test_four_counters_are_independent_and_grouped(self):
        rows = [
            self.row(),
            self.row(execution_stage="judged", actual_run=True,
                     mutation_receipt={"hit": True}),
            self.row(execution_stage="judged", actual_run=True),
            self.row(sampling_kind="jump", chain_depth=2),
            self.row(execution_stage="judged", actual_run=True,
                     sampling_kind="jump", chain_depth=2),
            self.row(generation_status="rejected", mutation_plan=None),
        ]
        table = exploratory_coverage_table(rows)
        index = {(r["surface_id"], r["sampling_kind"], r["chain_depth"]): r for r in table}
        boundary = index[("P01", "boundary", 1)]
        self.assertEqual((boundary["generated"], boundary["executed"],
                          boundary["judged"], boundary["hit"]), (3, 2, 2, 1))
        deeper = index[("P01", "jump", 2)]
        self.assertEqual((deeper["generated"], deeper["executed"],
                          deeper["judged"], deeper["hit"]), (2, 1, 1, 0))

    def test_rejected_rows_are_never_generated(self):
        table = exploratory_coverage_table([self.row(
            generation_status="rejected", mutation_plan=None)])
        self.assertEqual(table, [])


class ReportCoverageWiring(CycleHarness):
    def test_cycle_rows_feed_the_report_coverage_table(self):
        doc = profile(SURFACE_TARGET["P04"], ["P04"], sampling_kinds=["jump"],
                      sampling_weight={"jump": 1})
        _, result = self.cycle("report-coverage", corpus_for("P04"), doc)
        row = result["rows"][0]
        table = exploratory_coverage_table(result["rows"])
        self.assertEqual(len(table), 1)
        entry = table[0]
        self.assertEqual((entry["target_id"], entry["surface_id"],
                          entry["sampling_kind"], entry["chain_depth"]),
                         (row["target_id"], row["surface_id"],
                          row["sampling_kind"], row["chain_depth"]))
        self.assertEqual(entry["generated"], 1)
        self.assertEqual(entry["executed"], 0)
        self.assertEqual(entry["judged"], 0)
        self.assertEqual(entry["hit"], 0)


if __name__ == "__main__":
    unittest.main()
