"""T05/T06: exploratory runner routing for all four F1 targets.

These tests only exercise *planned* generation on the shared runner.  Nothing
here starts ROS, a namespace, nft or a campaign, and no row produced here is
execution evidence.  ``execute=True`` for projection/recovery must stop with an
explicit ``unsupported_capability`` rather than a fabricated ``judged`` row.
"""
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from r2f.campaign.corpus import Corpus
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.runner import (
    _EXPLORATORY_ROUTES, _F1_CYCLE_TARGETS, _adapt_parent, _recovery_windows,
    run_exploratory_cycle,
)
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.core.mutation_plan import validate_plan
from r2f.modules.s1 import fuzz_catalog
from r2f.modules.s1.module import register_exploratory
from r2f.core import Registry
from r2f.modules.s1.targets import F2_WINDOWS_CAPABILITY, F2_WINDOWS_TARGET
from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET

LATE = "discovery.late_endpoint.fuzz_v1"
LATE_JOIN = "discovery.late_join_delivery.fuzz_v1"
RECOVERY = "discovery.interruption_recovery.fuzz_v1"
PROJECTION = "builtin.discovery_projection_reconciliation.fuzz_v1"
COMPAT = "discovery.match_compatibility.fuzz_v1"

RMW = "rmw_cyclonedds_cpp"


def _contract(target_id):
    registry = Registry()
    register_exploratory(registry, include_required=False)
    return registry.get_target(target_id).exploration_contract


def _profile(target_id, surfaces):
    return {
        "schema_version": "r2f.s1-fuzz-profile/v1",
        "profile_id": "s1-fuzz-target-routing-fixture",
        "campaign_kind": "exploratory",
        "enabled": False,
        "required_catalog_included": False,
        "target_ids": [target_id],
        "surfaces": list(surfaces),
        "rmws": [RMW],
        "seed_specs": [],
        "execution_boundary": "offline planning only; no ROS",
    }


def _corpus(target_id, surface_id, *, boundary=None, mutator_id=None, seed_id=None,
            config_overrides=None):
    """Corpus holding exactly one authored seed template for one surface."""
    seed_id = seed_id or surface_id
    seed = fuzz_catalog.load_seeds()["seeds"][seed_id]
    contract = _contract(target_id)
    surface = contract.surface_by_id[surface_id]
    config = json.loads(json.dumps(seed["logical_config"]))
    config.update(config_overrides or {})
    corpus = Corpus()
    corpus.add_seed({
        "target_id": target_id,
        "target_revision": 0,
        "rmw": RMW,
        "contract_hash": contract.contract_hash(),
        "surface_id": surface_id,
        "boundary_class": boundary or surface.boundary_classes[0],
        "seed_id": seed_id,
        "executable": True,
        "mutator_id": mutator_id or surface.mutator_ids[0],
        "final_config": config,
    })
    return corpus


class _Cycle:
    """Small helper: run one bounded planned cycle and keep the artefacts."""

    def __init__(self, test, target_id, surface_id, **corpus_kwargs):
        self.temporary = tempfile.TemporaryDirectory()
        test.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.corpus = _corpus(target_id, surface_id, **corpus_kwargs)
        self.feedback = FeedbackLog()
        self.scheduler = ExploratoryScheduler(self.corpus, self.feedback, rng_seed=7, warmup=0)
        self.profile = _profile(target_id, [surface_id])

    def run(self, *, execute=False, runtime=None, max_inputs=1, name="cycle"):
        return run_exploratory_cycle(
            self.root / name, corpus=self.corpus, feedback=self.feedback,
            scheduler=self.scheduler, profile=self.profile, execute=execute,
            runtime=runtime, max_inputs=max_inputs, run_id="route-" + name)

    def config(self, row):
        return json.loads(pathlib.Path(row["runtime_config_path"]).read_text())


class RouteTableContracts(unittest.TestCase):
    def test_routes_cover_four_legacy_targets_plus_exact_g01(self):
        self.assertEqual(_F1_CYCLE_TARGETS, {LATE, LATE_JOIN, RECOVERY, PROJECTION, COMPAT})
        self.assertEqual(_EXPLORATORY_ROUTES[COMPAT]["backend"], "relation_pair")
        self.assertEqual(_EXPLORATORY_ROUTES[COMPAT]["target_revision"], 1)
        self.assertEqual(_EXPLORATORY_ROUTES[LATE]["carrier"], "ros2_endpoint")
        self.assertEqual(_EXPLORATORY_ROUTES[LATE_JOIN]["carrier"], "ros2_endpoint")
        self.assertEqual(_EXPLORATORY_ROUTES[RECOVERY]["carrier"], "wire_proxy")
        self.assertEqual(_EXPLORATORY_ROUTES[RECOVERY]["backend"], "discovery_namespace")
        self.assertEqual(_EXPLORATORY_ROUTES[PROJECTION]["backend"], "projection_pair")
        self.assertEqual(_EXPLORATORY_ROUTES[PROJECTION]["carrier"], "ros2_endpoint")

    def test_route_carrier_and_kind_match_the_declared_target_slots(self):
        capabilities = {None, F2_WINDOWS_CAPABILITY, TOPOLOGY_CAPABILITY}
        legacy = {LATE, LATE_JOIN, RECOVERY, PROJECTION, COMPAT}
        capability_targets = {None: set(), F2_WINDOWS_CAPABILITY: {F2_WINDOWS_TARGET},
                              TOPOLOGY_CAPABILITY: {TOPOLOGY_TARGET}}
        self.assertEqual({route.get("runtime_capability")
                          for route in _EXPLORATORY_ROUTES.values()}, capabilities)
        checked = set()
        for capability in capabilities:
            registry = Registry()
            register_exploratory(registry, include_required=False,
                                 runtime_capability=capability)
            # A route is dispatch knowledge, not default or cross-capability permission.
            self.assertEqual(set(registry.targets), legacy | capability_targets[capability])
            if capability is None:
                with self.assertRaises(KeyError):
                    registry.get_target(F2_WINDOWS_TARGET)
            for denied in {F2_WINDOWS_TARGET, TOPOLOGY_TARGET} - capability_targets[capability]:
                with self.subTest(capability=capability, denied=denied):
                    with self.assertRaises(KeyError):
                        registry.get_target(denied)
                    with self.assertRaises(KeyError):
                        registry.get_services(denied)
            for target_id, route in _EXPLORATORY_ROUTES.items():
                if route.get("runtime_capability") != capability:
                    continue
                with self.subTest(target=target_id, capability=capability):
                    slot = registry.get_target(target_id).mutation_slots[0]
                    self.assertEqual(slot.carrier, route["carrier"])
                    self.assertEqual(slot.mutation_kind, route["mutation_kind"])
                    checked.add(target_id)
        self.assertEqual(checked, set(_EXPLORATORY_ROUTES))

    def test_route_names_do_not_grant_topology_capability(self):
        self.assertEqual(_EXPLORATORY_ROUTES[TOPOLOGY_TARGET]["runtime_capability"],
                         "topology_v1")
        self.assertEqual(_EXPLORATORY_ROUTES[TOPOLOGY_TARGET]["backend"], "topology_pair")
        for capability in (TOPOLOGY_TARGET, "topology", "topology_v1_extra"):
            with self.subTest(capability=capability):
                with self.assertRaisesRegex(ValueError, "unsupported exploratory runtime_capability"):
                    register_exploratory(Registry(), runtime_capability=capability)

    def test_only_verified_routes_are_execution_admitted(self):
        # Live-admitted routes are evidence-based: late endpoint, projection
        # and single-window recovery all have real runtime receipts.
        self.assertTrue(_EXPLORATORY_ROUTES[LATE]["execution_admitted"])
        self.assertTrue(_EXPLORATORY_ROUTES[RECOVERY]["execution_admitted"])
        self.assertTrue(_EXPLORATORY_ROUTES[PROJECTION]["execution_admitted"])


class ProjectionPlannedGeneration(unittest.TestCase):
    def test_p02_enum_mutation_reaches_projection_pair_config(self):
        cycle = _Cycle(self, PROJECTION, "P02")
        result = cycle.run(name="p02")
        self.assertEqual(result["rejected"], [])
        row = result["rows"][0]
        self.assertEqual(row["target_id"], PROJECTION)
        self.assertEqual(row["execution_stage"], "planned")
        self.assertIs(row["actual_run"], False)
        self.assertEqual(row["surface_id"], "P02")
        self.assertEqual(row["binding"]["status"], "preserved")

        config = cycle.config(row)
        self.assertEqual(config["target_id"], PROJECTION)
        self.assertEqual(config["runtime_route"]["backend"], "projection_pair")
        self.assertEqual(config["carrier"], "ros2_endpoint")
        # projection_pair consumes exactly these keys.
        for key in ("qos", "baseline_qos", "writer_qos", "reader_qos"):
            self.assertIsInstance(config[key], dict, key)
        self.assertEqual(config["writer_qos"], config["qos"])
        self.assertEqual(config["reader_qos"], config["qos"])
        # The mutated value is really in the submitted and runtime QoS.
        self.assertIn("qos.reliability", config["submitted_parameters"])
        self.assertEqual(config["qos"]["reliability"],
                         config["submitted_parameters"]["qos.reliability"])
        parent = fuzz_catalog.load_seeds()["seeds"]["P02"]["logical_config"]
        self.assertNotEqual(config["qos"]["reliability"], parent["qos"]["reliability"])
        # A neutral baseline must stay distinguishable from the mutated profile.
        self.assertNotEqual(config["baseline_qos"], config["qos"])

    def test_p04_boundary_mutation_changes_deadline_and_drops_annotations(self):
        cycle = _Cycle(self, PROJECTION, "P04")
        result = cycle.run(name="p04")
        self.assertEqual(result["rejected"], [])
        row = result["rows"][0]
        self.assertEqual(row["surface_id"], "P04")
        self.assertEqual(row["binding"]["status"], "preserved")
        config = cycle.config(row)
        parent = fuzz_catalog.load_seeds()["seeds"]["P04"]["logical_config"]
        self.assertIn("qos.deadline_ns", config["submitted_parameters"])
        self.assertEqual(config["qos"]["deadline_ns"],
                         config["submitted_parameters"]["qos.deadline_ns"])
        self.assertNotEqual(config["qos"]["deadline_ns"], parent["qos"]["deadline_ns"])
        # Runtime QoS contains only fields projection_pair.  Catalog modelling
        # annotations, when a seed carries them, are dropped and recorded.
        self.assertNotIn("deadline_class", config["qos"])
        self.assertNotIn("deadline_class", row["final_input"]["qos"])
        self.assertEqual(config["writer_qos"], config["qos"])
        self.assertEqual(config["reader_qos"], config["qos"])
        adapted, receipt = _adapt_parent(
            PROJECTION, _EXPLORATORY_ROUTES[PROJECTION],
            {"topic": "/annotated", "qos": {"reliability": "reliable", "depth": 64,
                                            "deadline_class": "finite"},
             "baseline_qos": {"reliability": "reliable", "depth": 64,
                              "liveliness_lease_class": "finite"}})
        self.assertEqual(sorted(receipt["dropped_fields"]),
                         ["baseline_qos.liveliness_lease_class", "deadline_class"])
        self.assertNotIn("deadline_class", adapted["qos"])
        self.assertNotIn("liveliness_lease_class", adapted["baseline_qos"])
        # Runtime defaults are not invented by the adapter; normalization adds them.
        self.assertNotIn("deadline_ns", adapted["qos"])

    def test_projection_execute_is_admitted_and_calls_the_runtime(self):
        cycle = _Cycle(self, PROJECTION, "P02")
        calls = []

        def runtime(config, output_dir):
            calls.append(config)
            return {"real_execution": False, "runtime_status": "blocked",
                    "reason": "fixture", "evidence_origin": "offline_fixture"}

        result = cycle.run(execute=True, runtime=runtime, name="p02-exec")
        row = result["rows"][0]
        self.assertEqual(len(calls), 1)
        self.assertEqual(row["execution_stage"], "aborted")
        self.assertIs(row["actual_run"], False)
        self.assertNotEqual(row.get("reason_code"),
                            "unsupported_capability:execution_backend_not_admitted:projection_pair")


class RecoveryPlannedGeneration(unittest.TestCase):
    def test_e01_single_window_reaches_namespace_config(self):
        cycle = _Cycle(self, RECOVERY, "E01")
        result = cycle.run(name="e01")
        self.assertEqual(result["rejected"], [])
        row = result["rows"][0]
        self.assertEqual(row["target_id"], RECOVERY)
        self.assertEqual(row["execution_stage"], "planned")
        self.assertEqual(row["surface_id"], "E01")
        self.assertEqual(row["binding"]["status"], "preserved")
        config = cycle.config(row)
        self.assertEqual(config["carrier"], "wire_proxy")
        self.assertEqual(config["runtime_route"]["backend"], "discovery_namespace")
        for key in ("drop_duration_sec", "recovery_timeout_sec", "counter_interval_sec"):
            self.assertIn(key, row["final_input"])
            self.assertIn(key, config)
            self.assertEqual(config[key], row["final_input"][key])
        self.assertIn("drop_duration_sec", config["submitted_parameters"])
        # Exactly one derived drop->restore window, anchored on fresh match.
        self.assertEqual(len(row["mutation_plan"]["runtime_windows"]), 1)
        self.assertEqual(len(config["environment_windows"]), 1)
        window = config["environment_windows"][0]
        self.assertEqual(window["trigger_anchor"], "fresh_match")
        self.assertEqual(window["drop_duration_sec"], config["drop_duration_sec"])
        parent = fuzz_catalog.load_seeds()["seeds"]["E01"]["logical_config"]
        self.assertNotEqual(row["final_input"]["drop_duration_sec"],
                            parent["drop_duration_sec"])

    def test_recovery_execute_reaches_the_injected_runtime(self):
        cycle = _Cycle(self, RECOVERY, "E01")
        calls = []

        def runtime(config, output_dir):
            calls.append(config)
            return {"real_execution": False, "runtime_status": "blocked",
                    "reason": "fixture", "evidence_origin": "offline_fixture"}

        result = cycle.run(execute=True, runtime=runtime, name="e01-exec")
        row = result["rows"][0]
        self.assertEqual(len(calls), 1)
        self.assertNotEqual(row.get("reason_code"),
                            "unsupported_capability:execution_backend_not_admitted:discovery_namespace")

    def test_multi_window_shapes_are_still_refused(self):
        # The runner derives exactly one window; two windows cannot be
        # generated through this route.
        core, runtime = _recovery_windows(0.2)
        self.assertEqual(core["window_index"], 0)
        self.assertEqual(runtime["window_id"], "w0")
        two = [dict(core), dict(core, window_id="w1", window_index=1,
                                trigger_anchor="previous_restore_confirmed")]
        validation = validate_plan({"plan_version": "r2f.mutation-plan/v1",
                                    "rng_algorithm": "mt19937-v1", "rng_seed": 1,
                                    "parent_content_sha256": "0" * 64, "operations": [],
                                    "final_config": {"drop_duration_sec": 0.2},
                                    "runtime_windows": two, "contract_hash": "0" * 64})
        self.assertEqual(validation.status, "invalid")
        self.assertFalse(validation.executable)

    def test_e02_e03_surfaces_remain_rejected_by_the_route(self):
        # E02/E03 are capability-pending surfaces; they are not in the recovery
        # contract, so a corpus entry naming them is refused, never planned.
        for surface_id in ("E02", "E03"):
            cycle = _Cycle(self, RECOVERY, "E01")
            contract = _contract(RECOVERY)
            self.assertNotIn(surface_id, contract.surface_by_id)
            entry = dict(next(iter(cycle.corpus.entries.values())))
            entry["surface_id"] = surface_id
            cycle.corpus.entries.clear()
            cycle.corpus.add_seed(entry)
            result = cycle.run(name="pending-" + surface_id)
            generated = [row for row in result["rows"]
                         if row.get("generation_status") != "rejected"]
            self.assertEqual(generated, [])
            self.assertEqual(len(result["rejected"]), 1)
            self.assertIn("surface_not_in_contract", result["rejected"][0]["reason_code"])
            self.assertIs(result["rejected"][0]["actual_run"], False)
            self.assertIsNone(result["rejected"][0]["mutation_plan"])
        self.assertFalse(list(cycle.root.rglob("capsule.json")))
        self.assertEqual(result["budget"]["generated"], 0)
        self.assertEqual(result["budget"]["actual"], 0)


class UnknownAndLatePaths(unittest.TestCase):
    def test_unknown_target_is_rejected_without_generation(self):
        cycle = _Cycle(self, LATE, "P01")
        entry = dict(next(iter(cycle.corpus.entries.values())))
        entry["target_id"] = "discovery.not_a_target.fuzz_v1"
        # Rebuild identity for the deliberately unknown target. A stale known-
        # target digest is a separate corpus-integrity error, not a route test.
        entry.pop("content_sha256")
        cycle.corpus.entries.clear()
        cycle.corpus.add_seed(entry)
        result = cycle.run(name="unknown")
        generated = [row for row in result["rows"]
                     if row.get("generation_status") != "rejected"]
        self.assertEqual(generated, [])
        self.assertEqual(len(result["rejected"]), 1)
        self.assertEqual(result["rejected"][0]["reason_code"],
                         "target_not_in_exploratory_catalog")
        self.assertIsNone(result["rejected"][0]["mutation_plan"])
        self.assertFalse(list(cycle.root.rglob("capsule.json")))
        self.assertEqual(result["budget"]["generated"], 0)
        self.assertEqual(result["budget"]["actual"], 0)

    def test_unknown_target_with_stale_content_hash_fails_closed(self):
        corpus = _corpus(LATE, "P01")
        entry = dict(next(iter(corpus.entries.values())))
        entry["target_id"] = "discovery.not_a_target.fuzz_v1"
        with self.assertRaisesRegex(ValueError, "corpus content hash mismatch"):
            Corpus().add_seed(entry)

    def test_original_late_join_noop_never_executes_or_prepares_capsule(self):
        # Keep the original default W02/M03+seed7 sample: local sampling may
        # select the authored value, which is a rejection, not a valid route hit.
        cycle = _Cycle(self, LATE_JOIN, "W02")
        runtime = mock.Mock(side_effect=AssertionError("no-op cannot run"))
        result = cycle.run(name="late-join-noop", execute=True, runtime=runtime)
        runtime.assert_not_called()
        self.assertEqual(result["rejected"], result["rows"])
        self.assertEqual(len(result["rows"]), 1)
        row = result["rows"][0]
        self.assertEqual(row["reason_code"], "no_effective_input")
        self.assertEqual(row["binding"]["status"], "not_executed")
        self.assertFalse(row["actual_run"])
        self.assertFalse(row["candidate_eligible"])
        self.assertEqual(cycle.feedback.events, [])
        self.assertFalse(list(cycle.root.rglob("capsule.json")))
        for key in ("generated", "unique_inputs", "attempted", "actual", "unique_actual"):
            self.assertEqual(result["budget"][key], 0, key)

    def test_match_compatibility_is_exactly_routed_not_globally_promoted(self):
        entry_contract = _contract(COMPAT)
        self.assertEqual(set(entry_contract.surface_by_id), {"G01"})
        self.assertIn(COMPAT, _F1_CYCLE_TARGETS)
        self.assertFalse(fuzz_catalog.f2_generation_surface("G01").executable)
        with self.assertRaises(fuzz_catalog.UnsupportedCapabilityError):
            fuzz_catalog.surface_to_core(fuzz_catalog.SURFACES["G01"])
        from r2f.core.mutators import MUTATORS
        self.assertNotIn("M08", MUTATORS)
        for target_id in (LATE, LATE_JOIN, RECOVERY, PROJECTION):
            self.assertNotIn("G01", _contract(target_id).surface_by_id)

    def test_late_endpoint_path_is_unchanged(self):
        cycle = _Cycle(self, LATE, "P01")
        result = cycle.run(name="late")
        self.assertEqual(result["rejected"], [])
        row = result["rows"][0]
        self.assertEqual(row["target_id"], LATE)
        self.assertEqual(row["binding"]["status"], "preserved")
        config = cycle.config(row)
        self.assertEqual(config["runtime_route"]["backend"], "endpoint_pair")
        self.assertEqual(config["carrier"], "ros2_endpoint")
        self.assertIn("reader_delay_sec", config["submitted_parameters"])
        self.assertNotIn("environment_windows", config)

    def test_late_join_path_is_unchanged(self):
        cycle = _Cycle(self, LATE_JOIN, "W02", boundary="finite", mutator_id="M03",
                       config_overrides={"publish_interval_sec": 0.0305})
        result = cycle.run(name="late-join")
        self.assertEqual(result["rejected"], [])
        row = result["rows"][0]
        self.assertEqual(row["target_id"], LATE_JOIN)
        self.assertEqual(row["binding"]["status"], "preserved")
        config = cycle.config(row)
        self.assertEqual(config["runtime_route"]["backend"], "endpoint_pair")
        self.assertIn("publish_interval_sec", config["submitted_parameters"])
        self.assertNotEqual(config["submitted_parameters"]["publish_interval_sec"], 0.0305)


if __name__ == "__main__":
    unittest.main()
