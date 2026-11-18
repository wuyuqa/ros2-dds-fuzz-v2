"""Offline T-B contracts: wide sampling surface and bounded mutation chains.

Nothing here starts ROS, a namespace, nft, a worker or a campaign.  A proposal
is an input document; a chain is a plan.  Neither is an execution receipt, a
hit, or a candidate.  The golden values below were captured from HEAD
``e2268bf`` with the registered ``mutate()`` path and are pinned byte-for-byte
so the wide namespace cannot drift the already-sealed F1 output.
"""
import copy
import unittest

from r2f.core.evidence import canonical_hash
from r2f.core.mutation_plan import (
    MUTATION_CHAIN_SCHEMA, CompositionRejection, MutationPlan, MutationProposal,
    compose, compose_chain, validate_plan,
)
from r2f.core.mutators import (
    F2_MUTATORS, F2_MUTATOR_SPECS, MUTATORS, MUTATOR_SPECS, WIDE_SAMPLING_KINDS,
    make_rng, mutate, mutate_wide, mutator_registry_signature,
)
from r2f.modules.s1 import fuzz_catalog as catalog

#: Registry signature captured on HEAD e2268bf.  ``mutate_wide`` must not move
#: a single byte of it, and M01-M03 specs must stay the F1-registered set.
HEAD_MUTATOR_REGISTRY_SIGNATURE = (
    "3ad538230ba02309b3accbf26958c47acd9e461120bb0328b31bb72f050a22ac")

CONTRACT_IDS = ("P01", "P02", "P03", "P04", "P05", "P06", "E01", "W02")

PARENTS = {
    "P01": {"reader_delay_sec": 0.0},
    "P02": {"qos": {"reliability": "reliable"}},
    "P03": {"qos": {"durability": "volatile"}},
    "P04": {"qos": {"deadline_ns": 0}},
    "P05": {"qos": {"liveliness": "automatic"}},
    "P06": {"qos": {"lease_duration_ns": 100000000}},
    "E01": {"drop_duration_sec": 0.2},
    "W02": {"publish_interval_sec": 0.0},
}

#: Golden samples: (surface, mutator, context, seed, parent, expected change,
#: expected plan hash, expected proposal hash) reproduced from HEAD.
GOLDENS = (
    ("P01", "M01", {"boundary_class": "finite", "seed_index": 0}, 4, {"reader_delay_sec": 0.0},
     {"reader_delay_sec": 0.01},
     "408d73b5734170904a6d568147eb3af8e9b9e6d29692ed2eea5834ac5f759159",
     "f13cd358e52810cbc4b0df0829914dafa5745e2888ae08711f653d4b2bd5e2ff"),
    ("W02", "M03", {"boundary_class": "log_range", "seed_index": 0}, 7, {"publish_interval_sec": 0.0},
     {"publish_interval_sec": 0.0},
     "5709405ce90f5cdc285f6a1244a3c62f8bba0ba28046957501430d115b9bd6ec",
     "f2f0f355184f3f002a80182dc880e3a332079a8e54319d194808d0f883b238ab"),
    ("E01", "M03", {"boundary_class": "log_range", "seed_index": 0}, 7, {"drop_duration_sec": 0.2},
     {"drop_duration_sec": 0.203},
     "f27cea37db9c3733ac556d0b02579d1277f399d1f6dca59e011efe3ed54f769b",
     "5affd10f5d309b521bae171dead76ffcee229342bae147d59aa7125d8145904f"),
)


def contract():
    return catalog.exploration_contract(CONTRACT_IDS)


def surface(contract_, sid):
    return contract_.surface_by_id[sid]


class GoldenRegression(unittest.TestCase):
    """The legacy ``mutate()`` path must be byte-identical to HEAD."""

    def setUp(self):
        self.contract = contract()

    def test_registry_signature_is_byte_identical_to_head(self):
        self.assertEqual(mutator_registry_signature(), HEAD_MUTATOR_REGISTRY_SIGNATURE)
        self.assertEqual(set(MUTATOR_SPECS), {"M01", "M02", "M03"})
        self.assertEqual(set(MUTATORS), {"M01", "M02", "M03"})
        self.assertEqual(set(F2_MUTATOR_SPECS), {"M04", "M05", "M06", "M07", "M08"})
        self.assertNotIn("M04", MUTATORS)
        with self.assertRaisesRegex(ValueError, "unknown or not implemented mutator"):
            mutate(PARENTS["P01"], surface(self.contract, "P01"), make_rng(0), {}, "M04")

    def test_golden_samples_keep_changes_and_hashes(self):
        for sid, mutator_id, context, seed, parent, changes, plan_hash, proposal_hash \
                in GOLDENS:
            with self.subTest(surface=sid, mutator=mutator_id):
                proposal = mutate(parent, surface(self.contract, sid),
                                  make_rng(seed), context, mutator_id)
                self.assertEqual(proposal.changes, changes)
                self.assertEqual(canonical_hash(proposal.to_dict()), proposal_hash)
                plan = compose(parent, [proposal], self.contract, rng_seed=seed)
                if sid == "W02":
                    # The old golden remains the generator-state reference:
                    # proposal bytes/changes are unchanged, but admission now
                    # labels this equal-input compile-time operation no_op.
                    # Build the expected document independently, not by editing
                    # the production plan's serialization or reusing its hash.
                    expected = {
                        "plan_version": "r2f.mutation-plan/v1",
                        "rng_algorithm": "mt19937-v1", "rng_seed": 7,
                        "parent_content_sha256":
                            "ab7f63de6935856db1bc365d1a652e5b90b76811e1b4b444b41073847bd77fd4",
                        "operations": [{
                            "schema": "r2f.mutation-operation/v1",
                            "op_id": "op-00-W02-M03", "surface_id": "W02", "mutator_id": "M03",
                            "parameters": {"sampling": "local_bounded", "step": 0.001},
                            "input_before_sha256":
                                "ab7f63de6935856db1bc365d1a652e5b90b76811e1b4b444b41073847bd77fd4",
                            "input_after_sha256":
                                "ab7f63de6935856db1bc365d1a652e5b90b76811e1b4b444b41073847bd77fd4",
                            "application_phase": "compile_time",
                            "applicability_result": {
                                "status": "no_op",
                                "sut_guards": {"actual_send_interval_observed": "unknown"},
                                "reason": "unwindowed_compile_time_input_unchanged",
                            },
                        }],
                        "final_config": {"publish_interval_sec": 0.0}, "runtime_windows": [],
                        "contract_hash":
                            "e6b1572f55fceca51a3f0d2ec434a3f92f2930dcbb47892762346665a2344f9a",
                    }

                    def literal_plan_hash(document):
                        # Reproduce the documented canonical envelope directly;
                        # do not call MutationPlan.content_hash/plan_hash here.
                        content = {key: value for key, value in document.items() if key != "rng_seed"}
                        return canonical_hash({"plan": document, "content_hash": canonical_hash(content)})

                    self.assertEqual(plan.to_dict(), expected)
                    expected_hash = literal_plan_hash(expected)
                    self.assertEqual(plan.plan_hash(), expected_hash)
                    self.assertNotEqual(expected_hash, plan_hash)
                    self.assertEqual(validate_plan(plan, contract=self.contract).status, "invalid")
                    legacy = copy.deepcopy(expected)
                    legacy["operations"][0]["applicability_result"] = {
                        "status": "applicable",
                        "sut_guards": {"actual_send_interval_observed": "unknown"},
                    }
                    self.assertEqual(literal_plan_hash(legacy), plan_hash)
                else:
                    self.assertEqual(plan.plan_hash(), plan_hash)
                # The corrected status does not add a chain key or change schema.
                self.assertNotIn("chain", plan.to_dict())

    def test_legacy_single_proposal_plan_dict_has_no_new_fields(self):
        proposal = mutate(PARENTS["E01"], surface(self.contract, "E01"),
                          make_rng(7), {"boundary_class": "log_range"}, "M03")
        plan = compose(PARENTS["E01"], [proposal], self.contract, rng_seed=7)
        self.assertEqual(set(plan.to_dict()), {
            "plan_version", "rng_algorithm", "rng_seed", "parent_content_sha256",
            "operations", "final_config", "runtime_windows", "contract_hash"})
        self.assertEqual(MutationPlan.from_dict(plan.to_dict()).to_dict(), plan.to_dict())

    def test_mutate_wide_does_not_touch_mutate_dispatch(self):
        sid = "P04"
        legacy = mutate(PARENTS[sid], surface(self.contract, sid), make_rng(11), {}, "M01")
        mutate_wide(PARENTS[sid], surface(self.contract, sid), make_rng(11), {}, "M01")
        again = mutate(PARENTS[sid], surface(self.contract, sid), make_rng(11), {}, "M01")
        self.assertEqual(canonical_hash(legacy.to_dict()), canonical_hash(again.to_dict()))


class WideSamplingKinds(unittest.TestCase):
    def setUp(self):
        self.contract = contract()

    def _wide(self, sid, kind, seed=3, parent=None, context=None, mutator_id=None):
        context = dict(context or {})
        if kind is not None:
            context["sampling_kind"] = kind
        return mutate_wide(PARENTS[sid] if parent is None else parent,
                           surface(self.contract, sid), make_rng(seed), context, mutator_id)

    def test_every_declared_kind_is_reachable_and_records_sampling_kind(self):
        reachable = {}
        for sid in CONTRACT_IDS:
            for kind in WIDE_SAMPLING_KINDS:
                try:
                    proposal = self._wide(sid, kind)
                except ValueError:
                    continue
                self.assertEqual(proposal.parameters["sampling_kind"], kind)
                self.assertEqual(proposal.notes["sampling_kind"], kind)
                reachable.setdefault(kind, set()).add(sid)
        self.assertEqual(set(reachable), set(WIDE_SAMPLING_KINDS), reachable)
        # Each declared class is reachable on at least one real catalog surface.
        for kind, sids in reachable.items():
            self.assertTrue(sids, kind)

    def test_numeric_values_stay_inside_the_declared_domain(self):
        numeric = ("P01", "P04", "P06", "E01", "W02")
        sampled = {"log_uniform", "linear_uniform", "neighbor", "jump"}
        for sid in numeric:
            surface_ = surface(self.contract, sid)
            minimum, maximum = surface_.domain["min"], surface_.domain["max"]
            declared_special = {item["value"] for item in surface_.special_values}
            for kind in WIDE_SAMPLING_KINDS:
                for seed in range(24):
                    try:
                        proposal = self._wide(sid, kind, seed=seed)
                    except ValueError:
                        break
                    for value in proposal.changes.values():
                        if kind in sampled:
                            self.assertGreaterEqual(value, minimum, (sid, kind, seed))
                            self.assertLessEqual(value, maximum, (sid, kind, seed))
                        else:
                            # Boundary/representative may select a declared
                            # special class (e.g. P04 zero=0 below min=1); the
                            # declaration is the authority for that class.
                            self.assertTrue(
                                minimum <= value <= maximum
                                or value in declared_special,
                                (sid, kind, seed, value))

    def test_undeclared_domain_is_rejected_with_surface_id(self):
        # E01 declares log_range only: no M01-honourable boundary class exists,
        # and it declares no representative set.
        with self.assertRaisesRegex(ValueError, "surface E01: .*not declared"):
            self._wide("E01", "boundary")
        with self.assertRaisesRegex(ValueError, "surface E01: .*not declared"):
            self._wide("E01", "representative")
        # P01 starts at zero, so no logarithmic decade exists in its domain.
        with self.assertRaisesRegex(ValueError, "surface P01: .*not declared"):
            self._wide("P01", "log_uniform")
        # A surface without declared min/max cannot be sampled numerically.
        with self.assertRaisesRegex(ValueError, "surface P02"):
            self._wide("P02", "linear_uniform")
        # An enum surface has no numeric domain at all.
        with self.assertRaisesRegex(ValueError, "surface P03"):
            self._wide("P03", "neighbor")
        # Out-of-range explicit fractions are refused, never clamped silently.
        with self.assertRaisesRegex(ValueError, "surface E01"):
            self._wide("E01", "linear_uniform", context={"fraction": 1.5})

    def test_log_uniform_spans_decades_inside_domain(self):
        values = {self._wide("E01", "log_uniform", seed=seed).changes["drop_duration_sec"]
                  for seed in range(48)}
        self.assertGreater(len(values), 16)
        # A log-uniform sampler must cover more than one order of magnitude.
        self.assertTrue(any(v < 0.1 for v in values), sorted(values))
        self.assertTrue(any(v >= 1.0 for v in values), sorted(values))
        for value in values:
            self.assertTrue(0.05 <= value <= 8.0, value)

    def test_neighbor_is_centered_on_parent_with_declared_multiples(self):
        parent = {"qos": {"deadline_ns": 1000}}
        surface_ = surface(self.contract, "P04")
        for multiplier in (1, 2, 4):
            proposal = mutate_wide(parent, surface_, make_rng(0),
                                   {"sampling_kind": "neighbor",
                                    "neighbor_multiplier": multiplier,
                                    "neighbor_direction": 1})
            self.assertEqual(proposal.changes["qos.deadline_ns"], 1000 + multiplier)
            proposal = mutate_wide(parent, surface_, make_rng(0),
                                   {"sampling_kind": "neighbor",
                                    "neighbor_multiplier": multiplier,
                                    "neighbor_direction": -1})
            self.assertEqual(proposal.changes["qos.deadline_ns"], 1000 - multiplier)
        with self.assertRaisesRegex(ValueError, "neighbor_multiplier"):
            mutate_wide(parent, surface_, make_rng(0),
                        {"sampling_kind": "neighbor", "neighbor_multiplier": 3})

    def test_jump_lands_in_a_different_declared_decade(self):
        parent = {"qos": {"deadline_ns": 100000000}}
        for seed in range(16):
            proposal = mutate_wide(parent, surface(self.contract, "P04"), make_rng(seed),
                                   {"sampling_kind": "jump"})
            self.assertNotEqual(proposal.parameters["decade"],
                                proposal.parameters["parent_decade"])
            self.assertGreaterEqual(proposal.changes["qos.deadline_ns"], 1)
            self.assertLessEqual(proposal.changes["qos.deadline_ns"], 9000000000)

    def test_same_rng_seed_reproduces_wide_sample(self):
        first = self._wide("E01", "log_uniform", seed=5)
        second = self._wide("E01", "log_uniform", seed=5)
        self.assertEqual(first.to_dict(), second.to_dict())

    def test_mutator_namespace_does_not_launder_sampling_kind(self):
        # M01 is boundary-only; M03 must never emit a boundary sample.
        with self.assertRaisesRegex(ValueError, "M01 only produces boundary"):
            self._wide("E01", "log_uniform", mutator_id="M01")
        with self.assertRaisesRegex(ValueError, "M03 does not produce boundary"):
            self._wide("E01", "boundary", mutator_id="M03")
        for seed in range(8):
            proposal = self._wide("P01", None, seed=seed, mutator_id="M03")
            self.assertNotEqual(proposal.parameters["sampling_kind"], "boundary")
            self.assertEqual(proposal.mutator_id, "M03")
        for seed in range(4):
            proposal = self._wide("P01", None, seed=seed, mutator_id="M01")
            self.assertEqual(proposal.parameters["sampling_kind"], "boundary")
            self.assertEqual(proposal.mutator_id, "M01")

    def test_wide_proposals_are_marked_as_wide_sampling(self):
        proposal = self._wide("E01", "log_uniform")
        self.assertTrue(proposal.parameters["wide_sampling"])
        self.assertTrue(proposal.notes["wide_sampling"])
        # The registered path is never marked as wide.
        legacy = mutate(PARENTS["E01"], surface(self.contract, "E01"), make_rng(7),
                        {"boundary_class": "log_range"}, "M03")
        self.assertNotIn("wide_sampling", legacy.parameters)

    def test_enum_surfaces_use_representative_not_numeric_guess(self):
        proposal = self._wide("P03", "representative", seed=1)
        self.assertEqual(proposal.parameters["sampling_kind"], "representative")
        self.assertEqual(proposal.changes, {"qos.durability": "transient_local"})

    def test_m04_and_m05_are_exposed_without_joining_the_registry(self):
        w01 = catalog.f2_generation_surface("W01")
        for relation in ("lt", "eq", "gt"):
            proposal = mutate_wide({"qos": {"depth": 8}, "post_match_count": 6}, w01,
                                   make_rng(1), {"relation_class": relation}, "M04")
            self.assertEqual(proposal.parameters["sampling_kind"], "boundary")
            self.assertEqual(proposal.parameters["relation"], relation)
        for special in ("zero", "finite", "infinite"):
            proposal = mutate_wide({"qos": {"deadline_ns": 5}},
                                   catalog.f2_generation_surface("P04"), make_rng(0),
                                   {"special_class": special}, "M05")
            self.assertEqual(proposal.parameters["sampling_kind"], "representative")
            self.assertEqual(proposal.parameters["special_class"], special)
        self.assertNotIn("M04", MUTATORS)
        self.assertNotIn("M04", MUTATOR_SPECS)
        self.assertIn("M04", F2_MUTATORS)

    def test_wide_output_still_carries_no_execution_fields(self):
        proposal = self._wide("E01", "linear_uniform")
        for forbidden in ("executed", "hit", "raw_path", "oracle_verdict", "candidate"):
            self.assertNotIn(forbidden, proposal.to_dict())
        self.assertEqual(proposal.applicability_result["status"], "applicable")


class ChainComposition(unittest.TestCase):
    def setUp(self):
        self.contract = contract()

    def _layers(self):
        def wide(sid, kind, seed):
            return mutate_wide(PARENTS[sid], surface(self.contract, sid), make_rng(seed),
                               {"sampling_kind": kind})
        return [wide("P03", "representative", 1),
                wide("P04", "log_uniform", 2),
                wide("P05", "representative", 3),
                wide("P06", "linear_uniform", 4)]

    def test_four_layer_chain_is_generated_with_stable_order(self):
        parent = copy.deepcopy(PARENTS["P04"])
        layers = self._layers()
        plan = compose_chain(layers, self.contract, parent=parent, rng_seed=5)
        self.assertEqual(len(plan.operations), 4)
        self.assertEqual([op.surface_id for op in plan.operations],
                         ["P03", "P04", "P05", "P06"])
        self.assertEqual(plan.chain["schema"], MUTATION_CHAIN_SCHEMA)
        self.assertEqual(plan.chain["chain_length"], 4)
        self.assertEqual(plan.chain["surface_order"], ["P03", "P04", "P05", "P06"])
        self.assertEqual(len(plan.chain["proposal_order"]), 4)
        self.assertIn("order_after", plan.chain["constraints"])
        self.assertIn("conflicts_with", plan.chain["constraints"])
        # Every layer is applied on the previous layer's output.
        previous = plan.parent_content_sha256
        for operation in plan.operations:
            self.assertEqual(operation.input_before_sha256, previous)
            previous = operation.input_after_sha256
        self.assertIn("durability", plan.final_config["qos"])
        self.assertIn("deadline_ns", plan.final_config["qos"])
        self.assertIn("liveliness", plan.final_config["qos"])

    def test_chain_is_reproducible_and_roundtrips(self):
        parent = copy.deepcopy(PARENTS["P04"])
        first = compose_chain(self._layers(), self.contract, parent=parent, rng_seed=5)
        second = compose_chain(self._layers(), self.contract, parent=parent, rng_seed=5)
        self.assertEqual(first.plan_hash(), second.plan_hash())
        self.assertEqual(first.to_dict(), second.to_dict())
        reloaded = MutationPlan.from_dict(first.to_dict())
        self.assertEqual(reloaded.plan_hash(), first.plan_hash())
        self.assertEqual(reloaded.chain, first.chain)
        self.assertEqual(validate_plan(first, {"mutators": ["M01", "M02", "M03"],
                                               "multi_window": False},
                                       self.contract).status, "valid")

    def test_chain_key_is_absent_unless_chain_length_is_requested(self):
        proposal = mutate_wide(PARENTS["E01"], surface(self.contract, "E01"), make_rng(7),
                               {"sampling_kind": "linear_uniform"})
        legacy = compose(PARENTS["E01"], [proposal], self.contract, rng_seed=7)
        chained = compose(PARENTS["E01"], [proposal], self.contract, rng_seed=7,
                          chain_length=1)
        self.assertNotIn("chain", legacy.to_dict())
        self.assertIn("chain", chained.to_dict())
        self.assertEqual(chained.chain["chain_length"], 1)
        # Same content, different declared composition mode.
        self.assertNotEqual(legacy.plan_hash(), chained.plan_hash())

    def test_conflicting_same_path_writes_are_rejected_not_overwritten(self):
        parent = copy.deepcopy(PARENTS["P04"])
        first = mutate_wide(parent, surface(self.contract, "P04"), make_rng(1),
                            {"sampling_kind": "linear_uniform"})
        second = mutate_wide(parent, surface(self.contract, "P04"), make_rng(2),
                             {"sampling_kind": "linear_uniform"})
        self.assertNotEqual(first.changes, second.changes)
        with self.assertRaises(CompositionRejection) as caught:
            compose_chain([first, second], self.contract, parent=parent)
        self.assertIn("conflict", str(caught.exception))
        self.assertEqual(caught.exception.details["reason"], "conflict")
        self.assertEqual(caught.exception.details["field"], "qos.deadline_ns")
        self.assertEqual(caught.exception.details["layers"], [0, 1])

    def test_same_value_written_twice_is_marked_overridden(self):
        parent = copy.deepcopy(PARENTS["E01"])
        proposal = mutate_wide(parent, surface(self.contract, "E01"), make_rng(7),
                               {"sampling_kind": "linear_uniform"})
        twin = MutationProposal(
            surface_id=proposal.surface_id, mutator_id=proposal.mutator_id,
            parameters=proposal.parameters, changes=proposal.changes,
            input_before=proposal.input_before, input_after=proposal.input_after,
            application_phase=proposal.application_phase,
            applicability_result=proposal.applicability_result)
        plan = compose_chain([proposal, twin], self.contract, parent=parent)
        self.assertEqual(len(plan.operations), 2)
        # Both operations remain in the audit description: the earlier one
        # is overridden, while reapplying the same value makes the last one a
        # no_op.  Neither may be counted as a new independent live mutation.
        self.assertEqual(plan.operations[0].applicability_status, "overridden")
        self.assertEqual(plan.operations[1].applicability_status, "no_op")
        result = validate_plan(plan, contract=self.contract)
        self.assertEqual(result.status, "invalid")
        self.assertFalse(result.executable)
        self.assertIn("active no_op", "; ".join(result.reasons))

    def test_order_after_violation_is_rejected(self):
        # X01 declares order_after P04, so [X01, P04] is the wrong order.
        from test_fuzz_model_contracts import fixture_contract
        fixture = fixture_contract()
        base = {"reader_delay_sec": 0.0, "qos": {"deadline_ns": 5,
                                                 "reliability": "reliable"}}

        def proposal(sid, changes):
            return MutationProposal(
                surface_id=sid, mutator_id="M01", parameters={}, changes=changes,
                input_before=base, input_after=base,
                application_phase="compile_time",
                applicability_result={"status": "applicable"})

        # X01 must follow P04.  Order is checked before any field overlap, so
        # the violation is reported as an order rejection rather than a conflict.
        with self.assertRaises(CompositionRejection) as caught:
            compose_chain([proposal("X01", {"qos.deadline_ns": 7}),
                           proposal("P04", {"qos.deadline_ns": 9})], fixture, parent=base)
        self.assertIn("order dependency violated", str(caught.exception))
        # The dependency itself composes; the pair still conflicts on the field.
        with self.assertRaises(CompositionRejection) as caught:
            compose_chain([proposal("P04", {"qos.deadline_ns": 9}),
                           proposal("X01", {"qos.deadline_ns": 7})], fixture, parent=base)
        self.assertEqual(caught.exception.details["reason"], "conflict")

    def test_declared_conflicts_with_pair_is_rejected(self):
        from test_fuzz_model_contracts import fixture_contract
        fixture = fixture_contract()
        first = mutate({"reader_delay_sec": 0.0}, fixture.surface_by_id["P01"],
                       make_rng(1), {}, "M03")
        second = mutate({"qos": {"reliability": "reliable"}}, fixture.surface_by_id["P02"],
                        make_rng(1), {}, "M02")
        with self.assertRaises(CompositionRejection) as caught:
            compose_chain([first, second], fixture,
                          parent={"reader_delay_sec": 0.0,
                                  "qos": {"reliability": "reliable"}})
        self.assertIn("surface conflict", str(caught.exception))

    def test_chain_length_is_bounded_to_one_through_four(self):
        parent = copy.deepcopy(PARENTS["E01"])
        one = mutate_wide(parent, surface(self.contract, "E01"), make_rng(1),
                          {"sampling_kind": "linear_uniform"})
        with self.assertRaises(CompositionRejection):
            compose_chain([], self.contract, parent=parent)
        with self.assertRaises(CompositionRejection):
            compose_chain([one] * 5, self.contract, parent=parent)
        # A single layer is legal; chain_length must still match the count.
        self.assertEqual(len(compose_chain([one], self.contract, parent=parent).operations), 1)
        with self.assertRaises(CompositionRejection):
            compose(parent, [one], self.contract, chain_length=2)

    def test_imported_plan_layer_keeps_declared_windows(self):
        from r2f.core.mutation_plan import RUNTIME_WINDOW_SCHEMA
        parent = copy.deepcopy(PARENTS["E01"])
        proposal = mutate_wide(parent, surface(self.contract, "E01"), make_rng(7),
                               {"sampling_kind": "linear_uniform"})
        window = {"schema": RUNTIME_WINDOW_SCHEMA, "window_id": "w1", "window_index": 0,
                  "trigger_anchor": "fresh_match", "trigger_offset_sec": 0.0,
                  "drop_duration_sec": proposal.changes["drop_duration_sec"],
                  "restore_interval_sec": 0.02}
        single = compose(parent, [proposal], self.contract, rng_seed=7,
                         runtime_windows=[window])
        chained = compose_chain([single], self.contract, parent=parent)
        self.assertEqual(chained.runtime_windows, [window])

    def test_imported_plan_layer_keeps_net_diff_and_requires_parent(self):
        parent = copy.deepcopy(PARENTS["E01"])
        proposal = mutate_wide(parent, surface(self.contract, "E01"), make_rng(7),
                               {"sampling_kind": "linear_uniform"})
        single = compose(parent, [proposal], self.contract, rng_seed=7)
        chained = compose_chain([single], self.contract, parent=parent)
        self.assertEqual(chained.final_config, single.final_config)
        with self.assertRaises(CompositionRejection) as caught:
            compose_chain([single], self.contract)
        self.assertEqual(caught.exception.details["reason"], "parent_required")

    def test_multi_operation_plan_layer_is_refused(self):
        parent = copy.deepcopy(PARENTS["P04"])
        first = mutate_wide(parent, surface(self.contract, "P04"), make_rng(1),
                            {"sampling_kind": "linear_uniform"})
        second = mutate_wide(parent, surface(self.contract, "P06"), make_rng(2),
                             {"sampling_kind": "linear_uniform"})
        two = compose(parent, [first, second], self.contract, rng_seed=3)
        with self.assertRaises(CompositionRejection) as caught:
            compose_chain([two], self.contract, parent=parent)
        self.assertEqual(caught.exception.details["reason"], "plan_layer_not_decomposable")


if __name__ == "__main__":
    unittest.main()
