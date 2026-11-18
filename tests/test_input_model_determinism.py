"""Offline declaration identities; no ROS, DDS or execution evidence."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from dataclasses import replace

from r2f.modules.s1 import input_model as model


def declaration(**changes):
    fields = {
        "operators": ("queue.depth", "param.enum"),
        "rationale": "队列边界 followed by a QoS policy change",
        "status": "pending",
        "depends_on": ("queue_ready", "policy_ready"),
        "conflicts_with": ("lease_refresh", "endpoint_destroy"),
        "reference_document": "authored_seed",
        "pending_reason": "input declaration only; runtime binding pending",
    }
    fields.update(changes)
    return fields


class CombinationIdentity(unittest.TestCase):
    def test_identical_declarations_have_identical_ids(self):
        combinations = [model.CombinationSpec(**declaration()) for _ in range(8)]
        self.assertEqual(len({combo.combination_id for combo in combinations}), 1)
        self.assertTrue(all(combo == combinations[0] for combo in combinations))

    def test_default_id_is_canonical_content_sha256(self):
        fields = declaration()
        encoded = json.dumps(fields, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode("utf-8")
        expected = "queue.depth+param.enum@" + hashlib.sha256(encoded).hexdigest()
        self.assertEqual(model.CombinationSpec(**fields).combination_id, expected)

    def test_keyword_order_and_list_tuple_forms_do_not_change_identity(self):
        fields = declaration()
        reordered = dict(reversed(list(fields.items())))
        for key in ("operators", "depends_on", "conflicts_with"):
            reordered[key] = list(reordered[key])
        self.assertEqual(model.CombinationSpec(**fields).to_dict(),
                         model.CombinationSpec(**reordered).to_dict())

    def test_every_non_id_declaration_field_separates_identity(self):
        original = model.CombinationSpec(**declaration())
        alternatives = {
            "operators": ("param.enum", "queue.depth"),
            "rationale": "a different declaration",
            "status": "declared",
            "depends_on": ("another_dependency",),
            "conflicts_with": ("another_conflict",),
            "reference_document": "another_seed",
            "pending_reason": "another pending reason",
        }
        ids = {original.combination_id}
        for name, value in alternatives.items():
            with self.subTest(field=name):
                changed = model.CombinationSpec(**declaration(**{name: value}))
                self.assertNotIn(changed.combination_id, ids)
                ids.add(changed.combination_id)

    def test_ordered_operator_dependency_and_conflict_fields_are_not_sorted(self):
        original = model.CombinationSpec(**declaration())
        for name in ("operators", "depends_on", "conflicts_with"):
            with self.subTest(field=name):
                fields = declaration()
                fields[name] = tuple(reversed(fields[name]))
                self.assertNotEqual(model.CombinationSpec(**fields).combination_id,
                                    original.combination_id)

    def test_json_round_trip_keeps_auto_identity_and_existing_field_schema(self):
        original = model.CombinationSpec(**declaration())
        payload = original.to_dict()
        self.assertEqual(set(payload), set(model.COMBINATION_FIELDS))
        for name in ("operators", "depends_on", "conflicts_with"):
            self.assertIsInstance(payload[name], list)
        rebuilt = model.CombinationSpec.from_dict(json.loads(json.dumps(payload)))
        self.assertEqual(rebuilt, original)
        self.assertEqual(rebuilt.to_dict(), payload)
        payload["combination_id"] = ""
        self.assertEqual(model.CombinationSpec.from_dict(payload), original)

    def test_explicit_and_legacy_address_shaped_ids_are_preserved_verbatim(self):
        for identity in ("owner.recipe.v1", "queue.depth+param.enum@126341203838448"):
            with self.subTest(identity=identity):
                original = model.CombinationSpec(**declaration(combination_id=identity))
                payload = json.loads(json.dumps(original.to_dict()))
                self.assertEqual(original.combination_id, identity)
                self.assertEqual(model.CombinationSpec.from_dict(payload), original)
                # An explicit id is not replaced even if declaration content changes.
                payload["rationale"] = "an explicitly named recipe"
                self.assertEqual(model.CombinationSpec.from_dict(payload).combination_id,
                                 identity)

    def test_combination_serialization_keeps_strict_field_validation(self):
        payload = model.CombinationSpec(**declaration()).to_dict()
        with self.assertRaisesRegex(ValueError, "unknown field"):
            model.CombinationSpec.from_dict(dict(payload, process_id=123))
        for name in model.COMBINATION_FIELDS:
            with self.subTest(missing=name):
                incomplete = dict(payload)
                del incomplete[name]
                with self.assertRaisesRegex(ValueError, "missing field"):
                    model.CombinationSpec.from_dict(incomplete)

    def test_operator_duplicate_and_pending_reason_validation_are_preserved(self):
        with self.assertRaisesRegex(ValueError, "operators must be distinct"):
            model.CombinationSpec(**declaration(operators=("param.enum", "param.enum")))
        with self.assertRaisesRegex(ValueError, "must state a reason"):
            model.CombinationSpec(**declaration(pending_reason=""))


class CatalogIdentity(unittest.TestCase):
    def test_repeated_default_catalog_construction_is_identical(self):
        catalogs = [model.default_domain() for _ in range(5)]
        original = catalogs[0]
        for catalog in catalogs[1:]:
            self.assertEqual(catalog.to_dict(), original.to_dict())
            self.assertEqual(catalog.digest(), original.digest())
            self.assertEqual(catalog.manifest(), original.manifest())

    def test_default_catalog_json_round_trip_is_identical(self):
        original = model.default_domain()
        payload = json.loads(json.dumps(original.to_dict()))
        rebuilt = model.DomainCatalog.from_dict(payload)
        self.assertEqual(rebuilt.to_dict(), payload)
        self.assertEqual(rebuilt.digest(), original.digest())

    def test_catalog_with_legacy_ids_preserves_payload_and_digest(self):
        payload = model.default_domain().to_dict()
        for surface in payload["surfaces"]:
            for index, combo in enumerate(surface["combinations"]):
                combo["combination_id"] = "+".join(combo["operators"]) + f"@1263412038384{index}"
        expected_digest = model.content_hash({
            "schema": payload["schema"], "version": payload["version"],
            "surfaces": sorted(payload["surfaces"], key=lambda item: item["surface_id"]),
        })
        rebuilt = model.DomainCatalog.from_dict(json.loads(json.dumps(payload)))
        self.assertEqual(rebuilt.to_dict(), payload)
        self.assertEqual(rebuilt.digest(), expected_digest)
        self.assertEqual(model.DomainCatalog.from_dict(rebuilt.to_dict()).digest(),
                         expected_digest)

    def test_surface_order_does_not_change_catalog_digest(self):
        original = model.default_domain()
        reordered = model.DomainCatalog(tuple(reversed(original.surfaces)))
        self.assertEqual(reordered.digest(), original.digest())

    def test_different_combination_declaration_changes_catalog_digest(self):
        original = model.topology_surface()
        fields = original.combinations[0].to_dict()
        fields["combination_id"] = ""
        fields["rationale"] += " with another input assumption"
        changed = replace(original, combinations=(model.CombinationSpec.from_dict(fields),)
                          + original.combinations[1:])
        self.assertNotEqual(model.DomainCatalog((original,)).digest(),
                            model.DomainCatalog((changed,)).digest())

    def test_duplicate_auto_combinations_are_rejected(self):
        first = model.CombinationSpec(**declaration())
        second = model.CombinationSpec(**declaration())
        with self.assertRaisesRegex(ValueError, "duplicate combination_id"):
            replace(model.topology_surface(), combinations=(first, second))

    def test_duplicate_explicit_id_cannot_hide_different_declarations(self):
        first = model.CombinationSpec(**declaration(combination_id="same-id"))
        second = model.CombinationSpec(**declaration(combination_id="same-id",
                                                   rationale="different recipe"))
        with self.assertRaisesRegex(ValueError, "duplicate combination_id"):
            replace(model.topology_surface(), combinations=(first, second))

    def test_distinct_explicit_ids_cannot_hide_duplicate_declarations(self):
        first = model.CombinationSpec(**declaration(combination_id="id-one"))
        second = model.CombinationSpec(**declaration(combination_id="id-two"))
        with self.assertRaisesRegex(ValueError, "duplicate combination declaration"):
            replace(model.topology_surface(), combinations=(first, second))

    def test_auto_and_explicit_ids_cannot_hide_duplicate_declarations(self):
        first = model.CombinationSpec(**declaration())
        second = model.CombinationSpec(**declaration(combination_id="explicit-id"))
        with self.assertRaisesRegex(ValueError, "duplicate combination declaration"):
            replace(model.topology_surface(), combinations=(first, second))

    def test_duplicate_short_and_serialized_combinations_are_rejected(self):
        for short_form in (False, True):
            with self.subTest(short_form=short_form):
                payload = model.topology_surface().to_dict()
                combo = declaration()
                if not short_form:
                    combo = model.CombinationSpec(**combo).to_dict()
                payload["combinations"] = [combo, dict(combo)]
                with self.assertRaisesRegex(ValueError, "duplicate combination_id"):
                    model.SurfaceDomain.from_dict(payload)

    def test_same_operators_with_different_declarations_are_not_duplicates(self):
        first = model.CombinationSpec(**declaration())
        second = model.CombinationSpec(**declaration(rationale="different input assumption"))
        surface = replace(model.topology_surface(), combinations=(first, second))
        self.assertEqual(len(surface.combinations), 2)
        self.assertNotEqual(first.combination_id, second.combination_id)


class CrossProcessIdentity(unittest.TestCase):
    def test_declaration_and_catalog_reproduce_across_pythonhashseeds(self):
        root = Path(__file__).resolve().parents[1]
        script = """
import json
import sys
from r2f.modules.s1.input_model import CombinationSpec, DomainCatalog, default_domain
fields = json.loads(sys.argv[1])
combos = [CombinationSpec(**fields) for _ in range(2)]
catalogs = [default_domain() for _ in range(2)]
print(json.dumps({
    'combination_ids': [combo.combination_id for combo in combos],
    'combination': combos[0].to_dict(),
    'catalog_digests': [catalog.digest() for catalog in catalogs],
    'catalog_payloads_equal': catalogs[0].to_dict() == catalogs[1].to_dict(),
    'round_trip_digest': DomainCatalog.from_dict(catalogs[0].to_dict()).digest(),
}, sort_keys=True))
"""
        original = model.default_domain()
        expected = {
            "combination_ids": [model.CombinationSpec(**declaration()).combination_id] * 2,
            "combination": model.CombinationSpec(**declaration()).to_dict(),
            "catalog_digests": [original.digest()] * 2,
            "catalog_payloads_equal": True,
            "round_trip_digest": original.digest(),
        }
        for seed in ("0", "1", "42", "random"):
            with self.subTest(PYTHONHASHSEED=seed):
                env = dict(os.environ, PYTHONHASHSEED=seed,
                           PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(root / "src"))
                result = subprocess.run(
                    [sys.executable, "-c", script, json.dumps(declaration())],
                    cwd=root, env=env, capture_output=True, text=True,
                    check=True, timeout=30,
                )
                self.assertEqual(json.loads(result.stdout), expected)


if __name__ == "__main__":
    unittest.main()
