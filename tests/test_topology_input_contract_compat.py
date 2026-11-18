"""Pinned pre-I2 hashes and exhaustive old catalog subset hash compatibility."""
import itertools
from dataclasses import replace

import pytest

from r2f.core.evidence import canonical_hash
from r2f.core.mutation_plan import ExplorationContract
from r2f.core import mutators
from r2f.modules.s1 import fuzz_catalog, targets
from r2f.modules.s1.topology_input import topology_contract

LEGACY = {'registry': '3ad538230ba02309b3accbf26958c47acd9e461120bb0328b31bb72f050a22ac',
 'specs': {'M01': {'schema': 'r2f.mutator-spec/v1',
                   'mutator_id': 'M01',
                   'version': 1,
                   'accepted_surface_kinds': ['parameter_numeric'],
                   'name': 'boundary_pick',
                   'description': 'pick a declared boundary neighbour'},
           'M02': {'schema': 'r2f.mutator-spec/v1',
                   'mutator_id': 'M02',
                   'version': 1,
                   'accepted_surface_kinds': ['parameter_enum'],
                   'name': 'enum_replace',
                   'description': 'pick a different declared enum value'},
           'M03': {'schema': 'r2f.mutator-spec/v1',
                   'mutator_id': 'M03',
                   'version': 1,
                   'accepted_surface_kinds': ['parameter_numeric', 'parameter_timing'],
                   'name': 'range_sample',
                   'description': 'sample a bounded, quantized numeric range'}},
 'required': {'discovery.late_endpoint': 'c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940',
              'discovery.late_join_delivery': '1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d',
              'discovery.interruption_recovery': 'fdfd62d15683ec5ef7c08f864a2ea6d4d6f08ec16247966d2b9590b38ebe89ce',
              'builtin.discovery_projection_reconciliation': 'eced91c5611dfda5f30759837409fbdd822f85e5dbbbf0b4ba67d046de8bf6e0'},
 'exploratory': {'discovery.late_endpoint.fuzz_v1': {'target': 'e173db6ca90927065f2840e72c40aa0d30aed43592f8c62d2d39858c5a2d6525',
                                                     'contract': 'ecc45d394a55b5c77a2a93b0ce3617b98fcde94dcfa3043d434a19de415a5044'},
                 'discovery.late_join_delivery.fuzz_v1': {'target': '098fb797d0480d17a672e2ecce8edf41bb95ef9f4d5ff0f7f8a959ea6402dd1d',
                                                          'contract': 'ecc45d394a55b5c77a2a93b0ce3617b98fcde94dcfa3043d434a19de415a5044'},
                 'discovery.interruption_recovery.fuzz_v1': {'target': 'd0d008bb84f33012a6a562a7ab7acc2066076b1b42b40d896287dd767e7fb494',
                                                             'contract': '1ab37734a463b3af6955889e976304fa243d1cc5dad5a2a1a32e54dca359a257'},
                 'builtin.discovery_projection_reconciliation.fuzz_v1': {'target': '0288795a666bb50d30cdf07a789ec60f7a9a3fde9f1643f15ea0c3607221a183',
                                                                         'contract': '1b0186c3e30e4dcce8c7215a2e7c41270c60e0230096b34fdb58d7a280fd4557'},
                 'discovery.match_compatibility.fuzz_v1': {'target': '1a884ef74dbfc8e15ca4ddb60ac121ba6dd4b81c43f7581b988426d942e68b2e',
                                                           'contract': '6a07a5882b09da7eef068565efcd820df61f19264ee39551b030bb9423ab6ea6'},
                 'discovery.interruption_recovery.fuzz_windows_v1': {'target': 'fda04262bcd89fe974e4a8febff6f98c30c5fd5f2cfd25ab0e51a5a34e8ae661',
                                                                     'contract': '4963654505779aba389bf46cd0d4c0f61dfea4697a011e1b6beea9fb13605303'}},
 'contracts': {'P01': '4ddfb8a01b78722b7e29e69d7ca54a7dc8e8b1b5d175cd207e735f19950b457f',
               'P02': '000cb3f39d63ec136459c7051cd9e6fa83e80d971fbac9d18031246bc1265459',
               'P03': '203cb7b44285fe56e1f75f143fa68f851ac7666ea8a074e51a5c5f847ad57ba5',
               'P04': 'f4fce5f22b92fa5c42959f798473a496c614ea96d5e883e076fa2b1138cd75ec',
               'P05': '203e26cd615576af3b2299062354cbfc026b2edc10115ac91e07d085710ab39e',
               'P06': '3e9bd041cbac6447f1d04094a25422c6980cf562c8c1bb58fd1674bd2d824420',
               'E01': '1ab37734a463b3af6955889e976304fa243d1cc5dad5a2a1a32e54dca359a257',
               'W02': '7c0229214b991370089f58d3d14b7c5764b9898772ec49d96ad7497550bfa46c',
               'empty': '6eba70cbd55049c50ca89369bee64edbfd1f213d4f1b8fb0cfdf2df155c11782',
               'f2_live': '4963654505779aba389bf46cd0d4c0f61dfea4697a011e1b6beea9fb13605303'}}


def test_default_specs_and_signature_are_byte_for_byte_unchanged():
    assert {k: v.to_dict() for k, v in mutators.MUTATOR_SPECS.items()} == LEGACY["specs"]
    assert mutators.mutator_registry_signature() == LEGACY["registry"]
    assert mutators.TOPOLOGY_MUTATOR_ID not in mutators.MUTATOR_SPECS
    assert mutators.TOPOLOGY_MUTATOR_ID not in mutators.ALL_MUTATOR_SPECS
    assert mutators.TOPOLOGY_MUTATOR_ID not in mutators.F2_MUTATOR_SPECS


@pytest.mark.parametrize("target_id,digest", list(LEGACY["required"].items()))
def test_all_four_required_target_hashes_unchanged(target_id, digest):
    assert canonical_hash(targets.target_declarations()[target_id].to_dict()) == digest


@pytest.mark.parametrize("target_id,expected", list(LEGACY["exploratory"].items()))
def test_every_old_exploratory_target_and_contract_unchanged(target_id, expected):
    target = targets.exploratory_target_declarations(runtime_capability=targets.F2_WINDOWS_CAPABILITY)[target_id]
    assert canonical_hash(target.to_dict()) == expected["target"]
    assert target.exploration_contract.contract_hash() == expected["contract"]


@pytest.mark.parametrize("name,digest", list(LEGACY["contracts"].items()))
def test_every_old_generation_contract_unchanged(name, digest):
    if name == "empty": contract = fuzz_catalog.exploration_contract()
    elif name == "f2_live": contract = fuzz_catalog.f2_live_contract()
    else: contract = fuzz_catalog.exploration_contract([name])
    assert contract.contract_hash() == digest


OLD_SURFACES = tuple(name for name in LEGACY["contracts"] if name not in {"empty", "f2_live"})
OLD_SUBSETS = [subset for n in range(len(OLD_SURFACES) + 1)
               for subset in itertools.combinations(OLD_SURFACES, n)]


@pytest.mark.parametrize("subset", OLD_SUBSETS)
def test_all_256_old_surface_subset_contract_hashes_use_identical_old_formula(subset):
    contract = fuzz_catalog.exploration_contract(subset)
    old_digest = canonical_hash({"hash_schema": "r2f.exploration-contract-hash/v1",
                                 "contract": contract.to_dict(),
                                 "mutator_registry_sha256": LEGACY["registry"]})
    assert contract.contract_hash() == old_digest


def test_new_spec_extension_digest_is_scoped_and_versioned(monkeypatch):
    contract = topology_contract()
    legacy = fuzz_catalog.exploration_contract(["P01"])
    before = contract.contract_hash()
    spec_id = mutators.TOPOLOGY_MUTATOR_ID
    original = mutators.TOPOLOGY_MUTATOR_SPECS[spec_id]
    monkeypatch.setitem(mutators.ACTION_PROGRAM_MUTATOR_SPECS, spec_id, replace(original, version=2))
    assert contract.contract_hash() != before
    assert legacy.contract_hash() == LEGACY["contracts"]["P01"]
    assert mutators.mutator_registry_signature() == LEGACY["registry"]
    assert mutators.mutator_extension_digest([spec_id]) != mutators.mutator_extension_digest([])


def test_opt_in_contract_roundtrip_and_explicit_registry_remains_authoritative():
    contract = topology_contract()
    assert ExplorationContract.from_dict(contract.to_dict()).contract_hash() == contract.contract_hash()
    with pytest.raises(ValueError, match="unknown mutator"):
        from r2f.core.mutation_plan import validate_contract
        validate_contract(contract, mutators={})
    data = contract.to_dict(); data["surfaces"][0]["activation"] = "F1"
    with pytest.raises(ValueError, match="unknown mutator"):
        ExplorationContract.from_dict(data)


def test_default_mutate_does_not_activate_topology():
    from r2f.core.mutators import mutate, make_rng
    with pytest.raises(ValueError):
        mutate({}, topology_contract().surfaces[0], make_rng(0),
               mutator_id=mutators.TOPOLOGY_MUTATOR_ID)
