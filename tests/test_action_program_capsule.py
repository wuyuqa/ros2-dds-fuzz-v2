"""Generic versioned action-program capsule layout; no runtime invocation."""
from copy import deepcopy
import json
from unittest.mock import patch

import pytest

from r2f.core import ReplayCapsule, canonical_hash
from test_topology_cycle import capsule_from_registry


@pytest.mark.parametrize('depth', [1, 2, 4])
def test_layout_is_derived_bound_and_roundtrips_without_pair_fields(depth):
    _, scenario, plan, capsule = capsule_from_registry(depth)
    data = capsule.to_dict()
    layout = data['input']['payload_layout']
    assert layout == {'schema': 'r2f.replay-action-program-payload/v1',
                      'surface_id': 'S1.topology.program', 'input_path': 'topology_program',
                      'program_schema': plan.final_config['topology_program']['schema'],
                      'contract_hash': plan.contract_hash,
                      'payload_sha256': canonical_hash(plan.final_config['topology_program'])}
    assert not {'topic', 'message_type', 'qos'} & set(capsule.input)
    with patch('r2f.modules.s1.topology_input.make_topology_plan', side_effect=AssertionError('NO MUTATION')):
        restored = ReplayCapsule.from_dict(json.loads(json.dumps(data)))
    assert restored.to_dict() == data
    assert canonical_hash(capsule.input['effective_input']) == canonical_hash(
        scenario.effective_input_v2(final_input=plan.final_config,
        input_layer_receipts=capsule.input['effective_input']['input_layer_receipts'],
        source_manifest=capsule.input['source_manifest']))


@pytest.mark.parametrize('field', ['schema', 'surface_id', 'input_path', 'program_schema',
                                   'contract_hash', 'payload_sha256', 'missing', 'fake_pair'])
def test_layout_tamper_rejected(field):
    _, _, _, capsule = capsule_from_registry()
    data = deepcopy(capsule.to_dict())
    if field == 'missing': data['input'].pop('payload_layout')
    elif field == 'fake_pair': data['input']['topic'] = '/fake-first-pair'
    else: data['input']['payload_layout'][field] = 'tampered'
    with pytest.raises(ValueError):
        ReplayCapsule.from_dict(data)


@pytest.mark.parametrize('field', ['final_config', 'input_before_sha256', 'input_after', 'schema', 'order'])
def test_plan_and_program_tampering_rejected(field):
    _, _, _, capsule = capsule_from_registry()
    data = deepcopy(capsule.to_dict())
    plan = data['input']['mutation_plan']
    if field == 'final_config': plan['final_config']['topology_program']['schema'] = 'wrong'
    elif field == 'order': plan['operations'].reverse()
    else: plan['operations'][0]['parameters']['input_layer_receipts'][0][field] = 'tampered'
    with pytest.raises(ValueError): ReplayCapsule.from_dict(data)
