"""Deterministic oracle regression: unrelated unknowns cannot erase a residual."""
import pytest

from r2f.modules.s1.topology_oracles import TopologyOracle
from test_topology_oracles import evaluate
from test_topology_adapter import RawFixture


@pytest.mark.parametrize('status', ['unknown', 'conflict'])
def test_identity_limit_retains_independently_proved_delivery_gap(tmp_path, status):
    fixture = RawFixture(missing=(2,))
    facts, original = evaluate(fixture, tmp_path)
    assert original['forbidden_residuals'] == ['topology.closed_delivery_gap']
    facts['participant_identity'] = {'status': status, 'unknown_reasons': ['missing_iid_api']}
    result = TopologyOracle().evaluate(fixture.scenario, facts)
    assert result['verdict'] == 'fail'
    assert 'topology.closed_delivery_gap' in result['forbidden_residuals']
    assert bool(result['closure']['pairs'])
    assert ('topology.identity_mismatch' in result['forbidden_residuals']) == (status == 'conflict')
    assert not result['candidate_eligible']
