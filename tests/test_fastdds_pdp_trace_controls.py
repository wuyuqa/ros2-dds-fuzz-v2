"""PDP controls, including invalid control artifacts, never acquire SUT credit."""
from test_topology_adapter import RawFixture


def test_off_control_declaration_stays_excluded_even_when_its_evidence_is_invalid(tmp_path):
    fixture=RawFixture(missing=(2,))
    fixture.add('native_pdp_provider_selected',capture_enabled=False)
    fixture.events.insert(-1,fixture.events.pop())
    for i,event in enumerate(fixture.events):event['event_index']=i
    facts=fixture.facts(tmp_path)
    assert facts['control_excluded'] is True
    assert facts['candidate_eligible'] is False
