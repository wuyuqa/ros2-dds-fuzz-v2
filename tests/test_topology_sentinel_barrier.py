"""Disqualified offline fixtures: sentinel receipts are not filter effects."""
from copy import deepcopy
import pytest
from r2f.modules.s1 import topology_model as model
from r2f.runtime.topology_pair import _sentinel_delivery
from test_topology_delivery_windows import WindowFixture, _action, _run


def program():
    p = model.generate_program(7, writers=1, readers=1)
    p['actions'] = [
        _action('cw', 'create', 'w0', 1), _action('cr', 'create', 'r0', 1),
        _action('middle', 'publish_batch', 'w0', 1, count=1, start_sequence=0),
        _action('sentinel', 'publish_batch', 'w0', 1, count=1, start_sequence=1),
        _action('wait', 'barrier', condition='delivery_sentinel', timeout_sec=0.01,
                endpoint_ids=['w0', 'r0'], publish_action_ids=['sentinel'])]
    return p


@pytest.mark.parametrize('omit,expected', [((0,), True), ((0, 1), False)])
def test_exact_sentinel_not_all_delivery(tmp_path, omit, expected):
    runtime, events, _, _ = _run(tmp_path, program(), WindowFixture(omit=omit))
    row = next(e for e in events if e['event'] == 'topology_barrier')
    assert row['satisfied'] is expected
    assert not row['candidate_eligible']
    if expected:
        witness = row['after']['sentinel_witnesses'][0]
        assert events[witness['send_ref']]['phase'] == 'sentinel'
        assert events[witness['receive_ref']]['event'] == 'receive'
    assert not runtime['real_execution']


@pytest.mark.parametrize('change', ['future', 'external', 'batch', 'repeat', 'scope', 'stale', 'empty'])
def test_invalid_references(change):
    p = program()
    pub, barrier = p['actions'][3:]
    if change == 'future':
        p['actions'][3:] = [barrier, pub]
    elif change == 'external':
        barrier['params']['publish_action_ids'] = ['external']
    elif change == 'batch':
        pub['params']['count'] = 2
    elif change == 'repeat':
        pub['params']['repeat_data_from'] = 'middle'
    elif change == 'scope':
        barrier['params']['endpoint_ids'] = ['r0']
    elif change == 'empty':
        barrier['params']['publish_action_ids'] = []
    else:
        p['actions'].insert(4, _action('new', 'recreate', 'w0', 2))
    assert model.validate_program(p)['status'] == 'invalid'


@pytest.mark.parametrize('fault', ['duplicate', 'writer_generation', 'reader_generation', 'gid', 'no_receive'])
def test_raw_identity_gaps(tmp_path, fault):
    _, events, _, _ = _run(tmp_path, program(), WindowFixture(omit=(0,)))
    barrier = next(e for e in events if e['event'] == 'topology_barrier')
    witness = barrier['after']['sentinel_witnesses'][0]
    raw = deepcopy(events[:barrier['event_index']])
    send, receive = raw[witness['send_ref']], raw[witness['receive_ref']]
    pair = dict(writer='w0', reader='r0', writer_generation=1, reader_generation=1,
                writer_gid=send['writer_gid'], reader_gid=receive['reader_gid'])
    if fault == 'duplicate':
        raw.append(deepcopy(send))
    elif fault == 'writer_generation':
        receive['payload_writer_generation'] = 2
    elif fault == 'reader_generation':
        receive['generation'] = 2
    elif fault == 'gid':
        receive['publisher_gid'] = 'wrong'
    else:
        raw = [e for e in raw if e['event'] != 'receive']
    handles = {'w0': dict(logical_id='w0', role='writer', generation=1),
               'r0': dict(logical_id='r0', role='reader', generation=1)}
    ok, detail = _sentinel_delivery([pair], handles, raw, [program()['actions'][3]])
    assert not ok and detail['sentinel_gaps']


@pytest.mark.parametrize('field', ['action_id', 'endpoint_id'])
@pytest.mark.parametrize('value', [[], {}])
@pytest.mark.parametrize('index', [0, 2, 3])
def test_malformed_action_keys_are_named_invalid(field, value, index):
    p = program()
    p['actions'][index][field] = deepcopy(value)
    result = model.validate_program(p)
    assert result['status'] == 'invalid'
    assert any(e['path'] == f'program.actions[{index}].{field}'
               and e['code'] in ('value', 'reference') for e in result['errors'])
    # Earlier shape errors must not suppress subsequent sentinel validation.
    p['actions'][-1]['params']['publish_action_ids'] = ['absent']
    result = model.validate_program(p)
    assert any(e['code'] == 'sentinel_publish_reference' for e in result['errors'])


def test_missing_sentinel_with_other_valid_receive_has_reason(tmp_path):
    runtime, events, _, _ = _run(tmp_path, program(), WindowFixture(omit=(1,)))
    assert any(e['event'] == 'receive' and e['send_sequence'] == 0 for e in events)
    row = next(e for e in events if e['event'] == 'topology_barrier')
    assert not row['satisfied'] and not row['candidate_eligible']
    assert not runtime['real_execution']
    gaps = row['after']['sentinel_gaps']
    assert gaps and all(isinstance(g['reason'], str) and g['reason'] for g in gaps)
    assert gaps[0]['reason'] == 'sentinel_receive_for_other_send'


@pytest.mark.parametrize('fault,expected', [('captured_receive_first', True),
                                            ('receive_before_publish', False),
                                            ('foreign_clock', False)])
def test_sentinel_chronology_uses_publish_begin_not_stdout_merge_order(tmp_path, fault, expected):
    """Synthetic clock-bound callbacks may precede publish-return/stdout drain."""
    _, events, _, _ = _run(tmp_path, program(), WindowFixture(omit=(0,)))
    barrier = next(e for e in events if e['event'] == 'topology_barrier')
    refs = barrier['after']['sentinel_witnesses'][0]
    raw = deepcopy(events[:barrier['event_index']])
    send, receive = raw[refs['send_ref']], raw[refs['receive_ref']]
    pair = dict(writer='w0', reader='r0', writer_generation=1, reader_generation=1,
                writer_gid=send['writer_gid'], reader_gid=receive['reader_gid'])
    stamp = receive['monotonic_ns']
    send.update(publish_begin_ns=stamp-100, publish_return_ns=stamp+100, monotonic_ns=stamp+200)
    # Delay only the captured send row; preserve its actual API start time.
    raw.remove(send)
    raw.insert(raw.index(receive)+1, send)
    for index, row in enumerate(raw):
        row['event_index'] = index
    if fault == 'receive_before_publish':
        receive['monotonic_ns'] = send['publish_begin_ns']-1
    elif fault == 'foreign_clock':
        receive['monotonic_clock_identity'] = {'boot_id': 'foreign'}
    handles = {'w0': dict(logical_id='w0', role='writer', generation=1),
               'r0': dict(logical_id='r0', role='reader', generation=1)}
    ok, _ = _sentinel_delivery([pair], handles, raw, [program()['actions'][3]])
    assert ok is expected
