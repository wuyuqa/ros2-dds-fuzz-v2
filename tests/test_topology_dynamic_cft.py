"""Offline dynamic CFT wiring; fixtures are not DDS evidence."""
import json
from pathlib import Path
from types import SimpleNamespace

from r2f.modules.s1 import topology_model as model
from r2f.runtime.filter_backend import ContentFilterAdapter
from r2f.runtime.topology_pair import run_topology
from r2f.runtime.topology_worker import OfflineTopologyBackend


def action(aid, op, eid, **params):
    return {'action_id': aid, 'op': op, 'endpoint_id': eid, 'generation': 1,
            'params': params, 'depends_on': []}


def test_update_action_reaches_reader_backend_and_raw(tmp_path):
    program = model.generate_program(7, writers=1, readers=1)
    program['actions'] = [action('create-r', 'create', 'r0'),
                          action('change', 'update_filter', 'r0', expression='data = %0', parameters=['x'])]
    assert model.validate_program(program)['status'] == 'valid'
    result = run_topology(program, {'backend': 'offline_fake', 'run_id': 'dynamic-cft-fixture',
                                    'domain_id': 191}, tmp_path, backend=OfflineTopologyBackend())
    rows = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    update = next(row for row in rows if row['event'] == 'topology_filter_update_result')
    assert update['logical_id'] == 'r0' and update['generation'] == 1
    assert update['reader_gid'] and update['receipt']['api_status'] == 'unavailable_api'
    assert update['filter_effect_observed'] is None
    assert not result['real_execution'] and not result['candidate_eligible']


def test_update_api_success_readback_and_absence_are_separate():
    class Sub:
        def set_content_filter(self, expression, parameters):
            self.expression, self.parameters = expression, parameters
        def get_content_filter(self):
            return SimpleNamespace(filter_expression=self.expression, expression_parameters=self.parameters)
    adapter = ContentFilterAdapter()
    adapter.attach(Sub())
    value = adapter.update('data = %0', ['x'], field_types={'data': 'string'})
    assert value['api_status'] == 'succeeded' and value['readback_status'] == 'matched'
    assert value['applied'] and value['filter_effect_observed'] is None and not value['hit']
    adapter.attach(SimpleNamespace())
    value = adapter.update('data = %0', ['x'], field_types={'data': 'string'})
    assert value['api_status'] == 'unavailable_api' and not value['applied']


def test_fixture_records_exact_string_data_and_hash(tmp_path):
    import hashlib
    program = model.generate_program(7, writers=1, readers=1)
    program['actions'] = [action('create-w', 'create', 'w0'),
                          action('create-r', 'create', 'r0'),
                          action('send', 'publish_batch', 'w0', count=2, start_sequence=1)]
    result = run_topology(program, {'backend': 'offline_fake', 'run_id': 'encoded-fixture',
                                    'domain_id': 191}, tmp_path, backend=OfflineTopologyBackend())
    rows = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    sends = [row for row in rows if row['event'] == 'send']
    assert len(sends) == 2
    for row in sends:
        data = row['string_data']
        assert hashlib.sha256(data.encode()).hexdigest() == row['payload_sha256']
        assert json.loads(data)['send_sequence'] == row['send_sequence']
    assert sends[0]['string_data'] != sends[1]['string_data']
    assert not result['real_execution'] and not result['candidate_eligible']


class DynamicFixture(OfflineTopologyBackend):
    """Scripted contract double: neither API nor delivery is real DDS."""
    def __init__(self, *, omit=(2,), api=True, corrupt=None):
        super().__init__()
        self.omit = set(omit)
        self.api = api
        self.corrupt = corrupt
        self.changed = False

    def _emit(self, event, **fields):
        if event == 'receive' and self.changed and fields.get('send_sequence') in self.omit:
            return {'event': event, **fields}
        if event == 'receive' and self.corrupt == 'encoding' and self.changed:
            fields['string_data'] = fields.get('string_data', '') + 'x'
        if event == 'receive' and self.corrupt == 'generation' and self.changed:
            fields['payload_writer_generation'] = 999
        return super()._emit(event, **fields)

    def update_filter(self, handle, expression, parameters):
        if not self.api:
            return super().update_filter(handle, expression, parameters)
        import time
        self.command_serial += 1
        start = time.monotonic_ns()
        opts = {'expression': expression, 'parameters': list(parameters)}
        receipt = {'receipt_kind': 'filter_update', 'api_status': 'succeeded',
                   'readback_status': 'matched', 'applied': True,
                   'requested': {**opts, 'enabled': True}, 'observed': opts, 'fixture': True}
        identity = {k: handle.get(k) for k in ('logical_id', 'generation', 'pid',
                                               'process_start_ticks', 'endpoint_generation')}
        for stage in ('set_begin', 'set_success', 'get_begin', 'get_success'):
            extra = {'requested': receipt['requested']} if stage == 'set_begin' else (
                {'observed': opts, 'raw_readback': {'type': 'builtins.dict', 'status': 'captured',
                  'shape': 'dict', 'fields': {'filter_expression': expression,
                  'expression_parameters': list(parameters)}, 'parameters_type': 'builtins.list'}}
                if stage == 'get_success' else {})
            self._emit('content_filter_api_call', command_id=self.command_serial,
                       reader_gid=handle['endpoint_gid'], api_stage=stage, fixture=True,
                       api_monotonic_ns=time.monotonic_ns(), **identity, **extra)
        self._emit('content_filter_dynamic_update', command_id=self.command_serial,
                   reader_gid=handle['endpoint_gid'], update_started_ns=start,
                   update_finished_ns=time.monotonic_ns(), filter_receipt=receipt,
                   **{k: handle.get(k) for k in ('logical_id', 'generation', 'pid',
                                                'process_start_ticks', 'endpoint_generation')})
        self.changed = True
        return {'command_id': self.command_serial, 'filter_receipt': receipt}


def run_contrast(tmp_path, *, backend=None, expression='data LIKE %0', parameter="'%: 3%'"):
    program = model.generate_program(7, writers=1, readers=1)
    for endpoint in program['endpoints']:
        endpoint['qos']['reliability'] = 'reliable'
    program['actions'] = [action('create-w', 'create', 'w0'), action('create-r', 'create', 'r0'),
                          action('before', 'publish_batch', 'w0', count=1, start_sequence=1, payload='same'),
                          action('change', 'update_filter', 'r0', expression=expression, parameters=[parameter]),
                          action('after', 'publish_batch', 'w0', count=2, start_sequence=2, payload='same')]
    assert model.validate_program(program)['status'] == 'valid'
    return run_topology(program, {'backend': 'offline_fake', 'run_id': 'contrast-fixture',
                                  'domain_id': 191}, tmp_path, backend=backend or DynamicFixture())


def test_dynamic_contrast_end_to_end_fixture(tmp_path):
    result = run_contrast(tmp_path)
    observation = result['dynamic_filter_effects'][0]
    assert observation['status'] == 'delivery_contrast', observation
    assert observation['fixture_contrast_observed'] is True
    assert observation['filter_effect_observed'] is None
    assert not observation['candidate_eligible'] and not result['real_execution']
    rows = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    assert rows[observation['positive_control_send_refs'][0]]['event'] == 'send'
    assert rows[observation['sentinel_receive_ref']]['event'] == 'receive'
    assert result['facts']['dynamic_filter_effects'] == result['dynamic_filter_effects']


def test_dynamic_contrast_fails_closed(tmp_path):
    cases = [(DynamicFixture(api=False), {}, 'update_api_or_actual_readback_unavailable'),
             (DynamicFixture(omit=()), {}, 'positive_control_or_post_contrast_missing'),
             (DynamicFixture(omit=(2, 3)), {}, 'window_not_raw_closed'),
             (DynamicFixture(corrupt='encoding'), {}, 'receive_sample_identity_or_encoding_invalid'),
             (DynamicFixture(corrupt='generation'), {}, 'receive_sample_identity_or_encoding_invalid'),
             (DynamicFixture(), {'expression': 'data = %0', 'parameter': "'x'"}, 'predicate_unsupported'),
             (DynamicFixture(), {'parameter': "'%_%'"}, 'predicate_parameter_unsupported')]
    for index, (backend, options, reason) in enumerate(cases):
        observation = run_contrast(tmp_path / str(index), backend=backend, **options)['dynamic_filter_effects'][0]
        assert observation['status'] == 'unknown', observation
        assert observation['filter_effect_observed'] is None
        assert observation['reason'] == reason, observation


def test_consumer_rejects_raw_tampering_and_missing_controls(tmp_path):
    from copy import deepcopy
    from r2f.runtime.topology_pair import _dynamic_cft_effect
    result = run_contrast(tmp_path)
    original = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    uid = next(e['event_index'] for e in original if e['event'] == 'topology_filter_update_result')
    successful = _dynamic_cft_effect(original[uid], original, real_ros=False)
    assert successful['status'] == 'delivery_contrast'
    # Even explicitly passing real_ros cannot promote fixture rows to DDS.
    assert _dynamic_cft_effect(original[uid], original, real_ros=True)['reason'] == 'fixture_cannot_qualify_real_effect'

    rows = deepcopy(original)
    rows[uid]['receipt']['observed']['parameters'] = ["'different'"]
    assert _dynamic_cft_effect(rows[uid], rows, real_ros=False)['reason'] == 'update_api_or_actual_readback_unavailable'

    rows = deepcopy(original)
    rows[successful['worker_update_ref']]['generation'] += 1
    assert _dynamic_cft_effect(rows[uid], rows, real_ros=False)['reason'] == 'worker_update_missing_or_ambiguous'

    rows = deepcopy(original)
    rows[successful['positive_control_send_refs'][0]]['string_data'] += 'x'
    assert _dynamic_cft_effect(rows[uid], rows, real_ros=False)['reason'] == 'sample_encoding_or_hash_invalid'

    rows = deepcopy(original)
    rows[successful['worker_update_ref']]['update_finished_ns'] = 2**63
    assert _dynamic_cft_effect(rows[uid], rows, real_ros=False)['reason'] == 'pre_or_post_raw_window_missing'

    rows = deepcopy(original)
    receive = deepcopy(rows[successful['sentinel_receive_ref']])
    send = rows[successful['missing_send_refs'][0]]
    receive.update(event_index=len(rows), send_sequence=send['send_sequence'], payload_sha256=send['payload_sha256'])
    rows.append(receive)
    assert _dynamic_cft_effect(rows[uid], rows, real_ros=False)['reason'] == 'late_receive_after_window'


def test_consumer_requires_timed_closure_and_preupdate_control(tmp_path):
    from copy import deepcopy
    from r2f.runtime.topology_pair import _dynamic_cft_effect
    result = run_contrast(tmp_path)
    rows = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    uid = next(e['event_index'] for e in rows if e['event'] == 'topology_filter_update_result')
    observation = _dynamic_cft_effect(rows[uid], rows, real_ros=False)
    mutated = deepcopy(rows)
    missing = mutated[observation['missing_send_refs'][0]]
    missing['monotonic_ns'] = mutated[observation['sentinel_receive_ref']]['monotonic_ns'] + 1
    assert _dynamic_cft_effect(mutated[uid], mutated, real_ros=False)['status'] == 'unknown'

    mutated = deepcopy(rows)
    pre = mutated[observation['pre_window_ref']]
    pre['receive_refs'] = []
    assert _dynamic_cft_effect(mutated[uid], mutated, real_ros=False)['reason'] == 'window_not_raw_closed'


def test_independent_api_events_and_failures():
    class Sub:
        def set_content_filter(self, expression, parameters):
            self.options = SimpleNamespace(filter_expression=expression, expression_parameters=parameters)
        def get_content_filter(self):
            return self.options
    adapter = ContentFilterAdapter()
    adapter.attach(Sub())
    events = []
    result = adapter.update('data LIKE %0', ["'%x%'"], field_types={'data': 'string'},
                            api_event=lambda stage, **kw: events.append((stage, kw)))
    assert [x[0] for x in events] == ['set_begin', 'set_success', 'get_begin', 'get_success']
    assert events[0][1]['requested'] == result['requested']
    assert events[-1][1]['observed'] == result['observed']
    class Broken(Sub):
        def get_content_filter(self):
            raise RuntimeError('failure')
    adapter.attach(Broken())
    events.clear()
    assert adapter.update('data LIKE %0', ["'%x%'"], field_types={'data': 'string'},
                          api_event=lambda stage, **kw: events.append((stage, kw)))['readback_status'] == 'failed'
    assert [x[0] for x in events] == ['set_begin', 'set_success', 'get_begin', 'get_failure']


def test_api_evidence_and_payload_equivalence_are_conservative(tmp_path):
    from copy import deepcopy
    from r2f.runtime.topology_pair import _dynamic_cft_effect
    result = run_contrast(tmp_path)
    rows = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    uid = next(e['event_index'] for e in rows if e['event'] == 'topology_filter_update_result')
    contrast = _dynamic_cft_effect(rows[uid], rows, real_ros=False)
    assert contrast['status'] == 'delivery_contrast' and contrast['filter_effect_observed'] is None
    assert contrast['control_equivalence'].startswith('declared_payload_only')
    assert rows[contrast['positive_control_send_refs'][0]]['string_data'] != rows[contrast['missing_send_refs'][0]]['string_data']
    for field, value in [('api_stage', 'get_failure'), ('generation', 999),
                         ('observed', {'expression': 'fake', 'parameters': []})]:
        altered = deepcopy(rows)
        altered[contrast['api_call_refs'][-1]][field] = value
        assert _dynamic_cft_effect(altered[uid], altered, real_ros=False)['status'] == 'unknown'
    altered = deepcopy(rows)
    altered[contrast['api_call_refs'][0]]['event'] = 'removed'
    assert _dynamic_cft_effect(altered[uid], altered, real_ros=False)['reason'] == 'independent_api_call_evidence_invalid'
    altered = deepcopy(rows)
    altered[contrast['api_call_refs'][1]]['api_monotonic_ns'] = 0
    assert _dynamic_cft_effect(altered[uid], altered, real_ros=False)['status'] == 'unknown'


def test_readback_capture_type_fields_and_unsupported():
    from collections import namedtuple
    from r2f.runtime.filter_backend import capture_update_readback, normalize_captured_update_readback
    opts = namedtuple('ContentFilterOptions', ['filter_expression', 'expression_parameters'])
    opts.__module__ = 'rclpy.subscription_content_filter_options'
    raw = capture_update_readback(opts('data LIKE %0', ["'%x%'"]))
    assert raw['type'] == 'rclpy.subscription_content_filter_options.ContentFilterOptions'
    assert raw['fields'] == {'filter_expression': 'data LIKE %0', 'expression_parameters': ["'%x%'"]}
    assert normalize_captured_update_readback(raw) == {'expression': 'data LIKE %0', 'parameters': ["'%x%'"]}
    assert capture_update_readback({'filter_expression': 'x', 'expression_parameters': ('a',)})['parameters_type'] == 'builtins.tuple'
    assert capture_update_readback(object())['status'] == 'unsupported'
    assert capture_update_readback({'filter_expression': 'x', 'expression_parameters': object()})['status'] == 'unsupported'
    assert capture_update_readback({'filter_expression': 'x'})['status'] == 'unsupported'
    assert normalize_captured_update_readback(capture_update_readback(object())) is None


def test_unsupported_getter_return_and_raw_conversion_tampering(tmp_path):
    from copy import deepcopy
    from r2f.runtime.topology_pair import _dynamic_cft_effect
    class Sub:
        def set_content_filter(self, expression, parameters): pass
        def get_content_filter(self): return object()
    adapter = ContentFilterAdapter()
    adapter.attach(Sub())
    events = []
    receipt = adapter.update('data LIKE %0', ["'%x%'"], field_types={'data': 'string'},
                             api_event=lambda stage, **kw: events.append((stage, kw)))
    assert receipt['readback_status'] == 'invalid_result'
    assert events[-1][0] == 'get_success' and events[-1][1]['observed'] is None
    assert events[-1][1]['raw_readback']['status'] == 'unsupported'
    assert 'repr' not in events[-1][1]['raw_readback']
    result = run_contrast(tmp_path)
    rows = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    uid = next(e['event_index'] for e in rows if e['event'] == 'topology_filter_update_result')
    proof = _dynamic_cft_effect(rows[uid], rows, real_ros=False)
    ref = proof['api_call_refs'][-1]
    mutations = [lambda r: r.pop('raw_readback'),
                 lambda r: r['raw_readback']['fields'].__setitem__('filter_expression', 'wrong'),
                 lambda r: r['raw_readback'].__setitem__('type', 'unknown.Type'),
                 lambda r: r['raw_readback']['fields'].__setitem__('expression_parameters', None),
                 lambda r: r['raw_readback'].__setitem__('parameters_type', 'unknown')]
    for mutate in mutations:
        altered = deepcopy(rows)
        mutate(altered[ref])
        value = _dynamic_cft_effect(altered[uid], altered, real_ros=False)
        assert value['status'] == 'unknown' and value['reason'] == 'raw_api_readback_invalid_or_inconsistent'


def test_legacy_object_readback_matches_adapter_but_not_dynamic_evidence(tmp_path):
    from copy import deepcopy
    from r2f.runtime.topology_pair import _dynamic_cft_effect
    class LegacyOptions:
        def __init__(self, expression, parameters):
            self.filter_expression = expression
            self.expression_parameters = parameters
    class Sub:
        def set_content_filter(self, expression, parameters):
            self.options = LegacyOptions(expression, parameters)
        def get_content_filter(self):
            return self.options
    adapter = ContentFilterAdapter()
    adapter.attach(Sub())
    events = []
    receipt = adapter.update('data LIKE %0', ["'%x%'"], field_types={'data': 'string'},
                             api_event=lambda stage, **fields: events.append((stage, fields)))
    assert receipt['readback_status'] == 'matched' and receipt['applied'] is True
    assert events[-1][1]['observed'] == receipt['observed']
    assert events[-1][1]['raw_readback']['status'] == 'unsupported'
    result = run_contrast(tmp_path)
    rows = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    uid = next(e['event_index'] for e in rows if e['event'] == 'topology_filter_update_result')
    contrast = _dynamic_cft_effect(rows[uid], rows, real_ros=False)
    assert contrast['status'] == 'delivery_contrast'
    altered = deepcopy(rows)
    raw = altered[contrast['api_call_refs'][-1]]['raw_readback']
    raw.update(type='legacy.FakeOptions', status='unsupported', reason='unsupported_return_shape')
    verdict = _dynamic_cft_effect(altered[uid], altered, real_ros=False)
    assert verdict['reason'] == 'raw_api_readback_invalid_or_inconsistent'
    assert verdict['filter_effect_observed'] is None


def test_legacy_normalization_exception_remains_failed_readback():
    class BrokenOptions:
        @property
        def filter_expression(self):
            raise ValueError('bad field')
    class Sub:
        def set_content_filter(self, expression, parameters): pass
        def get_content_filter(self): return BrokenOptions()
    adapter = ContentFilterAdapter()
    adapter.attach(Sub())
    events = []
    receipt = adapter.update('data LIKE %0', ["'%x%'"], field_types={'data': 'string'},
                             api_event=lambda stage, **fields: events.append((stage, fields)))
    assert receipt['readback_status'] == 'failed' and not receipt['applied']
    assert events[-1][0] == 'get_success'
    assert events[-1][1]['raw_readback']['status'] == 'unsupported'
    assert events[-1][1]['normalization_error_type'] == 'ValueError'


def test_repeated_complete_string_via_runner_and_ambiguity(tmp_path):
    from copy import deepcopy
    from r2f.runtime.topology_pair import _repeat_data_observation, payload_delivery_witness
    program = model.generate_program(7, writers=1, readers=1)
    program['actions'] = [action('create-w', 'create', 'w0'), action('create-r', 'create', 'r0'),
                          action('before', 'publish_batch', 'w0', count=1, start_sequence=1, payload='same'),
                          action('change', 'update_filter', 'r0', expression='data LIKE %0', parameters=["'%same%'"]),
                          action('repeat', 'publish_batch', 'w0', count=1, start_sequence=2, repeat_data_from='before')]
    assert model.validate_program(program)['status'] == 'valid'
    result = run_topology(program, {'backend': 'offline_fake', 'run_id': 'repeat-fixture', 'domain_id': 191},
                          tmp_path, backend=DynamicFixture(omit=()))
    rows = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    observation = result['repeat_data_observations'][0]
    assert observation['status'] == 'identical_input_sent', observation
    assert observation['pre_control_receive_ref'] is not None and observation['between_update_ref'] is not None
    assert result['facts']['repeat_data_observations'] == result['repeat_data_observations']
    source, repeat = (rows[observation[k]] for k in ('source_send_ref', 'repeat_send_ref'))
    assert source['string_data'] == repeat['string_data']
    assert source['send_sequence'] != repeat['send_sequence'] and source['command_id'] != repeat['command_id']
    assert observation['filter_effect_observed'] is None and observation['delivery_attribution'] == 'unknown'
    receives = [e for e in rows if e['event'] == 'receive' and e.get('payload_sha256') == source['payload_sha256']]
    assert len(receives) == 2
    assert all(e['send_sequence'] == source['send_sequence'] for e in receives)
    pair = {'writer': 'w0', 'writer_generation': 1, 'writer_gid': source['writer_gid'],
            'reader': 'r0', 'reader_generation': 1, 'reader_gid': receives[0]['reader_gid']}
    assert payload_delivery_witness(pair, receives[-1], rows)[1] == 'payload_send_mapping_not_unique'
    binding = next(e for e in rows if e['event'] == 'topology_repeat_data_binding')
    altered = deepcopy(rows)
    altered[observation['pre_control_receive_ref']]['generation'] += 1
    unresolved = _repeat_data_observation(altered[binding['event_index']], altered)
    assert unresolved['pre_control_receive_ref'] is None
    assert unresolved['filter_effect_observed'] is None
    for change in ('data', 'generation', 'command', 'time'):
        altered = deepcopy(rows)
        row = altered[observation['repeat_send_ref']]
        if change == 'data': row['string_data'] += 'x'
        if change == 'generation': row['generation'] += 1
        if change == 'command': row['command_id'] = source['command_id']
        if change == 'time': row['monotonic_ns'] = 0
        assert _repeat_data_observation(altered[binding['event_index']], altered)['status'] == 'unknown'

    # A receive can be drained before the writer's post-return send row.
    source_idx = observation['source_send_ref']
    receive_idx = observation['pre_control_receive_ref']
    update_raw = next(e for e in rows if e['event'] == 'content_filter_dynamic_update')
    call = next(e for e in rows if e['event'] == 'content_filter_api_call' and e['api_stage'] == 'set_begin')
    altered = deepcopy(rows)
    altered[source_idx], altered[receive_idx] = altered[receive_idx], altered[source_idx]
    for idx, row in enumerate(altered):
        row['event_index'] = idx
    moved_binding = altered[binding['event_index']]
    moved_binding['source_send_ref'] = receive_idx
    assert _repeat_data_observation(moved_binding, altered)['pre_control_receive_ref'] == source_idx

    # Arrival during publish is valid; arrival before begin or at update begin is not.
    altered = deepcopy(rows)
    send = altered[source_idx]
    receive = altered[receive_idx]
    receive['monotonic_ns'] = send['publish_begin_ns']
    assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] == receive_idx
    for field, value in [('monotonic_ns', send['publish_begin_ns'] - 1),
                         ('monotonic_ns', update_raw['update_started_ns']),
                         ('monotonic_clock_identity', None),
                         ('pid', -1)]:
        altered = deepcopy(rows)
        altered[receive_idx][field] = value
        assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None
    for index, field, value in [(source_idx, 'publish_begin_ns', None),
                                 (source_idx, 'publish_return_ns', 0),
                                 (call['event_index'], 'monotonic_clock_identity', None),
                                 (call['event_index'], 'process_start_ticks', -1),
                                 (update_raw['event_index'], 'update_started_ns', None)]:
        altered = deepcopy(rows)
        altered[index][field] = value
        assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None

    # Matching forged API identities must also match the unique reader creation.
    reader_created = next(e for e in rows if e['event'] == 'endpoint_created' and e.get('logical_id') == 'r0')
    update_row = rows[observation['between_update_ref']]
    for field in ('pid', 'process_start_ticks', 'endpoint_generation', 'run_id'):
        altered = deepcopy(rows)
        for idx in (call['event_index'], update_raw['event_index']):
            altered[idx][field] = 'wrong' if isinstance(altered[idx][field], str) else -1
        assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None
    altered = deepcopy(rows)
    for idx in (call['event_index'], update_raw['event_index'], update_row['event_index']):
        altered[idx]['generation'] += 1
    assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None
    altered = deepcopy(rows)
    altered[reader_created['event_index']]['event'] = 'missing_creation'
    assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None
    altered = deepcopy(rows)
    altered.append({'event': 'endpoint_destroyed', 'logical_id': 'r0', 'generation': reader_created['generation'],
                    'monotonic_ns': update_raw['update_started_ns'] - 1,
                    'monotonic_clock_identity': reader_created['monotonic_clock_identity']})
    assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None
    altered[-1]['monotonic_ns'] = update_raw['update_finished_ns'] + 1
    assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] == receive_idx
    altered[-1].pop('monotonic_ns')
    assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None
    # A real-backend lifecycle emission with a foreign or missing time namespace cannot close this window.
    for event in ('topology_worker_exit', 'worker_exit', 'topology_endpoint_destroyed'):
        altered = deepcopy(rows)
        retirement = {'event': event, 'logical_id': 'r0', 'generation': reader_created['generation'],
                      'monotonic_ns': update_raw['update_finished_ns'] + 1,
                      'monotonic_clock_identity': dict(reader_created['monotonic_clock_identity'])}
        altered.append(retirement)
        assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] == receive_idx
        retirement['monotonic_ns'] = update_raw['update_started_ns'] - 1
        assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None
        retirement['monotonic_ns'] = update_raw['update_finished_ns'] + 1
        retirement['monotonic_clock_identity']['inode'] += 1
        assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None
        retirement.pop('monotonic_clock_identity')
        assert _repeat_data_observation(altered[binding['event_index']], altered)['pre_control_receive_ref'] is None


def test_repeat_missing_source_rejected_without_send(tmp_path):
    program = model.generate_program(7, writers=1, readers=1)
    program['actions'] = [action('create-w', 'create', 'w0'),
                          action('repeat', 'publish_batch', 'w0', count=1, start_sequence=1, repeat_data_from='missing')]
    result = run_topology(program, {'backend': 'offline_fake', 'run_id': 'missing-repeat', 'domain_id': 191},
                          tmp_path, backend=DynamicFixture())
    assert any(x.get('code') == 'repeat_source_missing_or_ambiguous' for x in result['guard_receipts'])
    assert not result['repeat_data_observations']


def test_real_backend_lifecycle_emits_own_clock_without_ros(tmp_path, monkeypatch):
    from r2f.runtime import topology_worker as worker
    clock = {'boot_id': 'fixture-boot', 'device': 7, 'inode': 42}
    monkeypatch.setattr(worker, '_monotonic_clock_identity', lambda: dict(clock))
    captured = []
    backend = worker.RosTopologyBackend({}, tmp_path,
        event_sink=lambda event, **fields: captured.append({'event': event, **fields}))
    class FakeProcess:
        process = None
        def close(self):
            return {'closed': True, 'returncode': 0}
    def handle(process):
        return {'process': process, 'alive': True, 'logical_id': 'r0', 'generation': 1,
                'role': 'reader', 'endpoint_gid': 'gid', 'pid': 123, 'pgid': 123,
                'process_start_ticks': 99, 'endpoint_generation': 'gen'}
    process = FakeProcess()
    assert backend.destroy_endpoint(handle(process))['cleanup_status'] == 'closed'
    assert [x['event'] for x in captured] == ['topology_worker_exit', 'worker_exit', 'topology_endpoint_destroyed']
    assert all(x['monotonic_clock_identity'] == clock and type(x['monotonic_ns']) is int for x in captured)
    # Even explicit fields cannot substitute a worker's time namespace for the emitter's.
    assert backend._emit('probe', monotonic_clock_identity={'inode': 999})['monotonic_clock_identity'] == clock
    captured.clear()
    process = FakeProcess()
    backend.processes = [process]
    backend.handles = {'r0': handle(process)}
    assert backend.close()['closed'] is True
    exits = [x for x in captured if x['event'] in ('topology_worker_exit', 'worker_exit')]
    assert len(exits) == 2 and all(x['monotonic_clock_identity'] == clock for x in exits)
    monkeypatch.setattr(worker, '_monotonic_clock_identity', lambda: None)
    assert backend._emit('unavailable_clock')['monotonic_clock_identity'] is None
