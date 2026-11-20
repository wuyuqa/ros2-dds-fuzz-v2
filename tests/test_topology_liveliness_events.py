"""Offline callback-contract tests; no ROS context or DDS participant is started."""
import ast
import inspect
from types import SimpleNamespace

from r2f.runtime import topology_worker as tw


def test_reader_early_event_buffer_preserves_time_and_aggregate_scope():
    rows = []
    c = tw._LivelinessEventCapture('reader', lambda event, **kw: rows.append((event, kw)))
    c.callback(SimpleNamespace(alive_count=2, not_alive_count=1, alive_count_change=1, not_alive_count_change=-1))
    assert not rows
    c.bind('reader-gid')
    event, row = rows[0]
    assert event == 'topology_liveliness_event' and row['endpoint_gid'] is None and row['later_bound_endpoint_gid'] == 'reader-gid'
    assert row['buffered'] is True and row['identity_bound_monotonic_ns'] >= row['callback_monotonic_ns']
    assert row['status_fields'] == {'alive_count': 2, 'not_alive_count': 1, 'alive_count_change': 1, 'not_alive_count_change': -1}
    assert row['observation_scope'] == 'reader_aggregate' and row['writer_attribution'] is None
    assert type(row['callback_monotonic_ns']) is int and row['callback_clock_identity'] is not None
    assert row['during_cleanup'] is False


def test_writer_lost_fields_cleanup_and_unbound_flush():
    rows = []
    c = tw._LivelinessEventCapture('writer', lambda event, **kw: rows.append(kw))
    c.callback(SimpleNamespace(total_count=3, total_count_change=1))
    c.close()
    assert rows[0]['event_kind'] == 'liveliness_lost'
    assert rows[0]['endpoint_gid'] is None and rows[0]['endpoint_identity_status'] == 'unavailable'
    assert rows[0]['status_fields'] == {'total_count': 3, 'total_count_change': 1}
    c.callback(SimpleNamespace(total_count=4))
    assert rows[-1]['during_cleanup'] is True and rows[-1]['status_fields']['total_count_change'] is None


def test_reader_creation_passes_callbacks_with_and_without_filter():
    calls = []
    class Node:
        def create_subscription(self, *args, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(get_content_filter=lambda: None)
    token = object()
    tw._create_reader_with_initial_filter(Node(), object, '/x', lambda *_: None, object(), event_callbacks=token)
    assert calls[-1]['event_callbacks'] is token
    tw._create_reader_with_initial_filter(Node(), object, '/x', lambda *_: None, object(),
        {'expression': 'data = %0', 'parameters': ['x']}, event_callbacks=token,
        options_factory=lambda expression, parameters: SimpleNamespace(filter_expression=expression, expression_parameters=parameters))
    assert calls[-1]['event_callbacks'] is token and 'content_filter_options' in calls[-1]


def test_worker_public_api_wiring_and_fixture_separation():
    source = inspect.getsource(tw._worker_main)
    tree = ast.parse(source)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
    publisher = next(n for n in calls if isinstance(n.func, ast.Attribute) and n.func.attr == 'create_publisher')
    reader = next(n for n in calls if isinstance(n.func, ast.Name) and n.func.id == '_create_reader_with_initial_filter')
    assert 'event_callbacks' in ast.unparse(publisher)
    assert any(k.arg == 'event_callbacks' for k in reader.keywords)
    assert 'PublisherEventCallbacks if kind == "writer" else SubscriptionEventCallbacks' in source
    assert 'callback_type(liveliness=liveliness.callback)' in source
    assert 'liveliness.bind(local_gid)' in source and 'liveliness.close()' in source
    assert '_LivelinessEventCapture' not in inspect.getsource(tw.OfflineTopologyBackend)


def test_unsupported_creation_never_retries_or_swallows_other_failures():
    import pytest
    class Unsupported(Exception): pass
    for role in ('writer', 'reader', 'reader_cft'):
        rows, calls = [], []
        def create():
            calls.append(role)
            try:
                raise Unsupported('event')
            except Unsupported as cause:
                if role == 'reader_cft':
                    raise tw.TopologyUnsupported('filter', 'wrapped') from cause
                raise
        with pytest.raises(tw.TopologyUnsupported, match='no retry'):
            tw._create_with_liveliness_audit(create, Unsupported, lambda event, **kw: rows.append((event, kw)))
        assert calls == [role]
        assert rows[0][1]['status'] == 'unavailable_rmw_event'
        assert rows[0][1]['retry'] == 'refused_cleanup_unverified'
    def broken(): raise ValueError('unrelated')
    with pytest.raises(ValueError):
        tw._create_with_liveliness_audit(broken, Unsupported, lambda *_a, **_k: None)


def test_capture_marks_creation_and_binding_boundaries():
    rows = []
    c = tw._LivelinessEventCapture('writer', lambda e, **kw: rows.append(kw))
    c.create_begin_ns = 1
    c.callback(SimpleNamespace(total_count=1, total_count_change=1))
    c.create_return_ns = 2
    c.bind('g')
    c.callback(SimpleNamespace(total_count=2, total_count_change=1))
    assert rows[0]['buffered'] and rows[0]['endpoint_gid'] is None
    assert rows[0]['create_call_begin_monotonic_ns'] == 1 and rows[0]['create_call_return_monotonic_ns'] == 2
    assert rows[1]['buffered'] is False and rows[1]['endpoint_gid'] == 'g'
    assert rows[1]['identity_bound_monotonic_ns'] <= rows[1]['callback_monotonic_ns']


def test_real_cft_helper_exception_chain_is_audited_without_retry():
    import pytest
    class Unsupported(Exception):
        pass
    class Node:
        calls = 0
        def create_subscription(self, *args, **kwargs):
            self.calls += 1
            assert 'content_filter_options' in kwargs
            assert kwargs['event_callbacks'] is token
            raise Unsupported('unsupported event')
    node, token, rows = Node(), object(), []
    def emit(event, **fields):
        rows.append((event, fields))
    def create():
        return tw._create_reader_with_initial_filter(
            node, object, '/x', lambda *_: None, object(),
            {'expression': 'data = %0', 'parameters': ['x']}, emit=emit,
            event_callbacks=token,
            options_factory=lambda expression, parameters: SimpleNamespace(
                filter_expression=expression, expression_parameters=parameters))
    with pytest.raises(tw.TopologyUnsupported, match='no retry') as caught:
        tw._create_with_liveliness_audit(create, Unsupported, emit)
    assert isinstance(caught.value.__cause__, tw.TopologyUnsupported)
    assert isinstance(caught.value.__cause__.__cause__, Unsupported)
    assert node.calls == 1
    assert any(e == 'content_filter_initial_guard' for e, _ in rows)
    assert any(e == 'topology_liveliness_capability' and
               v['retry'] == 'refused_cleanup_unverified' for e, v in rows)


def test_cleanup_records_each_outcome_and_does_not_claim_release():
    rows, calls = [], []
    def action(name, result=None, error=None):
        def run():
            calls.append(name)
            if error:
                raise error
            return result
        return run
    executor = SimpleNamespace(shutdown=action('executor', error=RuntimeError('failure')))
    node = SimpleNamespace(destroy_node=action('node', False))
    context = SimpleNamespace(shutdown=action('context'))
    outcomes = tw._audit_worker_cleanup(executor, node, context,
        lambda e, **kw: rows.append((e, kw)), endpoint_creation_returned=False)
    assert calls == ['executor', 'node', 'context']
    assert outcomes == {'executor': {'status': 'exception', 'error_type': 'RuntimeError'},
                        'node': {'status': 'returned_false', 'error_type': None},
                        'context': {'status': 'returned', 'error_type': None}}
    assert [e for e, _ in rows] == ['worker_cleanup_step'] * 3 + ['worker_shutdown']
    assert rows[-1][1]['endpoint_creation_returned'] is False
    assert rows[-1][1]['resource_release_verified'] is False


def test_assert_request_is_emitted_before_write_and_result_after_flush(tmp_path):
    import json
    rows, order = [], []
    class Pipe:
        def write(self, data):
            order.append('write')
            self.payload = json.loads(data)
            return len(data)
        def flush(self):
            order.append('flush')
    pipe = Pipe()
    def emit(event, **fields):
        order.append(event)
        rows.append((event, fields))
    process = tw._EndpointProcess({'generation': 3}, {}, tmp_path, 'writer', emit)
    process.process = SimpleNamespace(stdin=pipe, poll=lambda: None)
    process.events = [{'event': 'endpoint_created', 'logical_id': 'w', 'generation': 3,
                       'endpoint_gid': 'gid', 'endpoint_generation': 'gen', 'pid': 10,
                       'process_start_ticks': 20, 'run_id': 'run'}]
    assert process.command('assert_liveliness') == 1
    assert order == ['topology_assert_liveliness_requested', 'write', 'flush',
                     'topology_assert_liveliness_write_result']
    assert pipe.payload == {'command': 'assert_liveliness', 'command_id': 1}
    request, result = rows[0][1], rows[1][1]
    assert request['endpoint_gid'] == result['endpoint_gid'] == 'gid'
    assert request['pid'] == 10 and request['process_start_ticks'] == 20
    assert request['run_id'] == 'run' and request['endpoint_generation'] == 'gen'
    assert request['request_clock_identity'] == result['result_clock_identity']
    assert request['request_monotonic_ns'] <= result['result_monotonic_ns']
    assert result['status'] == 'flushed' and result['bytes_written'] == result['bytes_expected']


def test_assert_request_records_write_and_flush_failures(tmp_path):
    import pytest
    for mode in ('short', 'write_error', 'flush_error'):
        rows = []
        class Pipe:
            def write(self, data):
                if mode == 'write_error': raise BrokenPipeError()
                return len(data) - 1 if mode == 'short' else len(data)
            def flush(self):
                if mode == 'flush_error': raise OSError()
        process = tw._EndpointProcess({'generation': 1}, {}, tmp_path, 'writer',
                                      lambda e, **kw: rows.append((e, kw)))
        process.process = SimpleNamespace(stdin=Pipe(), poll=lambda: None)
        with pytest.raises(tw.TopologyBackendError):
            process.command('assert_liveliness')
        assert [x[0] for x in rows] == ['topology_assert_liveliness_requested',
                                       'topology_assert_liveliness_write_result']
        assert rows[1][1]['status'] == 'failed' and rows[1][1]['error_type']
        assert rows[0][1]['endpoint_gid'] is None


def test_cleanup_continues_after_broken_output_and_propagates_failure():
    import pytest
    calls = []
    def action(name):
        def run(): calls.append(name)
        return run
    def broken_emit(*args, **kwargs):
        raise BrokenPipeError('output closed')
    with pytest.raises(BrokenPipeError, match='output closed'):
        tw._audit_worker_cleanup(SimpleNamespace(shutdown=action('executor')),
            SimpleNamespace(destroy_node=action('node')),
            SimpleNamespace(shutdown=action('context')), broken_emit,
            endpoint_creation_returned=False)
    assert calls == ['executor', 'node', 'context']


def test_close_failure_cleanup_and_original_error_precedence():
    import pytest
    calls, rows = [], []
    def action(name):
        def run(): calls.append(name)
        return run
    executor = SimpleNamespace(shutdown=action('executor'))
    node = SimpleNamespace(destroy_node=action('node'))
    context = SimpleNamespace(shutdown=action('context'))
    close_error = RuntimeError('close failed')
    def emit(event, **fields): rows.append((event, fields))
    with pytest.raises(RuntimeError, match='close failed') as caught:
        tw._audit_worker_cleanup(executor, node, context, emit,
            endpoint_creation_returned=False, close_error=close_error)
    assert caught.value is close_error
    assert calls == ['executor', 'node', 'context']
    assert rows[-1][1]['liveliness_close_error_type'] == 'RuntimeError'
    calls.clear()
    original = ValueError('original')
    finally_error = None
    try:
        raise original
    except ValueError as exc:
        try:
            tw._audit_worker_cleanup(executor, node, context,
                lambda *a, **k: (_ for _ in ()).throw(BrokenPipeError()),
                endpoint_creation_returned=False, primary_error=exc, close_error=close_error)
            raise
        except ValueError as preserved:
            finally_error = preserved
    assert finally_error is original
    assert calls == ['executor', 'node', 'context']
