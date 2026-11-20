"""No DDS: explicit native trace scopes must bind, never infer send order."""
import threading
from types import SimpleNamespace
import pytest
from r2f.runtime.rmw_publish_observer import PublishOccurrenceObserver

GID = '0123456789abcdef' * 2

class Handle:
    pointer = 111
    def __enter__(self): return self
    def __exit__(self, *args): pass

class Native:
    provenance = {'fixture': True, 'execution_evidence': False}
    def __init__(self, callback): self.callback = callback; self.closed = False
    def publisher_identity(self, pointer): return 222, GID
    def close(self): self.closed = True; return 0

class Endpoint:
    handle = Handle()
    def __init__(self, publish): self.publish = publish

def setup(publish=None, native_factory=Native, rmw='rmw_fastrtps_cpp'):
    rows = []
    def emit(event, **fields):
        row = dict(event=event, **fields); rows.append(row); return row
    observer = PublishOccurrenceObserver(emit, rmw, native_factory=native_factory)
    def ordinary(message): observer.native.callback(222, 333, 123456789)
    endpoint = Endpoint(publish or ordinary)
    return observer, endpoint, rows

def invoke(observer, endpoint, message=None):
    return observer.publish(endpoint, message or SimpleNamespace(data='same full data'),
                            command_id='cmd-1', send_sequence=9, writer_gid=GID)

def test_scope_has_real_native_timestamp_and_normal_publish_once():
    observer, endpoint, rows = setup()
    receipt = invoke(observer, endpoint)
    assert receipt['status'] == 'observed'
    assert receipt['source_timestamp'] == 123456789
    assert receipt['publish_returned'] is True
    assert receipt['trace_count'] == 1
    assert receipt['trace_scope_matched'] is True
    assert receipt['writer_gid'] == GID
    assert receipt['observed_writer_gid'] == GID
    assert receipt['publisher_rmw_handle'] == 222
    assert [r['event'] for r in rows] == ['publish_observer_capability', 'publish_call_begin',
                                         'rmw_publish_trace', 'publish_call_end']
    scoped = rows[1:]
    assert all(r['call_id'] == receipt['call_id'] for r in scoped)
    assert all(r['command_id'] == 'cmd-1' for r in scoped)
    assert all(r['send_sequence'] == 9 for r in scoped)
    assert rows[2]['trace_ref'] == receipt['trace_ref']
    assert rows[1]['monotonic_ns'] <= rows[2]['monotonic_ns'] <= rows[3]['monotonic_ns']
    assert receipt['candidate_eligible'] is False
    assert observer.close()['status'] == 'closed'
    assert observer.native.closed

def test_missing_trace_is_unknown_but_publish_still_runs():
    called = []
    observer, endpoint, rows = setup(lambda message: called.append(message.data))
    receipt = invoke(observer, endpoint)
    assert called == ['same full data']
    assert receipt['status'] == 'unknown'
    assert receipt['reason'] == 'trace_count_not_one'
    assert receipt['source_timestamp'] is None
    observer.close()

@pytest.mark.parametrize('trace_values,reason', [
    ([(222, 333, 5), (222, 334, 6)], 'trace_count_not_one'),
    ([(999, 333, 5)], 'trace_publisher_handle_mismatch'),
    ([(222, 0, 5)], 'trace_message_handle_missing'),
    ([(222, 333, 0)], 'trace_timestamp_unavailable'),
    ([(222, 333, -1)], 'trace_timestamp_unavailable'),
])
def test_trace_invalid_never_becomes_occurrence(trace_values, reason):
    observer, endpoint, rows = setup()
    endpoint.publish = lambda msg: [observer.native.callback(*v) for v in trace_values]
    receipt = invoke(observer, endpoint)
    assert receipt['status'] == 'unknown'
    assert receipt['reason'] == reason
    assert receipt['source_timestamp'] is None
    observer.close()

def test_failed_native_bind_does_not_replace_or_suppress_publish():
    class BadNative(Native):
        def publisher_identity(self, pointer): raise ValueError('native bind failed')
    observer, endpoint, rows = setup(native_factory=BadNative)
    receipt = invoke(observer, endpoint)
    assert receipt['status'] == 'unknown'
    assert receipt['reason'].startswith('publisher_identity_unavailable')
    assert receipt['publish_returned'] is True
    observer.close()

def test_wrong_fresh_gid_cannot_pass_even_if_trace_handle_matches():
    class WrongGid(Native):
        def publisher_identity(self, pointer): return 222, 'fedcba9876543210' * 2
    observer, endpoint, rows = setup(native_factory=WrongGid)
    receipt = invoke(observer, endpoint)
    assert receipt['status'] == 'unknown'
    assert receipt['reason'] == 'writer_gid_mismatch'
    observer.close()

def test_publish_exception_keeps_failed_receipt_and_propagates():
    observer, endpoint, rows = setup()
    def failing(msg):
        observer.native.callback(222, 333, 5)
        raise RuntimeError('actual publish error')
    endpoint.publish = failing
    with pytest.raises(RuntimeError, match='actual publish error'): invoke(observer, endpoint)
    assert rows[-1]['event'] == 'publish_call_end'
    assert rows[-1]['publish_returned'] is False
    assert rows[-1]['error_type'] == 'RuntimeError'
    observer.close()

def test_other_thread_trace_not_assigned_by_nearest_time():
    observer, endpoint, rows = setup()
    def publish(msg):
        t = threading.Thread(target=lambda: observer.native.callback(222, 333, 5))
        t.start(); t.join()
    endpoint.publish = publish
    receipt = invoke(observer, endpoint)
    assert receipt['status'] == 'unknown'
    assert receipt['reason'] == 'trace_count_not_one'
    assert observer.out_of_scope_traces == 1
    observer.close()

def test_each_identical_send_has_new_scope_not_payload_identity():
    observer, endpoint, rows = setup()
    first = invoke(observer, endpoint); second = invoke(observer, endpoint)
    assert first['call_id'] != second['call_id']
    assert first['trace_ref'] != second['trace_ref']
    assert first['payload_sha256'] == second['payload_sha256']
    # Native repeated timestamps remain observations; join must reject collision.
    assert first['source_timestamp'] == second['source_timestamp']
    observer.close()

def test_payload_modified_during_call_cannot_bind_original_hash():
    observer, endpoint, rows = setup()
    def mutate(msg):
        observer.native.callback(222, 333, 5); msg.data = 'changed'
    endpoint.publish = mutate
    assert invoke(observer, endpoint)['reason'] == 'payload_changed_during_publish'
    observer.close()

def test_unsupported_native_keeps_original_publish_without_trace():
    called=[]
    observer, endpoint, rows = setup(lambda msg:called.append(True), rmw='rmw_cyclonedds_cpp')
    receipt = invoke(observer, endpoint)
    assert called == [True]
    assert receipt['status'] == 'unknown'
    assert receipt['reason'] == 'unsupported_rmw'
    assert observer.native is None
    assert observer.close()['status'] == 'not_registered'

def test_native_load_failure_is_capability_not_fake_observation():
    def fail(callback): raise OSError('library missing')
    called=[]
    observer, endpoint, rows = setup(lambda msg:called.append(True), native_factory=fail)
    receipt = invoke(observer, endpoint)
    assert called == [True]
    assert receipt['reason'].startswith('native_observer_unavailable:OSError')
    assert rows[0]['status'] == 'unavailable'
    observer.close()


def test_constructor_emit_failure_unregisters_before_losing_owner():
    created = []
    def factory(callback):
        native = Native(callback); created.append(native); return native
    def fail(event, **fields): raise OSError('capture unavailable')
    with pytest.raises(OSError, match='capture unavailable'):
        PublishOccurrenceObserver(fail, 'rmw_fastrtps_cpp', native_factory=factory)
    assert created[0].closed


@pytest.mark.parametrize('close_failure', ['returncode', 'exception'])
def test_constructor_cleanup_failure_retains_native_owner(close_failure):
    from r2f.runtime.rmw_publish_observer import _PINNED_FAILED_CLOSE
    created = []
    class FailedClose(Native):
        def close(self):
            if close_failure == 'exception': raise RuntimeError('cannot unregister')
            return 17
    def factory(callback):
        native = FailedClose(callback); created.append(native); return native
    def fail(event, **fields): raise OSError('primary telemetry error')
    with pytest.raises(OSError, match='primary telemetry error'):
        PublishOccurrenceObserver(fail, 'rmw_fastrtps_cpp', native_factory=factory)
    assert created[0] in _PINNED_FAILED_CLOSE
    _PINNED_FAILED_CLOSE.remove(created[0])  # Fake registration only; no native callback.


@pytest.mark.parametrize('event', ['publish_call_begin', 'publish_call_end', 'rmw_publish_trace'])
def test_telemetry_failure_cannot_suppress_original_publish_or_create_occurrence(event):
    observer, endpoint, rows = setup()
    original_emit = observer.emit
    calls = []
    def emit(kind, **fields):
        if kind == event: raise OSError('event sink broken')
        return original_emit(kind, **fields)
    def publish(msg):
        calls.append(msg.data)
        observer.native.callback(222, 333, 5)
    observer.emit = emit
    endpoint.publish = publish
    result = invoke(observer, endpoint)
    assert calls == ['same full data']
    assert result['status'] == 'unknown'
    assert result['publish_returned'] is True
    assert result['source_timestamp'] is None
    observer.close()


def test_end_telemetry_failure_does_not_mask_original_publish_exception():
    observer, endpoint, rows = setup()
    original_emit = observer.emit
    def emit(kind, **fields):
        if kind == 'publish_call_end': raise OSError('cannot write end')
        return original_emit(kind, **fields)
    def publish(msg): raise RuntimeError('actual publish error')
    observer.emit = emit
    endpoint.publish = publish
    with pytest.raises(RuntimeError, match='actual publish error'): invoke(observer, endpoint)
    assert getattr(observer.local, 'scope', None) is None
    observer.close()


def test_mapped_same_soname_override_does_not_pass_disk_pins():
    from r2f.runtime.rmw_publish_observer import _verify_mapped_identity
    with pytest.raises(RuntimeError, match='mapped_library_identity_mismatch:libtracetools'):
        _verify_mapped_identity('100-200 r-xp 0 00:00 0 /tmp/override/libtracetools.so\n')
    _verify_mapped_identity('100-200 r-xp 0 00:00 0 /opt/ros/jazzy/lib/libtracetools.so\n')
