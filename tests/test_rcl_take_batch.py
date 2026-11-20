"""Synthetic ctypes boundary only. Never load native libraries or start ROS."""
from types import SimpleNamespace
from unittest.mock import Mock
from r2f.runtime import rcl_take_bridge as b
from test_rcl_take_bridge import Handle, Native, String


def test_batch_calls_full_verification_twice_not_per_sample(monkeypatch):
    h = Handle()
    native = Native(h)
    native.abi = {"pin_sha256":"synthetic", "manifest_reference":{"fixture":True},
                  "vendor_identity":{"fixture":True}, "mapped_libraries":[]}
    verify = Mock()
    monkeypatch.setattr(b.RclTakeBridge, "_revalidate_instrumentation", verify)
    bridge = b.RclTakeBridge(instrumentation={"fixture":True}, _bindings=native, _string_type=String)
    verify.reset_mock()
    sub = SimpleNamespace(handle=h, msg_type=String, raw=False)
    result = bridge.take_batch(sub, max_samples=3, max_duration_ns=50_000_000)
    assert len(result) == 3
    assert verify.call_count == 2
    assert h.enters == h.exits == 3
    assert native.calls.count("fini") == 3


import asyncio
import threading
import pytest
from test_rcl_take_bridge import BaseExecutor, CallbackType, InvalidHandle, await_or_execute
from test_rcl_take_bridge_instrumentation import installation, make_bridge


class Clock:
    def __init__(self):
        self.now = 1_000_000_000
    def monotonic_ns(self):
        return self.now


@pytest.fixture
def rig(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(b, 'time', clock)
    h = Handle()
    native = Native(h)
    native.abi = {'pin_sha256':'synthetic', 'manifest_reference':{'fixture':True},
                  'vendor_identity':{'fixture':True}, 'mapped_libraries':[]}
    verify = Mock()
    monkeypatch.setattr(b.RclTakeBridge, '_revalidate_instrumentation', verify)
    bridge = b.RclTakeBridge(instrumentation={'fixture':True}, _bindings=native, _string_type=String)
    verify.reset_mock()
    sub = SimpleNamespace(handle=h, msg_type=String, raw=False,
                          _callback_type=CallbackType.WithMessageInfo, callback=Mock())
    return SimpleNamespace(bridge=bridge, sub=sub, native=native, verify=verify, clock=clock)


def executor(rig):
    cls = b._executor_class(BaseExecutor, await_or_execute, InvalidHandle, CallbackType)
    return cls(context=None, take_bridge=rig.bridge)


def test_defaults_count_and_fairness_allow_first_take_after_expensive_validation(rig):
    def slow_validation(): rig.clock.now += 900_000_000
    rig.verify.side_effect = slow_validation
    samples = rig.bridge.take_batch(rig.sub)
    assert len(samples) == 64
    assert rig.verify.call_count == 2
    assert samples[0][1]['rcl_take_receipt']['batch_validation']['max_samples'] == 64
    assert len([x for x in rig.native.calls if isinstance(x, tuple)]) == 64


def test_data_budget_ends_between_samples_and_excludes_verification(rig):
    rig.verify.side_effect = lambda: setattr(rig.clock, 'now', rig.clock.now + 900_000_000)
    rig.native.take_hook = lambda: setattr(rig.clock, 'now', rig.clock.now + 2)
    samples = rig.bridge.take_batch(rig.sub, max_duration_ns=5)
    assert len(samples) == 3  # starts at 0,2,4; no new take after time6
    assert rig.verify.call_count == 2
    proof = samples[-1][1]['rcl_take_receipt']['batch_validation']
    assert proof['drain_finished_ns'] - proof['drain_started_ns'] == 6
    assert proof['before_validation_finished_ns'] - proof['before_validation_started_ns'] == 900_000_000
    assert proof['after_validation_finished_ns'] - proof['after_validation_started_ns'] == 900_000_000


@pytest.mark.parametrize('count,budget', [(0,1),(65,1),(True,1),(1,0),(1,50_000_001),(1,True)])
def test_invalid_batch_budgets_never_consume(rig, count, budget):
    with pytest.raises(b.BridgeError, match='invalid_take_batch_budget'):
        rig.bridge.take_batch(rig.sub, max_samples=count, max_duration_ns=budget)
    assert not rig.native.calls
    rig.verify.assert_not_called()


def test_empty_queue_still_verifies_both_sides_and_finalizes(rig):
    rig.native.code = 401
    assert rig.bridge.take_batch(rig.sub) == []
    assert rig.verify.call_count == 2
    assert rig.native.calls.count('fini') == rig.native.calls.count('reset') == 1
    assert rig.sub.callback.call_count == 0


def test_sample_error_discards_prior_results_verifies_and_never_callbacks(rig):
    attempts = []
    def hook():
        attempts.append(1)
        if len(attempts) == 2: raise RuntimeError('second take error')
    rig.native.take_hook = hook
    with pytest.raises(RuntimeError, match='second take error'):
        executor(rig)._take_subscription(rig.sub)
    assert len(attempts) == 2
    assert rig.native.calls.count('fini') == 2
    assert rig.verify.call_count == 2
    rig.sub.callback.assert_not_called()
    assert not rig.bridge._lock.locked()


def test_post_verification_failure_discards_all_and_releases_owner(rig):
    def hook():
        if len([x for x in rig.native.calls if isinstance(x, tuple)]) == 3:
            rig.native.code = 401
    rig.native.take_hook = hook
    rig.verify.side_effect = [None, b.BridgeUnavailable('manifest_changed')]
    with pytest.raises(b.BridgeUnavailable, match='manifest_changed'):
        executor(rig)._take_subscription(rig.sub)
    assert rig.verify.call_count == 2
    assert rig.native.calls.count('fini') == 3
    rig.sub.callback.assert_not_called()
    assert not rig.bridge._lock.locked()
    rig.bridge.close()


def test_post_verification_also_runs_on_invalid_handle_after_partial_take(rig):
    rig.native.take_hook = lambda: setattr(rig.sub.handle, 'destroyed', True)
    assert executor(rig)._take_subscription(rig.sub) is None
    assert rig.verify.call_count == 2
    assert rig.native.calls.count('fini') == 1
    rig.sub.callback.assert_not_called()
    assert not rig.bridge._lock.locked()


def test_original_take_error_kept_when_post_validation_also_fails(rig):
    error = ValueError('native failure')
    rig.native.raise_take = error
    rig.verify.side_effect = [None, b.BridgeUnavailable('post failure')]
    with pytest.raises(b.BridgeError, match='take_batch_and_post_validation_failed:post failure') as caught:
        rig.bridge.take_batch(rig.sub)
    assert caught.value.__cause__ is error
    assert str(caught.value.post_validation_error) == 'post failure'
    assert rig.verify.call_count == 2
    assert rig.native.calls.count('fini') == 1


def test_batch_ownership_rejects_competing_take_batch_and_waits_close(rig):
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    results, errors = [], []
    def blocking():
        entered.set()
        if not release.wait(3): raise RuntimeError('test timeout')
    rig.native.take_hook = blocking
    def drain():
        try: results.extend(rig.bridge.take_batch(rig.sub, max_samples=1))
        except BaseException as e: errors.append(e)
    thread = threading.Thread(target=drain)
    thread.start()
    assert entered.wait(2)
    try:
        for op in (rig.bridge.take, rig.bridge.take_batch):
            with pytest.raises(b.BridgeError, match='concurrent_or_reentrant'):
                op(rig.sub)
        waiter = threading.Thread(target=lambda: (rig.bridge.close(), closed.set()))
        waiter.start()
        assert not closed.wait(0.02)
    finally:
        release.set()
        thread.join(3)
    waiter.join(3)
    assert not errors and len(results) == 1 and closed.is_set()
    with pytest.raises(b.BridgeError, match='bridge_closed'):
        rig.bridge.take_batch(rig.sub)


def test_each_sample_fresh_handle_none_enter_pointer_buffers_metadata_and_order(rig):
    handles = []
    class NoneHandle(Handle):
        def __enter__(self):
            super().__enter__()
            return None
    class Subscription:
        msg_type = String
        raw = False
        @property
        def handle(self):
            h = NoneHandle(0x1000 + 16 * len(handles))
            handles.append(h)
            rig.native.handle = h
            return h
    def data():
        ordinal = len(handles)
        rig.native.payload = ('sample%d\x00正文' % ordinal).encode()
        rig.native.pubseq = ordinal + 100
        rig.clock.now += 1
    rig.native.take_hook = data
    results = rig.bridge.take_batch(Subscription(), max_samples=3)
    assert [m.data for m, _ in results] == ['sample1\x00正文','sample2\x00正文','sample3\x00正文']
    assert [i['publication_sequence_number'] for _, i in results] == [101,102,103]
    assert [i['rcl_take_receipt']['rcl_subscription_pointer'] for _, i in results] == ['0x1000','0x1010','0x1020']
    assert all(h.enters == h.exits == 1 for h in handles)
    receipts = [i['rcl_take_receipt'] for _, i in results]
    assert [r['take_index'] for r in receipts] == [1,2,3]
    assert [r['batch_ordinal'] for r in receipts] == [1,2,3]
    assert len({r['batch_validation']['batch_id'] for r in receipts}) == 1
    for r in receipts:
        p = r['batch_validation']
        assert p['before_validation_finished_ns'] <= r['take_begin_monotonic_ns'] <= r['take_return_monotonic_ns'] <= p['after_validation_started_ns']
        assert r['instrumentation']['validation_scope'] == 'batch_before_after'
        assert r['candidate_eligible'] is False
    receipts[0]['batch_validation']['before_abi']['manifest_reference']['fixture'] = False
    assert receipts[1]['batch_validation']['before_abi']['manifest_reference']['fixture'] is True
    assert rig.bridge.instrumentation['fixture'] is True
    assert rig.native.calls.count('fini') == 3


@pytest.mark.parametrize('kind', [CallbackType.MessageOnly, CallbackType.WithMessageInfo])
def test_instrumentation_executor_awaits_order_and_finishes_validation_before_callbacks(rig, kind):
    counter = []
    def hook():
        counter.append(1)
        rig.native.payload = ('m%d' % len(counter)).encode()
        if len(counter) == 4: rig.native.code = 401
    rig.native.take_hook = hook
    received = []
    rig.sub._callback_type = kind
    async def callback(*args):
        assert rig.verify.call_count == 2
        received.append(args[0].data)
        await asyncio.sleep(0)
    rig.sub.callback = callback
    work = executor(rig)._take_subscription(rig.sub)
    assert received == [] and len(counter) == 4
    asyncio.run(work())
    assert received == ['m1','m2','m3']
    assert len(counter) == 4  # no take/retry from callbacks


def test_legacy_blackbox_and_fake_bridge_without_batch_use_exact_single_path(rig):
    old = SimpleNamespace(instrumentation={'synthetic':True}, take=Mock(return_value=(String(), {})))
    rig.bridge = old
    work = executor(rig)._take_subscription(rig.sub)
    asyncio.run(work())
    old.take.assert_called_once_with(rig.sub)
    old.instrumentation = None
    old.take_batch = Mock(side_effect=AssertionError('blackbox must not batch'))
    executor(rig)._take_subscription(rig.sub)
    old.take_batch.assert_not_called()
    assert old.take.call_count == 2


def test_legacy_take_still_validates_per_call_with_no_batch_receipt(rig):
    message, info = rig.bridge.take(rig.sub)
    assert message.data == 'original0\x00same String.data'
    assert rig.verify.call_count == 2
    assert 'batch_validation' not in info['rcl_take_receipt']
    assert 'validation_scope' not in info['rcl_take_receipt']['instrumentation']


@pytest.mark.parametrize('where', ['before','during'])
@pytest.mark.parametrize('fault', ['manifest','vendor','maps'])
def test_real_file_and_mapping_checks_reject_drift_no_callbacks(installation, monkeypatch, where, fault):
    bridge, sub, native = make_bridge(installation)
    sub._callback_type = CallbackType.WithMessageInfo
    sub.callback = Mock()
    def drift():
        if fault == 'manifest': installation.manifest_path.write_text('{}')
        elif fault == 'vendor': installation.vendor.write_bytes(b'changed actual synthetic bytes')
        else: installation.maps['paths'][-1] = installation.prefix / 'lib/libfastrtps.so'
    if where == 'before': drift()
    else: native.take_hook = drift
    cls = b._executor_class(BaseExecutor, await_or_execute, InvalidHandle, CallbackType)
    with pytest.raises(b.BridgeUnavailable):
        cls(context=None, take_bridge=bridge)._take_subscription(sub)
    attempts = len([c for c in native.calls if isinstance(c, tuple)])
    assert attempts == 0 if where == 'before' else 1 <= attempts <= 64
    sub.callback.assert_not_called()
    assert not bridge._lock.locked()


def test_complete_batch_pin_evidence_and_fresh_full_validation_each_batch(installation, monkeypatch):
    bridge, sub, native = make_bridge(installation)
    verify = Mock(wraps=bridge._revalidate_instrumentation)
    monkeypatch.setattr(bridge, '_revalidate_instrumentation', verify)
    first = bridge.take_batch(sub, max_samples=1)[0][1]['rcl_take_receipt']
    second = bridge.take_batch(sub, max_samples=1)[0][1]['rcl_take_receipt']
    assert verify.call_count == 4
    assert first['batch_validation']['batch_id'] != second['batch_validation']['batch_id']
    for receipt in (first, second):
        p = receipt['batch_validation']
        assert p['manifest_reference'] == installation.reference
        for key in ('before_abi','after_abi'):
            assert p[key]['files']['lib/libfastrtps.so'] == installation.manifest['binaries']['vendor']['sha256']
            assert p[key]['manifest_reference'] == installation.reference
            assert p[key]['mapped_libraries'] and p[key]['vendor_identity']['identity_verified']
            assert p[key]['file_paths']['lib/libfastrtps.so'] == str(installation.vendor)


def test_empty_queue_post_verification_failure_is_not_silently_success(rig):
    rig.native.code = 401
    rig.verify.side_effect = [None, b.BridgeUnavailable('empty post mapping changed')]
    with pytest.raises(b.BridgeUnavailable, match='empty post mapping changed'):
        executor(rig)._take_subscription(rig.sub)
    assert rig.verify.call_count == 2
    assert rig.native.calls.count('fini') == 1
    rig.sub.callback.assert_not_called()


@pytest.mark.parametrize('fault', ['return_code','bad_pointer','init','decode','fini'])
def test_batch_failure_paths_finalize_verify_and_do_not_callback(rig, fault):
    if fault == 'return_code': rig.native.code = 8
    elif fault == 'bad_pointer': rig.sub.handle._pointer = 0
    elif fault == 'init': rig.native.init_result = False
    elif fault == 'decode': rig.native.payload = b'\xff'
    else: rig.native.fini = Mock(side_effect=RuntimeError('fini failed'))
    with pytest.raises((b.BridgeError, UnicodeDecodeError, RuntimeError)):
        executor(rig)._take_subscription(rig.sub)
    assert rig.verify.call_count == 2
    rig.sub.callback.assert_not_called()
    assert not rig.bridge._lock.locked()
    assert rig.sub.handle.enters == rig.sub.handle.exits == 1
    if fault == 'fini': rig.native.fini.assert_called_once()
    elif fault != 'bad_pointer': assert rig.native.calls.count('fini') == 1


def test_invalid_handle_cannot_hide_failed_batch_post_validation(rig):
    # One copied sample precedes a destroyed handle; the final mandatory proof
    # fails too. InvalidHandle's ordinary executor path must not swallow it.
    rig.native.take_hook = lambda: setattr(rig.sub.handle, 'destroyed', True)
    validation = b.BridgeUnavailable('mandatory post proof changed')
    rig.verify.side_effect = [None, validation]
    with pytest.raises(b.BridgeError, match='take_batch_and_post_validation_failed') as caught:
        executor(rig)._take_subscription(rig.sub)
    assert isinstance(caught.value.__cause__, InvalidHandle)
    assert caught.value.post_validation_error is validation
    assert rig.verify.call_count == 2
    assert rig.native.calls.count('fini') == 1
    rig.sub.callback.assert_not_called()
    assert not rig.bridge._lock.locked()
