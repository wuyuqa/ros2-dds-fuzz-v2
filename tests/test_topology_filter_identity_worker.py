"""Offline producer/backend contracts; no ROS imports or DDS execution."""
from types import SimpleNamespace, ModuleType
from unittest.mock import patch
import sys

import pytest

from r2f.runtime.topology_worker import (
    _update_filter_identity, _graph_local_gid, RosTopologyBackend, TopologyBackendError,
)

A, B = "01" * 16, "02" * 16


def graph(gid=A, count=1):
    return {"publishers": [], "subscriptions": [
        {"node_name": "reader", "endpoint_gid": gid, "topic_type": "std_msgs/msg/String"} for _ in range(count)]}


class Adapter:
    def attach(self, endpoint):
        pass

    def update(self, expression, parameters, *, field_types, api_event):
        api_event("set_begin")
        api_event("set_failure" if self.fail else "set_success")
        if not self.fail:
            api_event("get_begin")
            api_event("get_success")
        return {"api_status": "failed" if self.fail else "succeeded",
                "applied": not self.fail, "filter_effect_observed": None}

    fail = False


@pytest.mark.parametrize("after,ordinal", [(A, 0), (B, 1)])
def test_producer_order_and_identity(after, ordinal):
    rows = []
    snapshots = iter([graph(), graph(after)])
    with patch("r2f.runtime.filter_backend.ContentFilterAdapter", Adapter):
        token = _update_filter_identity(
            SimpleNamespace(topic_name="/real"), lambda: next(snapshots), "reader", A, 0,
            {"command_id": 7}, lambda event, **kw: rows.append({"event": event, **kw}))
    assert token == (after, ordinal)
    assert [r["event"] for r in rows] == [
        "endpoint_identity_snapshot", *["content_filter_api_call"] * 4,
        "endpoint_identity_snapshot", "content_filter_dynamic_update",
        *(["endpoint_identity_transition"] if ordinal else [])]
    assert rows[0]["message_type"] == rows[5]["message_type"] == "std_msgs/msg/String"
    assert rows[0]["phase"] == "before"
    assert rows[5]["phase"] == "after"
    assert rows[0]["topic"] == rows[5]["topic"] == "/real"
    assert all(r["reader_gid"] == A for r in rows if r["event"] == "content_filter_api_call")
    assert rows[6]["before_gid"] == A and rows[6]["after_gid"] == after
    if ordinal:
        assert rows[-1]["incarnation_after"] == 1


@pytest.mark.parametrize("snapshot", [graph(count=0), graph(count=2), graph("00" * 16), graph("bad"), graph("01" * 24), graph("AB" * 16)])
def test_reject_invalid_graph(snapshot):
    with pytest.raises(TopologyBackendError):
        _graph_local_gid(snapshot, "reader", "reader")


def test_before_mismatch_prevents_api():
    with patch("r2f.runtime.filter_backend.ContentFilterAdapter", side_effect=AssertionError("API forbidden")):
        with pytest.raises(TopologyBackendError, match="before_mismatch"):
            _update_filter_identity(SimpleNamespace(topic_name="/real"), lambda: graph(B),
                                    "reader", A, 0, {"command_id": 1}, lambda *a, **k: None)


def test_failed_set_records_after_but_no_success():
    rows = []
    class Failed(Adapter):
        fail = True
    with patch("r2f.runtime.filter_backend.ContentFilterAdapter", Failed):
        _update_filter_identity(SimpleNamespace(topic_name="/real"), lambda: graph(),
                                "reader", A, 0, {"command_id": 1},
                                lambda event, **kw: rows.append({"event": event, **kw}))
    assert [r.get("api_stage") for r in rows if "api_stage" in r] == ["set_begin", "set_failure"]
    assert rows[-1]["filter_receipt"]["applied"] is False
    assert rows[-2]["phase"] == "after"


def test_after_poll_never_spins_and_rejects_unresolved():
    snapshots = iter([graph(), graph(count=0)])
    with patch("r2f.runtime.filter_backend.ContentFilterAdapter", Adapter):
        with pytest.raises(TopologyBackendError, match="not_unique"):
            _update_filter_identity(SimpleNamespace(topic_name="/real"), lambda: next(snapshots),
                                    "reader", A, 0, {"command_id": 1}, lambda *a, **k: None,
                                    timeout_sec=0)


@pytest.mark.parametrize("reject", [False, True])
def test_backend_changes_handle_only_after_validation(reject):
    backend = object.__new__(RosTopologyBackend)
    backend._batch = lambda *a, **k: 7
    row = {"event": "content_filter_dynamic_update", "command_id": 7, "filter_receipt": {}}
    process = SimpleNamespace(events=[row], metadata={"endpoint_gid": A})
    handle = {"process": process, "endpoint_gid": A, "incarnation_ordinal": 0}
    module = ModuleType("r2f.runtime.endpoint_incarnation")
    def validate(events, current, command_id):
        assert events is process.events and command_id == 7
        assert current["endpoint_gid"] == A
        if reject:
            raise ValueError("rejected")
        return {"endpoint_gid": B, "incarnation_ordinal": 1, "identity_valid_after_ns": 123,
                "identity_transition_ref": 8}
    module.validate_filter_identity_update = validate
    with patch.dict(sys.modules, {module.__name__: module}):
        if reject:
            before_handle, before_metadata = dict(handle), dict(process.metadata)
            with pytest.raises(TopologyBackendError, match="filter_identity_validation_failed") as error:
                backend.update_filter(handle, "", [])
            assert isinstance(error.value.__cause__, ValueError)
            assert handle == before_handle and process.metadata == before_metadata
            assert handle["endpoint_gid"] == process.metadata["endpoint_gid"] == A
        else:
            backend.update_filter(handle, "", [])
            assert handle["endpoint_gid"] == process.metadata["endpoint_gid"] == B
            assert handle["incarnation_ordinal"] == 1
            assert handle["identity_valid_after_ns"] == process.metadata["identity_valid_after_ns"] == 123
            assert "identity_transition_ref" not in handle
            assert "identity_transition_ref" not in process.metadata


@pytest.mark.parametrize("after", [A, B])
@pytest.mark.parametrize("failed", [False, True])
def test_real_resolver_accepts_producer_and_backend(after, failed):
    import time
    from r2f.runtime.endpoint_incarnation import validate_filter_identity_update
    lineage = dict(logical_id="r0", generation=1, role="reader", pid=100, pgid=100,
                   process_start_ticks=5, endpoint_generation="original-process",
                   run_id="offline-fixture", domain_id=191, node_name="reader",
                   rmw_identifier="offline-fixture", monotonic_clock_identity={"boot_id": "fixture"})
    origin = dict(event="endpoint_created", endpoint_gid=A, topic="/real",
                  message_type="std_msgs/msg/String", created_monotonic_ns=time.monotonic_ns(),
                  **lineage)
    events = [origin]
    snapshots = iter([graph(), graph(after)])
    def emit(event, **kw):
        events.append(dict(event=event, monotonic_ns=time.monotonic_ns(), **lineage, **kw))
    class FixtureAdapter(Adapter):
        fail = failed
    with patch("r2f.runtime.filter_backend.ContentFilterAdapter", FixtureAdapter):
        gid, ordinal = _update_filter_identity(
            SimpleNamespace(topic_name="/real"), lambda: next(snapshots), "reader", A, 0,
            {"command_id": 7}, emit)
    emit("command_done", command="update_filter", command_id=7, endpoint_gid=gid,
         reader_gid=gid, incarnation_ordinal=ordinal)
    token = validate_filter_identity_update(events, origin, 7)
    assert token["endpoint_gid"] == after
    assert token["incarnation_ordinal"] == int(after != A)
    backend = object.__new__(RosTopologyBackend)
    backend._batch = lambda *a, **kw: 7
    # A real process object is deliberately not deepcopy-able by the resolver.
    class Process:
        def __deepcopy__(self, memo):
            raise AssertionError("do not deepcopy live process")
    process = Process()
    process.events, process.metadata = events, {"endpoint_gid": A}
    handle = {**origin, "process": process}
    backend.update_filter(handle, "", [])
    assert handle["endpoint_gid"] == process.metadata["endpoint_gid"] == after
    assert handle["identity_valid_after_ns"] == process.metadata["identity_valid_after_ns"] == events[-1]["monotonic_ns"]
    assert "identity_transition_ref" not in handle
    assert "identity_transition_ref" not in process.metadata
    assert origin["endpoint_gid"] == A


def test_snapshot_request_uses_authenticated_metadata_not_creation():
    from io import BytesIO
    from r2f.runtime.topology_worker import _EndpointProcess
    process = object.__new__(_EndpointProcess)
    process.config = {}
    process.process = SimpleNamespace(poll=lambda: None, stdin=BytesIO())
    process.command_serial, process.role = 0, "reader"
    process.events = [{"event": "endpoint_created", "endpoint_gid": A}]
    process.metadata = {"endpoint_gid": B}
    rows = []
    process.emit = lambda event, **kw: rows.append({"event": event, **kw})
    process.command("snapshot")
    assert rows[0]["endpoint_gid"] == B
    assert process.events[0]["endpoint_gid"] == A


def test_after_graph_poll_waits_without_dispatching_callbacks():
    snapshots = iter([graph(), graph(count=0), graph(B)])
    rows = []
    with patch("r2f.runtime.filter_backend.ContentFilterAdapter", Adapter), \
            patch("r2f.runtime.topology_worker.time.sleep") as sleep:
        gid, ordinal = _update_filter_identity(
            SimpleNamespace(topic_name="/real"), lambda: next(snapshots), "reader", A, 0,
            {"command_id": 7}, lambda event, **kw: rows.append({"event": event, **kw}))
    sleep.assert_called_once_with(0.01)
    assert (gid, ordinal) == (B, 1)
    assert len([r for r in rows if r.get("phase") == "after"]) == 1


def test_real_resolver_value_error_becomes_backend_error_without_identity_mutation():
    backend = object.__new__(RosTopologyBackend)
    backend._batch = lambda *args, **kwargs: 7
    process = SimpleNamespace(events=[], metadata={"endpoint_gid": A, "incarnation_ordinal": 0})
    handle = {"process": process, "endpoint_gid": A, "incarnation_ordinal": 0,
              "role": "reader", "logical_id": "r0", "generation": 1}
    before_handle, before_metadata = dict(handle), dict(process.metadata)
    with pytest.raises(TopologyBackendError, match="filter_identity_validation_failed") as error:
        backend.update_filter(handle, "", [])
    assert isinstance(error.value.__cause__, ValueError)
    assert str(error.value.__cause__).startswith("endpoint_incarnation:")
    assert handle == before_handle and process.metadata == before_metadata
