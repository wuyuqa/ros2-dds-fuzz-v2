"""Offline retirement: real epoll and pipes, fake children; never launch ROS."""
from io import BytesIO
import json
import os
import selectors
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from r2f.runtime import topology_worker as tw


@pytest.fixture
def workers(tmp_path, monkeypatch):
    """Own all fixture FDs, and forbid real child launches/signals."""
    monkeypatch.setattr(tw.subprocess, "Popen", Mock(side_effect=AssertionError("NO CHILD LAUNCH")))
    monkeypatch.setattr(tw.os, "killpg", Mock(side_effect=AssertionError("NO HOST SIGNAL")))
    allocated = []

    def make(pid, *, generation=1, alive=False, emitted=None):
        events = emitted if emitted is not None else []
        read_fd, write_fd = os.pipe()
        stdout = os.fdopen(read_fd, "rb", buffering=0)
        writer = os.fdopen(write_fd, "wb", buffering=0)
        os.set_blocking(read_fd, False)
        child = SimpleNamespace(pid=pid, stdout=stdout, stdin=BytesIO(),
                                returncode=None if alive else 0)
        child.poll = lambda: child.returncode
        child.wait = Mock(side_effect=subprocess.TimeoutExpired("fixture-child", 2))
        process = tw._EndpointProcess(
            {"logical_id": "w0", "generation": generation}, {}, tmp_path,
            "writer", lambda event, **fields: events.append((event, fields)))
        process.process = child
        process.stderr = BytesIO()
        selector = selectors.EpollSelector()
        selector.register(stdout, selectors.EVENT_READ)
        process.selector = selector
        process.metadata = {"role": "writer", "pid": pid, "pgid": pid,
                            "logical_id": "w0", "generation": generation}
        allocated.append((selector, stdout, writer, child.stdin, process.stderr))
        return process, writer, events

    yield make
    for handles in allocated:
        for handle in handles:
            handle.close()


def test_real_epoll_close_then_pump_drains_before_retirement(workers):
    process, writer, emitted = workers(1001)
    selector = process.selector
    writer.write(b'{"event":"command_done","command_id":7}\n')
    assert process.close()["closed"] is True
    # Before the fix this raises the exact P05 closed-epoll exception.
    process.pump(0)
    process.pump(0)
    assert process.selector is None
    with pytest.raises(ValueError, match="closed epoll"):
        selector.select(0)
    assert [event for event, _ in emitted] == ["command_done", "process_exit"]
    assert process.events[0]["command_id"] == 7
    assert process.metadata["returncode"] == 0
    assert process.process.stdout.closed


def test_close_twice_emits_one_exit_and_preserves_history(workers):
    process, writer, emitted = workers(1002)
    writer.write(b'{"event":"worker_stopping"}\n')
    events, metadata, child = process.events, process.metadata, process.process
    first = process.close()
    saved_events, saved_metadata = list(events), dict(metadata)
    second = process.close()
    process.pump(0)
    assert first["closed"] is second["closed"] is True
    assert second["already"] is True
    assert [event for event, _ in emitted] == ["worker_stopping", "process_exit"]
    assert process.events is events and events == saved_events
    assert process.metadata is metadata and metadata == saved_metadata
    assert process.process is child


def test_destroy_recreate_backend_keeps_retired_history_and_services_new_worker(
        workers, tmp_path, monkeypatch):
    emitted = []
    old, old_writer, _ = workers(1003, emitted=emitted)
    new, new_writer, _ = workers(1004, generation=2, emitted=emitted)
    for process, writer in ((old, old_writer), (new, new_writer)):
        writer.write((json.dumps({"event": "endpoint_created", "pid": process.process.pid,
                                 "endpoint_gid": f"{process.process.pid:032x}",
                                 "endpoint_generation": f"token-{process.process.pid}",
                                 "process_start_ticks": process.process.pid * 10,
                                 "logical_id": "w0", "generation": process.config["generation"]})
                      + "\n").encode())
        monkeypatch.setattr(process, "start", lambda: None)  # no Popen
    pending = iter((old, new))
    monkeypatch.setattr(tw, "_EndpointProcess", lambda *args: next(pending))
    monkeypatch.setattr(tw, "endpoint_expressibility", lambda *args: {
        "public_qos": {}, "initial_content_filter": None, "binding": {}})
    backend = tw.RosTopologyBackend(
        {"run_id": "offline-retirement", "domain_id": 192, "rmw": "rmw_cyclonedds_cpp"},
        tmp_path, lambda event, **fields: emitted.append((event, fields)))
    backend.environment = {}  # fixture only; no prepare/lease/preflight
    monkeypatch.setattr(backend, "_start_ticks", lambda pid: pid * 10)
    spec = {"logical_id": "w0", "role": "writer", "topic": "/fixture",
            "type_name": "std_msgs/msg/String", "qos": {}}
    old_handle = backend.create_endpoint(spec, 1, "writer")
    assert backend.destroy_endpoint(old_handle)["cleanup_status"] == "closed"
    old_events, old_metadata = list(old.events), dict(old.metadata)
    new_handle = backend.create_endpoint(spec, 2, "writer")
    new_writer.write(b'{"event":"command_done","command_id":42}\n')
    backend.pump(0)
    assert old.selector is None and new.selector is not None
    assert new.has("command_done", command_id=42)
    assert backend.processes == [old, new]  # preserve history, do not prune
    assert old.events == old_events and old.metadata == old_metadata
    assert not old_handle["alive"] and new_handle["alive"]
    assert old_handle["generation"] == 1 and new_handle["generation"] == 2
    assert backend.handles["w0"] is new_handle
    result = backend.close()
    assert result["closed"] and len(result["workers"]) == 2
    assert result["survivors"] == result["errors"] == []
    assert [fields["pid"] for event, fields in emitted if event == "process_exit"] == [1003, 1004]
    assert backend.close()["already_closed"] is True


@pytest.mark.parametrize("error_type", [ValueError, OSError, RuntimeError])
def test_active_selector_failure_propagates_from_worker_and_backend(
        workers, tmp_path, error_type):
    process, _, emitted = workers(1005, alive=True)
    failure = error_type("active selector failure")
    selector = Mock()
    selector.select.side_effect = failure
    process.selector = selector
    backend = tw.RosTopologyBackend({}, tmp_path)
    backend.processes = [process]
    with pytest.raises(error_type, match="active selector failure") as caught:
        process.pump(0)
    assert caught.value is failure
    with pytest.raises(error_type, match="active selector failure") as caught:
        backend.pump(0)
    assert caught.value is failure
    assert process.selector is selector and "returncode" not in process.metadata
    assert emitted == []


def test_unexpected_closed_active_epoll_is_not_silently_retired(workers):
    process, _, emitted = workers(1006, alive=True)
    selector = process.selector
    selector.close()  # not a successful owned close/retirement
    with pytest.raises(ValueError, match="closed epoll"):
        process.pump(0)
    assert process.selector is selector
    assert emitted == [] and "returncode" not in process.metadata


def test_selector_close_failure_does_not_retire_or_emit_exit(workers):
    process, _, emitted = workers(1007)
    actual = process.selector
    selector = Mock(wraps=actual)
    selector.close.side_effect = OSError("selector close failed")
    process.selector = selector
    with pytest.raises(OSError, match="selector close failed"):
        process.close()
    assert process.selector is selector
    assert "returncode" not in process.metadata
    assert process.process.stdout.closed is False
    assert emitted == []


def test_alive_close_timeout_keeps_selector_and_never_claims_exit(
        workers, tmp_path, monkeypatch):
    process, _, emitted = workers(1008, alive=True)
    selector = process.selector
    signals = Mock()
    monkeypatch.setattr(tw.os, "killpg", signals)  # record only; never signal OS
    with pytest.raises(subprocess.TimeoutExpired):
        process.close(timeout=0)
    assert [args.args[1] for args in signals.call_args_list] == [tw.signal.SIGTERM, tw.signal.SIGKILL]
    assert process.selector is selector and process.process.poll() is None
    assert process.process.stdout.closed is False
    assert "returncode" not in process.metadata
    assert not any(event == "process_exit" for event, _ in emitted)
    backend = tw.RosTopologyBackend({}, tmp_path)
    backend.processes = [process]
    monkeypatch.setattr(process, "close", lambda: (_ for _ in ()).throw(
        subprocess.TimeoutExpired("fixture-child", 2)))
    result = backend.close()
    assert result["closed"] is False and backend.closed is False
    assert result["survivors"] == [1008]
    assert result["errors"][0]["error"] == "TimeoutExpired"
