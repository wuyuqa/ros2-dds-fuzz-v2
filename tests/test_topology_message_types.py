"""CP1 contracts at public input/carrier seams; fixtures never qualify DDS."""
import json
from pathlib import Path

import pytest

from r2f.runtime.topology_pair import run_topology, topology_capabilities
from test_topology_pair import CFG, action, endpoint, program

ARRAY_TYPE = "std_msgs/msg/UInt8MultiArray"



def worker_config(run_id, *, message_type=ARRAY_TYPE, role="writer", rmw="rmw_cyclonedds_cpp"):
    from r2f.runtime.topology_worker import endpoint_expressibility
    from test_topology_worker import spec
    return {"run_id": run_id, "domain_id": 173, "rmw": rmw,
            "logical_id": "w0" if role == "writer" else "r0", "generation": 1,
            "topic": "/r2f/type", "type_name": message_type,
            "qos": endpoint_expressibility(spec(type_name=message_type), {})["public_qos"]}


def test_array_type_binds_schema_through_topology_carrier(tmp_path):
    prog = program(
        [endpoint("w0", "writer", type_name=ARRAY_TYPE),
         endpoint("r0", "reader", type_name=ARRAY_TYPE)],
        [action("cw", "create", "w0", 1), action("cr", "create", "r0", 1),
         action("p", "publish_batch", "w0", 1, count=2, start_sequence=1)])
    result = run_topology(prog, CFG, tmp_path)
    assert result["status"] == "executed", result["reason"]
    bindings = result["input_receipts"]["endpoints"]
    assert all(row["binding_status"] == "expressible" for row in bindings)
    for row in bindings:
        support = row["binding"]["message_type_support"]
        assert support["type_name"] == ARRAY_TYPE
        assert support["field_types"] == {
            "layout": "std_msgs/MultiArrayLayout", "data": "sequence<uint8>"}
        assert support["payload_encoding"] == "utf8_json_uint8_sequence"
        assert support["execution_evidence"] is False
    raw = [json.loads(line) for line in Path(result["raw_path"]).read_text().splitlines()]
    created = [row for row in raw if row["event"] == "endpoint_created"]
    assert len(created) == 2
    assert all(row["message_type"] == ARRAY_TYPE for row in created)
    sent = [row for row in raw if row["event"] == "send"]
    assert sent and all(row["string_data"] is None for row in sent)
    assert all("serialization_receipt" not in row for row in sent)
    assert result["real_execution"] is False
    assert ARRAY_TYPE in topology_capabilities()["supported_message_types"]


@pytest.mark.parametrize("message_type", ["std_msgs/msg/String", ARRAY_TYPE])
def test_public_worker_constructs_and_serializes_message(monkeypatch, message_type):
    """ROS is an external ABI fixture here, never a real execution receipt."""
    import sys
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture

    cfg = worker_config("offline-type-fixture", message_type=message_type)
    commands = [{"command": "publish_batch", "command_id": 1,
                 "sequences": [7], "phase": "typed", "payload": "字节"},
                {"command": "stop", "command_id": 2}]
    with ros_fixture(monkeypatch, commands) as (_, _, stream):
        actual_publishes = []
        node = sys.modules["rclpy.node"].Node
        original_create = node.create_publisher
        def create(self, cls, topic, qos, **kwargs):
            actual_publishes.append(cls.__name__)
            publisher = original_create(self, cls, topic, qos, **kwargs)
            original_publish = publisher.publish
            def publish(message):
                actual_publishes.append(message)
                return original_publish(message)
            publisher.publish = publish
            return publisher
        monkeypatch.setattr(node, "create_publisher", create)
        monkeypatch.setattr(sys, "argv", ["worker", json.dumps(cfg), "writer"])
        rc = worker.worker_main()
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert rc == 0, rows
    assert actual_publishes[0] == message_type.rsplit("/", 1)[-1]
    assert type(actual_publishes[1]).__name__ == actual_publishes[0]
    support = next(row for row in rows if row["event"] == "topology_message_type_support")
    assert support["status"] == "observed"
    assert support["observed_field_types"]["data"] == (
        "sequence<uint8>" if message_type == ARRAY_TYPE else "string")
    sent = next(row for row in rows if row["event"] == "send")
    assert sent["message_type"] == message_type
    assert sent["string_data"] == (None if message_type == ARRAY_TYPE else sent["payload_text"])
    body = json.loads(sent["payload_text"])
    assert (body["run_id"], body["send_sequence"], body["declared_payload"]) == (
        "offline-type-fixture", 7, "字节")
    serialized = sent["serialization_receipt"]
    assert serialized["api"] == "rclpy.serialization.serialize_message"
    assert serialized["roundtrip_equal"] is True
    assert serialized["capture_stage"] == "pre_publish"
    assert serialized["wire_capture"] is False
    assert bytes.fromhex(serialized["serialized_hex"]).startswith(b"fixture:")


def test_native_publish_observer_keeps_array_bytes_and_occurrence():
    from types import SimpleNamespace
    from test_rmw_publish_observer import setup, invoke
    observer, endpoint, rows = setup()
    try:
        result = invoke(observer, endpoint, SimpleNamespace(data=[65, 66, 67]))
        assert result["status"] == "observed"
        # SHA-256 of the independently specified bytes b'ABC'.
        assert result["payload_sha256"] == "b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78"
        assert result["publish_returned"] is True
    finally:
        observer.close()


def test_array_reader_uses_stock_take_and_decodes_correlated_envelope(monkeypatch):
    import sys
    from types import ModuleType
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture

    cfg = worker_config("offline-reader", role="reader", rmw="rmw_fastrtps_cpp")
    with ros_fixture(monkeypatch, [{"command": "stop", "command_id": 1}]) as (_, entities, stream):
        monkeypatch.setattr(sys.modules["rclpy"], "get_rmw_implementation_identifier",
                            lambda: "rmw_fastrtps_cpp")
        bridge = ModuleType("r2f.runtime.rcl_take_bridge")
        def forbidden_bridge(**kwargs):
            raise AssertionError("String-only take bridge selected for array")
        bridge.make_full_info_executor = forbidden_bridge
        bridge.BridgeUnavailable = RuntimeError
        monkeypatch.setitem(sys.modules, bridge.__name__, bridge)
        executor = sys.modules["rclpy.executors"].SingleThreadedExecutor
        original = executor.spin_once
        delivered = []
        def spin(self, **kwargs):
            original(self, **kwargs)
            if delivered or not entities:
                return
            delivered.append(True)
            message = entities[0].message_type()
            message.data = list(b'{"run_id":"offline-reader","writer_id":"w7","writer_generation":3,"send_sequence":42}')
            entities[0].callback(message, {})
        monkeypatch.setattr(executor, "spin_once", spin)
        monkeypatch.setattr(sys, "argv", ["worker", json.dumps(cfg), "reader"])
        rc = worker.worker_main()
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert rc == 0, rows
    sample = next(row for row in rows if row["event"] == "receive")
    assert sample["payload_writer_id"] == "w7"
    assert sample["payload_writer_generation"] == 3
    assert sample["send_sequence"] == 42
    assert sample["reader_gid"] == "22" * 16
    assert sample["publisher_gid"] is None
    assert sample["publisher_gid_status"] == "unavailable_api"
    assert sample["identity_basis"] == "endpoint_correlated"
    assert sample["serialization_receipt"]["capture_stage"] == "callback_reserialization"
    assert sample["serialization_receipt"]["wire_capture"] is False


@pytest.mark.parametrize("failure,reason,status", [
    ("schema", "message_schema_mismatch", "binding_error"),
    ("nested_schema", "message_schema_mismatch", "binding_error"),
    ("type_support", "message_type_support_unavailable", "unsupported"),
    ("no_type_support", "message_type_support_unavailable", "unsupported"),
])
def test_worker_preserves_named_binding_failure_and_closes_context(monkeypatch, failure, reason, status):
    import sys
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture

    cfg = worker_config("offline-binding-failure")
    with ros_fixture(monkeypatch, []) as (calls, _, stream):
        if failure in {"schema", "nested_schema"}:
            cls = getattr(sys.modules["std_msgs.msg"],
                          "UInt8MultiArray" if failure == "schema" else "MultiArrayLayout")
            monkeypatch.setattr(cls, "get_fields_and_field_types", classmethod(lambda _: {"data": "string"}))
        else:
            def absent(_):
                if failure == "no_type_support":
                    raise sys.modules["rclpy.exceptions"].NoTypeSupportImportedException()
                raise RuntimeError("fixture_type_support_absent")
            monkeypatch.setattr(sys.modules["rclpy.type_support"], "check_for_type_support", absent)
        monkeypatch.setattr(sys, "argv", ["worker", json.dumps(cfg), "writer"])
        rc = worker.worker_main()
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert rc == 1
    failure_row = next(row for row in rows if row["event"] == "topology_message_type_support")
    assert (failure_row["reason"], failure_row["status"]) == (reason, status)
    assert failure_row["attribution"] == "binding_adapter"
    assert failure_row["candidate_eligible"] is False
    assert not any(row["event"] == "endpoint_created" for row in rows)
    assert "destroy_node" in calls and "shutdown" in calls


@pytest.mark.parametrize("operation,code", [
    ("initial_filter", "content_filter_message_type_not_expressible"),
    ("instrumentation", "instrumented_message_type_not_expressible"),
])
def test_input_rejects_string_only_bindings_for_array(operation, code):
    from r2f.runtime.topology_pair import build_input_receipts
    spec = endpoint("r0", "reader", type_name=ARRAY_TYPE)
    cfg = dict(CFG)
    if operation == "initial_filter":
        spec["filter"] = {"expression": "data = %0", "parameters": ["x"]}
    else:
        cfg["observation_mode"] = "instrumentation"
    receipts = build_input_receipts(program([spec], [action("c", "create", "r0", 1)]), cfg)
    row = receipts["endpoints"][0]
    assert row["binding_status"] == "not_expressible"
    assert row["reason"] == code
    assert receipts["execution_evidence"] is False


@pytest.mark.parametrize("failure,reason", [
    ("serialize", "message_serialization_failed"),
    ("roundtrip", "message_serialization_roundtrip_mismatch"),
])
def test_failed_serialization_never_reports_publish_success(monkeypatch, failure, reason):
    import sys
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture

    cfg = worker_config("offline-serialization-failure")
    commands = [{"command": "publish_batch", "command_id": 1, "sequences": [1]}]
    with ros_fixture(monkeypatch, commands) as (calls, _, stream):
        serialization = sys.modules["rclpy.serialization"]
        if failure == "serialize":
            def broken(message):
                raise ValueError("fixture_serialization_error")
            monkeypatch.setattr(serialization, "serialize_message", broken)
        else:
            def corrupt(data, cls):
                message = cls()
                message.data = list(b'corrupted')
                return message
            monkeypatch.setattr(serialization, "deserialize_message", corrupt)
        monkeypatch.setattr(sys, "argv", ["worker", json.dumps(cfg), "writer"])
        rc = worker.worker_main()
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert rc == 1
    assert any(reason in row.get("error", "") for row in rows if row["event"] == "worker_error")
    assert not any(row["event"] == "send" for row in rows)
    assert "destroy_node" in calls and "shutdown" in calls
