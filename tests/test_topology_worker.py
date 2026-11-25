"""Offline contracts for the F3 topology worker/backend components.

Nothing here starts ROS, rclpy, a DDS domain, a namespace, or a real endpoint.
The only child process launched is the module entry point with invalid
arguments, which must fail before importing rclpy.
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest

from r2f.runtime.topology_worker import (
    MAX_INTERVAL_SEC,
    OFFLINE_BACKEND,
    ROS_BACKEND,
    OfflineTopologyBackend,
    TopologyBackendError,
    TopologyUnsupported,
    _EndpointProcess,
    endpoint_expressibility,
    normalize_topology_config,
    sha256_json,
    worker_role_for,
    worker_role_kind,
)


def qos():
    return {
        "reliability": "reliable", "durability": "volatile",
        "deadline_sec": "infinite", "liveliness": "automatic",
        "lease_duration_sec": "infinite", "ownership": "shared",
        "partitions": [], "history": "keep_last", "depth": 16,
    }


def spec(**overrides):
    value = {
        "logical_id": "w0", "role": "writer", "topic": "/r2f/topology/a",
        "type_name": "std_msgs/msg/String", "qos": qos(),
        "vendor": None, "participant_id": None,
    }
    value.update(overrides)
    return value


class WorkerRoleContract(unittest.TestCase):
    def test_roles_are_bounded_and_deterministic(self):
        self.assertEqual(worker_role_for("writer", 0), "writer")
        self.assertEqual(worker_role_for("writer", 3), "writer4")
        self.assertEqual(worker_role_for("reader", 1), "reader2")
        for bad in (-1, 4, 99):
            with self.assertRaises(ValueError):
                worker_role_for("writer", bad)
        for bad in (True, "1", 1.0):
            with self.assertRaises(ValueError):
                worker_role_for("writer", bad)
        with self.assertRaises(ValueError):
            worker_role_for("probe", 0)

    def test_role_kind_parsing(self):
        self.assertEqual(worker_role_kind("writer4"), "writer")
        self.assertEqual(worker_role_kind("reader2"), "reader")
        self.assertIsNone(worker_role_kind("probe"))
        self.assertIsNone(worker_role_kind("writer10"))
        self.assertIsNone(worker_role_kind(None))


class EndpointBindingContract(unittest.TestCase):
    def test_supported_endpoint_binds_every_requested_field(self):
        prepared = endpoint_expressibility(spec(), {"rmw": "rmw_cyclonedds_cpp"})
        binding = prepared["binding"]
        self.assertEqual(binding["binding_status"], "exact_with_default_equivalent_fields")
        self.assertEqual(binding["topic"], "/r2f/topology/a")
        self.assertEqual(binding["public_api_qos"]["depth"], 16)
        self.assertEqual(binding["public_api_qos"]["deadline_ns"], 9223372036854775807)
        self.assertEqual(binding["public_api_qos"]["liveliness_lease_duration_ns"],
                         9223372036854775807)
        # Declared identity is never turned into a measured binding.
        self.assertEqual(binding["participant_id_status"], "declaration_only")
        self.assertFalse(binding["execution_evidence"])
        self.assertFalse(binding["candidate_eligible"])
        self.assertEqual(
            {row["field"] for row in binding["default_equivalent_fields"]},
            {"ownership", "partitions"})

    def test_deadline_seconds_become_bounded_nanoseconds(self):
        prepared = endpoint_expressibility(
            spec(qos={**qos(), "deadline_sec": 0.5}), {"rmw": "rmw_cyclonedds_cpp"})
        self.assertEqual(prepared["binding"]["public_api_qos"]["deadline_ns"], 500_000_000)

    def test_unobservable_or_unsupported_inputs_are_rejected_not_dropped(self):
        cases = {
            "qos_unknown_field": spec(qos={**qos(), "reliability": None}),
            "qos_not_expressible": spec(qos={**qos(), "ownership": "exclusive"}),
            "partitions_not_expressible": spec(qos={**qos(), "partitions": ["/p"]}),
            "message_type_not_expressible": spec(type_name="std_msgs/msg/Int32"),
            "content_filter_invalid_field": spec(
                role="reader",
                filter={"expression": "data.x > 1", "parameters": [], "evaluation": True}),
        }
        for code, endpoint in cases.items():
            with self.assertRaises(TopologyUnsupported) as caught:
                endpoint_expressibility(endpoint, {"rmw": "rmw_cyclonedds_cpp"})
            self.assertEqual(caught.exception.code, code)

    def test_unknown_qos_values_are_not_silently_defaulted(self):
        for field in ("reliability", "durability", "history", "depth", "liveliness",
                      "deadline_sec", "lease_duration_sec", "ownership", "partitions"):
            endpoint = spec(qos={**qos(), field: None})
            with self.assertRaises(TopologyUnsupported) as caught:
                endpoint_expressibility(endpoint, {"rmw": "rmw_cyclonedds_cpp"})
            self.assertEqual(caught.exception.code, "qos_unknown_field", field)

    def test_qos_enum_and_range_guards(self):
        for value in ("transient", "persistent"):
            with self.assertRaises(TopologyUnsupported):
                endpoint_expressibility(spec(qos={**qos(), "durability": value}),
                                        {"rmw": "rmw_cyclonedds_cpp"})
        prepared = endpoint_expressibility(spec(qos={**qos(), "history": "keep_all"}),
                                           {"rmw": "rmw_cyclonedds_cpp"})
        self.assertEqual(prepared["public_qos"]["history"], "keep_all")
        for depth in (0, 65537, True, "16"):
            with self.assertRaises(TopologyUnsupported):
                endpoint_expressibility(spec(qos={**qos(), "depth": depth}),
                                        {"rmw": "rmw_cyclonedds_cpp"})
        for deadline in (-1, float("nan"), float("inf"), True, "1"):
            with self.assertRaises(TopologyUnsupported):
                endpoint_expressibility(spec(qos={**qos(), "deadline_sec": deadline}),
                                        {"rmw": "rmw_cyclonedds_cpp"})

    def test_vendor_declaration_must_match_configured_rmw(self):
        prepared = endpoint_expressibility(
            spec(vendor="cyclonedds"), {"rmw": "rmw_cyclonedds_cpp"})
        self.assertEqual(prepared["binding"]["vendor_bound_rmw"], "rmw_cyclonedds_cpp")
        with self.assertRaises(TopologyUnsupported) as caught:
            endpoint_expressibility(spec(vendor="cyclonedds"), {"rmw": "rmw_fastrtps_cpp"})
        self.assertEqual(caught.exception.code, "vendor_declaration_mismatch")
        with self.assertRaises(TopologyUnsupported) as caught:
            endpoint_expressibility(spec(vendor="not_a_vendor"),
                                    {"rmw": "rmw_cyclonedds_cpp"})
        self.assertEqual(caught.exception.code, "vendor_declaration_not_expressible")

    def test_null_identity_fields_are_accepted_as_unknown(self):
        prepared = endpoint_expressibility(spec(vendor=None, participant_id=None),
                                           {"rmw": "rmw_cyclonedds_cpp"})
        self.assertIsNone(prepared["binding"]["vendor_bound_rmw"])
        self.assertIsNone(prepared["binding"]["participant_id_declared"])


class OfflineBackendContract(unittest.TestCase):
    def test_each_generation_gets_a_fresh_fake_identity(self):
        backend = OfflineTopologyBackend()
        backend.prepare({"environment": {}}, {"run_id": "offline"})
        first = backend.create_endpoint(spec(), 1, "writer")
        second = backend.create_endpoint(spec(), 2, "writer")
        self.assertNotEqual(first["endpoint_gid"], second["endpoint_gid"])
        self.assertNotEqual(first["pid"], second["pid"])
        self.assertFalse(first["real_ros"])
        # The fake identity is never a real GUID/PID claim.
        self.assertEqual(first["identity_source"], "offline_fake")

    def test_destroy_removes_the_endpoint_from_the_fake_graph(self):
        backend = OfflineTopologyBackend()
        backend.prepare({"environment": {}}, {"run_id": "offline"})
        writer = backend.create_endpoint(spec(), 1, "writer")
        reader = backend.create_endpoint(spec(logical_id="r0", role="reader"), 1, "reader")
        backend.destroy_endpoint(reader)
        snapshots = backend.snapshot({writer["logical_id"]: writer})["snapshots"]
        self.assertEqual(snapshots["w0"]["match_snapshot"]["matched_count"], 0)
        self.assertEqual(snapshots["w0"]["graph_snapshot"]["subscriptions"], [])

    def test_batch_publish_uses_one_controller_ack_not_per_sample(self):
        backend = OfflineTopologyBackend()
        backend.prepare({"environment": {}}, {"run_id": "offline"})
        writer = backend.create_endpoint(spec(), 1, "writer")
        receipt = backend.publish_batch(writer, [0, 1, 2], 0.0, "batch")
        self.assertEqual(receipt["sent_sequences"], [0, 1, 2])
        self.assertEqual(receipt["controller_ack_count"], 1)
        self.assertFalse(receipt["per_sample_ack"])
        self.assertLessEqual(receipt["end_ns"], receipt["end_ns"])

    def test_interval_outside_the_bounded_worker_input_is_rejected(self):
        backend = OfflineTopologyBackend()
        backend.prepare({"environment": {}}, {"run_id": "offline"})
        writer = backend.create_endpoint(spec(), 1, "writer")
        with self.assertRaises(TopologyUnsupported):
            backend.publish_batch(writer, [0], MAX_INTERVAL_SEC + 0.5, "batch")

    def test_closed_backend_refuses_further_actions(self):
        backend = OfflineTopologyBackend()
        backend.prepare({"environment": {}}, {"run_id": "offline"})
        backend.close()
        with self.assertRaises(TopologyBackendError):
            backend.create_endpoint(spec(), 1, "writer")
        self.assertTrue(backend.close()["already_closed"])

    def test_offline_prepare_labels_itself_not_ros(self):
        backend = OfflineTopologyBackend()
        prepared = backend.prepare({"environment": {"backend_available": True}},
                                   {"run_id": "offline"})
        self.assertFalse(prepared["ros_execution"])
        self.assertFalse(prepared["execution_evidence"])
        self.assertIn("offline_fake_backend_is_not_ros", prepared["limitations"])


class LiveGoGateContract(unittest.TestCase):
    def test_ros_backend_prepare_refuses_without_a_live_go(self):
        from r2f.runtime.topology_worker import RosTopologyBackend
        backend = RosTopologyBackend({"run_id": "x", "domain_id": 1},
                                     tempfile.mkdtemp())
        receipt = backend.prepare({"environment": {}}, {})
        self.assertFalse(receipt["ok"])
        self.assertEqual(receipt["reason"], "blocked:no_live_ros_go")
        self.assertFalse(receipt["ros_execution"])
        self.assertFalse(receipt["execution_evidence"])
        # No environment was activated, so any create call must fail closed.
        with self.assertRaises(TopologyBackendError):
            backend.create_endpoint(spec(), 1, "writer")

    def test_ros_backend_never_creates_an_endpoint_before_prepare(self):
        from r2f.runtime.topology_worker import RosTopologyBackend
        backend = RosTopologyBackend({"run_id": "x", "domain_id": 1},
                                     tempfile.mkdtemp())
        with self.assertRaises(TopologyBackendError):
            backend.create_endpoint(spec(), 1, "writer")
        self.assertEqual(backend.processes, [])


class EndpointProcessContract(unittest.TestCase):
    def test_command_without_a_running_worker_fails_closed(self):
        process = _EndpointProcess({"run_id": "x", "domain_id": 1, "rmw": "rmw_cyclonedds_cpp",
                                    "source_repo": os.getcwd()},
                                   {}, tempfile.mkdtemp(), "writer", lambda *a, **k: None)
        with self.assertRaises(TopologyBackendError):
            process.command("snapshot")

    def test_close_without_start_is_a_noop(self):
        process = _EndpointProcess({"run_id": "x", "domain_id": 1, "rmw": "rmw_cyclonedds_cpp",
                                    "source_repo": os.getcwd()},
                                   {}, tempfile.mkdtemp(), "writer", lambda *a, **k: None)
        self.assertTrue(process.close()["closed"])

    def test_module_entry_rejects_bad_arguments_before_importing_rclpy(self):
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        env = dict(os.environ, PYTHONPATH=os.path.join(repo, "src"))
        completed = subprocess.run(
            [sys.executable, "-B", "-m", "r2f.runtime.topology_worker"],
            capture_output=True, text=True, env=env, timeout=60)
        self.assertEqual(completed.returncode, 2)
        self.assertIn("usage", completed.stdout)

    def test_module_entry_rejects_an_unsupported_role(self):
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        env = dict(os.environ, PYTHONPATH=os.path.join(repo, "src"))
        completed = subprocess.run(
            [sys.executable, "-B", "-m", "r2f.runtime.topology_worker",
             json.dumps({"run_id": "x", "domain_id": 1, "rmw": "rmw_cyclonedds_cpp",
                         "logical_id": "w0", "generation": 1, "topic": "/t",
                         "type_name": "std_msgs/msg/String", "qos": qos()}), "probe"],
            capture_output=True, text=True, env=env, timeout=60)
        self.assertEqual(completed.returncode, 2)
        self.assertIn("unsupported_role", completed.stdout)


class ConfigContract(unittest.TestCase):
    def test_config_normalization_bounds_domain_and_timeouts(self):
        cfg = normalize_topology_config({"source_repo": "/tmp/x"})
        self.assertEqual(cfg["rmw"], "rmw_cyclonedds_cpp")
        self.assertEqual(cfg["domain_id"], 173)
        for bad in (-1, 232, True, "173"):
            with self.assertRaises(ValueError):
                normalize_topology_config({"domain_id": bad})
        with self.assertRaises(ValueError):
            normalize_topology_config({"rmw": "rmw_unknown"})
        for bad in (0, -1, True, float("inf"), "10"):
            with self.assertRaises(ValueError):
                normalize_topology_config({"startup_timeout_sec": bad})

    def test_backend_names_are_distinct(self):
        self.assertNotEqual(OFFLINE_BACKEND, ROS_BACKEND)

    def test_sha256_json_is_order_insensitive(self):
        self.assertEqual(sha256_json({"a": 1, "b": 2}), sha256_json({"b": 2, "a": 1}))
        self.assertNotEqual(sha256_json({"a": 1}), sha256_json({"a": 2}))


if __name__ == "__main__":
    unittest.main()
