"""F3 topology worker: one fresh public-rclpy endpoint per logical generation.

Two halves live here on purpose:

* ``_worker_main`` is the child process.  It builds exactly one publisher or
  subscription from explicit public QoS, reports its real GUID/PID/start ticks,
  publishes bounded batches without per-sample controller ACKs, answers bounded
  graph/match snapshots, and asserts writer liveliness through the public
  ``Publisher.assert_liveliness`` API.
* ``RosTopologyBackend`` / ``OfflineTopologyBackend`` are controller-side
  adapters with the same small surface.  The offline half is a contract-test
  double and is always ``not_ros`` / ``not candidate``.

One process per endpoint generation is deliberate: ``recreate`` must allocate a
fresh GUID *and* a fresh PID, and a retired generation can never be mistaken for
its successor.  The existing ``WorkerPool`` remains untouched; this module only
reuses its lifecycle ideas and the repository's public worker protocol shape.
"""
from __future__ import annotations

from copy import deepcopy
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time
import traceback
import uuid

from .environment import REPO, RMW_NAMES, activated_environment, check_environment

def _monotonic_clock_identity():
    try:
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        ns = os.stat("/proc/self/ns/time")
        return {"boot_id": boot, "device": ns.st_dev, "inode": ns.st_ino} if boot else None
    except OSError:
        return None


INFINITE_NS = 9223372036854775807
SUPPORTED_MESSAGE_TYPE = "std_msgs/msg/String"
ROS_BACKEND = "ros2_public_api"
OFFLINE_BACKEND = "offline_fake"
MAX_INTERVAL_SEC = 30.0
ROLE_RE = re.compile(r"(writer|reader)([1-9])?")

_SUPPORTED_PUBLIC_FIELDS = frozenset({
    "depth", "history", "reliability", "durability", "deadline_ns",
    "liveliness", "liveliness_lease_duration_ns",
})
_VENDOR_ALIASES = {
    "rmw_cyclonedds_cpp": "rmw_cyclonedds_cpp",
    "cyclonedds": "rmw_cyclonedds_cpp",
    "cyclone_dds": "rmw_cyclonedds_cpp",
    "cyclonedds_cpp": "rmw_cyclonedds_cpp",
    "rmw_fastrtps_cpp": "rmw_fastrtps_cpp",
    "fastdds": "rmw_fastrtps_cpp",
    "fast_dds": "rmw_fastrtps_cpp",
    "fastrtps": "rmw_fastrtps_cpp",
    "fast_rtps": "rmw_fastrtps_cpp",
}


NETEM_ISOLATION_ENV = "R2F_DISCOVERY_NETNS_PROOF"


def normalize_topology_environment_mutation(value):
    """Validate the opt-in topology environment mutation, or return None.

    Only the existing ``DiscoveryNetemController`` capability is expressible:
    network-layer delay/reorder/duplicate on derived default-RTPS UDP ports.
    ``discovery_drop`` is refused by name because that carrier lives on the
    nft/wire_proxy discovery target, not on this topology route, and the netem
    controller fixes loss at 0% so it can never drop.  A neutral spec and a
    zero ``restore_sec`` are refused: they cannot bound a real window, and a
    no-op must not be reported as an applied environment mutation.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("topology_environment_mutation_must_be_object")
    unknown = set(value) - {"kind", "spec"}
    if unknown:
        raise ValueError("topology_environment_mutation_unknown_field:" + ",".join(sorted(unknown)))
    kind = value.get("kind")
    if kind == "discovery_drop":
        raise ValueError("topology_netem_drop_not_supported:controller_loss_fixed_zero")
    if kind != "discovery_netem":
        raise ValueError("topology_environment_mutation_unsupported_kind:" + str(kind))
    if "spec" not in value:
        raise ValueError("topology_netem_spec_required")
    from .discovery_network import DiscoveryNetworkError, normalize_netem_spec
    try:
        spec = normalize_netem_spec(value["spec"])
    except DiscoveryNetworkError as exc:
        raise ValueError("invalid_netem_spec:" + str(exc)) from exc
    if spec["neutral"]:
        raise ValueError("topology_netem_neutral_spec_not_a_mutation")
    if not spec["restore_sec"]:
        raise ValueError("topology_netem_restore_sec_required")
    return {"kind": "discovery_netem", "spec": spec}


def netem_isolation_receipt():
    """Read-only check that this process really runs in the isolated netns.

    Never calls tc/nft/ip and never creates a namespace.  Missing or mismatched
    ``R2F_DISCOVERY_NETNS_PROOF`` is a hard fail-closed result: a topology route
    that declared an environment mutation must not touch the host loopback.
    """
    from .discovery_network import NetworkIsolationError, assert_isolated_namespace
    proof_present = bool(os.environ.get(NETEM_ISOLATION_ENV))
    try:
        isolation = assert_isolated_namespace()
    except NetworkIsolationError as exc:
        return {"schema": "r2f.topology-netem-isolation/v1", "isolated": False,
                "proof_present": proof_present, "proof_env": NETEM_ISOLATION_ENV,
                "error": str(exc), "tc_invoked": False,
                "reason": "topology_netem_requires_isolated_netns"}
    return {"schema": "r2f.topology-netem-isolation/v1", "isolated": True,
            "proof_present": True, "proof_env": NETEM_ISOLATION_ENV,
            "tc_invoked": False, "isolation": isolation,
            "reason": None}


class TopologyBackendError(RuntimeError):
    """A backend/tool error, never a SUT verdict."""


def _graph_local_gid(snapshot, node_name, kind):
    rows = snapshot["publishers" if kind == "writer" else "subscriptions"]
    own = [row for row in rows if row.get("node_name") == node_name]
    if len(own) != 1:
        raise TopologyBackendError("local_graph_endpoint_not_unique")
    gid = own[0].get("endpoint_gid")
    if (not isinstance(gid, str) or not re.fullmatch(r"[0-9a-f]{32}", gid)
            or not any(c != "0" for c in gid)):
        raise TopologyBackendError("local_graph_endpoint_gid_invalid")
    return gid


def _observe_filter_state(endpoint, emit, *, command, command_id, reader_gid, phase):
    """Observe a live reader's state at an explicitly identity-bound raw point."""
    from .filter_backend import ContentFilterAdapter
    common = {"command": command, "command_id": command_id, "phase": phase,
              "reader_gid": reader_gid, "endpoint_gid": reader_gid,
              "topic": endpoint.topic_name}
    adapter = ContentFilterAdapter()
    adapter.attach(endpoint)
    value = adapter.observe_state(api_event=lambda stage, **fields: emit(
        "content_filter_state_api_call", api_stage=stage,
        api_monotonic_ns=time.monotonic_ns(), **common, **fields))
    value.update(common)
    emit("content_filter_state_observation", filter_receipt=value, **common)
    return value


def _update_filter_identity(endpoint, graph, node_name, local_gid, incarnation,
                            command, emit, *, timeout_sec=1.0, state_observer=None):
    """Observe a setter's identity change without dispatching executor callbacks."""
    from .filter_backend import ContentFilterAdapter
    command_id = command["command_id"]
    common = {"command": "update_filter", "command_id": command_id,
              "identity_source": "get_subscriptions_info_by_topic",
              "topic": endpoint.topic_name}
    started = time.monotonic_ns()
    before = graph()
    if _graph_local_gid(before, node_name, "reader") != local_gid:
        raise TopologyBackendError("filter_identity_before_mismatch")
    before_type = next(row["topic_type"] for row in before["subscriptions"]
                       if row.get("node_name") == node_name)
    emit("endpoint_identity_snapshot", phase="before", endpoint_gid=local_gid,
         message_type=before_type, **common, **before)
    adapter = ContentFilterAdapter()
    adapter.attach(endpoint)
    value = adapter.update(command.get("expression"), command.get("parameters"),
                           field_types={"data": "string"},
                           api_event=lambda stage, **fields: emit(
                               "content_filter_api_call", command="update_filter",
                               command_id=command_id, reader_gid=local_gid,
                               api_stage=stage, api_monotonic_ns=time.monotonic_ns(), **fields))
    deadline = time.monotonic() + timeout_sec
    while True:
        after = graph()
        try:
            after_gid = _graph_local_gid(after, node_name, "reader")
            break
        except TopologyBackendError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)
    after_type = next(row["topic_type"] for row in after["subscriptions"]
                      if row.get("node_name") == node_name)
    emit("endpoint_identity_snapshot", phase="after", endpoint_gid=after_gid,
         message_type=after_type, **common, **after)
    # The production worker supplies this independent observer. Legacy helper
    # callers without it produce no state evidence, never a fabricated disabled
    # value. Keep set/get outcomes unchanged, even when state is observed false.
    if state_observer is not None:
        value["state_receipt"] = state_observer(
            endpoint, emit, command="update_filter", command_id=command_id,
            reader_gid=after_gid, phase="after")
    emit("content_filter_dynamic_update", command="update_filter", command_id=command_id,
         reader_gid=after_gid, endpoint_gid=after_gid, before_gid=local_gid,
         after_gid=after_gid, update_started_ns=started,
         update_finished_ns=time.monotonic_ns(), filter_receipt=value)
    next_incarnation = incarnation + (after_gid != local_gid)
    if after_gid != local_gid:
        emit("endpoint_identity_transition", schema="r2f.endpoint-identity-transition/v1",
             before_gid=local_gid, after_gid=after_gid,
             incarnation_before=incarnation, incarnation_after=next_incarnation,
             endpoint_gid=after_gid,
             api="rclpy.subscription.Subscription.set_content_filter", **common)
    return after_gid, next_incarnation


class TopologyBackendBlocked(TopologyBackendError):
    """Preflight or capability admission blocked; no endpoint was launched."""


class TopologyBackendTimeout(TopologyBackendError):
    """Bounded command/barrier deadline expired."""

    def __init__(self, message, *, command_id=None, endpoint_id=None, events=None):
        super().__init__(message)
        self.command_id = command_id
        self.endpoint_id = endpoint_id
        self.events = list(events or [])


class TopologyUnsupported(TopologyBackendError):
    """The model expresses something the public API cannot bind faithfully."""

    def __init__(self, code, message=None, *, details=None):
        self.code = code
        self.details = dict(details or {})
        super().__init__(message or code)


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def sha256_json(value):
    import hashlib
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _sha256_text(value):
    import hashlib
    return hashlib.sha256(value.encode()).hexdigest()


def worker_role_for(role, index):
    """Map a logical role/ordinal to a bounded worker role alias."""
    if role not in ("writer", "reader"):
        raise ValueError("role must be writer or reader")
    if type(index) is not int or index < 0 or index > 3:
        raise ValueError("at most four endpoints per role are supported")
    return role if index == 0 else f"{role}{index + 1}"


def worker_role_kind(role):
    match = ROLE_RE.fullmatch(role) if isinstance(role, str) else None
    return match.group(1) if match else None


def _finite_ns(seconds, field):
    if seconds == "infinite":
        return INFINITE_NS
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise TopologyUnsupported("qos_not_expressible", f"{field} must be seconds or infinite",
                                  details={"field": field, "value": seconds})
    if not math.isfinite(seconds) or seconds < 0:
        raise TopologyUnsupported("qos_not_expressible", f"{field} must be finite nonnegative",
                                  details={"field": field, "value": seconds})
    ns = int(seconds * 1_000_000_000)
    if ns < 0 or ns > INFINITE_NS:
        raise TopologyUnsupported("qos_not_expressible", f"{field} exceeds Duration range",
                                  details={"field": field, "value": seconds})
    return ns


def _required_qos(qos, field, allowed=None):
    value = qos.get(field)
    if value is None:
        raise TopologyUnsupported("qos_unknown_field", f"qos.{field} is unknown",
                                  details={"field": field})
    if allowed is not None and value not in allowed:
        raise TopologyUnsupported("qos_not_expressible", f"qos.{field}={value!r} unsupported",
                                  details={"field": field, "value": value,
                                           "allowed": list(allowed)})
    return value


def _vendor_rmw(declared, configured):
    if declared is None:
        return None
    key = str(declared).strip().lower()
    target = _VENDOR_ALIASES.get(key)
    if target is None:
        raise TopologyUnsupported("vendor_declaration_not_expressible",
                                  "vendor declaration is not a known RMW alias",
                                  details={"vendor": declared})
    if target != configured:
        raise TopologyUnsupported("vendor_declaration_mismatch",
                                  "declared vendor does not match configured RMW",
                                  details={"vendor": declared, "declared_rmw": target,
                                           "configured_rmw": configured})
    return target



def initial_content_filter_capability():
    """Initial reader configuration is conditional, not full CFT coverage."""
    return {
        "status": "conditional", "scope": "initial_only", "roles": ["reader"],
        "message_type": SUPPORTED_MESSAGE_TYPE, "field_types": {"data": "string"},
        "requirements": ["public_creation_api", "rmw_acceptance",
                         "get_content_filter", "is_cft_enabled"],
        "sample_filter_effect": False, "dynamic_update": False, "lease": False,
    }


def _initial_content_filter(value, role):
    """Bind a bounded String.data filter, without evaluating any samples."""
    from .filter_backend import validate_filter
    if role != "reader":
        raise TopologyUnsupported("content_filter_role_not_supported",
                                  "initial content filtering belongs to a reader")
    if (not isinstance(value, dict)
            or set(value) - {"expression", "parameters", "evaluation"}
            or not {"expression", "parameters"} <= set(value)):
        raise TopologyUnsupported("content_filter_invalid", "invalid initial filter object",
                                  details={"layer": "filter_specification"})
    expression, parameters = value["expression"], value["parameters"]
    if not isinstance(expression, str) or len(expression) > 1024:
        raise TopologyUnsupported("content_filter_invalid", "filter expression must be text <=1024",
                                  details={"layer": "expression_type"})
    if (not isinstance(parameters, list) or len(parameters) > 16
            or any(not isinstance(item, str) or len(item) > 256 for item in parameters)):
        raise TopologyUnsupported("content_filter_invalid",
                                  "filter parameters must be <=16 strings of length <=256",
                                  details={"layer": "parameter_type"})
    if value.get("evaluation") is not None and type(value["evaluation"]) is not bool:
        raise TopologyUnsupported("content_filter_invalid", "evaluation must be bool or null",
                                  details={"layer": "filter_specification"})
    try:
        validation = validate_filter(expression, parameters, field_types={"data": "string"})
    except RecursionError as exc:
        raise TopologyUnsupported("content_filter_invalid", "filter nesting exceeds harness limit",
                                  details={"layer": "unsupported_feature"}) from exc
    if not validation["ok"]:
        code = ("content_filter_invalid_field" if validation["layer"] == "unsupported_field"
                else "content_filter_invalid")
        raise TopologyUnsupported(code, validation["reason"],
                                  details={"layer": validation["layer"],
                                           "reason": validation["reason"],
                                           "validation_scope": "harness_subset"})
    # Whitespace and parameter spelling belong to the exact submitted input.
    # In particular, evaluation is never a runtime fact or an API argument.
    return {"expression": expression, "parameters": list(parameters)}


class _LivelinessEventCapture:
    """Buffer early callbacks until endpoint identity is known, without inventing it."""
    def __init__(self, kind, emit):
        self.kind, self.emit = kind, emit
        self.pending = []
        self.gid = None
        self.ready = False
        self.closed = False
        self.bound_ns = None
        self.create_begin_ns = None
        self.create_return_ns = None
        self.callback_serial = 0
        # Installed only by the real worker after its endpoint GUID is bound.
        self.peer_sampler = None

    def callback(self, status):
        self.callback_serial += 1
        fields = (('alive_count', 'not_alive_count', 'alive_count_change', 'not_alive_count_change')
                  if self.kind == 'reader' else ('total_count', 'total_count_change'))
        values = {}
        for name in fields:
            try:
                value = getattr(status, name)
                values[name] = value if type(value) is int else None
            except Exception:
                values[name] = None
        row = {'status_fields': values, 'callback_id': self.callback_serial,
               'callback_monotonic_ns': time.monotonic_ns(),
               'callback_time_ns': time.time_ns(), 'callback_clock_identity': _monotonic_clock_identity(),
               'status_type': type(status).__module__ + '.' + type(status).__qualname__,
               'event_kind': 'liveliness_changed' if self.kind == 'reader' else 'liveliness_lost',
               'observation_scope': 'reader_aggregate' if self.kind == 'reader' else 'local_writer',
               'writer_attribution': None, 'fixture': False, 'real_ros': True,
               'during_cleanup': self.closed, 'buffered': not self.ready and not self.closed,
               'endpoint_gid_at_callback': self.gid if self.ready else None}
        if self.ready or self.closed:
            self._write(row)
        else:
            self.pending.append(row)

    def _write(self, row):
        self.emit('topology_liveliness_event', endpoint_gid=row['endpoint_gid_at_callback'],
                  endpoint_identity_status='bound' if row['endpoint_gid_at_callback'] is not None else 'unavailable',
                  later_bound_endpoint_gid=self.gid, identity_bound_monotonic_ns=self.bound_ns,
                  create_call_begin_monotonic_ns=self.create_begin_ns,
                  create_call_return_monotonic_ns=self.create_return_ns, **row)
        # A peer sample is an additive diagnostic, never a continuous inventory
        # or an attribution of this aggregate callback to a particular writer.
        # Avoid touching an endpoint during creation or after cleanup begins.
        if (self.kind == 'reader' and self.peer_sampler is not None
                and not row['buffered'] and not row['during_cleanup']
                and row['endpoint_gid_at_callback'] is not None):
            try:
                observed = self.peer_sampler()
                if not isinstance(observed, dict):
                    raise TypeError('peer_sampler_result_must_be_object')
            except Exception as exc:
                observed = {'status': 'unknown',
                            'reason': 'peer_sampler_exception:' + type(exc).__name__,
                            'peer_gids_complete': False}
            observed = deepcopy(observed)
            # These invariants belong to the worker, not to a native return.
            observed.update(scope='bounded_peer_sample_not_continuous',
                            continuous_coverage=False, external_peers_known=None,
                            effect_observed=None, candidate_eligible=False,
                            s1_mutation_hit=False, execution_evidence=False)
            self.emit('topology_liveliness_peer_snapshot',
                      **{**observed, 'callback_id': row['callback_id'],
                         'callback_monotonic_ns': row['callback_monotonic_ns'],
                         'callback_clock_identity': row['callback_clock_identity'],
                         'reader_gid': row['endpoint_gid_at_callback']})

    def bind(self, gid):
        self.bound_ns = time.monotonic_ns()
        self.gid, self.ready = gid, True
        for row in self.pending:
            self._write(row)
        self.pending.clear()

    def close(self):
        self.closed = True
        for row in self.pending:
            self._write(row)
        self.pending.clear()


def _create_with_liveliness_audit(create, unsupported_type, emit):
    """Refuse unsafe retry: a failed public create does not return a cleanup handle."""
    try:
        return create()
    except Exception as exc:
        cause = exc
        seen = set()
        while cause is not None and id(cause) not in seen:
            seen.add(id(cause))
            if unsupported_type is not None and isinstance(cause, unsupported_type):
                emit("topology_liveliness_capability", status="unavailable_rmw_event",
                     retry="refused_cleanup_unverified", endpoint_gid=None,
                     cleanup_status="pending_worker_finally", error_type=type(cause).__name__)
                raise TopologyUnsupported("liveliness_event_unsupported_cleanup_unverified",
                                          "no endpoint handle to verify cleanup; no retry") from exc
            cause = cause.__cause__
        raise


def _audit_worker_cleanup(executor, node, context, emit, *, endpoint_creation_returned,
                          primary_error=None, close_error=None):
    """Attempt every cleanup even if telemetry fails; retain the first failure."""
    outcomes = {}
    audit_errors = []
    take_bridge_error = None

    def record(event, **fields):
        try:
            emit(event, **fields)
        except BaseException as exc:
            audit_errors.append(exc)

    actions = [("executor", executor.shutdown)]
    take_bridge = getattr(executor, "take_bridge", None)
    if take_bridge is not None:
        actions.append(("message_info_bridge", take_bridge.close))
    actions.extend((("node", node.destroy_node), ("context", context.shutdown)))
    for name, action in actions:
        try:
            result = action()
            outcomes[name] = {"status": "returned_false" if result is False else "returned",
                              "error_type": None}
        except BaseException as exc:
            outcomes[name] = {"status": "exception", "error_type": type(exc).__name__}
            if name == "message_info_bridge":
                take_bridge_error = exc
        record("worker_cleanup_step", component=name, **outcomes[name])
    record("worker_shutdown", cleanup_outcomes=outcomes,
           endpoint_creation_returned=endpoint_creation_returned,
           resource_release_verified=False, cleanup_scope="public_api_calls_only",
           liveliness_close_error_type=type(close_error).__name__ if close_error else None,
           take_bridge_close_error_type=type(take_bridge_error).__name__ if take_bridge_error else None,
           cleanup_audit_error_types=[type(exc).__name__ for exc in audit_errors])
    if primary_error is None:
        if close_error is not None:
            raise close_error
        if take_bridge_error is not None:
            raise take_bridge_error
        if audit_errors:
            raise audit_errors[0]
    return outcomes


def _create_reader_with_initial_filter(node, message_type, topic, callback, qos,
                                       initial_filter=None, *, emit=None,
                                       options_factory=None, fixture=False, event_callbacks=None):
    """The worker owns creation; the existing adapter observes the returned API.

    No setter, sample admission, match inference or unfiltered fallback exists
    on this path. A missing readback does not destroy an otherwise created
    subscription: it leaves configuration unverified and independently visible.
    """
    from .filter_backend import (
        ContentFilterAdapter, content_filter_options, filter_api_failure, receipt,
    )
    if initial_filter is None:
        return node.create_subscription(message_type, topic, callback, qos, **(
            {"event_callbacks": event_callbacks} if event_callbacks is not None else {})), None
    requested = _initial_content_filter(initial_filter, "reader")
    requested["enabled"] = bool(requested["expression"].strip())
    common = {"scope": "initial_only", "fixture": fixture,
              "execution_evidence": False, "filter_effect_observed": None,
              "filter_false_implies_unmatch": False,
              "dynamic_update_covered": False, "lease_covered": False}

    def record(event, value):
        if emit is not None:
            emit(event, filter_receipt=value)
        return value

    record("content_filter_initial_begin", receipt(
        "filter_initial_begin", requested=deepcopy(requested), submitted=False,
        configured=None, configuration_status="unverified", **common))
    try:
        options = content_filter_options(requested["expression"], requested["parameters"],
                                         options_factory=options_factory)
        create = getattr(node, "create_subscription", None)
        if not callable(create):
            raise AttributeError("Node.create_subscription unavailable")
        kwargs = {"event_callbacks": event_callbacks} if event_callbacks is not None else {}
        if options is not None:
            # An old public Node signature is an API limitation, not an RMW
            # rejection. If introspection is unavailable, let the real call
            # report its exception rather than declaring the API absent.
            import inspect
            try:
                signature = inspect.signature(create)
            except (TypeError, ValueError):
                signature = None
            if signature is not None and not (
                    "content_filter_options" in signature.parameters
                    or any(p.kind == inspect.Parameter.VAR_KEYWORD
                           for p in signature.parameters.values())):
                raise AttributeError("Node.create_subscription lacks content_filter_options")
            kwargs["content_filter_options"] = options
    except Exception as exc:
        failure = filter_api_failure(exc)
        # No subscription submission has happened at this point: constructor/
        # import/signature failures cannot establish an RMW rejection.
        failure["layer"] = "submission_api"
        value = record("content_filter_initial_guard", receipt(
            "filter_initial_guard", requested=deepcopy(requested), submitted=False,
            configured=None, configuration_status="unverified",
            reason=("content_filter_creation_api_unavailable"
                    if isinstance(exc, (ImportError, AttributeError))
                    else "content_filter_options_or_api_probe_failed"), **failure, **common))
        raise TopologyUnsupported("content_filter_submission_api", value["reason"],
                                  details=value) from exc
    try:
        endpoint = create(message_type, topic, callback, qos, **kwargs)
        created_ns = time.monotonic_ns()
    except Exception as exc:
        failure = filter_api_failure(exc)
        value = record("content_filter_initial_guard", receipt(
            "filter_initial_guard", requested=deepcopy(requested), submitted=True,
            accepted=False, configured=None, configuration_status="unverified",
            reason="create_subscription_failed", **failure, **common))
        raise TopologyUnsupported("content_filter_" + failure["layer"], value["reason"],
                                  details=value) from exc
    record("content_filter_initial_submitted", receipt(
        "filter_initial_submitted", requested=deepcopy(requested), submitted=True,
        accepted=True, configured=None, configuration_status="unverified",
        creation_returned_monotonic_ns=created_ns, **common))
    adapter = ContentFilterAdapter()
    adapter.attach(endpoint, created_with=requested)
    observed = adapter.initial_readback(fixture=fixture)
    observed["creation_returned_monotonic_ns"] = created_ns
    record("content_filter_initial_configuration", observed)
    return endpoint, observed


def endpoint_expressibility(spec, config):
    """Translate model endpoint fields into the exact public-rclpy input.

    The returned receipt is an input binding, never an observed identity.
    Unsupported model fields are rejected instead of silently dropped.
    """
    if not isinstance(spec, dict):
        raise TopologyUnsupported("invalid_endpoint", "endpoint spec must be an object")
    role = spec.get("role")
    if role not in ("writer", "reader"):
        raise TopologyUnsupported("invalid_role", "role must be writer or reader")
    topic = spec.get("topic")
    if not isinstance(topic, str) or not topic or len(topic) > 256:
        raise TopologyUnsupported("invalid_topic", "nonempty bounded topic required")
    type_name = spec.get("type_name")
    if type_name != SUPPORTED_MESSAGE_TYPE:
        raise TopologyUnsupported(
            "message_type_not_expressible",
            "this worker constructs only std_msgs/msg/String",
            details={"type_name": type_name, "supported": SUPPORTED_MESSAGE_TYPE})
    initial_filter = (_initial_content_filter(spec["filter"], role)
                      if "filter" in spec else None)
    qos = spec.get("qos")
    if not isinstance(qos, dict):
        raise TopologyUnsupported("invalid_qos", "qos object required")
    reliability = _required_qos(qos, "reliability", ("best_effort", "reliable"))
    durability = _required_qos(qos, "durability", ("volatile", "transient_local"))
    history = _required_qos(qos, "history", ("keep_last",))
    depth = _required_qos(qos, "depth")
    if isinstance(depth, bool) or not isinstance(depth, int) or not 1 <= depth <= 65536:
        raise TopologyUnsupported("qos_not_expressible", "depth must be 1..65536")
    liveliness = _required_qos(qos, "liveliness",
                               ("automatic", "manual_by_topic"))
    deadline_ns = _finite_ns(_required_qos(qos, "deadline_sec"), "deadline_sec")
    lease_ns = _finite_ns(_required_qos(qos, "lease_duration_sec"), "lease_duration_sec")
    ownership = _required_qos(qos, "ownership", ("shared",))
    partitions = _required_qos(qos, "partitions")
    if partitions != []:
        raise TopologyUnsupported(
            "partitions_not_expressible",
            "rclpy QoSProfile has no public partition selector",
            details={"partitions": deepcopy(partitions)})
    configured_rmw = config.get("rmw", "rmw_cyclonedds_cpp")
    declared_vendor = spec.get("vendor")
    bound_vendor = _vendor_rmw(declared_vendor, configured_rmw)
    public_qos = {
        "depth": depth,
        "history": history,
        "reliability": reliability,
        "durability": durability,
        "deadline_ns": deadline_ns,
        "liveliness": liveliness,
        "liveliness_lease_duration_ns": lease_ns,
    }
    assert not (set(public_qos) - _SUPPORTED_PUBLIC_FIELDS)
    binding = {
        "schema": "r2f.topology-endpoint-binding/v1",
        "logical_id": spec.get("logical_id"),
        "role": role,
        "topic": topic,
        "type_name": type_name,
        "requested_qos": deepcopy(qos),
        "public_api_qos": dict(public_qos),
        "api_fields": sorted(public_qos),
        "default_equivalent_fields": [
            {"field": "ownership", "requested": ownership, "runtime_default": "shared"},
            {"field": "partitions", "requested": deepcopy(partitions), "runtime_default": []},
        ],
        "unknown_fields": [],
        "unsupported_fields": [],
        "binding_status": "exact_with_default_equivalent_fields",
        "vendor_declared": declared_vendor,
        "vendor_bound_rmw": bound_vendor,
        "participant_id_declared": spec.get("participant_id"),
        "participant_id_status": "declaration_only",
        "source": "topology_worker_public_rclpy_input",
        "execution_evidence": False,
        "candidate_eligible": False,
    }
    if initial_filter is not None:
        binding["initial_content_filter"] = {
            **deepcopy(initial_filter), "scope": "initial_only",
            "rmw_support": "unverified", "observation_status": "input_only",
        }
    return {"public_qos": public_qos, "binding": binding,
            "initial_content_filter": initial_filter}


def _public_qos_kwargs(qos):
    from rclpy.qos import (
        DurabilityPolicy, HistoryPolicy, LivelinessPolicy, ReliabilityPolicy,
    )
    from rclpy.duration import Duration
    kwargs = {
        "depth": qos["depth"],
        "history": HistoryPolicy.KEEP_LAST,
        "reliability": {
            "best_effort": ReliabilityPolicy.BEST_EFFORT,
            "reliable": ReliabilityPolicy.RELIABLE,
        }[qos["reliability"]],
        "durability": {
            "volatile": DurabilityPolicy.VOLATILE,
            "transient_local": DurabilityPolicy.TRANSIENT_LOCAL,
        }[qos["durability"]],
        "deadline": Duration(nanoseconds=qos["deadline_ns"]),
        "liveliness": {
            "automatic": LivelinessPolicy.AUTOMATIC,
            "manual_by_topic": LivelinessPolicy.MANUAL_BY_TOPIC,
        }[qos["liveliness"]],
        "liveliness_lease_duration": Duration(nanoseconds=qos["liveliness_lease_duration_ns"]),
    }
    return kwargs



class CycloneMatchedPeerObserver:
    """Read existing process-local DDS entities; never create or instrument one.

    Public CycloneDDS 0.10.5 ABI only. The library MUST already be mapped by the
    RMW in this worker. RCL pointers are never cast to vendor-private objects.
    Handles have process-local scope; only the copied GUIDs cross the worker
    boundary. Failure/incomplete/racing enumerations deliberately have no peer
    proof fields accepted by the projection consumer.
    """
    SCHEMA = "r2f.cyclone-matched-peer-observation/v1"
    C_ABI_PROBE_SHA256 = "979a4a0a59e49f248eb9eb954e4137aea943595b01cbafb89050ac1f6ac0fab0"
    C_ABI_PROBE_SOURCE_SHA256 = "46ed3f9ae8d2df687d3e647e20d4c3cfa2e8774e3275d26e4429283ad8f5b694"
    VERIFIED_C_ABIS = (('35ceb2bc0c8a19745050826cdf84845357dd89098e5ffd51b93a4a7937d693d1', '9a63f94ee673c5efddb337a7dd012e4c98960b10ce256dcc3a7b4c331fabf5b5', (64, 0, 16, 32, 40, 48, 56)),)
    VERIFIED_C_HEADERS = {'/opt/ros/jazzy/include/CycloneDDS/dds/dds.h': '35ceb2bc0c8a19745050826cdf84845357dd89098e5ffd51b93a4a7937d693d1', '/opt/ros/jazzy/include/CycloneDDS/dds/export.h': 'fdf2229d1cb37d8a57c147e768d7b42f735976cd42adacdaccc70043cd450469', '/opt/ros/jazzy/include/CycloneDDS/dds/features.h': 'f1c7cbdb42eede111f7b8cf8e61bdfbda1596f9086bbd4c1785e37a7319ac697', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsc/dds_basic_types.h': '53f7f02ce723ecc3f4a45fc1c7e0b050c5bedfd68614e0111182a8598f14c6b8', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsrt/time.h': '3050907039419bbaaef63a0be8fecc8d66aaee023712b7bef5f132b0ba926a25', '/opt/ros/jazzy/include/CycloneDDS/dds/config.h': 'f9eb858b057c643ad4dcba489cb94b3c1dbf667116858e07510ea2656a66ef52', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsrt/types.h': 'fe6dcdfd0f45560c0fe11202d4abaa0baf1f5289073b797c76e3dc6de3cfb996', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsrt/types/posix.h': 'f2541f643665f17e3a5e916ad216e143b8e99dc3498a609f305cbc584444104c', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsrt/retcode.h': 'de1d07082289f2578d2b57739d62df33c319e10ece6320ac775a6d2e2bdceedb', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsrt/log.h': '7c10558cf25254870021446471ff740321785b588598dbf41e907b02b3f986d0', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsrt/attributes.h': '5a4f6d101bebd049b56e96088aa339258b3d57ebb81d3cd2311d5bdd6c567c38', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsc/dds_public_impl.h': '8129a989c21dfd7ed7d1d5194aa1473c7bbd9af4c08b38ebccea0f8a15d9be8a', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsrt/align.h': 'bda2ca56cf563007f39268608e25030a1883d9b3ac742e28aa87c6c9d5121ce4', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsc/dds_public_alloc.h': 'c615a1e4c552b98f16ba29dd30cad697c6a90334f9b1f222cb7fde3cb1ac3e5e', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsc/dds_opcodes.h': '0de425493ca5c1f273e147f809e225eac414cfaad43b28b2f3373fc34d5c8009', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsrt/static_assert.h': '26f1bf25b4aa0db38518223761235e75f55f42365a795bb2e0b4b40a7cade69b', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsc/dds_public_qos.h': '241a23a86737b929fb141cffa2514e1de6c2297dc784b64227238db60a925ddc', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsc/dds_public_qosdefs.h': '914ac31fec83a25f22456ed6b8a610428ef59902c6b4447394bebc96c14aadb9', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsc/dds_public_error.h': 'cb15af587ba62bf24185e7cc2bd61ac45486e0ada603a0180b82aa9b18dbd273', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsc/dds_public_status.h': 'a25ea12a098a7484fd7a814f658c83d782173c035e238f89804c809ad252e59d', '/opt/ros/jazzy/include/CycloneDDS/dds/ddsc/dds_public_listener.h': '9f785db8b9a00082cfe278826f30de618217ea2eb1b78e9419a341bafa505632', '/opt/ros/jazzy/include/CycloneDDS/dds/version.h': '57674091945002fd69395e208a0a98d504905948cd6ef9a3a04b017310045e55'}
    MAX_ENTITIES = 512
    MAX_PEERS = 256

    def __init__(self):
        import ctypes as c
        import hashlib
        self.c = c

        class Guid(c.Structure):
            _fields_ = [("v", c.c_uint8 * 16)]

        class Endpoint(c.Structure):
            _fields_ = [("key", Guid), ("participant_key", Guid),
                        ("participant_instance_handle", c.c_uint64),
                        ("topic_name", c.c_char_p), ("type_name", c.c_char_p),
                        ("qos", c.c_void_p)]

        self.Guid, self.Endpoint = Guid, Endpoint
        # Restrict the hand-declared ABI; unknown installations are a capability
        # gap, not an excuse to dereference undocumented RMW internals.
        library = Path("/opt/ros/jazzy/lib/x86_64-linux-gnu/libddsc.so.0.10.5").resolve()
        header = Path("/opt/ros/jazzy/include/CycloneDDS/dds/dds.h")
        mapped = set()
        mapping_identity = []
        for row in Path("/proc/self/maps").read_text().splitlines():
            fields = row.split(maxsplit=5)
            if len(fields) == 6 and "/libddsc.so" in fields[5]:
                mapped.add(fields[5])
                mapping_identity.append({"device": fields[3], "inode": fields[4], "offset": fields[2]})
        if (mapped != {str(library)} or c.sizeof(Endpoint) != 64
                or c.sizeof(c.c_void_p) != 8 or not hasattr(os, "RTLD_NOLOAD")):
            raise ValueError("loaded_cyclone_library_or_abi_not_unique")
        stat = library.stat()
        if (not mapping_identity or sum(int(row["offset"], 16) == 0 for row in mapping_identity) != 1
                or any(int(row["inode"]) != stat.st_ino
                       or tuple(int(v, 16) for v in row["device"].split(":")) != (os.major(stat.st_dev), os.minor(stat.st_dev))
                       for row in mapping_identity)):
            raise ValueError("loaded_cyclone_mapping_instance_or_inode_not_unique")
        import platform
        if platform.machine() != "x86_64" or sys.byteorder != "little" or c.sizeof(c.c_ulong) != 8:
            raise ValueError("native_endpoint_c_abi_platform_unsupported")
        if any(hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest
               for path, digest in self.VERIFIED_C_HEADERS.items()):
            raise ValueError("native_endpoint_c_header_fingerprint_changed")
        actual_layout = (c.sizeof(Endpoint), *(getattr(Endpoint, name).offset for name in
                          ("key", "participant_key", "participant_instance_handle", "topic_name", "type_name", "qos")))
        abi_key = (hashlib.sha256(header.read_bytes()).hexdigest(),
                   hashlib.sha256(library.read_bytes()).hexdigest(), actual_layout)
        if abi_key not in self.VERIFIED_C_ABIS:
            raise ValueError("native_endpoint_c_abi_probe_unverified")
        self.lib = c.CDLL(str(library), mode=os.RTLD_NOLOAD | os.RTLD_LOCAL)
        entity, handle = c.c_int32, c.c_uint64
        self.lib.dds_lookup_participant.argtypes = [c.c_uint32, c.POINTER(entity), c.c_size_t]
        self.lib.dds_lookup_participant.restype = c.c_int32
        self.lib.dds_get_children.argtypes = [entity, c.POINTER(entity), c.c_size_t]
        self.lib.dds_get_children.restype = c.c_int32
        self.lib.dds_get_guid.argtypes = [entity, c.POINTER(Guid)]
        self.lib.dds_get_guid.restype = c.c_int32
        self.lib.dds_get_domainid.argtypes = [entity, c.POINTER(c.c_uint32)]
        self.lib.dds_get_domainid.restype = c.c_int32
        for kind in ("subscriptions", "publications"):
            function = getattr(self.lib, "dds_get_matched_" + kind)
            function.argtypes = [entity, c.POINTER(handle), c.c_size_t]
            function.restype = c.c_int32
        for kind in ("subscription", "publication"):
            function = getattr(self.lib, "dds_get_matched_" + kind + "_data")
            function.argtypes = [entity, handle]
            function.restype = c.POINTER(Endpoint)
        self.lib.dds_builtintopic_free_endpoint.argtypes = [c.POINTER(Endpoint)]
        self.lib.dds_builtintopic_free_endpoint.restype = None
        self.provenance = {"library": str(library),
                           "library_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
                           "header": str(header),
                           "header_sha256": hashlib.sha256(header.read_bytes()).hexdigest(),
                           "abi": "CycloneDDS-0.10.5-lp64-endpoint64",
                           "c_probe_sha256": self.C_ABI_PROBE_SHA256,
                           "c_probe_source_sha256": self.C_ABI_PROBE_SOURCE_SHA256,
                           "c_probe_layout": actual_layout,
                           "header_fingerprints": dict(self.VERIFIED_C_HEADERS),
                           "loaded_mapping_identity": mapping_identity,
                           "attachment": "existing_process_local_entities_no_instrumentation",
                           "creates_entities": False}

    def _enumerate(self, function, *args, peers=False):
        c = self.c
        capacity = self.MAX_PEERS if peers else self.MAX_ENTITIES
        array = ((c.c_uint64 if peers else c.c_int32) * capacity)()
        count = function(*args, array, capacity)
        if type(count) is not int or not 0 <= count <= capacity:
            raise ValueError("native_enumeration_error_or_capacity_exceeded")
        values = list(array[:count])
        if any(value <= 0 for value in values) or len(values) != len(set(values)):
            raise ValueError("native_handles_invalid_or_aliased")
        return sorted(values)

    def _guid(self, handle):
        guid = self.Guid()
        return bytes(guid.v).hex() if self.lib.dds_get_guid(handle, self.c.byref(guid)) == 0 else None

    def _locate(self, domain, gid):
        roots = self._enumerate(self.lib.dds_lookup_participant, domain)
        pending, seen, edges, matches = list(roots), set(), [], []
        while pending:
            handle = pending.pop()
            if handle in seen or len(seen) >= self.MAX_ENTITIES:
                raise ValueError("entity_inventory_cycle_alias_or_capacity_exceeded")
            seen.add(handle)
            if self._guid(handle) == gid:
                matches.append(handle)
            children = self._enumerate(self.lib.dds_get_children, handle)
            edges.append([handle, children])
            pending.extend(children)
        if len(matches) != 1:
            raise ValueError("local_gid_not_unique_in_existing_entities")
        observed = self.c.c_uint32()
        if (self.lib.dds_get_domainid(matches[0], self.c.byref(observed)) != 0
                or observed.value != domain):
            raise ValueError("local_entity_domain_mismatch")
        return matches[0], sorted(edges)

    def _topic_kind_diagnostic(self, handle):
        """Independent optional readbacks, never ownership or GUID authority.

        Cyclone 0.10.5 dds_topic.c pins DDS_KIND_TOPIC for these public APIs;
        dds.h defines a nonnegative return as full byte length (excluding NUL).
        One fixed buffer/call per symbol bounds copying, including error output.
        """
        capacity = 256
        evidence = {"handle": handle, "kind": "unknown", "probes": {}}
        for name in ("dds_get_name", "dds_get_type_name"):
            probe = {"return_code": None, "status": "unavailable",
                     "capacity_bytes": capacity, "buffer_hex": None, "value": None}
            evidence["probes"][name] = probe
            buffer = None
            try:
                function = getattr(self.lib, name, None)
                if function is None:
                    probe["error"] = "optional_symbol_absent"
                    continue
                # Lazy binding keeps optional symbols out of matched/count setup.
                function.argtypes = [self.c.c_int32, self.c.POINTER(self.c.c_char),
                                     self.c.c_size_t]
                function.restype = self.c.c_int32
                buffer = self.c.create_string_buffer(b"\xff" * capacity, capacity)
                rc = function(handle, buffer, capacity)
                probe["return_code"] = rc
                raw = bytes(buffer)
                probe["buffer_hex"] = raw.hex()
                if type(rc) is not int:
                    probe.update(status="malformed", error="return_code_not_integer")
                elif rc == -12:
                    probe.update(status="unsupported", error="illegal_operation")
                elif rc < 0:
                    probe.update(status="error", error="api_error")
                elif rc >= capacity:
                    probe.update(status="truncated", error="reported_length_exceeds_buffer")
                elif raw[rc] != 0 or b"\x00" in raw[:rc]:
                    probe.update(status="malformed", error="length_or_terminator_mismatch")
                elif rc == 0:
                    probe.update(status="malformed", error="empty_name")
                else:
                    try:
                        probe["value"] = raw[:rc].decode("utf-8", errors="strict")
                        probe["status"] = "observed"
                    except UnicodeDecodeError:
                        probe.update(status="malformed", error="invalid_utf8")
            except Exception as exc:
                # Optional observation must not replace a primary inventory error.
                try:
                    detail = str(exc)[:256]
                except BaseException:
                    # Exception formatting is untrusted diagnostic work, even
                    # if __str__ raises outside the usual Exception hierarchy.
                    detail = "<exception_text_unavailable>"
                probe.update(status="error", error="optional_probe_exception",
                             exception_type=type(exc).__name__, detail=detail)
                if buffer is not None:
                    probe["buffer_hex"] = bytes(buffer).hex()
        if all(p["status"] == "observed" for p in evidence["probes"].values()):
            evidence["kind"] = "topic"
        return evidence

    def sample_ownership(self, domain, gid, kind):
        """Supporting local ownership observation; never a sharing/control claim.

        Handles are process-local diagnostics. The worker raw envelope binds the
        copied GUIDs to logical/generation/process/run identity. Two equal bounded
        samples cannot establish atomicity or participant lifecycle generation.
        """
        result = {"schema": "r2f.cyclone-participant-ownership/v1",
                  "complete": False, "status": "unknown", "candidate_eligible": False,
                  "domain_id": domain, "local_gid": gid, "role_kind": kind,
                  "provenance": self.provenance,
                  "started_monotonic_ns": time.monotonic_ns(),
                  "ownership_api": "dds_get_participant+dds_get_parent",
                  "lookup_api": "dds_lookup_participant+dds_get_children+dds_get_guid",
                  "limitations": ["bounded_stable_samples_not_atomic_or_continuous",
                                  "process_local_handles_not_lifecycle_generation",
                                  "configured_participant_sharing_not_controlled"]}
        try:
            if (type(domain) is not int or not 0 <= domain < 2**32
                    or not isinstance(gid, str) or re.fullmatch(r"[0-9a-f]{32}", gid) is None
                    or gid == "0" * 32 or kind not in ("writer", "reader")):
                raise ValueError("participant_ownership_input_identity_invalid")
            # Optional symbols are bound here so their absence cannot disable
            # the existing matched-peer/count observer.
            for name in ("dds_get_parent", "dds_get_participant"):
                function = getattr(self.lib, name, None)
                if function is None:
                    result["status"] = "unavailable"
                    raise ValueError("participant_ownership_api_unavailable:" + name)
                function.argtypes = [self.c.c_int32]
                function.restype = self.c.c_int32

            def capture():
                local, inventory = self._locate(domain, gid)
                edges = dict(inventory)
                participant = self.lib.dds_get_participant(local)
                if type(participant) is not int or participant <= 0:
                    raise ValueError("participant_lookup_failed")
                chain = [local]
                while chain[-1] != participant:
                    parent = self.lib.dds_get_parent(chain[-1])
                    if (parent not in edges or chain[-1] not in edges[parent]
                            or parent in chain or len(chain) >= self.MAX_ENTITIES):
                        raise ValueError("participant_parent_chain_invalid")
                    chain.append(parent)
                # Keep each attempt, including failed API values, in the raw
                # supporting receipt. Never promote these diagnostics to proof.
                root = {"participant_handle": participant}
                result.setdefault("root_diagnostics", []).append(root)
                root["participant_parent_return"] = self.lib.dds_get_parent(participant)
                root["participant_self_return"] = self.lib.dds_get_participant(participant)
                root["lookup_participants"] = self._enumerate(self.lib.dds_lookup_participant, domain)
                parent = root["participant_parent_return"]
                if (local == participant or root["participant_self_return"] != participant
                        or participant not in root["lookup_participants"]
                        or type(parent) is not int or parent < 0):
                    raise ValueError("participant_root_invalid")
                # dds.h documents NIL; pinned Cyclone 0.10.5 attaches the
                # participant to its domain entity instead (dds_participant.c).
                # Corroborate a positive parent using public relations, without
                # treating the domain handle as a participant or wire identity.
                if parent > 0:
                    if parent in edges:
                        raise ValueError("participant_root_invalid")
                    parent_domain = self.c.c_uint32()
                    rc = self.lib.dds_get_domainid(parent, self.c.byref(parent_domain))
                    root["parent_domain_return_code"] = rc
                    root["parent_domain_id"] = parent_domain.value if rc == 0 else None
                    root["parent_participant_return"] = self.lib.dds_get_participant(parent)
                    root["parent_children_queries"] = []
                    def read_children(handle, array, capacity):
                        value = self.lib.dds_get_children(handle, array, capacity)
                        root["parent_children_queries"].append(
                            {"capacity": capacity, "return_code": value})
                        return value
                    root["parent_children"] = self._enumerate(read_children, parent)
                    if (rc != 0 or parent_domain.value != domain
                            or root["parent_participant_return"] != 0
                            or participant not in root["parent_children"]):
                        raise ValueError("participant_root_invalid")
                    # A domain may itself have a library parent. Bound and
                    # retain the ancestry, rejecting loops and endpoint aliases.
                    ancestors = [parent]
                    root["parent_ancestry"] = []
                    while ancestors[-1] != 0:
                        ancestor = self.lib.dds_get_parent(ancestors[-1])
                        root["parent_ancestry"].append([ancestors[-1], ancestor])
                        if (type(ancestor) is not int or ancestor < 0
                                or ancestor in edges or ancestor in ancestors
                                or len(ancestors) >= self.MAX_ENTITIES):
                            raise ValueError("participant_root_invalid")
                        ancestors.append(ancestor)
                # Retain attempted reads before validation can fail. These are
                # supporting diagnostics, not successful ownership fields.
                diagnostics = {"entity_inventory": inventory, "entity_identities": [],
                               "topic_kind_diagnostics": []}
                result.setdefault("inventory_diagnostics", []).append(diagnostics)
                identities = []
                for handle, _ in inventory:
                    diagnostic = {"handle": handle, "known_labels": [],
                                  "domain_return_code": None, "observed_domain_id": None,
                                  "guid_return_code": None, "guid": None}
                    if handle == local:
                        diagnostic["known_labels"].append("local_endpoint")
                    if handle == participant:
                        diagnostic["known_labels"].append("participant")
                    diagnostics["entity_identities"].append(diagnostic)
                    topic = self._topic_kind_diagnostic(handle)
                    diagnostics["topic_kind_diagnostics"].append(topic)
                    if topic["kind"] == "topic":
                        diagnostic["known_labels"].append("topic")
                    observed_domain = self.c.c_uint32()
                    domain_rc = self.lib.dds_get_domainid(handle, self.c.byref(observed_domain))
                    diagnostic["domain_return_code"] = domain_rc
                    diagnostic["observed_domain_id"] = observed_domain.value if domain_rc == 0 else None
                    if domain_rc != 0 or observed_domain.value != domain:
                        raise ValueError("participant_inventory_domain_mismatch")
                    guid = self.Guid()
                    rc = self.lib.dds_get_guid(handle, self.c.byref(guid))
                    diagnostic["guid_return_code"] = rc
                    # Cyclone 0.10.5 also reads topic GUIDs (dds_entity.c),
                    # potentially all zero. Preserve this strict GUID gate;
                    # optional kind evidence never qualifies an identity.
                    value = bytes(guid.v).hex() if rc == 0 else None
                    diagnostic["guid"] = value
                    if (rc not in (0, -12) or (rc == -12 and handle in (local, participant))
                            or value == "0" * 32):
                        raise ValueError("participant_inventory_guid_invalid_or_aliased")
                    identities.append({"handle": handle, "gid": value,
                                       "guid_return_code": rc,
                                       "domain_id": observed_domain.value})
                if next(row["gid"] for row in identities if row["handle"] == local) != gid:
                    raise ValueError("participant_endpoint_guid_changed")
                gids = [row["gid"] for row in identities if row["gid"] is not None]
                if len(gids) != len(set(gids)):
                    raise ValueError("participant_inventory_guid_invalid_or_aliased")
                participant_gid = next(row["gid"] for row in identities
                                       if row["handle"] == participant)
                if not participant_gid or participant_gid == "0" * 32 or participant_gid == gid:
                    raise ValueError("participant_guid_invalid")
                return {"local_handle": local, "participant_handle": participant,
                        "participant_gid": participant_gid, "parent_chain": chain,
                        "entity_inventory": inventory, "entity_identities": identities,
                        "participant_root_observation": root}

            first, second = capture(), capture()
            if first != second:
                raise ValueError("participant_ownership_inventory_changed")
            result.update(first, complete=True, status="observed", stable_sample_count=2)
        except (ValueError, TypeError, OSError, AttributeError, RuntimeError) as exc:
            if result.get("inventory_diagnostics"):
                result["inventory_diagnostics"][-1]["failure_category"] = str(exc)
            result.update(complete=False, reason=str(exc))
        result["ended_monotonic_ns"] = time.monotonic_ns()
        return result

    def sample(self, domain, gid, kind):
        began = time.monotonic_ns()
        result = {"schema": self.SCHEMA, "complete": False, "provenance": self.provenance,
                  "domain_id": domain, "local_gid": gid, "role_kind": kind,
                  "started_monotonic_ns": began}
        try:
            if (type(domain) is not int or not 0 <= domain < 2**32
                    or not isinstance(gid, str) or re.fullmatch(r"[0-9a-f]{32}", gid) is None
                    or gid == "0" * 32 or kind not in ("writer", "reader")):
                raise ValueError("native_observation_input_identity_invalid")
            local, inventory = self._locate(domain, gid)
            suffix = "subscription" if kind == "writer" else "publication"
            enum = getattr(self.lib, "dds_get_matched_" + suffix + "s")
            data = getattr(self.lib, "dds_get_matched_" + suffix + "_data")
            handles = self._enumerate(enum, local, peers=True)
            peers = []
            for handle in handles:
                pointer = data(local, handle)
                if not pointer:
                    raise ValueError("matched_endpoint_data_unavailable")
                try:
                    row = pointer.contents
                    peers.append({"instance_handle": handle,
                                  "endpoint_gid": bytes(row.key.v).hex(),
                                  "participant_gid": bytes(row.participant_key.v).hex(),
                                  "topic_name": row.topic_name.decode("utf-8") if row.topic_name else None,
                                  "type_name": row.type_name.decode("utf-8") if row.type_name else None})
                finally:
                    self.lib.dds_builtintopic_free_endpoint(pointer)
            gids = [row["endpoint_gid"] for row in peers]
            if (len(set(gids)) != len(gids) or gid in gids or "0" * 32 in gids
                    or self._enumerate(enum, local, peers=True) != handles
                    or self._locate(domain, gid) != (local, inventory)):
                raise ValueError("matched_peer_or_local_inventory_changed_or_aliased")
            result.update(complete=True, local_handle=local, peers=peers,
                          matched_peer_gids=gids, entity_inventory=inventory,
                          enumeration_api="dds_get_matched_" + suffix + "s",
                          data_api="dds_get_matched_" + suffix + "_data",
                          lookup_api="dds_lookup_participant+dds_get_children+dds_get_guid",
                          limitations=["bounded_stable_samples_not_atomic_or_continuous_match",
                                       "instance_handles_are_process_local_not_wire_guids"])
        except (ValueError, TypeError, OSError, UnicodeError, AttributeError) as exc:
            result.update(complete=False, reason=str(exc))
        result["ended_monotonic_ns"] = time.monotonic_ns()
        return result


def _matched_peer_fields(observer, domain, gid, kind, count_before, count_after, observation):
    """Expose the existing projection protocol only for a complete raw sample."""
    result = {"native_peer_observation": observation, "peer_gids_complete": False}
    peers = observation.get("matched_peer_gids", [])
    native_schema = "r2f.fastdds-native-matched-peer-observation/v1"
    is_native = getattr(observer, "SCHEMA", None) == native_schema
    if (observer is not None and observation.get("complete") is True
            and isinstance(peers, list)
            and (not is_native or observation.get("schema") == native_schema)
            and type(count_before) is int and type(count_after) is int
            and count_before == count_after == len(peers)
            and observation.get("local_gid") == gid
            and observation.get("domain_id") == domain
            and observation.get("role_kind") == kind):
        proof_source = ("fastdds_native_stateful_peer_snapshot"
            if is_native
            else "dds_matched_endpoint_api")
        result.update(peer_proof_source=proof_source,
                      peer_gids_complete=True, matched_peer_gids=peers)
    else:
        result["peer_observation_limitation"] = observation.get("reason", "native_and_public_counts_differ")
    return result


class _LivelinessPeerSampler:
    """Read a bounded native peer set around a reader aggregate count.

    No polling thread, DDS entity, or status-reset API is added. The native
    observer only reads matched endpoints through the already pinned public
    ABI. Even a complete sample cannot establish an uninterrupted peer set.
    """
    def __init__(self, endpoint, rmw, domain, gid, *, instrumentation=None):
        self.endpoint, self.rmw, self.domain, self.gid = endpoint, rmw, domain, gid
        self.instrumentation = instrumentation
        self.observer = None

    def __call__(self):
        value = {'schema': 'r2f.liveliness-peer-snapshot/v1', 'status': 'unknown',
                 'scope': 'bounded_peer_sample_not_continuous',
                 'sample_started_ns': time.monotonic_ns(),
                 'sample_clock_identity': _monotonic_clock_identity(),
                 'domain_id': self.domain, 'reader_gid': self.gid,
                 'rmw_identifier': self.rmw, 'public_count_before': None,
                 'public_count_after': None, 'peer_gids_complete': False,
                 'continuous_coverage': False, 'external_peers_known': None,
                 'effect_observed': None, 'candidate_eligible': False,
                 's1_mutation_hit': False, 'execution_evidence': False}
        observation = {'complete': False, 'reason': 'rmw_has_no_public_peer_observer'}
        try:
            value['public_count_before'] = self.endpoint.get_publisher_count()
            if self.rmw == 'rmw_cyclonedds_cpp':
                try:
                    if self.observer is None:
                        self.observer = CycloneMatchedPeerObserver()
                    observation = self.observer.sample(self.domain, self.gid, 'reader')
                except Exception as exc:
                    observation = {'complete': False,
                                   'reason': 'native_peer_observation_failed:' + type(exc).__name__}
            elif self.rmw == 'rmw_fastrtps_cpp' and self.instrumentation is not None:
                try:
                    from .fastdds_peer_observer import FastddsMatchedPeerObserver
                    if self.observer is None:
                        self.observer = FastddsMatchedPeerObserver(
                            self.endpoint, instrumentation=self.instrumentation)
                    observation = self.observer.sample(self.domain, self.gid, 'reader')
                except Exception as exc:
                    observation = {'complete': False,
                                   'reason': 'native_peer_observation_failed:' + type(exc).__name__}
            value['public_count_after'] = self.endpoint.get_publisher_count()
            counts = (value['public_count_before'], value['public_count_after'])
            if any(type(count) is not int or count < 0 for count in counts):
                raise ValueError('public_publisher_count_invalid')
            value.update(_matched_peer_fields(self.observer, self.domain, self.gid,
                                             'reader', *counts, observation))
            if value['peer_gids_complete']:
                value.update(status='observed_sample', reason=None)
            else:
                value['reason'] = observation.get('reason', 'native_and_public_counts_differ')
        except Exception as exc:
            value.update(status='unknown', reason='peer_sample_failed:' + type(exc).__name__,
                         peer_gids_complete=False)
        value['sample_finished_ns'] = time.monotonic_ns()
        clock = value['sample_clock_identity']
        if (not isinstance(clock, dict) or not isinstance(clock.get('boot_id'), str)
                or not clock['boot_id'] or type(clock.get('device')) is not int
                or type(clock.get('inode')) is not int
                or clock['device'] <= 0 or clock['inode'] <= 0
                or _monotonic_clock_identity() != clock):
            value.update(status='unknown', reason='sample_clock_unavailable_or_changed',
                         peer_gids_complete=False)
        return value


def _drain_filter_decisions(observer, emit, *, final=False):
    """Forward actual native returns unchanged; overflow is a tool failure."""
    records, stats = observer.drain()
    for receipt in records:
        emit("native_content_filter_decision", receipt=receipt, candidate_eligible=False)
    if records or final or stats["dropped"] or stats["resolve_status"] == 2:
        emit("native_filter_trace_health", manifest_reference=observer.reference,
             stats=stats, final=final, capture_complete=stats["dropped"] == 0,
             candidate_eligible=False)
    if stats["dropped"] or stats["resolve_status"] == 2:
        raise TopologyBackendError("native_filter_trace_overflow_or_resolver_failure")
    return stats


def _worker_main(argv):
    """Child entry point: one endpoint generation, strict JSONL on stdout."""
    if len(argv) != 3:
        print(json.dumps({"event": "worker_error", "error": "usage: config_json role"}))
        return 2
    config = json.loads(argv[1])
    role = argv[2]
    kind = worker_role_kind(role)
    if kind is None:
        print(json.dumps({"event": "worker_error", "error": "unsupported_role"}))
        return 2
    from r2f.instrumentation.fastdds_peer.activation import (
        reference_from_config, manifest_for_reference, verify_vendor_mapping,
    )
    instrumentation = reference_from_config(config)
    import rclpy
    from rclpy.context import Context
    from rclpy.executors import SingleThreadedExecutor
    try:
        from rclpy.event_handler import PublisherEventCallbacks, SubscriptionEventCallbacks, UnsupportedEventTypeError
    except ImportError:
        PublisherEventCallbacks = SubscriptionEventCallbacks = UnsupportedEventTypeError = None
    from rclpy.node import Node
    from rclpy.qos import QoSProfile
    from std_msgs.msg import String

    context = Context()
    rclpy.init(context=context)
    rmw_identifier = rclpy.get_rmw_implementation_identifier()
    if instrumentation is not None and rmw_identifier != "rmw_fastrtps_cpp":
        raise TopologyBackendError("instrumented_worker_rmw_mismatch")
    vendor_identity = (verify_vendor_mapping(instrumentation)
                       if rmw_identifier == "rmw_fastrtps_cpp" else None)
    try:
        observed_domain_id = context.get_domain_id()
    except (AttributeError, RuntimeError, TypeError):
        observed_domain_id = None
    start_ticks = int(Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19])
    node_token = uuid.uuid4().hex
    node_name = f"r2f_topo_{config['logical_id']}_g{config['generation']}_{node_token[:8]}"
    node = Node(node_name, context=context, enable_rosout=False, start_parameter_services=False)
    take_capability = {"status": "unavailable", "reason": "unsupported_rmw_identifier",
                       "original_rclpy_dict_provided_gid": False}
    if kind == "reader" and rmw_identifier == "rmw_fastrtps_cpp":
        from .rcl_take_bridge import make_full_info_executor, BridgeUnavailable
        try:
            executor = make_full_info_executor(context=context, rmw_identifier=rmw_identifier,
                **({"instrumentation": instrumentation} if instrumentation is not None else {}))
            take_capability = {"status": "observed", "reason": None,
                               "observation_basis": "single_take_owner_public_rcl_api",
                               "original_rclpy_dict_provided_gid": False,
                               "abi": executor.take_bridge.abi_receipt}
        except BridgeUnavailable as exc:
            if instrumentation is not None:
                raise TopologyBackendError("instrumented_take_bridge_unavailable:" + str(exc)) from exc
            # Explicitly keep the stock path if this pinned public ABI is absent.
            # Its callback still has unavailable GID; no second consumer is added.
            executor = SingleThreadedExecutor(context=context)
            take_capability = dict(exc.capability_receipt)
    else:
        executor = SingleThreadedExecutor(context=context)
    executor.add_node(node)
    local_gid = None
    native_peer_observer = None
    filter_decision_observer = None
    incarnation_ordinal = 0
    receive_sequence = 0
    endpoint_generation = node_token

    def emit(event, **fields):
        if event == "command_done" and filter_decision_observer is not None:
            _drain_filter_decisions(filter_decision_observer, emit)
        value = {
            "event": event, "time_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(),
            "monotonic_clock_identity": _monotonic_clock_identity(),
            "role": role, "logical_id": config["logical_id"], "generation": config["generation"],
            "pid": os.getpid(), "pgid": os.getpgrp(), "process_start_ticks": start_ticks,
            "run_id": config["run_id"], "domain_id": config["domain_id"],
            "observed_domain_id": observed_domain_id,
            "domain_observation_source": "rclpy.context.get_domain_id" if observed_domain_id is not None else "unavailable_api",
            "rmw_identifier": rmw_identifier, "node_name": node_name,
            "endpoint_generation": endpoint_generation,
            "observation_mode": config.get("observation_mode", "blackbox"),
            "topic": config["topic"], "message_type": config["type_name"],
        }
        value.update(fields)
        print(json.dumps(value, sort_keys=True), flush=True)
        return value

    if instrumentation is not None:
        selected = manifest_for_reference(instrumentation)
        emit("native_peer_provider_selected", manifest_reference=instrumentation,
             vendor_identity=vendor_identity, provider_binary=selected["binaries"]["provider"],
             vendor_binary=selected["binaries"]["vendor"],
             provider_source_sha256=selected["provider_source_sha256"],
             provider_abi=selected["patch"]["abi"], candidate_eligible=False)
        if selected.get("filter_trace") is not None:
            from r2f.instrumentation.fastdds_filter_trace.observer import FilterDecisionObserver
            filter_decision_observer = FilterDecisionObserver(selected)
            emit("native_filter_trace_provider_selected",
                 manifest_reference=filter_decision_observer.reference,
                 peer_manifest_reference=instrumentation,
                 binary=filter_decision_observer.manifest["binary"],
                 vendor_binary=selected["binaries"]["vendor"],
                 provider_mapping=filter_decision_observer.mapping,
                 hook_source_sha256=filter_decision_observer.manifest["hook_source_sha256"],
                 abi=filter_decision_observer.manifest["abi"],
                 capture_scope="actual_evaluate_calls_only", candidate_eligible=False)

    def gid_bytes(value):
        return bytes(value).hex() if value is not None else None

    def receive(message, info):
        nonlocal receive_sequence
        receive_sequence += 1
        body = None
        try:
            body = json.loads(message.data)
        except (ValueError, TypeError):
            body = None
        get = info.get if isinstance(info, dict) else lambda key: getattr(info, key, None)
        publisher_gid = gid_bytes(get("publisher_gid"))
        from .message_info_receipt import capture_message_info
        # Preserve the full native take receipt rather than recreating the
        # stock rclpy four-field view and dropping the independently read GID.
        native_info = get("message_info_receipt")
        message_info = native_info if isinstance(native_info, dict) else capture_message_info(info)
        take_receipt = get("rcl_take_receipt")
        emit(
            "receive", rcl_take_receipt=take_receipt, message_info_receipt=message_info, send_sequence=body.get("send_sequence") if isinstance(body, dict) else None,
            phase=body.get("phase") if isinstance(body, dict) else None,
            receive_sequence=receive_sequence, reader_gid=local_gid,
            publisher_gid=publisher_gid,
            publisher_gid_status=(get("publisher_gid_status") or
                                  ("available" if publisher_gid is not None else "unavailable_api")),
            identity_basis="message_info_gid" if publisher_gid is not None else "endpoint_correlated",
            payload_sha256=__import__("hashlib").sha256(message.data.encode()).hexdigest(),
            string_data=message.data,
            payload_run_id=body.get("run_id") if isinstance(body, dict) else None,
            payload_writer_id=body.get("writer_id") if isinstance(body, dict) else None,
            payload_writer_generation=body.get("writer_generation") if isinstance(body, dict) else None,
            payload_endpoint_gid=body.get("endpoint_gid") if isinstance(body, dict) else None,
            declared_payload_sha256=body.get("declared_payload_sha256") if isinstance(body, dict) else None,
            message_info_type=type(info).__name__,
        )

    if kind == "reader":
        emit("topology_message_info_capability", **{
            **take_capability, "candidate_eligible": False, "filtering_effect_proven": False})
    qos = QoSProfile(**_public_qos_kwargs(config["qos"]))
    liveliness = _LivelinessEventCapture(kind, emit)
    callback_type = PublisherEventCallbacks if kind == "writer" else SubscriptionEventCallbacks
    callbacks = callback_type(liveliness=liveliness.callback) if callback_type is not None else None
    if callbacks is None:
        emit("topology_liveliness_capability", status="unavailable_api", endpoint_gid=None)
    publish_observer = None
    if kind == "writer":
        from .rmw_publish_observer import PublishOccurrenceObserver
        publish_observer = PublishOccurrenceObserver(emit, rmw_identifier)
    endpoint_creation_returned = False
    try:
        create_begin = emit("endpoint_create_begin", topic=config["topic"],
                            qos=config["qos"], message_type=config["type_name"])
        filter_configuration = None
        def create_endpoint():
            if kind == "writer":
                return node.create_publisher(String, config["topic"], qos, **(
                    {"event_callbacks": callbacks} if callbacks is not None else {})), None
            return _create_reader_with_initial_filter(
                node, String, config["topic"], receive, qos,
                config.get("initial_content_filter"), emit=emit, event_callbacks=callbacks)
        liveliness.create_begin_ns = time.monotonic_ns()
        endpoint, filter_configuration = _create_with_liveliness_audit(
            create_endpoint, UnsupportedEventTypeError, emit)
        endpoint_creation_returned = True
        liveliness.create_return_ns = time.monotonic_ns()
        created_ns = (filter_configuration["creation_returned_monotonic_ns"]
                      if filter_configuration is not None else time.monotonic_ns())

        def graph(*, authenticate=True):
            def entry(info):
                profile = info.qos_profile
                return {
                    "node_name": info.node_name, "node_namespace": info.node_namespace,
                    "topic_type": info.topic_type, "endpoint_gid": gid_bytes(info.endpoint_gid),
                    "qos": {
                        "reliability": profile.reliability.name.lower(),
                        "durability": profile.durability.name.lower(),
                        "depth": profile.depth,
                        "history": profile.history.name.lower(),
                        "deadline_ns": profile.deadline.nanoseconds,
                        "liveliness": profile.liveliness.name.lower(),
                        "liveliness_lease_duration_ns": profile.liveliness_lease_duration.nanoseconds,
                    },
                }
            publishers = [entry(x) for x in node.get_publishers_info_by_topic(endpoint.topic_name)]
            subscriptions = [entry(x) for x in node.get_subscriptions_info_by_topic(endpoint.topic_name)]
            nonlocal local_gid
            snapshot = {"publishers": publishers, "subscriptions": subscriptions}
            if authenticate:
                rows = publishers if kind == "writer" else subscriptions
                if local_gid is None and not any(x["node_name"] == node_name for x in rows):
                    return snapshot
                observed = _graph_local_gid(snapshot, node_name, kind)
                if local_gid is None:
                    local_gid = observed
                elif local_gid != observed:
                    raise TopologyBackendError("unreceipted_endpoint_identity_change")
            return snapshot

        initial_graph = graph()
        deadline = time.monotonic() + float(config.get("startup_timeout_sec", 10.0))
        while local_gid is None and time.monotonic() < deadline:
            executor.spin_once(timeout_sec=0.02)
            initial_graph = graph()
        if local_gid is None:
            emit("worker_error", error="endpoint_gid_not_observed_within_startup_timeout")
            return 1
        # Graph retains the independent remote contract and GID/type. Local
        # history/depth and resolved defaults come only from the actual public
        # endpoint API; endpoint.qos_profile is merely the supplied Python object.
        own_rows = initial_graph["publishers" if kind == "writer" else "subscriptions"]
        own = next((row for row in own_rows if row["endpoint_gid"] == local_gid), {})
        from .actual_qos import read_public_endpoint_qos
        from .qos_binding import compare_public_qos
        actual = read_public_endpoint_qos(endpoint, kind)
        comparison = compare_public_qos(config["qos"], actual["qos"])
        if comparison["status"] != "matched":
            raise RuntimeError("endpoint_actual_qos_request_mismatch:" + str(comparison["mismatches"]))
        emit("endpoint_created", endpoint_gid=local_gid, created_monotonic_ns=created_ns,
             create_begin_monotonic_ns=create_begin["monotonic_ns"],
             topic=endpoint.topic_name, qos=actual["qos"], message_type=own.get("topic_type"),
             requested_qos=config["qos"], configuration_source="public_endpoint_actual_qos",
             actual_endpoint_qos=actual, qos_request_comparison=comparison,
             graph_qos=own.get("qos"),
             identity_source="get_publishers_info_by_topic" if kind == "writer"
             else "get_subscriptions_info_by_topic",
             initial_content_filter=filter_configuration,
             initial_content_filter_request=config.get("initial_content_filter"))
        if kind == "reader":
            _observe_filter_state(endpoint, emit, command="create_endpoint", command_id=None,
                                  reader_gid=local_gid, phase="initial")
        liveliness.bind(local_gid)
        if kind == "reader":
            liveliness.peer_sampler = _LivelinessPeerSampler(
                endpoint, rmw_identifier, config["domain_id"], local_gid,
                instrumentation=instrumentation)
        emit("graph_snapshot", **initial_graph)

        pending_publish = None
        stdin_buffer = b""
        queue = []
        last_count = None
        last_graph_ns = 0
        while True:
            executor.spin_once(timeout_sec=0.01)
            if filter_decision_observer is not None:
                _drain_filter_decisions(filter_decision_observer, emit)
            now = time.monotonic_ns()
            count = (endpoint.get_subscription_count() if kind == "writer"
                     else endpoint.get_publisher_count())
            if count != last_count:
                emit("match", matched_count=count, endpoint_gid=local_gid)
                emit("graph_snapshot", **graph())
                last_count = count
            if now - last_graph_ns > 500_000_000:
                emit("graph_snapshot", **graph())
                last_graph_ns = now
            if sys.stdin is not None and selectors_for_stdin():
                chunk = os.read(sys.stdin.fileno(), 65536)
                if not chunk:
                    break
                stdin_buffer += chunk
                while b"\n" in stdin_buffer:
                    line, stdin_buffer = stdin_buffer.split(b"\n", 1)
                    queue.append(line)
            if not queue:
                continue
            command = json.loads(queue.pop(0))
            command_id = command.get("command_id")
            if command["command"] == "prepare_concurrent_publish":
                seqs = command.get("sequences")
                delay = command.get("interval_sec")
                if (pending_publish is not None or kind != "writer"
                        or not isinstance(command.get("concurrency_group"), str) or not command["concurrency_group"]
                        or not isinstance(seqs, list) or not 1 <= len(seqs) <= 1024
                        or any(type(n) is not int or n < 0 for n in seqs)
                        or len(set(seqs)) != len(seqs)
                        or type(delay) not in (int, float) or not math.isfinite(delay) or not 0 <= delay <= MAX_INTERVAL_SEC
                        or command.get("repeat_string_data") is not None
                        or (command.get("payload") is not None and
                            (not isinstance(command["payload"], str) or len(command["payload"]) > 1024))):
                    emit("worker_command_error", command_id=command_id, error="concurrent_prepare_invalid")
                    continue
                pending_publish = command
                emit("concurrent_publish_ready", command_id=command_id,
                     concurrency_group=command["concurrency_group"], endpoint_gid=local_gid)
                continue
            if command["command"] == "release_concurrent_publish":
                if (pending_publish is None or command.get("prepared_command_id") != pending_publish["command_id"]
                        or command.get("concurrency_group") != pending_publish["concurrency_group"]):
                    emit("worker_command_error", command_id=command_id, error="concurrent_release_invalid")
                    continue
                command = pending_publish
                pending_publish = None
                command_id = command["command_id"]
                command["command"] = "publish_batch"
                emit("concurrent_publish_started", command_id=command_id,
                     concurrency_group=command["concurrency_group"], endpoint_gid=local_gid)
            if command["command"] in ("stop", "destroy"):
                emit("worker_stopping", command_id=command_id)
                break
            if command["command"] == "snapshot":
                count_api = (endpoint.get_subscription_count if kind == "writer"
                             else endpoint.get_publisher_count)
                count_before = count_api()
                observer = None
                observation = {"complete": False, "reason": "rmw_has_no_public_peer_observer"}
                if rmw_identifier == "rmw_cyclonedds_cpp":
                    try:
                        observer = CycloneMatchedPeerObserver()
                        observation = observer.sample(config["domain_id"], local_gid, kind)
                    except (ValueError, OSError, AttributeError) as exc:
                        observation = {"complete": False, "reason": str(exc)}
                elif rmw_identifier == "rmw_fastrtps_cpp" and instrumentation is not None:
                    try:
                        from .fastdds_peer_observer import FastddsMatchedPeerObserver
                        if native_peer_observer is None:
                            native_peer_observer = FastddsMatchedPeerObserver(
                                endpoint, instrumentation=instrumentation)
                        observer = native_peer_observer
                        observation = observer.sample(config["domain_id"], local_gid, kind)
                    except (ValueError, OSError, AttributeError, RuntimeError) as exc:
                        observation = {"complete": False, "reason": str(exc),
                            "schema": "r2f.fastdds-native-matched-peer-observation/v1",
                            "manifest_reference": instrumentation}
                ownership = {"schema": "r2f.participant-ownership-unavailable/v1",
                             "complete": False, "status": "unavailable",
                             "candidate_eligible": False,
                             "domain_id": config["domain_id"], "local_gid": local_gid,
                             "role_kind": kind,
                             "reason": "rmw_participant_ownership_observer_unavailable"}
                if rmw_identifier == "rmw_cyclonedds_cpp":
                    try:
                        if observer is None:
                            raise ValueError("cyclone_ownership_observer_unavailable:"
                                             + str(observation.get("reason")))
                        ownership = observer.sample_ownership(config["domain_id"], local_gid, kind)
                    except (ValueError, TypeError, OSError, AttributeError, RuntimeError) as exc:
                        ownership.update(status="unknown", reason=str(exc))
                # Ownership is supporting diagnostic evidence, independent of
                # matched-peer proof and the two canonical public count samples.
                count_after = count_api()
                emit("participant_ownership_snapshot", command_id=command_id,
                     endpoint_gid=local_gid, supporting_diagnostic=True,
                     candidate_eligible=False, participant_ownership_observation=ownership)
                peer_observer_selected = (
                    rmw_identifier == "rmw_cyclonedds_cpp"
                    or (rmw_identifier == "rmw_fastrtps_cpp" and instrumentation is not None))
                if peer_observer_selected:
                    # Selection can fail before constructing an observer. Keep
                    # those attempted captures incomplete, even if observer=None.
                    peer_fields = _matched_peer_fields(
                        observer, config["domain_id"], local_gid, kind,
                        count_before, count_after, observation)
                else:
                    # A named API absence is diagnostic, not an incomplete
                    # peer-list claim. Preserve it without declaring peer proof.
                    peer_fields = {"peer_observation_limitation": observation["reason"]}
                # A changing interval cannot qualify a stable public count.
                # Keep both actual samples even when no peer observer exists.
                emit("match_snapshot", command_id=command_id,
                     matched_count=count_after if count_before == count_after else None,
                     public_count_before=count_before, public_count_after=count_after,
                     **peer_fields,
                     endpoint_gid=local_gid, sampled_monotonic_ns=time.monotonic_ns(),
                     count_api="get_subscription_count" if kind == "writer"
                     else "get_publisher_count")
                emit("graph_snapshot", command_id=command_id, **graph())
                emit("command_done", command_id=command_id)
            elif command["command"] == "update_filter" and kind == "reader":
                local_gid, incarnation_ordinal = _update_filter_identity(
                    endpoint, lambda: graph(authenticate=False), node_name, local_gid,
                    incarnation_ordinal, command, emit, state_observer=_observe_filter_state)
                liveliness.gid = local_gid
                if liveliness.peer_sampler is not None:
                    liveliness.peer_sampler.gid = local_gid
                emit("command_done", command="update_filter", command_id=command_id,
                     endpoint_gid=local_gid, reader_gid=local_gid,
                     incarnation_ordinal=incarnation_ordinal)
            elif command["command"] == "publish_batch" and kind == "writer":
                sequences = command.get("sequences")
                interval = command.get("interval_sec", 0.0)
                payload = command.get("payload")
                phase = command.get("phase") or "publish"
                if (not isinstance(sequences, list) or not sequences
                        or any(type(x) is not int or x < 0 for x in sequences)):
                    emit("worker_command_error", command_id=command_id,
                         error="invalid_sequences")
                    continue
                if (isinstance(interval, bool) or not isinstance(interval, (int, float))
                        or not math.isfinite(interval) or interval < 0 or interval > MAX_INTERVAL_SEC):
                    emit("worker_command_error", command_id=command_id,
                         error="invalid_interval")
                    continue
                if payload is not None and (not isinstance(payload, str) or len(payload) > 1024):
                    emit("worker_command_error", command_id=command_id,
                         error="invalid_payload")
                    continue
                repeat_data = command.get("repeat_string_data")
                if repeat_data is not None and (not isinstance(repeat_data, str) or len(sequences) != 1):
                    emit("worker_command_error", command_id=command_id, error="invalid_repeat_data")
                    continue
                for index, sequence in enumerate(sequences):
                    body = {
                        "run_id": config["run_id"], "send_sequence": sequence,
                        "phase": phase, "writer_id": config["logical_id"],
                        "writer_generation": config["generation"],
                        "endpoint_gid": local_gid,
                    }
                    if payload is not None:
                        body["declared_payload"] = payload
                        body["declared_payload_sha256"] = _sha256_text(payload)
                    message = String()
                    message.data = repeat_data if repeat_data is not None else json.dumps(body, sort_keys=True)
                    publish_begin_ns = time.monotonic_ns()
                    occurrence = publish_observer.publish(
                        endpoint, message, command_id=command_id,
                        send_sequence=sequence, writer_gid=local_gid)
                    publish_return_ns = time.monotonic_ns()
                    native_receipt = ({"publish_occurrence_receipt": occurrence}
                                      if rmw_identifier == "rmw_fastrtps_cpp" else {})
                    emit("send", **native_receipt,
                         publish_begin_ns=publish_begin_ns, publish_return_ns=publish_return_ns, send_sequence=sequence, phase=phase,
                         writer_gid=local_gid, publish_returned=True,
                         payload_sha256=_sha256_text(message.data), string_data=message.data,
                         declared_payload_sha256=body.get("declared_payload_sha256"),
                         command_id=command_id, burst=True)
                    if interval and index + 1 < len(sequences):
                        end = time.monotonic() + float(interval)
                        while time.monotonic() < end:
                            executor.spin_once(timeout_sec=min(0.01, max(0.0, end - time.monotonic())))
                if command.get("concurrency_group") is not None:
                    emit("concurrent_publish_finished", command_id=command_id,
                         concurrency_group=command["concurrency_group"], endpoint_gid=local_gid)
                emit("command_done", command_id=command_id, burst_count=len(sequences))
            elif command["command"] == "assert_liveliness" and kind == "writer":
                endpoint.assert_liveliness()
                emit("liveliness_asserted", command_id=command_id,
                     endpoint_gid=local_gid, api="rclpy.publisher.Publisher.assert_liveliness")
                emit("command_done", command_id=command_id)
            else:
                emit("worker_command_error", command_id=command_id,
                     error="unsupported_command:" + str(command.get("command")))
    finally:
        primary_error = sys.exc_info()[1]
        close_error = None
        if filter_decision_observer is not None:
            try:
                _drain_filter_decisions(filter_decision_observer, emit, final=True)
            except BaseException as exc:
                close_error = exc
        if publish_observer is not None:
            try:
                emit("publish_observer_closed", **publish_observer.close())
            except BaseException as exc:
                close_error = exc
        try:
            liveliness.close()
        except BaseException as exc:
            close_error = exc
        # The same cleanup auditor owns executor -> take bridge -> node ->
        # context teardown, so secondary close/telemetry errors cannot mask
        # an original worker failure or skip remaining resource cleanup.
        _audit_worker_cleanup(executor, node, context, emit,
                              endpoint_creation_returned=endpoint_creation_returned,
                              primary_error=primary_error, close_error=close_error)
    return 0


def selectors_for_stdin():
    import select
    return select.select([sys.stdin], [], [], 0)[0]


def worker_main():  # console/module entry used by RosTopologyBackend
    try:
        return _worker_main(sys.argv)
    except BaseException as exc:  # noqa: BLE001 - must report a real child failure
        print(json.dumps({
            "event": "worker_error", "pid": os.getpid(), "pgid": os.getpgrp(),
            "time_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(),
            "error": type(exc).__name__ + ":" + str(exc),
            "traceback": traceback.format_exc(),
        }), flush=True)
        return 1


class _EndpointProcess:
    """Own exactly one fresh worker process group; never signal a foreign PID."""

    def __init__(self, config, environment, output_dir, role, emit):
        self.config = dict(config)
        self.environment = dict(environment)
        self.output_dir = Path(output_dir)
        self.role = role
        self.emit = emit
        self.process = None
        self.stderr = None
        self.selector = None
        self.buffer = b""
        self.events = []
        self.command_serial = 0
        self.stop_command_id = None
        self.stop_requested_ns = None
        self.metadata = None

    def start(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        env = dict(self.environment)
        env.update({
            "ROS_DOMAIN_ID": str(self.config["domain_id"]),
            "RMW_IMPLEMENTATION": self.config["rmw"],
            "ROS_LOCALHOST_ONLY": "1",
            "ROS_LOG_DIR": str(self.output_dir / "ros_logs"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
        })
        attempt = 1
        stderr_path = self.output_dir / f"{self.role}.stderr.log"
        while True:
            try:
                self.stderr = stderr_path.open("xb")
                break
            except FileExistsError:
                attempt += 1
                stderr_path = self.output_dir / f"{self.role}.{attempt}.stderr.log"
        self.process = subprocess.Popen(
            ["/usr/bin/python3", "-B", "-m", "r2f.runtime.topology_worker",
             json.dumps(self.config, sort_keys=True), self.role],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
            env=env, start_new_session=True, cwd=self.config["source_repo"])
        os.set_blocking(self.process.stdout.fileno(), False)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.metadata = {
            "role": self.role, "pid": self.process.pid, "pgid": self.process.pid,
            "fresh_process": True, "separate_context": True,
            "logical_id": self.config["logical_id"], "generation": self.config["generation"],
            "worker_role": self.role, "worker_generation": self.config["generation"],
            "stderr_path": str(stderr_path),
        }
        self.emit("process_started", **self.metadata)
        return self.metadata

    def _dispatch(self, line):
        try:
            event = json.loads(line)
            if not isinstance(event, dict) or "event" not in event:
                raise ValueError("bad event")
            kind = event.pop("event")
            if kind.startswith("concurrent_publish_") and event.get("pgid") != self.process.pid:
                raise ValueError("concurrent pgid mismatch")
            if event.get("pid") not in (None, self.process.pid):
                raise ValueError("pid mismatch")
            event["pid"] = self.process.pid
            event["pgid"] = self.process.pid
            event["worker_role"] = self.role
            event["worker_generation"] = self.config["generation"]
            if kind == "endpoint_created":
                self.metadata.update({k: deepcopy(event.get(k)) for k in
                    ("logical_id", "generation", "endpoint_gid", "endpoint_generation", "process_start_ticks",
                     "rmw_identifier", "domain_id", "observed_domain_id", "domain_observation_source")})
            self.events.append({"event": kind, **event})
            self.emit(kind, **event)
        except (ValueError, TypeError):
            self.emit("worker_stdout_diagnostic", role=self.role,
                      text=line.decode(errors="replace")[:512])

    def pump(self, timeout=0.05):
        if self.selector is None:
            return
        for _, _ in self.selector.select(timeout):
            try:
                chunk = self.process.stdout.read()
            except (OSError, ValueError):
                chunk = b""
            if not chunk:
                continue
            self.buffer += chunk
            while b"\n" in self.buffer:
                line, self.buffer = self.buffer.split(b"\n", 1)
                if line.strip():
                    self._dispatch(line)

    def command(self, command, **fields):
        if self.process is None or self.process.poll() is not None:
            raise TopologyBackendError("worker_not_running:" + self.role)
        self.command_serial += 1
        command_id = self.command_serial
        payload = {"command": command, "command_id": command_id, **fields}
        if command in {"prepare_concurrent_publish", "release_concurrent_publish"}:
            created = next((e for e in reversed(self.events) if e.get("event") == "endpoint_created"), {})
            self.emit("concurrent_publish_requested" if command == "prepare_concurrent_publish" else "concurrent_publish_release",
                      concurrency_group=fields["concurrency_group"],
                      command_id=command_id if command == "prepare_concurrent_publish" else fields["prepared_command_id"],
                      **{k: created.get(k) for k in ("logical_id", "generation", "endpoint_gid", "pid", "pgid", "process_start_ticks", "endpoint_generation")})
        if command == "snapshot":
            created = next((e for e in reversed(self.events) if e.get("event") == "endpoint_created"), {})
            self.emit("topology_snapshot_requested", command="snapshot", command_id=command_id,
                      **{k: (self.metadata or {}).get(k, created.get(k)) for k in ("logical_id", "generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation")})
        data = (json.dumps(payload) + "\n").encode()
        audit = command == "assert_liveliness"
        if audit:
            created = next((e for e in reversed(self.events) if e.get("event") == "endpoint_created"), {})
            identity = {k: created.get(k) for k in ("logical_id", "generation", "endpoint_gid", "pid", "process_start_ticks", "endpoint_generation", "run_id")}
            identity["worker_role"] = self.role
            identity["worker_generation"] = self.config.get("generation")
            self.emit("topology_assert_liveliness_requested", command=command, command_id=command_id,
                      request_monotonic_ns=time.monotonic_ns(),
                      request_clock_identity=_monotonic_clock_identity(), **identity)
        written = None
        try:
            written = self.process.stdin.write(data)
            if written != len(data):
                raise OSError("short command pipe write")
            self.process.stdin.flush()
        except (BrokenPipeError, OSError, ValueError) as exc:
            if audit:
                self.emit("topology_assert_liveliness_write_result", command=command, command_id=command_id,
                          status="failed", bytes_written=written, bytes_expected=len(data),
                          error_type=type(exc).__name__, result_monotonic_ns=time.monotonic_ns(),
                          result_clock_identity=_monotonic_clock_identity(), **identity)
            raise TopologyBackendError("worker_pipe_closed:" + self.role) from exc
        if audit:
            self.emit("topology_assert_liveliness_write_result", command=command, command_id=command_id,
                      status="flushed", bytes_written=written, bytes_expected=len(data), error_type=None,
                      result_monotonic_ns=time.monotonic_ns(),
                      result_clock_identity=_monotonic_clock_identity(), **identity)
        if command in ("stop", "destroy"):
            self.stop_command_id = command_id
            self.stop_requested_ns = time.monotonic_ns()
        return command_id

    def has(self, event, **fields):
        return any(row.get("event") == event and all(row.get(k) == v for k, v in fields.items())
                   for row in self.events)

    def wait(self, predicate, timeout, label):
        deadline = time.monotonic() + float(timeout)
        while True:
            if predicate():
                return
            if self.process.poll() is not None:
                self.pump(0)
                if predicate():
                    return
                raise TopologyBackendError(
                    "worker_exited:" + label + ":" + str(self.process.returncode))
            if time.monotonic() >= deadline:
                raise TopologyBackendTimeout("timeout:" + label)
            self.pump(min(0.05, max(0.0, deadline - time.monotonic())))

    def close(self, timeout=3.0):
        if self.process is None:
            return {"closed": True, "returncode": None, "already": True}
        if self.metadata is not None and "returncode" in self.metadata:
            return {"closed": self.process.poll() is not None, "returncode": self.metadata["returncode"],
                    "pid": self.process.pid, "pgid": self.process.pid, "already": True}
        if self.process.poll() is None:
            try:
                self.command("stop")
            except TopologyBackendError:
                pass
            deadline = time.monotonic() + float(timeout)
            while self.process.poll() is None and time.monotonic() < deadline:
                self.pump(0.05)
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=2)
        self.pump(0)
        if self.selector is not None:
            try:
                self.selector.unregister(self.process.stdout)
            except (KeyError, ValueError):
                pass
            self.selector.close()
            # Keep the retired process/history for audit, but future backend
            # pumps must not select on its successfully closed epoll object.
            self.selector = None
        for handle in (self.process.stdin, self.process.stdout, self.stderr):
            try:
                handle.close()
            except (AttributeError, OSError):
                pass
        self.metadata["returncode"] = self.process.returncode
        self.emit("process_exit", **self.metadata)
        return {"closed": True, "returncode": self.process.returncode,
                "pid": self.process.pid, "pgid": self.process.pid}


class OfflineTopologyBackend:
    """Contract-test double.  Never ROS, never SUT evidence, never a candidate."""

    backend_name = OFFLINE_BACKEND
    real_ros = False
    supports_assert_liveliness = True

    def __init__(self, event_sink=None, *, auto_match=True, auto_delivery=True,
                 match_policy="all", graph_visibility="matched",
                 publisher_gid_available=True):
        # Legacy default contract: all same-topic opposite-role edges match
        # automatically, graph inventory is filtered by that match relation, and
        # simulated MessageInfo carries a publisher GID. The ambiguous N-graph
        # and no-MessageInfo-GID shapes exist only when a caller explicitly asks
        # for them; defaults never change under test convenience.
        if match_policy not in {"one_to_one", "all"}:
            raise ValueError("offline_match_policy_must_be_one_to_one_or_all")
        if graph_visibility not in {"matched", "all_live_topic"}:
            raise ValueError("offline_graph_visibility_must_be_matched_or_all_live_topic")
        if type(publisher_gid_available) is not bool:
            raise ValueError("offline_publisher_gid_available_must_be_bool")
        self.match_policy = match_policy
        self.graph_visibility = graph_visibility
        self.publisher_gid_available = publisher_gid_available
        self.visibility = {}
        self.count_overrides = {}
        self.event_sink = event_sink
        self.serial = 0
        self.command_serial = 0
        self.handles = {}
        self.closed = False
        self.graph = {}
        self.writer_sees = {}
        self.reader_sees = {}
        self.prepared = None
        self.auto_match = auto_match
        self.auto_delivery = auto_delivery

    def set_event_sink(self, sink):
        self.event_sink = sink

    def _emit(self, event, **fields):
        value = {"event": event, "time_ns": time.time_ns(),
                 "monotonic_ns": time.monotonic_ns(),
                 "monotonic_clock_identity": _monotonic_clock_identity(),
                 "role": "offline_backend", "pid": os.getpid(), "pgid": os.getpgrp(),
                 "backend": self.backend_name, "real_ros": False}
        value.update(fields)
        if self.event_sink is not None:
            self.event_sink(event, **{key: item for key, item in value.items() if key != "event"})
        return value

    def prepare(self, program, config):
        if config.get("environment_mutation") is not None:
            # The offline fixture can never own a real namespace or tc window.
            self.prepared = {
                "ok": False, "backend": self.backend_name, "ros_execution": False,
                "reason": "blocked:offline_fixture_cannot_claim_environment_mutation",
                "execution_evidence": False, "candidate_eligible": False,
                "declared_environment_mutation": deepcopy(config.get("environment_mutation")),
            }
            self._emit("topology_backend_blocked", **self.prepared)
            return self.prepared
        self.prepared = {
            "ok": True, "backend": self.backend_name, "ros_execution": False,
            "run_id": config.get("run_id"),
            "execution_evidence": False, "candidate_eligible": False,
            "declared_environment": deepcopy(program.get("environment", {})),
            "observed_environment": {"backend": self.backend_name, "ros": False,
                                     "note": "offline fake backend; not ROS/DDS execution"},
            "limitations": ["offline_fake_backend_is_not_ros"],
            "fixture_observation_controls": {
                "match_policy": self.match_policy,
                "graph_visibility": self.graph_visibility,
                "publisher_gid_available": self.publisher_gid_available,
                "auto_match": self.auto_match,
                "auto_delivery": self.auto_delivery,
                "scope": "explicit_offline_fixture_control_not_live_semantics",
            },
        }
        self._emit("topology_backend_prepared", **self.prepared)
        return self.prepared

    def _fake_gid(self):
        return f"{0xFA000000000000000000000000000000 + self.serial:032x}"

    def _refresh_matches(self, topic):
        writers = sorted(h["logical_id"] for h in self.handles.values()
                         if h.get("alive") and h["topic"] == topic and h["role"] == "writer")
        readers = sorted(h["logical_id"] for h in self.handles.values()
                         if h.get("alive") and h["topic"] == topic and h["role"] == "reader")
        edges = set(zip(writers, readers)) if self.match_policy == "one_to_one" else {
            (writer, reader) for writer in writers for reader in readers}
        for writer in writers:
            for reader in readers:
                self.writer_sees[(writer, reader)] = (writer, reader) in edges
                self.reader_sees[(writer, reader)] = (writer, reader) in edges

    def set_match_edge(self, writer_id, reader_id, *, writer_sees=True, reader_sees=True):
        """Simulated match relation, independent of graph visibility."""
        self.writer_sees[(writer_id, reader_id)] = bool(writer_sees)
        self.reader_sees[(writer_id, reader_id)] = bool(reader_sees)

    def _graph_visible(self, observer_id, peer_id, peer_role):
        """Explicit per-observer override, else the selected fixture policy.

        ``matched`` reproduces the legacy filtered inventory. ``all_live_topic``
        is an explicit synthetic "full discovery inventory" control. Neither is
        a per-peer match receipt; only ``match_snapshot`` counts/edges claim a
        simulated match relation.
        """
        override = self.visibility.get((observer_id, peer_id))
        if override is not None:
            return override
        if self.graph_visibility == "all_live_topic":
            return True
        observer = self.handles.get(observer_id) or {}
        observer_role = observer.get("role")
        if observer_role == peer_role or peer_id == observer_id:
            # Same-role inventory has no match relation to filter by; the fake
            # backend lists every alive same-topic endpoint of that role.
            return True
        if peer_role == "reader":
            return bool(self.writer_sees.get((observer_id, peer_id), False))
        return bool(self.reader_sees.get((peer_id, observer_id), False))

    def set_graph_visibility(self, observer_id, peer_id, *, visible=True):
        """Simulated discovery inventory, never a per-peer match receipt."""
        self.visibility[(observer_id, peer_id)] = bool(visible)

    def set_matched_count(self, logical_id, count):
        """Explicit aggregate observation override for inconsistent fixtures."""
        if count is not None and (type(count) is not int or count < 0):
            raise ValueError("offline_matched_count_must_be_nonnegative_integer_or_none")
        if count is None:
            self.count_overrides.pop(logical_id, None)
        else:
            self.count_overrides[logical_id] = count

    def create_endpoint(self, spec, generation, worker_role):
        if self.closed:
            raise TopologyBackendError("backend_closed")
        initial_filter = (_initial_content_filter(spec["filter"], spec["role"])
                          if "filter" in spec else None)
        filter_configuration = None
        if initial_filter is not None:
            from .filter_backend import receipt
            filter_configuration = receipt(
                "filter_initial_configuration", scope="initial_only", fixture=True,
                backend=OFFLINE_BACKEND, real_ros=False, execution_evidence=False,
                requested=initial_filter, submitted=False, observed=None,
                is_cft_enabled=None, configured=None, configuration_status="unverified",
                readback_status="unavailable", reason="offline_fixture_no_cft_api",
                filter_effect_observed=None, filter_false_implies_unmatch=False,
                dynamic_update_covered=False, lease_covered=False)
            self._emit("content_filter_initial_configuration", logical_id=spec["logical_id"],
                       generation=generation, fixture=True, candidate_eligible=False,
                       execution_evidence=False, filter_receipt=filter_configuration)
        self.serial += 1
        gid = self._fake_gid()
        handle = {
            "logical_id": spec["logical_id"], "role": spec["role"],
            "generation": generation, "worker_role": worker_role,
            "topic": spec["topic"], "type_name": spec["type_name"],
            "qos": deepcopy(spec["qos"]), "endpoint_gid": gid,
            "pid": 200000 + self.serial, "pgid": 200000 + self.serial,
            "process_start_ticks": self.serial, "worker_generation": self.serial,
            "endpoint_generation": f"offline-generation-{self.serial}",
            "rmw_identifier": "offline_fake_no_ros",
            "identity_source": "offline_fake", "real_ros": False, "alive": True,
            "created_monotonic_ns": time.monotonic_ns(),
        }
        if filter_configuration is not None:
            handle["initial_content_filter"] = filter_configuration
        self.handles[spec["logical_id"]] = handle
        self.graph.setdefault(spec["topic"], {"writer": set(), "reader": set()})
        self.graph[spec["topic"]][spec["role"]].add(spec["logical_id"])
        if self.auto_match:
            self._refresh_matches(spec["topic"])
        self._emit("endpoint_created", logical_id=spec["logical_id"],
                   role=spec["role"], worker_role=worker_role,
                   generation=generation, endpoint_gid=gid, pid=handle["pid"],
                   pgid=handle["pgid"], process_start_ticks=handle["process_start_ticks"],
                   endpoint_generation=handle["endpoint_generation"],
                   topic=spec["topic"], qos=deepcopy(spec["qos"]),
                   message_type=spec["type_name"], identity_source="offline_fake",
                   configuration_source="offline_fixture",
                   **({"initial_content_filter": filter_configuration}
                      if filter_configuration is not None else {}))
        return handle

    def destroy_endpoint(self, handle):
        if self.closed:
            raise TopologyBackendError("backend_closed")
        handle = self.handles.get(handle["logical_id"], handle)
        if handle.get("alive") is not True:
            raise TopologyBackendError("endpoint_generation_not_alive")
        handle["alive"] = False
        self.graph.get(handle["topic"], {}).get(handle["role"], set()).discard(
            handle["logical_id"])
        for key in list(self.writer_sees):
            if handle["logical_id"] in key:
                self.writer_sees.pop(key, None)
                self.reader_sees.pop(key, None)
        identity = {k: handle[k] for k in ("logical_id", "generation", "endpoint_gid", "pid", "pgid",
                                           "process_start_ticks", "endpoint_generation")}
        # 'closed' describes the simulated handle's actual removal above.
        # Backend/real_ros/fixture identity explicitly prevent real qualification.
        self._emit("endpoint_destroyed", role=handle["role"], **identity,
                   returncode=0, cleanup_status="closed", fixture=True,
                   backend=self.backend_name, real_ros=False,
                   cleanup_scope="offline_simulated_endpoint")
        self._emit("topology_worker_exit", **identity, returncode=0, fixture=True,
                   cleanup_scope="offline_simulated_endpoint")
        return {"cleanup_status": "closed", "returncode": 0, **identity,
                "fixture": True, "real_ros": False, "cleanup_scope": "offline_simulated_endpoint"}

    def update_filter(self, handle, expression, parameters):
        self.command_serial += 1
        value = {"receipt_kind": "filter_update", "api_status": "unavailable_api",
                 "readback_status": "not_attempted", "applied": False,
                 "filter_effect_observed": None, "hit": False, "fixture": True}
        self._emit("content_filter_dynamic_update", command_id=self.command_serial,
                   reader_gid=handle.get("endpoint_gid"), filter_receipt=value,
                   **{k: handle.get(k) for k in ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation")})
        return {"command_id": self.command_serial, "filter_receipt": value}

    def publish_batch(self, handle, sequences, interval_sec, phase, payload=None, repeat_string_data=None):
        if self.closed:
            raise TopologyBackendError("backend_closed")
        if interval_sec < 0 or interval_sec > MAX_INTERVAL_SEC:
            raise TopologyUnsupported("burst_interval_not_expressible",
                                      "interval exceeds bounded worker input")
        self.command_serial += 1
        command_id = self.command_serial
        start_ns = time.monotonic_ns()
        for sequence in sequences:
            body = {"run_id": self.prepared_run_id(), "send_sequence": sequence,
                    "phase": phase, "writer_id": handle["logical_id"],
                    "writer_generation": handle["generation"],
                    "endpoint_gid": handle["endpoint_gid"]}
            if payload is not None:
                body.update(declared_payload=payload, declared_payload_sha256=_sha256_text(payload))
            string_data = repeat_string_data if repeat_string_data is not None else json.dumps(body, sort_keys=True)
            if repeat_string_data is not None:
                body = json.loads(string_data)
            payload_sha256 = _sha256_text(string_data)
            publish_begin_ns = time.monotonic_ns()
            publish_return_ns = time.monotonic_ns()  # simulated, not DDS
            self._emit("send", publish_begin_ns=publish_begin_ns, publish_return_ns=publish_return_ns, logical_id=handle["logical_id"],
                       role=handle["role"], generation=handle["generation"],
                       worker_role=handle["worker_role"], endpoint_gid=handle["endpoint_gid"],
                       writer_gid=handle["endpoint_gid"], send_sequence=sequence,
                       phase=phase, publish_returned=True, command_id=command_id,
                       burst=True, payload_sha256=payload_sha256, string_data=string_data,
                       pid=handle["pid"], pgid=handle["pgid"],
                       process_start_ticks=handle["process_start_ticks"],
                       endpoint_generation=handle["endpoint_generation"],
                       declared_payload_sha256=_sha256_text(payload) if payload is not None else None)
            if self.auto_delivery:
                for reader in self.handles.values():
                    if (reader["role"] == "reader" and reader.get("alive")
                            and reader["topic"] == handle["topic"]
                            and self.reader_sees.get((handle["logical_id"], reader["logical_id"]))):
                        self._emit(
                            "receive", logical_id=reader["logical_id"],
                            role=reader["role"], generation=reader["generation"],
                            endpoint_gid=reader["endpoint_gid"],
                            reader_gid=reader["endpoint_gid"],
                            publisher_gid=handle["endpoint_gid"] if self.publisher_gid_available else None,
                            publisher_gid_status="available" if self.publisher_gid_available else "unavailable_api",
                            identity_basis="message_info_gid" if self.publisher_gid_available else "endpoint_correlated_payload",
                            send_sequence=body["send_sequence"], phase=body["phase"],
                            payload_run_id=body["run_id"],
                            pid=reader["pid"], pgid=reader["pgid"],
                            process_start_ticks=reader["process_start_ticks"],
                            endpoint_generation=reader["endpoint_generation"],
                            payload_sha256=payload_sha256, string_data=string_data,
                            payload_writer_id=body["writer_id"],
                            payload_writer_generation=body["writer_generation"],
                            payload_endpoint_gid=body["endpoint_gid"],
                            backend=self.backend_name)
        end_ns = time.monotonic_ns()
        self._emit("command_done", command_id=command_id, burst_count=len(sequences),
                   logical_id=handle["logical_id"], role=handle["role"],
                   endpoint_gid=handle["endpoint_gid"], generation=handle["generation"],
                   pid=handle["pid"], pgid=handle["pgid"],
                   process_start_ticks=handle["process_start_ticks"],
                   endpoint_generation=handle["endpoint_generation"])
        return {"command_id": command_id, "sent_sequences": list(sequences),
                "start_ns": start_ns, "end_ns": end_ns,
                "controller_ack_count": 1, "per_sample_ack": False,
                "payload_binding": "declared_payload_embedded" if payload is not None
                else "canonical_metadata_only",
                "backend": self.backend_name}

    def snapshot(self, handles):
        if self.closed:
            raise TopologyBackendError("backend_closed")
        self.command_serial += 1
        command_id = self.command_serial
        snapshots = {}
        for handle in (handles.values() if isinstance(handles, dict) else handles):
            identity = {k: handle[k] for k in ("logical_id", "generation", "pid", "process_start_ticks", "endpoint_generation")}
            self._emit("topology_snapshot_requested", command="snapshot", command_id=command_id, **identity)
            topic = handle["topic"]
            rows = self.graph.get(topic, {"writer": set(), "reader": set()})
            publishers = [self.handles[eid]["endpoint_gid"] for eid in sorted(rows["writer"])
                          if self._graph_visible(handle["logical_id"], eid, "writer")]
            subscriptions = [self.handles[eid]["endpoint_gid"] for eid in sorted(rows["reader"])
                             if self._graph_visible(handle["logical_id"], eid, "reader")]
            if handle["role"] == "writer":
                matched_count = sum(self.writer_sees.get((handle["logical_id"], eid), False)
                                    for eid in rows["reader"])
            else:
                matched_count = sum(self.reader_sees.get((eid, handle["logical_id"]), False)
                                    for eid in rows["writer"])
            matched_count = self.count_overrides.get(handle["logical_id"], matched_count)
            graph = {
                "publishers": [{"endpoint_gid": gid, "node_name": "offline_fake",
                                "node_namespace": "/", "topic_type": handle["type_name"],
                                "qos": {}} for gid in publishers],
                "subscriptions": [{"endpoint_gid": gid, "node_name": "offline_fake",
                                   "node_namespace": "/", "topic_type": handle["type_name"],
                                   "qos": {}} for gid in subscriptions],
            }
            match = {"command_id": command_id, "matched_count": matched_count,
                     "endpoint_gid": handle["endpoint_gid"],
                     "sampled_monotonic_ns": time.monotonic_ns(),
                     "count_api": "get_subscription_count" if handle["role"] == "writer"
                     else "get_publisher_count"}
            snapshots[handle["logical_id"]] = {
                "match_snapshot": match, "graph_snapshot": graph,
                "command_id": command_id, "complete": True,
            }
            self._emit("match_snapshot", role=handle["role"], **identity, **match)
            self._emit("graph_snapshot", role=handle["role"], **identity,
                       endpoint_gid=handle["endpoint_gid"], command_id=command_id, **graph)
            self._emit("command_done", command_id=command_id, **identity)
        return {"snapshots": snapshots, "command_id": command_id,
                "complete": all(v["complete"] for v in snapshots.values())}

    def assert_liveliness(self, handle):
        if self.closed:
            raise TopologyBackendError("backend_closed")
        self._emit("liveliness_asserted", logical_id=handle["logical_id"],
                   generation=handle["generation"], endpoint_gid=handle["endpoint_gid"],
                   api="offline_fake_assert_liveliness", real_ros=False)
        return {"api": "offline_fake_assert_liveliness", "real_ros": False}

    def prepared_run_id(self):
        return (self.prepared or {}).get("run_id")

    def set_graph_edge(self, writer_id, reader_id, *, writer_sees=True, reader_sees=True):
        # Legacy convenience keeps its historical combined control. New tests
        # use independent set_match_edge / set_graph_visibility instead.
        self.set_match_edge(writer_id, reader_id, writer_sees=writer_sees, reader_sees=reader_sees)
        self.set_graph_visibility(writer_id, reader_id, visible=writer_sees)
        self.set_graph_visibility(reader_id, writer_id, visible=reader_sees)

    def deliver(self, writer_id, reader_id, sequence, *, publisher_gid=True, payload_run_id=None):
        writer = self.handles[writer_id]
        reader = self.handles[reader_id]
        self._emit("receive", logical_id=reader_id, role=reader["role"],
                   generation=reader["generation"], endpoint_gid=reader["endpoint_gid"],
                   reader_gid=reader["endpoint_gid"], send_sequence=sequence,
                   publisher_gid=writer["endpoint_gid"] if publisher_gid else None,
                   publisher_gid_status="available" if publisher_gid else "unavailable_api",
                   identity_basis="message_info_gid" if publisher_gid else "endpoint_correlated",
                   payload_run_id=payload_run_id, backend=self.backend_name)

    def pump(self, timeout=0.0):
        return 0

    def close(self):
        if self.closed:
            return {"already_closed": True, "closed": True}
        for handle in self.handles.values():
            if handle.get("alive"):
                identity = {k: handle[k] for k in ("logical_id", "generation", "endpoint_gid", "pid", "pgid",
                                                   "process_start_ticks", "endpoint_generation")}
                handle["alive"] = False
                self._emit("topology_worker_exit", **identity, returncode=0, fixture=True,
                           cleanup_scope="offline_simulated_endpoint")
        self.closed = True
        self._emit("topology_backend_closed", backend=self.backend_name, real_ros=False)
        return {"closed": True, "backend": self.backend_name}


class RosTopologyBackend:
    """Controller for one fresh worker process per endpoint generation."""

    backend_name = ROS_BACKEND
    real_ros = True
    supports_assert_liveliness = True

    def __init__(self, config, output_dir, event_sink=None):
        self.config = dict(config or {})
        self.output_dir = Path(output_dir).resolve()
        self.event_sink = event_sink
        self.preflight = None
        self.environment = None
        self.execution_lease = None
        self.execution_preflight = None
        self.execution_environment = None
        self.processes = []
        self.handles = {}
        self.closed = False
        self.netem_isolation = None
        self.instrumentation_reference = None

    def set_event_sink(self, sink):
        self.event_sink = sink

    def _emit(self, event, **fields):
        value = {"event": event, "time_ns": time.time_ns(),
                 "monotonic_ns": time.monotonic_ns(),
                 "role": "topology_backend", "pid": os.getpid(), "pgid": os.getpgrp(),
                 "backend": self.backend_name, "real_ros": True}
        value.update(fields)
        # Backend lifecycle timestamps are sampled here, not inherited from a worker.
        value["monotonic_clock_identity"] = _monotonic_clock_identity()
        if self.event_sink is not None:
            self.event_sink(event, **{key: item for key, item in value.items() if key != "event"})
        return value

    def prepare(self, program, config=None):
        cfg = dict(self.config)
        cfg.update(config or {})
        cfg.setdefault("source_repo", str(REPO))
        # Hard gate: this backend must refuse to touch a live domain unless an
        # explicit owner GO was supplied.  Direct API use cannot bypass it.
        if cfg.get("live_execution_go") is not True:
            receipt = {"ok": False, "backend": self.backend_name, "ros_execution": False,
                       "reason": "blocked:no_live_ros_go",
                       "requires": ["explicit owner GO for real ROS/DDS execution"],
                       "execution_evidence": False, "candidate_eligible": False}
            self.preflight = receipt
            self._emit("topology_backend_blocked", **receipt)
            return receipt
        # Environment mutation is only meaningful inside a verified isolated
        # namespace.  Direct backend use must fail exactly like the route gate.
        try:
            netem = normalize_topology_environment_mutation(cfg.get("environment_mutation"))
        except ValueError as exc:
            raise TopologyBackendBlocked(str(exc)) from exc
        if netem is not None:
            isolation = netem_isolation_receipt()
            if not isolation["isolated"]:
                raise TopologyBackendBlocked("topology_netem_requires_isolated_netns")
            self.netem_isolation = isolation
        # These are actual runtime-owned lease handles/environment measurements,
        # never cfg.live_execution_go / cfg.lease_receipts assertions.
        from .ros2_pair import verify_pair_lease
        try:
            locks = verify_pair_lease(self.execution_lease, cfg, self.output_dir)
        except (RuntimeError, OSError, ValueError) as exc:
            raise TopologyBackendBlocked(str(exc)) from exc
        env = self.execution_preflight
        if not isinstance(env, dict) or env.get("ok") is not True or not self.execution_environment:
            raise TopologyBackendBlocked("canonical_environment_preflight_missing")
        self.preflight = env
        self.environment = dict(self.execution_environment)
        from r2f.instrumentation.fastdds_peer.activation import (
            reference_from_config, manifest_for_reference, environment_for_reference,
        )
        try:
            reference = reference_from_config(cfg)
            if reference is not None:
                selected = manifest_for_reference(reference)
                # Keep the exact build receipt with this run; this is evidence
                # intake, not another runner, state machine or candidate store.
                self.output_dir.mkdir(parents=True, exist_ok=True)
                copied = self.output_dir / "native-peer-build-manifest.json"
                with copied.open("xb") as stream:
                    stream.write(Path(reference["manifest_path"]).read_bytes())
                reference = {**reference, "manifest_path": str(copied)}
                manifest_for_reference(reference)
                self.environment = environment_for_reference(self.environment, reference)
            self.instrumentation_reference = reference
        except (ValueError, OSError) as exc:
            raise TopologyBackendBlocked("native_peer_activation_failed:" + str(exc)) from exc
        activated = env
        receipt = {
            "ok": True, "backend": self.backend_name, "ros_execution": True,
            "declared_environment": deepcopy(program.get("environment", {})),
            "observed_environment": {
                "ros_distro": activated.get("ros_distro"),
                "rmw_availability": activated.get("rmw"),
                "rmw": None, "domain_id": None,
                "requested_rmw": cfg.get("rmw"), "requested_domain_id": cfg.get("domain_id"),
                "source": env.get("source"), "process_scan": env.get("process_scan"),
                "rtps_probe": env.get("rtps_probe"),
                "environment_mode": env.get("environment_mode"),
            },
            "lease_context": {"status": "held_canonical_three_leases", "locks": locks},
            "native_peer_instrumentation": deepcopy(self.instrumentation_reference),
            "limitations": [],
            "execution_evidence": False,
            "candidate_eligible": False,
        }
        self._emit("topology_backend_prepared", **receipt)
        return receipt

    def _start_ticks(self, pid):
        try:
            text = Path(f"/proc/{int(pid)}/stat").read_text()
            return int(text.rsplit(")", 1)[1].split()[19])
        except (OSError, ValueError, IndexError) as exc:
            raise TopologyBackendError("cannot_verify_process_start_ticks") from exc

    def create_endpoint(self, spec, generation, worker_role):
        if self.closed:
            raise TopologyBackendError("backend_closed")
        if self.environment is None:
            raise TopologyBackendError("backend_not_prepared")
        prepared = endpoint_expressibility(spec, self.config)
        endpoint_dir = (self.output_dir / "endpoints"
                        / re.sub(r"[^A-Za-z0-9_.-]", "_", spec["logical_id"])
                        / f"g{generation}")
        endpoint_dir.mkdir(parents=True, exist_ok=True)
        worker_config = {
            "run_id": self.config["run_id"], "domain_id": self.config["domain_id"],
            "rmw": self.config["rmw"], "source_repo": str(self.config.get("source_repo", REPO)),
            "logical_id": spec["logical_id"], "generation": generation,
            "topic": spec["topic"], "type_name": spec["type_name"],
            "qos": prepared["public_qos"],
            "startup_timeout_sec": float(self.config.get("startup_timeout_sec", 10.0)),
        }

        if self.instrumentation_reference is not None:
            worker_config["observation_mode"] = "instrumentation"
            worker_config["fastdds_peer_instrumentation"] = {
                key: self.instrumentation_reference[key]
                for key in ("manifest_path", "manifest_sha256")}
        if prepared["initial_content_filter"] is not None:
            worker_config["initial_content_filter"] = deepcopy(prepared["initial_content_filter"])

        def sink(event, **fields):
            value = dict(fields)
            value.setdefault("topology_logical_id", spec["logical_id"])
            value.setdefault("topology_generation", generation)
            value.setdefault("topology_worker_role", worker_role)
            value.setdefault("backend", self.backend_name)
            value.setdefault("real_ros", True)
            if self.event_sink is not None:
                self.event_sink(event, **value)

        process = _EndpointProcess(worker_config, self.environment, endpoint_dir,
                                   worker_role, sink)
        self.processes.append(process)
        try:
            process.start()
            pid = process.process.pid
            process.wait(lambda: process.has("endpoint_created", pid=pid),
                         worker_config["startup_timeout_sec"],
                         "topology_endpoint_created:" + spec["logical_id"])
            row = [e for e in process.events
                   if e.get("event") == "endpoint_created" and e.get("pid") == pid][-1]
            gid = row.get("endpoint_gid")
            if not isinstance(gid, str) or not re.fullmatch(r"[0-9a-fA-F]{2,}", gid):
                raise TopologyBackendError("endpoint_created_without_real_guid")
            if not isinstance(row.get("endpoint_generation"), str) or not row["endpoint_generation"]:
                raise TopologyBackendError("endpoint_created_without_worker_generation")
            if row.get("pgid") != pid:
                raise TopologyBackendError("endpoint_process_group_identity_mismatch")
            ticks = self._start_ticks(pid)
            if row.get("process_start_ticks") != ticks:
                raise TopologyBackendError("endpoint_process_start_ticks_mismatch")
            handle = {
                "logical_id": spec["logical_id"], "role": spec["role"],
                "generation": generation, "worker_role": worker_role,
                "topic": spec["topic"], "type_name": spec["type_name"],
                # Preserve child API observation separately from requested type.
                "message_type": row.get("message_type"),
                "run_id": row.get("run_id"), "domain_id": row.get("domain_id"),
                "monotonic_clock_identity": deepcopy(row.get("monotonic_clock_identity")),
                "incarnation_ordinal": 0,
                "qos": deepcopy(spec["qos"]), "public_qos": prepared["public_qos"],
                "endpoint_gid": gid, "endpoint_generation": row["endpoint_generation"],
                "pid": pid, "pgid": row.get("pgid"), "process_start_ticks": ticks,
                "rmw_identifier": row.get("rmw_identifier"),
                "observation_mode": row.get("observation_mode", "blackbox"),
                "observed_domain_id": row.get("observed_domain_id"),
                "node_name": row.get("node_name"),
                "identity_source": row.get("identity_source"),
                "binding": prepared["binding"],
                "process": process, "alive": True, "real_ros": True,
                "created_monotonic_ns": row.get("created_monotonic_ns"),
            }
            if prepared["initial_content_filter"] is not None:
                # Only child API results can populate this observation.
                handle["initial_content_filter"] = deepcopy(row.get("initial_content_filter"))
            self.handles[spec["logical_id"]] = handle
            self._emit("topology_endpoint_bound", logical_id=spec["logical_id"],
                       role=spec["role"], generation=generation, worker_role=worker_role,
                       endpoint_gid=gid, pid=pid,
                       endpoint_generation=handle["endpoint_generation"],
                       binding=prepared["binding"],
                       identity_source=handle["identity_source"])
            return handle
        except Exception:
            process.close()
            raise

    def destroy_endpoint(self, handle):
        process = handle.get("process")
        receipt = process.close() if process is not None else {"closed": False}
        handle["alive"] = False
        if receipt.get("closed"):
            self._emit("topology_worker_exit", returncode=receipt.get("returncode"),
                       **{k: handle.get(k) for k in ("logical_id", "generation", "endpoint_gid", "pid", "pgid", "process_start_ticks", "endpoint_generation")})
            self._emit("worker_exit", returncode=receipt.get("returncode"),
                       **{k: handle.get(k) for k in ("logical_id", "generation", "endpoint_gid", "pid", "pgid", "process_start_ticks", "endpoint_generation")})
        identity = {k: handle.get(k) for k in ("logical_id", "generation", "endpoint_gid", "pid", "pgid",
                                               "process_start_ticks", "endpoint_generation")}
        self._emit("topology_endpoint_destroyed", role=handle["role"], **identity,
                   returncode=receipt.get("returncode"),
                   cleanup_status="closed" if receipt.get("closed") else "unknown")
        return {"cleanup_status": "closed" if receipt.get("closed") else "unknown",
                "returncode": receipt.get("returncode"), **identity}

    def _batch(self, handle, command, **fields):
        process = handle["process"]
        command_id = process.command(command, **fields)
        try:
            process.wait(lambda: process.has("command_done", command_id=command_id),
                         float(self.config.get("publish_timeout_sec", 10.0)),
                         "topology_" + command + ":" + handle["logical_id"])
        except TopologyBackendError as exc:
            if isinstance(exc, TopologyBackendTimeout):
                raise TopologyBackendTimeout(
                    str(exc), command_id=command_id, endpoint_id=handle["logical_id"]) from exc
            raise
        errors = [e for e in process.events
                  if e.get("event") == "worker_command_error" and e.get("command_id") == command_id]
        if errors:
            raise TopologyBackendError("worker_command_error:" + str(errors[-1].get("error")))
        return command_id

    def update_filter(self, handle, expression, parameters):
        command_id = self._batch(handle, "update_filter", expression=expression, parameters=parameters)
        from .endpoint_incarnation import validate_filter_identity_update
        try:
            token = validate_filter_identity_update(
                handle["process"].events,
                {k: v for k, v in handle.items() if k != "process"}, command_id)
        except ValueError as exc:
            raise TopologyBackendError("filter_identity_validation_failed:" + str(exc)) from exc
        rows = [e for e in handle["process"].events if e.get("event") == "content_filter_dynamic_update"
                and e.get("command_id") == command_id]
        if len(rows) != 1:
            raise TopologyBackendError("dynamic_filter_receipt_missing_or_duplicate")
        identity = {key: token[key] for key in
                    ("endpoint_gid", "incarnation_ordinal", "identity_valid_after_ns")}
        handle.update(identity)
        handle["process"].metadata.update(identity)
        return {"command_id": command_id, "filter_receipt": deepcopy(rows[0].get("filter_receipt")),
                "worker_event": deepcopy(rows[0])}

    def publish_concurrent(self, group, requests):
        """Two authenticated workers; submit all, ready all, release all, bounded join.

        No timing evidence is synthesized. Caller owns normal delivery windows.
        On any failure retire the whole group before returning control.
        """
        if len(requests) != 2 or len({r["handle"]["logical_id"] for r in requests}) != 2:
            raise TopologyBackendError("concurrency_requires_two_writers")
        if self.closed:
            raise TopologyBackendError("backend_closed")
        timeout = float(self.config.get("publish_timeout_sec", 10.0))
        if not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise TopologyBackendError("concurrent_timeout_out_of_bounds")
        deadline = time.monotonic() + timeout
        pending = []
        def wait_all(event):
            while not all(p.has(event, command_id=cid) for _, p, cid in pending):
                if time.monotonic() >= deadline:
                    raise TopologyBackendTimeout("concurrent_publish_timeout:" + event)
                for _, process, _ in pending:
                    process.pump(min(0.01, max(0, deadline - time.monotonic())))
                    if process.process.poll() is not None:
                        raise TopologyBackendError("concurrent_worker_exit")
                    if any(e.get("event") in {"worker_error", "worker_command_error"} for e in process.events):
                        raise TopologyBackendError("concurrent_worker_error")
        try:
            for request in requests:
                process = request["handle"]["process"]
                cid = process.command("prepare_concurrent_publish", concurrency_group=group,
                    sequences=request["sequences"], interval_sec=request["interval_sec"],
                    phase=request["phase"], payload=request.get("payload"))
                pending.append((request, process, cid))
            wait_all("concurrent_publish_ready")
            # Authenticate every ready *before* release, not only after execution.
            for request, process, cid in pending:
                ready = [e for e in process.events if e.get("event") == "concurrent_publish_ready" and e.get("command_id") == cid]
                created = [e for e in process.events if e.get("event") == "endpoint_created"]
                keys = ("logical_id", "generation", "endpoint_gid", "pid", "pgid", "process_start_ticks", "endpoint_generation",
                        "run_id", "domain_id", "monotonic_clock_identity")
                if (len(ready) != 1 or len(created) != 1
                        or any(created[0].get(k) is None or ready[0].get(k) != created[0][k] for k in keys)
                        or ready[0].get("concurrency_group") != group
                        or type(ready[0].get("monotonic_ns")) is not int
                        or ready[0]["monotonic_ns"] < created[0]["monotonic_ns"]):
                    raise TopologyBackendError("concurrent_ready_authentication_failed")
            ready_clocks = [next(e["monotonic_clock_identity"] for e in process.events
                                 if e.get("event") == "concurrent_publish_ready" and e.get("command_id") == cid)
                            for _, process, cid in pending]
            if ready_clocks[0] != ready_clocks[1]:
                raise TopologyBackendError("concurrent_ready_clock_mismatch")
            # Flush every release before awaiting any completion.
            for request, process, cid in pending:
                process.command("release_concurrent_publish", concurrency_group=group, prepared_command_id=cid)
            wait_all("command_done")
            results = {}
            for request, process, cid in pending:
                sends = [e for e in process.events if e.get("event") == "send" and e.get("command_id") == cid]
                if [e.get("send_sequence") for e in sends] != request["sequences"]:
                    raise TopologyBackendError("concurrent_publish_send_set_mismatch")
                results[request["phase"]] = {
                    "command_id": cid, "sent_sequences": request["sequences"],
                    "start_ns": min(e["publish_begin_ns"] for e in sends),
                    "end_ns": max(e["publish_return_ns"] for e in sends),
                    "controller_ack_count": 1, "per_sample_ack": False,
                    "payload_binding": "declared_payload_embedded" if request.get("payload") is not None else "canonical_metadata_only",
                    "backend": self.backend_name}
            return results
        except BaseException:
            # Include the worker whose submit failed; one failed cleanup must
            # not prevent attempting cleanup of its peer or mask root cause.
            for request in requests:
                try:
                    self.destroy_endpoint(request["handle"])
                except Exception as cleanup_error:
                    self._emit("topology_tool_error", reason="concurrent_cleanup_error", concurrency_group=group,
                               logical_id=request["handle"]["logical_id"], error_type=type(cleanup_error).__name__)
            raise

    def publish_batch(self, handle, sequences, interval_sec, phase, payload=None, repeat_string_data=None):
        if self.closed:
            raise TopologyBackendError("backend_closed")
        if interval_sec < 0 or interval_sec > MAX_INTERVAL_SEC:
            raise TopologyUnsupported("burst_interval_not_expressible",
                                      "interval outside bounded worker input")
        command_id = self._batch(handle, "publish_batch", sequences=list(sequences),
                                 interval_sec=float(interval_sec), phase=phase,
                                 payload=payload, repeat_string_data=repeat_string_data)
        sends = [e for e in handle["process"].events if e.get("event") == "send"
                 and e.get("command_id") == command_id]
        sent = [e.get("send_sequence") for e in sends]
        if sent != list(sequences):
            raise TopologyBackendError("publish_batch_send_set_mismatch")
        return {
            "command_id": command_id, "sent_sequences": sent,
            "start_ns": min(e["monotonic_ns"] for e in sends),
            "end_ns": max(e["monotonic_ns"] for e in sends),
            "controller_ack_count": 1, "per_sample_ack": False,
            "payload_binding": "declared_payload_embedded" if payload is not None
            else "canonical_metadata_only",
            "backend": self.backend_name,
        }

    def snapshot(self, handles):
        if self.closed:
            raise TopologyBackendError("backend_closed")
        snapshots = {}
        for logical_id, handle in (handles.items() if isinstance(handles, dict) else handles):
            process = handle["process"]
            command_id = process.command("snapshot")
            try:
                process.wait(lambda: process.has("command_done", command_id=command_id),
                             float(self.config.get("snapshot_timeout_sec", 5.0)),
                             "topology_snapshot:" + logical_id)
            except TopologyBackendError as exc:
                if isinstance(exc, TopologyBackendTimeout):
                    raise TopologyBackendTimeout(str(exc), command_id=command_id,
                                                 endpoint_id=logical_id) from exc
                raise
            matches = [e for e in process.events if e.get("event") == "match_snapshot"
                       and e.get("command_id") == command_id]
            graphs = [e for e in process.events if e.get("event") == "graph_snapshot"
                      and e.get("command_id") == command_id]
            snapshots[logical_id] = {
                "match_snapshot": dict(matches[-1]) if matches else None,
                "graph_snapshot": dict(graphs[-1]) if graphs else None,
                "command_id": command_id,
                "complete": bool(matches and graphs),
            }
        return {"snapshots": snapshots, "command_id": None,
                "complete": all(v["complete"] for v in snapshots.values())}

    def assert_liveliness(self, handle):
        command_id = self._batch(handle, "assert_liveliness")
        rows = [e for e in handle["process"].events
                if e.get("event") == "liveliness_asserted" and e.get("command_id") == command_id]
        return {"api": rows[-1].get("api") if rows else None, "command_id": command_id,
                "real_ros": True}

    def pump(self, timeout=0.0):
        for process in self.processes:
            process.pump(timeout)

    def close(self):
        if self.closed:
            return {"already_closed": True, "closed": True}
        workers, errors = [], []
        for process in self.processes:
            try:
                receipt = process.close()
                workers.append(receipt)
                handle = next((h for h in self.handles.values() if h.get("process") is process and h.get("alive")), None)
                if handle is not None and receipt.get("closed"):
                    handle["alive"] = False
                    self._emit("topology_worker_exit", returncode=receipt.get("returncode"),
                               **{k: handle.get(k) for k in ("logical_id", "generation", "endpoint_gid", "pid", "pgid", "process_start_ticks", "endpoint_generation")})
                    self._emit("worker_exit", returncode=receipt.get("returncode"),
                               **{k: handle.get(k) for k in ("logical_id", "generation", "endpoint_gid", "pid", "pgid", "process_start_ticks", "endpoint_generation")})
            except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                errors.append({"error": type(exc).__name__, "detail": str(exc)})
        alive = [p.process.pid for p in self.processes
                 if p.process is not None and p.process.poll() is None]
        self.closed = not alive and not errors and all(w.get("closed") for w in workers)
        self._emit("topology_backend_closed", backend=self.backend_name,
                   workers=len(workers), closed=self.closed, survivors=alive, errors=errors,
                   returncodes=[w.get("returncode") for w in workers])
        return {"closed": self.closed, "backend": self.backend_name, "workers": workers,
                "survivors": alive, "errors": errors,
                "returncodes": [w.get("returncode") for w in workers]}


def normalize_topology_config(config):
    """Minimal config normalization for the topology backend; no ROS created."""
    if not isinstance(config, dict):
        raise ValueError("topology config must be an object")
    c = dict(config)
    c.setdefault("run_id", uuid.uuid4().hex)
    c.setdefault("campaign_id", c["run_id"])
    c.setdefault("domain_id", 173)
    if (isinstance(c["domain_id"], bool) or not isinstance(c["domain_id"], int)
            or not 0 <= c["domain_id"] <= 231):
        raise ValueError("domain_id must be an integer in 0..231")
    c.setdefault("rmw", "rmw_cyclonedds_cpp")
    if c["rmw"] not in RMW_NAMES:
        raise ValueError("unsupported_rmw:" + str(c["rmw"]))
    c.setdefault("source_repo", str(REPO))
    c.setdefault("startup_timeout_sec", 10.0)
    c.setdefault("publish_timeout_sec", 30.0)
    c.setdefault("snapshot_timeout_sec", 10.0)
    for key in ("startup_timeout_sec", "publish_timeout_sec", "snapshot_timeout_sec"):
        value = c[key]
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value <= 0):
            raise ValueError("invalid_timeout:" + key)
        c[key] = float(value)
    return c


if __name__ == "__main__":
    raise SystemExit(worker_main())
