"""Bounded S1 topology *inputs* and policy predictions, never execution evidence.

No ROS imports, context, runner, candidate path, or installed-SDK defaults.  Logical
``generation`` is an expectation; a backend must bind it to real GUID/PID evidence.
Missing QoS and environment information remains unknown.  A positive graph edge
means policy-compatible, NOT discovered, matched, delivered, or alive at runtime.

Rules are intentionally narrower than all of DDS: exact topic names, explicit
requested/offered QoS, optional exact-name type contract, literal partitions.
XTypes coercion and wildcard-only partition compatibility are unknown; a proven
shared literal partition is sufficient.  Content-filter evaluation
does not determine graph matching.  SPDP participant lease is not writer liveliness.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import random

SCHEMA = "r2f.s1-topology-program/v1"
EPISODE_CONFIG_SCHEMA = "r2f.s1-topology-episode-config/v1"
DEFAULT_BUDGET = {
    "max_writers": 4, "max_readers": 4, "max_endpoints": 8,
    "max_actions": 128, "max_batch_size": 64, "max_total_samples": 1024,
    "max_timeout_sec": 30.0, "max_generations": 16,
}
# Configurable ceilings keep offline generation, validation, and receipt size bounded.
_BUDGET_LIMITS = {
    "max_writers": 4, "max_readers": 4, "max_endpoints": 8,
    "max_actions": 512, "max_batch_size": 1024, "max_total_samples": 16384,
    "max_timeout_sec": 300.0, "max_generations": 1024,
}
RULE_REFS = {
    "discovery": "https://fast-dds.docs.eprosima.com/en/2.14.x/fastdds/discovery/discovery.html",
    "qos": "https://docs.ros.org/en/jazzy/Concepts/Intermediate/About-Quality-of-Service-Settings.html",
    "filter": "https://fast-dds.docs.eprosima.com/en/2.14.x/fastdds/dds_layer/topic/contentFilteredTopic/createContentFilteredTopic.html",
    "participant_lease": "https://github.com/eclipse-cyclonedds/cyclonedds/blob/master/docs/manual/config/discovery-behavior.rst",
    "writer_assert": "rclpy.publisher.Publisher.assert_liveliness (installed public SDK)",
    "model": SCHEMA,
}
_ENUMS = {
    "reliability": ("best_effort", "reliable"),
    "durability": ("volatile", "transient_local", "transient", "persistent"),
    "liveliness": ("automatic", "manual_by_participant", "manual_by_topic"),
    "ownership": ("shared", "exclusive"),
    "history": ("keep_last", "keep_all"),
}
_QOS_KEYS = set(_ENUMS) | {"deadline_sec", "lease_duration_sec", "partitions", "depth"}
_ENV_KEYS = {"backend_available", "transport_ready", "discovery_enabled",
             "type_matching", "filter_support", "filter_validity"}
_ACTION_PARAMS = {
    "create": (set(), set()), "destroy": (set(), set()),
    "recreate": (set(), set()), "assert_liveliness": (set(), set()),
    "publish_batch": ({"count", "start_sequence"}, {"interval_sec", "payload", "repeat_data_from", "concurrency_group"}),
    "update_filter": ({"expression", "parameters"}, set()),
    "snapshot": (set(), {"label", "timeout_sec"}),
    "lookup_topic": ({"scope", "name"}, set()),
    "update_participant_userdata": ({"data"}, set()),
    "barrier": ({"condition", "timeout_sec"}, {"endpoint_ids", "publish_action_ids"}),
}
MUTATORS = (
    "change_topic", "change_qos", "change_filter", "add_endpoint",
    "insert_action", "delete_action", "swap_independent", "structure_guard",
    "dependency_guard", "parameter_guard", "environment_guard",
)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def _hash(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _text(value, *, empty=False, limit=256):
    return isinstance(value, str) and (empty or bool(value)) and len(value) <= limit


def _userdata(value):
    if not _text(value, limit=256) or "\x00" in value:
        return False
    try:
        return len(value.encode("utf-8")) <= 256
    except UnicodeError:
        return False


def _int(value, lower, upper):
    return type(value) is int and lower <= value <= upper


def _number(value, lower, upper):
    return type(value) in (int, float) and math.isfinite(value) and lower <= value <= upper


def _budget(value=None):
    if value is None:
        value = {}
    if not isinstance(value, dict) or set(value) - set(DEFAULT_BUDGET):
        raise ValueError("budget: unknown fields or non-object")
    result = {**DEFAULT_BUDGET, **value}
    for name, limit in _BUDGET_LIMITS.items():
        low = 0 if name in ("max_writers", "max_readers", "max_endpoints") else 1
        valid = (_number(result[name], low, limit) if name == "max_timeout_sec"
                 else _int(result[name], low, limit))
        if not valid:
            raise ValueError(f"budget.{name}: expected {low}..{limit}, not bool/NaN")
    return result


def _error(errors, path, code, message):
    errors.append({"path": path, "code": code, "message": message})


def _object(value, required, optional, path, errors):
    if not isinstance(value, dict):
        _error(errors, path, "type", "expected JSON object")
        return False
    for key in sorted(required - set(value)):
        _error(errors, f"{path}.{key}", "missing", "required field")
    for key in sorted(set(value) - required - optional, key=str):
        _error(errors, f"{path}.{key}", "unknown", "unknown field; no silent passthrough")
    return required <= set(value)


def _episode_config(program, actions, errors):
    """Optional bounded input declaration, never checkpoint/reset evidence."""
    if "episode" not in program:
        return
    episode = program["episode"]
    path = "program.episode"
    if not _object(episode, {"schema", "max_seconds", "checkpoint_action_ids"},
                   {"reset_action_ids"}, path, errors):
        return
    if episode["schema"] != EPISODE_CONFIG_SCHEMA:
        _error(errors, path + ".schema", "schema", "unknown episode config schema")
    seconds = episode["max_seconds"]
    if not (type(seconds) in (int, float) and 0 < seconds <= 180 and math.isfinite(seconds)):
        _error(errors, path + ".max_seconds", "value", "finite positive seconds <=180, not bool")
    if len(actions) > 50:
        _error(errors, "program.actions", "episode_budget", "episode supports at most 50 actions")
    by_id = {a["action_id"]: a for a in actions
             if isinstance(a, dict) and _text(a.get("action_id"))}
    for key, op in (("checkpoint_action_ids", "snapshot"), ("reset_action_ids", "reset")):
        if key not in episode:
            continue
        refs = episode[key]
        if (not isinstance(refs, list) or (key == "checkpoint_action_ids" and not refs)
                or not all(_text(ref) for ref in refs) or len(set(refs)) != len(refs)):
            _error(errors, path + "." + key, "reference", "unique action ids required; checkpoints nonempty")
            continue
        for ref in refs:
            action = by_id.get(ref, {})
            valid = (action.get("op") == "snapshot" if op == "snapshot" else
                     action.get("op") in ("create", "recreate")
                     and type(action.get("generation")) is int and action["generation"] >= 2)
            if not valid:
                _error(errors, path + "." + key, "reference",
                       "requires existing " + op + " action: " + ref)
    if any(isinstance(a, dict) and isinstance(a.get("params"), dict)
           and "concurrency_group" in a["params"] for a in actions):
        _error(errors, "program.actions", "episode_concurrency", "episode adapter does not support concurrency groups")


def _shape(program):
    errors = []
    try:
        _canonical(program)
    except (ValueError, TypeError, RecursionError):
        return [{"path": "program", "code": "json", "message": "finite JSON required"}], None
    if not _object(program, {"schema", "endpoints", "actions"},
                   {"budget", "environment", "episode", "participant_control"}, "program", errors):
        return errors, None
    if program["schema"] != SCHEMA:
        _error(errors, "program.schema", "schema", f"expected {SCHEMA}")
    try:
        budget = _budget(program.get("budget", {}))
    except ValueError as exc:
        _error(errors, "program.budget", "budget", str(exc))
        budget = dict(DEFAULT_BUDGET)
    if program.get("budget", {}) is None:
        _error(errors, "program.budget", "type", "explicit null is not an omitted budget")
    env = program.get("environment", {})
    if _object(env, set(), _ENV_KEYS, "program.environment", errors):
        for key, value in env.items():
            if key == "type_matching":
                valid = value in (None, "exact_name", "assignable")
            else:
                valid = value is None or type(value) is bool
            if not valid:
                _error(errors, f"program.environment.{key}", "value", "invalid guard assumption")
    endpoints = program["endpoints"]
    if not isinstance(endpoints, list) or len(endpoints) > budget["max_endpoints"]:
        _error(errors, "program.endpoints", "budget", "expected bounded endpoint array")
        endpoints = []
    ids, counts = set(), {"writer": 0, "reader": 0}
    for i, ep in enumerate(endpoints):
        path = f"program.endpoints[{i}]"
        if not _object(ep, {"logical_id", "role", "topic", "type_name", "qos", "vendor", "participant_id"},
                       {"filter"}, path, errors):
            continue
        for key in ("logical_id", "topic", "type_name"):
            if not _text(ep[key]):
                _error(errors, f"{path}.{key}", "value", "expected bounded nonempty text")
        eid = ep["logical_id"]
        if isinstance(eid, str):
            if eid in ids:
                _error(errors, f"{path}.logical_id", "duplicate", "duplicate endpoint id")
            ids.add(eid)
        if not isinstance(ep["role"], str) or ep["role"] not in counts:
            _error(errors, f"{path}.role", "value", "expected writer or reader")
        else:
            counts[ep["role"]] += 1
        for key in ("vendor", "participant_id"):
            if ep[key] is not None and not _text(ep[key]):
                _error(errors, f"{path}.{key}", "identity", "expected declared text or unknown null")
        qos = ep["qos"]
        if _object(qos, set(), _QOS_KEYS, f"{path}.qos", errors):
            for key, value in qos.items():
                if value is None:
                    continue  # Explicit unknown, never SDK default.
                if key in _ENUMS:
                    valid = isinstance(value, str) and value in _ENUMS[key]
                elif key in ("deadline_sec", "lease_duration_sec"):
                    valid = value == "infinite" or _number(value, 0, 86400)
                elif key == "depth":
                    valid = _int(value, 1, 65536)
                else:
                    valid = (isinstance(value, list) and len(value) <= 16
                             and all(_text(v, empty=True) for v in value))
                if not valid:
                    _error(errors, f"{path}.qos.{key}", "value", "invalid/unsupported QoS value")
            if qos.get("history") == "keep_last" and qos.get("depth") is None:
                _error(errors, f"{path}.qos.depth", "missing", "keep_last needs explicit positive depth")
        if "filter" in ep:
            f = ep["filter"]
            if ep["role"] != "reader":
                _error(errors, f"{path}.filter", "role", "content filter belongs to reader")
            if _object(f, {"expression", "parameters", "evaluation"}, set(), f"{path}.filter", errors):
                if not _text(f["expression"], empty=True, limit=1024):
                    _error(errors, f"{path}.filter.expression", "value", "bounded expression text required")
                if (not isinstance(f["parameters"], list) or len(f["parameters"]) > 16
                        or any(not _text(p, empty=True) for p in f["parameters"])):
                    _error(errors, f"{path}.filter.parameters", "value", "bounded string array required")
                if f["evaluation"] is not None and type(f["evaluation"]) is not bool:
                    _error(errors, f"{path}.filter.evaluation", "value", "expected bool or unknown null")
    for role, count in counts.items():
        if count > budget[f"max_{role}s"]:
            _error(errors, "program.endpoints", "budget", f"{role} count exceeds budget")
    actions = program["actions"]
    if not isinstance(actions, list) or len(actions) > budget["max_actions"]:
        _error(errors, "program.actions", "budget", "expected bounded action array")
        actions = []
    aids, samples = set(), 0
    for i, action in enumerate(actions):
        path = f"program.actions[{i}]"
        if not _object(action, {"action_id", "op", "endpoint_id", "generation", "params"},
                       {"depends_on"}, path, errors):
            continue
        aid, op = action["action_id"], action["op"]
        if not _text(aid):
            _error(errors, f"{path}.action_id", "value", "bounded action id required")
        elif aid in aids:
            _error(errors, f"{path}.action_id", "duplicate", "duplicate action id")
        else:
            aids.add(aid)
        if not isinstance(op, str) or op not in _ACTION_PARAMS:
            _error(errors, f"{path}.op", "value", "unknown operation")
            continue
        eid, gen = action["endpoint_id"], action["generation"]
        if eid is None and op in ("snapshot", "barrier"):
            if gen is not None:
                _error(errors, f"{path}.generation", "value", "global action generation must be null")
        elif not isinstance(eid, str) or eid not in ids:
            _error(errors, f"{path}.endpoint_id", "reference", "unknown endpoint")
        elif not _int(gen, 1, budget["max_generations"]):
            _error(errors, f"{path}.generation", "value", "bounded positive logical generation required")
        deps = action.get("depends_on", [])
        if (not isinstance(deps, list) or any(not _text(d) for d in deps)
                or (all(isinstance(d, str) for d in deps) and len(set(deps)) != len(deps))):
            _error(errors, f"{path}.depends_on", "value", "unique bounded action-id array required")
        p = action["params"]
        required, optional = _ACTION_PARAMS[op]
        if not _object(p, required, optional, f"{path}.params", errors):
            continue
        if "timeout_sec" in p and not _number(p["timeout_sec"], 0.000001, budget["max_timeout_sec"]):
            _error(errors, f"{path}.params.timeout_sec", "budget", "positive bounded timeout required")
        if op == "publish_batch":
            if not _int(p["count"], 1, budget["max_batch_size"]):
                _error(errors, f"{path}.params.count", "budget", "positive bounded batch required")
            else:
                samples += p["count"]
            if not _int(p["start_sequence"], 0, 1000000000):
                _error(errors, f"{path}.params.start_sequence", "value", "bounded nonnegative integer required")
            if "interval_sec" in p and not _number(p["interval_sec"], 0, budget["max_timeout_sec"]):
                _error(errors, f"{path}.params.interval_sec", "budget", "bounded nonnegative interval required")
            if "repeat_data_from" in p and (not _text(p["repeat_data_from"], limit=128)
                    or p.get("count") != 1 or "payload" in p):
                _error(errors, f"{path}.params.repeat_data_from", "value", "single sample and prior action id required; payload prohibited")
            if "payload" in p and not _text(p["payload"], empty=True, limit=1024):
                _error(errors, f"{path}.params.payload", "value", "bounded payload text required")
        if op == "update_participant_userdata":
            if not _userdata(p["data"]):
                _error(errors, f"{path}.params.data", "value", "1..256 UTF-8 bytes without NUL required")
        if op == "lookup_topic":
            if p["scope"] not in ("participant", "local_domain", "global"):
                _error(errors, f"{path}.params.scope", "value", "unknown native lookup scope")
            if not _text(p["name"], limit=256) or "\x00" in p["name"]:
                _error(errors, f"{path}.params.name", "value", "bounded native topic name without NUL required")
        if op == "update_filter":
            if not _text(p["expression"], empty=True, limit=1024):
                _error(errors, f"{path}.params.expression", "value", "bounded expression required")
            if (not isinstance(p["parameters"], list) or len(p["parameters"]) > 16
                    or any(not _text(v, empty=True) for v in p["parameters"])):
                _error(errors, f"{path}.params.parameters", "value", "bounded string array required")
        if op == "snapshot" and "label" in p and not _text(p["label"], empty=True, limit=128):
            _error(errors, f"{path}.params.label", "value", "bounded label text required")
        if op == "barrier":
            if p["condition"] not in ("all_created", "matched", "delivery", "delivery_sentinel", "quiescent"):
                _error(errors, f"{path}.params.condition", "value", "unknown barrier condition")
            targets = p.get("endpoint_ids", [])
            if (not isinstance(targets, list) or any(not isinstance(t, str) or t not in ids for t in targets)
                    or (all(isinstance(t, str) for t in targets) and len(set(targets)) != len(targets))):
                _error(errors, f"{path}.params.endpoint_ids", "reference", "unique known endpoint ids required")
    # Sentinel references are input dependencies, never predicted delivery.
    endpoints = {e["logical_id"]: e for e in program["endpoints"]
                 if isinstance(e, dict) and isinstance(e.get("logical_id"), str)}
    prior, live = {}, {}
    for i, action in enumerate(actions):
        if not isinstance(action, dict) or not isinstance(action.get("params"), dict):
            continue
        op, eid, gen = action.get("op"), action.get("endpoint_id"), action.get("generation")
        p = action["params"]
        if op == "barrier" and p.get("condition") == "delivery_sentinel":
            refs, targets = p.get("publish_action_ids"), p.get("endpoint_ids")
            path = f"program.actions[{i}].params"
            valid_refs = (isinstance(refs, list) and bool(refs)
                          and all(isinstance(r, str) for r in refs)
                          and len(set(refs)) == len(refs))
            valid_targets = (isinstance(targets, list) and bool(targets)
                             and all(isinstance(t, str) and t in endpoints for t in targets))
            if not valid_refs:
                _error(errors, path, "sentinel_publish_references", "unique nonempty publish_action_ids required")
            if not valid_targets or not any(endpoints[t].get("role") == "reader" for t in targets):
                _error(errors, path, "sentinel_endpoint_scope", "explicit writer and reader endpoint_ids required")
            if valid_refs:
                for ref in refs:
                    pub = prior.get(ref, {})
                    pp = pub.get("params", {})
                    writer = pub.get("endpoint_id")
                    if (pub.get("op") != "publish_batch" or pp.get("count") != 1
                            or "repeat_data_from" in pp):
                        _error(errors, path, "sentinel_publish_reference", "requires preceding single non-repeated publish_batch: " + ref)
                    elif (not isinstance(writer, str) or not valid_targets or writer not in targets
                          or endpoints.get(writer, {}).get("role") != "writer"
                          or live.get(writer) != pub.get("generation")):
                        _error(errors, path, "sentinel_current_generation", "requires scoped current writer generation: " + ref)
            if valid_targets and any(t not in live for t in targets):
                _error(errors, path, "sentinel_current_generation", "sentinel endpoints must be alive")
        elif "publish_action_ids" in p:
            _error(errors, f"program.actions[{i}].params", "sentinel_parameter", "publish_action_ids only belongs to delivery_sentinel")
        # Shape errors already name invalid IDs. Do not use them as hash keys,
        # but keep validating later sentinel actions rather than returning early.
        if isinstance(eid, str) and eid in endpoints:
            if op in ("create", "recreate"):
                live[eid] = gen
            elif op == "destroy":
                live.pop(eid, None)
        aid = action.get("action_id")
        if _text(aid):
            prior[aid] = action
    groups = {}
    for i, action in enumerate(actions):
        if not isinstance(action, dict) or not isinstance(action.get("params"), dict):
            continue
        group = action["params"].get("concurrency_group")
        if "concurrency_group" in action["params"]:
            if not _text(group, limit=128):
                _error(errors, "program.actions", "concurrency_group", "bounded nonempty group required")
            elif action.get("op") == "publish_batch":
                groups.setdefault(group, []).append((i, action))
    for group, members in groups.items():
        indices = [i for i, _ in members]
        writers = [a.get("endpoint_id") for _, a in members]
        preceding = {a.get("action_id") for a in actions[:indices[0]]
                     if isinstance(a, dict) and isinstance(a.get("action_id"), str)}
        if (len(members) != 2 or indices != list(range(indices[0], indices[0] + len(members)))
                or any(not isinstance(w, str) for w in writers)
                or len({w for w in writers if isinstance(w, str)}) != len(writers)
                or any(not isinstance(a.get("depends_on", []), list)
                       or any(not isinstance(dep, str) or dep not in preceding for dep in a.get("depends_on", []))
                       for _, a in members)
                or any("repeat_data_from" in a["params"] for _, a in members)):
            _error(errors, "program.actions", "concurrency_group", "requires contiguous two distinct writers, no intra-group dependency or repeated data")
    _episode_config(program, actions, errors)
    if "participant_control" in program:
        from r2f.runtime.participant_groups import validate_control
        try:
            validate_control(program)
        except (ValueError, TypeError, KeyError) as exc:
            _error(errors, "program.participant_control", "participant_control_scope", str(exc))
    if samples > budget["max_total_samples"]:
        _error(errors, "program.actions", "budget", "total publications exceed sample budget")
    return errors, budget


def _require(name, value, *, kind="dependency", rule="model"):
    return {"id": name, "kind": kind, "status": "satisfied" if value is True else
            "unsatisfied" if value is False else "unknown", "expected": value,
            "rule_ref": RULE_REFS[rule]}


def _evaluate(program, count=None):
    endpoints = {e["logical_id"]: e for e in program["endpoints"]}
    states = {eid: {"generation": 0, "alive": False, "next_sequence": 0} for eid in endpoints}
    env = program.get("environment", {})
    actions = program["actions"] if count is None else program["actions"][:count]
    rows, completed, streams, bindings = [], {}, {}, []
    for index, action in enumerate(actions):
        aid, op, eid, gen = (action[k] for k in ("action_id", "op", "endpoint_id", "generation"))
        p, requires = action["params"], []
        for dep in action.get("depends_on", []):
            requires.append(_require("action:" + dep, completed.get(dep, False)))
        state = states.get(eid)
        ep = endpoints.get(eid)
        if state is not None:
            current = state["generation"]
            if op == "create":
                requires += [_require("first_generation", gen == 1 and current == 0)]
            elif op == "recreate":
                requires += [_require("previous_generation_exists", current > 0),
                             _require("next_generation", gen == current + 1)]
            else:
                requires += [_require("current_generation", gen == current),
                             _require("endpoint_alive", state["alive"])]
        if op in ("create", "recreate"):
            requires += [_require("backend_available", env.get("backend_available"), kind="environment")]
            f = ep.get("filter", {})
            if f.get("expression"):
                requires += [_require("filter_support", env.get("filter_support"), kind="environment", rule="filter"),
                             _require("filter_validity", env.get("filter_validity"), kind="environment", rule="filter")]
        if op == "update_filter":
            requires += [_require("reader_role", ep["role"] == "reader")]
        if op in ("publish_batch", "assert_liveliness", "update_participant_userdata"):
            requires += [_require("writer_role", ep["role"] == "writer")]
        if op == "publish_batch":
            requires += [_require("nonoverlapping_writer_generation_sequence",
                                  p["start_sequence"] >= state["next_sequence"])]
        if op == "assert_liveliness":
            policy = ep["qos"].get("liveliness")
            requires += [_require("public_writer_manual_by_topic", None if policy is None else
                                  policy == "manual_by_topic", rule="writer_assert")]
        if op == "barrier":
            if p["condition"] == "delivery_sentinel":
                for ref in p["publish_action_ids"]:
                    pub = next(a for a in actions if a["action_id"] == ref)
                    requires.append(_require("sentinel_current_generation:" + ref,
                                             states[pub["endpoint_id"]]["alive"] and
                                             states[pub["endpoint_id"]]["generation"] == pub["generation"]))
            for target in p.get("endpoint_ids", []):
                requires.append(_require("barrier_endpoint_alive:" + target, states[target]["alive"]))
            requires.append(_require("runtime_barrier:" + p["condition"], None, kind="observation"))
        if op == "update_participant_userdata":
            requires.append(_require("runtime_normal_participant_refresh", None, kind="observation"))
        if op == "lookup_topic":
            requires.append(_require("runtime_typed_topic_lookup", None, kind="observation"))
        if op == "snapshot":
            requires.append(_require("runtime_snapshot_receipt", None, kind="observation"))
        failed = any(r["status"] == "unsatisfied" for r in requires)
        unknown = any(r["status"] == "unknown" for r in requires)
        disposition = "expected_guard_reject" if failed else "requires_runtime" if unknown else "expected_apply"
        completed[aid] = False if failed else None if unknown else True
        rows.append({"action_id": aid, "index": index, "op": op, "endpoint_id": eid,
                     "generation": gen, "requires": requires, "disposition": disposition,
                     "prediction_only": True})
        if failed:
            continue
        if op in ("create", "recreate"):
            states[eid] = {"generation": gen, "alive": None if unknown else True, "next_sequence": 0}
            bindings.append({"logical_id": eid, "generation": gen,
                             "required": ["guid", "pid"], "status": "unbound"})
        elif op == "destroy":
            state["alive"] = None if unknown else False
        elif op == "publish_batch":
            key = (eid, gen)
            stream = streams.setdefault(key, {"writer_id": eid, "generation": gen,
                                             "planned_sequences": [], "action_ids": [],
                                             "prediction_only": True, "order_scope": "writer_generation"})
            stream["planned_sequences"].extend(range(p["start_sequence"], p["start_sequence"] + p["count"]))
            stream["action_ids"].append(aid)
            state["next_sequence"] = p["start_sequence"] + p["count"]
    return rows, states, list(streams.values()), bindings


def validate_program(program):
    """Return JSON even for malformed inputs; deliberate guard negatives are valid inputs.

    ``status=valid`` checks the bounded input structure, not runtime preconditions.
    ``guard_disposition=guard_violation`` is an expected rejection test, not SUT bug.
    Environment booleans are declared assumptions, never measured health evidence.
    """
    errors, budget = _shape(program)
    if errors:
        actions = program.get("actions", []) if isinstance(program, dict) else []
        rows = [{"action_id": a.get("action_id"), "index": i,
                 "requires": [_require("valid_structure", False, kind="structure")],
                 "disposition": "invalid_input", "prediction_only": True}
                for i, a in enumerate(actions) if isinstance(a, dict)] if isinstance(actions, list) else []
        return {"status": "invalid", "errors": errors, "actions": rows,
                "guard_disposition": "invalid_input", "execution_evidence": False}
    rows, _, streams, bindings = _evaluate(program)
    disposition = ("guard_violation" if any(r["disposition"] == "expected_guard_reject" for r in rows)
                   else "requires_runtime" if any(r["disposition"] == "requires_runtime" for r in rows)
                   else "satisfied_under_declared_assumptions")
    return {"status": "valid", "errors": [], "actions": rows, "effective_budget": budget,
            "guard_disposition": disposition, "runtime_bindings": bindings,
            "delivery_streams": streams, "execution_evidence": False,
            "candidate_eligible": False, "environment_is_declared_assumption": True,
            "input_sha256": _hash(program)}


def _prefix(program, prefix):
    if prefix is None:
        return len(program["actions"])
    if type(prefix) is int and 0 <= prefix <= len(program["actions"]):
        return prefix
    if isinstance(prefix, str):
        for i, action in enumerate(program["actions"]):
            if action["action_id"] == prefix:
                return i + 1
    raise ValueError("prefix: expected action count or existing inclusive action_id")


def _pair(writer, reader, ws, rs, env):
    failures, unknown, rules = [], [], []
    if ws["alive"] is False or rs["alive"] is False:
        failures.append("endpoint_not_alive_in_prefix")
    elif ws["alive"] is None or rs["alive"] is None:
        unknown.append("endpoint_existence_pending_runtime_guard")
    if writer["topic"] != reader["topic"]:
        failures.append("topic_mismatch")
    if writer["type_name"] != reader["type_name"]:
        (failures if env.get("type_matching") == "exact_name" else unknown).append("type_name_mismatch")
    rules.append(RULE_REFS["discovery"])
    wq, rq = writer["qos"], reader["qos"]
    for key in ("reliability", "durability", "liveliness"):
        w, r = wq.get(key), rq.get(key)
        if w is None or r is None:
            unknown.append("unknown_" + key)
        elif _ENUMS[key].index(w) < _ENUMS[key].index(r):
            failures.append(key + "_requested_offered")
    for key in ("deadline_sec", "lease_duration_sec"):
        w, r = wq.get(key), rq.get(key)
        if w is None or r is None:
            unknown.append("unknown_" + key)
        elif (math.inf if w == "infinite" else w) > (math.inf if r == "infinite" else r):
            failures.append(key + "_requested_offered")
    w, r = wq.get("ownership"), rq.get("ownership")
    if w is None or r is None:
        unknown.append("unknown_ownership")
    elif w != r:
        failures.append("ownership_mismatch")
    w, r = wq.get("partitions"), rq.get("partitions")
    if w is None or r is None:
        unknown.append("unknown_partitions")
    else:
        # DDS empty sequence means the default empty-name partition. Wildcards
        # require DDS pattern semantics; Python fnmatch is not a substitute.
        w, r = w or [""], r or [""]
        patterns = {name for name in w + r if any(c in name for c in "*?[]\\")}
        # Identical pattern tokens are not proof of a shared literal partition.
        # A known literal overlap suffices even if other entries are patterns;
        # otherwise leave vendor-dependent pattern semantics unevaluated.
        if not (set(w) & set(r)) - patterns:
            if patterns:
                unknown.append("partition_pattern_not_evaluated")
            else:
                failures.append("partition_mismatch")
    rules.append(RULE_REFS["qos"])
    for key in ("transport_ready", "discovery_enabled"):
        if env.get(key) is not True:
            unknown.append("unverified_environment_" + key)
    status = "forbidden" if failures else "unknown" if unknown else "allowed"
    return {"writer_id": writer["logical_id"], "reader_id": reader["logical_id"],
            "writer_generation": ws["generation"] or None, "reader_generation": rs["generation"] or None,
            "status": status, "reasons": failures + unknown, "rule_refs": rules,
            "filter_changes_match": False, "execution_evidence": False}


def predict_graph(program, prefix=None):
    """Predict every directed writer-reader pair after a bounded action prefix.

    Known incompatibility wins over unknown observations.  Filter evaluation and
    cross-vendor labels never invent unmatch or match evidence.  The exact-name
    type restriction applies only when explicitly selected in environment.
    """
    validation = validate_program(program)
    if validation["status"] != "valid":
        return {"status": "invalid", "errors": validation["errors"],
                "pairs": [], "allowed": [], "forbidden": [], "unknown": []}
    try:
        count = _prefix(program, prefix)
    except ValueError as exc:
        return {"status": "invalid", "errors": [{"path": "prefix", "code": "prefix", "message": str(exc)}],
                "pairs": [], "allowed": [], "forbidden": [], "unknown": []}
    rows, states, streams, bindings = _evaluate(program, count)
    writers = [e for e in program["endpoints"] if e["role"] == "writer"]
    readers = [e for e in program["endpoints"] if e["role"] == "reader"]
    pairs = [_pair(w, r, states[w["logical_id"]], states[r["logical_id"]],
                   program.get("environment", {})) for w in writers for r in readers]
    return {"status": "valid", "prefix_length": count, "pairs": pairs,
            **{kind: [p for p in pairs if p["status"] == kind] for kind in ("allowed", "forbidden", "unknown")},
            "actions": rows, "delivery_streams": streams, "runtime_bindings": bindings,
            "cross_writer_order_required": False, "execution_evidence": False,
            "prediction_kind": "policy_only_not_observed", "fingerprints": fingerprints(program, count)}


def fingerprints(program, prefix=None):
    """Identity-sensitive state/history fingerprints, not counts or runtime identities."""
    if validate_program(program)["status"] != "valid":
        raise ValueError("fingerprints requires structurally valid program")
    count = _prefix(program, prefix)
    _, states, streams, _ = _evaluate(program, count)
    control = ({"participant_control": program["participant_control"]}
               if "participant_control" in program else {})
    return {"input_sha256": _hash(program),
            "state_sha256": _hash({"endpoints": program["endpoints"], "states": states,
                                    "streams": streams, "environment": program.get("environment", {}), **control}),
            "history_sha256": _hash({"endpoints": program["endpoints"], "actions": program["actions"][:count],
                                      "environment": program.get("environment", {}), **control})}


def _qos():
    # Explicit authored input values, NOT inferred endpoint/SUT observations.
    return {"reliability": "reliable", "durability": "volatile", "deadline_sec": "infinite",
            "liveliness": "automatic", "lease_duration_sec": "infinite", "ownership": "shared",
            "partitions": [], "history": "keep_last", "depth": 16}


def _action(aid, op, eid=None, generation=None, params=None, depends_on=None):
    return {"action_id": aid, "op": op, "endpoint_id": eid, "generation": generation,
            "params": {} if params is None else params, "depends_on": depends_on or []}


def generate_program(seed=0, *, writers=None, readers=None, budget=None):
    """Authored boundary/random inputs; no vendor/participant identity is manufactured."""
    if type(seed) is not int:
        raise ValueError("seed must be integer, not bool")
    b = _budget(budget)
    rng = random.Random(seed)
    if writers is None:
        writers = rng.randint(0, min(b["max_writers"], b["max_endpoints"]))
    if readers is None:
        readers = rng.randint(0, min(b["max_readers"], b["max_endpoints"] - writers)) if type(writers) is int and 0 <= writers <= b["max_endpoints"] else 0
    if (not _int(writers, 0, b["max_writers"]) or not _int(readers, 0, b["max_readers"])
            or writers + readers > b["max_endpoints"]):
        raise ValueError("endpoint cardinality exceeds role/total budget")
    endpoints = []
    for role, number in (("writer", writers), ("reader", readers)):
        for i in range(number):
            qos = _qos()
            qos["reliability"] = rng.choice(_ENUMS["reliability"])
            endpoints.append({"logical_id": f"{role[0]}{i}", "role": role,
                              "topic": rng.choice(("/r2f/topology/a", "/r2f/topology/b")),
                              "type_name": "std_msgs/msg/String", "qos": qos,
                              "vendor": None, "participant_id": None})
    creation_order = endpoints[:]
    rng.shuffle(creation_order)
    actions = [_action("create_" + e["logical_id"], "create", e["logical_id"], 1) for e in creation_order]
    remaining = b["max_total_samples"]
    for e in endpoints:
        if e["role"] == "writer" and remaining:
            count = rng.randint(1, min(3, b["max_batch_size"], remaining))
            actions.append(_action("publish_" + e["logical_id"], "publish_batch", e["logical_id"], 1,
                                   {"count": count, "start_sequence": 0, "interval_sec": 0.01, "payload": "topology"}))
            remaining -= count
    actions.append(_action("snapshot_final", "snapshot", params={"label": "final"}))
    program = {"schema": SCHEMA, "budget": b, "endpoints": endpoints, "actions": actions,
               "environment": {"backend_available": True, "transport_ready": True,
                               "discovery_enabled": True, "type_matching": "exact_name",
                               "filter_support": None, "filter_validity": None}}
    if validate_program(program)["status"] != "valid":
        raise ValueError("budget cannot fit generated program; increase action/time limits or lower cardinality")
    return program


def _case(program, *, case_id, intent, receipt=None):
    validation = validate_program(program)
    return {"case_id": case_id, "program": program, "input_sha256": _hash(program), "intent": intent,
            "expected_guard_disposition": validation["guard_disposition"],
            "expected_action_dispositions": [{"action_id": r["action_id"], "disposition": r["disposition"],
                                               "requires": r["requires"]} for r in validation["actions"]],
            "validation": validation, "receipt": receipt, "execution_evidence": False,
            "candidate_eligible": False}


def seed_corpus(seed=0, *, random_cases=8, budget=None):
    """Cardinality/value boundaries plus bounded seeded variation, not a fixed matrix."""
    if type(seed) is not int or not _int(random_cases, 0, 64):
        raise ValueError("integer seed and random_cases=0..64 required")
    b = _budget(budget)
    w, r, total = b["max_writers"], b["max_readers"], b["max_endpoints"]
    counts = [(0, 0), (min(w, total), 0), (0, min(r, total)),
              (min(w, total), min(r, max(0, total - min(w, total)))),
              (min(w, 1, total), min(r, 1, max(0, total - min(w, 1, total))))]
    rng = random.Random(seed)
    counts += [(rng.randint(0, min(w, total)), None) for _ in range(random_cases)]
    cases, seen = [], set()
    for i, (nw, nr) in enumerate(counts):
        if nr is None:
            nr = rng.randint(0, min(r, total - nw))
        p = generate_program(seed + i, writers=nw, readers=nr, budget=b)
        # Deliberately sample compatible and incompatible QoS/topic policy space.
        if i % 2 == 0:
            for ep in p["endpoints"]:
                ep["topic"] = "/r2f/topology/a"
                ep["qos"] = _qos()
        digest = _hash(p)
        if digest not in seen:
            seen.add(digest)
            cases.append(_case(p, case_id=f"seed-{seed}-{i}", intent="legal_requires_runtime"))
    return cases


def _dependencies(program):
    """Explicit dependencies plus lifecycle/sequence and global observation fences."""
    deps, last, prior, fence = {}, {}, [], None
    for a in program["actions"]:
        aid, eid, op = a["action_id"], a["endpoint_id"], a["op"]
        refs = set(a.get("depends_on", []))
        refs.update(a["params"].get("publish_action_ids", []))
        if eid in last:
            refs.add(last[eid])
        if fence:
            refs.add(fence)
        if op in ("snapshot", "barrier") and eid is None:
            refs.update(prior)
        deps[aid] = refs
        if eid is not None:
            last[eid] = aid
        if op == "barrier":
            fence = aid
        prior.append(aid)
    return deps


def _new_id(actions, prefix):
    ids = {a["action_id"] for a in actions}
    i = 0
    while f"{prefix}_{i}" in ids:
        i += 1
    return f"{prefix}_{i}"


def mutate_program(program, mutator, *, seed=0):
    """One deterministic change with explicit positive/negative guard expectation.

    Inapplicable operations fail explicitly.  Legal deletion uses a dependency
    closure; swapping cannot cross lifecycle, sequence, or barrier dependencies.
    Invalid-structure output is intentional and cannot be executed as a SUT test.
    """
    if mutator not in MUTATORS or type(seed) is not int:
        raise ValueError("unknown mutator or non-integer seed")
    if validate_program(program)["status"] != "valid":
        raise ValueError("mutation requires structurally valid input")
    p = copy.deepcopy(program)
    rng, intent = random.Random(seed), "legal_requires_runtime"
    b, eps, actions = _budget(p.get("budget", {})), p["endpoints"], p["actions"]
    details = {}
    if mutator in ("change_topic", "change_qos", "change_filter"):
        choices = [e for e in eps if mutator != "change_filter" or e["role"] == "reader"]
        if not choices:
            raise ValueError(f"{mutator}: no applicable endpoint")
        ep = rng.choice(choices)
        if mutator == "change_topic":
            ep["topic"] = "/r2f/topology/changed" if ep["topic"] != "/r2f/topology/changed" else "/r2f/topology/a"
        elif mutator == "change_qos":
            ep["qos"]["reliability"] = "best_effort" if ep["qos"].get("reliability") == "reliable" else "reliable"
        else:
            f = ep.get("filter")
            ep["filter"] = ({"expression": "", "parameters": [], "evaluation": None}
                            if f and f["expression"] else
                            {"expression": "seq >= %0", "parameters": ["100"], "evaluation": False})
        details["endpoint_id"] = ep["logical_id"]
    elif mutator == "add_endpoint":
        roles = [role for role in ("writer", "reader")
                 if sum(e["role"] == role for e in eps) < b[f"max_{role}s"]]
        if not roles or len(eps) >= b["max_endpoints"] or len(actions) >= b["max_actions"]:
            raise ValueError("add_endpoint: endpoint/action budget exhausted")
        role = rng.choice(roles)
        ids = {e["logical_id"] for e in eps}
        index = 0
        while f"{role[0]}_added_{index}" in ids:
            index += 1
        eid = f"{role[0]}_added_{index}"
        eps.append({"logical_id": eid, "role": role, "topic": "/r2f/topology/a",
                    "type_name": "std_msgs/msg/String", "qos": _qos(), "vendor": None, "participant_id": None})
        actions.insert(0, _action(_new_id(actions, "create_added"), "create", eid, 1))
        details["endpoint_id"] = eid
    elif mutator == "insert_action":
        if len(actions) >= b["max_actions"]:
            raise ValueError("insert_action: action budget exhausted")
        index = rng.randrange(len(actions) + 1)
        action = _action(_new_id(actions, "snapshot_added"), "snapshot", params={"label": "mutation"})
        actions.insert(index, action)
        details["inserted_action_id"] = action["action_id"]
    elif mutator == "delete_action":
        if not actions:
            raise ValueError("delete_action: empty program")
        selected = rng.choice(actions)["action_id"]
        deps, removed = _dependencies(p), {selected}
        while True:
            extra = {aid for aid, refs in deps.items() if refs & removed} - removed
            if not extra:
                break
            removed.update(extra)
        p["actions"] = [a for a in actions if a["action_id"] not in removed]
        details["removed_action_ids"] = [a["action_id"] for a in actions if a["action_id"] in removed]
    elif mutator == "swap_independent":
        deps = _dependencies(p)
        choices = [i for i in range(len(actions) - 1)
                   if actions[i]["action_id"] not in deps[actions[i + 1]["action_id"]]
                   and actions[i + 1]["action_id"] not in deps[actions[i]["action_id"]]]
        if not choices:
            raise ValueError("swap_independent: no adjacent independent actions")
        i = rng.choice(choices)
        details["swapped_action_ids"] = [actions[i]["action_id"], actions[i + 1]["action_id"]]
        actions[i], actions[i + 1] = actions[i + 1], actions[i]
    elif mutator == "structure_guard":
        intent = "invalid_structure"
        a = copy.deepcopy(actions[0]) if actions else _action("duplicate", "snapshot")
        if not actions:
            actions.append(copy.deepcopy(a))
        actions.append(a)
        details["duplicate_action_id"] = a["action_id"]
    elif mutator == "dependency_guard":
        intent = "guard_negative"
        a = _action(_new_id(actions, "bad_dependency"), "snapshot", depends_on=["absent_dependency"])
        ids = {x["action_id"] for x in actions}
        while a["depends_on"][0] in ids:
            a["depends_on"][0] += "_missing"
        actions.insert(0, a)
    elif mutator == "parameter_guard":
        intent = "invalid_parameters"
        pubs = [a for a in actions if a["op"] == "publish_batch"]
        if not pubs:
            raise ValueError("parameter_guard: no publish_batch")
        rng.choice(pubs)["params"]["count"] = b["max_batch_size"] + 1
    else:
        intent = "guard_negative"
        env = p.setdefault("environment", {})
        if env.get("backend_available") is False:
            raise ValueError("environment_guard: backend guard already false")
        env["backend_available"] = False
    before, after = _hash(program), _hash(p)
    if before == after:
        raise ValueError(f"{mutator}: refusing silent no-op")
    v = validate_program(p)
    if intent == "legal_requires_runtime" and v["status"] != "valid":
        raise ValueError(f"{mutator}: resulting input exceeds budget or violates structure")
    if intent == "legal_requires_runtime" and v["guard_disposition"] == "guard_violation":
        raise ValueError(f"{mutator}: dependency-preserving operation would leave failed guards")
    receipt = {"mutator": mutator, "seed": seed, "before_sha256": before,
               "after_sha256": after, "intent": intent, "details": details, "execution_evidence": False}
    return _case(p, case_id=f"mutation-{mutator}-{seed}", intent=intent, receipt=receipt)


def mutate_chain(program, mutators, *, seed=0):
    """Compose input mutations, preserving each layer's before/after identity."""
    if (not isinstance(mutators, (list, tuple)) or not 1 <= len(mutators) <= 16
            or any(m not in MUTATORS for m in mutators) or type(seed) is not int):
        raise ValueError("chain requires 1..16 known mutators and integer seed")
    current, layers = copy.deepcopy(program), []
    for i, name in enumerate(mutators):
        result = mutate_program(current, name, seed=seed + i)
        layers.append(result["receipt"])
        current = result["program"]
        if result["validation"]["status"] == "invalid" and i + 1 < len(mutators):
            raise ValueError(f"chain layer {i}: invalid input is terminal; later layers not applied")
    final = _case(current, case_id=f"chain-{seed}", intent=result["intent"])
    final["layers"] = layers
    final["initial_sha256"] = _hash(program)
    return final
