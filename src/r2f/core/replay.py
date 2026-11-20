"""Replay audit capsules and an injected normal public-API conformance runner.

No exploit, wire-payload or vulnerability reproduction machinery is provided.
Input metadata can be constructed offline. Execution metadata is accepted only
with runtime origin, matching identity and verified raw evidence; it is not a
cryptographic attestation that the callback actually ran a ROS2 process.
"""

from dataclasses import dataclass, field
from enum import Enum

from ._json import integer, nonempty, sha256_digest, snapshot
from .evidence import (INPUT_SCHEMA, INPUT_SCHEMA_V2, INPUT_SCHEMA_VERSIONS,
                       canonical_hash, effective_input_hash)
from .execution import validate_execution
from .scenario import Scenario
from .oracle import _effect_names
from .state_machine import utc_now

CAPSULE_SCHEMA = "r2f.conformance-capsule/v1"
ACTION_PROGRAM_PAYLOAD_SCHEMA = "r2f.replay-action-program-payload/v1"


def _action_program_layout(scenario, submitted):
    """Derive a unique versioned layout from the saved generic contract.

    No module/target-name inference or mutation runs here. The layout adds no
    new effective-input hash keys: its values are determined by the already
    bound contract, operations and final input. Historical pair capsules omit it.
    """
    contract = scenario.target.exploration_contract
    if contract is None:
        return None
    surfaces = [surface for surface in contract.surfaces
                if surface.value_type == "action_program"]
    if not surfaces:
        return None
    if len(surfaces) != 1 or scenario.mutation_plan is None:
        raise ValueError("action-program payload requires one unique contract surface and plan")
    surface = surfaces[0]
    if (surface.surface_kind != "action_program" or len(surface.input_paths) != 1
            or {op.surface_id for op in scenario.mutation_plan.operations} != {surface.surface_id}):
        raise ValueError("action-program payload surface/path/plan mismatch")
    path = surface.input_paths[0]
    schema = surface.domain.get("program_schema")
    nonempty(path, "action-program input path")
    nonempty(schema, "action-program schema")
    payload = submitted
    for part in path.split("."):
        if not part or not isinstance(payload, dict) or set(payload) != {part}:
            raise ValueError("action-program final input must contain only its declared path")
        payload = payload[part]
    if not isinstance(payload, dict) or payload.get("schema") != schema:
        raise ValueError("action-program payload schema mismatch")
    if scenario.mutation_plan.contract_hash != contract.contract_hash():
        raise ValueError("action-program payload contract hash mismatch")
    return {"schema": ACTION_PROGRAM_PAYLOAD_SCHEMA, "surface_id": surface.surface_id,
            "input_path": path, "program_schema": schema,
            "contract_hash": contract.contract_hash(), "payload_sha256": canonical_hash(payload)}


class ReplayStatus(str, Enum):
    NOT_RUN = "not_run"
    PASS = "pass"
    FAIL_TIMING = "fail_timing"
    FAIL_ENVIRONMENT = "fail_environment"
    FAIL_ARTIFACT = "fail_artifact"
    INCONCLUSIVE = "inconclusive"


# Input-identity v2 additions that must never appear on a v1 capsule.  The
# envelope schema itself stays ``r2f.conformance-capsule/v1`` so old readers
# can still load v1 capsules unchanged.
_V2_ONLY_INPUT_KEYS = frozenset({
    "payload_layout",
    "input_identity_schema", "mutation_plan", "operations", "runtime_windows",
    "contract_hash", "input_model", "model_hash", "profile_id",
})
_PLAN_BINDING_KEYS = ("mutation_plan", "operations", "runtime_windows", "contract_hash")


def validate_replay_input(data):
    """Dispatch input-identity validation by the declared schema.

    Unknown versions are rejected; v2 must never silently fall back to v1.
    """
    if not isinstance(data, dict):
        raise ValueError("replay input must be a dict")
    schema = data.get("input_identity_schema")
    if schema not in INPUT_SCHEMA_VERSIONS:
        raise ValueError("unsupported replay input identity schema")
    if schema == INPUT_SCHEMA:
        return _validate_replay_input_v1(data)
    return _validate_replay_input_v2(data)


def _validate_replay_input_v1(data):
    # Historical path: byte-identical semantics and no extra accepted fields.
    leaked = set(data) & (_V2_ONLY_INPUT_KEYS - {"input_identity_schema"})
    if leaked:
        raise ValueError("v1 replay input cannot carry v2-only fields: " + ",".join(sorted(leaked)))
    effective = data.get("effective_input")
    if isinstance(effective, dict) and set(effective) & (_V2_ONLY_INPUT_KEYS - {"input_identity_schema"}):
        raise ValueError("v1 effective input cannot carry v2-only fields")
    digest = effective_input_hash(effective)
    if data.get("effective_input_sha256") != digest:
        raise ValueError("replay effective input SHA256 mismatch")
    scenario = Scenario.from_dict(data["scenario"])
    if scenario.target.to_dict() != data.get("target"):
        raise ValueError("replay target manifest mismatch")
    if canonical_hash(data["target"]) != effective.get("target_sha256"):
        raise ValueError("replay effective target manifest hash mismatch")
    for key in ("target_id", "target_revision", "seed", "rmw", "arm", "repeat", "mutation_kind", "carrier"):
        if data.get(key) != effective.get(key) or data.get(key) != scenario.to_dict().get(key):
            raise ValueError(f"replay input metadata mismatch: {key}")
    if data.get("state_steps") != effective.get("state_steps") or data.get("state_steps") != scenario.to_dict()["state_steps"]:
        raise ValueError("replay state plan mismatch")
    if data.get("mutation_parameters") != effective.get("mutation_parameters") or data.get("mutation_parameters") != scenario.mutation_parameters:
        raise ValueError("replay mutation parameters mismatch")
    if not isinstance(data.get("source_manifest"), dict) or not data["source_manifest"]:
        raise ValueError("replay source_manifest must be nonempty")
    if data["source_manifest"] != effective.get("source_manifest"):
        raise ValueError("replay source_manifest mismatch")
    for key in ("topic", "message_type"):
        nonempty(data.get(key), key)
    if not isinstance(data.get("qos"), dict):
        raise ValueError("replay qos must be a dict")
    submitted = effective["final_input"]
    if not isinstance(submitted, dict):
        raise ValueError("normal conformance final_input must be a config dict")
    for key in ("topic", "message_type", "qos"):
        if key in submitted and data[key] != submitted[key]:
            raise ValueError(f"replay {key} differs from final submitted config")
    if data.get("origin_domain") is not None:
        integer(data["origin_domain"], "origin_domain")
    return True


def _validate_replay_input_v2(data):
    """Validate a plan-bearing exploration input without regenerating it.

    The saved plan is authoritative: loading a capsule must not call a mutator
    or provider ``prepare``.  Every plan field is cross-bound to the effective
    input, the scenario, and the submitted final input.
    """
    effective = data.get("effective_input")
    if not isinstance(effective, dict):
        raise ValueError("v2 replay effective_input must be a dict")
    if effective.get("input_identity_schema") != INPUT_SCHEMA_V2:
        raise ValueError("v2 replay effective input must declare the v2 schema")
    digest = effective_input_hash(effective, schema=INPUT_SCHEMA_V2)
    if data.get("effective_input_sha256") != digest:
        raise ValueError("replay effective input SHA256 mismatch")

    has_plan = "mutation_plan" in effective or "mutation_plan" in data
    if has_plan:
        for key in _PLAN_BINDING_KEYS:
            if key not in data or key not in effective:
                raise ValueError(f"v2 replay input is missing plan binding field: {key}")
            if canonical_hash(data[key]) != canonical_hash(effective[key]):
                raise ValueError(f"v2 replay plan binding differs from effective input: {key}")
    else:
        partial = [key for key in _PLAN_BINDING_KEYS if key in data or key in effective]
        if partial:
            raise ValueError("v2 replay input declares partial plan fields: " + ",".join(sorted(partial)))

    scenario = Scenario.from_dict(data["scenario"])
    scenario_data = scenario.to_dict()
    if scenario.target.to_dict() != data.get("target"):
        raise ValueError("replay target manifest mismatch")
    if canonical_hash(data["target"]) != effective.get("target_sha256"):
        raise ValueError("replay effective target manifest hash mismatch")
    for key in ("target_id", "target_revision", "seed", "rmw", "arm", "repeat", "mutation_kind", "carrier"):
        if data.get(key) != effective.get(key) or data.get(key) != scenario_data.get(key):
            raise ValueError(f"replay input metadata mismatch: {key}")
    if data.get("state_steps") != effective.get("state_steps") or data.get("state_steps") != scenario_data["state_steps"]:
        raise ValueError("replay state plan mismatch")
    if data.get("mutation_parameters") != effective.get("mutation_parameters") or data.get("mutation_parameters") != scenario.mutation_parameters:
        raise ValueError("replay mutation parameters mismatch")

    if has_plan:
        saved_plan = data["mutation_plan"]
        if canonical_hash(scenario_data.get("mutation_plan")) != canonical_hash(saved_plan):
            raise ValueError("replay scenario mutation plan differs from capsule plan")
        from .mutation_plan import MutationPlan, _validate_multiwindow_input
        plan = MutationPlan.from_dict(saved_plan)
        if canonical_hash(plan.final_config) != canonical_hash(effective["final_input"]):
            raise ValueError("replay mutation plan final_config differs from submitted final input")
        if canonical_hash(scenario.mutation_plan.to_dict()) != canonical_hash(saved_plan):
            raise ValueError("replay scenario plan is not the saved plan")
        # Structural identity cannot depend on live route capabilities. With
        # capabilities=None, validate_plan may report unsupported before it
        # checks multi-window equality; never let that mask a different input.
        environment_windows = plan.final_config.get('environment_windows')
        if (len(plan.runtime_windows) > 1
                or isinstance(environment_windows, list) and len(environment_windows) > 1):
            _validate_multiwindow_input(plan)
    elif "mutation_plan" in scenario_data:
        raise ValueError("v2 replay scenario carries an unbounded mutation plan")

    if "contract" in effective:
        contract = effective["contract"]
        if not isinstance(contract, dict) or not contract:
            raise ValueError("v2 replay contract must be a nonempty dict")
        from .mutation_plan import ExplorationContract
        declared = ExplorationContract.from_dict(contract)
        if has_plan and data["contract_hash"] != declared.contract_hash():
            raise ValueError("replay plan contract_hash differs from the declared contract")

    # The saved effective input must be the scenario's v2 projection, not
    # merely a self-consistent hash over a different plan/contract/order.
    expected = scenario.effective_input_v2(
        final_input=effective['final_input'],
        input_layer_receipts=effective['input_layer_receipts'],
        source_manifest=data['source_manifest'])
    for key, value in expected.items():
        if canonical_hash(effective.get(key)) != canonical_hash(value):
            raise ValueError('v2 replay scenario/effective input mismatch: ' + key)
    if has_plan and scenario.target.exploration_contract is not None:
        from .mutation_plan import validate_plan
        result = validate_plan(scenario.mutation_plan, contract=scenario.target.exploration_contract)
        # Runtime capabilities/observation remain the registered adapter's
        # responsibility; a structurally invalid plan can never be replayed.
        if result.status == 'invalid':
            raise ValueError('v2 replay plan/target contract invalid: ' + '; '.join(result.reasons))

    if not isinstance(data.get("source_manifest"), dict) or not data["source_manifest"]:
        raise ValueError("replay source_manifest must be nonempty")
    if data["source_manifest"] != effective.get("source_manifest"):
        raise ValueError("replay source_manifest mismatch")
    submitted = effective["final_input"]
    if not isinstance(submitted, dict):
        raise ValueError("normal conformance final_input must be a config dict")
    layout = _action_program_layout(scenario, submitted)
    if layout is not None:
        if canonical_hash(data.get("payload_layout")) != canonical_hash(layout):
            raise ValueError("action-program payload layout mismatch")
        if {"topic", "message_type", "qos"} & set(data):
            raise ValueError("action-program capsule cannot declare pair payload fields")
    else:
        if "payload_layout" in data:
            raise ValueError("pair capsule cannot declare action-program payload layout")
        for key in ("topic", "message_type"):
            nonempty(data.get(key), key)
        if not isinstance(data.get("qos"), dict):
            raise ValueError("replay qos must be a dict")
        for key in ("topic", "message_type", "qos"):
            if key in submitted and data[key] != submitted[key]:
                raise ValueError(f"replay {key} differs from final submitted config")
    if data.get("origin_domain") is not None:
        integer(data["origin_domain"], "origin_domain")
    return True


def _v2_execution_input(data, input_data, *, require_submission=False):
    """Validate declared execution input against the saved v2 plan, canonically.

    Historical exploratory rows carry the actual plan but may predate explicit
    submission fields. Loading them remains possible; a *new plan-aware replay*
    must additionally return the submitted final input/receipts and payload hash.
    These are boundary bindings, not attestation that a callback ran ROS.
    """
    effective = input_data['effective_input']
    expected = {'input_identity_schema': INPUT_SCHEMA_V2,
                'target': input_data['target'], 'source_manifest': input_data['source_manifest'],
                'effective_input_sha256': input_data['effective_input_sha256'],
                'mutation_parameters': input_data['mutation_parameters'],
                'state_steps': input_data['state_steps']}
    for key in ('target_id', 'target_revision', 'seed', 'rmw', 'arm', 'repeat',
                'mutation_kind', 'carrier'):
        expected[key] = input_data[key]
    plan = input_data.get('mutation_plan')
    if plan is not None:
        expected['mutation_plan'] = plan
        actual_plan = data.get('mutation_plan')
        if not isinstance(actual_plan, dict) or canonical_hash(actual_plan.get('final_config')) != canonical_hash(effective['final_input']):
            raise ValueError('v2 execution actual plan input differs from sealed final input')
    for key, value in expected.items():
        if canonical_hash(data.get(key)) != canonical_hash(value):
            raise ValueError('v2 execution input binding mismatch: ' + key)
    optional = {'final_input': effective['final_input'],
                'input_layer_receipts': effective['input_layer_receipts'],
                'effective_input': effective,
                'actual_submitted_payload_sha256': canonical_hash(effective['final_input'])}
    if plan is not None:
        optional.update(mutation_plan_sha256=canonical_hash(plan),
                        operations=input_data['operations'], runtime_windows=input_data['runtime_windows'],
                        contract_hash=input_data['contract_hash'])
    required = {'final_input', 'input_layer_receipts', 'actual_submitted_payload_sha256'} if require_submission else set()
    for key, value in optional.items():
        if key in data or key in required:
            if canonical_hash(data.get(key)) != canonical_hash(value):
                raise ValueError('v2 execution actual input mismatch: ' + key)


def _projection_anomaly(facts):
    """Stable mismatch fields/role aliases, never process GUIDs or raw paths."""
    projection = facts.get('projection')
    if not isinstance(projection, dict):
        return None
    differences = projection.get('differences')
    if not isinstance(differences, list):
        return None
    aliases = {}
    for role, endpoint in (facts.get('projection_endpoint_inventory') or {}).items():
        if isinstance(endpoint, dict) and endpoint.get('gid'):
            aliases[endpoint['gid']] = role
    for role in ('writer', 'reader'):
        endpoint = facts.get(role)
        if isinstance(endpoint, dict) and endpoint.get('id'):
            aliases.setdefault(endpoint['id'], role)
    rows = []
    for difference in differences:
        if not isinstance(difference, dict) or difference.get('equal') is not False:
            return None
        role = aliases.get(difference.get('endpoint_guid'))
        field = difference.get('field')
        if not isinstance(role, str) or not isinstance(field, str) or not field:
            return None
        row = {'role': role, 'field': field}
        if field != 'endpoint_guid':
            wire = difference.get('wire')
            if not isinstance(wire, dict) or wire.get('status') != 'observed' or 'value' not in wire or 'graph_value' not in difference:
                return None
            row.update(wire_value=snapshot(wire['value']), graph_value=snapshot(difference['graph_value']))
        rows.append(row)
    return sorted(rows, key=canonical_hash)



TOPOLOGY_SIGNATURE_SCHEMA = "r2f.conformance-signature/topology-v1"
_TOPOLOGY_TARGET = "discovery.endpoint_topology.fuzz_v1"
_TOPOLOGY_FACTS = "r2f.s1-topology-facts/v1"
_TOPOLOGY_RULE = "s1.topology.program_consistency.v1"


def _topology_conformance_signature(metadata, facts, rule):
    """A normalised outcome, not admission: qualification denials stay closed.

    Only the registered topology rule and concrete typed pair proofs are
    understood here. Caller-supplied anomaly_signature is never a fallback.
    Instance GIDs/process IDs/raw paths are checked but excluded from equality.
    """
    import json
    from .candidate import (TOPOLOGY_IDENTITY_SCHEMA, _validate_observation_identity,
                            _validate_topology_admission, topology_observation_identity)
    try:
        oracle = metadata.get("oracle", {})
        verdict = metadata.get("oracle_verdict")
        if (metadata.get("target_id") != _TOPOLOGY_TARGET or facts.get("schema") != _TOPOLOGY_FACTS
                or rule != _TOPOLOGY_RULE or oracle.get("rule_id") != rule
                or oracle.get("verdict") != verdict or verdict not in {"pass", "fail"}
                or metadata.get("input_identity_schema") != INPUT_SCHEMA_V2
                or metadata.get("execution_stage") != "judged" or metadata.get("actual_run") is not True
                or metadata.get("arm") == "negative" or metadata.get("control_kind")
                or metadata.get("tool_error") or metadata.get("runtime_status") == "tool_error"
                or metadata.get("observer_complete") is False or metadata.get("observer_dropped") is True
                or facts.get("fixture") is not False or facts.get("control_excluded") is not False
                or facts.get("tool_error") is not False or facts.get("evidence_error")
                or facts.get("unknown_reasons_sample_identity") or facts.get("unresolved_historical_obligations")
                or facts.get("diagnostic_valid") is not True or oracle.get("diagnostic_only") is not False
                or any(facts.get(k) is not True for k in (
                    "execution_evidence", "environment_healthy", "process_healthy",
                    "source_provenance_qualified", "endpoint_identity_complete", "observation_complete"))):
            return None
        integer(metadata.get("target_revision"), "topology signature target_revision")
        sha256_digest(metadata.get("effective_input_sha256"), "topology signature input")
        manifest = metadata.get("target", {})
        if manifest and (manifest.get("target_id") != _TOPOLOGY_TARGET
                         or manifest.get("target_revision") != metadata["target_revision"]
                         or manifest.get("rule_contract", {}).get("primary_rule") != rule):
            return None
        evidence = dict(metadata)
        evidence.setdefault("endpoint_identity", topology_observation_identity(metadata))
        _validate_observation_identity(evidence)
        if verdict == "fail":
            _validate_topology_admission(evidence)
            # Ineffective mutations/tool observations cannot acquire a fail
            # signature. Full execution and independent review remain upstream.
            if metadata.get("arm") == "mutated":
                receipt = metadata.get("mutation_receipt", {})
                if any(receipt.get(k) is not True for k in
                       ("carrier_admitted", "configured", "applied", "hit", "effect_observed")):
                    return None
        with open(metadata["raw_path"], encoding="utf-8") as stream:
            events = [json.loads(line) for line in stream if line.strip()]
        if any(ev.get("fixture") is True for ev in events):
            return None

        def event(ref):
            if (not isinstance(ref, dict) or ref.get("path") != metadata["raw_path"]
                    or ref.get("sha256") != metadata["raw_sha256"]
                    or type(ref.get("event_index")) is not int
                    or not 0 <= ref["event_index"] < len(events)):
                raise ValueError("topology signature proof reference invalid")
            return events[ref["event_index"]]

        tokens = {(e["logical_id"], e["generation"]): e for e in facts["endpoint_lineage"]}

        def witness(pair, sample):
            for role in ("writer", "reader"):
                if any(sample.get(k) != pair.get(k) for k in (role, role + "_generation")):
                    raise ValueError("topology witness crosses pair generation")
            seq = sample.get("sequence")
            if type(seq) is not int or seq < 0:
                raise ValueError("topology witness sequence invalid")
            send, receive = event(sample.get("send_ref")), event(sample.get("receive_ref"))
            from r2f.campaign.topology_sample_attribution import native_delivery_identity
            native = native_delivery_identity(receive,
                [ev for ev in events if ev.get("event") == "send"], events,
                ref=lambda ev: dict(path=metadata["raw_path"], sha256=metadata["raw_sha256"],
                                    event_index=ev["event_index"]))
            if native is not None:
                # Compare every native core field, including nested scope,
                # control and incarnation refs. Body sequence is deliberately
                # absent: repeated complete String.data retains its old value.
                if any(key not in sample or canonical_hash(sample[key]) != canonical_hash(value)
                       for key, value in native.items()):
                    raise ValueError("topology native witness differs from raw recomputation")
                for role, ev in (("writer", send), ("reader", receive)):
                    token = tokens[(pair[role], pair[role + "_generation"])]
                    if any(ev.get(key) != token.get(key) for key in
                           ("logical_id", "generation", "endpoint_generation", "pid", "process_start_ticks")):
                        raise ValueError("topology native witness differs from raw worker identity")
                return seq, send, receive
            if ("native_witness" in sample or sample.get("basis") == "direct_gid_scoped_publish_occurrence"):
                raise ValueError("topology native witness without raw native declaration")
            for role, ev, kind in (("writer", send, "send"), ("reader", receive, "receive")):
                token = tokens[(pair[role], pair[role + "_generation"])]
                if (ev.get("event") != kind or ev.get("logical_id") != token["logical_id"]
                        or ev.get("generation") != token["generation"]
                        or ev.get("endpoint_generation") != token["endpoint_generation"]
                        or ev.get("send_sequence") != seq):
                    raise ValueError("topology witness differs from raw endpoint generation")
            if (receive.get("payload_writer_id") != pair["writer"]
                    or receive.get("payload_writer_generation") != pair["writer_generation"]
                    or receive.get("payload_run_id") != metadata["run_id"]
                    or receive.get("payload_endpoint_gid") != tokens[(pair["writer"], pair["writer_generation"])]["endpoint_gid"]
                    or not send.get("payload_sha256") or send.get("payload_sha256") != receive.get("payload_sha256")):
                raise ValueError("topology sentinel payload identity mismatch")
            return seq, send, receive

        declarations = {e["logical_id"]: e for e in metadata["final_input"]["topology_program"]["endpoints"]}
        anomalies, residuals = [], set()
        claimed = oracle.get("forbidden_residuals")
        if not isinstance(claimed, list) or any(not isinstance(v, str) for v in claimed):
            return None
        if set(claimed) - {"topology.pair_graph_mismatch", "topology.closed_delivery_gap"}:
            return None
        for pair in facts["pairs"]:
            subjects = {role: {"logical_id": pair[role], "generation": pair[role + "_generation"]}
                        for role in ("writer", "reader")}
            if any(not isinstance(declarations[pair[role]].get(k), str)
                   or not declarations[pair[role]][k].strip()
                   for role in ("writer", "reader") for k in ("topic", "type_name")):
                return None
            if any(not isinstance(declarations[pair[role]].get("qos"), dict) for role in ("writer", "reader")):
                return None
            obligation = {role: {key: snapshot(declarations[pair[role]].get(key))
                                for key in ("topic", "type_name", "qos")}
                          for role in ("writer", "reader")}
            proofs = pair.get("proofs", {})
            compatibility = proofs.get("compatibility", {})
            match = proofs.get("observed_match", {})
            samples = pair.get("communication_witnesses", [])
            if not isinstance(samples, list):
                return None
            attributed = [witness(pair, sample) for sample in samples]
            native_samples = [sample for sample in samples if "native_witness" in sample]
            if len({sample["receive_ref"]["event_index"] for sample in native_samples}) != len(native_samples):
                return None
            native_peer_kinds = {"direct_native_per_peer", "direct_mixed_per_peer"}
            from r2f.campaign.topology_state_projection import declares_fastdds_peer
            raw_native_peer = any(
                ev.get("logical_id") in (pair["writer"], pair["reader"])
                and ev.get("generation") == pair["writer_generation" if ev.get("logical_id") == pair["writer"]
                                                 else "reader_generation"]
                and (ev.get("event") == "native_peer_provider_selected"
                     or ev.get("event") == "endpoint_created" and ev.get("observation_mode") == "instrumentation"
                     or ev.get("event") == "match_snapshot" and declares_fastdds_peer(ev))
                for ev in events)
            native_peer = raw_native_peer or match.get("detail", {}).get("proof_kind") in native_peer_kinds

            def native_peer_proof():
                from r2f.campaign.topology_state_projection import _Projection
                from r2f.runtime.endpoint_incarnation import endpoint_identity_at
                if (match.get("status") != "observed" or match.get("value") != pair.get("observed_match")
                        or match.get("detail", {}).get("proof_kind") not in native_peer_kinds):
                    return None
                refs = match.get("refs", [])
                rows = [event(ref) for ref in refs]
                if not rows:
                    return None
                # Canonical facts bind their final observation boundary. Do
                # not truncate raw at a caller-selected earlier match ref.
                boundary = event(facts.get("state_projection_receipt", {}).get("after", {}).get("boundary_ref"))
                if any(type(ev.get("event_index")) is not int or ev["event_index"] != index
                       or type(ev.get("monotonic_ns")) is not int or ev["monotonic_ns"] <= 0
                       for index, ev in enumerate(events)):
                    return None
                program = metadata["final_input"]["topology_program"]
                bindings = [ev for ev in events[:boundary["event_index"] + 1]
                            if ev.get("event") == "input_binding"]
                if (len(bindings) != 1 or bindings[0].get("run_id") != metadata["run_id"]
                        or bindings[0].get("receipt", {}).get("program_sha256") != canonical_hash(program)):
                    return None
                projection = _Projection(program, metadata["raw_path"], metadata["raw_sha256"], events,
                                         {"input_ref": {"event_index": bindings[0]["event_index"]}})
                current = []
                for role in ("writer", "reader"):
                    creation = event(tokens[(pair[role], pair[role + "_generation"])]["creation_ref"])
                    current.append(endpoint_identity_at(events, creation,
                                   before_index=boundary["event_index"] + 1))
                w, r = current
                wc, rc = projection.snapshot_chain(w, boundary), projection.snapshot_chain(r, boundary)
                value, required_rows, isolation = projection.match(w, r, wc, rc, boundary)
                sources = {chain[1].get("peer_proof_source") for chain in (wc, rc)}
                kind = ("direct_native_per_peer" if sources == {"fastdds_native_stateful_peer_snapshot"}
                        else "direct_mixed_per_peer")
                valid = (isolation is None and value == pair.get("observed_match")
                        and "fastdds_native_stateful_peer_snapshot" in sources
                        and value in {"bidirectional", "writer_only", "reader_only", "none"}
                        and match["detail"]["proof_kind"] == kind
                        and ("peer_proof_sources" not in match["detail"]
                             or match["detail"]["peer_proof_sources"] == sorted(sources))
                        and {ev["event_index"] for ev in required_rows}
                            <= {ev["event_index"] for ev in rows})
                return value if valid else None

            native_match_value = native_peer_proof() if native_peer else None

            def direct_peer_match():
                if native_peer:
                    return native_match_value in {"bidirectional", "writer_only", "reader_only"}
                if (match.get("status") != "observed" or match.get("value") != pair.get("observed_match")
                        or match.get("detail", {}).get("proof_kind") != "direct_per_peer_api"):
                    return False
                directions = set()
                for ref in match.get("refs", []):
                    ev = event(ref)
                    if ev.get("event") != "match_snapshot":
                        continue
                    for local, peer in (("writer", "reader"), ("reader", "writer")):
                        token = tokens[(pair[local], pair[local + "_generation"])]
                        peer_gid = tokens[(pair[peer], pair[peer + "_generation"])]["endpoint_gid"]
                        if (ev.get("logical_id") == token["logical_id"] and ev.get("generation") == token["generation"]
                                and ev.get("endpoint_generation") == token["endpoint_generation"]
                                and peer_gid in (ev.get("matched_peer_gids") or [])):
                            directions.add(local)
                required = {"bidirectional": {"writer", "reader"}, "writer_only": {"writer"},
                            "reader_only": {"reader"}}.get(pair.get("observed_match"))
                return required is not None and required <= directions

            # A native declaration selects the strict peer contract even if
            # samples exist or the claimed pair is compatible. No GID-list or
            # communication fallback may rescue a malformed native receipt.
            if native_peer and native_match_value is None:
                return None
            graph_mismatch = (pair.get("compatibility") == "incompatible"
                and compatibility.get("status") == "observed" and compatibility.get("value") == "incompatible"
                and (bool(attributed) or direct_peer_match()))
            if graph_mismatch:
                residual = "topology.pair_graph_mismatch"
                residuals.add(residual)
                anomalies.append(dict(subjects, residual=residual, obligation=obligation,
                                      compatibility="incompatible", observed_match=pair.get("observed_match"),
                                      communication_observed=bool(attributed)))
            if pair.get("delivery_closure") != "closed_with_gap":
                continue
            component = proofs.get("delivery_closure", {})
            detail = component.get("detail", {})
            if (component.get("status") != "observed" or component.get("value") != "closed_with_gap"
                    or not component.get("refs")
                    or any(pair.get(k) is not True for k in
                           ("reliable_history_contract", "filter_free", "delivery_window_identity_complete"))):
                return None
            expected, missing = detail.get("expected"), detail.get("missing")
            if (not isinstance(expected, list) or not expected or not isinstance(missing, list) or not missing
                    or any(type(n) is not int or n < 0 for n in expected + missing)
                    or len(set(expected)) != len(expected) or expected != sorted(expected)
                    or len(set(missing)) != len(missing)):
                return None
            if any(ref not in component["refs"] for ref in (detail.get("open_ref"), detail.get("close_ref"))):
                return None
            opened, closed = event(detail.get("open_ref")), event(detail.get("close_ref"))
            if (opened.get("event") != "topology_delivery_window_open"
                    or closed.get("event") != "topology_delivery_window_closed"
                    or opened["event_index"] >= closed["event_index"] or closed.get("window_complete") is not True):
                return None
            if closed.get("open_ref") != opened["event_index"]:
                return None
            for role in ("writer", "reader"):
                if any(ev.get(k) != pair.get(k) for ev in (opened, closed)
                       for k in (role, role + "_generation")):
                    return None
            if (closed.get("expected_sequences") != expected
                    or closed.get("missing_sequences") != sorted(missing)
                    or closed.get("received_sequences") != detail.get("received")):
                return None
            inside = events[opened["event_index"] + 1:closed["event_index"]]
            sends = [ev for ev in inside if ev.get("event") == "send"
                     and ev.get("logical_id") == pair["writer"] and ev.get("generation") == pair["writer_generation"]]
            token = tokens[(pair["writer"], pair["writer_generation"])]
            if any(ev.get("endpoint_generation") != token["endpoint_generation"] for ev in sends):
                return None
            sent = [ev.get("send_sequence") for ev in sends]
            if sent != expected or len(set(sent)) != len(sent):
                return None
            received = [seq for seq, send, receive in attributed
                        if opened["event_index"] < send["event_index"] < closed["event_index"]
                        and opened["event_index"] < receive["event_index"] < closed["event_index"]]
            raw_pair_receives = {ev["event_index"] for ev in inside if ev.get("event") == "receive"
                and ev.get("logical_id") == pair["reader"] and ev.get("generation") == pair["reader_generation"]
                and ev.get("payload_writer_id") == pair["writer"]
                and ev.get("payload_writer_generation") == pair["writer_generation"]}
            witnessed_receives = {receive["event_index"] for seq, send, receive in attributed
                if opened["event_index"] < receive["event_index"] < closed["event_index"]}
            if raw_pair_receives != witnessed_receives:
                return None
            sentinel = detail.get("sentinel_sequence")
            if (type(sentinel) is not int or sentinel != max(expected) or sentinel not in received
                    or closed.get("sentinel_sequence") != sentinel
                    or set(received) - set(expected) or len(set(received)) != len(received)
                    or sorted(set(expected) - set(received)) != sorted(missing)
                    or detail.get("received") != sorted(received)
                    or any(n >= sentinel for n in missing)):
                return None
            sentinel_samples = [(send, receive) for seq, send, receive in attributed if seq == sentinel]
            if (len(sentinel_samples) != 1
                    or closed.get("sentinel_send_ref") != sentinel_samples[0][0]["event_index"]
                    or closed.get("sentinel_receive_ref") != sentinel_samples[0][1]["event_index"]):
                return None
            # A same-writer later sequence, never an unrelated pair/global sort.
            residual = "topology.closed_delivery_gap"
            residuals.add(residual)
            anomalies.append(dict(subjects, residual=residual, obligation=obligation,
                                  expected=expected, missing=sorted(missing), received=sorted(received),
                                  sentinel_sequence=sentinel, reliable_history_contract=True, filter_free=True))
        if verdict == "fail" and (not anomalies or residuals != set(claimed)):
            return None
        if verdict == "pass" and (anomalies or claimed):
            return None
        return {"schema": TOPOLOGY_SIGNATURE_SCHEMA, "endpoint_identity_schema": TOPOLOGY_IDENTITY_SCHEMA,
                "rule_id": rule, "target_id": _TOPOLOGY_TARGET, "target_revision": metadata["target_revision"],
                "effective_input_sha256": metadata["effective_input_sha256"], "oracle_verdict": verdict,
                "typed_anomaly_signature": sorted(anomalies, key=canonical_hash)}
    except (ValueError, KeyError, TypeError, OSError, IndexError, AttributeError):
        return None


def conformance_signature(metadata):
    """Return a typed, stable normal-conformance outcome signature, or None.

    For a failed oracle the adapter should supply typed_anomaly_signature (dict
    or list), or one of the documented typed failure summaries below. Raw event
    times, domain numbers and endpoint instance ids are never a signature.
    Merely observing the same verdict=fail without typed failure facts is
    insufficient to establish consistency.
    """
    verdict = metadata.get("oracle_verdict")
    if verdict not in {"pass", "fail"}:
        return None
    facts = metadata.get("typed_facts", {})
    if not isinstance(facts, dict):
        return None
    target = metadata.get("target", {})
    rule = metadata.get("rule_id") or target.get("rule_contract", {}).get("primary_rule")
    if not isinstance(rule, str) or not rule:
        return None
    # Exact topology routing precedes the legacy caller-supplied signature path.
    if metadata.get("target_id") == _TOPOLOGY_TARGET or facts.get("schema") == _TOPOLOGY_FACTS:
        return _topology_conformance_signature(metadata, facts, rule)
    explicit = metadata.get("typed_anomaly_signature", facts.get("anomaly_signature"))
    if explicit is not None:
        if not isinstance(explicit, (dict, list)) or not explicit:
            return None
        anomaly = snapshot(explicit)
    else:
        summary_keys = ("missing_sequences", "missing_postmatch_sequences", "forbidden_residuals",
                        "violations", "reason_codes", "identity_mismatches", "order_violation",
                        "delivery_order_violation")
        anomaly = {key: snapshot(facts[key]) for key in summary_keys if facts.get(key)}
        if facts.get("matched") is False:
            anomaly["matched"] = False
        # Typed delivery records can establish an intermediate omission: a later
        # received sequence closes that gap; a missing tail does not suffice.
        sent, received = facts.get("sent"), facts.get("received")
        if isinstance(sent, list) and isinstance(received, list):
            sent_seq = {v["sequence"] for v in sent if isinstance(v, dict) and type(v.get("sequence")) is int}
            received_seq = [v["sequence"] for v in received if isinstance(v, dict) and type(v.get("sequence")) is int]
            match_time = facts.get("match", {}).get("at_ns") if isinstance(facts.get("match"), dict) else None
            if type(match_time) is int:
                sent_seq = {v["sequence"] for v in sent if isinstance(v, dict)
                            and type(v.get("sequence")) is int and type(v.get("at_ns")) is int
                            and v["at_ns"] >= match_time}
            if received_seq:
                missing = sorted(v for v in sent_seq - set(received_seq) if v < max(received_seq))
                if missing:
                    anomaly["missing_postmatch_sequences"] = missing
                if received_seq != sorted(received_seq):
                    anomaly["delivery_order_violation"] = True
        residuals = _effect_names(metadata.get("observed_effect")).intersection(
            target.get("expected_effect_set", {}).get("forbidden_residuals", []))
        if residuals:
            anomaly["observed_forbidden_residuals"] = sorted(residuals)
        if metadata.get('input_identity_schema') == INPUT_SCHEMA_V2:
            if isinstance(facts.get('projection'), dict):
                fields = _projection_anomaly(facts)
                if fields is None or (verdict == 'fail' and not fields):
                    return None
                if fields:
                    anomaly['projection_differences'] = fields
            recovery = facts.get('recovery')
            if isinstance(recovery, dict) and recovery.get('identity_stable') is False:
                anomaly['recovery_identity_stable'] = False
        if verdict == "fail" and not anomaly:
            return None
    signature = {"schema": "r2f.conformance-signature/v1", "rule_id": rule,
                 "oracle_verdict": verdict, "typed_anomaly_signature": anomaly}
    if metadata.get('input_identity_schema') == INPUT_SCHEMA_V2:
        signature.update(schema='r2f.conformance-signature/v2',
                         target_id=metadata.get('target_id'), target_revision=metadata.get('target_revision'),
                         effective_input_sha256=metadata.get('effective_input_sha256'))
    return signature


def validate_replay_attempt(attempt):
    if not isinstance(attempt, dict):
        raise ValueError("replay attempt must be a dict")
    status = ReplayStatus(attempt.get("status"))
    if status == ReplayStatus.NOT_RUN:
        raise ValueError("not_run is not a replay attempt")
    nonempty(attempt.get("attempt_id"), "attempt_id")
    nonempty(attempt.get("timestamp"), "timestamp")
    if status != ReplayStatus.PASS:
        nonempty(attempt.get("failure_reason"), "failure_reason")
    return True


@dataclass
class ReplayCapsule:
    input: dict
    execution: dict | None = None
    replay_attempts: list = field(default_factory=list)
    non_replayable: dict | None = None
    execution_history: list = field(default_factory=list)

    def __post_init__(self):
        self.input = snapshot(self.input)
        validate_replay_input(self.input)
        initial = self.execution
        self.execution = None
        if initial is not None:
            self.record_execution(initial)
        self.replay_attempts = snapshot(self.replay_attempts)
        for attempt in self.replay_attempts:
            validate_replay_attempt(attempt)

    @classmethod
    def from_scenario(cls, scenario, *, source_manifest, final_input=None,
                      input_layer_receipts=None, origin_domain=None, input_schema=None):
        """Build an offline capsule for an executed-or-planned input.

        ``input_schema`` may force a version.  By default a scenario carrying a
        ``mutation_plan`` writes ``r2f.input-identity/v2`` so the plan, its
        operations and runtime windows enter the digest; a legacy scenario keeps
        writing v1 with byte-identical canonical bytes.
        """
        has_plan = scenario.mutation_plan is not None
        if input_schema is None:
            schema = INPUT_SCHEMA_V2 if has_plan else INPUT_SCHEMA
        else:
            if input_schema not in INPUT_SCHEMA_VERSIONS:
                raise ValueError("unsupported replay input identity schema")
            schema = input_schema
        if schema == INPUT_SCHEMA_V2 and not has_plan:
            raise ValueError("a v2 capsule requires an explicit mutation_plan")
        if schema == INPUT_SCHEMA and has_plan:
            raise ValueError("a plan-bearing scenario must use the v2 input identity")

        if has_plan:
            # Do not inject defaults that would differ from the plan's final
            # config; the plan is authoritative and the hash must bind it.
            submitted = None if final_input is None else snapshot(final_input)
            effective = scenario.effective_input_v2(final_input=submitted,
                                                   input_layer_receipts=input_layer_receipts,
                                                   source_manifest=source_manifest)
            submitted = effective["final_input"]
        else:
            submitted = snapshot(scenario.parameters if final_input is None else final_input)
            if not isinstance(submitted, dict):
                raise ValueError("normal conformance config must be a dict")
            # Endpoint defaults are explicit in the final submitted configuration.
            submitted.setdefault("topic", "/r2f/conformance")
            submitted.setdefault("message_type", "std_msgs/msg/String")
            submitted.setdefault("qos", {})
            effective = scenario.effective_input(final_input=submitted,
                                                 input_layer_receipts=input_layer_receipts,
                                                 source_manifest=source_manifest)
        if not isinstance(submitted, dict):
            raise ValueError("normal conformance config must be a dict")
        layout = _action_program_layout(scenario, submitted) if schema == INPUT_SCHEMA_V2 else None
        if schema == INPUT_SCHEMA_V2 and layout is None:
            missing = [key for key in ("topic", "message_type", "qos") if key not in submitted]
            if missing:
                raise ValueError("v2 replay final input is missing submitted config field(s): "
                                 + ",".join(missing))

        scenario_data = scenario.to_dict()
        if schema == INPUT_SCHEMA_V2:
            digest = effective_input_hash(effective, schema=INPUT_SCHEMA_V2)
        else:
            digest = effective_input_hash(effective)
        input_data = {"input_identity_schema": schema, "scenario": scenario_data,
                      "target": scenario.target.to_dict(), "target_id": scenario.target_id,
                      "target_revision": scenario.target_revision, "seed": scenario.seed, "rmw": scenario.rmw,
                      "arm": scenario.arm, "repeat": scenario.repeat, "mutation_kind": scenario.mutation_kind,
                      "carrier": scenario.carrier, "mutation_parameters": scenario.mutation_parameters,
                      "state_steps": [s.to_dict() for s in scenario.state_steps],
                      "source_manifest": snapshot(source_manifest), "effective_input": effective,
                      "effective_input_sha256": digest, "origin_domain": origin_domain,
                      **{key: submitted[key] for key in ("topic", "message_type", "qos") if key in submitted}}
        if layout is not None:
            input_data["payload_layout"] = layout
        if schema == INPUT_SCHEMA_V2:
            for key in _PLAN_BINDING_KEYS:
                if key in effective:
                    input_data[key] = snapshot(effective[key])
        return cls(input_data)

    def record_execution(self, metadata):
        validate_replay_input(self.input)
        data = snapshot(metadata)
        if data.get("actual_run") is not True or data.get("execution_source") != "ros2_public_api":
            raise ValueError("execution metadata must originate in an actual public ROS2 API run")
        for key in ("target_id", "target_revision", "seed", "rmw", "arm", "repeat", "mutation_kind", "carrier"):
            if data.get(key) != self.input[key]:
                raise ValueError(f"replay execution identity mismatch: {key}")
        if data.get("target") != self.input["target"]:
            raise ValueError("execution target manifest mismatch")
        if data.get("effective_input_sha256") != self.input["effective_input_sha256"]:
            raise ValueError("execution must reference the sealed effective input SHA256")
        if data.get("source_manifest") != self.input["source_manifest"]:
            raise ValueError("execution source_manifest mismatch")
        if self.input.get('input_identity_schema') == INPUT_SCHEMA_V2:
            _v2_execution_input(data, self.input)
        validate_execution(data)
        if data["execution_stage"] not in {"effect_observed", "judged", "aborted"}:
            raise ValueError("execution metadata must contain an observation or a terminal failure")
        if data["execution_stage"] == "aborted":
            nonempty(data.get("abort_reason"), "abort_reason")
        else:
            nonempty(data.get("raw_path"), "raw_path")
            sha256_digest(data.get("raw_sha256"), "raw_sha256")
        if self.execution is None:
            self.execution = data
        else:
            # The first actual execution remains the comparison baseline.
            self.execution_history.append(data)
        return snapshot(data)

    def record_attempt(self, status, *, failure_reason="", **metadata):
        attempt = snapshot(metadata)
        attempt.update(status=ReplayStatus(status).value, failure_reason=failure_reason,
                       attempt_id=f"attempt-{len(self.replay_attempts) + 1}", timestamp=utc_now())
        validate_replay_attempt(attempt)
        self.replay_attempts.append(attempt)
        return snapshot(attempt)

    def mark_non_replayable(self, disposition, reason):
        nonempty(disposition, "non_replayable.disposition")
        nonempty(reason, "non_replayable.reason")
        self.non_replayable = {"disposition": disposition, "reason": reason, "timestamp": utc_now()}

    def to_dict(self):
        return {"schema": CAPSULE_SCHEMA, "input": snapshot(self.input), "execution": snapshot(self.execution),
                "execution_history": snapshot(self.execution_history), "replay_attempts": snapshot(self.replay_attempts),
                "non_replayable": snapshot(self.non_replayable)}

    @classmethod
    def from_dict(cls, data):
        if data.get("schema") != CAPSULE_SCHEMA:
            raise ValueError("unsupported replay capsule schema")
        result = cls(data["input"], replay_attempts=data.get("replay_attempts", []))
        if data.get("execution") is not None:
            result.record_execution(data["execution"])
        for execution in data.get("execution_history", []):
            result.record_execution(execution)
        disposition = data.get("non_replayable")
        if disposition is not None:
            nonempty(disposition.get("disposition"), "non_replayable.disposition")
            nonempty(disposition.get("reason"), "non_replayable.reason")
            nonempty(disposition.get("timestamp"), "non_replayable.timestamp")
            result.non_replayable = snapshot(disposition)
        return result


def rerun_conformance(capsule, runner, *, domain, plan_aware=False, comparison_view=None):
    """runner(scenario, domain) runs the normal scene and returns execution metadata.

    The compiled scenario's final_input/input_layer_receipts/source_manifest fields
    are available as properties for the adapter to submit and bind its receipt.
    V2 requires plan_aware=True: the callback must preserve the saved plan and
    return an explicit actual submitted input/hash binding. This does not qualify
    an injected callback as real ROS; campaign qualification is a separate gate.
    comparison_view, when provided, independently revalidates comparison copies
    for original/fresh evidence. It never replaces the saved execution/history.
    """
    try:
        validate_replay_input(capsule.input)
    except (ValueError, KeyError, TypeError) as exc:
        return capsule.record_attempt("fail_artifact", failure_reason=f"{type(exc).__name__}: {exc}", domain=domain)
    v2 = capsule.input.get('input_identity_schema') == INPUT_SCHEMA_V2
    if v2 and plan_aware is not True:
        return capsule.record_attempt(
            'fail_artifact', failure_reason='v2 replay requires an explicitly plan-aware runner',
            domain=domain, reason_code='v2_plan_aware_runner_required', phase='preflight')
    if v2:
        if 'mutation_plan' not in capsule.input:
            return capsule.record_attempt(
                'fail_artifact', failure_reason='plan-aware v2 replay requires a saved mutation plan',
                domain=domain, reason_code='v2_mutation_plan_required', phase='preflight')
        if type(domain) is not int or not 0 <= domain <= 231:
            return capsule.record_attempt(
                'fail_environment', failure_reason='replay domain must be an integer in 0..231',
                domain=domain, reason_code='invalid_domain', phase='preflight')
        prior_domains = [capsule.input.get('origin_domain')]
        for execution in [capsule.execution, *capsule.execution_history]:
            if execution:
                prior_domains.extend(execution.get(key) for key in ('domain', 'domain_id'))
                prior_domains.append((execution.get('runtime_execution_metadata') or {}).get('domain_id'))
        if domain in prior_domains:
            return capsule.record_attempt(
                'fail_environment', failure_reason='conformance rerun requires a fresh isolated domain',
                domain=domain, reason_code='fresh_domain_required', phase='preflight')
    else:
        # Preserve the historical v1 API and its error semantics.
        integer(domain, 'domain')
        if domain == capsule.input.get('origin_domain'):
            raise ValueError('conformance rerun requires a new isolated domain')
    scenario = Scenario.from_dict(capsule.input["scenario"])
    scenario.final_input = snapshot(capsule.input["effective_input"]["final_input"])
    scenario.input_layer_receipts = snapshot(capsule.input["effective_input"]["input_layer_receipts"])
    scenario.source_manifest = snapshot(capsule.input["source_manifest"])
    scenario.effective_input_sha256 = capsule.input["effective_input_sha256"]
    if v2:
        scenario.input_identity_schema = INPUT_SCHEMA_V2
    sealed_input = snapshot(capsule.input)
    supplied = snapshot({'scenario': scenario.to_dict(), 'final_input': scenario.final_input,
                         'input_layer_receipts': scenario.input_layer_receipts,
                         'source_manifest': scenario.source_manifest,
                         'effective_input_sha256': scenario.effective_input_sha256})
    original = snapshot(capsule.execution)
    try:
        metadata = runner(scenario, domain)
    except Exception as exc:
        if v2 and canonical_hash(capsule.input) != canonical_hash(sealed_input):
            capsule.input = sealed_input
            return capsule.record_attempt('fail_artifact', failure_reason='runner changed sealed capsule input',
                                          domain=domain, runner_error=f'{type(exc).__name__}: {exc}')
        status = "fail_timing" if isinstance(exc, TimeoutError) else "fail_environment"
        return capsule.record_attempt(status, failure_reason=f"{type(exc).__name__}: {exc}", domain=domain)
    try:
        if v2:
            if canonical_hash(capsule.input) != canonical_hash(sealed_input):
                capsule.input = sealed_input
                raise ValueError('runner changed sealed capsule input')
            observed = {'scenario': scenario.to_dict(), 'final_input': scenario.final_input,
                        'input_layer_receipts': scenario.input_layer_receipts,
                        'source_manifest': scenario.source_manifest,
                        'effective_input_sha256': scenario.effective_input_sha256}
            if canonical_hash(observed) != canonical_hash(supplied):
                raise ValueError('runner changed sealed scenario input')
            _v2_execution_input(metadata, capsule.input, require_submission=True)
            if metadata.get('domain_id') != domain or type(metadata.get('domain_id')) is not int:
                raise ValueError('v2 execution did not bind the fresh isolated domain_id')
        if metadata.get("domain") != domain:
            raise ValueError("runner did not record the fresh isolated domain")
        capsule.record_execution(metadata)
    except (ValueError, KeyError, TypeError, OSError, AttributeError) as exc:
        try:
            retained = snapshot(metadata)
        except (ValueError, TypeError):
            retained = {"unserializable_type": type(metadata).__name__}
        return capsule.record_attempt("fail_artifact", failure_reason=f"{type(exc).__name__}: {exc}", domain=domain,
                                      execution_metadata=retained)
    if metadata["execution_stage"] == "aborted":
        return capsule.record_attempt("fail_environment", failure_reason=metadata["abort_reason"], domain=domain,
                                      execution_metadata=metadata)
    if original is None:
        # This verifies only that an ordinary execution completed. It provides no
        # claim that a previously observed anomaly has been matched.
        completed = metadata["execution_stage"] == "judged"
        return capsule.record_attempt("pass" if completed else "inconclusive", domain=domain,
                                      failure_reason="" if completed else "execution was not judged",
                                      execution_metadata=metadata, comparison_basis="execution_success_only",
                                      comparison_matches_original=False)
    # Comparison views are ephemeral, independently validated projections. The
    # first execution and append-only history remain exactly as recorded above.
    before_view, after_view = snapshot(original), snapshot(metadata)
    if comparison_view is not None:
        try:
            before_view = comparison_view(snapshot(original), original=True)
            after_view = comparison_view(snapshot(metadata), original=False)
            if not isinstance(before_view, dict) or not isinstance(after_view, dict):
                raise ValueError("comparison views must be execution objects")
        except (ValueError, KeyError, TypeError, OSError, AttributeError) as exc:
            return capsule.record_attempt("fail_artifact", domain=domain,
                failure_reason="comparison view invalid: " + str(exc),
                execution_metadata=metadata, comparison_basis="original_execution_signature",
                comparison_matches_original=False)
    original_signature = conformance_signature(before_view)
    replay_signature = conformance_signature(after_view)
    consistent = (original_signature is not None and replay_signature is not None
                  and canonical_hash(original_signature) == canonical_hash(replay_signature))
    original_identity = {key: original.get(key) for key in (
        "run_id", "target_id", "target_revision", "rmw", "arm", "repeat", "raw_sha256", "effective_input_sha256")}
    return capsule.record_attempt("pass" if consistent else "inconclusive", domain=domain,
                                  failure_reason="" if consistent else "original oracle/typed anomaly signature did not match",
                                  execution_metadata=metadata, comparison_basis="original_execution_signature",
                                  comparison_matches_original=consistent, original_signature=original_signature,
                                  replay_signature=replay_signature, original_execution_identity=original_identity)
