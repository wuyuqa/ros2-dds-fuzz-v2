"""Typed primary delivery oracle for the two late-reader targets.

An unreceived tail has no finite failure proof. A later delivered sequence closes
only intermediate omissions. Prejoin volatile samples carry no obligation.
"""
from .facts import FactError, PrimaryFacts, RecoveryFacts, ProjectionFacts
from .targets import RULES, EXECUTION_ORDER


def evaluate_primary(facts: dict, target_id=EXECUTION_ORDER[0]) -> dict:
    result = {"rule_id": RULES[target_id], "rule_role": "primary", "verdict": "unknown",
              "reason_codes": [], "forbidden_residuals": [], "closure": {"kind": "delivered_later_sequence", "proven_gaps": [], "tail_inflight": []}}
    if target_id == EXECUTION_ORDER[3]:
        result["closure"] = {"kind": "independent_wire_graph_field_coverage"}
        try:
            if facts.get("evidence_error"):
                raise FactError(facts["evidence_error"])
            projection = ProjectionFacts.parse(facts)
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            result["reason_codes"] = [f"unknown_projection_evidence:{exc}"]
            return result
        result.update(verdict="fail" if projection.differences else "pass",
                      reason_codes=["builtin_graph_field_mismatch"] if projection.differences else ["independent_projection_fields_agree"],
                      forbidden_residuals=["builtin_graph_field_mismatch"] if projection.differences else [])
        return result
    if target_id == EXECUTION_ORDER[2]:
        try:
            recovery = RecoveryFacts.parse(facts)
        except (KeyError, TypeError, FactError, AttributeError) as exc:
            result["reason_codes"] = [f"unknown_recovery_evidence:{exc}"]
            return result
        result = _delivery_primary(facts, target_id, require_late=False)
        if not recovery.identity_stable:
            result["forbidden_residuals"].append("post_restore_discovery_inconsistency")
            result.update(verdict="fail", reason_codes=list(result["forbidden_residuals"]))
        return result
    return _delivery_primary(facts, target_id)


def _delivery_primary(facts, target_id, require_late=True):
    result = {"rule_id": RULES[target_id], "rule_role": "primary", "verdict": "unknown",
              "reason_codes": [], "forbidden_residuals": [], "closure": {"kind": "delivered_later_sequence", "proven_gaps": [], "tail_inflight": []}}
    if target_id not in EXECUTION_ORDER[:3]:
        result["reason_codes"] = ["blocked:no_implemented_primary_observation_contract"]
        return result
    try:
        f = PrimaryFacts.parse(facts)
    except (KeyError, TypeError, FactError, AttributeError) as exc:
        result["reason_codes"] = [f"unknown_evidence:{exc}"]
        return result
    if not f.window_complete:
        result["reason_codes"] = ["window_incomplete"]
        return result
    if require_late and f.reader.created_ns <= f.writer.created_ns:
        result["reason_codes"] = ["reach_or_planning:reader_not_late"]
        return result
    sent = [x for x in f.sent if x.at_ns > f.matched_ns and x.at_ns >= f.window_start_ns]
    received = [x for x in f.received if x.at_ns >= f.window_start_ns]
    if not sent:
        result["reason_codes"] = ["no_postmatch_delivery_obligation"]
        return result
    if len({x.sequence for x in sent}) != len(sent):
        result["reason_codes"] = ["ambiguous_sent_sequence"]
        return result
    if [x.sequence for x in sent] != sorted(x.sequence for x in sent):
        result["reason_codes"] = ["harness_sent_order_invalid"]
        return result
    residuals = []
    if (f.matched_writer, f.matched_reader) != (f.writer.id, f.reader.id):
        residuals.append("wrong_endpoint_match")
    if any(x.writer_id != f.writer.id or x.writer_generation != f.writer.generation for x in sent + received) or any((x.reader_id, x.reader_generation) != (f.reader.id, f.reader.generation) for x in received):
        residuals.append("identity_generation_mixup")
    expected = {x.sequence: x for x in sent}
    # A volatile prejoin sample that happens to arrive is not a postmatch obligation.
    prejoin = {x.sequence for x in f.sent if x.at_ns <= f.matched_ns}
    if any(x.sequence not in expected and x.sequence not in prejoin for x in received):
        result["reason_codes"] = ["uncorrelated_received_sequence"]
        return result
    relevant = [x for x in received if x.sequence in expected]
    # Send tokens follow publish() return; a fast reader may timestamp receive
    # before that return token. Sequence + stable GID establish correlation.
    sequences = [x.sequence for x in relevant]
    if sequences != sorted(sequences) or len(sequences) != len(set(sequences)):
        residuals.append("delivery_order_violation")
    max_delivered = max(sequences, default=-1)
    missing = sorted(set(expected) - set(sequences))
    proven = [seq for seq in missing if seq < max_delivered]
    tail = [seq for seq in missing if seq >= max_delivered]
    result["closure"].update(proven_gaps=proven, tail_inflight=tail, sentinel_sequence=max_delivered if sequences else None)
    if proven:
        residuals.append("post_restore_delivery_loss" if target_id == EXECUTION_ORDER[2] else "post_match_delivery_loss")
    result["forbidden_residuals"] = residuals
    if residuals:
        result.update(verdict="fail", reason_codes=residuals)
    elif tail:
        result["reason_codes"] = ["tail_inflight"]
    else:
        result.update(verdict="pass", reason_codes=["postrestore_delivery_contract_satisfied" if target_id == EXECUTION_ORDER[2] else "matched_postjoin_delivery_contract_satisfied"])
    return result


# --- G01 offered/requested reliability compatibility guard -------------------
#
# This evaluator answers one narrow question: within a complete observation
# window, did an identity-correlated match or delivery occur for a pair whose
# offered/requested reliability is incompatible?  Creation succeeding is not a
# violation; a compatible pair that simply has not matched inside a finite
# window is unknown, not a DDS contract failure.

COMPATIBILITY_RULE_ID = "s1.discovery.compatibility_guard.v1"
_OFFERED = ("reliable", "best_effort")
_INCOMPATIBLE_PAIRS = frozenset({("best_effort", "reliable")})


def _guard_result(verdict="unknown", reason=""):
    return {"rule_id": COMPATIBILITY_RULE_ID, "rule_role": "primary", "verdict": verdict,
            "reason_codes": [reason] if reason else [],
            "forbidden_residuals": [],
            "applicability": {"relation": "offered_requested_reliability",
                              "offered": None, "requested": None, "compatible": None,
                              "incompatible_pairs": ["BR"]},
            "unknown_reasons": [], "closure": {"window_complete": False,
                                              "match_correlated": False,
                                              "delivery_correlated": False}}


def _guard_unknown(result, reason):
    result["verdict"] = "unknown"
    result["reason_codes"] = [reason]
    result["unknown_reasons"] = [reason]
    return result


def _guard_identity(value):
    if not isinstance(value, dict):
        raise ValueError("endpoint identity missing")
    endpoint_id = value.get("id")
    if not isinstance(endpoint_id, str) or not endpoint_id:
        raise ValueError("endpoint id missing")
    generation = value.get("generation")
    if type(generation) is not int and not (isinstance(generation, str) and generation):
        raise ValueError("endpoint generation missing")
    return endpoint_id, generation


def _guard_sequence(items, name):
    if not isinstance(items, list):
        raise ValueError(f"{name} must be a list")
    out = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError(f"{name} entries must be objects")
        sequence = item.get("sequence")
        if type(sequence) is not int or sequence < 0:
            raise ValueError(f"{name} sequence must be a nonnegative integer")
        out.append(sequence)
    if len(set(out)) != len(out):
        raise ValueError(f"{name} has duplicate sequences")
    return out


def evaluate_match_compatibility_guard(facts) -> dict:
    """Evaluate the G01 relation guard. Absence of evidence is never a pass."""
    result = _guard_result()
    if not isinstance(facts, dict):
        return _guard_unknown(result, "guard_facts_not_an_object")
    try:
        writer_id, writer_generation = _guard_identity(facts.get("writer"))
        reader_id, reader_generation = _guard_identity(facts.get("reader"))
        submitted = facts.get("submitted")
        if not isinstance(submitted, dict):
            raise ValueError("submitted reliability missing")
        offered, requested = submitted.get("writer_reliability"), submitted.get("reader_reliability")
        if offered not in _OFFERED or requested not in _OFFERED:
            return _guard_unknown(result, "unsupported_reliability_value")
        window = facts.get("window")
        if not isinstance(window, dict):
            raise ValueError("observation window missing")
        start, end, complete = window.get("start_ns"), window.get("end_ns"), window.get("complete")
        if type(start) is not int or type(end) is not int or start < 0 or end <= start:
            raise ValueError("invalid observation window")
        if type(complete) is not bool:
            raise ValueError("window completeness must be boolean")
    except (ValueError, TypeError) as exc:
        return _guard_unknown(result, f"guard_evidence_invalid:{exc}")

    compatible = (offered, requested) not in _INCOMPATIBLE_PAIRS
    result["applicability"].update(offered=offered, requested=requested, compatible=compatible)
    result["closure"]["window_complete"] = complete

    match = facts.get("match")
    correlated_match = False
    if match is not None:
        if not isinstance(match, dict):
            return _guard_unknown(result, "guard_match_not_an_object")
        if match.get("identity_correlated") is not True:
            return _guard_unknown(result, "match_without_identity_correlation")
        if (match.get("writer_id"), match.get("writer_generation")) != (writer_id, writer_generation) or \
                (match.get("reader_id"), match.get("reader_generation")) != (reader_id, reader_generation):
            return _guard_unknown(result, "match_identity_mismatch")
        at_ns = match.get("at_ns")
        if type(at_ns) is not int or not start <= at_ns <= end:
            return _guard_unknown(result, "match_outside_observation_window")
        correlated_match = True
        result["closure"]["match_correlated"] = True

    delivery = facts.get("delivery")
    correlated_delivery = False
    if delivery is not None:
        if not isinstance(delivery, dict):
            return _guard_unknown(result, "guard_delivery_not_an_object")
        if delivery.get("identity_correlated") is not True:
            return _guard_unknown(result, "delivery_without_identity_correlation")
        try:
            sent = _guard_sequence(delivery.get("sent"), "sent")
            received = _guard_sequence(delivery.get("received"), "received")
        except ValueError as exc:
            return _guard_unknown(result, f"guard_delivery_invalid:{exc}")
        if not sent:
            return _guard_unknown(result, "delivery_window_has_no_send_evidence")
        if any(item.get("writer_id") != writer_id or item.get("writer_generation") != writer_generation
               for item in delivery.get("sent", [])) or \
                any(item.get("reader_id") != reader_id or item.get("reader_generation") != reader_generation
                    for item in delivery.get("received", [])):
            return _guard_unknown(result, "delivery_identity_mismatch")
        if received:
            if not correlated_match:
                return _guard_unknown(result, "delivery_observed_without_correlated_match")
            correlated_delivery = True
            result["closure"]["delivery_correlated"] = True

    if not complete:
        return _guard_unknown(result, "observation_window_incomplete")

    if not compatible:
        if correlated_delivery:
            result.update(verdict="fail", reason_codes=["prohibited_delivery_observed"],
                          forbidden_residuals=["prohibited_delivery_observed"])
            return result
        if correlated_match:
            result.update(verdict="fail", reason_codes=["prohibited_match_observed"],
                          forbidden_residuals=["prohibited_match_observed"])
            return result
        observed = facts.get("observed")
        absence_proven = (isinstance(observed, dict)
                          and observed.get("match") == "absent"
                          and observed.get("delivery") == "absent"
                          and observed.get("observer_complete") is True
                          and observed.get("provenance") is True)
        if absence_proven:
            result.update(verdict="pass", reason_codes=["incompatible_pair_not_matched"])
            return result
        return _guard_unknown(result, "prohibited_event_absence_not_established")

    if correlated_match:
        result.update(verdict="pass", reason_codes=["compatible_pair_matched"])
        return result
    return _guard_unknown(result, "compatible_pair_unmatched_within_finite_window")
