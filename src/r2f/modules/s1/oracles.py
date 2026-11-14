"""Typed primary delivery oracle for the two late-reader targets.

An unreceived tail has no finite failure proof. A later delivered sequence closes
only intermediate omissions. Prejoin volatile samples carry no obligation.
"""
from .facts import FactError, PrimaryFacts
from .targets import RULES, EXECUTION_ORDER


def evaluate_primary(facts: dict, target_id=EXECUTION_ORDER[0]) -> dict:
    result = {"rule_id": RULES[target_id], "rule_role": "primary", "verdict": "unknown",
              "reason_codes": [], "forbidden_residuals": [], "closure": {"kind": "delivered_later_sequence", "proven_gaps": [], "tail_inflight": []}}
    if target_id not in EXECUTION_ORDER[:2]:
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
    if f.reader.created_ns <= f.writer.created_ns:
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
        residuals.append("post_match_delivery_loss")
    result["forbidden_residuals"] = residuals
    if residuals:
        result.update(verdict="fail", reason_codes=residuals)
    elif tail:
        result["reason_codes"] = ["tail_inflight"]
    else:
        result.update(verdict="pass", reason_codes=["matched_postjoin_delivery_contract_satisfied"])
    return result
