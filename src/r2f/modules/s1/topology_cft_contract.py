"""Execution-side CFT predicate contract; existing mutation manifests stay frozen.

A source-bound observation extension, not a new input language or DDS matching
policy. Its supporting result is aggregated by the existing topology oracle.
"""
RULE = "s1.topology.cft_predicate.v1"
RESIDUAL = "topology.cft_predicate_mismatch"


def predicate_contract():
    return {
        "schema": "r2f.cft-predicate-contract/v1", "rule_id": RULE,
        "rule_role": "supporting", "verdict_rule_id": "s1.topology.program_consistency.v1",
        "spec_refs": ["S1#6:C14/T10", "S1#19"],
        "forbidden_residuals": [RESIDUAL],
        "predicate_scope": "complete std_msgs/msg/String.data; empty filter or data LIKE %0 with one quoted ASCII literal and percent wildcards",
        "evidence_scope": "authenticated configuration, producer occurrence, current reader incarnation, native decision or direct occurrence delivery",
        "missing_policy": "unknown; missing delivery never proves filter rejection",
        "conflict_policy": "native reject plus same-occurrence delivery requires review; no SUT candidate",
        "filter_false_implies_unmatch": False,
        "input_contract_changed": False,
    }
