"""Versioned provenance sidecar, never a predicate or execution qualification.

Resolve only after catalog membership validation. Sources identify captured
bytes, not a requirement to load a local results directory at runtime. Verified
means the cited artifact was read/hash-checked, not full normative conformance,
issue reproduction, or verification of every statement in an engineering note.
"""
from r2f.core._json import nonempty, sha256_digest, snapshot

SCHEMA = "r2f.s1-rule-lineage/v1"


def _source(source_id, version, kind, locator, digest=None, *, artifact_locator=None):
    return {"source_id": source_id, "version": version, "kind": kind,
            "locator": locator, "artifact_locator": artifact_locator or locator,
            "artifact_sha256": digest,
            "verification_status": "verified_local_artifact" if digest else "unverified",
            "verification_scope": "captured_artifact_only" if digest else "primary_not_checked"}


# Immutable artifact identities from the 2026-11-19 read-only audit. No runtime
# file existence, machine path, network access, or future source fetch is implied.
SOURCES = {
    "dds-1.4": _source("dds-1.4", "1.4/formal-15-04-10", "normative",
        "https://www.omg.org/spec/DDS/1.4/PDF",
        "16d6f8385c2ba79f7346dc18c867b624bc6dcc8fcf7c2ec52c55b7ae3dc113f2"),
    "rtps-2.5": _source("rtps-2.5", "2.5/formal-22-04-01", "normative",
        "https://www.omg.org/spec/DDSI-RTPS/2.5/PDF",
        "c362eaa590c9c95fc6223359ce1ebfa57776cf7ea1e47824f58d48dc16907088"),
    "s1-decisions": _source("s1-decisions", "snapshot-2026-11-19", "engineering",
        "repo:docs/decisions.md",
        "8e4e526e40ad0c2fafcc2bfa3c62d926a725134e0f5f859d2f30be40df320493"),
    "s1-expansion-plan": _source("s1-expansion-plan", "snapshot-2026-11-19", "engineering",
        "repo:docs/s1-fuzz-expansion-implementation-plan-2026-11-16.md",
        "6b9dd95cbfe9416214724b5e81ccc8d9405bc8051085f3878ef73a95ab94b63c"),
    "old-taxonomy": _source("old-taxonomy", "snapshot-2026-11-19", "engineering",
        "legacy-repo:DDS_three_level_spec_family_semantic_taxonomy(1).md",
        "b455d479a2d2ab40f6b42566821b20225bb20184d33f21390d39bb86df65089b"),
    "fastdds-3770": _source("fastdds-3770", "issue-3770/body-capture-2026-11-19", "issue_heuristic",
        "https://github.com/eProsima/Fast-DDS/issues/3770",
        "86e0c74f4580290db3824035b4461369a949d799d6776275af2f011e61faf7fc",
        artifact_locator="https://api.github.com/repos/eProsima/Fast-DDS/issues/3770"),
    "cyclone-2292": _source("cyclone-2292", "PR-2292/body-capture-2026-11-19", "issue_heuristic",
        "https://github.com/eclipse-cyclonedds/cyclonedds/pull/2292",
        "91e15c2daea603ef9a3f820d5c6f4df302fe7ef124475fd7e05223fb2d9eadc7",
        artifact_locator="https://api.github.com/repos/eclipse-cyclonedds/cyclonedds/pulls/2292"),
    "cyclone-1097": _source("cyclone-1097", "issue-1097", "issue_heuristic",
        "https://github.com/eclipse-cyclonedds/cyclonedds/issues/1097"),
    "cyclone-215": _source("cyclone-215", "issue-215", "issue_heuristic",
        "https://github.com/eclipse-cyclonedds/cyclonedds/issues/215"),
    "opendds-5162": _source("opendds-5162", "PR-5162", "issue_heuristic",
        "https://github.com/OpenDDS/OpenDDS/pull/5162"),
}

_LATE = "discovery.late_endpoint.match_and_receive_after_join"
_DELIVERY = "discovery.late_join.delivery_completeness"
_RECOVERY = "discovery.recovery.resume_delivery"
_PROJECTION = "builtin.discovery.builtin_matches_projection"
_GUARD = "s1.discovery.compatibility_guard.v1"
_TOPOLOGY = "s1.topology.program_consistency.v1"

# Declared catalog IDs are not necessarily IDs returned in oracle verdicts.
# Workload is supporting, evaluated with the applicable late-reader primary;
# there is no independent bounded_send verdict or coverage credit.
_CATALOG_VERDICTS = {
    "s1.late_join.delivery.v1": (_LATE, _DELIVERY),
    "s1.projection.qos_field.v1": (_PROJECTION,),
    "s1.discovery.drop_restore.v1": (_RECOVERY,),
    _GUARD: (_GUARD,),
    "s1.workload.bounded_send.v1": (_LATE, _DELIVERY),
    _TOPOLOGY: (_TOPOLOGY,),
}
_DELIVERY_FIELDS = ["writer.{id,generation,created_ns}", "reader.{id,generation,created_ns}",
                    "match.{writer_id,reader_id,at_ns,identity_correlated}",
                    "window.{start_ns,end_ns,complete}", "sent[]", "received[]"]
_RUNTIME = {
    _LATE: {"fact_fields": _DELIVERY_FIELDS, "taxonomy_l3": ["S1.B1", "S1.B4", "S3.B2"],
            "semantic_scope": "late-reader discovery prerequisites plus bounded postmatch delivery"},
    _DELIVERY: {"fact_fields": _DELIVERY_FIELDS, "taxonomy_l3": ["S1.B4", "S3.B2"],
                "semantic_scope": "reliable volatile postmatch delivery, not prejoin history retention"},
    _RECOVERY: {"fact_fields": _DELIVERY_FIELDS + ["drop", "recovery"],
                "taxonomy_l3": ["S1.B1", "S3.B2"],
                "semantic_scope": "identity-correlated discovery restore plus postrestore delivery"},
    _PROJECTION: {"fact_fields": ["writer", "reader", "window", "projection.independent_sources",
                    "projection.comparisons", "projection.baseline_fields", "projection.differences"],
                  "taxonomy_l3": ["S1.B3"],
                  "semantic_scope": "independent builtin/graph field equality; history/depth excluded"},
    _GUARD: {"fact_fields": ["writer", "reader", "submitted", "window", "match", "delivery", "observed"],
             "taxonomy_l3": ["S1.B1", "S1.B4"],
             "semantic_scope": "offered/requested reliability no-match guard, not full S2 QoS semantics"},
    _TOPOLOGY: {"fact_fields": ["fact_values", "pairs[].compatibility", "pairs[].observed_match",
                    "pairs[].communication_witnesses", "pairs[].proofs.delivery_closure",
                    "pairs[].reliable_history_contract", "pairs[].filter_free",
                    "pairs[].delivery_window_identity_complete", "endpoint_identity_complete",
                    "unknown_reasons_sample_identity", "unresolved_historical_obligations"],
                "taxonomy_l3": ["S1.B1", "S1.B4", "S3.B2"],
                "semantic_scope": "observed directed pair graph and closed per-generation delivery; "
                                  "not independent builtin projection or S8 lifecycle conformance"},
}


def _ref(source_id, locator, note):
    source = snapshot(SOURCES[source_id])
    for key in ("source_id", "version", "locator", "artifact_locator"):
        nonempty(source[key], "lineage.source." + key)
    if source["source_id"] != source_id:
        raise ValueError("lineage source identity mismatch")
    if source["kind"] not in {"normative", "engineering", "issue_heuristic"}:
        raise ValueError("unknown lineage source kind")
    status = source["verification_status"]
    if status == "verified_local_artifact":
        sha256_digest(source["artifact_sha256"], "lineage.source.artifact_sha256")
    elif status != "unverified" or source["artifact_sha256"] is not None:
        raise ValueError("unknown or contradictory lineage source verification")
    support = {"normative": "normative_support", "engineering": "engineering_contract",
               "issue_heuristic": "heuristic_only"}[source["kind"]]
    return {"source": source, "locator": nonempty(locator, "lineage.locator"),
            "support": support, "note": note}


def _refs(runtime_id):
    refs = [("old-taxonomy", "S1.B1 / S1.B3 / S1.B4 and cross-family boundaries",
             "Audit crosswalk only; old classification is not normative authority."),
            ("s1-expansion-plan", "5.3", "Existing catalog engineering spec_refs, not a normative clause.")]
    if runtime_id in {_LATE, _DELIVERY, _RECOVERY, _GUARD, _TOPOLOGY}:
        refs.append(("dds-1.4", "2.2.3.14 RELIABILITY",
                     "Offered >= requested; per-writer reliability/order under applicable history contract."))
    if runtime_id in {_LATE, _RECOVERY, _PROJECTION, _TOPOLOGY}:
        refs.append(("rtps-2.5", "8.5.3 / 8.5.4", "Participant/endpoint discovery, not a finite timeout guarantee."))
    if runtime_id == _PROJECTION:
        refs += [("dds-1.4", "2.2.5 Built-in Topics", "Remote entity/QoS information; graph normalization is engineering."),
                 ("rtps-2.5", "8.5.2", "RTPS builtin discovery endpoints."),
                 ("s1-decisions", "D014", "Nine endpoint fields; generation is not a fabricated wire field."),
                 ("cyclone-215", "issue body (unverified)", "Old B3 taxonomy inspiration, not a verified predicate mapping.")]
    if runtime_id in {_LATE, _DELIVERY, _RECOVERY, _TOPOLOGY}:
        refs.append(("s1-decisions", "D009 / D013", "Same-generation sentinel and fresh snapshot command-ACK chronology are engineering gates."))
    if runtime_id in {_LATE, _DELIVERY}:
        refs.append(("opendds-5162", "PR body (unverified)", "Old B4 harness prerequisite inspiration; no SUT reproduction claim."))
    if runtime_id in {_RECOVERY, _TOPOLOGY}:
        refs += [("cyclone-2292", "PR body", "Proxy endpoint creation/deletion crash/hang motivates exploration; normal health-gated semantic admission does not cover those symptoms."),
                 ("cyclone-1097", "issue body (unverified)", "Old B1 identity-uniqueness inspiration only.")]
    if runtime_id == _TOPOLOGY:
        refs += [("dds-1.4", "2.2.3.7 DEADLINE", "Offered period <= requested period; no deadline-missed event coverage implied."),
                 ("dds-1.4", "2.2.3.11 LIVELINESS", "Kind/lease compatibility; no expiry/refresh lifecycle evidence implied."),
                 ("s1-decisions", "D019", "N-endpoint raw observation and per-pair applicability are engineering gates."),
                 ("fastdds-3770", "issue body", "PDP concurrency coredump; old B3 label is not evidence for a builtin/graph equality predicate or crash admission.")]
    return [_ref(*ref) for ref in refs]


def resolve(target, rule_id, *, scope):
    """Resolve a catalog-validated exact target/rule; never infer missing aliases."""
    contract = target.rule_contract
    runtime_id = contract.get("primary_rule") or contract.get("compatibility_rule")
    specs = {r.id: r for r in target.exploration_contract.rules} if target.exploration_contract else {}
    from .topology_lookup_contract import RULE as LOOKUP_RULE, predicate_contract as lookup_contract
    if runtime_id == _TOPOLOGY and rule_id == LOOKUP_RULE:
        result = resolve(target, _TOPOLOGY, scope=scope)
        result.update(rule_id=LOOKUP_RULE, rule_role="supporting", independent_verdict=False,
                      verdict_semantics="shared_evaluator_not_independent",
                      execution_observation_contract=lookup_contract(),
                      fact_fields=["topic_lookup_observations.checks", "topic_lookup_observations.unknown_reasons"])
        result["scope"]["semantic_scope"] = "qualified raw-bound native topic lookup; vendor-specific scope semantics"
        return result
    from .topology_cft_contract import RULE as CFT_RULE, predicate_contract
    if runtime_id == _TOPOLOGY and rule_id == CFT_RULE:
        result = resolve(target, _TOPOLOGY, scope=scope)
        result.update(rule_id=CFT_RULE, rule_role="supporting", independent_verdict=False,
                      verdict_semantics="shared_evaluator_not_independent",
                      execution_observation_contract=predicate_contract(),
                      fact_fields=["content_filter_observations.configuration_observations",
                                   "content_filter_observations.sample_observations",
                                   "content_filter_observations.evidence_conflicts"])
        result["scope"]["semantic_scope"] = "source-bound CFT sample predicate observation, independent of endpoint matching"
        return result
    if rule_id != runtime_id and rule_id not in specs:
        raise KeyError("rule not declared for target: " + rule_id)
    runtime = _RUNTIME[runtime_id]  # Unknown runtime rule has no lineage fallback.
    spec = specs.get(rule_id)
    if spec is not None:
        if runtime_id not in _CATALOG_VERDICTS[rule_id]:
            raise KeyError("unmapped declared/runtime rule relation: " + rule_id)
        fact_ids, role = list(spec.required_fact_ids), spec.primary_or_supporting
    else:
        related = [r for rid, r in specs.items() if runtime_id in _CATALOG_VERDICTS[rid]]
        fact_ids = sorted({f for r in related for f in r.required_fact_ids})
        role = "primary"
    independent = rule_id == runtime_id
    from .coverage import target_lineage
    return snapshot({"coverage_lineage": target_lineage(target), "schema": SCHEMA, "target_id": target.target_id,
        "target_revision": target.target_revision, "rule_id": rule_id, "rule_role": role,
        "verdict_rule_ids": [runtime_id], "independent_verdict": independent,
        "verdict_semantics": "runtime_rule" if independent else "shared_evaluator_not_independent",
        "required_fact_ids": fact_ids, "fact_fields": runtime["fact_fields"],
        "fact_schema": "r2f.s1-topology-facts/v1" if runtime_id == _TOPOLOGY else None,
        "observation_schema": target.observation_contract.get("schema"),
        "scope": {**scope, "target_id": target.target_id, "semantic_scope": runtime["semantic_scope"]},
        "taxonomy": {"l3": runtime["taxonomy_l3"], "status": "audit_crosswalk_not_normative",
                     "builtin_projection_covered": runtime_id == _PROJECTION},
        "source_refs": _refs(runtime_id), "unverified_normative_clauses": [
            "Other DDS matching policies/precise durability clause not checked in this audit",
            "No normative clause claimed for PID/generation/raw chronology or finite-window proof rules"],
        "limitations": ["Metadata only; not execution authorization, a new predicate, candidate or maturity evidence.",
            "Configured/hit does not imply independent semantic effect.",
            "Crash/hang require separate SUT provenance; do not relax current health admission."],
        "execution_evidence": False, "maturity_evidence": False})
