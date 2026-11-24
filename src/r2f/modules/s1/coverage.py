"""Captured S1 knowledge crosswalk, independent of execution qualification.

No results directory or network is read at runtime. Captured research remains
bound to its original source, and never grants coverage to a calling checkout.
"""
import hashlib
import json
import re
from importlib.resources import files

SCHEMA = "r2f.s1-coverage-lineage/v1"
_CAPTURE = files(__package__).joinpath("s1_coverage.json")
_LIFECYCLE_CAPTURE = files(__package__).joinpath("fastdds_lifecycle_calibration.json")


def catalog_lineage():
    """Return detached source captures; counts have distinct units."""
    data = json.loads(_CAPTURE.read_text(encoding="utf-8"))
    if data.get("schema") != SCHEMA:
        raise ValueError("unsupported S1 coverage lineage schema")
    _attach_lifecycle_calibration(data)
    for row in data["obligations"]:
        row["case_ids"] = [case["case_id"] for case in data["cases"]
                           if row["id"] in case["coverage_rows"]]
    from .catalog import catalog_declarations, target_catalog
    data["seed_lineage"], seeds = _seed_metadata()
    data["model_bindings"] = {}
    declarations = {}
    for entry in target_catalog():
        scope = (entry["usage"], entry["runtime_capability"])
        if scope not in declarations:
            declarations[scope] = catalog_declarations(exploratory=scope[0] == "fuzz",
                                                       runtime_capability=scope[1])
        target = declarations[scope][entry["target_id"]]
        data["model_bindings"][target.target_id] = {**_model_binding(target, seeds, data["seed_lineage"]), **entry}
    for story in data["user_stories"]:
        story["obligation_ids"] = [row["id"] for row in data["obligations"]
                                   if story["id"] in row["story_ids"]]
    _validate(data)
    return data


def _attach_lifecycle_calibration(data):
    """Extend the query, never the original byte-pinned knowledge capture."""
    addition = json.loads(_LIFECYCLE_CAPTURE.read_text(encoding="utf-8"))
    if (not isinstance(addition, dict)
            or addition.get("schema") != "r2f.s1-lifecycle-calibration-capture/v1"
            or addition.get("case_id") != "Fast-DDS-issue3770"):
        raise ValueError("invalid S1 lifecycle calibration: capture scope")
    sources = addition.get("source_captures")
    if not isinstance(sources, dict) or set(sources).intersection(data["source_captures"]):
        raise ValueError("invalid S1 lifecycle calibration: captured source overwrite")
    cases = [row for row in data["cases"] if row["case_id"] == addition["case_id"]]
    if len(cases) != 1 or "lifecycle_calibration" in cases[0]:
        raise ValueError("invalid S1 lifecycle calibration: original case overwrite or missing")
    data["source_captures"].update(sources)
    cases[0]["lifecycle_calibration"] = addition.get("lifecycle_calibration")


def target_lineage(target):
    """Attach original declarations as context, never infer execution support."""
    data = catalog_lineage()
    data["model_binding"] = data["model_bindings"][target.target_id]
    data["contextual_obligation_ids"] = [row["id"] for row in data["obligations"]
        if any(link["target_id"] == target.target_id for link in row["model_links"])]
    return data


def _seed_metadata():
    """Separate captured declarations from the current strict corpus read."""
    from .fuzz_catalog import AUTHORED_SEEDS, SEED_DIR, load_seeds, seed_content_sha256

    lineage = json.loads(files(__package__).joinpath("seed_lineage.json").read_text(encoding="utf-8"))
    if (lineage.get("schema") != "r2f.s1-seed-lineage/v1"
            or set(lineage["seeds"]) != set(AUTHORED_SEEDS)):
        raise ValueError("invalid S1 seed lineage capture")
    # A present corpus (including a broken link or incomplete parent) must go
    # through the unchanged strict loader. Never hide a CatalogError as unknown.
    if (SEED_DIR.exists() or SEED_DIR.is_symlink()
            or SEED_DIR.parent.exists() or SEED_DIR.parent.is_symlink()):
        seeds = load_seeds()["seeds"]
        refs = {sid: {
            "seed_id": sid, "surface_refs": list(seed["surface_refs"]),
            "provider": "r2f.modules.s1.fuzz_catalog.load_seeds",
            "content_sha256": seed_content_sha256(seed), "status": seed["status"],
            "capability": seed["capability"], "execution_evidence": False,
            "source_file": {"path": f"config/s1_fuzz/seeds/{sid.lower()}.json",
                "file_sha256": hashlib.sha256((SEED_DIR / f"{sid.lower()}.json").read_bytes()).hexdigest()},
        } for sid, seed in seeds.items()}
        lineage["reference_source"] = "strictly_validated_corpus"
        lineage["current_loadability"] = {"status": "validated", "reason": "strict_load_seeds_passed"}
    else:
        refs = lineage["seeds"]
        lineage["reference_source"] = "captured_snapshot"
        lineage["current_loadability"] = {"status": "unknown", "reason": "original_seed_corpus_absent"}
    return lineage, refs


def _model_binding(target, seeds, seed_lineage):
    from r2f.core import canonical_hash
    from .topology_targets import TOPOLOGY_TARGET

    contract = target.exploration_contract
    surface_ids = {row.surface_id for row in contract.surfaces} if contract else set()
    seed_refs = [{**seed, "metadata_source": seed_lineage["reference_source"],
                  "current_loadability": dict(seed_lineage["current_loadability"])}
                 for seed in seeds.values() if surface_ids.intersection(seed["surface_refs"])]
    seed_sources = []
    if seed_refs:
        seed_sources.append({"provider": "r2f.modules.s1.fuzz_catalog.load_seeds",
                             "metadata_source": seed_lineage["reference_source"],
                             "current_loadability": dict(seed_lineage["current_loadability"]),
                             "runtime_qualification": "unverified"})
    if target.target_id == TOPOLOGY_TARGET:
        seed_sources.append({"provider": "r2f.modules.s1.fuzz_mutators_v2.corpus_v2",
            "scope": "topology seeds; concurrent/CFT/episode variants require explicit opt-in",
            "runtime_qualification": "unverified"})
    runtime_rule = target.rule_contract.get("primary_rule") or target.rule_contract.get("compatibility_rule")
    rule_ids = sorted({runtime_rule} | {row.id for row in contract.rules}) if contract else [runtime_rule]
    return {
        "target_id": target.target_id, "target_revision": target.target_revision,
        "relation": "declared_model_context_not_full_obligation_coverage",
        "declaration_status": "model_input_only",
        "manifest_sha256": canonical_hash(target.to_dict()),
        "seed_refs": seed_refs, "seed_sources": seed_sources,
        "seed_gap": ("Required conformance scenarios are not exploratory authored seeds."
                     if not contract else "" if seed_sources else "No authored seed mapping declared."),
        "rule_lineage_refs": [{"target_id": target.target_id, "rule_id": rule_id,
            "lookup": "Registry.get_rule_lineage", "requires_registration": True} for rule_id in rule_ids],
        "surfaces": [row.to_dict() for row in contract.surfaces] if contract else [],
        "facts": [row.to_dict() for row in contract.facts] if contract else [],
        "rules": [row.to_dict() for row in contract.rules] if contract else [],
        "mutation_slots": [row.to_dict() for row in target.mutation_slots],
        "state_steps": [row.to_dict() for row in target.state_steps],
        "observation_contract": dict(target.observation_contract),
        "rule_contract": dict(target.rule_contract),
        "rmw_applicability": list(target.rmw_applicability),
    }


def _validate(data):
    """Reject dangling crosswalks and any accidental execution-credit upgrade."""
    def require(condition, reason):
        if not condition:
            raise ValueError("invalid S1 coverage lineage: " + reason)

    obligations = {row["id"]: row for row in data["obligations"]}
    stories = {row["id"] for row in data["user_stories"]}
    cases = {row["case_id"] for row in data["cases"]}
    evidence = {row["evidence_id"] for row in data["evidence_captures"]}
    require(set(obligations) == {f"C{i:02d}" for i in range(1, 19)}
            and len(data["obligations"]) == 18, "C01-C18 inventory")
    require(stories == set(range(1, 43)) and len(data["user_stories"]) == 42, "story inventory")
    require(len(cases) == len(data["cases"]) == 15, "case inventory")
    require(data["execution_evidence"] is False and data["maturity_evidence"] is False,
            "sidecar cannot grant execution or maturity")
    for source in data["source_captures"].values():
        require(re.fullmatch(r"[0-9a-f]{64}", source["artifact_sha256"]) is not None,
                "invalid captured source hash")
    for case in data["cases"]:
        require(set(case["coverage_rows"]) <= set(obligations), "unknown case obligation")
        require(case["source_ref"]["artifact_id"] in data["source_captures"], "case source")
    linked_stories = set()
    for row in obligations.values():
        require(set(row["story_ids"]) <= stories and row["story_ids"], "unknown or empty story links")
        linked_stories.update(row["story_ids"])
        require(row["applicability"]["status"] == "applicable"
                and row["applicability"]["basis"], "scope exclusions require a separate reviewed contract")
        require(row["current_source_status"] == "unverified"
                and row["missing_observation_policy"] == "unknown", "unqualified current-source status")
        require(row["remaining_obligations"], "missing remaining obligations")
        require(row["model_links"] or row["unmapped_model_reason"], "unnamed model gap")
        require(set(row["evidence_refs"]) <= evidence, "unknown evidence reference")
        for link in row["model_links"]:
            require(link["target_id"] in data["model_bindings"], "undeclared model target")
            model = data["model_bindings"][link["target_id"]]
            require(set(link["surface_ids"]) <= {s["surface_id"] for s in model["surfaces"]},
                    "undeclared model surface")
            require(link["relation"] == "context_not_full_coverage", "model is context only")
    require(linked_stories == stories, "unlinked user story")
    for capture in data["evidence_captures"]:
        require(capture["qualified_for_current_source"] is False, "historical evidence is not current credit")
        require(set(capture["obligation_ids"]) <= set(obligations), "unknown evidence obligation")
    for target_id, revision in data["required_baseline"]["target_revisions"].items():
        require(target_id in data["model_bindings"]
                and data["model_bindings"][target_id]["target_revision"] == revision == 0,
                "required target revision drift")
    require(data["required_baseline"]["cell_count"] == 72
            and len(data["required_baseline"]["target_revisions"]) == 4, "required baseline count")
    _validate_lifecycle_calibration(data)


def _validate_lifecycle_calibration(data):
    """Keep the source-case contract contextual, referenceable and unqualified."""
    def require(condition, reason):
        if not condition:
            raise ValueError("invalid S1 coverage lineage: lifecycle calibration: " + reason)

    cases = [row for row in data["cases"] if row["case_id"] == "Fast-DDS-issue3770"]
    require(len(cases) == 1, "source case missing")
    case = cases[0]
    calibration = case.get("lifecycle_calibration")
    require(isinstance(calibration, dict), "contract missing")
    require(calibration.get("schema") == "r2f.s1-lifecycle-calibration/v1"
            and calibration.get("checkpoint") == "source_lifecycle_obligations"
            and calibration.get("status") == "requirements_identified_not_validated",
            "unsupported contract scope")
    require(calibration.get("execution_evidence") is False
            and calibration.get("maturity_evidence") is False
            and type(calibration.get("new_discovery_credit")) is int
            and calibration["new_discovery_credit"] == 0, "contract cannot grant credit")
    require(set(calibration["obligation_ids"]) == {"C01", "C02", "C06", "C08", "C16"}
            and set(calibration["story_ids"]) == {1, 5, 8, 14, 20, 21, 37}
            and set(calibration["test_ids"]) == {"T08", "T09", "T11"}, "spec mapping drift")
    requirements = calibration["requirements"]
    expected = {"proxy_lifetime", "entity_incarnation", "independent_observation",
                "normal_and_observer_controls", "historical_comparison"}
    require(len(requirements) == len(expected)
            and {row["id"] for row in requirements} == expected, "lifecycle requirement missing or repeated")
    for row in requirements:
        require(row["obligation_ids"] and set(row["obligation_ids"]) <= set(calibration["obligation_ids"]),
                "unknown requirement obligation")
        require(row.get("current_source_status") == "unverified" and row.get("named_gaps")
                and row.get("statement") and row.get("evidence_required") and row.get("source_refs"),
                "requirements need evidence and named gaps, not completion labels")
    binding = {"source_repo", "head", "behavior_tree_sha256", "target_id", "target_revision",
               "input_sha256", "run_id", "rmw", "repeat", "domain_id", "observation_mode",
               "vendor_version", "build_manifest_sha256", "binary_sha256", "logical_id",
               "participant_guid", "endpoint_gid", "generation", "pid", "process_start_ticks",
               "event_index", "raw_path", "raw_sha256"}
    require(binding <= set(calibration["execution_binding_required"]), "execution or entity binding missing")
    historical = calibration["historical_comparison"]
    require(isinstance(historical, dict), "malformed historical comparison")
    require(historical.get("status") == "proposed_not_executed"
            and historical.get("reported_version_is_exact_pre") is False
            and historical.get("pre") == case["fix"]["recommended_exact_pre"]
            == case["proposed_pre_post"]["pre"]
            and historical.get("post") == case["fix"]["recommended_exact_post"]
            == case["proposed_pre_post"]["post"], "exact pre/post or historical status drift")
    require(historical.get("pre_source_ref") == {
                "artifact_id": "Fast-DDS-4220-merge.json", "json_pointer": "/parents/0/sha"}
            and historical.get("post_source_ref") == {
                "artifact_id": "Fast-DDS-4220-merge.json", "json_pointer": "/sha"},
            "exact pre/post need commit-parent source references")
    refs = [historical["pre_source_ref"], historical["post_source_ref"]]
    refs.extend(ref for row in requirements for ref in row["source_refs"])
    for ref in refs:
        require(isinstance(ref, dict), "malformed source reference")
        source = data["source_captures"].get(ref.get("artifact_id"))
        require(source is not None and source.get("format") == "json"
                and source.get("verification_scope") == "captured_artifact_only"
                and isinstance(ref.get("json_pointer"), str) and ref["json_pointer"].startswith("/"),
                "uncaptured source reference")
    policy = calibration["assessment_policy"]
    require(isinstance(policy, dict), "malformed assessment policy")
    require(policy.get("missing_observation") == "unknown"
            and policy.get("internal_race_status") == "not_observed"
            and policy.get("historical_reproduction_status") == "unverified"
            and policy.get("candidate_interface") == "existing_common_anomaly_replay_review"
            and policy.get("tool_or_control_result") == "not_natural_sut_candidate"
            and type(policy.get("historical_discovery_credit")) is int
            and policy["historical_discovery_credit"] == 0
            and policy.get("user_confirmation_required_for_new_root") is True,
            "knowledge contract cannot qualify observations or discoveries")
