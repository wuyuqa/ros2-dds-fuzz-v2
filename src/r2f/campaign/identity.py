"""Separate deterministic submitted semantics from execution environment metadata."""
from pathlib import Path
import hashlib
import subprocess

TRANSIENT_KEYS = frozenset({"origin_domain", "domain", "domain_id", "run_id", "cell_id", "output_dir", "state_dir", "raw_path", "manifest_path", "wallclock", "started_at", "finished_at"})


def reject_execution_identity(value, path="input"):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in TRANSIENT_KEYS:
                raise ValueError(f"{path}.{key} is execution metadata, not semantic input")
            reject_execution_identity(item, f"{path}.{key}")
    elif isinstance(value, list):
        for item in value:
            reject_execution_identity(item, path)


def _behavior_snapshot(repo, tracked, script_suffixes):
    """Read content, never stat/mtime, including HEAD paths missing on disk."""
    files = {path for folder in (repo / "src", repo / "config")
             for path in folder.rglob("*") if path.is_file()}
    files.update(path for path in (repo / "scripts").rglob("*")
                 if path.is_file() and path.suffix in script_suffixes)
    files.update(repo / relative for relative in tracked)
    return {str(path.relative_to(repo)): path.read_bytes() if path.is_file() else None
            for path in sorted(files)
            if "__pycache__" not in path.parts and path.suffix != ".pyc"}


def _head_behavior_blobs(repo, head, script_suffixes):
    # NUL paths avoid git's core.quotePath quoting, including tabs/newlines.
    listing = subprocess.run(
        ["git", "-C", str(repo), "ls-tree", "-r", "-z", head, "--", "src", "config", "scripts"],
        capture_output=True, timeout=10)
    if listing.returncode:
        raise ValueError("cannot enumerate tracked behavior sources at HEAD")
    tracked = {}
    try:
        records = listing.stdout.split(b"\0")
        if records.pop() != b"":
            raise ValueError("unterminated ls-tree output")
        for record in records:
            metadata, raw_path = record.split(b"\t", 1)
            _mode, kind, oid = metadata.split(b" ")
            relative = raw_path.decode()
            if not relative.startswith("scripts/") or Path(relative).suffix in script_suffixes:
                tracked[relative] = (kind, oid)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("cannot parse tracked behavior sources at HEAD") from exc
    return tracked


def _batch_behavior_blobs(repo, tracked, snapshot):
    # Request only object IDs, not paths: --batch's line input is unambiguous
    # even when a path contains newlines. HEAD is pinned by the ls-tree OIDs.
    oids = list(dict.fromkeys(tracked[relative][1] for relative, content in snapshot.items()
                             if content is not None and relative in tracked))
    if not oids:
        return {}
    batch = subprocess.run(["git", "-C", str(repo), "cat-file", "--batch"],
                           input=b"".join(oid + b"\n" for oid in oids),
                           capture_output=True, timeout=10)
    if batch.returncode:
        raise ValueError("cannot read committed behavior source blobs")
    blobs, offset = {}, 0
    try:
        for oid in oids:
            end = batch.stdout.index(b"\n", offset)
            actual_oid, kind, raw_size = batch.stdout[offset:end].split(b" ")
            if actual_oid != oid or kind != b"blob" or not raw_size.isdigit():
                raise ValueError("unexpected object header")
            size = int(raw_size)
            offset = end + 1
            end = offset + size
            if end >= len(batch.stdout) or batch.stdout[end:end + 1] != b"\n":
                raise ValueError("truncated object body")
            blobs[oid] = batch.stdout[offset:end]
            offset = end + 1
        if offset != len(batch.stdout):
            raise ValueError("trailing batch data")
    except ValueError as exc:
        raise ValueError("cannot parse committed behavior source blobs") from exc
    return blobs


def source_manifest(repo):
    repo = Path(repo).resolve()
    command = ["git", "-C", str(repo), "rev-parse", "HEAD"]
    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    head = result.stdout.strip() if result.returncode == 0 else None
    script_suffixes = {".py", ".sh", ".bash"}
    tracked = _head_behavior_blobs(repo, head, script_suffixes) if head else {}
    snapshot = _behavior_snapshot(repo, tracked, script_suffixes)
    blobs = _batch_behavior_blobs(repo, tracked, snapshot) if head else {}
    digest = hashlib.sha256()
    dirty = []
    for relative, content in snapshot.items():
        if content is None:
            dirty.append(relative)
            digest.update(relative.encode() + b"\0<missing>\0")
            continue
        digest.update(relative.encode() + b"\0" + content + b"\0")
        if not head or relative not in tracked or blobs[tracked[relative][1]] != content:
            dirty.append(relative)
    # A bounded second content read catches persistent edits/additions/deletions
    # during collection without trusting mtime, size, or an identity cache.
    if _behavior_snapshot(repo, tracked, script_suffixes) != snapshot:
        raise ValueError("behavior sources changed while collecting source manifest")
    final = subprocess.run(command, capture_output=True, text=True, timeout=10)
    final_head = final.stdout.strip() if final.returncode == 0 else None
    if final_head != head:
        raise ValueError("HEAD changed while collecting source manifest")
    return {"source_repo": str(repo), "head": head, "behavior_tree_sha256": digest.hexdigest(),
            "source_frozen": bool(head) and not dirty,
            "dirty_behavior_paths": sorted(set(dirty))}


def semantic_input(arm, parameters=None, *, target_id="discovery.late_endpoint"):
    p = dict(parameters or {})
    if target_id == "builtin.discovery_projection_reconciliation":
        return projection_input(arm, p)
    supported = {"topic", "qos", "pre_match_count", "post_match_count", "reader_delay_sec", "baseline_reader_delay_sec", "match_timeout_sec", "delivery_timeout_sec", "startup_timeout_sec", "publish_interval_sec"}
    recovery = target_id == "discovery.interruption_recovery"
    discovery_defaults = {"drop_duration_sec": 3.0, "recovery_timeout_sec": 8.0, "counter_interval_sec": 0.25}
    if recovery:
        supported |= set(discovery_defaults) | {"observe_participant_leases"}
    if set(p) - supported:
        raise ValueError("unknown semantic parameter(s): " + ",".join(sorted(set(p) - supported)))
    if "observe_participant_leases" in p and type(p["observe_participant_leases"]) is not bool:
        raise ValueError("observe_participant_leases must be a bool")
    base_delay = p.get("baseline_reader_delay_sec", 0.02)
    result = {
        "topic": p.get("topic", "/r2f/s1/discovery_recovery" if recovery else "/r2f/s1/late_endpoint"), "message_type": "std_msgs/msg/String",
        "qos": p.get("qos", {"reliability": "reliable", "durability": "volatile", "depth": 64}),
        "pre_match_count": p.get("pre_match_count", 2), "post_match_count": p.get("post_match_count", 6),
        "reader_delay_sec": p.get("reader_delay_sec", base_delay if recovery else 0.15) if arm == "mutated" else base_delay,
        "match_timeout_sec": p.get("match_timeout_sec", 8.0),
        "delivery_timeout_sec": p.get("delivery_timeout_sec", 5.0),
        "startup_timeout_sec": p.get("startup_timeout_sec", 10.0),
        "publish_interval_sec": p.get("publish_interval_sec", 0.03),
        "fault_id": ("post_restore_delivery_omission" if recovery else "late_endpoint_delivery_omission") if arm == "negative" else None,
    }
    if result["qos"].get("reliability") != "reliable" or result["qos"].get("durability") != "volatile":
        raise ValueError("late-reader contract requires reliable volatile QoS")
    if type(result["post_match_count"]) is not int or result["post_match_count"] < 3:
        raise ValueError("post_match_count requires an intermediate omission and a later sentinel")
    if recovery:
        result.update({key: p.get(key, default) for key, default in discovery_defaults.items()})
        # Supporting advertisement observation only; omission preserves legacy identity.
        if "observe_participant_leases" in p:
            result["observe_participant_leases"] = p["observe_participant_leases"]
    reject_execution_identity(result)
    return result


def runtime_config(submitted_input, **execution):
    final_input = submitted_input
    if execution.get("target_id") == "builtin.discovery_projection_reconciliation":
        reject_execution_identity(final_input)
        config = dict(execution, **final_input)
        config["mutation_parameters"] = {"baseline_qos":final_input["baseline_qos"], "submitted_qos":final_input["qos"]}
        from r2f.runtime.ros2_pair import normalize_config
        normalized = normalize_config(config)
        from r2f.runtime.projection_pair import normalize_projection_config
        normalized = normalize_projection_config(normalized)
        for key, value in final_input.items():
            if normalized.get(key) != value:
                raise ValueError(f"runtime normalized away submitted projection {key}")
        return normalized
    """The capsule's exact submitted values reach normalized runtime config.

    The runtime selects baseline_reader_delay_sec for every non-mutated arm;
    supply the semantic delay there too, rather than relying on its default.
    Omission is the middle postmatch sequence selected by fault_id in runtime.
    """
    reject_execution_identity(final_input)
    config = dict(execution, **final_input)
    config["baseline_reader_delay_sec"] = final_input["reader_delay_sec"]
    recovery = execution.get("target_id") == "discovery.interruption_recovery"
    config["mutation_parameters"] = ({key: final_input[key] for key in ("drop_duration_sec", "recovery_timeout_sec", "counter_interval_sec")}
                                     if recovery else {"reader_delay_sec": final_input["reader_delay_sec"]})
    from r2f.runtime.ros2_pair import normalize_config
    normalized = normalize_config(config)
    if recovery:
        from r2f.runtime.namespace_pair import normalize_discovery_config
        normalized = normalize_discovery_config(normalized)
    for key, value in final_input.items():
        if normalized.get(key) != value:
            raise ValueError(f"runtime normalized away submitted {key}")
    arm = normalized["arm"]
    delay = normalized["reader_delay_sec"] if arm == "mutated" else normalized["baseline_reader_delay_sec"]
    if delay != final_input["reader_delay_sec"]:
        raise ValueError("runtime effective reader delay mismatch")
    return normalized


def projection_input(arm, parameters=None):
    """Initial fourth-target payload; no execution/source paths enter identity."""
    from copy import deepcopy
    p = dict(parameters or {})
    supported = {"topic", "baseline_qos", "variant_qos", "match_timeout_sec", "startup_timeout_sec", "projection_timeout_sec", "projection_discovery_layers", "projection_discovery_tools", "projection_sut_environment"}
    if set(p) - supported:
        raise ValueError("unknown projection parameters: " + ",".join(sorted(set(p) - supported)))
    baseline = deepcopy(p.get("baseline_qos", {"reliability": "reliable", "durability": "volatile", "depth": 64,
        "deadline_ns": 9223372036854775807, "liveliness": "automatic", "liveliness_lease_duration_ns": 9223372036854775807}))
    variant = deepcopy(p.get("variant_qos", dict(baseline, deadline_ns=1250000000)))
    if baseline == variant:
        raise ValueError("endpoint QoS variant must actually differ from baseline")
    selected = deepcopy(variant if arm == "mutated" else baseline)
    result = {"topic": p.get("topic", "/r2f/s1/discovery_projection"), "message_type": "std_msgs/msg/String",
              "baseline_qos": baseline, "qos": selected, "reader_qos": deepcopy(selected),
              "match_timeout_sec": p.get("match_timeout_sec", 8.0), "startup_timeout_sec": p.get("startup_timeout_sec", 10.0),
              "projection_timeout_sec": p.get("projection_timeout_sec", 8.0),
              "fault_id": "controlled_projection_field_mismatch" if arm == "negative" else None}
    if "projection_discovery_layers" in p:
        from r2f.runtime.discovery_profiles import validate_discovery_layers
        # Validate the observation token here; runtime_config separately checks
        # the actual cell RMW and continues to reject non-Cyclone admission.
        validate_discovery_layers(p["projection_discovery_layers"], "rmw_cyclonedds_cpp")
        result["projection_discovery_layers"] = p["projection_discovery_layers"]
    if "projection_discovery_tools" in p:
        from r2f.runtime.discovery_tools import validate_tools_reference
        result["projection_discovery_tools"] = deepcopy(p["projection_discovery_tools"])
        validate_tools_reference(result)
    if "projection_sut_environment" in p:
        from r2f.runtime.projection_sut import validate_sut_reference
        result["projection_sut_environment"] = deepcopy(p["projection_sut_environment"])
        validate_sut_reference(result)
    reject_execution_identity(result)
    return result
