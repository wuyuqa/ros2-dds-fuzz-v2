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


def source_manifest(repo):
    repo = Path(repo).resolve()
    result = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=10)
    head = result.stdout.strip() if result.returncode == 0 else None
    files = sorted(path for folder in (repo / "src", repo / "config") for path in folder.rglob("*") if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc")
    setup = repo / "scripts/setup_overlay.bash"
    if setup.is_file():
        files.append(setup)
    digest = hashlib.sha256()
    dirty, tracked = [], []
    if head:
        listing = subprocess.run(["git", "-C", str(repo), "ls-tree", "-r", "--name-only", "HEAD", "--", "src", "config", "scripts/setup_overlay.bash"], capture_output=True, text=True, timeout=10)
        tracked = listing.stdout.splitlines() if listing.returncode == 0 else []
        files = sorted(set(files) | {repo / p for p in tracked})
    for path in files:
        relative = str(path.relative_to(repo))
        if "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        if not path.is_file():
            dirty.append(relative)
            digest.update(relative.encode() + b"\0<missing>\0")
            continue
        content = path.read_bytes()
        digest.update(relative.encode() + b"\0" + content + b"\0")
        if not head or relative not in tracked:
            dirty.append(relative)
            continue
        committed = subprocess.run(["git", "-C", str(repo), "show", "HEAD:" + relative], capture_output=True, timeout=10)
        if committed.returncode or committed.stdout != content:
            dirty.append(relative)
    return {"source_repo": str(repo), "head": head, "behavior_tree_sha256": digest.hexdigest(),
            "source_frozen": bool(head) and not dirty,
            "dirty_behavior_paths": sorted(set(dirty))}


def semantic_input(arm, parameters=None):
    p = dict(parameters or {})
    supported = {"topic", "qos", "pre_match_count", "post_match_count", "reader_delay_sec", "baseline_reader_delay_sec", "match_timeout_sec", "delivery_timeout_sec", "startup_timeout_sec", "publish_interval_sec"}
    if set(p) - supported:
        raise ValueError("unknown semantic parameter(s): " + ",".join(sorted(set(p) - supported)))
    base_delay = p.get("baseline_reader_delay_sec", 0.02)
    result = {
        "topic": p.get("topic", "/r2f/s1/late_endpoint"), "message_type": "std_msgs/msg/String",
        "qos": p.get("qos", {"reliability": "reliable", "durability": "volatile", "depth": 64}),
        "pre_match_count": p.get("pre_match_count", 2), "post_match_count": p.get("post_match_count", 6),
        "reader_delay_sec": p.get("reader_delay_sec", 0.15) if arm == "mutated" else base_delay,
        "match_timeout_sec": p.get("match_timeout_sec", 8.0),
        "delivery_timeout_sec": p.get("delivery_timeout_sec", 5.0),
        "startup_timeout_sec": p.get("startup_timeout_sec", 10.0),
        "publish_interval_sec": p.get("publish_interval_sec", 0.03),
        "fault_id": "late_endpoint_delivery_omission" if arm == "negative" else None,
    }
    if result["qos"].get("reliability") != "reliable" or result["qos"].get("durability") != "volatile":
        raise ValueError("late-reader contract requires reliable volatile QoS")
    if type(result["post_match_count"]) is not int or result["post_match_count"] < 3:
        raise ValueError("post_match_count requires an intermediate omission and a later sentinel")
    reject_execution_identity(result)
    return result


def runtime_config(submitted_input, **execution):
    final_input = submitted_input
    """The capsule's exact submitted values reach normalized runtime config.

    The runtime selects baseline_reader_delay_sec for every non-mutated arm;
    supply the semantic delay there too, rather than relying on its default.
    Omission is the middle postmatch sequence selected by fault_id in runtime.
    """
    reject_execution_identity(final_input)
    config = dict(execution, **final_input)
    config["baseline_reader_delay_sec"] = final_input["reader_delay_sec"]
    config["mutation_parameters"] = {"reader_delay_sec": final_input["reader_delay_sec"]}
    from r2f.runtime.ros2_pair import normalize_config
    normalized = normalize_config(config)
    for key, value in final_input.items():
        if normalized.get(key) != value:
            raise ValueError(f"runtime normalized away submitted {key}")
    arm = normalized["arm"]
    delay = normalized["reader_delay_sec"] if arm == "mutated" else normalized["baseline_reader_delay_sec"]
    if delay != final_input["reader_delay_sec"]:
        raise ValueError("runtime effective reader delay mismatch")
    return normalized
