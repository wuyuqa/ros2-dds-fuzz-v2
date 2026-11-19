"""Compatible cross-run aggregation of canonical FeedbackLog artifacts.

No runner activation, BFS admission, corpus novelty or candidate accounting.
The ordered output is a FeedbackLog, not a second feedback/event schema.
Run-local novelty counts are never summed into global unique edge coverage.
Callers must bind compatibility metadata to the authoritative run manifest and
rule contract; self-consistent artifacts alone do not certify raw execution.
"""
from __future__ import annotations

from copy import deepcopy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile

from r2f.core.evidence import canonical_hash
from r2f.core.state_signature import STATE_ABSTRACTION_VERSION, EDGE_SCHEMA
from r2f.core.topology_state import SCHEMA as TOPOLOGY_STATE, EDGE_SCHEMA as TOPOLOGY_EDGE
from .feedback import FeedbackLog

SCHEMA = "r2f.s1-feedback-aggregate/v1"
_COMPATIBILITY_KEYS = {
    "source_head", "source_behavior_tree_sha256", "target_id", "target_revision",
    "rule_id", "rule_sha256", "state_signature_version", "edge_schema",
}
_SIGNATURES = {
    STATE_ABSTRACTION_VERSION: (EDGE_SCHEMA, "r2f.s1-state-projection/v1"),
    TOPOLOGY_STATE: (TOPOLOGY_EDGE, "r2f.s1-topology-state-projection/v1"),
}


def _compatibility(value):
    if not isinstance(value, dict) or set(value) != _COMPATIBILITY_KEYS:
        raise ValueError("feedback compatibility fields missing or unknown")
    for key in ("source_head", "source_behavior_tree_sha256", "rule_sha256"):
        size = 40 if key == "source_head" else 64
        if not isinstance(value[key], str) or not re.fullmatch(r"[0-9a-f]{%d}" % size, value[key]):
            raise ValueError("invalid compatibility " + key)
    for key in ("target_id", "rule_id", "state_signature_version", "edge_schema"):
        if not isinstance(value[key], str) or not value[key].strip():
            raise ValueError("invalid compatibility " + key)
    if type(value["target_revision"]) is not int or value["target_revision"] < 0:
        raise ValueError("invalid compatibility target_revision")
    signature = _SIGNATURES.get(value["state_signature_version"])
    if signature is None or signature[0] != value["edge_schema"]:
        raise ValueError("unsupported feedback state signature/edge schema")
    return deepcopy(value)


def feedback_compatibility(source_manifest, *, target_id, target_revision, rule_id,
                           rule_sha256, state_signature_version):
    """Build the gate from a frozen canonical source and authoritative contract.

    The caller obtains target/revision/rule hash from the registry for that run,
    not the current checkout when importing history. Profile/run IDs and RNG do
    not define compatibility; changing source, rule or state schema does.
    """
    if (not isinstance(source_manifest, dict) or source_manifest.get("source_frozen") is not True
            or source_manifest.get("dirty_behavior_paths") != []):
        raise ValueError("feedback persistence requires frozen source manifest")
    signature = _SIGNATURES.get(state_signature_version)
    if signature is None:
        raise ValueError("unsupported feedback state signature")
    return _compatibility({
        "source_head": source_manifest.get("head"),
        "source_behavior_tree_sha256": source_manifest.get("behavior_tree_sha256"),
        "target_id": target_id, "target_revision": target_revision,
        "rule_id": rule_id, "rule_sha256": rule_sha256,
        "state_signature_version": state_signature_version, "edge_schema": signature[0],
    })


class FeedbackAggregate:
    """Append-only imports keyed by stable run ID and run-local event offset.

    Reimporting an unchanged prefix is idempotent; shrinking/replacing one is
    an error. Identical events in distinct runs remain distinct observations.
    Global import order is retained across interleaved resume of older runs.
    """
    def __init__(self, compatibility):
        self._compatibility = _compatibility(compatibility)
        self._runs = {}
        self._order = []

    def _check_compatibility(self, value):
        if _compatibility(value) != self._compatibility:
            raise ValueError("incompatible feedback source/target/rule/state signature")

    def _validated_log(self, data):
        log = FeedbackLog.from_dict(data)
        projection_schema = _SIGNATURES[self._compatibility["state_signature_version"]][1]
        for event in log.events:
            if event["coverage"] and event.get("oracle_verdict") not in ("pass", "fail"):
                raise ValueError("unknown/nonjudgment feedback must not award coverage")
            if event.get("edge_id") is not None:
                receipt = event["edge_provenance"]["observation"]["typed_facts"]["state_projection_receipt"]
                if receipt.get("schema") != projection_schema:
                    raise ValueError("feedback state signature does not match projection")
        return log

    def import_run(self, run_id, feedback, *, compatibility):
        self._check_compatibility(compatibility)
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("stable nonempty feedback run_id required")
        log = self._validated_log(feedback)
        previous = self._runs.get(run_id, [])
        if len(log.events) < len(previous) or log.events[:len(previous)] != previous:
            raise ValueError("feedback run prefix replaced or truncated")
        before = set(self.feedback_log().edge_counts())
        imported = len(log.events) - len(previous)
        self._order.extend([run_id, i] for i in range(len(previous), len(log.events)))
        self._runs[run_id] = deepcopy(log.events)
        after = set(self.feedback_log().edge_counts())
        return {"run_id": run_id, "imported_events": imported,
                "run_local_unique_edges": len(log.edge_counts()),
                "global_new_edge_ids": sorted(after - before),
                "global_unique_edges": len(after), "observation_events": len(self._order)}

    def feedback_log(self):
        """Detached canonical log, suitable for explicit scheduler epoch input.

        Wiring remains the runner owner's responsibility. Never swap a running
        scheduler's historical prefix or infer BFS eligibility from this log.
        """
        return FeedbackLog.from_events([self._runs[run][i] for run, i in self._order])

    def statistics(self):
        counts = self.feedback_log().edge_counts()
        local_unique = sum(len(FeedbackLog.from_events(events).edge_counts()) for events in self._runs.values())
        return {"run_count": len(self._runs), "observation_events": len(self._order),
                "edge_observation_events": sum(counts.values()), "global_unique_edges": len(counts),
                "run_local_unique_edge_sum": local_unique,
                "cross_run_duplicate_edge_claims": local_unique - len(counts),
                "edge_counts": dict(sorted(counts.items()))}

    def to_dict(self):
        body = {"schema": SCHEMA, "compatibility": deepcopy(self._compatibility),
                "runs": {run: FeedbackLog.from_events(events).to_dict()
                         for run, events in sorted(self._runs.items())},
                "order": deepcopy(self._order), "statistics": self.statistics()}
        return {**body, "identity": canonical_hash(body)}

    @classmethod
    def from_dict(cls, data, *, compatibility):
        if not isinstance(data, dict) or data.get("schema") != SCHEMA:
            raise ValueError("unsupported feedback aggregate schema")
        aggregate = cls(compatibility)
        aggregate._check_compatibility(data.get("compatibility"))
        if not isinstance(data.get("runs"), dict) or not isinstance(data.get("order"), list):
            raise ValueError("invalid feedback aggregate runs/order")
        for run, log in data["runs"].items():
            if not isinstance(run, str) or not run.strip():
                raise ValueError("invalid feedback aggregate run_id")
            aggregate._runs[run] = aggregate._validated_log(log).events
        offsets = {run: 0 for run in aggregate._runs}
        for ref in data["order"]:
            if (not isinstance(ref, list) or len(ref) != 2 or not isinstance(ref[0], str)
                    or ref[0] not in offsets or type(ref[1]) is not int
                    or ref[1] != offsets[ref[0]] or ref[1] >= len(aggregate._runs[ref[0]])):
                raise ValueError("invalid feedback aggregate order/prefix")
            offsets[ref[0]] += 1
        if any(offsets[run] != len(events) for run, events in aggregate._runs.items()):
            raise ValueError("feedback aggregate order omits events")
        aggregate._order = deepcopy(data["order"])
        if aggregate.to_dict() != data:
            raise ValueError("feedback aggregate identity or statistics mismatch")
        return aggregate

    @classmethod
    def load(cls, path, *, compatibility):
        with Path(path).open(encoding="utf-8") as stream:
            return cls.from_dict(json.load(stream), compatibility=compatibility)


def _atomic_json(path, data):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix="." + path.name + ".", delete=False) as stream:
            temporary = stream.name
            json.dump(data, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            os.unlink(temporary)


def merge_feedback_file(path, *, compatibility, run_id, feedback):
    """Locked read/validate/merge + atomic replace; no stale-writer overwrite.

    Durable state contains the canonical log bytes and event positions, not just
    counters. All cooperating writers use this operation and a stable sidecar
    lock (which must not be unlinked). Readers see either full old or full new
    JSON. A failed import never overwrites prior evidence; retry after a crash
    uses the same run ID/prefix to avoid double counting.
    """
    path = Path(path).absolute()
    lock_path = path.with_name(path.name + ".lock")
    for candidate in (path, lock_path, *path.parents):
        if candidate.is_symlink():
            raise ValueError("feedback aggregate path must not traverse symlinks")
    for candidate in (path, lock_path):
        if candidate.exists():
            info = candidate.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("feedback aggregate path must be a single-link regular file")
    if lock_path.exists() and lock_path.stat().st_size != 0:
        raise ValueError("feedback aggregate lock path is not an empty lock file")
    path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        aggregate = (FeedbackAggregate.load(path, compatibility=compatibility) if path.exists()
                     else FeedbackAggregate(compatibility))
        before = aggregate.to_dict()
        receipt = aggregate.import_run(run_id, feedback, compatibility=compatibility)
        after = aggregate.to_dict()
        if not path.exists() or after != before:
            _atomic_json(path, after)
        with path.open("rb") as stream:
            digest = hashlib.sha256()
            for block in iter(lambda: stream.read(1048576), b""):
                digest.update(block)
            receipt["aggregate_file_sha256"] = digest.hexdigest()
        receipt["aggregate_identity"] = after["identity"]
        receipt["statistics"] = after["statistics"]
        return aggregate, receipt
