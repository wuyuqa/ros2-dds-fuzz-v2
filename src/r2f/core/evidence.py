"""Canonical input identity and raw-byte evidence identity.

input-identity/v1 uses deterministic input_layer_receipts. Execution receipts,
wall-clock event times, campaign kind and origin domain never amend input identity.
"""

import hashlib
import json
from pathlib import Path

from ._json import sha256_digest, snapshot

INPUT_SCHEMA = "r2f.input-identity/v1"
INPUT_KEYS = frozenset({"target_id", "target_revision", "target_sha256", "mutation_kind", "carrier",
                        "mutation_parameters", "rmw", "repeat", "seed", "arm", "mutation_layer_order",
                        "input_layer_receipts", "final_input", "state_steps", "source_manifest"})
INPUT_REQUIRED = frozenset({"target_id", "target_revision", "mutation_kind", "carrier", "mutation_parameters",
                            "rmw", "repeat", "mutation_layer_order", "input_layer_receipts", "final_input"})
_EVENT_KEYS = frozenset({"hit", "applied", "effect_observed", "execution_stage", "timestamp", "event_time",
                         "started_at", "finished_at", "mutation_receipt"})


def canonical_hash(data):
    encoded = json.dumps(snapshot(data), sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                         allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _deterministic_receipt(value):
    if isinstance(value, dict):
        if _EVENT_KEYS.intersection(value):
            raise ValueError("input_layer_receipts cannot contain execution event fields")
        for child in value.values():
            _deterministic_receipt(child)
    elif isinstance(value, list):
        for child in value:
            _deterministic_receipt(child)


def effective_input_hash(data):
    data = snapshot(data)
    if not isinstance(data, dict) or not INPUT_REQUIRED.issubset(data):
        raise ValueError("effective input is missing required identity fields")
    if "layer_receipts" in data:
        raise ValueError("use deterministic input_layer_receipts, not ambiguous layer_receipts")
    if not isinstance(data["input_layer_receipts"], list):
        raise ValueError("input_layer_receipts must be a list")
    if any(not isinstance(receipt, dict) for receipt in data["input_layer_receipts"]):
        raise ValueError("input_layer_receipts must contain dicts")
    _deterministic_receipt(data["input_layer_receipts"])
    return canonical_hash({"schema": INPUT_SCHEMA, **{key: data[key] for key in INPUT_KEYS if key in data}})


def raw_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_raw(path, digest):
    sha256_digest(digest, "raw_sha256")
    if raw_sha256(path) != digest:
        raise ValueError("raw SHA256 mismatch")
    return True
