"""Canonical input identity and raw-byte evidence identity.

Two input-identity schemas coexist:

* ``r2f.input-identity/v1`` is the historical contract.  It hashes a fixed
  whitelist of fields and deliberately ignores campaign/runtime metadata such
  as ``origin_domain``.  Existing capsules keep their exact old digest.
* ``r2f.input-identity/v2`` additionally binds versioned exploration inputs
  (mutation plan, operations, runtime windows, model/contract references).
  Unlike v1 it does not silently drop unrecognised fields: every key is either
  explicitly part of the identity, explicitly declared non-identity execution
  metadata, or rejected with an error.

Execution receipts, wall-clock event times, campaign kind and origin domain
never amend input identity in either schema.  Per-layer execution hits are not
input identity either; :func:`execution_chain_status` aggregates those from a
separate execution-layer structure.
"""

import hashlib
import json
from pathlib import Path

from ._json import sha256_digest, snapshot

INPUT_SCHEMA = "r2f.input-identity/v1"
INPUT_SCHEMA_V2 = "r2f.input-identity/v2"
INPUT_SCHEMA_VERSIONS = frozenset({INPUT_SCHEMA, INPUT_SCHEMA_V2})

INPUT_KEYS = frozenset({"target_id", "target_revision", "target_sha256", "mutation_kind", "carrier",
                        "mutation_parameters", "rmw", "repeat", "seed", "arm", "mutation_layer_order",
                        "input_layer_receipts", "final_input", "state_steps", "source_manifest"})
INPUT_REQUIRED = frozenset({"target_id", "target_revision", "mutation_kind", "carrier", "mutation_parameters",
                            "rmw", "repeat", "mutation_layer_order", "input_layer_receipts", "final_input"})

# v2 additions.  ``input_identity_schema`` is included in the digest so a v2
# capsule cannot be replayed as a v1 capsule with a trimmed field set.
IDENTITY_V2_KEYS = frozenset(INPUT_KEYS | {
    "input_identity_schema", "mutation_plan", "operations", "runtime_windows",
    "contract", "contract_hash", "input_model", "model_hash", "profile_id",
})
INPUT_REQUIRED_V2 = frozenset(INPUT_REQUIRED | {"input_identity_schema"})

# Keys that are explicitly *not* part of input identity.  They are accepted by
# v2 and excluded by declaration (not by silent omission).  Anything outside
# both sets is rejected.
NON_IDENTITY_V2_KEYS = frozenset({
    "run_id", "campaign_id", "campaign_kind", "origin_domain", "domain", "domain_id",
    "timestamp", "event_time", "started_at", "finished_at", "executed_at", "wall_time",
    "raw_path", "raw_sha256", "output_dir", "execution_stage", "stage", "mutation_receipt",
    "hit", "applied", "effect_observed", "selection_id", "case_id", "cell_id",
})

_EVENT_KEYS = frozenset({"hit", "applied", "effect_observed", "execution_stage", "timestamp", "event_time",
                         "started_at", "finished_at", "mutation_receipt", "executed_at", "wall_time",
                         "time", "observed_at"})


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


def _common_input_checks(data):
    if not isinstance(data, dict):
        raise ValueError("effective input must be a dict")
    if "layer_receipts" in data:
        raise ValueError("use deterministic input_layer_receipts, not ambiguous layer_receipts")
    if not isinstance(data.get("input_layer_receipts"), list):
        raise ValueError("input_layer_receipts must be a list")
    if any(not isinstance(receipt, dict) for receipt in data["input_layer_receipts"]):
        raise ValueError("input_layer_receipts must contain dicts")
    _deterministic_receipt(data["input_layer_receipts"])


def identity_partition(data, *, schema=INPUT_SCHEMA):
    """Return ``(identity, excluded)`` for an input-identity document.

    ``identity`` is the exact mapping that enters the digest.  ``excluded``
    lists v2 non-identity keys that were present and explicitly excluded.
    v1 keeps its historical whitelist behaviour (unknown keys are ignored);
    v2 rejects unknown keys instead of silently dropping them.
    """
    data = snapshot(data)
    if schema not in INPUT_SCHEMA_VERSIONS:
        raise ValueError(f"unsupported input identity schema: {schema!r}")
    if schema == INPUT_SCHEMA:
        identity = {key: data[key] for key in INPUT_KEYS if key in data}
        return identity, sorted(set(data) - INPUT_KEYS)

    missing = INPUT_REQUIRED_V2 - set(data)
    if missing:
        raise ValueError("effective input v2 is missing required identity fields: " + ",".join(sorted(missing)))
    declared = data["input_identity_schema"]
    if declared != INPUT_SCHEMA_V2:
        raise ValueError("input_identity_schema field does not match the requested v2 schema")
    unknown = set(data) - IDENTITY_V2_KEYS - NON_IDENTITY_V2_KEYS
    if unknown:
        raise ValueError("input-identity/v2 has undeclared fields (declare identity or not_applicable): "
                         + ",".join(sorted(unknown)))
    identity = {key: data[key] for key in IDENTITY_V2_KEYS if key in data}
    return identity, sorted(set(data) & NON_IDENTITY_V2_KEYS)


def effective_input_hash(data, *, schema=None):
    """Hash deterministic input identity.

    ``schema`` defaults to the document's declared version, else v1 for
    backward compatibility.  Unknown declared versions are rejected.
    """
    data = snapshot(data)
    if not isinstance(data, dict):
        raise ValueError("effective input must be a dict")
    declared = data.get("input_identity_schema")
    if declared is not None and declared not in INPUT_SCHEMA_VERSIONS:
        raise ValueError(f"unsupported input identity schema: {declared!r}")
    if schema is None:
        schema = declared or INPUT_SCHEMA
    if schema not in INPUT_SCHEMA_VERSIONS:
        raise ValueError(f"unsupported input identity schema: {schema!r}")
    if declared is not None and declared != schema:
        raise ValueError("declared input_identity_schema differs from requested schema")

    if schema == INPUT_SCHEMA:
        if not INPUT_REQUIRED.issubset(data):
            raise ValueError("effective input is missing required identity fields")
        # v1 historically ignored unknown keys.  It must not, however, silently
        # drop fields that only exist in the v2 identity contract: that would
        # let an exploration plan escape the digest.
        undeclared_v2 = ((set(data) - INPUT_KEYS - NON_IDENTITY_V2_KEYS) & IDENTITY_V2_KEYS) - {
            "input_identity_schema"}
        if undeclared_v2:
            raise ValueError("input-identity/v1 cannot carry v2-only identity fields: "
                             + ",".join(sorted(undeclared_v2)))
        _common_input_checks(data)
        identity, _ = identity_partition(data, schema=INPUT_SCHEMA)
        return canonical_hash({"schema": INPUT_SCHEMA, **identity})

    if not INPUT_REQUIRED_V2.issubset(data):
        raise ValueError("effective input v2 is missing required identity fields")
    _common_input_checks(data)
    identity, _ = identity_partition(data, schema=INPUT_SCHEMA_V2)
    return canonical_hash({"schema": INPUT_SCHEMA_V2, **identity})


EXECUTION_LAYER_KEYS = frozenset({"layer_id", "hit", "required", "overridden"})
CHAIN_SCHEMA = "r2f.execution-chain/v1"


def execution_chain_status(layers):
    """Aggregate real per-layer execution hits for one composite input.

    This is *not* input identity and must never be placed inside
    ``input_layer_receipts``.  Each entry describes an execution layer:
    ``layer_id``, boolean ``hit`` and optional ``required``/``overridden``.
    An overridden layer is retained for provenance but cannot contribute a hit.
    The overall chain claims mutation applied only when every required layer
    actually hit; a single miss keeps the chain from passing while preserving
    the per-layer partial results unchanged.
    """
    if not isinstance(layers, list) or not layers:
        raise ValueError("execution layers must be a non-empty list")
    checked = []
    seen = set()
    for index, layer in enumerate(layers):
        layer = snapshot(layer)
        if not isinstance(layer, dict):
            raise ValueError(f"execution layer {index} must be a dict")
        unknown = set(layer) - EXECUTION_LAYER_KEYS
        if unknown:
            raise ValueError(f"execution layer {index} has unknown fields: " + ",".join(sorted(unknown)))
        layer_id = layer.get("layer_id")
        if not isinstance(layer_id, str) or not layer_id.strip():
            raise ValueError(f"execution layer {index} requires layer_id")
        if layer_id in seen:
            raise ValueError("duplicate execution layer_id: " + layer_id)
        seen.add(layer_id)
        if type(layer.get("hit")) is not bool:
            raise ValueError(f"execution layer {layer_id}.hit must be a boolean")
        required = layer.get("required", True)
        overridden = layer.get("overridden", False)
        if type(required) is not bool or type(overridden) is not bool:
            raise ValueError(f"execution layer {layer_id} required/overridden must be booleans")
        checked.append({"layer_id": layer_id, "hit": layer["hit"],
                        "required": required, "overridden": overridden})
    effective = [layer for layer in checked if layer["required"] and not layer["overridden"]]
    if not effective:
        raise ValueError("execution chain has no required, non-overridden layer")
    missed = [layer["layer_id"] for layer in effective if not layer["hit"]]
    return {
        "schema": CHAIN_SCHEMA,
        "chain_hit": not missed,
        "layers": checked,
        "hit_layers": [layer["layer_id"] for layer in effective if layer["hit"]],
        "missed_layers": missed,
        "overridden_layers": [layer["layer_id"] for layer in checked if layer["overridden"]],
    }


RECEIPT_FIELDS = ("planned", "submitted", "carrier_admitted", "configured",
                   "applied", "hit", "effect_observed")


def validate_chain_receipt(layers, mutation_receipt):
    """Guard a composite execution against claiming a chain hit on partial layers.

    ``layers`` is the real per-layer execution evidence described by
    :func:`execution_chain_status`.  A chain whose required layer missed cannot
    carry ``applied`` or ``hit`` in its aggregate MutationReceipt, even if a
    different layer hit.  Per-layer partial results are preserved unchanged; the
    aggregate simply may not pass.  Input-layer receipts are never consulted.
    """
    chain = execution_chain_status(layers)
    if not isinstance(mutation_receipt, dict) or set(mutation_receipt) != set(RECEIPT_FIELDS):
        raise ValueError("mutation_receipt must contain exactly the seven receipt fields")
    previous = True
    for name in RECEIPT_FIELDS:
        value = mutation_receipt[name]
        if type(value) is not bool:
            raise ValueError(f"mutation_receipt.{name} must be a boolean")
        if value and not previous:
            raise ValueError(f"mutation_receipt.{name} requires all preceding receipt flags")
        previous = value
    if not chain["chain_hit"]:
        if mutation_receipt["applied"] or mutation_receipt["hit"] or mutation_receipt["effect_observed"]:
            raise ValueError("chain cannot claim mutation applied/hit/effect_observed while a "
                             "required layer missed: " + ",".join(chain["missed_layers"]))
    return chain


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


def aggregate_layer_receipts(layers):
    """Derive one truthful aggregate receipt from real per-layer receipts.

    Each layer is ``{"layer_id", "receipt", "required"?, "overridden"?}`` where
    ``receipt`` is the seven-field execution receipt for that layer.  The
    aggregate is the logical AND over required, non-overridden layers: a single
    missing layer keeps the chain at "not applied", while every per-layer
    receipt remains available for partial reporting.
    """
    if not isinstance(layers, list) or not layers:
        raise ValueError("layers must be a non-empty list")
    status_layers, receipts = [], []
    for index, layer in enumerate(layers):
        layer = snapshot(layer)
        if not isinstance(layer, dict):
            raise ValueError(f"layer {index} must be a dict")
        unknown = set(layer) - {"layer_id", "receipt", "required", "overridden"}
        if unknown:
            raise ValueError(f"layer {index} has unknown fields: " + ",".join(sorted(unknown)))
        receipt = layer.get("receipt")
        if not isinstance(receipt, dict) or set(receipt) != set(RECEIPT_FIELDS):
            raise ValueError(f"layer {index} requires a seven-field receipt")
        status_layers.append({"layer_id": layer.get("layer_id"), "hit": receipt["hit"],
                              "required": layer.get("required", True),
                              "overridden": layer.get("overridden", False)})
        receipts.append((layer["layer_id"], receipt))
    chain = execution_chain_status(status_layers)
    aggregate = {name: all(receipt[name] for _, receipt in receipts) for name in RECEIPT_FIELDS}
    # A single missed required layer keeps the chain from passing.  Clamp the
    # dependent tail flags first so the aggregate is a valid ordered receipt,
    # then run the gate that rejects any residual applied/hit claim.
    if not chain["chain_hit"]:
        aggregate["applied"] = False
        aggregate["hit"] = False
        aggregate["effect_observed"] = False
    validate_chain_receipt(status_layers, aggregate)
    return aggregate
