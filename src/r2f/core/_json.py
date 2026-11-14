"""Strict JSON helpers shared by the contracts."""

import copy
import json
import re
from collections.abc import Mapping
from enum import Enum


def plain(value):
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "to_dict"):
        return plain(value.to_dict())
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("JSON object keys must be strings")
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    if value is None or type(value) in (str, int, float, bool):
        # Reject NaN/infinity even when nested in an otherwise valid object.
        try:
            json.dumps(value, allow_nan=False)
        except ValueError as exc:
            raise ValueError("non-finite JSON number") from exc
        return value
    raise ValueError(f"not JSON serializable: {type(value).__name__}")


def snapshot(value):
    return copy.deepcopy(plain(value))


def nonempty(value, field):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def integer(value, field, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    return value


def sha256_digest(value, field="sha256"):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be a lowercase SHA256 digest")
    return value
