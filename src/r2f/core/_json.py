"""Strict JSON helpers shared by the contracts."""

import copy
import json
import math
import re
import sys
from collections.abc import Mapping
from enum import Enum

# An integer of at most 640 bits has at most 193 decimal digits, which is
# below CPython's smallest configurable int->str digit limit (640).
_INT_FAST_BITS = 640


class _Opaque(Exception):
    """Internal signal: this tree needs the legacy plain()+deepcopy path."""


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


def _checked(value):
    """Reproduce json.dumps(value, allow_nan=False) acceptance and error text."""
    try:
        json.dumps(value, allow_nan=False)
    except ValueError as exc:
        raise ValueError("non-finite JSON number") from exc
    return value


def _integer(value):
    if value.bit_length() <= _INT_FAST_BITS:
        return value
    read_limit = getattr(sys, "get_int_max_str_digits", None)
    if read_limit is None:  # no digit limit in this interpreter
        return value
    limit = read_limit()
    if limit == 0 or limit >= value.bit_length():
        return value
    # int->str raises exactly when the magnitude needs more than limit digits,
    # i.e. when it is >= 10 ** limit.
    magnitude = -value if value < 0 else value
    if magnitude < 10 ** limit:
        return value
    return _checked(value)


def _mapping(value):
    for key in value:
        if type(key) is not str:
            raise _Opaque
    return {key: _native(item) for key, item in value.items()}


def _sequence(value):
    return [_native(item) for item in value]


def _native(value):
    """Rebuild an all-builtin tree; raise _Opaque for anything else."""
    kind = type(value)
    if kind is str or kind is bool:
        return value
    if kind is dict:
        return _mapping(value)
    if kind is list or kind is tuple:
        return _sequence(value)
    if value is None:
        return None
    if kind is int:
        return _integer(value)
    if kind is float:
        return value if math.isfinite(value) else _checked(value)
    raise _Opaque


def snapshot(value):
    """Return a fresh JSON tree with no aliasing to (or inside) the input."""
    try:
        return _native(value)
    except (_Opaque, RecursionError):
        # Enum / custom Mapping / to_dict / subclasses keep the legacy
        # plain()+deepcopy behaviour verbatim, including its call order.
        # A tree too deep for the probe is retried the same way as well: the
        # legacy path spends one frame per level while plain() runs, so it can
        # still reach a domain error the probe would have masked as
        # RecursionError. Both paths reject; this keeps the reported
        # exception identical to the pre-change implementation.
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
