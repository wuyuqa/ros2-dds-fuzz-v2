"""Exact snapshot compatibility, detached containers and native-path work bounds.

The legacy reference is pinned to the source captured before this optimization.
No ROS, worker, network, campaign or source-freeze operation is run here.
"""
import copy
import enum
import hashlib
import json
import math
import sys
from collections import UserDict
from collections.abc import Mapping

import pytest

from r2f.core import _json


def legacy_plain(value):
    if isinstance(value, enum.Enum):
        return value.value
    if hasattr(value, "to_dict"):
        return legacy_plain(value.to_dict())
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("JSON object keys must be strings")
        return {key: legacy_plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [legacy_plain(item) for item in value]
    if value is None or type(value) in (str, int, float, bool):
        try:
            json.dumps(value, allow_nan=False)
        except ValueError as exc:
            raise ValueError("non-finite JSON number") from exc
        return value
    raise ValueError(f"not JSON serializable: {type(value).__name__}")


def legacy_snapshot(value):
    return copy.deepcopy(legacy_plain(value))


def canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()


def outcome(call, value):
    try:
        return ("accepted", call(value))
    except Exception as exc:
        cause = exc.__cause__
        return ("rejected", type(exc), str(exc), type(cause), str(cause))


@pytest.mark.parametrize("value", [
    None, True, False, 0, 1, -1, 2**63, 2**2000, 0.0, -0.0, 1e-300, 1e300,
    "", "你好\n\x00\ud800", [], {}, (), (1, 2, {"a": (True, None)}),
    {"a": [1, {"b": -0.0}, [True, False, None]], "z": (), "": "value"},
])
def test_native_values_match_legacy_types_and_serialization(value):
    before = legacy_snapshot(value)
    after = _json.snapshot(value)
    assert type(after) is type(before)
    assert after == before
    # Lone surrogates remain accepted by snapshot; ensure_ascii=True can still
    # serialize them exactly as the pre-change json.dumps scalar validation did.
    assert json.dumps(after, sort_keys=True) == json.dumps(before, sort_keys=True)
    if type(after) is float:
        assert math.copysign(1, after) == math.copysign(1, before)
    if type(after) is bool:
        assert type(after) is bool


def test_native_containers_are_fresh_and_repeated_input_aliases_are_split():
    shared = {"nested": [1, {"x": 2}]}
    original = {"a": shared, "b": shared, "tuple": (shared,)}
    result = _json.snapshot(original)
    reference = legacy_snapshot(original)
    assert result == reference
    assert result is not original
    assert result["a"] is not result["b"]
    assert result["a"] is not result["tuple"][0]
    assert result["a"]["nested"] is not shared["nested"]
    result["a"]["nested"][1]["x"] = 99
    assert shared["nested"][1]["x"] == 2
    assert result["b"]["nested"][1]["x"] == 2
    shared["nested"].append("later input edit")
    assert "later input edit" not in result["b"]["nested"]


def test_nested_tuple_is_a_new_list_not_a_shared_mutable_reference():
    source = ([1], ({"x": []},))
    result = _json.snapshot(source)
    assert type(result) is list and type(result[1]) is list
    result[0].append(2)
    result[1][0]["x"].append(3)
    assert source == ([1], ({"x": []},))


class MutableEnum(enum.Enum):
    ITEM = {"list": [1], "nested": {"a": []}}


class TextEnum(str, enum.Enum):
    ITEM = "hello"


class NumberEnum(enum.IntEnum):
    ITEM = 7


@pytest.mark.parametrize("value", [MutableEnum.ITEM, TextEnum.ITEM, NumberEnum.ITEM,
                                   {"x": [MutableEnum.ITEM, NumberEnum.ITEM]}])
def test_enum_fallback_preserves_legacy_values(value):
    assert outcome(_json.snapshot, value) == outcome(legacy_snapshot, value)


def test_enum_mutable_value_is_deeply_isolated_and_legacy_alias_topology_preserved():
    value = MutableEnum.ITEM.value
    value["list"][:] = [1]
    value["nested"]["a"].clear()
    result = _json.snapshot([MutableEnum.ITEM, MutableEnum.ITEM, value])
    reference = legacy_snapshot([MutableEnum.ITEM, MutableEnum.ITEM, value])
    assert result == reference
    assert result[0] is result[1]  # deepcopy memo of repeated Enum.value
    assert result[0] is not result[2]  # native plain() container was reconstructed
    assert result[0] is not value
    result[0]["nested"]["a"].append(4)
    assert result[1]["nested"]["a"] == [4]
    assert result[2]["nested"]["a"] == []
    assert value["nested"]["a"] == []


def test_enum_payload_is_not_newly_revalidated_or_normalized():
    class Unchecked(enum.Enum):
        VALUE = {9: (float("nan"), {1, 2})}
    result = _json.snapshot(Unchecked.VALUE)
    assert set(result) == {9}
    assert type(result[9]) is tuple
    assert math.isnan(result[9][0])
    assert result[9][1] == {1, 2}
    assert result[9][1] is not Unchecked.VALUE.value[9][1]


class ToDict:
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    def to_dict(self):
        self.calls += 1
        return self.payload


class PropertyToDict(ToDict):
    def __init__(self, payload):
        super().__init__(payload)
        self.lookups = 0

    @property
    def to_dict(self):
        self.lookups += 1
        def convert():
            self.calls += 1
            return self.payload
        return convert


@pytest.mark.parametrize("factory", [ToDict, PropertyToDict])
def test_to_dict_calls_and_property_lookups_match_once_per_occurrence(factory):
    old = factory({"list": [{"x": []}]})
    new = factory({"list": [{"x": []}]})
    assert _json.snapshot({"x": [new, new]}) == legacy_snapshot({"x": [old, old]})
    assert new.calls == old.calls == 2
    assert getattr(new, "lookups", 0) == getattr(old, "lookups", 0)
    assert new.payload == {"list": [{"x": []}]}
    result = _json.snapshot(new)
    result["list"][0]["x"].append(1)
    assert new.payload["list"][0]["x"] == []


def test_to_dict_nested_conversion_no_extra_calls_on_native_probe():
    obj = ToDict(ToDict({"x": []}))
    assert _json.snapshot({"native_first": [1, 2, 3], "converted": obj}) == {"native_first": [1, 2, 3], "converted": {"x": []}}
    assert obj.calls == obj.payload.calls == 1


def test_to_dict_failure_keeps_error_and_call_count():
    class Explodes:
        def __init__(self): self.calls = 0
        def to_dict(self):
            self.calls += 1
            raise RuntimeError("conversion failure")
    old, new = Explodes(), Explodes()
    assert outcome(_json.snapshot, [new]) == outcome(legacy_snapshot, [old])
    assert new.calls == old.calls == 1


def test_to_dict_lookup_failure_is_not_hidden_or_retried():
    class LookupFailure:
        @property
        def to_dict(self): raise RuntimeError("lookup failure")
    assert outcome(_json.snapshot, LookupFailure()) == outcome(legacy_snapshot, LookupFailure())


class StringSubclass(str):
    pass


class IntSubclass(int):
    pass


class ListSubclass(list):
    def to_dict(self): return {"override": list(self)}


@pytest.mark.parametrize("value", [
    UserDict({"a": [1, 2]}), {StringSubclass("a"): [1]}, ListSubclass([1, 2]),
    {"a": UserDict({"b": (1, 2)})}, StringSubclass("a"), IntSubclass(1),
])
def test_custom_mappings_keys_containers_and_scalar_subclasses_match_legacy(value):
    assert outcome(_json.snapshot, value) == outcome(legacy_snapshot, value)


def test_custom_string_key_deepcopy_is_not_skipped():
    class Key(str):
        def __deepcopy__(self, memo):
            return "copied-key"
    value = {Key("original-key"): [1]}
    assert _json.snapshot(value) == legacy_snapshot(value) == {"copied-key": [1]}


@pytest.mark.parametrize("value", [
    float("nan"), float("inf"), -float("inf"), {"a": [float("nan")]},
    {1: "not-string"}, {(1,): "not-string"}, {False: "not-string"},
    {"a": [{1: 1}]}, {1, 2}, b"bytes", bytearray(b"bytes"), object(),
    complex(1, 2), {"a": object()}, {"a": [None, {"n": float("inf")}]},
])
def test_invalid_values_preserve_exception_type_message_and_cause(value):
    assert outcome(_json.snapshot, value) == outcome(legacy_snapshot, value)


def test_key_validation_precedes_value_conversion_and_late_invalid_scalar():
    obj = ToDict({"x": 1})
    # Legacy plain() validates every mapping key before visiting any value.
    with pytest.raises(ValueError, match="JSON object keys must be strings"):
        _json.snapshot({"first": obj, 3: None})
    assert obj.calls == 0
    old, new = ToDict({"x": 1}), ToDict({"x": 1})
    assert outcome(_json.snapshot, {"first": new, "second": float("nan")}) == outcome(
        legacy_snapshot, {"first": old, "second": float("nan")})
    assert old.calls == new.calls == 1


@pytest.mark.parametrize("height", [1, 20, 63, 64, 65, 100, 150, 200, 300, 600])
@pytest.mark.parametrize("kind", ["list", "dict", "tuple"])
def test_deep_nesting_preserves_acceptance_or_recursion_failure(height, kind):
    value = 0
    for _ in range(height):
        value = [value] if kind == "list" else {"x": value} if kind == "dict" else (value,)
    before = outcome(legacy_snapshot, value)
    after = outcome(_json.snapshot, value)
    assert after[0] == before[0]
    if before[0] == "rejected":
        assert after[1] is before[1] is RecursionError
    else:
        assert canonical(after[1]) == canonical(before[1])


def test_native_cycle_still_rejected_with_recursion_error():
    cycle = []; cycle.append(cycle)
    assert outcome(_json.snapshot, cycle)[1] is RecursionError
    assert outcome(legacy_snapshot, cycle)[1] is RecursionError


def test_enum_value_cycles_preserve_legacy_deepcopy_behavior():
    class Cyclic(enum.Enum):
        VALUE = []
    value = Cyclic.VALUE.value
    value.append(value)
    try:
        actual = _json.snapshot(Cyclic.VALUE)
        assert actual is not value and actual[0] is actual
    finally:
        value.clear()


@pytest.mark.parametrize("limit", [0, 640, 4300])
def test_large_integer_limit_acceptance_and_errors_exactly_match_python_encoder(limit):
    previous = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(limit)
        for digits in (602, 603, 639, 640, 641, 4299, 4300, 4301, 5000):
            value = 10 ** (digits - 1)
            for sign in (1, -1):
                candidate = {"integer": sign * value}
                assert outcome(_json.snapshot, candidate) == outcome(legacy_snapshot, candidate)
    finally:
        sys.set_int_max_str_digits(previous)


def test_native_path_avoids_scalar_json_encoding_and_redundant_deepcopy(monkeypatch):
    def unnecessary(*args, **kwargs):
        raise AssertionError("native snapshot must not encode scalar JSON or deepcopy its fresh tree")
    value = {"x": [None, True, 1, -2, 2**1999, 0.125, "x"], "tuple": ({"a": []},)}
    monkeypatch.setattr(_json.json, "dumps", unnecessary)
    monkeypatch.setattr(_json.copy, "deepcopy", unnecessary)
    result = _json.snapshot(value)
    assert result["x"] == value["x"]
    assert result["tuple"] == [{"a": []}]


def test_plain_public_api_itself_is_unchanged():
    value = MutableEnum.ITEM
    assert _json.plain(value) is value.value  # NOT deep-copied/normalized
    assert _json.plain((1, 2)) == [1, 2]
    assert outcome(_json.plain, float("inf")) == outcome(legacy_plain, float("inf"))


# --- optimizer-resume additions: recursion-budget and statelessness parity ---

def _nest(height, kind, leaf=0):
    value = leaf
    for _ in range(height):
        value = [value] if kind == "list" else {"x": value} if kind == "dict" else (value,)
    return value


def _accepts(call, value):
    """True when the call got past traversal; only frame exhaustion is a no."""
    try:
        call(value)
    except RecursionError:
        return False
    except Exception:
        return True
    return True


def _depth_boundary(call, kind, leaf=0, limit=1000):
    """Largest nesting depth accepted, as a single frame-budget measurement."""
    previous = sys.getrecursionlimit()
    sys.setrecursionlimit(limit)
    try:
        low, high = 1, limit
        while low < high:
            middle = (low + high + 1) // 2
            if _accepts(call, _nest(middle, kind, leaf)):
                low = middle
            else:
                high = middle - 1
        return low
    finally:
        sys.setrecursionlimit(previous)


class _LeafEnum(enum.Enum):
    VALUE = 1


class _LeafToDict:
    def to_dict(self):
        return {"k": [1]}


class _LeafStr(str):
    pass


@pytest.mark.parametrize("limit", [400, 1000, 2000])
@pytest.mark.parametrize("kind", ["list", "dict", "tuple"])
@pytest.mark.parametrize("leaf", [_LeafEnum.VALUE, _LeafToDict(), _LeafStr("s")])
def test_opaque_fallback_consumes_the_same_frame_budget_as_legacy(limit, kind, leaf):
    """A native prelude that bails out must not shrink the reachable depth.

    The fallback runs the untouched plain()+deepcopy pair, so the accepted
    nesting depth has to match the pre-change implementation exactly.
    """
    legacy = _depth_boundary(legacy_snapshot, kind, leaf, limit)
    current = _depth_boundary(_json.snapshot, kind, leaf, limit)
    assert current == legacy


@pytest.mark.parametrize("limit", [400, 1000])
@pytest.mark.parametrize("width", [1, 2, 8])
def test_wide_containers_do_not_change_the_frame_budget(limit, width):
    """Wide fan-out creates no extra persistent frame in either implementation."""

    def build(height):
        value = 0
        for _ in range(height):
            value = [value] + [0] * (width - 1)
        return value

    previous = sys.getrecursionlimit()
    sys.setrecursionlimit(limit)
    try:
        bounds = {}
        for label, call in (("legacy", legacy_snapshot), ("current", _json.snapshot)):
            low, high = 1, limit
            while low < high:
                middle = (low + high + 1) // 2
                if _accepts(call, build(middle)):
                    low = middle
                else:
                    high = middle - 1
            bounds[label] = low
    finally:
        sys.setrecursionlimit(previous)
    assert bounds["current"] == bounds["legacy"]


@pytest.mark.parametrize("limit", [640, 700, 4300])
def test_integer_digit_limit_is_read_at_call_time_not_cached(limit):
    """No snapshot state may be reused across calls with a new digit limit.

    ``set_int_max_str_digits`` only accepts 0 or values >= 640, so the wide
    setting below is a legal value that accepts every magnitude used here.
    """
    generous = max(limit, 4300) + 1000
    big = 10 ** (limit + 5)              # needs limit + 6 digits
    small = 10 ** (limit - 5)            # needs limit - 4 digits
    previous = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(generous)
        assert outcome(_json.snapshot, big)[0] == "accepted"
        sys.set_int_max_str_digits(limit)
        assert outcome(_json.snapshot, big) == outcome(legacy_snapshot, big)
        assert outcome(_json.snapshot, small) == outcome(legacy_snapshot, small)
        sys.set_int_max_str_digits(generous)
        assert outcome(_json.snapshot, big)[0] == "accepted"
        assert outcome(_json.snapshot, small) == outcome(legacy_snapshot, small)
    finally:
        sys.set_int_max_str_digits(previous)


def test_repeated_calls_never_share_output_containers():
    """No memo, hash or cache may alias two snapshots of equal input."""
    value = {"a": [1, {"b": ()}], "c": (True, 1.5)}
    first = _json.snapshot(value)
    second = _json.snapshot(value)
    assert first == second
    for key in ("a", "c"):
        assert first[key] is not second[key]
        assert first[key] is not value[key]
    first["a"][1]["b"] = "mutated"
    assert second["a"][1]["b"] == []
    assert value["a"][1]["b"] == ()
