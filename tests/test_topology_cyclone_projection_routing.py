"""Offline routing contracts only; fixtures are never ROS or SUT evidence."""
from copy import deepcopy
import json

import pytest

from r2f.campaign.topology_state_projection import declares_fastdds_peer, project_topology_state
from test_topology_state_projection import RawFixture


CYCLONE_SCHEMA = "r2f.cyclone-matched-peer-observation/v1"


def cyclone_fixture(**kwargs):
    fixture = RawFixture(payload=True, publisher_gid=False, **kwargs)
    for row in fixture.events:
        row.update(rmw_identifier="rmw_cyclonedds_cpp", observation_mode="blackbox",
                   domain_id=168)
        if row["event"] == "receive":
            row["publisher_gid_status"] = "unavailable_api"
        elif row["event"] == "match_snapshot":
            row["native_peer_observation"] = {
                "schema": CYCLONE_SCHEMA, "complete": True,
                "local_gid": row["endpoint_gid"], "domain_id": 168,
                "role_kind": fixture.roles[row["logical_id"]],
                "started_monotonic_ns": row["monotonic_ns"] - 1,
                "ended_monotonic_ns": row["monotonic_ns"],
                "matched_peer_gids": deepcopy(row["matched_peer_gids"]),
            }
    return fixture


def project(tmp_path, fixture):
    path = tmp_path / "offline-cyclone.jsonl"
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n"
                            for row in fixture.events))
    return project_topology_state(fixture.program, path,
                                  before_ref=fixture.before, after_ref=fixture.after)


def pair(result):
    return result["state_after"]["pairs"][0]


def test_cyclone_public_peer_schema_is_not_a_fastdds_declaration(tmp_path):
    fixture = cyclone_fixture()
    original = deepcopy(fixture.events)
    result = project(tmp_path, fixture)
    assert pair(result)["observed_match"] == "bidirectional", result["receipt"]["reasons"]
    assert result["receipt"]["status"] == "observed"
    assert result["eligible"] is False
    assert result["receipt"]["execution_evidence"] is False
    assert fixture.events == original


@pytest.mark.parametrize("damage", ["incomplete", "complete_as_int", "outer_incomplete",
                                    "wrong_peers", "wrong_local", "wrong_domain", "bad_clock"])
def test_bad_cyclone_receipt_cannot_use_public_or_isolation_fallback(tmp_path, damage):
    fixture = cyclone_fixture(isolation=True)
    row = fixture.event("match_snapshot", logical_id="w0")
    observation = row["native_peer_observation"]
    if damage == "incomplete":
        observation["complete"] = False
    elif damage == "complete_as_int":
        observation["complete"] = 1
    elif damage == "outer_incomplete":
        row["peer_gids_complete"] = False
    elif damage == "wrong_peers":
        observation["matched_peer_gids"] = []
    elif damage == "wrong_local":
        observation["local_gid"] = "other-writer"
    elif damage == "wrong_domain":
        observation["domain_id"] += 1
    else:
        observation["started_monotonic_ns"] = observation["ended_monotonic_ns"] + 1
    result = project(tmp_path, fixture)
    assert pair(result)["observed_match"] == "unknown"
    assert any("cyclone_peer_observation" in reason for reason in result["receipt"]["reasons"])
    assert result["eligible"] is False


def add_public_publish_scopes(fixture):
    """Insert actual envelope shapes without changing the fixture's raw links."""
    events, remap = [], {}
    for old in fixture.events:
        if old["event"] == "send":
            common = {k: deepcopy(v) for k, v in old.items()
                      if k not in {"event", "event_index", "monotonic_ns"}}
            common.update(call_id="public-call-" + str(old["event_index"]),
                          command_id="public-publish-" + old["logical_id"],
                          publisher_rmw_handle=None, rcl_publisher_handle=None)
            events.extend([dict(common, event="publish_call_begin"),
                           dict(common, event="publish_call_end", trace_count=0)])
        remap[old["event_index"]] = len(events)
        events.append(deepcopy(old))
    for index, row in enumerate(events):
        row.update(event_index=index, monotonic_ns=(index + 1) * 100)
        for key in ("carrier_ref", "open_ref"):
            if key in row:
                row[key] = remap[row[key]]
        if "graph_refs" in row:
            row["graph_refs"] = [remap[ref] for ref in row["graph_refs"]]
        if row["event"] == "match_snapshot":
            row["native_peer_observation"].update(
                started_monotonic_ns=row["monotonic_ns"] - 1,
                ended_monotonic_ns=row["monotonic_ns"])
    fixture.before, fixture.after = remap[fixture.before], remap[fixture.after]
    fixture.events = events
    return fixture


def test_generic_cyclone_scopes_do_not_request_ust_timestamp_or_control(tmp_path):
    fixture = add_public_publish_scopes(cyclone_fixture())
    original = deepcopy(fixture.events)
    result = project(tmp_path, fixture)
    assert pair(result)["delivery_closure"] == "closed", result["receipt"]["reasons"]
    assert result["receipt"]["status"] == "observed"
    assert result["eligible"] is False
    assert fixture.events == original


@pytest.mark.parametrize("row,expected", [
    ({}, False),
    ({"native_peer_observation": {"schema": CYCLONE_SCHEMA},
      "peer_proof_source": "dds_matched_endpoint_api", "observation_mode": "blackbox"}, False),
    ({"native_peer_observation": None}, True),
    ({"native_peer_observation": {}}, True),
    ({"native_peer_observation": {"schema": "unknown"}}, True),
    ({"native_peer_observation": {"schema": CYCLONE_SCHEMA}}, True),
    ({"event": "native_peer_provider_selected"}, True),
    ({"peer_proof_source": "fastdds_native_stateful_peer_snapshot"}, True),
    ({"native_peer_observation": {"schema": CYCLONE_SCHEMA},
      "peer_proof_source": "dds_matched_endpoint_api", "observation_mode": "instrumentation"}, True),
])
def test_peer_declaration_interface_is_pure_dispatch_not_authentication(row, expected):
    original = deepcopy(row)
    assert declares_fastdds_peer(row) is expected
    assert row == original


@pytest.mark.parametrize("observation", [None, {}, {"schema": "unknown"}])
def test_unknown_peer_receipt_cannot_fall_back_to_public_or_isolation(tmp_path, observation):
    fixture = cyclone_fixture(isolation=True)
    fixture.event("match_snapshot", logical_id="w0")["native_peer_observation"] = observation
    result = project(tmp_path, fixture)
    assert pair(result)["observed_match"] == "unknown"
    assert any("native_peer_proof_source_mislabeled" in r for r in result["receipt"]["reasons"])


@pytest.mark.parametrize("declaration", ["null_receipt", "empty_receipt", "trace",
                                       "fastdds_scope", "instrumentation_scope"])
def test_declared_native_send_stays_strict_even_without_any_receive(tmp_path, declaration):
    fixture = add_public_publish_scopes(cyclone_fixture(missing={0, 1, 2}))
    if declaration.endswith("receipt"):
        fixture.event("send")["publish_occurrence_receipt"] = None if declaration == "null_receipt" else {}
    else:
        row = fixture.event("publish_call_begin")
        if declaration == "trace":
            row["event"] = "rmw_publish_trace"
        elif declaration == "fastdds_scope":
            row["rmw_identifier"] = "rmw_fastrtps_cpp"
        else:
            row["observation_mode"] = "instrumentation"
    result = project(tmp_path, fixture)
    assert pair(result)["delivery_closure"] == "unknown"
    assert any("native_publish_occurrence_invalid:" in r for r in result["receipt"]["reasons"])


def test_cyclone_routing_does_not_close_a_missing_trailing_sentinel(tmp_path):
    fixture = add_public_publish_scopes(cyclone_fixture(missing={2}))
    result = project(tmp_path, fixture)
    assert pair(result)["observed_match"] == "bidirectional"
    assert pair(result)["delivery_closure"] == "open"
    assert result["eligible"] is False
