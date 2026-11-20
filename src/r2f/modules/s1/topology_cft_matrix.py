"""Opt-in dynamic CFT input; no filter-effect evaluation or execution evidence.

LIKE is evaluated against the complete serialized std_msgs/String.data, NOT
against declared_payload. ASCII alphanumeric tokens have no LIKE metacharacters.
Repeats request the original raw send's *complete* String.data, preserving writer
and reader generations. Without independent DDS occurrence identity, repeated
identical data can remain attribution-ambiguous: input intent grants no effect.
"""
from . import topology_model

CFT_MATRIX_SEED = "topology.cft_matrix"
ORIGINAL_TOKEN = "L04ORIGINAL7C2A"
SENTINEL_TOKEN = "L04SENTINEL9B3D"


def cft_matrix_program():
    program = topology_model.generate_program(11, writers=1, readers=2)
    for endpoint in program["endpoints"]:
        endpoint["topic"] = "/r2f/topology/cftmatrix"
        endpoint["qos"].update(reliability="reliable", history="keep_last", depth=128)
        if endpoint["logical_id"] == "r0":
            endpoint["filter"] = {"expression": "", "parameters": [], "evaluation": None}
    actions = [a for a in program["actions"] if a["op"] == "create"]

    def append(aid, op, eid=None, params=None, dependencies=None):
        deps = dependencies if dependencies is not None else [actions[-1]["action_id"]]
        actions.append(topology_model._action(aid, op, eid, 1 if eid else None,
                                              params, depends_on=deps))

    append("matched", "barrier", params={"condition": "matched", "timeout_sec": 5.0,
           "endpoint_ids": [e["logical_id"] for e in program["endpoints"]]},
           dependencies=[a["action_id"] for a in actions])
    append("original", "publish_batch", "w0", {"count": 1, "start_sequence": 0,
           "payload": ORIGINAL_TOKEN})
    append("allow_all", "update_filter", "r0", {"expression": "", "parameters": []})
    append("published_prior", "publish_batch", "w0", {"count": 1, "start_sequence": 1,
           "payload": "L04PRIOR4D8E"})
    # Only sentinel-bearing complete String.data matches; original has no such token.
    append("reject_original", "update_filter", "r0",
           {"expression": "data LIKE %0", "parameters": ["'%" + SENTINEL_TOKEN + "%'"]})
    append("repeat_rejected", "publish_batch", "w0", {"count": 1, "start_sequence": 2,
           "repeat_data_from": "original"}, dependencies=["original", "reject_original"])
    append("reject_sentinel", "publish_batch", "w0", {"count": 1, "start_sequence": 3,
           "payload": SENTINEL_TOKEN})
    append("reject_snapshot", "snapshot", params={"label": "reject-window"})
    append("restore_allow_all", "update_filter", "r0", {"expression": "", "parameters": []})
    append("repeat_restored", "publish_batch", "w0", {"count": 1, "start_sequence": 4,
           "repeat_data_from": "original"}, dependencies=["original", "restore_allow_all"])
    append("restore_sentinel", "publish_batch", "w0", {"count": 1, "start_sequence": 5,
           "payload": SENTINEL_TOKEN})
    append("restore_snapshot", "snapshot", params={"label": "restore-window"})
    program["actions"] = actions
    if topology_model.validate_program(program)["status"] != "valid":
        raise ValueError("CFT matrix seed violated the topology contract")
    return program
