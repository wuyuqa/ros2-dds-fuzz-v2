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

    def await_sample(publish_id):
        # Observe the exact later sample, not every sample the filter may reject.
        append(publish_id + "_observed", "barrier", params={
            "condition": "delivery_sentinel", "timeout_sec": 5.0,
            "endpoint_ids": [e["logical_id"] for e in program["endpoints"]],
            "publish_action_ids": [publish_id]}, dependencies=[publish_id])

    append("matched", "barrier", params={"condition": "matched", "timeout_sec": 5.0,
           "endpoint_ids": [e["logical_id"] for e in program["endpoints"]]},
           dependencies=[a["action_id"] for a in actions])
    append("original", "publish_batch", "w0", {"count": 1, "start_sequence": 0,
           "payload": ORIGINAL_TOKEN})
    await_sample("original")
    append("allow_all", "update_filter", "r0", {"expression": "", "parameters": []})
    append("published_prior", "publish_batch", "w0", {"count": 1, "start_sequence": 1,
           "payload": "L04PRIOR4D8E"})
    await_sample("published_prior")
    # Only sentinel-bearing complete String.data matches; original has no such token.
    append("reject_original", "update_filter", "r0",
           {"expression": "data LIKE %0", "parameters": ["'%" + SENTINEL_TOKEN + "%'"]})
    append("repeat_rejected", "publish_batch", "w0", {"count": 1, "start_sequence": 2,
           "repeat_data_from": "original"}, dependencies=["original", "reject_original"])
    append("reject_sentinel", "publish_batch", "w0", {"count": 1, "start_sequence": 3,
           "payload": SENTINEL_TOKEN})
    await_sample("reject_sentinel")
    append("reject_snapshot", "snapshot", params={"label": "reject-window"})
    append("restore_allow_all", "update_filter", "r0", {"expression": "", "parameters": []})
    append("repeat_restored", "publish_batch", "w0", {"count": 1, "start_sequence": 4,
           "repeat_data_from": "original"}, dependencies=["original", "restore_allow_all"])
    append("restore_sentinel", "publish_batch", "w0", {"count": 1, "start_sequence": 5,
           "payload": SENTINEL_TOKEN})
    await_sample("restore_sentinel")
    append("restore_snapshot", "snapshot", params={"label": "restore-window"})
    program["actions"] = actions
    if topology_model.validate_program(program)["status"] != "valid":
        raise ValueError("CFT matrix seed violated the topology contract")
    return program


CFT_LIFECYCLE_SEED = "topology.cft_lifecycle"
UPDATED_TOKEN = "L04UPDATED6E4F"


def cft_lifecycle_program():
    """Opt-in initial/update/disable/enable input; no authored sample verdicts."""
    program = cft_matrix_program()
    for endpoint in program['endpoints']:
        if endpoint['logical_id'] == 'r0':
            endpoint['filter'] = {'expression': 'data LIKE %0',
                                  'parameters': ["'%" + SENTINEL_TOKEN + "%'"],
                                  'evaluation': None}
    actions = [a for a in program['actions'] if a['op'] == 'create']

    def append(aid, op, eid=None, params=None):
        actions.append(topology_model._action(aid, op, eid, 1 if eid else None,
                       params, depends_on=[actions[-1]['action_id']]))

    append('matched', 'barrier', params={'condition': 'matched', 'timeout_sec': 5.0,
           'endpoint_ids': ['w0', 'r0', 'r1']})
    phases = [
        ('initial', None, ORIGINAL_TOKEN, SENTINEL_TOKEN),
        ('updated', ('update_parameters', 'data LIKE %0', ["'%" + UPDATED_TOKEN + "%'"]),
         SENTINEL_TOKEN, UPDATED_TOKEN),
        ('disabled', ('disable', '', []), ORIGINAL_TOKEN, SENTINEL_TOKEN),
        ('enabled', ('enable', 'data LIKE %0', ["'%" + SENTINEL_TOKEN + "%'"]),
         ORIGINAL_TOKEN, SENTINEL_TOKEN),
        ('restored', ('restore_disabled', '', []), ORIGINAL_TOKEN, SENTINEL_TOKEN),
    ]
    for index, (phase, update, first, sentinel) in enumerate(phases):
        if update is not None:
            aid, expression, parameters = update
            append(aid, 'update_filter', 'r0', {'expression': expression, 'parameters': parameters})
        for offset, payload in enumerate((first, sentinel)):
            aid = phase + ('_sample' if offset == 0 else '_sentinel')
            params = {'count': 1, 'start_sequence': index * 2 + offset, 'payload': payload}
            if phase == 'restored' and offset == 0:
                params.pop('payload')
                params['repeat_data_from'] = 'initial_sample'
            append(aid, 'publish_batch', 'w0', params)
        append(phase + '_observed', 'barrier', params={'condition': 'delivery_sentinel',
               'timeout_sec': 5.0, 'endpoint_ids': ['w0', 'r0', 'r1'],
               'publish_action_ids': [phase + '_sentinel']})
        append(phase + '_snapshot', 'snapshot', params={'label': phase})
    program['actions'] = actions
    if topology_model.validate_program(program)['status'] != 'valid':
        raise ValueError('CFT lifecycle seed violated the topology contract')
    return program
