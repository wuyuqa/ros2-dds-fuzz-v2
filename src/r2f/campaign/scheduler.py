"""Finite required matrix only; no adaptive scheduling or automatic expansion."""
from itertools import product
from r2f.modules.s1.targets import RMWS, EXECUTION_ORDER


def first_slice_cells():
    rows = []
    for rmw, arm, repeat in product(RMWS, ("baseline", "transparent", "mutated"), range(3)):
        rows.append({"cell_id": f"{rmw}.{arm}.{repeat}", "target_id": EXECUTION_ORDER[0],
                     "rmw": rmw, "arm": arm, "repeat": repeat, "campaign_kind": "required", "control_kind": None})
    for rmw, repeat in product(RMWS, range(3)):
        rows.append({"cell_id": f"{rmw}.negative.{repeat}", "target_id": EXECUTION_ORDER[0],
                     "rmw": rmw, "arm": "negative", "repeat": repeat, "campaign_kind": "required", "control_kind": "fault", "fault_id": "late_endpoint_delivery_omission"})
    return rows


def control_references(rows):
    return {kind: [row["cell_id"] for row in rows if row["arm"] == arm]
            for kind, arm in (("health", "baseline"), ("mutation", "mutated"), ("transparency", "transparent"), ("fault", "negative"))}
