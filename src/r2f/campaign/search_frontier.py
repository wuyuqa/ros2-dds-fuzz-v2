"""Bounded *input* search, not a SUT state/action planner or evidence corpus.

Tasks enumerate declared routes x supported sampling kinds x finite RNG slots.
A slot does not enumerate an exact boundary value or exhaust a continuous range.
The append-only journal can be deterministically replayed; checkpoint additionally
regenerates each derived plan against the authoritative target/profile contract.
"""
from copy import deepcopy

from r2f.core.evidence import canonical_hash
from .corpus import content_identity
from .mutation_routes import (canonical_route_record, has_routes, inherit_routes,
                              route_view, validate_route_entry)

SCHEMA = "r2f.input-search-frontier/v1"
SEARCH_SCHEMA = "r2f.input-search-config/v1"


def search_settings(value):
    keys = {"schema", "mode", "max_depth", "max_nodes", "max_tasks", "samples_per_class"}
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("input_search_bfs requires explicit bounded search settings")
    if value["schema"] != SEARCH_SCHEMA or value["mode"] != "input_search_bfs":
        raise ValueError("unsupported input search schema/mode")
    for key, limit in (("max_depth", 64), ("max_nodes", 10000), ("max_tasks", 100000), ("samples_per_class", 64)):
        if type(value[key]) is not int or not 1 <= value[key] <= limit:
            raise ValueError(f"search.{key} must be an integer in [1,{limit}]")
    return deepcopy(value)


def derived_record(parent, plan):
    """Only model input and provenance; no actual/hit/coverage/effect fields."""
    record = {key: deepcopy(parent[key]) for key in
              ("target_id", "target_revision", "rmw", "contract_hash", "seed_id")}
    record.update(final_config=deepcopy(plan.final_config), mutation_plan=plan.to_dict(),
                  operations=[op.to_dict() for op in plan.operations],
                  runtime_windows=deepcopy(plan.runtime_windows),
                  parent_content_sha256=parent["content_sha256"], executable=True)
    inherit_routes(parent, record)
    record = canonical_route_record(record)
    record["content_sha256"] = content_identity(record)
    return record


class InputSearchFrontier:
    def __init__(self, roots, settings, *, rng_seed, sampling_kinds, sampling_weight):
        self.settings = search_settings(settings)
        self.rng_seed = rng_seed
        if type(rng_seed) is not int or rng_seed < 0:
            raise ValueError("invalid input search RNG seed")
        if not sampling_kinds:
            raise ValueError("input_search_bfs requires explicit sampling_kinds")
        self.sampling_kinds = deepcopy(sampling_kinds)
        self.sampling_weight = deepcopy(sampling_weight)
        self.roots = sorted(deepcopy(roots), key=lambda r: r["content_sha256"])
        if not self.roots or len(self.roots) > self.settings["max_nodes"]:
            raise ValueError("input search roots exceed max_nodes or are empty")
        self.nodes, self.tasks, self.journal = {}, [], []
        for root in self.roots:
            if not has_routes(root):
                raise ValueError("input_search_bfs currently requires declared F1 mutation routes")
            validate_route_entry(root)
            digest = root["content_sha256"]
            if digest != content_identity(root) or digest in self.nodes:
                raise ValueError("invalid or duplicate input search root identity")
            self._node(root, digest, 0)

    def _node(self, entry, baseline, depth):
        digest = entry["content_sha256"]
        self.nodes[digest] = {"entry": deepcopy(entry), "baseline_content_sha256": baseline,
                              "derivation_depth": depth, "expansion_status": "pending"}
        if depth >= self.settings["max_depth"]:
            self.nodes[digest]["expansion_status"] = "depth_bound"
            return
        # Reuse the catalog-supported kind predicate, never probe a live SUT.
        from .scheduler import ExploratoryScheduler
        tasks, applicability = [], []
        for route in validate_route_entry(entry):
            view = deepcopy(entry)
            view.update(route)
            view["mutation_route"] = deepcopy(route)
            supported = ExploratoryScheduler._supported_sampling_kinds(view)
            for kind in sorted(self.sampling_kinds):
                reason = ("kind_not_supported_by_route" if kind not in supported else
                          "zero_sampling_weight" if self.sampling_weight.get(kind, 1) <= 0 else None)
                applicability.append({"mutation_route": deepcopy(route), "sampling_kind": kind,
                                      "applicable": reason is None, "reason": reason})
                if reason is not None:
                    continue
                for slot in range(self.settings["samples_per_class"]):
                    key = {"baseline_content_sha256": baseline,
                           "parent_content_sha256": digest, "mutation_route": deepcopy(route),
                           "sampling_kind": kind, "sample_slot": slot,
                           "derivation_depth": depth + 1}
                    task_id = canonical_hash(key)
                    rng = int(canonical_hash({"mode": "input_search_bfs", "rng_seed": self.rng_seed,
                                               "task_id": task_id})[:16], 16)
                    tasks.append({**key, "task_id": task_id, "rng_seed": rng,
                                  "status": "pending", "execution": None, "actual": False})
        self.nodes[digest]["sampling_applicability"] = applicability
        if len(self.tasks) + len(tasks) > self.settings["max_tasks"]:
            self.nodes[digest]["expansion_status"] = "task_budget_blocked"
            self.nodes[digest]["required_task_count"] = len(tasks)
            return
        self.tasks.extend(tasks)
        self.nodes[digest]["expansion_status"] = "expanded"

    @property
    def stop_reason(self):
        if any(t["status"] in {"selected", "planned", "submitted"} for t in self.tasks):
            return "input_search_pending_attempt"
        if any(n["expansion_status"] == "task_budget_blocked" for n in self.nodes.values()):
            return "input_search_max_tasks_reached_work_retained"
        if len(self.nodes) >= self.settings["max_nodes"]:
            return "input_search_max_nodes_reached_work_retained"
        if not any(t["status"] == "pending" for t in self.tasks):
            return "input_search_depth_or_tasks_exhausted"
        return None

    def task(self, task_id):
        for task in self.tasks:
            if task["task_id"] == task_id:
                return task
        raise ValueError("unknown input search task")

    def _log(self, action, **data):
        self.journal.append({"sequence": len(self.journal), "action": action, **deepcopy(data)})

    def select(self):
        if self.stop_reason is not None:
            raise ValueError(self.stop_reason)
        pending = [t for t in self.tasks if t["status"] == "pending"]
        task = min(enumerate(pending), key=lambda it: (it[1]["derivation_depth"], it[0]))[1]
        task["status"] = "selected"
        self._log("select", task_id=task["task_id"])
        return deepcopy(task)

    def entry(self, task):
        return route_view(self.nodes[task["parent_content_sha256"]]["entry"], task["mutation_route"])

    def planned(self, task_id, record):
        task = self.task(task_id)
        if task["status"] == "planned":
            if task.get("generated_record") != record:
                raise ValueError("resumed input search generation differs")
            return
        if task["status"] != "selected":
            raise ValueError("input search plan without selected task")
        parent = self.entry(task)
        if (record.get("parent_content_sha256") != parent["content_sha256"] or
                record.get("content_sha256") != content_identity(record)):
            raise ValueError("input search generated identity/parent mismatch")
        digest = record["content_sha256"]
        operations = record.get("operations", [])
        compile_noop = bool(operations) and all(
            op.get("application_phase") == "compile_time" and
            op.get("input_before_sha256") == op.get("input_after_sha256")
            for op in operations)
        no_new_windows = record.get("runtime_windows", []) == parent.get("runtime_windows", [])
        if compile_noop and no_new_windows:
            outcome = "compile_time_no_change"
        elif record.get("mutation_plan") is not None and self._noop_plan(record["mutation_plan"]):
            outcome = "no_effective_input"
        elif digest in self.nodes:
            outcome = "duplicate_content"
        elif len(self.nodes) < self.settings["max_nodes"]:
            outcome = "new_input"
            self._node(record, task["baseline_content_sha256"], task["derivation_depth"])
        else:
            # Never evict or discard the unexpanded input on capacity pressure.
            raise ValueError("input search capacity invariant violated")
        task.update(status="planned", generated_record=deepcopy(record), generation_outcome=outcome)
        self._log("plan", task_id=task_id, record=record)

    @staticmethod
    def _noop_plan(data):
        from r2f.core.mutation_plan import MutationPlan, mutation_plan_noop_reason
        return mutation_plan_noop_reason(MutationPlan.from_dict(data)) is not None

    def submitted(self, task_id):
        task = self.task(task_id)
        if task["status"] != "planned":
            raise ValueError("input search submission without plan")
        outcome = task.get("generation_outcome")
        if outcome in {"compile_time_no_change", "duplicate_content", "no_effective_input"}:
            raise ValueError("input search no-change/duplicate content cannot be submitted")
        task["status"] = "submitted"
        self._log("submit", task_id=task_id)

    def finish(self, task_id, *, rejected=False, reason=None, actual=False, execution=None):
        task = self.task(task_id)
        if task["status"] not in {"selected", "planned", "submitted"}:
            raise ValueError("input search terminal transition without active task")
        if type(actual) is not bool or (not actual and execution is not None):
            raise ValueError("input search execution claims require actual execution")
        if task["status"] == "selected" and not rejected:
            raise ValueError("unplanned input search task must be rejected")
        if (task.get("generation_outcome") in {"compile_time_no_change", "duplicate_content", "no_effective_input"}
                and (not rejected or actual or execution is not None)):
            raise ValueError("input search no-change/duplicate content must be rejected without execution")
        task.update(status="rejected" if rejected else "completed", reason=reason,
                    actual=actual, execution=deepcopy(execution))
        self._log("finish", task_id=task_id, rejected=rejected, reason=reason,
                  actual=actual, execution=execution)

    def to_dict(self):
        return deepcopy({"schema": SCHEMA, "mode": "input_search_bfs", "settings": self.settings,
                         "rng_seed": self.rng_seed, "sampling_kinds": self.sampling_kinds,
                         "sampling_weight": self.sampling_weight, "roots": self.roots,
                         "nodes": self.nodes, "tasks": self.tasks, "journal": self.journal,
                         "stop_reason": self.stop_reason})

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict) or data.get("schema") != SCHEMA:
            raise ValueError("unsupported input search frontier schema")
        rebuilt = cls.prefix(data, len(data["journal"]))
        if rebuilt.to_dict() != data:
            raise ValueError("input search frontier state mismatch")
        return rebuilt

    @classmethod
    def prefix(cls, data, count):
        """Reconstruct a historical journal prefix, not a second snapshot store."""
        if type(count) is not int or not 0 <= count <= len(data["journal"]):
            raise ValueError("invalid input search journal prefix")
        rebuilt = cls(data["roots"], data["settings"], rng_seed=data["rng_seed"],
                      sampling_kinds=data["sampling_kinds"], sampling_weight=data["sampling_weight"])
        for event in data["journal"][:count]:
            rebuilt.apply_event(event)
        return rebuilt

    def apply_event(self, event):
        """Apply one authenticated journal item in order, without skipping work."""
        if event["action"] == "select":
            if self.select()["task_id"] != event["task_id"]:
                raise ValueError("input search historical BFS order mismatch")
        elif event["action"] == "plan":
            self.planned(event["task_id"], event["record"])
        elif event["action"] == "submit":
            self.submitted(event["task_id"])
        elif event["action"] == "finish":
            self.finish(event["task_id"], **{k: event[k] for k in
                        ("rejected", "reason", "actual", "execution")})
        else:
            raise ValueError("unknown input search journal action")
        if self.journal[-1] != event:
            raise ValueError("input search historical journal mismatch")

    def validate_model(self, *, profile, seeds, registry):
        """Rebuild every child using the existing runner's pure generation path."""
        if self.roots != sorted(deepcopy(list(seeds.entries.values())), key=lambda r: r["content_sha256"]):
            raise ValueError("input search roots differ from profile seeds")
        if search_settings(profile.get("search")) != self.settings:
            raise ValueError("input search profile bounds differ")
        from .runner import _build_case_plan, _profile_wide_settings, _NoEffectiveInputRejection
        kinds, weights, _ = _profile_wide_settings(profile)
        if kinds != self.sampling_kinds or weights != self.sampling_weight:
            raise ValueError("input search profile sampling differs")
        for event in self.journal:
            if event["action"] != "plan":
                continue
            task = self.task(event["task_id"])
            parent = self.entry(task)
            target = registry.get_target(parent["target_id"])
            contract = target.exploration_contract
            if (parent["target_revision"] != target.target_revision or
                    parent["contract_hash"] != contract.contract_hash() or
                    parent["rmw"] not in profile["rmws"]):
                raise ValueError("input search target/contract/RMW mismatch")
            validate_route_entry(parent, contract=contract, profile=profile)
            route = task["mutation_route"]
            surface = contract.surface_by_id[route["surface_id"]]
            try:
                plan = _build_case_plan(parent, target, contract, surface, parent["final_config"],
                                        route["mutator_id"], profile, task["rng_seed"], task)["plan"]
            except _NoEffectiveInputRejection as exc:
                plan = exc.plan  # Regenerate rejected proposal audit, never a node.
            if derived_record(parent, plan) != event["record"]:
                raise ValueError("input search child is not model-generatable from parent/slot")
