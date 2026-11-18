"""Bounded *input* search, not a SUT state/action planner or evidence corpus.

Tasks enumerate declared routes x supported sampling kinds x finite RNG slots.
A slot does not enumerate an exact boundary value or exhaust a continuous range.
The append-only journal can be deterministically replayed; checkpoint additionally
regenerates each derived plan against the authoritative target/profile contract.
"""
from copy import deepcopy
from contextlib import contextmanager
from contextvars import ContextVar
import json

from r2f.core.evidence import canonical_hash
from .corpus import content_identity
from .mutation_routes import (canonical_route_record, has_routes, inherit_routes,
                              route_view, validate_route_entry)

# Ephemeral deterministic work only: never retained across checkpoint validation,
# source/profile identities, hooks, processes, or exceptions. Values are private
# detached copies; callers cannot mutate a cached frontier through a return value.
_VALIDATION_MEMO = ContextVar("r2f_frontier_validation_memo", default=None)


@contextmanager
def _frontier_validation_scope():
    token = _VALIDATION_MEMO.set({"frontier": {}, "noop": {}, "node": {}})
    try:
        yield
    finally:
        _VALIDATION_MEMO.reset(token)


def _memo_key(value, *, allow_alias=False):
    # The complete values/types, never supplied content/selection hashes. The
    # ordinary memo declines non-tree callers. The post-model private handoff
    # additionally binds alias topology, without normalizing it into a tree.
    pending, seen, aliases = [((), value)], {}, []
    while pending:
        path, item = pending.pop()
        kind = type(item)
        if kind in (dict, list):
            if id(item) in seen:
                if not allow_alias:
                    return None
                aliases.append((path, seen[id(item)]))
                continue
            seen[id(item)] = path
        if kind is dict:
            if any(type(k) is not str for k in item):
                return None
            pending.extend((path + (k,), item[k]) for k in sorted(item, reverse=True))
        elif kind is list:
            pending.extend((path + (i,), item[i]) for i in range(len(item) - 1, -1, -1))
        elif item is not None and kind not in (str, int, float, bool):
            return None
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError):
        return None
    return (encoded, tuple(aliases)) if allow_alias else encoded


def _remember_model_validated_frontier(data, frontier):
    """Private handoff AFTER the checkpoint's complete model regeneration.

    Hook-produced task slots can share request dicts. Leave arbitrary aliased
    callers on the original reconstruction path; only this exact already-
    validated input object is eligible for the scheduler's second restore.
    The key is recomputed on consumption, including types and alias relations.
    """
    memo = _VALIDATION_MEMO.get()
    if memo is None:
        return
    key = _memo_key(data, allow_alias=True)
    if key is not None and key[1]:
        memo["model_validated"] = (data, key, deepcopy(frontier))


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
    if "topology_program" in plan.final_config:
        from r2f.modules.s1.topology_model import validate_program
        validation = validate_program(plan.final_config["topology_program"])
        guard = validation["status"] != "valid" or validation["guard_disposition"] == "guard_violation"
        guard = guard or any(op.applicability_result.get("expected_disposition") == "expect_guard_reject" for op in plan.operations)
        for key in ("surface_id", "mutator_id", "profile_id", "cycle_id", "runner_slice"):
            if key in parent:
                record[key] = deepcopy(parent[key])
        record.update(executable=not guard, case_kind="guard_negative" if guard else "derived_topology",
                      candidate_eligible=False, planned_only=True)
    inherit_routes(parent, record)
    record = canonical_route_record(record)
    record["content_sha256"] = content_identity(record)
    return record


class InputSearchFrontier:
    def __init__(self, roots, settings, *, rng_seed, sampling_kinds, sampling_weight, topology_descriptor=None):
        self.topology_descriptor = deepcopy(topology_descriptor)
        if topology_descriptor is not None:
            from .topology_search import validate_descriptor
            validate_descriptor(topology_descriptor)
        self.settings = search_settings(settings)
        self.rng_seed = rng_seed
        if type(rng_seed) is not int or rng_seed < 0:
            raise ValueError("invalid input search RNG seed")
        if self.topology_descriptor is None and not sampling_kinds:
            raise ValueError("input_search_bfs requires explicit sampling_kinds")
        if self.topology_descriptor is not None and (sampling_kinds is not None or sampling_weight is not None):
            raise ValueError("topology tasks cannot borrow F1 sampling namespace")
        self.sampling_kinds = deepcopy(sampling_kinds)
        self.sampling_weight = deepcopy(sampling_weight)
        self.roots = sorted(deepcopy(roots), key=lambda r: r["content_sha256"])
        if not self.roots or len(self.roots) > self.settings["max_nodes"]:
            raise ValueError("input search roots exceed max_nodes or are empty")
        self.nodes, self.tasks, self.journal = {}, [], []
        for root in self.roots:
            if self.topology_descriptor is None and not has_routes(root):
                raise ValueError("input_search_bfs currently requires declared F1 mutation routes")
            if self.topology_descriptor is None:
                validate_route_entry(root)
            digest = root["content_sha256"]
            if digest != content_identity(root) or digest in self.nodes:
                raise ValueError("invalid or duplicate input search root identity")
            self._node(root, digest, 0)

    def _node(self, entry, baseline, depth):
        memo = _VALIDATION_MEMO.get()
        # _build_node reads only these inputs and the current task count. In
        # particular it does not consult earlier nodes/tasks, observations or
        # caller-provided identity labels to decide the expansion. Keep the
        # count in the key: max_tasks must retain exactly the original work.
        key = (_memo_key([entry, baseline, depth, self.settings, self.rng_seed,
                          self.sampling_kinds, self.sampling_weight,
                          self.topology_descriptor, len(self.tasks)])
               if memo is not None else None)
        cache_key = (type(self), key)
        if key is not None and cache_key in memo["node"]:
            node, tasks = deepcopy(memo["node"][cache_key])
            self.nodes[entry["content_sha256"]] = node
            self.tasks.extend(tasks)
            return
        before = len(self.tasks)
        self._build_node(entry, baseline, depth)
        if key is not None:
            memo["node"][cache_key] = deepcopy(
                (self.nodes[entry["content_sha256"]], self.tasks[before:]))

    def _build_node(self, entry, baseline, depth):
        digest = entry["content_sha256"]
        self.nodes[digest] = {"entry": deepcopy(entry), "baseline_content_sha256": baseline,
                              "derivation_depth": depth, "expansion_status": "pending"}
        if depth >= self.settings["max_depth"]:
            self.nodes[digest]["expansion_status"] = "depth_bound"
            return
        if self.topology_descriptor is not None:
            from .topology_search import enumerate_tasks
            prototypes, ledger = enumerate_tasks(entry, self.topology_descriptor)
            tasks = []
            for proto in prototypes:
                for sample_slot in range(self.settings["samples_per_class"]):
                    key = dict(proto, baseline_content_sha256=baseline, parent_content_sha256=digest,
                               sample_slot=sample_slot, derivation_depth=depth + 1)
                    task_id = canonical_hash(key)
                    rng = int(canonical_hash({"mode": "input_search_bfs", "rng_seed": self.rng_seed,
                                              "task_id": task_id})[:16], 16)
                    tasks.append(dict(key, task_id=task_id, rng_seed=rng, status="pending", execution=None, actual=False))
            self.nodes[digest]["sampling_applicability"] = ledger
            from .topology_search import is_chain, sequence_applicability
            if is_chain(self.topology_descriptor):
                self.nodes[digest]["sequence_applicability"] = sequence_applicability(self.topology_descriptor)
            if len(self.tasks) + len(tasks) > self.settings["max_tasks"]:
                self.nodes[digest].update(expansion_status="task_budget_blocked", required_task_count=len(tasks))
                return
            self.tasks.extend(tasks)
            self.nodes[digest]["expansion_status"] = "expanded"
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
        entry = self.nodes[task["parent_content_sha256"]]["entry"]
        if self.topology_descriptor is not None:
            return deepcopy(entry)
        return route_view(entry, task["mutation_route"])

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
        if record.get("executable") is False:
            outcome = "guard_terminal"
        elif compile_noop and no_new_windows:
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
        if task.get("chain_schema"):
            from .topology_search import plan_accounting
            from r2f.core.mutation_plan import MutationPlan
            task["chain_accounting"] = plan_accounting(MutationPlan.from_dict(record["mutation_plan"]), task["declared_layer_count"])
        task.update(status="planned", generated_record=deepcopy(record), generation_outcome=outcome)
        self._log("plan", task_id=task_id, record=record)

    @staticmethod
    def _noop_plan(data):
        from r2f.core.mutation_plan import MutationPlan, mutation_plan_noop_reason
        memo = _VALIDATION_MEMO.get()
        key = _memo_key(data) if memo is not None else None
        if key is not None and key in memo["noop"]:
            return memo["noop"][key]
        result = mutation_plan_noop_reason(MutationPlan.from_dict(data)) is not None
        if key is not None:
            memo["noop"][key] = result
        return result

    def submitted(self, task_id):
        task = self.task(task_id)
        if task["status"] != "planned":
            raise ValueError("input search submission without plan")
        outcome = task.get("generation_outcome")
        if outcome in {"compile_time_no_change", "duplicate_content", "no_effective_input", "guard_terminal", "generation_rejected"}:
            raise ValueError("input search no-change/duplicate content cannot be submitted")
        task["status"] = "submitted"
        self._log("submit", task_id=task_id)

    def finish(self, task_id, *, rejected=False, reason=None, actual=False, execution=None):
        task = self.task(task_id)
        if task["status"] not in {"selected", "planned", "submitted"}:
            raise ValueError("input search terminal transition without active task")
        if type(actual) is not bool or (not actual and execution is not None):
            raise ValueError("input search execution claims require actual execution")
        if self.topology_descriptor is not None and task["status"] == "selected":
            if task.get("generation_outcome") != "generation_rejected" or reason != "topology_input_rejected:" + task["rejection"]["reason"]:
                raise ValueError("topology selected rejection requires model-backed reason")
        if task["status"] == "selected" and not rejected:
            raise ValueError("unplanned input search task must be rejected")
        if (task.get("generation_outcome") in {"compile_time_no_change", "duplicate_content", "no_effective_input", "guard_terminal", "generation_rejected"}
                and (not rejected or actual or execution is not None)):
            raise ValueError("input search no-change/duplicate content must be rejected without execution")
        task.update(status="rejected" if rejected else "completed", reason=reason,
                    actual=actual, execution=deepcopy(execution))
        self._log("finish", task_id=task_id, rejected=rejected, reason=reason,
                  actual=actual, execution=execution)

    def generation_rejected(self, task_id, reason, attempts):
        task = self.task(task_id)
        if self.topology_descriptor is None or task["status"] != "selected" or "generation_outcome" in task:
            raise ValueError("topology rejection requires selected task")
        no_change = "no content change" in reason or "equals the parent" in reason or "no distinct" in reason
        task.update(generation_outcome="generation_rejected",
                    task_applicability={"status": "not_applicable", "reason": "generation_no_change" if no_change else "handler_constraint", "detail": reason},
                    rejection={"reason": reason, "attempts": deepcopy(attempts)})
        if task.get("chain_schema"):
            from .topology_search import rejection_accounting
            task["chain_accounting"] = rejection_accounting(self.entry(task), task)
        self._log("generation_rejected", task_id=task_id, reason=reason, attempts=attempts)

    def to_dict(self):
        extra = ({"topology_descriptor": self.topology_descriptor} if self.topology_descriptor is not None else {})
        from .topology_search import is_chain, CHAINS_FRONTIER_SCHEMA
        schema = CHAINS_FRONTIER_SCHEMA if is_chain(self.topology_descriptor) else ("r2f.input-search-frontier/topology-v1" if extra else SCHEMA)
        return deepcopy({**extra, "schema": schema, "mode": "input_search_bfs", "settings": self.settings,
                         "rng_seed": self.rng_seed, "sampling_kinds": self.sampling_kinds,
                         "sampling_weight": self.sampling_weight, "roots": self.roots,
                         "nodes": self.nodes, "tasks": self.tasks, "journal": self.journal,
                         "stop_reason": self.stop_reason})

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict) or data.get("schema") not in (SCHEMA, "r2f.input-search-frontier/topology-v1", "r2f.input-search-frontier/topology-chains-v1"):
            raise ValueError("unsupported input search frontier schema")
        if (data["schema"] == SCHEMA) == ("topology_descriptor" in data):
            raise ValueError("input search schema cannot migrate topology descriptor")
        memo = _VALIDATION_MEMO.get()
        verified = memo.get("model_validated") if memo is not None else None
        if (verified is not None and cls is type(verified[2]) and data is verified[0] and
                _memo_key(data, allow_alias=True) == verified[1]):
            return deepcopy(verified[2])
        key = _memo_key(data) if memo is not None else None
        cache_key = (cls, key)
        if key is not None and cache_key in memo["frontier"]:
            return deepcopy(memo["frontier"][cache_key])
        rebuilt = cls.prefix(data, len(data["journal"]))
        if rebuilt.to_dict() != data:
            raise ValueError("input search frontier state mismatch")
        if key is not None:
            memo["frontier"][cache_key] = deepcopy(rebuilt)
        return rebuilt

    @classmethod
    def prefix(cls, data, count):
        """Reconstruct a historical journal prefix, not a second snapshot store."""
        if type(count) is not int or not 0 <= count <= len(data["journal"]):
            raise ValueError("invalid input search journal prefix")
        rebuilt = cls(data["roots"], data["settings"], rng_seed=data["rng_seed"],
                      sampling_kinds=data["sampling_kinds"], sampling_weight=data["sampling_weight"],
                      topology_descriptor=data.get("topology_descriptor"))
        for event in data["journal"][:count]:
            rebuilt.apply_event(event)
        return rebuilt

    def apply_event(self, event):
        """Apply one authenticated journal item in order, without skipping work."""
        if event["action"] == "select":
            if self.select()["task_id"] != event["task_id"]:
                raise ValueError("input search historical BFS order mismatch")
        elif event["action"] == "generation_rejected":
            self.generation_rejected(event["task_id"], event["reason"], event["attempts"])
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
        if self.roots != sorted(deepcopy(seeds.selectable()), key=lambda r: r["content_sha256"]):
            raise ValueError("input search roots differ from profile seeds")
        if search_settings(profile.get("search")) != self.settings:
            raise ValueError("input search profile bounds differ")
        from .runner import _build_case_plan, _profile_wide_settings, _NoEffectiveInputRejection
        kinds, weights, _ = _profile_wide_settings(profile)
        if kinds != self.sampling_kinds or weights != self.sampling_weight:
            raise ValueError("input search profile sampling differs")
        if self.topology_descriptor is not None:
            from .topology_search import descriptor, validate_chain_length
            expected = descriptor(profile.get("topology_search"), profile.get("operator_whitelist", []))
            validate_chain_length(profile, expected)
            if expected != self.topology_descriptor:
                raise ValueError("topology profile descriptor differs")
        elif "topology_search" in profile:
            raise ValueError("legacy frontier cannot restore topology search")
        for event in self.journal:
            if event["action"] not in {"plan", "generation_rejected"}:
                continue
            task = self.task(event["task_id"])
            parent = self.entry(task)
            target = registry.get_target(parent["target_id"])
            contract = target.exploration_contract
            if (parent["target_revision"] != target.target_revision or
                    parent["contract_hash"] != contract.contract_hash() or
                    parent["rmw"] not in profile["rmws"]):
                raise ValueError("input search target/contract/RMW mismatch")
            if self.topology_descriptor is not None:
                from .topology_search import generate, task_requests
                try:
                    regenerated = generate(parent, task, self.topology_descriptor)
                except ValueError as exc:
                    if event["action"] != "generation_rejected" or event["reason"] != str(exc) or event["attempts"] != task_requests(task):
                        raise ValueError("topology rejection not model-generatable") from exc
                    if task.get("chain_schema") and task.get("chain_accounting") != getattr(exc, "accounting", None):
                        raise ValueError("topology chain rejection accounting not model-generatable")
                else:
                    if event["action"] != "plan" or derived_record(parent, regenerated) != event["record"]:
                        raise ValueError("topology child not model-generatable")
                continue
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
