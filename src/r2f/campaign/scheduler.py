"""Required matrix plus budget-driven exploratory selection on one scheduler.

Required routing is unchanged and deterministic.  Exploratory routing uses a
fixed 20/40/40 cycle: uniform over all executable seeds, uncovered
surface x boundary classes, then rare observed edges.  Empty pools fall back in
a fixed order.  Selection is a pure function of the feedback log, RNG seed and
corpus snapshot; it never executes anything.  Explicit ``policy="fair"`` instead
traverses declared input classes using only selection history, not runtime
feedback. Explicit ``policy="bfs"`` instead traverses a bounded, model-generated
input worklist independently of novelty retention. None is a SUT state/action
planner or execution evidence.
"""
from fractions import Fraction
from itertools import product
import random
import re
from copy import deepcopy

from r2f.core.evidence import canonical_hash
from r2f.modules.s1.targets import RMWS, EXECUTION_ORDER
from .corpus import Corpus, content_sha256
from .mutation_routes import has_routes, route_view, validate_route_entry
from .feedback import FeedbackLog, edge_id, feedback_identity, surface_class


def _restore_checkpoint_corpus(data, *, model_parents=None):
    """Reuse exact restores only within one complete checkpoint validation.

    Full values (including parents and alias topology), not supplied hashes,
    qualify a hit. First use always performs Corpus.from_dict; no result or
    mutation of a returned object can affect the private detached snapshot.
    Outside the checkpoint's context this is the original cold path.
    """
    from .search_frontier import _VALIDATION_MEMO, _memo_key
    scope = _VALIDATION_MEMO.get()
    if scope is None:
        return Corpus.from_dict(data, model_parents=model_parents)
    key = _memo_key([data, model_parents], allow_alias=True)
    if key is None:
        return Corpus.from_dict(data, model_parents=model_parents)
    memo = scope.setdefault("corpus_restore", {})
    if key in memo:
        return deepcopy(memo[key])
    restored = Corpus.from_dict(data, model_parents=model_parents)
    memo[key] = deepcopy(restored)
    return restored


def first_slice_cells(target_id=EXECUTION_ORDER[0]):
    rows = []
    for rmw, arm, repeat in product(RMWS, ("baseline", "transparent", "mutated"), range(3)):
        rows.append({"cell_id": f"{rmw}.{arm}.{repeat}", "target_id": target_id,
                     "rmw": rmw, "arm": arm, "repeat": repeat, "campaign_kind": "required", "control_kind": None})
    for rmw, repeat in product(RMWS, range(3)):
        rows.append({"cell_id": f"{rmw}.negative.{repeat}", "target_id": target_id,
                     "rmw": rmw, "arm": "negative", "repeat": repeat, "campaign_kind": "required", "control_kind": "fault", "fault_id": "post_restore_delivery_omission" if target_id == EXECUTION_ORDER[2] else "controlled_projection_field_mismatch" if target_id == EXECUTION_ORDER[3] else "late_endpoint_delivery_omission"})
    return rows


def control_references(rows):
    return {kind: [row["cell_id"] for row in rows if row["arm"] == arm]
            for kind, arm in (("health", "baseline"), ("mutation", "mutated"), ("transparency", "transparent"), ("fault", "negative"))}


def required_catalog_hash():
    """Hash of the unchanged required catalog; exploratory imports never alter it."""
    from r2f.modules.s1.module import S1Module
    return canonical_hash(S1Module().to_dict())


def validated_feedback(events):
    """Validate the full ordered event bytes, not only the coverage summary."""
    if not isinstance(events, list):
        raise ValueError("feedback events must be a list")
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("feedback event must be a dict")
        for field in ("event_sha256", "content_sha256"):
            if not isinstance(event.get(field), str) or not re.fullmatch(r"[0-9a-f]{64}", event[field]):
                raise ValueError("invalid feedback " + field)
        if event["event_sha256"] != canonical_hash({k: v for k, v in event.items() if k != "event_sha256"}):
            raise ValueError("feedback event hash mismatch")
        if any(type(event.get(k)) is not bool for k in ("executed", "coverage", "complete", "tool_error")):
            raise ValueError("invalid feedback execution/coverage flags")
        if event["coverage"] != event["complete"] or (event["complete"] and (event["tool_error"] or not event["executed"])):
            raise ValueError("feedback coverage requires complete executed non-tool observation")
        if not event["complete"] and event.get("edge_id") is not None:
            raise ValueError("incomplete feedback cannot claim an edge")
        surface = event.get("surface_class")
        if surface is not None and (not isinstance(surface, list) or len(surface) not in (2, 3) or
                any(not isinstance(v, str) or not v for v in surface)):
            raise ValueError("invalid feedback surface class")
        if event.get("edge_id") is not None and (not isinstance(event["edge_id"], str) or not event["edge_id"]):
            raise ValueError("invalid feedback edge")
    return FeedbackLog.from_events(events)


class ExploratoryScheduler:
    """Pure selection over one corpus snapshot and one ordered feedback log.

    ``sampling_kinds``/``sampling_weight`` are an additive opt-in layer.  When
    neither is declared every selection event keeps the historical field set
    and bytes; when declared, one supported kind is chosen after the corpus
    entry and recorded as ``sampling_kind``.  The kind choice is deterministic
    under the scheduler RNG seed and prioritises kinds with fewer selections in
    this cycle (the same deficit-first shape as the existing uncovered pool).
    """

    WEIGHTS = (("uniform", 20), ("uncovered", 40), ("rare_edge", 40))
    FALLBACK = ("uniform", "uncovered", "rare_edge")
    POLICIES = ("feedback", "fair", "bfs")
    FAIR_DIMENSIONS = ("target_id", "surface_id", "boundary_class", "variant_id",
                       "chain_depth", "sampling_kind")
    _surface_cache = None

    def __init__(self, corpus, feedback=None, *, rng_seed=0, warmup=10,
                 sampling_kinds=None, sampling_weight=None, policy=None, search=None):
        if not isinstance(corpus, Corpus):
            raise ValueError("exploratory scheduler requires a Corpus")
        if type(rng_seed) is not int or rng_seed < 0:
            raise ValueError("rng_seed must be a nonnegative integer")
        if type(warmup) is not int or warmup < 0:
            raise ValueError("warmup must be a nonnegative integer")
        if policy is not None and (not isinstance(policy, str) or policy not in self.POLICIES):
            raise ValueError("unknown scheduler policy")
        # None is the byte-compatible historical path, not an explicit policy
        # marker. Keep the distinction even though both use feedback internally.
        self._policy = "feedback" if policy is None else policy
        self._explicit_policy = policy is not None
        self.corpus = corpus
        source_log = feedback if isinstance(feedback, FeedbackLog) else FeedbackLog.from_events(feedback or [])
        # Freeze until explicit advance_feedback; external append cannot alter
        # an already accepted prefix or historical selection identity.
        self.feedback_event_count = len(source_log.events)
        self.feedback_identity_at_start = source_log.identity()
        self.feedback = validated_feedback(source_log.events)
        if self.feedback.identity() != self.feedback_identity_at_start:
            raise ValueError("feedback snapshot identity drift")
        self.rng_seed = rng_seed
        self.warmup = warmup
        self.selection_log = []
        self.feedback_epochs = [self._epoch()]
        self.selection_contexts = []
        self.sampling_kinds = None
        self.sampling_weight = None
        self.frontier = None
        if sampling_kinds is not None or sampling_weight is not None:
            self.configure_sampling(sampling_kinds, sampling_weight)

        if search is not None:
            self.configure_search(search)

    def configure_search(self, search, topology_descriptor=None):
        from .search_frontier import InputSearchFrontier, search_settings
        if self.policy != "bfs":
            raise ValueError("search settings require explicit bfs policy")
        settings = search_settings(search)
        if self.frontier is not None:
            if topology_descriptor != self.frontier.topology_descriptor:
                raise ValueError("input search descriptor cannot drift")
            if settings != self.frontier.settings:
                raise ValueError("input search bounds cannot change after initialization")
            if (self.sampling_kinds != self.frontier.sampling_kinds or
                    self.sampling_weight != self.frontier.sampling_weight):
                raise ValueError("input search sampling drift")
            return
        if self.selection_log:
            raise ValueError("cannot initialize input search after selection")
        self.frontier = InputSearchFrontier(self.corpus.selectable(), settings,
            rng_seed=self.rng_seed, sampling_kinds=self.sampling_kinds,
            sampling_weight=self.sampling_weight, topology_descriptor=topology_descriptor)

    def _next_bfs(self):
        if self.frontier is None:
            raise ValueError("bfs requires explicit search bounds")
        frontier_journal_count = len(self.frontier.journal)
        task = self.frontier.select()
        chosen = self.frontier.entry(task)
        event = {key: deepcopy(value) for key, value in task.items()
                 if key not in {"status", "execution", "actual"}}
        event.update(sequence=len(self.selection_log), policy="bfs", mode="input_search_bfs",
                     content_sha256=task["parent_content_sha256"], seed_id=chosen.get("seed_id"),
                     phase="input_search", pool=("explicit_topology_requests" if task.get("task_kind") == "topology_request" else "declared_route_sample_slots"),
                     **({} if task.get("task_kind") == "topology_request" else {"route_selection": {
                         "schema": chosen["mutation_routes_schema"], "policy": "input_search_bfs",
                         "selected": deepcopy(task["mutation_route"])}}))
        event["selection_sha256"] = canonical_hash(event)
        self.selection_log.append(event)
        context = {"sequence": event["sequence"], "policy": "bfs",
                   "feedback_epoch": len(self.feedback_epochs) - 1, **self._epoch(),
                   "selection_sha256": event["selection_sha256"],
                   "corpus": self.corpus.to_dict(), "frontier_journal_count": frontier_journal_count,
                   "sampling": ({"kinds": list(self.sampling_kinds), "weight": dict(self.sampling_weight)}
                                if self.sampling_kinds is not None else None)}
        context["context_sha256"] = canonical_hash(context)
        self.selection_contexts.append(context)
        return dict(deepcopy(event), entry=chosen)

    @property
    def policy(self):
        """Canonical strategy name; omitted policy still serializes as legacy."""
        return self._policy

    def _epoch(self):
        return {"feedback_event_count": len(self.feedback.events),
                "feedback_events_sha256": canonical_hash(self.feedback.events),
                "feedback_identity": self.feedback.identity()}

    def advance_feedback(self, feedback):
        """Accept only an unchanged prefix plus append; never observe implicitly.

        Epochs and per-selection corpus snapshots are immutable audit context.
        A caller appending to the shared log affects next() only after this call.
        """
        if not isinstance(feedback, FeedbackLog):
            raise ValueError("advance_feedback requires a FeedbackLog")
        if self._epoch() != self.feedback_epochs[-1]:
            raise ValueError("accepted feedback epoch was modified")
        current = validated_feedback(feedback.events)
        count = len(self.feedback.events)
        if len(current.events) < count or current.events[:count] != self.feedback.events:
            raise ValueError("feedback must be append-only; prefix drift/truncation")
        if len(current.events) != count:
            self.feedback = current
            self.feedback_epochs.append(self._epoch())
        self.feedback_event_count = len(self.feedback.events)
        self.feedback_identity_at_start = self.feedback.identity()
        return deepcopy(self.feedback_epochs[-1])

    def configure_sampling(self, sampling_kinds, sampling_weight=None):
        """Validate and install the opt-in sampling namespace.

        No selection is made here.  ``sampling_kinds`` must name declared wide
        classes; weights are nonnegative integers with at least one positive
        entry.  A surface-specific subset that has no supported kind is reported
        by ``next()`` and rejected by the runner, never silently replaced.
        """
        from r2f.core.mutators import WIDE_SAMPLING_KINDS
        if not isinstance(sampling_kinds, list) or not sampling_kinds:
            raise ValueError("sampling_kinds must be a nonempty list")
        if any(not isinstance(kind, str) or not kind for kind in sampling_kinds):
            raise ValueError("sampling_kinds must contain nonempty strings")
        if len(set(sampling_kinds)) != len(sampling_kinds):
            raise ValueError("sampling_kinds contains duplicates")
        unknown = sorted(set(sampling_kinds) - set(WIDE_SAMPLING_KINDS))
        if unknown:
            raise ValueError("unsupported sampling kind(s): " + ",".join(unknown))
        if sampling_weight is None:
            sampling_weight = {}
        if not isinstance(sampling_weight, dict):
            raise ValueError("sampling_weight must be an object")
        unknown_weight = sorted(set(sampling_weight) - set(sampling_kinds))
        if unknown_weight:
            raise ValueError("sampling_weight names undeclared kind(s): "
                             + ",".join(unknown_weight))
        normalized = {}
        for kind in sampling_kinds:
            weight = sampling_weight.get(kind, 1)
            if type(weight) is not int or weight < 0:
                raise ValueError("sampling_weight must be nonnegative integers")
            normalized[kind] = weight
        if not any(normalized.values()):
            raise ValueError("sampling_weight must not be all zero")
        if self.selection_log and (self.sampling_kinds != sampling_kinds or self.sampling_weight != normalized):
            raise ValueError("sampling configuration cannot change after selection")
        self.sampling_kinds = list(sampling_kinds)
        self.sampling_weight = normalized

    @staticmethod
    def _supported_sampling_kinds(entry):
        """Kinds the entry's catalog surface can actually generate.

        Uses the same declared-domain logic as ``mutate_wide``; it never probes
        a mutator or records a proposal.  Unknown/synthetic surfaces support no
        wide kind and are reported rather than guessed.
        """
        from r2f.core.mutators import _wide_kinds
        from r2f.modules.s1.targets import exploratory_target_declarations
        if ExploratoryScheduler._surface_cache is None:
            ExploratoryScheduler._surface_cache = {
                (target_id, surface_id): surface
                for target_id, target in exploratory_target_declarations().items()
                if target.exploration_contract is not None
                for surface_id, surface in target.exploration_contract.surface_by_id.items()
            }
        surface = ExploratoryScheduler._surface_cache.get(
            (entry.get("target_id"), entry.get("surface_id")))
        parent = entry.get("final_config")
        if surface is None or not isinstance(parent, dict):
            return []
        try:
            kinds = _wide_kinds(surface, parent)
            if has_routes(entry):
                allowed = {"M01": {"boundary"}, "M02": {"representative"},
                           "M03": {"representative", "log_uniform", "linear_uniform", "neighbor", "jump"}}
                kinds = [kind for kind in kinds if kind in allowed.get(entry.get("mutator_id"), set())]
            return kinds
        except (TypeError, ValueError):
            return []

    def _choose_sampling_kind(self, sequence, entry, token=None):
        """Pick one configured kind supported by ``entry``.

        Kinds selected fewer times in this cycle win first.  Among equals the
        configured nonnegative weights are used.  A kind weighted zero is never
        selected; if every supported kind is excluded the result is ``None`` so
        the runner can reject the case.  All ordering is canonical.
        """
        supported = set(self._supported_sampling_kinds(entry))
        eligible = [kind for kind in self.sampling_kinds if kind in supported]
        if not eligible:
            return None, {"declared": list(self.sampling_kinds), "supported": []}
        # A zero weight is an exclusion, not just a lower preference.  If the
        # surface only supports excluded kinds the runner must reject rather
        # than silently sampling an excluded class.
        positive = [kind for kind in eligible if self.sampling_weight.get(kind, 1) > 0]
        if not positive:
            return None, {"declared": list(self.sampling_kinds),
                          "supported": sorted(supported), "eligible": [],
                          "reason": "all_supported_kinds_zero_weight"}
        eligible = positive
        used = {}
        for event in self.selection_log:
            kind = event.get("sampling_kind")
            if kind in eligible:
                used[kind] = used.get(kind, 0) + 1
        floor = min(used.get(kind, 0) for kind in eligible)
        candidates = sorted(kind for kind in eligible if used.get(kind, 0) == floor)
        weights = [self.sampling_weight.get(kind, 1) for kind in candidates]
        if not any(weights):
            weights = [1 for _ in candidates]
        material = canonical_hash({
            "rng_seed": self.rng_seed, "sequence": sequence,
            "token": token, "pool": candidates,
            "weights": weights, "feedback_identity": self.feedback.identity(),
            "sampling_log": [[e.get("sampling_kind")] for e in self.selection_log],
        })
        rng = random.Random(int(material[:32], 16))
        target = rng.randrange(sum(weights))
        chosen = candidates[0]
        for kind, weight in zip(candidates, weights):
            if target < weight:
                chosen = kind
                break
            target -= weight
        return chosen, {"declared": list(self.sampling_kinds),
                        "supported": sorted(supported), "eligible": candidates,
                        "used_counts": used, "weights": dict(zip(candidates, weights))}

    def _rng(self, sequence, pool):
        material = canonical_hash({"rng_seed": self.rng_seed, "sequence": sequence,
                                   "pool": pool, "feedback_identity": self.feedback.identity()})
        return random.Random(int(material[:32], 16))

    def _rows(self):
        rows = self.corpus.selectable()
        if not rows:
            raise ValueError("exploratory selection requires at least one executable corpus entry")
        return rows

    def _uniform(self, rows):
        return list(rows)

    def _uncovered(self, rows):
        covered = self.feedback.covered_surfaces()
        return [row for row in rows
                if surface_class(row) is not None and surface_class(row) not in covered]

    def _edge_by_content(self):
        """Feedback edges attach to the executed content they were observed on."""
        result = {}
        for event in self.feedback.events:
            if event.get("coverage") and event.get("edge_id"):
                result.setdefault(event["content_sha256"], event["edge_id"])
        return result

    def _rare(self, rows):
        observed_edges = self._edge_by_content()
        return [row for row in rows if row["content_sha256"] in observed_edges]

    def _pool(self, name, rows):
        if name == "uniform":
            return self._uniform(rows)
        if name == "uncovered":
            return self._uncovered(rows)
        if name == "rare_edge":
            return self._rare(rows)
        raise ValueError("unknown exploratory pool")

    @classmethod
    def _fair_classes(cls, entry):
        """Declared input axes only; observation/retention fields are excluded.

        A missing chain depth stays unknown, never an inferred execution depth.
        Variant falls back to the declared seed id for older corpus formats.
        """
        values = {name: entry.get(name) for name in cls.FAIR_DIMENSIONS}
        values["variant_id"] = entry.get("variant_id") or entry.get("seed_id")
        for name, value in values.items():
            if name == "chain_depth":
                if value is not None and (type(value) is not int or value < 1):
                    raise ValueError("fair chain_depth must be a positive integer or missing")
            elif value is not None and not isinstance(value, str):
                raise ValueError("fair input class must be a string: " + name)
        return values

    def _fair_tie(self, candidates, material, stage):
        """Stable, seeded tie-break over a canonical model-only candidate list."""
        ordered = sorted(candidates, key=canonical_hash)
        digest = canonical_hash({"decision": material, "stage": stage})
        return random.Random(int(digest[:32], 16)).choice(ordered)

    def _fair_sampling(self, entry, material):
        """Weighted least-selection shares, with no feedback-derived entropy."""
        supported = sorted(set(self._supported_sampling_kinds(entry)))
        declared = list(self.sampling_kinds)
        positive = [kind for kind in declared
                    if kind in supported and self.sampling_weight[kind] > 0]
        if not positive:
            meta = {"declared": declared, "supported": supported, "eligible": []}
            if any(kind in supported for kind in declared):
                meta["reason"] = "all_supported_kinds_zero_weight"
            return None, meta
        used = {kind: sum(event.get("sampling_kind") == kind for event in self.selection_log)
                for kind in positive}
        # Fractions avoid float rounding and give configured weights true
        # long-run shares, not merely tie-breaking preference.
        floor = min(Fraction(used[kind], self.sampling_weight[kind]) for kind in positive)
        candidates = sorted(kind for kind in positive
                            if Fraction(used[kind], self.sampling_weight[kind]) == floor)
        sampling_input = {"model": material, "kinds": declared,
                          "weight": dict(self.sampling_weight), "supported": supported}
        chosen = self._fair_tie(candidates, sampling_input, "sampling")
        return chosen, {"declared": declared, "supported": supported,
                        "eligible": candidates, "used_counts": used,
                        "weights": {kind: self.sampling_weight[kind] for kind in positive}}

    def _select_route(self, entry, sequence):
        """Baseline first; equal route shares using selection history only.

        No duplicate corpus rows, feedback-derived entropy, or execution claims.
        A newly unioned route starts at zero without rewriting past contexts.
        """
        routes = validate_route_entry(entry)
        if routes is None:
            return entry, None
        counts = [{"route": route, "count": sum(
            event.get("content_sha256") == entry["content_sha256"] and
            event.get("mutation_route") == route for event in self.selection_log)}
            for route in routes]
        floor = min(item["count"] for item in counts)
        candidates = [item["route"] for item in counts if item["count"] == floor]
        material = {"rng_seed": self.rng_seed, "sequence": sequence,
                    "content_sha256": entry["content_sha256"], "route_counts": counts}
        selected = self._fair_tie(candidates, material, "mutation_route")
        return route_view(entry, selected), {
            "schema": entry["mutation_routes_schema"], "policy": "least_used",
            "route_counts": counts, "selected": deepcopy(selected),
            "observation_guards_discharged": False}

    def _fair_event(self, rows, sequence):
        """Equal sibling shares down the input hierarchy, then content leaves.

        Row count cannot buy a class a larger share. Every tie and the final
        decision token use this projection, never a feedback hash, oracle,
        observed edge, corpus retention rank, or execution context. Historical
        classes live in events so corpus additions/removals do not rewrite them.
        """
        eligible = sorted(({
            "content_sha256": row["content_sha256"],
            "input_classes": self._fair_classes(row),
        } for row in rows), key=lambda row: row["content_sha256"])
        history = [{key: deepcopy(event[key])
                    for key in ("sequence", "content_sha256", "input_classes")}
                   | {"sampling_kind": event.get("sampling_kind")}
                   for event in self.selection_log]
        for item, event in zip(history, self.selection_log):
            if "baseline_input_classes" in event:
                item["input_classes"] = deepcopy(event["baseline_input_classes"])
                item["mutation_route"] = deepcopy(event["mutation_route"])
        material = {"policy": "fair", "rng_seed": self.rng_seed,
                    "sequence": sequence, "eligible": eligible, "history": history}
        candidates, prior, prefix = eligible, history, {}
        for dimension in self.FAIR_DIMENSIONS:
            values = {canonical_hash(row["input_classes"][dimension]): row["input_classes"][dimension]
                      for row in candidates}
            used = {key: sum(item["input_classes"][dimension] == value for item in prior)
                    for key, value in values.items()}
            floor = min(used.values())
            tied = [values[key] for key in sorted(values) if used[key] == floor]
            value = self._fair_tie(tied, material, {"dimension": dimension, "prefix": prefix})
            prefix = {**prefix, dimension: value}
            candidates = [row for row in candidates if row["input_classes"][dimension] == value]
            prior = [item for item in prior if item["input_classes"][dimension] == value]
        used = {row["content_sha256"]: sum(item["content_sha256"] == row["content_sha256"]
                                          for item in prior)
                for row in candidates}
        floor = min(used.values())
        tied = [row for row in candidates if used[row["content_sha256"]] == floor]
        selected = self._fair_tie(tied, material, {"dimension": "content_sha256", "prefix": prefix})
        chosen = self._record(selected["content_sha256"])
        chosen, route_selection = self._select_route(chosen, sequence)
        material["chosen"] = deepcopy(selected)
        if route_selection is not None:
            material["route_selection"] = deepcopy(route_selection)
        event = {"sequence": sequence,
                 "phase": "warmup" if sequence < self.warmup else "exploratory",
                 "pool": "fair", "used_pool": "fair", "requested_pool": "fair",
                 "fallback_chain": [], "weighted": False, "candidate_count": len(rows),
                 "seed_id": chosen.get("seed_id"), "content_sha256": chosen["content_sha256"],
                 "input_classes": deepcopy(selected["input_classes"]),
                 "feedback_identity": self.feedback.identity()}
        if route_selection is not None:
            event.update(mutation_route=deepcopy(chosen["mutation_route"]),
                         route_selection=route_selection,
                         baseline_input_classes=deepcopy(selected["input_classes"]),
                         input_classes=self._fair_classes(chosen))
        material["sampling"] = None
        if self.sampling_kinds is not None:
            kind, meta = self._fair_sampling(chosen, material)
            event.update(sampling_kind=kind, sampling=meta)
            if kind is None:
                event["sampling_error"] = "no_supported_sampling_kind"
            material["sampling"] = {"kinds": list(self.sampling_kinds),
                                    "weight": dict(self.sampling_weight),
                                    "kind": kind, "selection": deepcopy(meta)}
        event["decision_context"] = material
        event["decision_sha256"] = canonical_hash(material)
        return chosen, event

    def next(self):
        """Select legacy feedback/fair inputs or the next bounded BFS task."""
        if self._epoch() != self.feedback_epochs[-1]:
            raise ValueError("accepted feedback epoch was modified")
        if self.policy == "bfs":
            return self._next_bfs()
        rows = self._rows()
        sequence = len(self.selection_log)
        if self.policy == "fair":
            chosen, event = self._fair_event(rows, sequence)
        elif sequence < self.warmup:
            # Interleave variants before repeating a variant across RMWs so a short
            # warm-up covers distinct boundary classes.
            ordered = sorted(rows, key=lambda r: (r.get("rmw") or "",
                                                   r.get("variant_id") or r.get("seed_id") or "",
                                                   r["content_sha256"]))
            chosen = ordered[sequence % len(ordered)]
            event = {"sequence": sequence, "phase": "warmup", "pool": "warmup",
                     "requested_pool": "warmup", "fallback_chain": [], "weighted": False,
                     "candidate_count": len(rows), "seed_id": chosen.get("seed_id"),
                     "content_sha256": chosen["content_sha256"],
                     "feedback_identity": self.feedback.identity()}
        else:
            cycle = (sequence - self.warmup) % 5
            requested = "uniform" if cycle == 0 else "rare_edge" if cycle in (3, 4) else "uncovered"
            order = (requested,) + tuple(pool for pool in self.FALLBACK if pool != requested)
            fallback, pool, used_pool = [], None, None
            for candidate in order:
                pool = self._pool(candidate, rows)
                if pool:
                    used_pool = candidate
                    break
                fallback.append(candidate)
            if fallback:
                # Record the empty pool(s) and the pool that actually supplied
                # the selection, so the path is reconstructible end to end.
                fallback.append(used_pool)
            pool = sorted(pool, key=lambda r: (r.get("seed_id") or "", r["content_sha256"]))
            if requested == "rare_edge" and pool:
                observed_edges = self._edge_by_content()
                counts = self.feedback.edge_counts()
                pool = sorted(pool, key=lambda r: (counts.get(observed_edges.get(r["content_sha256"]), 0),
                                                   r.get("seed_id") or "", r["content_sha256"]))
                chosen = self._rng(sequence, requested).choice(pool[:max(1, len(pool) // 2)])
            else:
                chosen = self._rng(sequence, requested).choice(pool)
            event = {"sequence": sequence, "phase": "exploratory", "pool": requested,
                     "used_pool": used_pool,
                     "requested_pool": requested, "fallback_chain": fallback,
                     "weighted": True, "candidate_count": len(pool),
                     "seed_id": chosen.get("seed_id"), "content_sha256": chosen["content_sha256"],
                     "feedback_identity": self.feedback.identity()}
        if self.policy != "fair":
            chosen, route_selection = self._select_route(chosen, sequence)
            if route_selection is not None:
                event.update(mutation_route=deepcopy(chosen["mutation_route"]),
                             route_selection=route_selection)
        if self.policy != "fair" and self.sampling_kinds is not None:
            sampling_kind, sampling_meta = self._choose_sampling_kind(
                sequence, chosen, token=chosen["content_sha256"])
            event["sampling_kind"] = sampling_kind
            event["sampling"] = sampling_meta
            if sampling_kind is None:
                event["sampling_error"] = "no_supported_sampling_kind"
        if self._explicit_policy:
            event["policy"] = self.policy
        event["selection_sha256"] = canonical_hash(event)
        self.selection_log.append(event)
        context = {"sequence": sequence, "feedback_epoch": len(self.feedback_epochs) - 1,
                   **self._epoch(), "selection_sha256": event["selection_sha256"],
                   "corpus": self.corpus.to_dict(),
                   "sampling": ({"kinds": list(self.sampling_kinds), "weight": dict(self.sampling_weight)}
                                if self.sampling_kinds is not None else None)}
        if "mutation_route" in event:
            context["mutation_route"] = deepcopy(event["mutation_route"])
            context["route_selection"] = deepcopy(event["route_selection"])
        if self._explicit_policy:
            context["policy"] = self.policy
        context["context_sha256"] = canonical_hash(context)
        self.selection_contexts.append(context)
        return dict(deepcopy(event), entry=deepcopy(chosen))

    def _record(self, digest):
        return self.corpus.entries[digest]

    def plan(self, count):
        if type(count) is not int or count < 0:
            raise ValueError("selection count must be a nonnegative integer")
        return [self.next() for _ in range(count)]

    def to_dict(self):
        result = {"schema": "r2f.s1-selection-log/v2", "rng_seed": self.rng_seed,
                  "warmup": self.warmup, "feedback_identity": self.feedback.identity(),
                  "feedback_event_count": len(self.feedback.events),
                  "feedback_events_sha256": canonical_hash(self.feedback.events),
                  "feedback_epochs": deepcopy(self.feedback_epochs),
                  "selection_contexts": deepcopy(self.selection_contexts),
                  "selections": deepcopy(self.selection_log)}
        if self.sampling_kinds is not None:
            result["sampling"] = {"kinds": list(self.sampling_kinds), "weight": dict(self.sampling_weight)}
        if self._explicit_policy:
            result["policy"] = self.policy
        if self.policy == "bfs":
            if self.frontier is None:
                raise ValueError("bfs requires initialized input search frontier")
            result["frontier"] = self.frontier.to_dict()
        result["selection_identity"] = canonical_hash(result)
        return result

    @classmethod
    def from_dict(cls, state, corpus, feedback, *, model_parents=None):
        """Replay each historical selection against its own corpus and epoch."""
        if not isinstance(state, dict) or state.get("schema") != "r2f.s1-selection-log/v2":
            raise ValueError("unsupported resumable selection log")
        if state.get("selection_identity") != canonical_hash({k: v for k, v in state.items() if k != "selection_identity"}):
            raise ValueError("selection state hash mismatch")
        policy = state.get("policy")
        if "policy" in state and (not isinstance(policy, str) or policy not in cls.POLICIES):
            raise ValueError("unknown scheduler policy in selection state")
        full = validated_feedback(feedback.events)
        count = state.get("feedback_event_count")
        if type(count) is not int or count < 0 or count > len(full.events):
            raise ValueError("selection feedback prefix is missing")
        accepted = validated_feedback(full.events[:count])
        if (accepted.identity() != state.get("feedback_identity") or
                canonical_hash(accepted.events) != state.get("feedback_events_sha256")):
            raise ValueError("selection feedback prefix identity drift")
        epochs, contexts, events = (state.get(k) for k in ("feedback_epochs", "selection_contexts", "selections"))
        if not isinstance(epochs, list) or not epochs or not isinstance(contexts, list) or not isinstance(events, list) or len(events) != len(contexts):
            raise ValueError("selection contexts/epochs are incomplete")
        previous = -1
        for epoch in epochs:
            n = epoch.get("feedback_event_count")
            if type(n) is not int or n <= previous or n > count:
                raise ValueError("feedback epoch order drift")
            prefix = validated_feedback(full.events[:n])
            if epoch != {"feedback_event_count": n, "feedback_events_sha256": canonical_hash(prefix.events), "feedback_identity": prefix.identity()}:
                raise ValueError("feedback epoch hash drift")
            previous = n
        if previous != count:
            raise ValueError("feedback epoch terminal count differs")
        first = epochs[0]["feedback_event_count"]
        scheduler = cls(corpus, validated_feedback(full.events[:first]),
                        rng_seed=state["rng_seed"], warmup=state["warmup"], policy=policy)
        final_frontier = None
        if policy == "bfs":
            from .search_frontier import InputSearchFrontier
            final_frontier = InputSearchFrontier.from_dict(state.get("frontier"))
            offsets = [i for i, item in enumerate(final_frontier.journal) if item["action"] == "select"]
            if len(offsets) != len(events):
                raise ValueError("input search selection/journal count mismatch")
            historical_frontier = InputSearchFrontier.prefix(state["frontier"], 0)
            historical_offset = 0
        elif "frontier" in state:
            raise ValueError("non-bfs selection cannot carry frontier")
        last_epoch = 0
        for sequence, (context, event) in enumerate(zip(contexts, events)):
            if not isinstance(context, dict) or not isinstance(event, dict):
                raise ValueError("selection event/context must be objects")
            if ("policy" in context) != ("policy" in state) or context.get("policy") != policy:
                raise ValueError("selection context policy drift")
            if ("policy" in event) != ("policy" in state) or event.get("policy") != policy:
                raise ValueError("selection event policy drift")
            if context.get("context_sha256") != canonical_hash({k: v for k, v in context.items() if k != "context_sha256"}):
                raise ValueError("selection corpus/context hash mismatch")
            epoch_id = context.get("feedback_epoch")
            if type(epoch_id) is not int or not last_epoch <= epoch_id < len(epochs):
                raise ValueError("selection feedback epoch is invalid")
            while last_epoch < epoch_id:
                last_epoch += 1
                scheduler.advance_feedback(validated_feedback(full.events[:epochs[last_epoch]["feedback_event_count"]]))
            scheduler.corpus = _restore_checkpoint_corpus(context["corpus"], model_parents=model_parents)
            sampling = context.get("sampling")
            if sampling is not None:
                scheduler.configure_sampling(sampling["kinds"], sampling["weight"])
            if policy == "bfs":
                if context.get("frontier_journal_count") != offsets[sequence]:
                    raise ValueError("input search historical frontier prefix mismatch")
                for item in final_frontier.journal[historical_offset:offsets[sequence]]:
                    historical_frontier.apply_event(item)
                scheduler.frontier = historical_frontier
            rebuilt = scheduler.next()
            if policy == "bfs":
                historical_offset = offsets[sequence] + 1
            rebuilt.pop("entry")
            if rebuilt != event or scheduler.selection_contexts[-1] != context:
                raise ValueError("selection cannot be reproduced from historical corpus/prefix")
        while last_epoch + 1 < len(epochs):
            last_epoch += 1
            scheduler.advance_feedback(validated_feedback(full.events[:epochs[last_epoch]["feedback_event_count"]]))
        scheduler.corpus = corpus
        sampling = state.get("sampling")
        if sampling is not None:
            scheduler.configure_sampling(sampling["kinds"], sampling["weight"])
        if policy == "bfs":
            scheduler.frontier = final_frontier
            if (final_frontier.rng_seed != scheduler.rng_seed or
                    final_frontier.sampling_kinds != scheduler.sampling_kinds or
                    final_frontier.sampling_weight != scheduler.sampling_weight):
                raise ValueError("input search scheduler settings mismatch")
        if scheduler.to_dict() != state:
            raise ValueError("selection state cannot be reproduced")
        return scheduler


def replay_selection(log, corpus, feedback_events):
    """Rebuild a selection log offline from the recorded RNG and feedback prefix.

    Events appended after the recorded prefix are ignored for this run; the
    prefix identity itself must still match exactly and in order.
    """
    if isinstance(log, dict) and log.get("schema") == "r2f.s1-selection-log/v2":
        scheduler = ExploratoryScheduler.from_dict(log, corpus, validated_feedback(list(feedback_events)))
        return {"schema": "r2f.s1-selection-replay/v2", "valid": True,
                "selection_identity": scheduler.to_dict()["selection_identity"],
                "selections": deepcopy(scheduler.selection_log)}
    if not isinstance(log, dict) or log.get("schema") != "r2f.s1-selection-log/v1":
        raise ValueError("unsupported selection log")
    # New policy/decision records are v2-only: the v1 hash-only replay must
    # not accidentally endorse unverified additive decision fields.
    new_keys = {"policy", "decision_context", "decision_sha256",
                "mutation_route", "route_selection", "baseline_input_classes"}
    if (new_keys.intersection(log) or
            any(new_keys.intersection(event) for event in log.get("selections", [])
                if isinstance(event, dict))):
        raise ValueError("v1 selection log cannot carry policy/decision fields")
    count = log.get("feedback_event_count")
    if type(count) is not int or count < 0 or count > len(feedback_events):
        raise ValueError("selection log requires its recorded feedback prefix")
    feedback = FeedbackLog.from_events(list(feedback_events)[:count])
    if feedback.identity() != log.get("feedback_identity"):
        raise ValueError("feedback order differs from recorded selection identity")
    sampling = log.get("sampling")
    scheduler = ExploratoryScheduler(
        corpus, feedback, rng_seed=log["rng_seed"], warmup=log["warmup"],
        sampling_kinds=(sampling or {}).get("kinds"),
        sampling_weight=(sampling or {}).get("weight"))
    rebuilt = scheduler.plan(len(log["selections"]))
    expected = [e.get("selection_sha256") for e in log["selections"]]
    actual = [e.get("selection_sha256") for e in rebuilt]
    if actual != expected:
        raise ValueError("selection log cannot be reproduced")
    return {"schema": "r2f.s1-selection-replay/v1", "valid": True,
            "selection_identity": scheduler.to_dict()["selection_identity"],
            "selections": rebuilt}
