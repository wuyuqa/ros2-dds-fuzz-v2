"""Required matrix plus budget-driven exploratory selection on one scheduler.

Required routing is unchanged and deterministic.  Exploratory routing uses a
fixed 20/40/40 cycle: uniform over all executable seeds, uncovered
surface x boundary classes, then rare observed edges.  Empty pools fall back in
a fixed order.  Selection is a pure function of the feedback log, RNG seed and
corpus snapshot; it never executes anything.
"""
from itertools import product
import random

from r2f.core.evidence import canonical_hash
from r2f.modules.s1.targets import RMWS, EXECUTION_ORDER
from .corpus import Corpus, content_sha256
from .feedback import FeedbackLog, edge_id, feedback_identity, surface_class


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
    _surface_cache = None

    def __init__(self, corpus, feedback=None, *, rng_seed=0, warmup=10,
                 sampling_kinds=None, sampling_weight=None):
        if not isinstance(corpus, Corpus):
            raise ValueError("exploratory scheduler requires a Corpus")
        if type(rng_seed) is not int or rng_seed < 0:
            raise ValueError("rng_seed must be a nonnegative integer")
        if type(warmup) is not int or warmup < 0:
            raise ValueError("warmup must be a nonnegative integer")
        self.corpus = corpus
        source_log = feedback if isinstance(feedback, FeedbackLog) else FeedbackLog.from_events(feedback or [])
        # Freeze the feedback prefix at construction.  Selections made by this
        # scheduler must remain reproducible even if the caller appends more
        # observations between next() calls.  Later events belong to a later
        # scheduler instance and a distinct selection identity.
        self.feedback_event_count = len(source_log.events)
        self.feedback_identity_at_start = source_log.identity()
        self.feedback = FeedbackLog.from_events(source_log.events)
        if self.feedback.identity() != self.feedback_identity_at_start:
            raise ValueError("feedback snapshot identity drift")
        self.rng_seed = rng_seed
        self.warmup = warmup
        self.selection_log = []
        self.sampling_kinds = None
        self.sampling_weight = None
        if sampling_kinds is not None or sampling_weight is not None:
            self.configure_sampling(sampling_kinds, sampling_weight)

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
        self.sampling_kinds = list(sampling_kinds)
        self.sampling_weight = normalized

    def _supported_sampling_kinds(self, entry):
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
            return _wide_kinds(surface, parent)
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

    def next(self):
        """Select one entry on a 5-step 20/40/40 cycle; warm-up is deterministic."""
        rows = self._rows()
        sequence = len(self.selection_log)
        if sequence < self.warmup:
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
        if self.sampling_kinds is not None:
            sampling_kind, sampling_meta = self._choose_sampling_kind(
                sequence, chosen, token=chosen["content_sha256"])
            event["sampling_kind"] = sampling_kind
            event["sampling"] = sampling_meta
            if sampling_kind is None:
                event["sampling_error"] = "no_supported_sampling_kind"
        event["selection_sha256"] = canonical_hash(event)
        self.selection_log.append(event)
        return dict(event, entry=self._record(chosen["content_sha256"]))

    def _record(self, digest):
        return self.corpus.entries[digest]

    def plan(self, count):
        if type(count) is not int or count < 0:
            raise ValueError("selection count must be a nonnegative integer")
        return [self.next() for _ in range(count)]

    def to_dict(self):
        result = {"schema": "r2f.s1-selection-log/v1", "rng_seed": self.rng_seed,
                  "warmup": self.warmup, "feedback_identity": self.feedback_identity_at_start,
                  "feedback_event_count": self.feedback_event_count,
                  "selection_identity": canonical_hash({"rng_seed": self.rng_seed, "warmup": self.warmup,
                      "feedback_identity": self.feedback_identity_at_start,
                      "feedback_event_count": self.feedback_event_count,
                      "selections": [e["selection_sha256"] for e in self.selection_log]}),
                  "selections": list(self.selection_log)}
        if self.sampling_kinds is not None:
            result["sampling"] = {"kinds": list(self.sampling_kinds),
                                  "weight": dict(self.sampling_weight)}
        return result


def replay_selection(log, corpus, feedback_events):
    """Rebuild a selection log offline from the recorded RNG and feedback prefix.

    Events appended after the recorded prefix are ignored for this run; the
    prefix identity itself must still match exactly and in order.
    """
    if not isinstance(log, dict) or log.get("schema") != "r2f.s1-selection-log/v1":
        raise ValueError("unsupported selection log")
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
