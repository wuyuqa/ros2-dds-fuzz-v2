"""L02 raw-only concurrency reconstruction. Offline rows never qualify as DDS.

Only strictly positive overlap of individual public publish API calls counts;
ready waits, batch envelopes, sleeps, processes and controller ACKs do not.
"""
IDENTITY = ("logical_id", "generation", "pid", "pgid", "process_start_ticks",
            "endpoint_generation", "run_id", "domain_id")


def audit_concurrency(events, actions, group):
    members = [a for a in actions if a.get("params", {}).get("concurrency_group") == group]
    def require(ok, reason):
        if not ok:
            raise ValueError("concurrency:" + reason)
    require(len(members) == 2 and len({a["endpoint_id"] for a in members}) == 2, "members")
    intervals, refs, ready_times, release_times, clocks, pids = [], [], [], [], [], []
    ready_indices, release_indices = [], []
    provenance = [e for e in events if e.get("event") == "topology_provenance"]
    prepared = [e for e in events if e.get("event") == "topology_backend_prepared"]
    # Per-call overlap is useful even without execution provenance. Only an
    # explicit, unique global public-runtime declaration and preparation can
    # qualify it as real; missing/unknown/contradictory claims fail closed.
    runtime_qualified = (
        len(provenance) == len(prepared) == 1
        and provenance[0].get("runtime_kind") == "ros2_public_api"
        and provenance[0].get("fixture") is False
        and provenance[0].get("backend") == "ros2_public_api"
        and prepared[0].get("backend") == "ros2_public_api"
        and prepared[0].get("real_ros") is True
        and not any(
            ("fixture" in e and e["fixture"] is not False)
            or ("real_ros" in e and e["real_ros"] is not True)
            or ("offline_fake" in e and e["offline_fake"] is not False)
            or ("backend" in e and e["backend"] != "ros2_public_api")
            for e in events))
    fixture = not runtime_qualified
    for action in members:
        eid, gen = action["endpoint_id"], action["generation"]
        creates = [e for e in events if e.get("event") == "endpoint_created"
                   and e.get("logical_id") == eid and e.get("generation") == gen]
        require(len(creates) == 1, "creation")
        created = creates[0]
        require(all(created.get(k) is not None for k in IDENTITY) and created.get("endpoint_gid"), "identity_missing")
        pids.append(created["pid"])
        rows = [e for e in events if e.get("concurrency_group") == group and e.get("logical_id") == eid]
        def one(kind):
            found = [e for e in rows if e.get("event") == kind]
            require(len(found) == 1, kind + "_missing_or_duplicate")
            return found[0]
        request, ready, release, start, finish = [one(k) for k in
            ("concurrent_publish_requested", "concurrent_publish_ready", "concurrent_publish_release",
             "concurrent_publish_started", "concurrent_publish_finished")]
        cid = request.get("command_id")
        require(type(cid) is int and cid > 0, "command_id")
        sends = [e for e in events if e.get("event") == "send" and e.get("logical_id") == eid
                 and e.get("generation") == gen and e.get("command_id") == cid]
        done = [e for e in events if e.get("event") == "command_done" and e.get("logical_id") == eid
                and e.get("generation") == gen and e.get("command_id") == cid]
        require(len(done) == 1, "completion_missing_or_duplicate")
        chain = [request, ready, release, start, *sends, finish, done[0]]
        clock = created.get("monotonic_clock_identity")
        require(isinstance(clock, dict) and bool(clock), "clock_missing")
        clocks.append(clock)
        require(all(all(e.get(k) == created[k] for k in IDENTITY) for e in chain), "foreign_identity")
        require(all(e.get("command_id") == cid and e.get("monotonic_clock_identity") == clock for e in chain), "command_or_clock")
        require(all(type(e.get("monotonic_ns")) is int and e["monotonic_ns"] > 0 for e in chain), "timestamp")
        require(all(a["monotonic_ns"] <= b["monotonic_ns"] for a, b in zip(chain, chain[1:])), "happens_before")
        require(all(type(e.get("event_index")) is int for e in chain)
                and all(a["event_index"] < b["event_index"] for a, b in zip(chain, chain[1:])), "raw_happens_before")
        require(created["monotonic_ns"] <= request["monotonic_ns"], "creation_hb")
        require(all(e.get("endpoint_gid", created["endpoint_gid"]) == created["endpoint_gid"] for e in chain), "foreign_gid")
        p = action["params"]
        require([e.get("send_sequence") for e in sends] == list(range(p["start_sequence"], p["start_sequence"] + p["count"])), "send_set")
        segments = []
        previous_return = start["monotonic_ns"]
        for send in sends:
            lo, hi = send.get("publish_begin_ns"), send.get("publish_return_ns")
            require(type(lo) is int and type(hi) is int and start["monotonic_ns"] <= lo < hi <= send["monotonic_ns"] <= finish["monotonic_ns"], "publish_interval")
            require(lo >= previous_return, "same_writer_overlapping_calls")
            previous_return = hi
            require(send.get("writer_gid") == created["endpoint_gid"] and send.get("publish_returned") is True
                    and send.get("phase") == action["action_id"], "publish_binding")
            segments.append((lo, hi, send["event_index"]))
        intervals.append(segments)
        ready_times.append(ready["monotonic_ns"])
        release_times.append(release["monotonic_ns"])
        ready_indices.append(ready["event_index"])
        release_indices.append(release["event_index"])
        if runtime_qualified:
            # Global claims must belong to this run/domain and already be in
            # the raw prefix, not be foreign or appended after publication.
            fixture |= not all(
                all(e.get(k) == created[k] for k in ("run_id", "domain_id"))
                and type(e.get("event_index")) is int
                and e["event_index"] < request["event_index"]
                and type(e.get("monotonic_ns")) is int
                and 0 < e["monotonic_ns"] <= request["monotonic_ns"]
                for e in provenance + prepared)
        refs.extend(e["event_index"] for e in chain)
        fixture |= any(e.get("fixture") is True or e.get("real_ros") is False for e in chain + [created])
    require(pids[0] != pids[1] and clocks[0] == clocks[1], "worker_or_clock_alias")
    require(max(ready_times) <= min(release_times), "all_ready_before_release")
    require(max(ready_indices) < min(release_indices), "raw_all_ready_before_release")
    overlaps = [{"left_ref": a[2], "right_ref": b[2], "overlap_ns": min(a[1], b[1]) - max(a[0], b[0])}
                for a in intervals[0] for b in intervals[1] if max(a[0], b[0]) < min(a[1], b[1])]
    return {"concurrency_group": group, "status": "overlap_observed" if overlaps else "serial_or_no_api_overlap",
            "concurrency_observed": bool(overlaps) and not fixture, "fixture_overlap_observed": bool(overlaps) and fixture,
            "fixture": fixture, "raw_refs": sorted(set(refs)), "publish_api_overlaps": overlaps,
            "candidate_eligible": False, "semantics": "strict_individual_publish_api_interval_overlap"}


def action_trace_complete(events, actions):
    """Check serial action blocks, allowing only raw-proven publish groups.

    Controller begin/done order remains the program order. A concurrent block
    must enclose its complete independently audited worker protocol; unrelated
    actions remain strictly serial. Acceptance here grants no real overlap,
    mutation hit, delivery completion or oracle result on its own.
    """
    begins = [e for e in events if e.get('event') == 'topology_action_begin']
    ends = [e for e in events if e.get('event') == 'topology_action_done']
    if ([e.get('action') for e in begins] != actions
            or [e.get('action') for e in ends] != actions):
        return False
    for i, (action, begin, end) in enumerate(zip(actions, begins, ends)):
        if (any(e.get('action_id') != action['action_id'] or e.get('action_index') != i
                or type(e.get('event_index')) is not int for e in (begin, end))
                or begin['event_index'] >= end['event_index']):
            return False
    previous_end, i = -1, 0
    try:
        while i < len(actions):
            group = actions[i].get('params', {}).get('concurrency_group')
            j = i + 1
            if group is not None:
                while j < len(actions) and actions[j].get('params', {}).get('concurrency_group') == group:
                    j += 1
                if j - i != 2 or any(a.get('op') != 'publish_batch' for a in actions[i:j]):
                    return False
                proof = audit_concurrency(events, actions, group)
                # Neither requests before action begin nor action done before
                # worker command completion may be hidden by the group marker.
                if not (max(e['event_index'] for e in begins[i:j]) < min(proof['raw_refs'])
                        and max(proof['raw_refs']) < min(e['event_index'] for e in ends[i:j])):
                    return False
            if previous_end >= min(e['event_index'] for e in begins[i:j]):
                return False
            previous_end = max(e['event_index'] for e in ends[i:j])
            i = j
    except (ValueError, KeyError, TypeError):
        return False
    return True
