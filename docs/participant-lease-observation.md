# Participant discovery lease observations (#16)

`discovery.interruption_recovery` retains its revision-0 primary recovery contract.
Its optional `observe_participant_leases: true` semantic input adds supporting
participant observations to `typed_facts.participant_lease`. It does not replace
writer liveliness, recovery gates, the candidate lifecycle, or the required
four-target / 72-cell denominator.

## Public execution and report boundary

Use `scripts/run_s1.py` and the canonical `isolated_netns` boundary. An explicit
recovery observation JSON contains exactly `observation_mode`,
`observe_adopted_config` (false), and `cell_id`. The cell must name an existing
Cyclone required or negative-control cell. Explicit observation execution requires
`--max-executions 1`; every other cell remains planned in the complete matrix.
Legacy first-target observation declarations retain their original two-field
shape and baseline-0 scope.

Example semantic parameters:

```json
{"observe_participant_leases": true, "drop_duration_sec": 12.0, "recovery_timeout_sec": 15.0}
```

Example execution-only observation selection:

```json
{"observation_mode": "instrumentation", "observe_adopted_config": false, "cell_id": "rmw_cyclonedds_cpp.mutated.0"}
```

The ordinary campaign without an observation declaration still executes the
unchanged required/control matrix. The lease flag may be omitted to preserve the
legacy semantic identity. Observation mode is execution metadata and is restored
by canonical replay from the hash-bound original execution/raw metadata, rather
than from mutable configuration or profile files.

## What each fact means

- Advertised duration and exact RTPS rational encoding come from independently
  captured SPDP bytes. Finite silence is not an expiry or cleanup fact.
- Local endpoint-to-participant ownership comes from two stable samples of the
  existing pinned Cyclone native public-API observer. Handles stay process-local;
  ownership samples do not establish atomicity or a receive incarnation.
- Cyclone instrumentation records per-worker XML, live process start identity,
  mapped library hashes, trace inode/file descriptor, closed raw hashes and exact
  line offsets. The parser separates participant SPDP discovery, post-CAS lease
  renewal, expiry and proxy-participant GC. Writer lease events stay separately
  scoped. Missing or invalid traces remain named unknowns.
- Receive incarnations are scoped to observing process and remote participant.
  Rediscovery creates a new receive incarnation while preserving the remote ROS
  endpoint generation. Retirement inversions are rejected, not called races.
- Public removal requires one identity-bound request/count/graph/ACK chain after
  expiry. Count acquisition is translated from its monotonic sample clock using
  paired worker emission clocks and a 50 ms guard; delayed emission cannot make a
  pre-expiry count fresh. Public samples remain discrete, not continuous.
- Primary recovery independently requires actual discovery mutation hit,
  restore-bound real discovery activity, fresh bilateral graph/count and
  post-restore delivery. The supporting report links those existing facts to
  internal rediscovery and observed public removal.

`blackbox` never enables vendor tracing. Its internal lifecycle facts are unknown;
wire and public recovery observations can still be available. Fast DDS has no
Cyclone native-ownership/lifecycle qualification through this optional observer.
Such gaps do not remove required cells or invent a participant fact from writer
QoS. A negative delivery omission remains a harness control excluded from natural
candidates.

## Limits and evidence preservation

Source references pin the Cyclone 0.10.5 logging statements and their file hashes;
installed-binary/source equivalence remains unknown. These observations do not
establish an internal race or reproduce historical OpenDDS #5202/#5221 or Cyclone
PR2292. Historical affected-version and reproduction obligations stay separate.

Configuration, path hit, semantic effect, primary verdict, candidate admission and
seal remain separate. Raw artifacts and failed attempts are immutable; fixes and
replay create new attempts. Source, HEAD and revision must stay fixed during each
run. Only the canonical boundary's completed subtree teardown plus independent
lease/resource readback permits the next attempt. A bounded successful cell is
not a complete required matrix or overall S1 maturity claim.
