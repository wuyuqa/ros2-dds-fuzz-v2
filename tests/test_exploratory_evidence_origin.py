"""Offline origin-source tests; canonical identities are mocks, never invoked live."""
from copy import deepcopy
import pytest
from r2f.campaign import runner
from r2f.campaign.replay import DEFAULT_RUNTIME, ISOLATED_RUNTIME, _qualification


def host(*args):
    raise AssertionError("never execute host")


def isolated(*args):
    raise AssertionError("never execute isolated")


@pytest.fixture
def identities(monkeypatch):
    monkeypatch.setattr(runner, "runtime_for_boundary",
                        lambda boundary: {"host": host, "isolated_netns": isolated}[boundary])


@pytest.mark.parametrize("claim", [None, "", "injected_runtime", DEFAULT_RUNTIME,
                                    ISOLATED_RUNTIME, "ros2_public_api", "invented", {}, []])
@pytest.mark.parametrize("flag", [True, False])
def test_unknown_cannot_self_attest(identities, claim, flag):
    payload = {"evidence_origin": claim, "real_execution": flag}
    before = deepcopy(payload)
    assert runner._evidence_origin(payload, runtime=lambda *args: None) == "injected_runtime"
    assert payload == before


@pytest.mark.parametrize("boundary, runtime, expected", [
    ("host", host, DEFAULT_RUNTIME),
    ("isolated_netns", isolated, ISOLATED_RUNTIME),
    ("host", isolated, "injected_runtime"),
    ("isolated_netns", host, "injected_runtime"),
])
def test_exact_boundary_identity_not_return_value(identities, boundary, runtime, expected):
    for result in ({}, {"real_execution": True, "evidence_origin": "injected_runtime"}):
        assert runner._evidence_origin(result, runtime=runtime, runtime_boundary=boundary) == expected


@pytest.mark.parametrize("label", ["fixture", "synthetic", "offline", "offline_fixture"])
@pytest.mark.parametrize("runtime", [None, host, isolated])
def test_explicit_offline_never_upgraded(identities, label, runtime):
    assert runner._evidence_origin({"evidence_origin": label, "real_execution": True},
                                   runtime=runtime) == "offline_fixture"


def test_names_wrappers_and_equality_do_not_attest(identities):
    class Impostor:
        __name__ = "run_pair"
        __module__ = "r2f.runtime.ros2_pair"
        def __call__(self, *args):
            raise AssertionError("not called")
        def __eq__(self, other):
            return True
    for runtime in (Impostor(), lambda *args: host(*args)):
        assert runner._evidence_origin({"real_execution": True,
                "evidence_origin": DEFAULT_RUNTIME}, runtime=runtime) == "injected_runtime"


@pytest.mark.parametrize("boundary, runtime, expected", [
    ("host", host, DEFAULT_RUNTIME), ("isolated_netns", isolated, ISOLATED_RUNTIME)])
def test_canonical_tool_error_is_source_only_not_real(identities, tmp_path, boundary, runtime, expected):
    result = {"runtime_status": "tool_error", "real_execution": False,
              "reason": "mock canonical failed", "evidence_origin": "injected_runtime"}
    row = {"actual_run": False, "execution_stage": "aborted", "candidate_eligible": False}
    original = deepcopy((result, row))
    source = runner._evidence_origin(result, runtime=runtime, runtime_boundary=boundary)
    assert source == expected  # describes selected callable, NOT qualified execution
    qualified = _qualification(source, {}, result, row, output_root=tmp_path)
    assert qualified["real_ros_execution"] is False
    assert (result, row) == original


def test_fixture_callsite_preserves_input_identity_and_qualification(tmp_path, monkeypatch):
    from test_exploratory_checkpoint import run_cycle, profile
    from test_feedback_epoch_runner import adapter_runtime
    def injected(config, output):
        result = adapter_runtime(config, output)
        result.pop("evidence_origin", None)
        return result
    unknown = run_cycle(tmp_path / "unknown", profile(), maximum=1, runtime=injected)
    monkeypatch.setattr(runner, "runtime_for_boundary", lambda boundary: injected)
    canonical = run_cycle(tmp_path / "canonical", profile(), maximum=1, runtime=injected)
    a, b = unknown["rows"][0], canonical["rows"][0]
    assert a["evidence_origin"] == "injected_runtime"
    assert b["evidence_origin"] == DEFAULT_RUNTIME
    for key in ("effective_input_sha256", "content_sha256", "mutation_plan_sha256", "mutation_plan"):
        assert a[key] == b[key]
    # Source selection alone must not make the mocked run canonical-qualified.
    from r2f.reports.run_report import audit_exploratory_report
    import json
    from pathlib import Path
    report = json.loads(Path(canonical["report_path"]).read_text())
    proofs = report["artifact_qualifications"]
    assert proofs and all(not item["qualification"]["real_ros_execution"] for item in proofs.values())
    assert audit_exploratory_report(canonical["report_path"])["evidence_valid"] is False


def test_toolerror_callsite_keeps_failure_not_sut(tmp_path, monkeypatch):
    from test_f1_exploratory_report import plan
    def failing(config, output):
        raise RuntimeError("mock canonical failure")
    monkeypatch.setattr(runner, "runtime_for_boundary", lambda boundary: failing)
    cycle, report = plan(tmp_path, runtime=failing)
    row = cycle["rows"][0]
    assert row["evidence_origin"] == DEFAULT_RUNTIME
    assert row["actual_run"] is False
    assert row.get("candidate_status") in (None, "not_candidate")
    assert report["artifact_qualifications"][row["cell_id"]]["qualification"]["real_ros_execution"] is False


@pytest.mark.parametrize('boundary, expected', [('host', DEFAULT_RUNTIME),
                         ('isolated_netns', ISOLATED_RUNTIME)])
def test_real_canonical_identity_resolution_without_execution(boundary, expected):
    runtime = runner.runtime_for_boundary(boundary)
    assert runner._evidence_origin({}, runtime=runtime, runtime_boundary=boundary) == expected


def test_bfs_frontier_receives_row_source_not_result_claim(tmp_path, monkeypatch):
    from test_model_search_frontier import profile, fresh
    from test_exploratory_checkpoint import SOURCE
    from test_feedback_epoch_runner import adapter_runtime
    from pathlib import Path
    import json
    def canonical_mock(config, output):
        result = adapter_runtime(config, output)
        result.pop('evidence_origin', None)
        return result
    doc = profile()
    doc['search'].update(max_depth=1, samples_per_class=1)
    monkeypatch.setattr(runner, 'source_manifest', lambda repo: deepcopy(SOURCE))
    monkeypatch.setattr(runner, 'runtime_for_boundary', lambda boundary: canonical_mock)
    state = fresh(doc)
    cycle = runner.run_exploratory_cycle(tmp_path, **state, profile=doc,
                execute=True, runtime=canonical_mock, max_inputs=2, run_id='origin-bfs-mock')
    rows = {r['cell_id']: r for r in cycle['rows'] if r.get('actual_run') is True}
    assert rows  # non-vacuous row -> saved frontier projection
    executions = [t['execution'] for t in state['scheduler'].frontier.tasks if t.get('execution')]
    assert len(executions) == len(rows)
    for execution in executions:
        row = rows[execution['cell_id']]
        assert execution['evidence_origin'] == row['evidence_origin'] == DEFAULT_RUNTIME
        assert execution['formal_runtime'] is True
    report = json.loads(Path(cycle['report_path']).read_text())
    assert all(not p['qualification']['real_ros_execution']
               for p in report['artifact_qualifications'].values())
