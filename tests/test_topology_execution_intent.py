"""Offline execution-intent wiring; canonical callable bodies never run.

live_execution_go records an explicit execute intent only. These tests do not
provide owner live GO, source freeze, ROS/DDS evidence, leases or qualification.
"""
from copy import deepcopy
from functools import wraps
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from r2f.campaign import runner
from r2f.core.evidence import INPUT_SCHEMA_V2, canonical_hash, effective_input_hash
from r2f.modules.s1.providers import S1Carrier
from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET
from test_topology_cycle import SOURCE, fresh, profile
from test_topology_runtime_route import config_for

BOUNDARIES = (runner.HOST_RUNTIME, runner.ISOLATED_RUNTIME)
SEMANTIC_FIELDS = ("final_input", "effective_input", "effective_input_sha256",
                   "input_layer_receipts", "mutation_plan", "plan_hash")


class BeforeRuntime(BaseException):
    """Stop at the carrier boundary, before invoking any canonical runtime."""


def topology_config():
    _, config = config_for()
    config.update(source_manifest=deepcopy(SOURCE),
                  lease_receipts={"caller_assertion": "not proof"})
    return config


@pytest.mark.parametrize("boundary", BOUNDARIES)
@pytest.mark.parametrize("supplied", [None, False, True])
def test_canonical_topology_intent_is_execution_only_deepcopy(boundary, supplied):
    config = topology_config()
    if supplied is not None:
        config["live_execution_go"] = supplied
    before = deepcopy(config)
    before_hash = canonical_hash(config)
    result = runner._runtime_execution_config(
        config, execute=True, runtime=runner.runtime_for_boundary(boundary),
        runtime_boundary=boundary)
    assert result["live_execution_go"] is True
    assert result is not config and config == before
    assert canonical_hash(config) == before_hash
    for key in SEMANTIC_FIELDS:
        assert result[key] == before[key]
    assert effective_input_hash(result["effective_input"], schema=INPUT_SCHEMA_V2) == before["effective_input_sha256"]
    assert result["source_manifest"] == before["source_manifest"]
    assert result["lease_receipts"] == before["lease_receipts"]
    assert "live_execution_go" not in result["final_input"]
    assert "live_execution_go" not in result["effective_input"]
    result["final_input"]["topology_program"]["execution_intent_alias_probe"] = True
    result["source_manifest"]["source_frozen"] = False
    assert config == before  # nested aliases cannot change the semantic input
    if boundary == runner.ISOLATED_RUNTIME:
        assert result["execution_boundary"] == {"enabled": True}


@pytest.mark.parametrize("boundary", BOUNDARIES)
@pytest.mark.parametrize("execute", [False, None, 0, 1, "True"])
def test_plan_or_nonboolean_execute_scrubs_caller_flag(boundary, execute):
    config = topology_config()
    config["live_execution_go"] = True
    before = deepcopy(config)
    result = runner._runtime_execution_config(
        config, execute=execute, runtime=runner.runtime_for_boundary(boundary),
        runtime_boundary=boundary)
    assert "live_execution_go" not in result
    assert config == before and result is not config
    for key in SEMANTIC_FIELDS:
        assert result[key] == before[key]


def injected_runtime(kind, canonical, boundary):
    if kind == "none":
        return None
    if kind == "wrapper":
        @wraps(canonical)
        def wrapper(*args, **kwargs):
            raise AssertionError("wrapper must not run")
        return wrapper
    if kind == "opposite_boundary":
        other = runner.ISOLATED_RUNTIME if boundary == runner.HOST_RUNTIME else runner.HOST_RUNTIME
        return runner.runtime_for_boundary(other)
    if kind == "equality_spoof":
        class Spoof:
            __module__ = canonical.__module__
            __name__ = canonical.__name__

            def __call__(self, *args, **kwargs):
                raise AssertionError("spoof must not run")

            def __eq__(self, other):
                raise AssertionError("identity gate must not consult equality")
        return Spoof()
    return Mock(side_effect=AssertionError("injected runtime must not run"))


@pytest.mark.parametrize("boundary", BOUNDARIES)
@pytest.mark.parametrize("kind", ["none", "wrapper", "opposite_boundary", "equality_spoof", "mock"])
def test_noncanonical_runtime_cannot_preserve_caller_go(boundary, kind):
    config = topology_config()
    config["live_execution_go"] = True
    before = deepcopy(config)
    canonical = runner.runtime_for_boundary(boundary)
    result = runner._runtime_execution_config(
        config, execute=True, runtime=injected_runtime(kind, canonical, boundary),
        runtime_boundary=boundary)
    assert "live_execution_go" not in result
    assert config == before
    result["final_input"]["topology_program"]["probe"] = True
    assert config == before


@pytest.mark.parametrize("boundary", BOUNDARIES)
@pytest.mark.parametrize("execute", [False, True])
def test_non_topology_default_retains_boundary_bytes_and_host_alias(boundary, execute):
    config = {"target_id": "discovery.late_endpoint", "nested": {"unchanged": [1, 2]},
              "effective_input_sha256": "a" * 64}
    expected = runner._boundary_execution_config(config, boundary)
    result = runner._runtime_execution_config(
        config, execute=execute, runtime=runner.runtime_for_boundary(boundary),
        runtime_boundary=boundary)
    assert json.dumps(result, sort_keys=True) == json.dumps(expected, sort_keys=True)
    assert "live_execution_go" not in result
    if boundary == runner.HOST_RUNTIME:
        assert result is config


@pytest.mark.parametrize("boundary", BOUNDARIES)
@pytest.mark.parametrize("execute", [False, True])
def test_non_topology_cannot_carry_caller_forged_go(boundary, execute):
    config = {"target_id": "discovery.late_endpoint", "live_execution_go": True,
              "final_input": {"value": [1]}}
    before = deepcopy(config)
    result = runner._runtime_execution_config(
        config, execute=execute, runtime=runner.runtime_for_boundary(boundary),
        runtime_boundary=boundary)
    assert "live_execution_go" not in result and config == before


@pytest.mark.parametrize("boundary", BOUNDARIES)
def test_flag_does_not_change_source_lease_or_qualification_gates(boundary):
    config = topology_config()
    config["source_manifest"].update(source_frozen=False, dirty_behavior_paths=["src/dirty.py"])
    config["lease_receipts"] = None
    result = runner._runtime_execution_config(
        config, execute=True, runtime=runner.runtime_for_boundary(boundary),
        runtime_boundary=boundary)
    assert result["live_execution_go"] is True
    assert result["source_manifest"] == config["source_manifest"]
    assert result["lease_receipts"] is None
    if boundary == runner.ISOLATED_RUNTIME:
        qualification = runner.execution_boundary_qualification(boundary, config=result)
        assert qualification["status"] != "qualified"
        assert qualification["eligible"] is False


def test_semantic_caller_flag_is_not_rewritten_or_promoted():
    config = topology_config()
    config["final_input"]["live_execution_go"] = True
    config["live_execution_go"] = True
    before = deepcopy(config)
    result = runner._runtime_execution_config(config, execute=True, runtime=Mock())
    assert "live_execution_go" not in result
    assert result["final_input"] == before["final_input"]
    assert config == before


@pytest.mark.parametrize("options", ["profile prose", {"enabled": False}, {"enabled": True, "foreign": 1}])
def test_existing_boundary_options_validation_is_reused(options):
    config = topology_config()
    config["execution_boundary"] = deepcopy(options)
    before = deepcopy(config)
    if isinstance(options, str):
        result = runner._runtime_execution_config(
            config, execute=True, runtime=runner.runtime_for_boundary(runner.ISOLATED_RUNTIME),
            runtime_boundary=runner.ISOLATED_RUNTIME)
        assert result["execution_boundary"] == {"enabled": True}
    else:
        with pytest.raises(ValueError):
            runner._runtime_execution_config(
                config, execute=True, runtime=runner.runtime_for_boundary(runner.ISOLATED_RUNTIME),
                runtime_boundary=runner.ISOLATED_RUNTIME)
    assert config == before


def test_unknown_boundary_still_rejected():
    with pytest.raises(ValueError, match="unsupported runtime_boundary"):
        runner._runtime_execution_config({}, execute=True, runtime=Mock(), runtime_boundary="fixture")


def cycle(out, boundary, *, execute, runtime, source=SOURCE):
    doc = profile(1)
    with patch.object(runner, "source_manifest", return_value=deepcopy(source)), \
         patch("r2f.reports.run_report.source_manifest", return_value=deepcopy(source)):
        return runner.run_exploratory_cycle(
            out, **fresh(doc), profile=doc, max_inputs=1, runtime=runtime,
            execute=execute, run_id="topology-intent-offline", domain_id=80,
            runtime_boundary=boundary)


@pytest.mark.parametrize("boundary", BOUNDARIES)
def test_unified_canonical_config_intent_does_not_change_capsule_or_plan(tmp_path, boundary):
    runtime = runner.runtime_for_boundary(boundary)
    plan = cycle(tmp_path/"plan", boundary, execute=False, runtime=runtime)
    assert not plan["abort"] and len(plan["rows"]) == 1
    planned = plan["rows"][0]
    planned_config = json.loads(Path(planned["runtime_config_path"]).read_text())
    assert "live_execution_go" not in planned_config
    captured = []

    def intercept(self, scenario, prepared, *, execution):
        assert execution["runtime"] is runtime
        captured.append(deepcopy(execution["config"]))
        raise BeforeRuntime()

    with patch.object(S1Carrier, "apply", intercept), pytest.raises(BeforeRuntime):
        cycle(tmp_path/"execute", boundary, execute=True, runtime=runtime)
    assert len(captured) == 1 and captured[0]["live_execution_go"] is True
    submitted = captured[0]
    for key in SEMANTIC_FIELDS:
        assert submitted[key] == planned_config[key]
    config_path, = (tmp_path/"execute/cases").glob("*/runtime_config.json")
    assert json.loads(config_path.read_text()) == submitted
    capsule_path, = (tmp_path/"execute/cases").glob("*/capsule.json")
    assert capsule_path.read_bytes() == Path(planned["replay_path"]).read_bytes()
    plan_path, = (tmp_path/"execute/cases").glob("*/mutation_plan.json")
    assert json.loads(plan_path.read_text()) == planned["mutation_plan"]


def test_unified_injected_wrapper_has_no_execution_intent(tmp_path):
    canonical = runner.runtime_for_boundary(runner.HOST_RUNTIME)
    wrapper = injected_runtime("wrapper", canonical, runner.HOST_RUNTIME)
    seen = []

    def intercept(self, scenario, prepared, *, execution):
        assert execution["runtime"] is wrapper
        seen.append(deepcopy(execution["config"]))
        raise BeforeRuntime()

    with patch.object(S1Carrier, "apply", intercept), pytest.raises(BeforeRuntime):
        cycle(tmp_path/"injected", runner.HOST_RUNTIME, execute=True, runtime=wrapper)
    assert len(seen) == 1 and "live_execution_go" not in seen[0]


@pytest.mark.parametrize("boundary", BOUNDARIES)
def test_canonical_source_gate_remains_before_carrier(tmp_path, boundary):
    source = dict(SOURCE, source_frozen=False, dirty_behavior_paths=["src/dirty.py"])
    with patch.object(S1Carrier, "apply", side_effect=AssertionError("source gate must block")) as carrier:
        result = cycle(tmp_path/"dirty", boundary, execute=True,
                       runtime=runner.runtime_for_boundary(boundary), source=source)
    carrier.assert_not_called()
    assert result["abort"]["reason_code"] == "source_behavior_not_tracked_and_clean_at_HEAD"
    assert result["rows"] == [] and result["budget"]["attempted"] == 0


@pytest.mark.parametrize("boundary", BOUNDARIES)
def test_required_canonical_path_keeps_default_non_topology_config(tmp_path, boundary):
    canonical = runner.runtime_for_boundary(boundary)
    seen = []

    def intercept(self, scenario, prepared, *, execution):
        assert execution["runtime"] is canonical
        seen.append(deepcopy(execution["config"]))
        raise BeforeRuntime()

    with patch.object(runner, "source_manifest", return_value=deepcopy(SOURCE)), \
         patch.object(S1Carrier, "apply", intercept), pytest.raises(BeforeRuntime):
        runner.run_campaign(tmp_path/"required", execute=True, runtime=canonical,
                            runtime_boundary=boundary, run_id="required-intent-offline", domain_id=80)
    assert len(seen) == 1 and "live_execution_go" not in seen[0]
    if boundary == runner.ISOLATED_RUNTIME:
        assert seen[0]["execution_boundary"] == {"enabled": True}


def test_profile_boundary_budget_reaches_persisted_execution_copy(tmp_path):
    """A declared isolation budget is metadata; the capsule stays deterministic."""
    options = {"enabled": True, "timeout_sec": 600, "startup_timeout_sec": 40}
    doc = profile(1)
    plain = cycle(tmp_path / "plain", runner.ISOLATED_RUNTIME, execute=False,
                  runtime=runner.runtime_for_boundary(runner.ISOLATED_RUNTIME))
    doc["execution_boundary"] = deepcopy(options)
    with patch.object(runner, "source_manifest", return_value=deepcopy(SOURCE)), \
         patch("r2f.reports.run_report.source_manifest", return_value=deepcopy(SOURCE)):
        result = runner.run_exploratory_cycle(
            tmp_path / "budget", **fresh(doc), profile=doc, max_inputs=1,
            runtime=runner.runtime_for_boundary(runner.ISOLATED_RUNTIME), execute=False,
            run_id="topology-intent-offline", domain_id=80,
            runtime_boundary=runner.ISOLATED_RUNTIME)
    row, = result["rows"]
    config = json.loads(Path(row["runtime_config_path"]).read_text())
    assert config["execution_boundary"] == options
    assert Path(row["replay_path"]).read_bytes() == Path(plain["rows"][0]["replay_path"]).read_bytes()
    assert row["effective_input_sha256"] == plain["rows"][0]["effective_input_sha256"]
    assert doc["execution_boundary"] == options
