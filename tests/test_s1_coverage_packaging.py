"""Exercise lineage queries from a wheel without a checkout on the import path."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

import pytest


_QUERY = """
import json
from pathlib import Path
import sys

site = Path(sys.argv[1]).resolve()
checkout = Path(sys.argv[2]).resolve()
sys.path.insert(0, str(site))
assert not any(Path(p).resolve().is_relative_to(checkout) for p in sys.path)

import r2f
from r2f.core import Registry
from r2f.modules.s1 import catalog
from r2f.modules.s1.module import register

assert Path(r2f.__file__).resolve().is_relative_to(site)
registry = Registry()
register(registry)
rule = registry.get_rule_lineage(
    "discovery.late_endpoint", "discovery.late_endpoint.match_and_receive_after_join")
coverage = catalog.coverage_lineage()
assert rule["coverage_lineage"]["source_captures"] == coverage["source_captures"]
assert rule["coverage_lineage"]["model_binding"]["target_id"] == "discovery.late_endpoint"
seed_lineage = coverage["seed_lineage"]
linked_seed_ids = sorted({seed["seed_id"] for binding in coverage["model_bindings"].values()
                          for seed in binding["seed_refs"]})
p02 = next(seed for seed in coverage["model_bindings"][
    "builtin.discovery_projection_reconciliation.fuzz_v1"]["seed_refs"]
    if seed["seed_id"] == "P02")
assert p02["content_sha256"] == seed_lineage["seeds"]["P02"]["content_sha256"]
assert p02["source_file"] == seed_lineage["seeds"]["P02"]["source_file"]
assert p02["metadata_source"] == "captured_snapshot"
assert p02["current_loadability"] == seed_lineage["current_loadability"]
assert p02["capability"] == seed_lineage["seeds"]["P02"]["capability"]
assert all(binding["declaration_status"] == "model_input_only"
           for binding in coverage["model_bindings"].values())
assert all(source["runtime_qualification"] == "unverified"
           for binding in coverage["model_bindings"].values()
           for source in binding["seed_sources"])
print(json.dumps({
    "rule_id": rule["rule_id"],
    "schema": coverage["schema"],
    "counts": [len(coverage["cases"]), len(coverage["obligations"]),
               len(coverage["user_stories"]), coverage["required_baseline"]["cell_count"]],
    "execution_evidence": rule["execution_evidence"],
    "maturity_evidence": rule["maturity_evidence"],
    "seed_metadata": {
        "capture_kind": seed_lineage["capture"]["kind"],
        "capture_head": seed_lineage["capture"]["source_head"],
        "captured_seed_count": len(seed_lineage["seeds"]),
        "captured_statuses": {sid: seed["status"] for sid, seed in seed_lineage["seeds"].items()},
        "reference_source": seed_lineage["reference_source"],
        "current_loadability": seed_lineage["current_loadability"],
        "model_binding_count": len(coverage["model_bindings"]),
        "linked_seed_ids": linked_seed_ids,
        "w01_target_ids": seed_lineage["seeds"]["W01"]["context"]["model_target_ids"],
        "p02_content_sha256": p02["content_sha256"],
        "p02_file_sha256": p02["source_file"]["file_sha256"],
        "qualified_for_current_source": seed_lineage["qualified_for_current_source"],
        "execution_evidence": seed_lineage["execution_evidence"],
        "maturity_evidence": seed_lineage["maturity_evidence"],
    },
}))
"""


def _query_env():
    env = {key: value for key, value in os.environ.items()
           if key not in {"PYTHONPATH", "PYTHONHOME"}}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


@pytest.fixture(scope="module")
def built_wheel(tmp_path_factory):
    checkout = Path(__file__).resolve().parents[1]
    tmp_path = tmp_path_factory.mktemp("built-wheel")
    assert not tmp_path.resolve().is_relative_to(checkout)
    build = tmp_path / "wheel-input"
    build.mkdir()
    shutil.copy2(checkout / "pyproject.toml", build / "pyproject.toml")
    shutil.copytree(checkout / "src", build / "src",
                    ignore=shutil.ignore_patterns("__pycache__", "*.egg-info"))
    shutil.copytree(checkout / "config", build / "config")
    wheels = tmp_path / "wheels"
    command = [sys.executable, "-I", "-m", "pip", "wheel", "--no-index", "--no-deps",
               "--no-build-isolation", "--no-cache-dir", "--wheel-dir", str(wheels), str(build)]
    result = subprocess.run(command, cwd=tmp_path, env=_query_env(), capture_output=True, text=True)
    (tmp_path / "wheel-build.log").write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    wheel, = wheels.glob("*.whl")
    # Remove build inputs: public queries must use only the distributed artifact.
    shutil.rmtree(build)
    return wheel


def test_wheel_public_lineage_queries_work_outside_checkout(tmp_path, built_wheel):
    checkout = Path(__file__).resolve().parents[1]
    site = tmp_path / "wheel-site"
    with zipfile.ZipFile(built_wheel) as archive:
        archive.extractall(site)
    query = subprocess.run([sys.executable, "-I", "-c", _QUERY, str(site), str(checkout)],
                           cwd=tmp_path, env=_query_env(), capture_output=True, text=True)
    (tmp_path / "wheel-query.json").write_text(json.dumps({
        "returncode": query.returncode, "stdout": query.stdout, "stderr": query.stderr,
    }, indent=2) + "\n")
    assert query.returncode == 0, query.stdout + query.stderr
    assert json.loads(query.stdout) == {
        "rule_id": "discovery.late_endpoint.match_and_receive_after_join",
        "schema": "r2f.s1-coverage-lineage/v1",
        "counts": [15, 18, 42, 72],
        "execution_evidence": False,
        "maturity_evidence": False,
        "seed_metadata": {
            "capture_kind": "new_seed_metadata_snapshot",
            "capture_head": "62d0b716a8ae6a2881c6b766b370cb6546b27d3d",
            "captured_seed_count": 12,
            "captured_statuses": {
                "P01": "authored", "P02": "authored", "P03": "authored",
                "P04": "authored", "P05": "authored", "P06": "authored",
                "E01": "authored", "E02": "capability_pending", "E03": "capability_pending",
                "G01": "capability_pending", "W01": "authored", "W02": "authored",
            },
            "reference_source": "captured_snapshot",
            "current_loadability": {"status": "unknown", "reason": "original_seed_corpus_absent"},
            "model_binding_count": 11,
            "linked_seed_ids": ["E01", "E02", "E03", "G01", "P01", "P02", "P03", "P04", "P05", "P06", "W02"],
            "w01_target_ids": [],
            "p02_content_sha256": "280b74dd7f0569035c598a659b825a562de29b3a18e1546b329d97138bcf85a1",
            "p02_file_sha256": "0123415ea37b0b75bad88bab7e3f4bae4a0b257a44aaa97b5523e30163ae13be",
            "qualified_for_current_source": False,
            "execution_evidence": False,
            "maturity_evidence": False,
        },
    }


_FAILURE_QUERY = """
import json
from pathlib import Path
import sys

site = Path(sys.argv[1]).resolve()
checkout = Path(sys.argv[2]).resolve()
sys.path.insert(0, str(site))
assert not any(Path(p).resolve().is_relative_to(checkout) for p in sys.path)
from r2f.core import Registry
from r2f.modules.s1 import catalog
from r2f.modules.s1.fuzz_catalog import CatalogError
from r2f.modules.s1.module import register
assert Path(catalog.__file__).resolve().is_relative_to(site)
registry = Registry()
register(registry)
results = {}
for name, query in {
    "registry": lambda: registry.get_rule_lineage(
        "discovery.late_endpoint", "discovery.late_endpoint.match_and_receive_after_join"),
    "catalog": catalog.coverage_lineage,
}.items():
    try:
        query()
    except CatalogError as exc:
        results[name] = {"exception": type(exc).__name__, "message": str(exc)}
    else:
        results[name] = {"unexpected_success": True}
print(json.dumps(results))
"""


@pytest.mark.parametrize("damage", ["empty", "missing_directory", "missing_file", "invalid_json"])
def test_present_seed_corpus_errors_stay_strict_through_wheel_queries(tmp_path, built_wheel, damage):
    checkout = Path(__file__).resolve().parents[1]
    site = tmp_path / "wheel-site"
    with zipfile.ZipFile(built_wheel) as archive:
        archive.extractall(site)
    corpus = tmp_path / "config/s1_fuzz/seeds"
    if damage == "missing_directory":
        corpus.parent.mkdir(parents=True)
    elif damage == "empty":
        corpus.mkdir(parents=True)
    else:
        shutil.copytree(checkout / "config/s1_fuzz/seeds", corpus)
        p02 = corpus / "p02.json"
        if damage == "missing_file":
            p02.unlink()
        else:
            p02.write_text("{invalid JSON")
    query = subprocess.run([sys.executable, "-I", "-c", _FAILURE_QUERY, str(site), str(checkout)],
                           cwd=tmp_path, env=_query_env(), capture_output=True, text=True)
    (tmp_path / "strict-query.json").write_text(json.dumps({
        "returncode": query.returncode, "stdout": query.stdout, "stderr": query.stderr,
    }, indent=2) + "\n")
    assert query.returncode == 0, query.stdout + query.stderr
    results = json.loads(query.stdout)
    assert set(results) == {"registry", "catalog"}
    for result in results.values():
        assert result["exception"] == "CatalogError"
        assert "seed corpus invalid:" in result["message"]
        assert "P02:" in result["message"]
        if damage != "invalid_json":
            assert "P02: authored seed file missing" in result["message"]


def test_wheel_preserves_original_coverage_capture_bytes(built_wheel):
    with zipfile.ZipFile(built_wheel) as archive:
        data = archive.read("r2f/modules/s1/s1_coverage.json")
        assert "r2f/modules/s1/seed_lineage.json" in archive.namelist()
    assert hashlib.sha256(data).hexdigest() == (
        "5f886f6a4e7d47e2ecb975e034c1d386f22fe82b06aeb4c7a66cd75fefbb7106")
