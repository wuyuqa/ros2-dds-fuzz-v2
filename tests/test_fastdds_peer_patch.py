"""Offline archive/patch/ABI-contract tests. Never compile, dlopen or start DDS.

C++ membership execution is intentionally NOT claimed by these tests. The build
owner must compile the real provider and verify it in isolated instrumentation.
"""
from __future__ import annotations

import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile

import pytest

from r2f.instrumentation import fastdds_peer
from r2f.instrumentation.fastdds_peer import patch_vendor as p

REPO = Path(__file__).resolve().parents[1]
ARCHIVE = REPO / "results/main-review/fastdds-0x80-source/source-v2.14.7.tar.gz"
PROVIDER = Path(p.__file__).with_name("provider.cpp")


@pytest.fixture(scope="module")
def originals():
    if not ARCHIVE.is_file():
        pytest.skip("official v2.14.7 source archive not available; archive-dependent tests not run")
    assert hashlib.sha256(ARCHIVE.read_bytes()).hexdigest() == p.REFERENCE_ARCHIVE_SHA256
    with tarfile.open(ARCHIVE) as archive:
        return {
            relative: archive.extractfile("Fast-DDS-2.14.7/" + relative).read()
            for relative in p.SOURCE_SHA256
        }


@pytest.fixture
def tree(tmp_path, originals):
    root = tmp_path / "isolated-source"
    for relative, data in originals.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return root


def snapshot(root):
    return {relative: (root / relative).read_bytes() for relative in p.SOURCE_SHA256}


def test_patch_exact_official_source_and_idempotence(tree, originals):
    original = p.patch_vendor(tree, check=True)
    assert original["source_state"] == "unpatched"
    assert snapshot(tree) == originals
    receipt = p.patch_vendor(tree)
    after = snapshot(tree)
    assert [rel for rel in originals if after[rel] != originals[rel]] == list(p.PATCH_ANCHORS)
    assert p.patch_vendor(tree) == receipt
    assert p.patch_vendor(tree, check=True) == receipt
    assert snapshot(tree) == after
    assert receipt["provider_sha256"] == p.sha256(PROVIDER.read_bytes())
    assert receipt["patcher_sha256"] == p.sha256(Path(p.__file__).read_bytes())
    assert receipt["verification"] == {
        "source_hashes": "verified", "cpp_compilation": "not_run",
        "native_behavior": "not_run", "dds": "not_run",
    }
    assert receipt["verification_scope"] == "listed_source_files_only_not_whole_tree"
    assert receipt["abi"]["snapshot_ordinal_basis"].endswith("not_match_epoch")
    for relative, info in receipt["files"].items():
        assert info["sha256"] == p.sha256(after[relative])
        assert info["original_sha256"] == p.SOURCE_SHA256[relative]


@pytest.mark.parametrize("relative", list(p.PATCH_ANCHORS))
def test_header_diff_only_forward_and_friend(originals, relative):
    source = originals[relative]
    patched = p.patch_bytes(relative, source)
    assert patched.count(p.FORWARD) == 1
    assert patched.count(p.FRIEND) == 1
    assert patched.replace(p.FORWARD, b"", 1).replace(p.FRIEND, b"", 1) == source
    assert p.patch_bytes(relative, patched) == patched
    assert len(patched) - len(source) == len(p.FORWARD) + len(p.FRIEND)
    assert b"virtual" not in p.FORWARD + p.FRIEND
    assert b"namespace r2f { class FastddsPeerAccess; }" in patched


@pytest.mark.parametrize("relative", list(p.SOURCE_SHA256))
def test_any_wrong_source_pin_rejects_before_any_write(tree, relative):
    path = tree / relative
    path.write_bytes(path.read_bytes() + b"\n// unknown vendor edit\n")
    before = snapshot(tree)
    with pytest.raises(p.PatchError, match="source_hash_mismatch"):
        p.patch_vendor(tree)
    assert snapshot(tree) == before


def test_wrong_version_is_rejected_before_patch(tree):
    path = tree / "CMakeLists.txt"
    before = path.read_bytes()
    assert b"2.14.7" in before
    path.write_bytes(before.replace(b"2.14.7", b"2.14.8"))
    source = snapshot(tree)
    with pytest.raises(p.PatchError, match="read_only_source_hash_mismatch:CMakeLists.txt"):
        p.patch_vendor(tree)
    assert snapshot(tree) == source


def test_partial_known_patch_is_resumable_not_unknown(tree):
    relative = next(iter(p.PATCH_ANCHORS))
    path = tree / relative
    path.write_bytes(p.patch_bytes(relative, path.read_bytes()))
    before = snapshot(tree)
    assert p.patch_vendor(tree, check=True)["source_state"] == "mixed"
    assert snapshot(tree) == before
    receipt = p.patch_vendor(tree)
    assert receipt["source_state"] == "patched"
    assert p.patch_vendor(tree) == receipt


@pytest.mark.parametrize("tamper", ["append", "duplicate", "move_forward", "move_friend", "strip_friend"])
def test_unknown_patched_state_rejected_before_write(tree, tamper):
    p.patch_vendor(tree)
    relative = next(iter(p.PATCH_ANCHORS))
    path = tree / relative
    data = path.read_bytes()
    if tamper == "append":
        data += b" "
    elif tamper == "duplicate":
        data += p.FRIEND
    elif tamper == "move_forward":
        data = data.replace(p.FORWARD, b"", 1) + p.FORWARD
    elif tamper == "move_friend":
        data = data.replace(p.FRIEND, b"", 1) + p.FRIEND
    else:
        data = data.replace(p.FRIEND, b"", 1)
    path.write_bytes(data)
    before = snapshot(tree)
    with pytest.raises(p.PatchError):
        p.patch_vendor(tree)
    assert snapshot(tree) == before


def test_unknown_anchor_is_not_guessed(originals, monkeypatch):
    relative = next(iter(p.PATCH_ANCHORS))
    monkeypatch.setitem(p.PATCH_ANCHORS, relative, "nonexistent class")
    with pytest.raises(p.PatchError, match="source_anchor_mismatch"):
        p.patch_bytes(relative, originals[relative])
    with pytest.raises(p.PatchError, match="not_a_patch_target"):
        p.patch_bytes("outside.hpp", b"")


def test_missing_source_rejected_before_write(tree):
    relative = list(p.SOURCE_SHA256)[-1]
    (tree / relative).unlink()
    before = {rel: (tree / rel).read_bytes() for rel in p.PATCH_ANCHORS}
    with pytest.raises(p.PatchError, match="missing_source_file"):
        p.patch_vendor(tree)
    assert {rel: (tree / rel).read_bytes() for rel in p.PATCH_ANCHORS} == before


@pytest.mark.parametrize("kind", ["file", "directory", "root", "hardlink"])
def test_linked_tree_not_modified(tree, tmp_path, kind):
    relative = next(iter(p.PATCH_ANCHORS))
    path = tree / relative
    outside = tmp_path / "outside"
    if kind in ("file", "hardlink"):
        path.rename(outside)
        if kind == "file":
            path.symlink_to(outside)
        else:
            os.link(outside, path)
    elif kind == "directory":
        directory = tree / "include"
        directory.rename(outside)
        directory.symlink_to(outside, target_is_directory=True)
    else:
        outside.symlink_to(tree, target_is_directory=True)
    before = path.read_bytes()
    with pytest.raises(p.PatchError, match="symlink|not_single_regular_file"):
        p.patch_vendor(outside if kind == "root" else tree)
    assert path.read_bytes() == before


def test_reject_installed_old_repo_or_git_checkout(tree):
    for root in ("/opt/ros/jazzy", "/usr", "/home/wuyuqa/ros2-dds-fuzz"):
        with pytest.raises(p.PatchError, match="forbidden_source_root|missing_source_root"):
            p.patch_vendor(root)
    (tree / ".git").mkdir()
    with pytest.raises(p.PatchError, match="git_checkout_is_not_isolated_archive_tree"):
        p.patch_vendor(tree)


def test_atomic_file_stage_failure_leaves_all_originals(tree, monkeypatch):
    before = snapshot(tree)
    real = p.tempfile.mkstemp
    calls = 0

    def fail_stage(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("synthetic_stage_failure")
        return real(*args, **kwargs)

    monkeypatch.setattr(p.tempfile, "mkstemp", fail_stage)
    with pytest.raises(OSError, match="synthetic_stage_failure"):
        p.patch_vendor(tree)
    assert snapshot(tree) == before
    assert not list(tree.rglob(".r2f-peer-*"))


def test_interrupted_replace_is_a_known_resumable_state(tree, monkeypatch):
    real = p.os.replace
    calls = 0

    def fail_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic_replace_interruption")
        return real(*args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr(p.os, "replace", fail_second)
        with pytest.raises(OSError, match="synthetic_replace_interruption"):
            p.patch_vendor(tree)
    assert not list(tree.rglob(".r2f-peer-*"))
    assert p.patch_vendor(tree, check=True)["source_state"] == "mixed"
    assert p.patch_vendor(tree)["source_state"] == "patched"


def test_file_permissions_preserved(tree):
    relative = next(iter(p.PATCH_ANCHORS))
    path = tree / relative
    path.chmod(0o640)
    p.patch_vendor(tree)
    assert path.stat().st_mode & 0o777 == 0o640


def test_cli_receipt_and_explicit_error_without_dds(tree, tmp_path, capsys):
    receipt_path = tmp_path / "receipt.json"
    assert p.main(["--source-root", str(tree), "--manifest", str(receipt_path)]) == 0
    output = capsys.readouterr()
    receipt = json.loads(output.out)
    assert receipt == json.loads(receipt_path.read_text())
    assert receipt["source_state"] == "patched"
    assert p.main(["--source-root", str(tree), "--manifest", str(receipt_path)]) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "manifest_already_exists_preserve_prior_receipt"
    assert p.main(["--source-root", str(tree), "--check"]) == 0
    assert json.loads(capsys.readouterr().out) == receipt


def test_manifest_cannot_replace_source(tree, capsys):
    before = snapshot(tree)
    assert p.main(["--source-root", str(tree), "--manifest", str(tree / "CMakeLists.txt")]) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "manifest_must_be_outside_source_tree"
    assert snapshot(tree) == before


def test_import_and_help_do_not_load_native_or_spawn_build():
    # Executes the real module CLI with native loaders AND process starters
    # disabled; this is not a substitute for compiling/executing provider.cpp.
    code = """
import ctypes, subprocess, runpy, sys

def forbidden(*a, **kw):
    raise AssertionError('native_load_or_process_start')
ctypes.CDLL = ctypes.PyDLL = forbidden
subprocess.Popen = forbidden
sys.argv = ['patch_vendor', '--help']
runpy.run_module('r2f.instrumentation.fastdds_peer.patch_vendor', run_name='__main__')
"""
    result = subprocess.run([sys.executable, "-c", code], text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert "--source-root" in result.stdout


def test_abi_layout_fixed_width_matches_cpp_assertions():
    class Result(C.Structure):
        _fields_ = [
            ("schema_version", C.c_uint32), ("struct_size", C.c_uint32),
            ("status", C.c_int32), ("endpoint_kind", C.c_uint32),
            ("local_guid", C.c_uint8 * 16), ("required_count", C.c_uint64),
            ("copied", C.c_uint64), ("snapshot_ordinal", C.c_uint64),
            ("complete", C.c_uint32), ("reserved", C.c_uint32),
        ]
    assert C.sizeof(Result) == fastdds_peer.RESULT_SIZE == 64
    assert fastdds_peer.GUID_SIZE == 16
    expected = dict(schema_version=0, struct_size=4, status=8, endpoint_kind=12,
                    local_guid=16, required_count=32, copied=40, snapshot_ordinal=48,
                    complete=56, reserved=60)
    source = PROVIDER.read_text()
    for name, offset in expected.items():
        assert getattr(Result, name).offset == offset
        assert f"offsetof(r2f_fastdds_peer_result_v1, {name}) == {offset}" in source


def test_provider_static_read_only_membership_and_exception_contract(originals):
    source = PROVIDER.read_text()
    writer = source.split("static int32_t writer(", 1)[1].split("static int32_t reader(", 1)[0]
    reader = source.split("static int32_t reader(", 1)[1].split("namespace {", 1)[0]
    for part in (writer, reader):
        assert part.count("guard(native->getMutex())") == 1
        assert "dynamic_cast<rtps::Stateful" in part
        assert "native->getAttributes().reliabilityKind != rtps::RELIABLE" in part
        assert "return UNSUPPORTED;" in part
        assert "return NOT_ENABLED;" in part
        assert "return finish(peers, buffer, capacity, result);" in part
    for container in ("matched_local_readers_", "matched_datasharing_readers_", "matched_remote_readers_"):
        assert f"native->{container}" in writer
    assert "proxy->is_alive()" not in writer
    assert "if (native->is_alive_)" in reader
    assert "if (proxy->is_alive())" in reader
    assert "matched_writers_pool_" not in reader
    assert "native->matched_writers_" in reader
    assert "catch (...)" in source and "result.status = INTERNAL_ERROR;" in source
    assert "if (out_bytes != sizeof(Result))" in source
    assert source.index("if (capacity < result.required_count)") < source.index("std::memcpy(buffer +")
    assert "result.copied = result.required_count;" in source
    assert "result.complete = 1;" in source
    assert "result.snapshot_ordinal = ordinal;" in source
    assert "std::sort(peers.begin(), peers.end())" in source
    for forbidden in ("reinterpret_cast", "set_listener(", "matched_reader_add(", "matched_writer_add(",
                      "matched_reader_remove(", "matched_writer_remove(", "get_publication_matched_status(",
                      "get_subscription_matched_status(", "->write(", "getMatchedReadersSize(", "getMatchedWritersSize("):
        assert forbidden not in source
    # Real frozen source predicate, not a prior/assumed API contract.
    w = originals["src/cpp/rtps/writer/StatefulWriter.cpp"].decode()
    w = w.split("bool StatefulWriter::matched_reader_is_matched(", 1)[1].split("bool StatefulWriter::", 1)[0]
    r = originals["src/cpp/rtps/reader/StatefulReader.cpp"].decode()
    r = r.split("bool StatefulReader::matched_writer_is_matched(", 1)[1].split("bool StatefulReader::", 1)[0]
    assert "reader->guid() == reader_guid" in w
    assert "matched_local_readers_, matched_datasharing_readers_, matched_remote_readers_" in w
    assert "if (is_alive_)" in r and "it->guid() == writer_guid && it->is_alive()" in r
    assert "guard(mp_mutex)" in w and "guard(mp_mutex)" in r
