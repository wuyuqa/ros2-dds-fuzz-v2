"""Strict, resumable friend-only patch for an isolated Fast DDS v2.14.7 tree.

Never extracts archives, compiles, installs, loads native libraries or starts DDS.
Only six headers are changed. All known input states are preflighted before any
write; an interrupted apply may be resumed (original/patched mixtures allowed).
Replacements are atomic per file, not a six-file filesystem transaction. Do not
modify/build the source concurrently with this command. Verification pins the
listed source files, not an uninspected complete source tree; the build owner
must retain the original archive and complete source/build/library manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import tempfile

VENDOR_VERSION = "2.14.7"
PATCH_ID = "r2f-fastdds-peer-friend-v1"
FORWARD = b"// R2F peer snapshot v1: friend-only, no layout/vtable changes.\nnamespace r2f { class FastddsPeerAccess; }\n\n"
FRIEND = b"\n    friend class ::r2f::FastddsPeerAccess; // R2F peer snapshot v1"

REFERENCE_ARCHIVE_SHA256 = '2bf599eb65ad5daf5b322af28e766d08e2ef436f4a242ffd7e2a34145d43c0de'
PATCH_ANCHORS = {'include/fastdds/dds/publisher/DataWriter.hpp': 'class DataWriter : public DomainEntity\n{', 'include/fastdds/dds/subscriber/DataReader.hpp': 'class DataReader : public DomainEntity\n{', 'src/cpp/fastdds/publisher/DataWriterImpl.hpp': 'class DataWriterImpl : protected rtps::IReaderDataFilter\n{', 'src/cpp/fastdds/subscriber/DataReaderImpl.hpp': 'class DataReaderImpl\n{', 'include/fastdds/rtps/writer/StatefulWriter.h': 'class StatefulWriter : public RTPSWriter\n{', 'include/fastdds/rtps/reader/StatefulReader.h': 'class StatefulReader : public RTPSReader\n{'}

SOURCE_SHA256 = {'include/fastdds/dds/publisher/DataWriter.hpp': 'af7f72b8ff6e7c2d8b408a0678dffe2ae911cbcd7973049fc4eaea19606a695f', 'include/fastdds/dds/subscriber/DataReader.hpp': '7d95fc6513d96ebe3e8bed11d71918a468ba5dfc3550b8c3b70e1a5569895c0f', 'src/cpp/fastdds/publisher/DataWriterImpl.hpp': '025225315fbd099203c262b20c05dcffca3e7905c3e718a3d0c24c5203b38dbf', 'src/cpp/fastdds/subscriber/DataReaderImpl.hpp': '8577b8b2b23a76a7c18635160443ce5034f44e8f91666df109a84cae733f23f0', 'include/fastdds/rtps/writer/StatefulWriter.h': 'e17e6c4b185a0ec4b5dd6512d77847a16d735fc3ac6aadb192be7dd9861b8877', 'include/fastdds/rtps/reader/StatefulReader.h': '015f2048fcd656d9843d0d6dcd510eabf139ede70c168d38425d2b50029ec255', 'CMakeLists.txt': '7290a0441aa2fc70d43d3767617cf9c8c855d52d5b971e3217a4c3b57c0d8475', 'src/cpp/rtps/writer/StatefulWriter.cpp': '6c34d5f74c64d640e13ba5495628c5d95cce5f2413bef4d7c8a0d9eff5f0efe2', 'src/cpp/rtps/reader/StatefulReader.cpp': 'b74f948da413cc413497224ac6ad2b1ec521daf96757a87f5fa98eafbce00520', 'include/fastdds/rtps/writer/ReaderProxy.h': 'f889357fdfaa840fcb334b0bae6cb2558ed9273180b28361b10ad244fd434567', 'src/cpp/rtps/reader/WriterProxy.h': '9930637581622873273689080ae765f0b85a9f89de0c4fc3bff8e230309bdef3', 'include/fastdds/rtps/writer/RTPSWriter.h': '4052b739a2ad9eab0c767cb855dc7e5762069e87053fbdf7f8b73e38c3008fe2', 'include/fastdds/rtps/reader/RTPSReader.h': '0efdc77c1f1b5e5c0c507212317cd9f0d08d9cb8206d558b12bc47f4b368615c', 'include/fastdds/rtps/Endpoint.h': '9166e78521be3444f54fa096fae47c0c1b1f24633adf77d666cb24d5bf706fb4', 'include/fastdds/rtps/attributes/EndpointAttributes.h': '54669958ffcdb07b6341f4ffe47dfea1c53aa6864a48e849bc90d17bf64ed002', 'include/fastdds/rtps/common/Types.h': '12633aaf5b934122fc29915715b3e559e656ed7541c9ac0bd104296b9be66684', 'include/fastdds/rtps/common/Guid.h': '446301ca61edd6b3f6340cbc43b1c56e8b9c63ca9984977851c904d319de98be', 'include/fastdds/rtps/common/GuidPrefix_t.hpp': 'de44e0e7b6952340268cb50505843427660f3a75a91ef0a7f8a795e08c110dab', 'include/fastdds/rtps/common/EntityId_t.hpp': 'ef55cc9a0c56903b6ad2e19414e2bd472f6b119e0fb7c06fff0ce883f1d43ad0', 'include/fastrtps/utils/TimedMutex.hpp': '5be8c4a7dd91b4113ca737eb3d9e8f585dac37b470a9fbedea1fe4f51eff64d7', 'include/fastrtps/utils/collections/ResourceLimitedVector.hpp': '7561ed4cf82be3f2ee25fb4a7e09a3f7b13deb1eb8ecdf7db3258dd8f9703b57'}


class PatchError(ValueError):
    """Unsupported source state; never guess an ABI or an insertion anchor."""


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def patch_bytes(relative: str, original: bytes) -> bytes:
    """Accept ONLY exact original or our deterministic exact patched bytes."""
    if relative not in PATCH_ANCHORS:
        raise PatchError(f"not_a_patch_target:{relative}")
    expected = SOURCE_SHA256[relative]
    if sha256(original) != expected:
        # Reverse only our exact markers, then check the complete baseline.
        if original.count(FORWARD) != 1 or original.count(FRIEND) != 1:
            raise PatchError(f"source_hash_mismatch:{relative}")
        unpatched = original.replace(FORWARD, b"", 1).replace(FRIEND, b"", 1)
        if sha256(unpatched) != expected:
            raise PatchError(f"patched_source_hash_mismatch:{relative}")
        if patch_bytes(relative, unpatched) != original:
            raise PatchError(f"patch_marker_position_mismatch:{relative}")
        return original
    anchor = PATCH_ANCHORS[relative].encode("ascii")
    namespace = b"namespace eprosima {"
    if original.count(anchor) != 1 or original.count(namespace) != 1:
        raise PatchError(f"source_anchor_mismatch:{relative}")
    return original.replace(namespace, FORWARD + namespace, 1).replace(anchor, anchor + FRIEND, 1)


def _root_path(source_root: str | Path) -> Path:
    root = Path(os.path.abspath(source_root))
    if root.resolve() != root:
        raise PatchError(f"symlink_source_root:{root}")
    if not root.is_dir():
        raise PatchError(f"missing_source_root:{root}")
    # The tool is explicitly for a separate, unpacked source tree, never for
    # installed headers or either working repository.
    forbidden = [Path("/opt"), Path("/usr"), Path("/home/wuyuqa/ros2-dds-fuzz")]
    if any(root == p or p in root.parents for p in forbidden):
        raise PatchError(f"forbidden_source_root:{root}")
    if (root / ".git").exists() or (root / ".git").is_symlink():
        raise PatchError(f"git_checkout_is_not_isolated_archive_tree:{root}")
    return root


def _regular_path(root: Path, relative: str) -> Path:
    path = root
    for component in Path(relative).parts:
        path = path / component
        if path.is_symlink():
            raise PatchError(f"symlink_source_path:{relative}")
    try:
        mode = path.stat()
    except FileNotFoundError as exc:
        raise PatchError(f"missing_source_file:{relative}") from exc
    if not stat.S_ISREG(mode.st_mode) or mode.st_nlink != 1:
        raise PatchError(f"not_single_regular_file:{relative}")
    return path


def patch_vendor(source_root: str | Path, *, check: bool = False) -> dict:
    """Preflight all pins, apply exact friends, return JSON-serializable receipt.

    ``check=True`` is strictly read-only, reports patched/unpatched/mixed.
    Call without check on a clean or interrupted known tree. The successful
    manifest is stable across repeat applications; no timestamp masquerades as
    source identity. Native compilation and membership behavior remain UNTESTED.
    """
    root = _root_path(source_root)
    changes: dict[str, tuple[Path, bytes, bytes]] = {}
    files = {}
    for relative, expected in SOURCE_SHA256.items():
        path = _regular_path(root, relative)
        original = path.read_bytes()
        current_hash = sha256(original)
        if relative in PATCH_ANCHORS:
            patched = patch_bytes(relative, original)
            if patched != original:
                changes[relative] = (path, original, patched)
            files[relative] = {
                "original_sha256": expected,
                "patched_sha256": sha256(patched),
                "sha256": current_hash if check else sha256(patched),
                "patch_target": True,
            }
        else:
            if current_hash != expected:
                raise PatchError(f"read_only_source_hash_mismatch:{relative}")
            files[relative] = {
                "original_sha256": expected, "sha256": current_hash,
                "patch_target": False,
            }
    if not check:
        # Stage every output before replacing any input. Reject known conflicts
        # before commit. No blind rollback can overwrite another owner's edits.
        staged: list[tuple[Path, Path, bytes]] = []
        try:
            for path, original, patched in changes.values():
                fd, name = tempfile.mkstemp(prefix=".r2f-peer-", dir=path.parent)
                temp = Path(name)
                staged.append((path, temp, original))
                with os.fdopen(fd, "wb") as stream:
                    stream.write(patched)
                    stream.flush()
                    os.fsync(stream.fileno())
                    os.fchmod(stream.fileno(), stat.S_IMODE(path.stat().st_mode))
            for path, _, original in staged:
                _regular_path(root, str(path.relative_to(root)))
                if path.read_bytes() != original:
                    raise PatchError(f"concurrent_source_change:{path.relative_to(root)}")
            for path, temp, original in staged:
                _regular_path(root, str(path.relative_to(root)))
                if path.read_bytes() != original:
                    raise PatchError(f"concurrent_source_change:{path.relative_to(root)}")
                os.replace(temp, path)
        finally:
            for _, temp, _ in staged:
                temp.unlink(missing_ok=True)
    if not check or not changes:
        state = "patched"
    elif len(changes) == len(PATCH_ANCHORS):
        state = "unpatched"
    else:
        state = "mixed"
    return {
        "schema_version": 1,
        "patch_id": PATCH_ID,
        "vendor_version": VENDOR_VERSION,
        "source_root": str(root),
        "source_state": state,
        "reference_archive_sha256": REFERENCE_ARCHIVE_SHA256,
        "verification_scope": "listed_source_files_only_not_whole_tree",
        "files": files,
        "provider_sha256": sha256(Path(__file__).with_name("provider.cpp").read_bytes()),
        "patcher_sha256": sha256(Path(__file__).read_bytes()),
        "abi": {
            "schema_version": 1, "result_size": 64, "guid_size": 16,
            "functions": ["r2f_fastdds_peer_writer_v1", "r2f_fastdds_peer_reader_v1"],
            "statuses": {"ok": 0, "invalid_argument": 1, "unsupported": 2,
                         "not_enabled": 3, "buffer_too_small": 4,
                         "internal_error": 5, "abi_mismatch": 6},
            "snapshot_ordinal_basis": "provider_instance_observation_serial_not_match_epoch",
            "membership_basis": "reliable_stateful_native_predicate_guid_set_under_entity_mutex",
            "layout_changed": False, "virtual_slots_added": False,
            "short_buffer": "required_count_set_no_guid_copy_complete_false_retry_new_snapshot",
        },
        "verification": {
            "source_hashes": "verified", "cpp_compilation": "not_run",
            "native_behavior": "not_run", "dds": "not_run",
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--check", action="store_true", help="read-only verification, no patch")
    parser.add_argument("--manifest", type=Path, help="optional explicit JSON receipt path")
    args = parser.parse_args(argv)
    try:
        if args.manifest:
            # Receipt must never overwrite a source input (even via symlinks).
            target = args.manifest.resolve()
            root = args.source_root.resolve()
            if target == root or root in target.parents:
                raise PatchError("manifest_must_be_outside_source_tree")
            if target == Path(__file__).resolve() or target == Path(__file__).with_name("provider.cpp").resolve():
                raise PatchError("manifest_must_not_overwrite_implementation")
            if args.manifest.exists():
                raise PatchError("manifest_already_exists_preserve_prior_receipt")
        receipt = patch_vendor(args.source_root, check=args.check)
        output = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
        if args.manifest:
            # Parent creation is deliberately the build owner's responsibility.
            with args.manifest.open("x", encoding="utf-8") as stream:
                stream.write(output)
        print(output, end="")
        return 0
    except (PatchError, OSError) as exc:
        print(json.dumps({"error": str(exc), "status": "failed", "dds_started": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
