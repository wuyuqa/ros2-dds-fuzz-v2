"""Exercise the public installer; custom roots do not qualify historical evidence."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / 'scripts/prepare_offline_evidence.py'
ARCHIVE = REPO / 'tests/fixtures/f1-5978d067.tar.gz'
MANIFEST = REPO / 'tests/fixtures/f1-5978d067.manifest.json'


def install(target, *args, capsule='f1'):
    return subprocess.run([sys.executable, str(SCRIPT), '--target', str(target), '--capsule', capsule, *map(str, args)],
                          text=True, capture_output=True)


@pytest.mark.parametrize('capsule,stem,count', [('f1', 'f1-5978d067', 217), ('p16', 'p16-db4f12e', 60)])
def test_custom_install_preserves_exact_historical_bytes(tmp_path, capsule, stem, count):
    target = tmp_path / 'evidence'
    result = install(target, capsule=capsule)
    assert result.returncode == 0, result.stderr
    receipt = json.loads(result.stdout)
    assert receipt['status'] == 'installed'
    assert receipt['historical_path_binding'] is False
    assert receipt['new_execution'] is False
    manifest = json.loads((REPO / 'tests/fixtures' / (stem + '.manifest.json')).read_text())
    assert receipt['capsule'] == capsule
    assert len(manifest['members']) == count
    assert set(p.relative_to(target).as_posix() for p in target.rglob('*') if p.is_file()) == set(manifest['members'])
    for name, expected in manifest['members'].items():
        content = (target / name).read_bytes()
        assert len(content) == expected['size']
        assert hashlib.sha256(content).hexdigest() == expected['sha256']


@pytest.mark.parametrize('capsule', ['f1', 'p16'])
def test_existing_exact_capsule_is_verified_without_writes(tmp_path, capsule):
    target = tmp_path / 'evidence'
    assert install(target, capsule=capsule).returncode == 0
    before = {p.relative_to(target): (p.lstat().st_ino, p.lstat().st_mtime_ns, p.lstat().st_mode)
              for p in [target, *target.rglob('*')]}
    result = install(target, capsule=capsule)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['status'] == 'verified_existing'
    after = {p.relative_to(target): (p.lstat().st_ino, p.lstat().st_mtime_ns, p.lstat().st_mode)
             for p in [target, *target.rglob('*')]}
    assert before == after


def test_arbitrary_existing_target_is_preserved(tmp_path):
    target = tmp_path / 'existing'
    target.mkdir()
    sentinel = target / 'keep'
    sentinel.write_bytes(b'actual evidence, not installer-owned')
    before = sentinel.stat()
    result = install(target)
    assert result.returncode != 0
    assert 'refusing overwrite' in result.stderr
    assert sentinel.read_bytes() == b'actual evidence, not installer-owned'
    assert sentinel.stat().st_mtime_ns == before.st_mtime_ns
    assert list(target.iterdir()) == [sentinel]


def test_corrupt_existing_capsule_is_not_repaired(tmp_path):
    target = tmp_path / 'evidence'
    assert install(target).returncode == 0
    (target / 'cycle.json').write_bytes(b'corrupt original')
    result = install(target)
    assert result.returncode != 0
    assert 'hash mismatch' in result.stderr
    assert (target / 'cycle.json').read_bytes() == b'corrupt original'


def test_corrupt_or_missing_archive_fails_before_creating_target(tmp_path):
    archive = tmp_path / 'bad.tar.gz'
    data = ARCHIVE.read_bytes()
    archive.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))
    target = tmp_path / 'evidence'
    result = install(target, '--archive', archive)
    assert result.returncode != 0
    assert 'pinned archive hash/size mismatch' in result.stderr
    assert not target.exists()
    result = install(target, '--archive', tmp_path / 'missing.tar.gz')
    assert result.returncode != 0
    assert not target.exists()


def test_target_symlink_ancestors_and_existing_member_links_are_rejected(tmp_path):
    outside = tmp_path / 'outside'
    outside.mkdir()
    link = tmp_path / 'link'
    link.symlink_to(outside, target_is_directory=True)
    for target in (link, link / 'evidence'):
        result = install(target)
        assert result.returncode != 0
        assert 'symlink' in result.stderr
    assert list(outside.iterdir()) == []
    target = tmp_path / 'evidence'
    assert install(target).returncode == 0
    cycle = target / 'cycle.json'
    cycle.unlink()
    sentinel = outside / 'cycle.json'
    sentinel.write_bytes(b'do not overwrite')
    cycle.symlink_to(sentinel)
    result = install(target)
    assert result.returncode != 0
    assert 'symlink' in result.stderr
    assert sentinel.read_bytes() == b'do not overwrite'


def test_unpinned_traversal_and_link_archives_are_rejected(tmp_path):
    import io
    import tarfile
    for kind in ('traversal', 'symlink', 'hardlink'):
        archive = tmp_path / f'{kind}.tar.gz'
        with tarfile.open(archive, 'w:gz') as stream:
            member = tarfile.TarInfo('../escaped' if kind == 'traversal' else 'link')
            if kind == 'traversal':
                member.size = 4
                stream.addfile(member, io.BytesIO(b'evil'))
            else:
                member.type = tarfile.SYMTYPE if kind == 'symlink' else tarfile.LNKTYPE
                member.linkname = '../escaped'
                stream.addfile(member)
        target = tmp_path / kind
        result = install(target, '--archive', archive)
        assert result.returncode != 0
        assert 'pinned archive' in result.stderr
        assert not target.exists()
        assert not (tmp_path / 'escaped').exists()
