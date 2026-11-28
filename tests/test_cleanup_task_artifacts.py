"""Cleanup contracts through the approved subprocess CLI; no ROS or old artifacts."""
import json
from pathlib import Path
import subprocess
import sys

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/cleanup_task_artifacts.py'


def cli(*args):
    run = subprocess.run([sys.executable, str(SCRIPT), *map(str, args)],
                         text=True, capture_output=True)
    assert run.returncode == 0, run.stderr
    return json.loads(run.stdout)


def setup_copy(tmp_path):
    root = tmp_path / 'bounded'
    root.mkdir()
    candidate = root / 'scratch'
    retained = tmp_path / 'retained'
    for path in (candidate, retained):
        path.mkdir()
        (path / 'blob').write_bytes(b'x' * 65536)
    record = tmp_path / 'delivery.json'
    record.write_text(json.dumps({'status': 'completed', 'retained': str(retained)}))
    refs = tmp_path / 'index.json'
    refs.write_text(json.dumps({'pending': [], 'raw': []}))
    plan = tmp_path / 'preview.json'
    args = ['preview', '--root', root, '--candidate', candidate, '--copy-of', retained,
            '--completion-record', record, '--completed-pointer', '/status',
            '--completed-value', '"completed"', '--reference-manifest', refs,
            '--output', plan]
    return candidate, retained, record, refs, plan, args


def test_cli_deletes_verified_copy_and_reports_observed_bytes(tmp_path):
    candidate, retained, _, _, plan, args = setup_copy(tmp_path)
    preview = cli(*args)
    assert candidate.exists()
    assert preview['decision'] == 'disposable'
    allocated = (candidate / 'blob').stat().st_blocks * 512
    result = cli('apply', '--preview', plan)
    assert result['status'] == 'deleted'
    assert result['unlinked_logical_bytes'] == 65536
    assert result['unlinked_file_allocated_bytes'] == allocated
    assert result['protected_objects_verified'] is True
    assert not candidate.exists()
    assert (retained / 'blob').read_bytes() == b'x' * 65536


def test_protect_regular_file_and_its_ancestor(tmp_path):
    candidate, _, _, _, plan, args = setup_copy(tmp_path)
    sentinel = tmp_path / 'pending.patch'
    sentinel.write_text('unmerged delivery')
    preview = cli(*args, '--protect', sentinel)
    assert preview['decision'] == 'disposable'
    result = cli('apply', '--preview', plan)
    assert result['status'] == 'deleted'
    assert sentinel.read_text() == 'unmerged delivery'


def test_output_must_be_writable_before_any_deletion(tmp_path):
    candidate, _, _, _, plan, args = setup_copy(tmp_path)
    cli(*args)
    receipt = tmp_path / 'existing.json'
    receipt.write_text('do not overwrite')
    run = subprocess.run([sys.executable, str(SCRIPT), 'apply', '--preview', str(plan),
                          '--output', str(receipt)], text=True, capture_output=True)
    assert run.returncode == 2
    assert candidate.exists()
    assert receipt.read_text() == 'do not overwrite'


def test_stale_content_even_same_size_and_mtime_is_skipped(tmp_path):
    import os
    candidate, _, _, _, plan, args = setup_copy(tmp_path)
    cli(*args)
    blob = candidate / 'blob'
    old = blob.stat()
    blob.write_bytes(b'y' * 65536)
    os.utime(blob, ns=(old.st_atime_ns, old.st_mtime_ns))
    result = cli('apply', '--preview', plan)
    assert result['status'] == 'skipped'
    assert result['unlinked_logical_bytes'] == 0
    assert blob.read_bytes() == b'y' * 65536


def test_new_reference_after_preview_skips_without_deleting(tmp_path):
    candidate, _, _, refs, plan, args = setup_copy(tmp_path)
    cli(*args)
    refs.write_text(json.dumps({'pending_delivery': str(candidate / 'blob')}))
    result = cli('apply', '--preview', plan)
    assert result['status'] == 'skipped'
    assert 'referenced_by_existing_record' in result['reasons']
    assert candidate.exists()


def test_protected_candidate_is_retained(tmp_path):
    candidate, _, _, _, plan, args = setup_copy(tmp_path)
    result = cli(*args, '--protect', candidate / 'blob')
    assert result['decision'] == 'keep'
    assert cli('apply', '--preview', plan)['status'] == 'skipped'
    assert candidate.exists()


def test_candidate_symlink_replacement_after_preview_is_skipped(tmp_path):
    candidate, retained, _, _, plan, args = setup_copy(tmp_path)
    cli(*args)
    saved = candidate.with_name('moved-scratch')
    candidate.rename(saved)
    candidate.symlink_to(retained, target_is_directory=True)
    assert cli('apply', '--preview', plan)['status'] == 'skipped'
    assert (retained / 'blob').exists()
    assert (saved / 'blob').exists()


def test_ancestor_symlink_escape_is_retained(tmp_path):
    candidate, retained, _, _, _, args = setup_copy(tmp_path)
    link = candidate.parent / 'escape'
    link.symlink_to(retained, target_is_directory=True)
    args[args.index('--candidate') + 1] = link
    result = cli(*args)
    assert result['decision'] == 'keep'
    assert (retained / 'blob').exists()


def test_registered_dirty_worktree_is_always_retained(tmp_path):
    _, _, _, _, _, args = setup_copy(tmp_path)
    args[args.index('--root') + 1] = SCRIPT.parents[1]
    args[args.index('--candidate') + 1] = SCRIPT.parent
    result = cli(*args)
    assert result['decision'] == 'keep'
    assert any('protected_worktree_or_path' in r for r in result['reasons'])
    assert SCRIPT.exists()


def test_unknown_provenance_is_retained(tmp_path):
    candidate, _, record, _, _, args = setup_copy(tmp_path)
    record.write_text('{"status": "pending"}')
    result = cli(*args)
    assert result['decision'] == 'keep'
    assert 'completion record does not confirm completion' in result['reasons']
    assert candidate.exists()


def test_root_and_prefix_sibling_are_never_candidates(tmp_path):
    candidate, _, _, _, _, args = setup_copy(tmp_path)
    args[args.index('--candidate') + 1] = candidate.parent
    assert cli(*args)['decision'] == 'keep'
    args[args.index('--output') + 1] = tmp_path / 'preview2.json'
    args[args.index('--candidate') + 1] = tmp_path / 'bounded-sibling'
    assert cli(*args)['decision'] == 'keep'


def test_original_evidence_and_unregistered_git_and_symlinks_are_retained(tmp_path):
    candidate, retained, _, _, _, args = setup_copy(tmp_path)
    for index, name in enumerate(['raw', 'report.json', 'failure.log', '.git', 'escape']):
        for folder in (candidate, retained):
            target = folder / name
            if name == 'escape':
                target.symlink_to(tmp_path / 'outside')
            else:
                target.write_text('must preserve')
        args[args.index('--output') + 1] = tmp_path / f'preview-{index}.json'
        result = cli(*args)
        assert result['decision'] == 'keep'
        assert (candidate / 'blob').exists()
        for folder in (candidate, retained):
            (folder / name).unlink()


def test_byte_accounting_does_not_claim_unlinked_blocks_are_free(tmp_path):
    candidate, _, _, _, plan, args = setup_copy(tmp_path)
    cli(*args)
    allocated = (candidate / 'blob').stat().st_blocks * 512
    with (candidate / 'blob').open('rb') as held_open:
        result = cli('apply', '--preview', plan)
        assert result['status'] == 'deleted'
        assert result['unlinked_file_allocated_bytes'] == allocated
        assert 'reclaimed_file_allocated_bytes' not in result
        assert result['filesystem_free_after_bytes'] - result['filesystem_free_before_bytes'] == result['filesystem_free_delta_bytes']
        assert held_open.read(1) == b'x'


def test_partial_cleanup_failure_reports_only_successful_unlinks(tmp_path):
    candidate, retained, _, _, plan, args = setup_copy(tmp_path)
    candidate.parent.chmod(0o500)
    try:
        cli(*args)
        run = subprocess.run([sys.executable, str(SCRIPT), 'apply', '--preview', str(plan)],
                             text=True, capture_output=True)
        assert run.returncode == 1, run.stderr
        result = json.loads(run.stdout)
        assert result['status'] == 'failed'
        assert result['unlinked_paths'] == ['blob']
        assert result['unlinked_logical_bytes'] == 65536
        assert result['protected_objects_verified'] is True
        assert candidate.exists()
        assert (retained / 'blob').exists()
    finally:
        candidate.parent.chmod(0o700)


@pytest.mark.parametrize("spaces", [False, True], ids=["plain_name", "spaced_name"])
@pytest.mark.parametrize("relative", [False, True], ids=["absolute", "relative"])
@pytest.mark.parametrize("form", ["plain", "markdown", "parentheses", "backticks", "single_quote",
                                  "double_quote", "encoded", "ambiguous", "escaped"])
def test_formatted_reference_retains_candidate(tmp_path, relative, form, spaces):
    candidate, retained, _, refs, plan, args = setup_copy(tmp_path)
    # Exercise actual spaces in paths, including encoded and ambiguous separators.
    spaced = candidate.with_name("scratch copy" if spaces else "scratch")
    candidate.rename(spaced)
    args[args.index('--candidate') + 1] = spaced
    path = str(spaced.relative_to(refs.parent) if relative else spaced)
    formats = {
        "plain": path,
        "markdown": f"active [scratch]({path})",
        "parentheses": f"active ({path})",
        "backticks": f"active `{path}`",
        "single_quote": f"active '{path}'",
        "double_quote": f'active "{path}"',
        "encoded": f"active [scratch]({path.replace(' ', '%20')})",
        "ambiguous": f"active {path};next=unknown",
        "escaped": "active (" + path.replace(" ", "\\ ") + ")",
    }
    refs.write_text(json.dumps({'pending': formats[form]}))
    result = cli(*args)
    assert result['decision'] == 'keep'
    assert 'referenced_by_existing_record' in result['reasons']
    assert result['reference_checks'][0]['references'] == [formats[form]]
    assert cli('apply', '--preview', plan)['status'] == 'skipped'
    assert spaced.exists() and (retained / 'blob').exists()


@pytest.mark.parametrize("form", ["markdown", "parentheses", "relative_backticks"])
def test_introduced_formatted_reference_revalidated_at_apply(tmp_path, form):
    candidate, retained, _, refs, plan, args = setup_copy(tmp_path)
    assert cli(*args)['decision'] == 'disposable'
    path = str(candidate / 'blob')
    text = {'markdown': f'[active]({path})', 'parentheses': f'active ({path})',
            'relative_backticks': f'active `{(candidate / "blob").relative_to(refs.parent)}`'}[form]
    refs.write_text(json.dumps({'pending': text}))
    result = cli('apply', '--preview', plan)
    assert result['status'] == 'skipped'
    assert 'referenced_by_existing_record' in result['reasons']
    assert result['unlinked_logical_bytes'] == 0
    assert (candidate / 'blob').exists() and (retained / 'blob').exists()
