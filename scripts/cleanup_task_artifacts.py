#!/usr/bin/env python3
"""Preview/apply one bounded, completed scratch COPY using existing JSON records.

No discovery by name and no deletion without a byte-identical retained copy.
Supply the existing completion record's JSON pointer/value and all relevant
reference indexes (any path reference is conservatively retained). Preview JSON
is the approval artifact; apply rechecks it, including git worktrees and the supplied
reference manifests. Supply other repos with --repo and known active,
original, pending delivery or history paths with --protect. This is a local
cooperative cleanup tool, not a transaction against concurrent writers: stop
writers before preview/apply. A changed state is retained, never force removed.
stdout/output are short continuation receipts, not a second task registry.

Example: preview --root /tmp/task --candidate /tmp/task/copy --copy-of /tmp/saved
 --completion-record /tmp/delivery.json --completed-pointer /status
 --completed-value '\"completed\"' --reference-manifest /tmp/index.json
 --output /tmp/cleanup-preview.json
Then: apply --preview /tmp/cleanup-preview.json --output /tmp/cleanup-result.json
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
from urllib.parse import unquote

OLD_REPO = '/home/wuyuqa/ros2-dds-fuzz'
DEFAULT_REPO = str(Path(__file__).resolve().parents[1])
# Retention guards only; names never select anything for deletion.
EVIDENCE = {'raw', 'results', 'reports', 'report', 'failures', 'failure', '.git'}


def overlap(a, b):
    a, b = Path(a), Path(b)
    return a == b or a in b.parents or b in a.parents


def absolute(value):
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('absolute path without .. required: ' + str(value))
    return str(path)


@contextmanager
def directory(path):
    """Pin every ancestor without following symlinks, including root parents."""
    parts = Path(absolute(path)).parts
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in parts[1:]:
            new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = new
        yield fd
    finally:
        os.close(fd)


def identity(s):
    return [s.st_dev, s.st_ino, s.st_mode, s.st_uid]


def state(s):
    return identity(s) + [s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_nlink, s.st_blocks]


def file_hash(fd):
    digest = hashlib.sha256()
    while chunk := os.read(fd, 1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def read_record(path):
    path = absolute(path)
    with directory(str(Path(path).parent)) as parent:
        fd = os.open(Path(path).name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ValueError('record is not regular: ' + path)
            data = stream.read()
            if state(before) != state(os.fstat(stream.fileno())):
                raise ValueError('record changed: ' + path)
    return {'path': path, 'state': state(before), 'sha256': hashlib.sha256(data).hexdigest()}, json.loads(data)


def snapshot(path):
    entries = []

    def visit(fd, rel):
        before = os.fstat(fd)
        entries.append({'path': rel, 'state': state(before), 'kind': 'directory'})
        for name in sorted(os.listdir(fd)):
            child = str(Path(rel) / name)
            s = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if name.lower() in EVIDENCE or re.search(r'(raw|failure|failed|report)([._-]|$)', name.lower()) or name.endswith(('.pcap', '.pcapng', '.jsonl', '.log', '.patch', '.diff')):
                raise ValueError('evidence_or_git_retained: ' + child)
            if s.st_dev != before.st_dev:
                raise ValueError('mount boundary: ' + child)
            if stat.S_ISDIR(s.st_mode):
                sub = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                try:
                    visit(sub, child)
                finally:
                    os.close(sub)
            elif stat.S_ISREG(s.st_mode) and s.st_nlink == 1:
                sub = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                try:
                    start = os.fstat(sub)
                    digest = file_hash(sub)
                    if state(s) != state(start) or state(start) != state(os.fstat(sub)):
                        raise ValueError('file changed: ' + child)
                    entries.append({'path': child, 'kind': 'file', 'sha256': digest, 'state': state(start)})
                finally:
                    os.close(sub)
            else:
                raise ValueError('symlink_special_or_hardlink_retained: ' + child)
        if state(before) != state(os.fstat(fd)):
            raise ValueError('directory changed: ' + rel)

    with directory(path) as fd:
        visit(fd, '.')
    return entries


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for k, v in value.items():
            yield k
            yield from strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from strings(v)


def references(value, base, candidate):
    hits = []
    # Delimiters are possible path endings, not proof a path ends there. Also
    # try the full suffix so literal spaces/parentheses in names remain visible.
    separators = r"[\s`\"'<>()[\]{},;|\\]"
    path_start = r"(?<![^\s`\"'<>()[\]{},;|\\=])(?:/|\.{1,2}/|[^\s`\"'<>()[\]{},;|\\=/]+/)"
    for text in strings(value):
        tokens = []
        for decoded in {text, unquote(text).replace('\\ ', ' ')}:
            tokens.extend([decoded, *re.split(separators, decoded)])
            for match in re.finditer(path_start, decoded):
                tail = decoded[match.start():]
                tokens.append(tail)
                tokens.extend(tail[:end.start()] for end in re.finditer(separators, tail))
        for token in tokens:
            if not token or '\n' in token or '\x00' in token:
                continue
            p = Path(token)
            p = Path(os.path.abspath(p if p.is_absolute() else Path(base) / p))
            if overlap(p, candidate) or overlap(p.resolve(), candidate):
                hits.append(text)
                break
    return sorted(set(hits))


def preview(options):
    result = {'schema': 1, 'options': options, 'decision': 'keep', 'reasons': [],
              'reference_checks': [], 'protected_objects': []}
    try:
        root, candidate, retained = [absolute(options[k]) for k in ('root', 'candidate', 'copy_of')]
        if root in ('/', '/tmp', '/dev', '/dev/shm', '/home', str(Path.home())):
            raise ValueError('root is too broad')
        if root == candidate or Path(root) not in Path(candidate).parents:
            raise ValueError('candidate must be strictly inside bounded root')
        if overlap(candidate, retained):
            raise ValueError('retained copy overlaps candidate')
        with directory(root) as fd:
            result['root_identity'] = identity(os.fstat(fd))
        protected = set(options['protect'] + [OLD_REPO, retained])
        worktrees = []
        for repo in options['repo']:
            repo = absolute(repo)
            run = subprocess.run(['git', '-C', repo, 'worktree', 'list', '--porcelain', '-z'],
                                 capture_output=True, check=True)
            worktrees.append({'repo': repo, 'listing_sha256': hashlib.sha256(run.stdout).hexdigest()})
            protected.add(repo)
            for item in run.stdout.decode().split('\0'):
                if item.startswith('worktree '):
                    protected.add(item[9:])
        result['worktrees'] = worktrees
        # Keep entire registered worktrees (dirty/unmerged included), not just .git.
        for path in sorted(protected):
            path = absolute(path)
            if overlap(path, candidate) or overlap(str(Path(path).resolve()), candidate):
                raise ValueError('protected_worktree_or_path: ' + path)
            if Path(path).exists():
                with directory(str(Path(path).parent)) as fd:
                    stamp = identity(os.stat(Path(path).name, dir_fd=fd, follow_symlinks=False))
                if stat.S_ISLNK(stamp[2]):
                    raise ValueError('ambiguous protected symlink: ' + path)
                result['protected_objects'].append({'path': path, 'identity': stamp})
            elif Path(path).is_symlink():
                raise ValueError('ambiguous protected symlink: ' + path)
            else:
                result['protected_objects'].append({'path': path, 'identity': None})
        record = absolute(options['completion_record'])
        if overlap(record, candidate):
            raise ValueError('completion record overlaps candidate')
        receipt, value = read_record(record)
        result['completion_record'] = receipt
        pointer = options['completed_pointer']
        if not pointer.startswith('/'):
            raise ValueError('nonempty JSON pointer required')
        for token in pointer[1:].split('/'):
            token = token.replace('~1', '/').replace('~0', '~')
            value = value[int(token)] if isinstance(value, list) else value[token]
        expected = options['completed_value']
        if value != expected or expected in (None, False, '', [], {}):
            raise ValueError('completion record does not confirm completion')
        for manifest in options['reference_manifest']:
            if overlap(manifest, candidate):
                raise ValueError('reference manifest overlaps candidate')
            receipt, value = read_record(manifest)
            receipt['references'] = references(value, Path(manifest).parent, candidate)
            result['reference_checks'].append(receipt)
        if any(r['references'] for r in result['reference_checks']):
            raise ValueError('referenced_by_existing_record')
        result['candidate_state'] = snapshot(candidate)
        result['retained_state'] = snapshot(retained)
        def contents(entries):
            return [(e['path'], e['kind'], e.get('sha256')) for e in entries]
        if contents(result['candidate_state']) != contents(result['retained_state']):
            raise ValueError('retained_copy_differs')
        result['logical_bytes'] = sum(e['state'][4] for e in result['candidate_state'] if e['kind'] == 'file')
        result['allocated_file_bytes'] = sum(e['state'][-1] * 512 for e in result['candidate_state'] if e['kind'] == 'file')
        result['decision'] = 'disposable'
        result['reasons'] = ['completed_record_confirmed', 'identical_retained_copy', 'no_reference_in_supplied_indexes']
    except (OSError, ValueError, KeyError, TypeError, IndexError, subprocess.SubprocessError) as exc:
        result['reasons'].append(str(exc))
    return result


def apply(plan):
    current = preview(plan['options'])
    result = {'status': 'skipped', 'path': plan['options']['candidate'],
              'unlinked_logical_bytes': 0, 'unlinked_file_allocated_bytes': 0,
              'protected_objects_verified': False, 'unlinked_paths': [], 'reasons': current['reasons']}
    if plan.get('decision') != 'disposable' or current != plan:
        result['reasons'] = ['preview_state_changed_or_not_disposable'] + current['reasons']
        return result
    path = Path(result['path'])
    before = shutil.disk_usage(path.parent).free
    result['filesystem_free_before_bytes'] = before
    try:
        # fd-relative unlink cannot follow a replacement symlink outside the bound.
        with directory(str(path.parent)) as parent:
            fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            try:
                expected = {e['path']: e for e in plan['candidate_state']}
                def remove(folder, rel):
                    for name in sorted(os.listdir(folder)):
                        key = str(Path(rel) / name)
                        entry = expected[key]
                        s = os.stat(name, dir_fd=folder, follow_symlinks=False)
                        if state(s) != entry['state']:
                            raise ValueError('changed_during_apply: ' + key)
                        if entry['kind'] == 'directory':
                            sub = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=folder)
                            try:
                                if state(os.fstat(sub)) != entry['state']:
                                    raise ValueError('directory_replaced_during_apply: ' + key)
                                remove(sub, key)
                            finally:
                                os.close(sub)
                            os.rmdir(name, dir_fd=folder)
                        else:
                            os.unlink(name, dir_fd=folder)
                            result['unlinked_paths'].append(key)
                            result['unlinked_logical_bytes'] += s.st_size
                            result['unlinked_file_allocated_bytes'] += s.st_blocks * 512
                if state(os.fstat(fd)) != expected['.']['state']:
                    raise ValueError('candidate_replaced_during_apply')
                remove(fd, '.')
                if identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) != identity(os.fstat(fd)):
                    raise ValueError('candidate_path_replaced_during_apply')
                os.rmdir(path.name, dir_fd=parent)
            finally:
                os.close(fd)
        result['status'] = 'deleted'
    except (OSError, ValueError, KeyError) as exc:
        result['status'] = 'failed'
        result['reasons'] = [str(exc)]
    result['filesystem_free_after_bytes'] = shutil.disk_usage(path.parent).free
    result['filesystem_free_delta_bytes'] = result['filesystem_free_after_bytes'] - before
    result['accounting'] = 'unlink counts are not reclaimed space (open readers may retain blocks); free delta is observed filesystem-wide, including directory blocks and unrelated activity'
    result['protected_objects_verified'] = all(
        (not os.path.lexists(p['path']) if p['identity'] is None else
         os.path.lexists(p['path']) and identity(os.lstat(p['path'])) == p['identity'])
        for p in plan['protected_objects'])
    if not result['protected_objects_verified']:
        result['status'] = 'failed'
        result['reasons'].append('protected_object_changed')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('preview')
    for flag in ('root', 'candidate', 'copy-of', 'completion-record', 'completed-pointer', 'completed-value'):
        p.add_argument('--' + flag, required=True)
    p.add_argument('--reference-manifest', action='append', required=True)
    p.add_argument('--protect', action='append', default=[])
    p.add_argument('--repo', action='append', default=[DEFAULT_REPO])
    p.add_argument('--output', required=True)
    a = sub.add_parser('apply')
    a.add_argument('--preview', required=True)
    a.add_argument('--output')
    args = vars(parser.parse_args())
    command, output = args.pop('command'), args.pop('output')
    stream = None
    try:
        if command == 'preview':
            args['completed_value'] = json.loads(args['completed_value'])
            options = args
        else:
            _, plan = read_record(args['preview'])
            options = plan['options']
        if output:
            output = absolute(output)
            if overlap(output, options['candidate']) or overlap(output, options['copy_of']):
                raise ValueError('output overlaps candidate or retained copy')
            # Reserve the receipt before any deletion. Never overwrite evidence.
            with directory(str(Path(output).parent)) as parent:
                fd = os.open(Path(output).name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=parent)
            stream = os.fdopen(fd, 'w')
        result = preview(options) if command == 'preview' else apply(plan)
        text = json.dumps(result, indent=2, sort_keys=True) + '\n'
        if stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        sys.stdout.write(text)
        return 1 if result.get('status') == 'failed' else 0
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(2, str(exc) + '\n')
    finally:
        if stream:
            stream.close()



if __name__ == '__main__':
    sys.exit(main())
