"""Operator-pinned signed-file ingress for the existing human-authority seam.

The host supplies policy_path, its independently obtained raw SHA256, and the
signature_store out of band. Never source these arguments from a report, record
or signature. This module creates no keys, trust policies or human decisions.

Policy v1: {"schema_version": "r2f.human-host-policy/v1", "principals": {
  "HUMAN_ID": {"public_key_path": "/ABSOLUTE/ed25519-public.pem",
               "public_key_sha256": "RAW_PEM_SHA256",
               "permitted_origins": ["user_instruction"]}}}
Detached binary Ed25519 signatures live at SIGNATURE_STORE/<full-record-hash>.sig.
Sign canonical_record_bytes(record), including record_sha256, without a newline.
Canonical candidate APIs still validate record schema, execution, instructions,
qualification and history; this callable proves only the signature's authority.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile

from r2f.core._json import snapshot


class HumanHostError(ValueError):
    """Fail-closed host outcome, also exposed unchanged by the CLI."""

    def __init__(self, code, details=None):
        self.code = code
        self.details = details
        super().__init__(code)


def canonical_record_bytes(record):
    """Exact full-record UTF-8 serialization used by evidence.canonical_hash."""
    if not isinstance(record, dict):
        raise HumanHostError('human_host_record_invalid')
    try:
        return json.dumps(snapshot(record), sort_keys=True, separators=(',', ':'),
                          ensure_ascii=False, allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, RecursionError) as exc:
        raise HumanHostError('human_host_record_invalid') from exc


def _digest(data):
    return hashlib.sha256(data).hexdigest()


def _sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def _absolute(value, code):
    try:
        path = Path(value)
        if not path.is_absolute():
            raise ValueError('absolute path required')
        return path
    except (TypeError, ValueError) as exc:
        raise HumanHostError(code) from exc


def _read(path, code):
    try:
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK), 'rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError('regular file required')
            return stream.read()
    except (OSError, ValueError) as exc:
        raise HumanHostError(code) from exc


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate policy key')
        result[key] = value
    return result


class HumanHost:
    """Reopening verifier; no cached policy/key/signature or JSON-derived trust.

    A host may explicitly select another trusted absolute OpenSSL 3 executable.
    PATH and report fields never select the verifier. A successful verify returns
    hashes of the exact copied bytes verified, not a qualification receipt.
    """

    def __init__(self, *, policy_path, expected_policy_sha256, signature_store,
                 openssl_executable='/usr/bin/openssl'):
        self.policy_path = _absolute(policy_path, 'human_host_policy_path_invalid')
        if not _sha(expected_policy_sha256):
            raise HumanHostError('human_host_policy_pin_invalid')
        self.expected_policy_sha256 = expected_policy_sha256
        self.signature_store = _absolute(signature_store, 'human_host_signature_store_invalid')
        self.openssl_executable = _absolute(openssl_executable, 'human_host_openssl_path_invalid')
        self._policy()
        self._check_openssl()

    def _run(self, args):
        try:
            return subprocess.run([str(self.openssl_executable), *args],
                                  stdin=subprocess.DEVNULL, capture_output=True, timeout=10,
                                  env={'LC_ALL': 'C', 'OPENSSL_CONF': '/dev/null'})
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise HumanHostError('human_host_openssl_unavailable') from exc

    def _check_openssl(self):
        version = self._run(['version'])
        if version.returncode or not version.stdout.startswith(b'OpenSSL 3.'):
            raise HumanHostError('human_host_openssl_unavailable')

    def _policy(self):
        data = _read(self.policy_path, 'human_host_policy_unavailable')
        if _digest(data) != self.expected_policy_sha256:
            raise HumanHostError('human_host_policy_hash_mismatch')
        try:
            policy = json.loads(data, object_pairs_hook=_unique_object)
            if (set(policy) != {'schema_version', 'principals'}
                    or policy['schema_version'] != 'r2f.human-host-policy/v1'
                    or not isinstance(policy['principals'], dict) or not policy['principals']):
                raise ValueError('invalid policy')
            for principal, config in policy['principals'].items():
                if (not principal.strip() or not isinstance(config, dict)
                        or set(config) != {'public_key_path', 'public_key_sha256', 'permitted_origins'}
                        or not _sha(config['public_key_sha256'])):
                    raise ValueError('invalid principal')
                _absolute(config['public_key_path'], 'human_host_key_path_invalid')
                origins = config['permitted_origins']
                if (not isinstance(origins, list) or not origins
                        or any(origin not in ('human_operation', 'user_instruction') for origin in origins)
                        or len(set(origins)) != len(origins)):
                    raise ValueError('invalid permitted origins')
        except HumanHostError:
            raise
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise HumanHostError('human_host_policy_invalid') from exc
        return policy

    def verify(self, record):
        """Authenticate a complete record, raising a named outcome on failure."""
        payload = canonical_record_bytes(record)
        record = json.loads(payload)  # Use the same snapshot for principal/origin and signature.
        policy = self._policy()
        principal = record.get('human_id')
        if not isinstance(principal, str) or principal not in policy['principals']:
            raise HumanHostError('human_host_principal_untrusted')
        config = policy['principals'][principal]
        if record.get('origin') not in config['permitted_origins']:
            raise HumanHostError('human_host_origin_forbidden')
        key = _read(Path(config['public_key_path']), 'human_host_key_unavailable')
        if _digest(key) != config['public_key_sha256']:
            raise HumanHostError('human_host_key_hash_mismatch')
        full_hash = _digest(payload)
        signature = _read(self.signature_store / (full_hash + '.sig'),
                          'human_host_signature_unavailable')
        if len(signature) != 64:
            raise HumanHostError('human_host_signature_invalid')
        self._check_openssl()
        # All OpenSSL inputs are private copies of the bytes just hashed. No
        # subprocess reopens a mutable key/signature path supplied by the caller.
        with tempfile.TemporaryDirectory(prefix='r2f-human-host-') as temporary:
            root = Path(temporary)
            key_copy, signature_copy, record_copy = (root / n for n in ('key', 'signature', 'record'))
            key_copy.write_bytes(key)
            signature_copy.write_bytes(signature)
            record_copy.write_bytes(payload)
            parsed = self._run(['pkey', '-pubin', '-in', str(key_copy), '-outform', 'DER'])
            # RFC 8410 SubjectPublicKeyInfo: id-Ed25519, absent parameters, 32-byte key.
            if (parsed.returncode or len(parsed.stdout) != 44
                    or not parsed.stdout.startswith(bytes.fromhex('302a300506032b6570032100'))):
                raise HumanHostError('human_host_key_not_ed25519')
            verified = self._run(['pkeyutl', '-verify', '-pubin', '-rawin', '-inkey', str(key_copy),
                                  '-in', str(record_copy), '-sigfile', str(signature_copy)])
            if verified.returncode:
                raise HumanHostError('human_host_signature_invalid')
        return {'verified': True, 'human_id': principal, 'origin': record['origin'],
                'policy_sha256': self.expected_policy_sha256, 'key_sha256': _digest(key),
                'signature_sha256': _digest(signature), 'signed_record_sha256': full_hash,
                'openssl_executable': str(self.openssl_executable)}

    def __call__(self, record):
        self.verify(record)
        return True


def human_decision_view(report_path, *, human_log=None, human_authority=None, record=None):
    """Audit an immutable original, then import/query its canonical human ledger.

    This is a projection, not a rewritten report or seal. Original index/replay
    associations remain attached exclusively to the original report. No decision
    is synthesized when record is absent. A supplied log must exist for queries;
    import alone may create it through the existing locked append transport.
    """
    from r2f.core.evidence import canonical_hash
    from r2f.reports.candidate_report import (
        append_human_record, audit_projection, exploratory_candidate_ledger, read_human_records)
    from r2f.reports.run_report import EXPLORATORY_SCHEMA, audit_report

    if human_authority is not None and not isinstance(human_authority, HumanHost):
        raise HumanHostError('human_host_authority_invalid')
    path = Path(report_path).resolve()
    original_bytes = _read(path, 'human_host_report_unavailable')
    try:
        original = json.loads(original_bytes)
    except ValueError as exc:
        raise HumanHostError('human_host_report_invalid') from exc
    if not isinstance(original, dict) or original.get('schema_version') != EXPLORATORY_SCHEMA:
        raise HumanHostError('human_host_report_kind_unsupported')
    audit = audit_report(path, human_authority=human_authority)
    if audit.get('evidence_valid') is not True:
        raise HumanHostError('human_host_original_report_audit_failed', audit)
    ledger_path = Path(original['candidate_ledger_path'])
    if not ledger_path.is_absolute():
        ledger_path = path.parent / ledger_path
    ledger_bytes = _read(ledger_path, 'human_host_original_ledger_unavailable')
    try:
        saved_ledger = json.loads(ledger_bytes)
    except ValueError as exc:
        raise HumanHostError('human_host_original_ledger_invalid') from exc
    if canonical_hash(saved_ledger) != original['candidate_ledger_sha256']:
        raise HumanHostError('human_host_original_ledger_changed')
    # Reuse the independently audited exclusions, never infer new qualification.
    exclusions = saved_ledger.get('execution_exclusions', [])
    excluded = {entry['cell_id'] for entry in exclusions}
    rows = [row for row in original['rows'] if row['cell_id'] not in excluded]
    reviews = original.get('review_records') or []
    original_records = original.get('human_records') or []
    baseline = exploratory_candidate_ledger(rows, reviews=reviews, human_records=original_records,
                                            human_authority=human_authority)
    if ([audit_projection(item) for item in baseline['candidates']]
            != [audit_projection(item) for item in saved_ledger['candidates']]):
        raise HumanHostError('human_host_original_projection_unsupported')

    def original_unchanged():
        if _read(path, 'human_host_report_unavailable') != original_bytes:
            raise HumanHostError('human_host_original_report_changed')
        if _read(ledger_path, 'human_host_original_ledger_unavailable') != ledger_bytes:
            raise HumanHostError('human_host_original_ledger_changed')

    original_unchanged()
    log = Path(human_log).resolve() if human_log is not None else None
    if log in (path, ledger_path.resolve()):
        raise HumanHostError('human_host_log_path_conflict')
    options = dict(original_rows=rows, reviews=reviews, human_authority=human_authority)
    if log is not None and log.exists():
        previous = read_human_records(log, **options)
    elif log is not None and record is None:
        raise HumanHostError('human_host_log_unavailable')
    else:
        previous = [] if log is not None else baseline['human_records']
    if any(item not in previous for item in original_records):
        raise HumanHostError('human_host_original_history_missing_from_log')
    if record is not None:
        if not isinstance(human_authority, HumanHost):
            raise HumanHostError('human_host_policy_required')
        if log is None:
            raise HumanHostError('human_host_log_required')
        human_authority.verify(record)  # Preserve named authenticity outcomes.
        append_human_record(log, record, **options)
    records = read_human_records(log, **options) if log is not None else previous
    ledger = exploratory_candidate_ledger(rows, reviews=reviews, human_records=records,
                                          human_authority=human_authority)
    if ledger['human_errors']:
        raise HumanHostError('human_host_projection_rejected', ledger['human_errors'])
    if exclusions:
        ledger['execution_exclusions'] = exclusions
    authenticity = [human_authority.verify(item) for item in records] if human_authority else []
    original_unchanged()
    audit = audit_report(path, human_authority=human_authority)
    if audit.get('evidence_valid') is not True:
        raise HumanHostError('human_host_original_report_audit_failed', audit)
    original_unchanged()
    return {'projection_kind': 'canonical_human_ledger',
            'original_report': {'path': str(path), 'raw_sha256': _digest(original_bytes),
                                'report_sha256': original['report_sha256'], 'audit': audit},
            'human_log': {'path': str(log) if log is not None else None,
                          'reopened_and_validated': log is not None,
                          'records_sha256': canonical_hash(records), 'authenticity': authenticity},
            'ledger': ledger, 'ledger_sha256': canonical_hash(ledger),
            'limitation': 'Projected canonical ledger; original report/index/replay/seal unchanged. '
                          'No new execution, replay, required maturity or independent-new evidence.'}
