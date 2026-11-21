#!/usr/bin/env python3
"""Import/query signed human decisions over an independently audited S1 original.

Produces a canonical ledger projection and proof envelope, never a replacement
report/index/seal. Omit --record for a fresh query; omit --human-log for the
original report's decisions only. Production trust must be independently pinned
by the operator, outside record/report/signature content. No keys are generated.
"""
import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from r2f.reports.human_host import HumanHost, HumanHostError, human_decision_view


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--record', type=Path, help='Existing complete human-adjudication JSON record')
    parser.add_argument('--human-log', type=Path, help='Canonical append-only human JSONL transport')
    parser.add_argument('--output-ledger', type=Path, help='New unique file for the projected canonical ledger')
    parser.add_argument('--human-policy', type=Path)
    parser.add_argument('--human-policy-sha256', help='Independently pinned SHA256 of exact policy file bytes')
    parser.add_argument('--human-signatures', type=Path, help='Detached signature store supplied by the host')
    parser.add_argument('--human-openssl', type=Path, default=Path('/usr/bin/openssl'),
                        help='Explicit trusted absolute OpenSSL 3 executable; never searched on PATH')
    args = parser.parse_args()
    configured = (args.human_policy, args.human_policy_sha256, args.human_signatures)
    if (any(configured) or args.human_openssl != Path('/usr/bin/openssl')) and not all(configured):
        parser.error('human_host_configuration_incomplete: supply policy, independent pin and signatures together')
    try:
        if (args.output_ledger is not None and args.human_log is not None
                and args.output_ledger.resolve() == args.human_log.resolve()):
            raise HumanHostError('human_host_output_path_conflict')
        if args.output_ledger is not None and args.output_ledger.exists():
            raise HumanHostError('human_host_output_exists')
        host = HumanHost(policy_path=args.human_policy, expected_policy_sha256=args.human_policy_sha256,
                         signature_store=args.human_signatures, openssl_executable=args.human_openssl
                         ) if all(configured) else None
        record = json.loads(args.record.read_bytes()) if args.record is not None else None
        if args.record is not None and not isinstance(record, dict):
            raise HumanHostError('human_host_record_invalid')
        view = human_decision_view(args.report, human_log=args.human_log, human_authority=host, record=record)
        if args.output_ledger is not None:
            # Exclusive creation: no original ledger/report or earlier projection is overwritten.
            with args.output_ledger.open('x', encoding='utf-8') as stream:
                json.dump(view['ledger'], stream, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            view['ledger_path'] = str(args.output_ledger.resolve())
        print(json.dumps(dict(view, ok=True), indent=2, ensure_ascii=False, allow_nan=False))
        return 0
    except HumanHostError as exc:
        result = {'ok': False, 'outcome': exc.code, 'details': exc.details}
    except (OSError, ValueError, TypeError, KeyError) as exc:
        result = {'ok': False, 'outcome': 'human_host_import_or_query_rejected', 'details': str(exc)}
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
