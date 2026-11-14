#!/usr/bin/env python3
"""Rerun one normal S1 capsule, or audit a persisted replay without starting ROS."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from r2f.campaign.replay import audit_replay, replay_cell


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('capsule', nargs='?', type=Path)
    parser.add_argument('--output', '--output-dir', dest='output', type=Path)
    parser.add_argument('--domain', type=int)
    parser.add_argument('--verify', type=Path, metavar='REPLAY_REPORT', help='read-only artifact and comparison audit')
    args = parser.parse_args()
    if args.verify:
        if args.capsule is not None or args.output is not None or args.domain is not None:
            parser.error('--verify cannot be combined with execution arguments')
        result = audit_replay(args.verify)
        code = 0 if result['valid'] else 2
    else:
        if args.capsule is None or args.output is None or args.domain is None:
            parser.error('execution requires capsule, --output and --domain')
        try:
            result = replay_cell(args.capsule, output_dir=args.output, domain_id=args.domain)
        except (ValueError, OSError) as exc:
            result = {'status': 'fail_artifact', 'failure_reason': f'{type(exc).__name__}: {exc}', 'report_path': None}
        code = 0 if result['status'] == 'pass' else 2
    print(json.dumps(result, sort_keys=True, indent=2, ensure_ascii=False))
    return code


if __name__ == '__main__':
    raise SystemExit(main())
