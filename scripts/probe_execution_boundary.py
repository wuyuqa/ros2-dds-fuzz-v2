#!/usr/bin/env python3
"""Non-DDS execution-boundary proof only. Never runs a ROS context or backend."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from r2f.runtime.execution_boundary import probe_execution_boundary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--mode', choices=('success', 'crash', 'hang', 'controller_error', 'nested_pid_proc'), default='success')
    parser.add_argument('--timeout-sec', type=float, default=5)
    parser.add_argument('--preflight', action='store_true', help='Unmodified no-context check_environment for both RMWs')
    parser.add_argument('--domain', type=int, default=181, help='Only private fixture locks / optional isolated port probe')
    args = parser.parse_args()
    result = probe_execution_boundary(args.output_dir, mode=args.mode, timeout_sec=args.timeout_sec,
        preflight_rmws=('rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp') if args.preflight else (), domain_id=args.domain)
    print(json.dumps(result, sort_keys=True, indent=2))
    boundary = result.get('execution_boundary', {})
    return 0 if boundary.get('cleanup', {}).get('confirmed') is True and (
        args.mode not in ('success', 'nested_pid_proc') or result.get('runtime_status') == 'nonDDS_probe_complete') else 2


if __name__ == '__main__':
    raise SystemExit(main())
