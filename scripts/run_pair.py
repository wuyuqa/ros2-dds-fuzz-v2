#!/usr/bin/env python3
"""Explicit real-pair integration entry point; never part of offline unittest discovery."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from r2f.runtime.ros2_pair import run_pair


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, help='JSON config; command-line values override it')
    parser.add_argument('--output', '--output-dir', dest='output', type=Path, required=True)
    parser.add_argument('--domain', type=int)
    parser.add_argument('--rmw', choices=['rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp'])
    parser.add_argument('--arm', choices=['baseline', 'transparent', 'mutated', 'negative'])
    parser.add_argument('--run-id')
    parser.add_argument('--lock-root', type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text()) if args.config else {}
    for argument, key in (('domain', 'domain_id'), ('rmw', 'rmw'), ('arm', 'arm'), ('run_id', 'run_id'), ('lock_root', 'lock_root')):
        value = getattr(args, argument)
        if value is not None:
            config[key] = str(value) if isinstance(value, Path) else value
    result = run_pair(config, args.output)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result['runtime_status'] == 'completed' else 2


if __name__ == '__main__':
    raise SystemExit(main())
