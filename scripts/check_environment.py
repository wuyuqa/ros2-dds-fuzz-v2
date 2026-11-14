#!/usr/bin/env python3
"""Read-only ROS/RMW/isolation probe; creates no endpoint and acquires no lease."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from r2f.runtime.environment import check_environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--domain', type=int, default=173)
    parser.add_argument('--rmw', choices=['rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp'])
    parser.add_argument('--overlay-setup')
    args = parser.parse_args()
    result = check_environment({'domain_id': args.domain, 'rmw': args.rmw, 'overlay_setup': args.overlay_setup})
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result['ok'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
