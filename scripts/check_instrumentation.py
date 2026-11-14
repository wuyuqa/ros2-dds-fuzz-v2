#!/usr/bin/env python3
"""Read-only vendor inventory by default; --development-pair needs coordination."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from r2f.instrumentation import parse_trace, probe_cyclonedds


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fastdds-feasibility', action='store_true', help='read installed Fast DDS headers/ELF; create no participants')
    parser.add_argument('--probe', action='store_true', help='inventory installed vendor tracing without creating any DDS entity')
    parser.add_argument('--parse', type=Path, help='parse an existing vendor log; provenance remains unverified')
    parser.add_argument('--recollect', type=Path, help='append corrected analysis of an existing development probe without rerunning it')
    parser.add_argument('--development-pair', action='store_true', help='one coordinated normal ROS2 pair; never a seal or campaign')
    parser.add_argument('--domain', type=int, default=174)
    parser.add_argument('--output-root', type=Path, default=Path(__file__).resolve().parents[1] / 'results/instrumentation-smoke')
    args = parser.parse_args()
    if sum((args.probe, args.parse is not None, args.development_pair, args.recollect is not None, args.fastdds_feasibility)) > 1:
        parser.error('choose one operation')
    if args.fastdds_feasibility:
        from r2f.instrumentation.fastdds import probe_fastdds
        print(json.dumps(probe_fastdds(), indent=2, sort_keys=True))
        return 0
    if args.recollect:
        from r2f.instrumentation.development import recollect_development_probe
        result = recollect_development_probe(args.recollect)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result['internal_event_witness'] else 1
    if args.development_pair:
        from r2f.instrumentation.development import run_development_probe
        result = run_development_probe(args.output_root, domain_id=args.domain)
        print(json.dumps({key: result.get(key) for key in ('status', 'error', 'output_dir', 'run_id', 'phase2_disposition',
                                                         'internal_event_witness', 'vendor_event_count', 'limited_projection', 'extended_projection')}, indent=2, sort_keys=True))
        return 0 if result['status'] == 'completed' and result['internal_event_witness'] else 1
    result = parse_trace(args.parse.read_bytes()) if args.parse else probe_cyclonedds()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if args.parse or result['available'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
