#!/usr/bin/env python3
"""Plan or explicitly execute a finite normal-public-API S1 campaign."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from r2f.campaign.runner import run_campaign


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--execute", action="store_true", help="execute only after owner coordinates the exclusive campaign window and freezes source")
    parser.add_argument("--domain-id", type=int, default=173)
    parser.add_argument("--run-id")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--target", dest="target_id", choices=("discovery.late_endpoint", "discovery.late_join_delivery", "discovery.interruption_recovery", "builtin.discovery_projection_reconciliation"), default="discovery.late_endpoint")
    parser.add_argument("--parameters-json", type=Path)
    args = vars(parser.parse_args())
    parameters_path = args.pop("parameters_json")
    if parameters_path:
        args["parameters"] = json.loads(parameters_path.read_text())
    result = run_campaign(**args)
    print(json.dumps({"run_id": result["run_id"], "report_path": str(Path(args["output_dir"]).resolve() / "report.json"), "seal": result["seal"]}, indent=2))
    return 0 if not args["execute"] or result["seal"]["sealed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
