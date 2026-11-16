#!/usr/bin/env python3
"""Plan or explicitly execute a finite normal-public-API S1 campaign."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from r2f.campaign.runner import run_campaign
from r2f.campaign.corpus import corpus_from_profile, available_cycles
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.campaign.runner import run_exploratory_cycle


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--execute", action="store_true", help="execute only after owner coordinates the exclusive campaign window and freezes source")
    parser.add_argument("--domain-id", type=int, default=173)
    parser.add_argument("--run-id")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--target", dest="target_id", choices=("discovery.late_endpoint", "discovery.late_join_delivery", "discovery.interruption_recovery", "builtin.discovery_projection_reconciliation"), default="discovery.late_endpoint")
    parser.add_argument("--parameters-json", type=Path)
    parser.add_argument("--exploratory-profile", type=Path,
                        help="explicit exploratory profile; selects a separate exploratory cycle")
    parser.add_argument("--cycle-id", help="cycle inside the exploratory profile")
    parser.add_argument("--max-inputs", type=int, default=1, help="bounded exploratory inputs (default 1)")
    args = vars(parser.parse_args())
    parameters_path = args.pop("parameters_json")
    if parameters_path:
        args["parameters"] = json.loads(parameters_path.read_text())
    profile_path = args.pop("exploratory_profile")
    cycle_id = args.pop("cycle_id")
    if profile_path:
        if not cycle_id:
            parser.error("--exploratory-profile requires --cycle-id")
        profile = json.loads(profile_path.read_text())
        corpus = corpus_from_profile(profile, cycle_id=cycle_id)
        scheduler = ExploratoryScheduler(corpus, FeedbackLog(), rng_seed=args["seed"], warmup=0)
        from r2f.runtime.ros2_pair import run_pair
        result = run_exploratory_cycle(
            args["output_dir"], corpus=corpus, feedback=FeedbackLog(),
            scheduler=scheduler, profile=profile, execute=args["execute"],
            runtime=run_pair if args["execute"] else None,
            run_id=args["run_id"], max_inputs=args["max_inputs"], domain_id=args["domain_id"])
        print(json.dumps({"run_id": result["run_id"], "cycle_path": str(Path(args["output_dir"]).resolve() / "cycle.json"),
                          "execution_stage": result["rows"][0].get("execution_stage") if result["rows"] else None}, indent=2))
        return 0 if not args["execute"] or any(r.get("actual_run") for r in result["rows"]) else 1
    result = run_campaign(**args)
    print(json.dumps({"run_id": result["run_id"], "report_path": str(Path(args["output_dir"]).resolve() / "report.json"), "seal": result["seal"]}, indent=2))
    return 0 if not args["execute"] or result["seal"]["sealed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
