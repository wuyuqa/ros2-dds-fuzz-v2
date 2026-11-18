#!/usr/bin/env python3
"""Plan or explicitly execute a finite normal-public-API S1 campaign."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from r2f.campaign.runner import run_campaign, runtime_for_boundary, RUNTIME_BOUNDARIES
from r2f.campaign.corpus import corpus_from_profile, available_cycles
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler
from r2f.campaign.runner import run_exploratory_cycle
from r2f.campaign.identity import source_manifest
from r2f.campaign.checkpoint import load_checkpoint, make_checkpoint_hook
from r2f.modules.s1.catalog import target_catalog


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list-targets", action="store_true",
                        help="read-only S1 identity/family directory, including opt-in requirements; takes precedence over execution options")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--execute", action="store_true", help="execute only after owner coordinates the exclusive campaign window and freezes source")
    parser.add_argument("--runtime-boundary", choices=RUNTIME_BOUNDARIES, default="host",
                        help="execution route only, not evidence qualification; host is the default, isolated_netns is opt-in")
    parser.add_argument("--domain-id", type=int, default=173)
    parser.add_argument("--run-id")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--target", dest="target_id", choices=("discovery.late_endpoint", "discovery.late_join_delivery", "discovery.interruption_recovery", "builtin.discovery_projection_reconciliation"), default="discovery.late_endpoint")
    parser.add_argument("--parameters-json", type=Path)
    parser.add_argument("--exploratory-profile", type=Path,
                        help="explicit exploratory profile (including topology_v1 opt-in); uses the unified exploratory cycle")
    parser.add_argument("--cycle-id", help="cycle inside the exploratory profile")
    parser.add_argument("--scheduler-policy", choices=("fair", "feedback", "bfs"), default=None,
                        help="exploratory selection policy; omitted preserves legacy feedback or inherits the checkpoint policy")
    parser.add_argument("--max-inputs", type=int, default=None,
                        help="explicit cumulative exploratory selection cap (default: profile budget)")
    parser.add_argument("--resume-from", type=Path,
                        help="saved checkpoint.json or its output directory; old evidence stays read-only")
    args = vars(parser.parse_args(argv))
    if args.pop("list_targets"):
        print(json.dumps({"schema_version": "r2f.s1-target-catalog/v1",
                          "declaration_only": True, "targets": target_catalog()}, indent=2))
        return 0
    if args["output_dir"] is None:
        parser.error("the following arguments are required: --output-dir")
    runtime_boundary = args.pop("runtime_boundary")
    parameters_path = args.pop("parameters_json")
    if parameters_path:
        args["parameters"] = json.loads(parameters_path.read_text())
    profile_path = args.pop("exploratory_profile")
    cycle_id = args.pop("cycle_id")
    resume_path = args.pop("resume_from")
    scheduler_policy = args.pop("scheduler_policy")
    if scheduler_policy is not None and not profile_path:
        parser.error("--scheduler-policy is only valid with --exploratory-profile")
    if resume_path and not profile_path:
        parser.error("--resume-from requires --exploratory-profile and --cycle-id")
    if profile_path:
        if not cycle_id:
            parser.error("--exploratory-profile requires --cycle-id")
        profile = json.loads(profile_path.read_text())
        if scheduler_policy is None:
            scheduler_policy = profile.get("scheduler_policy", "bfs" if "search" in profile else None)
        source = source_manifest(Path(__file__).resolve().parents[1])
        continuation = None
        try:
            declared_policy = profile.get("scheduler_policy", "bfs" if "search" in profile else None)
            if declared_policy is not None and scheduler_policy != declared_policy:
                raise ValueError("--scheduler-policy differs from profile scheduler_policy/search")
            warmup = profile.get("budget", {}).get("warmup_inputs", 10)
            if type(warmup) is not int or warmup < 0:
                raise ValueError("profile budget.warmup_inputs must be a nonnegative integer")
            if resume_path:
                restored = load_checkpoint(resume_path, profile=profile, cycle_id=cycle_id,
                                           source_manifest=source, output_dir=args["output_dir"])
                corpus, feedback, scheduler, continuation = (restored[k] for k in
                    ("corpus", "feedback", "scheduler", "continuation"))
                if scheduler_policy is not None and scheduler_policy != scheduler.policy:
                    raise ValueError("--scheduler-policy cannot change a resumed scheduler policy")
                if args["seed"] is not None and args["seed"] != scheduler.rng_seed:
                    raise ValueError("--seed cannot change a resumed selection identity")
                if scheduler.warmup != warmup:
                    raise ValueError("checkpoint warmup differs from profile budget")
            else:
                corpus = corpus_from_profile(profile, cycle_id=cycle_id)
                feedback = FeedbackLog()
                policy_options = {} if scheduler_policy is None else {"policy": scheduler_policy}
                if scheduler_policy == "bfs":
                    from r2f.campaign.search_frontier import search_settings
                    bounds = search_settings(profile.get("search"))
                    # Topology tasks carry an explicit model descriptor, not
                    # F1 mutation routes or sampling classes. Initialize the
                    # same scheduler first, then freeze that descriptor below.
                    if "topology_search" not in profile:
                        policy_options.update(search=bounds,
                                              sampling_kinds=profile.get("sampling_kinds"),
                                              sampling_weight=profile.get("sampling_weight"))
                scheduler = ExploratoryScheduler(corpus, feedback,
                    rng_seed=args["seed"] if args["seed"] is not None else 0, warmup=warmup,
                    **policy_options)
                if scheduler_policy == "bfs" and "topology_search" in profile:
                    from r2f.campaign.topology_search import descriptor
                    scheduler.configure_search(bounds, descriptor(
                        profile["topology_search"], profile["operator_whitelist"]))
            checkpoint_hook = make_checkpoint_hook(args["output_dir"], profile=profile,
                cycle_id=cycle_id, source_manifest=source, continuation=continuation)
        except (ValueError, KeyError, OSError) as error:
            parser.error(str(error))
        runtime = runtime_for_boundary(runtime_boundary) if args["execute"] else None
        result = run_exploratory_cycle(
            args["output_dir"], corpus=corpus, feedback=feedback,
            scheduler=scheduler, profile=profile, execute=args["execute"],
            runtime=runtime, continuation=continuation, checkpoint_hook=checkpoint_hook,
            run_id=args["run_id"], max_inputs=args["max_inputs"], domain_id=args["domain_id"],
            runtime_boundary=runtime_boundary)
        output = {"run_id": result["run_id"], "cycle_path": str(Path(args["output_dir"]).resolve() / "cycle.json"),
                  "execution_stage": result["rows"][0].get("execution_stage") if result["rows"] else None,
                  "runtime_boundary": result.get("runtime_boundary", runtime_boundary),
                  "runtime_formal": result.get("runtime_formal", False)}
        if "search_summary" in result:
            output["search_summary"] = result["search_summary"]
        print(json.dumps(output, indent=2))
        return 0 if not args["execute"] or any(r.get("actual_run") for r in result["rows"]) else 1
    args.pop("max_inputs")
    args["seed"] = args["seed"] if args["seed"] is not None else 0
    args["runtime_boundary"] = runtime_boundary
    result = run_campaign(**args)
    print(json.dumps({"run_id": result["run_id"], "report_path": str(Path(args["output_dir"]).resolve() / "report.json"),
                      "seal": result["seal"], "runtime_boundary": result.get("runtime_boundary", runtime_boundary),
                      "formal_runtime": result.get("formal_runtime", False)}, indent=2))
    return 0 if not args["execute"] or result["seal"]["sealed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
