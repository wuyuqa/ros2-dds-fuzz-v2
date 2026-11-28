#!/usr/bin/env python3
"""Recompute persisted S1 evidence, controls and target seal offline."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from r2f.reports.run_report import audit_report
from r2f.reports.human_host import HumanHost, HumanHostError


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--human-policy", type=Path)
    parser.add_argument("--human-policy-sha256", help="Independently pinned raw policy SHA256")
    parser.add_argument("--human-signatures", type=Path)
    parser.add_argument("--human-openssl", type=Path, default=Path("/usr/bin/openssl"))
    args = parser.parse_args()
    configured = (args.human_policy, args.human_policy_sha256, args.human_signatures)
    if (any(configured) or args.human_openssl != Path("/usr/bin/openssl")) and not all(configured):
        parser.error("human_host_configuration_incomplete: policy, independent pin and signatures required")
    if all(configured):
        try:
            host = HumanHost(policy_path=args.human_policy,
                             expected_policy_sha256=args.human_policy_sha256,
                             signature_store=args.human_signatures,
                             openssl_executable=args.human_openssl)
            result = audit_report(args.report, human_authority=host)
        except HumanHostError as exc:
            result = {"sealed": False, "evidence_valid": False, "errors": [exc.code]}
    else:
        result = audit_report(args.report)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["sealed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
