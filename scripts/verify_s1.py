#!/usr/bin/env python3
"""Recompute persisted S1 evidence, controls and target seal offline."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from r2f.reports.run_report import audit_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    result = audit_report(args.report)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["sealed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
