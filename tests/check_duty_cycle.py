#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    pd = None
import csv


def load_csv(path):
    if not Path(path).exists():
        print(f"[SKIP] file not found: {path}")
        return None
    if pd is not None:
        return pd.read_csv(path)
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows.extend(reader)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Check duty cycle usage")
    parser.add_argument("--file", default="mesh_dv_metrics_duty.csv", help="CSV with duty metrics")
    parser.add_argument("--limit", type=float, default=0.01, help="Duty cycle limit (e.g., 0.01 for 1%)")
    parser.add_argument("--margin", type=float, default=0.05, help="Additional tolerance fraction")
    args = parser.parse_args()

    data = load_csv(args.file)
    if data is None:
        sys.exit(2)

    limit = args.limit * (1.0 + args.margin)
    violations = []

    if pd is not None:
        if "dutyUsed" not in data.columns:
            print("[SKIP] 'dutyUsed' column missing")
            sys.exit(2)
        for _, row in data.iterrows():
            if row["dutyUsed"] > limit:
                violations.append((int(row["nodeId"]), float(row["dutyUsed"])))
        top = data.sort_values("dutyUsed", ascending=False).head(5)[["nodeId", "dutyUsed"]]
        print("[duty-cycle] top5 dutyUsed:\n", top.to_string(index=False))
    else:
        if len(data) == 0 or "dutyUsed" not in data[0]:
            print("[SKIP] 'dutyUsed' column missing")
            sys.exit(2)
        sorted_rows = sorted(data, key=lambda r: float(r["dutyUsed"]), reverse=True)
        print("[duty-cycle] top5 dutyUsed:")
        for row in sorted_rows[:5]:
            print(f"  node {row['nodeId']}: {row['dutyUsed']}")
        for row in data:
            if float(row["dutyUsed"]) > limit:
                violations.append((int(row["nodeId"]), float(row["dutyUsed"])))

    if violations:
        print(f"[duty-cycle] ERROR: {len(violations)} nodes exceed limit {limit:.4f}")
        for node, duty in violations:
            print(f"  node {node}: dutyUsed={duty}")
        sys.exit(1)

    print("[duty-cycle] OK: no duty cycle violations detected.")
    sys.exit(0)


if __name__ == "__main__":
    main()
