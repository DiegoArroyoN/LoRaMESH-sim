#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
from bisect import bisect_right

try:
    import pandas as pd
except ImportError:
    pd = None
import csv


def load_csv(path):
    if not Path(path).exists():
        print(f"[ERROR] CSV not found: {path}", file=sys.stderr)
        sys.exit(1)
    if pd is not None:
        return pd.read_csv(path)
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Check TX events align with known DV routes")
    parser.add_argument("--tx", default="mesh_dv_metrics_tx.csv", help="TX metrics CSV path")
    parser.add_argument("--routes", default="mesh_dv_metrics_routes.csv", help="Route events CSV path")
    parser.add_argument("--start-time", type=float, default=0.0, help="Only consider tx after this time (s)")
    parser.add_argument("--threshold", type=float, default=0.95, help="Minimum consistency ratio required")
    args = parser.parse_args()

    tx = load_csv(args.tx)
    routes = load_csv(args.routes)

    # Build route timeline per (node,dest)
    timeline = {}
    if pd is not None:
        for _, row in routes.iterrows():
            key = (int(row["nodeId"]), int(row["destination"]))
            timeline.setdefault(key, []).append((float(row["timestamp(s)"]), int(row["nextHop"])))
    else:
        for row in routes:
            key = (int(row["nodeId"]), int(row["destination"]))
            timeline.setdefault(key, []).append((float(row["timestamp(s)"]), int(row["nextHop"])))

    for key in timeline:
        timeline[key].sort(key=lambda x: x[0])

    def lookup_next_hop(node_id, dest, tstamp):
        key = (node_id, dest)
        if key not in timeline:
            return None
        times = [tp[0] for tp in timeline[key]]
        idx = bisect_right(times, tstamp) - 1
        if idx < 0:
            return None
        return timeline[key][idx][1]

    if pd is not None:
        tx_rows = tx[(tx["dst"] != 65535) & (tx["ok"] == 1) & (tx["timestamp(s)"] >= args.start_time)]
        total = len(tx_rows)
        consistent = 0
        for _, row in tx_rows.iterrows():
            nh = lookup_next_hop(int(row["nodeId"]), int(row["dst"]), float(row["timestamp(s)"]))
            if nh is not None and nh != 0:
                consistent += 1
    else:
        tx_rows = [r for r in tx if int(r["dst"]) != 65535 and int(r["ok"]) == 1 and float(r["timestamp(s)"]) >= args.start_time]
        total = len(tx_rows)
        consistent = 0
        for row in tx_rows:
            nh = lookup_next_hop(int(row["nodeId"]), int(row["dst"]), float(row["timestamp(s)"]))
            if nh is not None and nh != 0:
                consistent += 1

    ratio = (consistent / total) if total > 0 else 0.0
    print(f"[routing-consistency] tx_checked={total} with_route={consistent} ratio={ratio:.3f}")

    if total == 0:
        print("[routing-consistency] WARNING: no unicast TX events found")
        sys.exit(1)
    if ratio < args.threshold:
        print(f"[routing-consistency] ERROR: ratio {ratio:.3f} < threshold {args.threshold}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()

