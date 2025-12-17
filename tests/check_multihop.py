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
        print(f"[ERROR] CSV not found: {path}", file=sys.stderr)
        sys.exit(1)
    if pd is not None:
        return pd.read_csv(path)
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows.extend(reader)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Check that multi-hop deliveries exist")
    parser.add_argument("--rx", default="mesh_dv_metrics_rx.csv", help="RX metrics CSV path")
    parser.add_argument("--gw-id", type=int, default=3, help="Gateway node id")
    parser.add_argument("--min-hops", type=int, default=2, help="Minimum hops to count as multi-hop")
    args = parser.parse_args()

    rx = load_csv(args.rx)

    if pd is not None:
        rx_gw = rx[(rx["nodeId"] == args.gw_id) & (rx["dst"] == args.gw_id)]
        delivered = len(rx_gw)
        multihop = len(rx_gw[rx_gw["hops"] >= args.min_hops])
    else:
        rx_gw = [r for r in rx if int(r["nodeId"]) == args.gw_id and int(r["dst"]) == args.gw_id]
        delivered = len(rx_gw)
        multihop = len([r for r in rx_gw if int(r["hops"]) >= args.min_hops])

    ratio = (multihop / delivered) * 100.0 if delivered > 0 else 0.0
    print(f"[multihop] delivered_to_gw={delivered} multihop={multihop} ratio={ratio:.2f}% (hops>={args.min_hops})")

    if delivered == 0:
        print("[multihop] WARNING: no packets delivered to gateway")
        sys.exit(1)
    if multihop == 0:
        print("[multihop] ERROR: no multi-hop packets observed at gateway")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()

