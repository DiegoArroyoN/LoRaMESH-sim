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
        for r in reader:
            rows.append(r)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Check basic delivery to gateway")
    parser.add_argument("--rx", default="mesh_dv_metrics_rx.csv", help="RX metrics CSV path")
    parser.add_argument("--tx", default="mesh_dv_metrics_tx.csv", help="TX metrics CSV path")
    parser.add_argument("--gw-id", type=int, default=3, help="Gateway node id")
    args = parser.parse_args()

    rx = load_csv(args.rx)
    tx = load_csv(args.tx)

    if pd is not None:
        tx_to_gw = tx[(tx["dst"] == args.gw_id) & (tx["ok"] == 1)]
        rx_at_gw = rx[(rx["nodeId"] == args.gw_id) & (rx["dst"] == args.gw_id)]
        sent = len(tx_to_gw)
        received = len(rx_at_gw)
    else:
        tx_to_gw = [r for r in tx if int(r["dst"]) == args.gw_id and int(r["ok"]) == 1]
        rx_at_gw = [r for r in rx if int(r["nodeId"]) == args.gw_id and int(r["dst"]) == args.gw_id]
        sent = len(tx_to_gw)
        received = len(rx_at_gw)

    pdr = (received / sent) if sent > 0 else 0.0
    print(f"[delivery-basic] sent_to_gw={sent} received_at_gw={received} PDR={pdr:.3f}")

    if sent == 0:
        print("[delivery-basic] WARNING: no packets were sent to the gateway (dst==gw-id)")
        sys.exit(1)
    if pdr == 0.0:
        print("[delivery-basic] ERROR: PDR is zero")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()

