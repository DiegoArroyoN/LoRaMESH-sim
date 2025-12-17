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


def count_unique_sf(data):
    if data is None:
        return 0
    if pd is not None:
        if "sf" not in data.columns:
            return 0
        return data["sf"].nunique()
    if len(data) == 0 or "sf" not in data[0]:
        return 0
    return len({int(r["sf"]) for r in data})


def main():
    parser = argparse.ArgumentParser(description="Check ADR effect using SF distribution")
    parser.add_argument("--no-adr", default="adr_off_phy.csv", help="CSV without ADR (must include 'sf')")
    parser.add_argument("--adr", default="adr_on_phy.csv", help="CSV with ADR enabled (must include 'sf')")
    args = parser.parse_args()

    data_off = load_csv(args.no_adr)
    data_on = load_csv(args.adr)

    sf_off = count_unique_sf(data_off)
    sf_on = count_unique_sf(data_on)

    if data_off is None or data_on is None or sf_off == 0 or sf_on == 0:
        print("[SKIP] Missing data or 'sf' column; cannot evaluate ADR effect.")
        sys.exit(2)

    print(f"[adr-effect] unique_sf_off={sf_off} unique_sf_on={sf_on}")

    if sf_on < 2:
        print("[adr-effect] ERROR: ADR run did not show multiple SFs in use")
        sys.exit(1)

    print("[adr-effect] ADR shows diversity in SF usage.")
    sys.exit(0)


if __name__ == "__main__":
    main()

