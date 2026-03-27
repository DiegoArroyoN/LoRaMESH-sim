#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

import pandas as pd


BASE_CAMPAIGN = Path(
    "/home/diego/ns3/ns-3-dev/scratch/LoRaMESH-sim/validation_results/"
    "pueyo_fig11_fig12_20260309_160712"
)


def classify_run(row: pd.Series) -> str:
    generated = float(row.get("generated_count", 0) or 0)
    data_tx = float(row.get("data_tx_sent", 0) or 0)
    routes_total = float(row.get("routes_total", 0) or 0)
    drop_nr_src = float(row.get("drop_no_route_src", 0) or 0)
    n_nodes = float(row.get("n_nodes", 0) or 0)
    possible_routes = max(n_nodes * (n_nodes - 1), 1.0)

    if generated > 0 and data_tx <= 0.05 * generated:
        return "A"
    if generated > 0 and (
        drop_nr_src > 0.2 * generated or routes_total < 0.25 * possible_routes
    ):
        return "B"
    return "C"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def extract_sweep_from_summary(summary_path: Path, topology: str, spacing_m: int, n_nodes: int) -> dict:
    summary = load_json(summary_path)
    sim = summary["simulation"]
    pdr = summary["pdr"]
    thr = summary["throughput"]
    routes = summary["routes"]
    drops = summary["drops"]
    cp = summary["control_plane"]
    return {
        "topology": topology,
        "spacing_m": spacing_m,
        "n_nodes": n_nodes,
        "load": sim["traffic_load"],
        "sf_min": sim["sf_min"],
        "sf_max": sim["sf_max"],
        "delivery_ratio": pdr["delivery_ratio"],
        "goodput_bps": thr["goodput_bps"],
        "routes_total": routes["routes_total"],
        "drop_no_route_src": drops["drop_no_route_src"],
        "rx_scan_miss_before_lock": cp["rx_scan_miss_before_lock"],
        "rx_post_lock_interference_fail": cp["rx_post_lock_interference_fail"],
    }


def build_tidy_csv(base_dir: Path) -> pd.DataFrame:
    agg = pd.read_csv(base_dir / "results_agg.csv")
    mask = (
        agg["grid_side"].isin([3, 5, 8])
        & agg["grid_spacing_m"].isin([177, 247])
        & agg["topology"].isin(["pueyo_grid", "pueyo_random_equiv"])
        & agg["profile"].isin(["pueyo2024", "proposal_pueyo_like"])
        & agg["load"].isin(["low", "medium"])
        & (agg["seed_set"] == "1,2,3")
        & (agg["n"] == 3)
    )
    tidy = agg.loc[
        mask,
        [
            "topology",
            "grid_spacing_m",
            "load",
            "n_nodes",
            "profile",
            "delivery_ratio_mean",
            "delivery_ratio_std",
            "delivery_ratio_ci95_lo",
            "delivery_ratio_ci95_hi",
            "goodput_bps_mean",
            "delay_p95_s_mean",
            "seed_set",
            "n",
        ],
    ].copy()
    tidy = tidy.rename(columns={"grid_spacing_m": "spacing_m", "n": "n_seeds"})
    tidy = tidy.sort_values(
        ["topology", "spacing_m", "load", "n_nodes", "profile"]
    ).reset_index(drop=True)
    return tidy


def build_preflight_csv(base_dir: Path) -> pd.DataFrame:
    runs = pd.read_csv(base_dir / "results_runs.csv")
    mask = (
        runs["grid_side"].isin([3, 5, 8])
        & runs["grid_spacing_m"].isin([177, 247])
        & runs["topology"].isin(["pueyo_grid", "pueyo_random_equiv"])
        & runs["profile"].isin(["pueyo2024", "proposal_pueyo_like"])
        & runs["load"].isin(["low", "medium"])
        & runs["seed"].isin([1, 2, 3])
    )
    preflight = runs.loc[
        mask,
        [
            "run_id",
            "profile",
            "topology",
            "grid_side",
            "grid_spacing_m",
            "n_nodes",
            "load",
            "seed",
            "preflight_ok",
            "check_area_formula",
            "check_n_nodes_square",
            "check_shadowing_sigma",
            "check_wire_pueyo7b",
            "check_payload_27b",
        ],
    ].copy()
    preflight = preflight.rename(columns={"grid_spacing_m": "spacing_m"})
    preflight = preflight.sort_values(
        ["topology", "spacing_m", "load", "n_nodes", "profile", "seed"]
    ).reset_index(drop=True)
    return preflight


def build_pdr_zero_csv(base_dir: Path) -> pd.DataFrame:
    zero = pd.read_csv(base_dir / "pdr_zero_diagnosis_expanded.csv")
    zero["n_nodes"] = zero["grid_side"] * zero["grid_side"]
    zero["class"] = zero.apply(classify_run, axis=1)
    zero = zero.loc[
        :,
        [
            "run_id",
            "topology",
            "spacing_m",
            "load",
            "seed",
            "generated_count",
            "data_tx_sent",
            "delivered_count",
            "routes_total",
            "drop_no_route_src",
            "drop_no_route_relay",
            "rx_scan_attempts",
            "rx_scan_locks",
            "rx_scan_miss_before_lock",
            "rx_post_lock_interference_fail",
            "control_tx_sent",
            "beacon_tx_sent",
            "class",
        ],
    ].copy()
    zero = zero.sort_values(
        ["topology", "spacing_m", "load", "seed", "run_id"]
    ).reset_index(drop=True)
    return zero


def build_sweep_csv() -> pd.DataFrame:
    rows = []

    # n=9, spacing=177, grid
    base = Path(
        "/home/diego/ns3/ns-3-dev/scratch/LoRaMESH-sim/validation_results/"
        "pdr_zero_sfrange_grid_20260309_v2"
    )
    for variant in ["sf7_12", "sf7_9", "sf8_9"]:
        rows.append(
            extract_sweep_from_summary(
                base / variant / "mesh_dv_summary.json",
                topology="pueyo_grid",
                spacing_m=177,
                n_nodes=9,
            )
        )

    # n=9, spacing=177, random
    base = Path(
        "/home/diego/ns3/ns-3-dev/scratch/LoRaMESH-sim/validation_results/"
        "pdr_zero_sfrange_random_equiv_20260309"
    )
    for variant in ["sf7_12", "sf7_9", "sf8_9"]:
        rows.append(
            extract_sweep_from_summary(
                base / variant / "mesh_dv_summary.json",
                topology="pueyo_random_equiv",
                spacing_m=177,
                n_nodes=9,
            )
        )

    # n=9, spacing=247, grid/random
    base = Path(
        "/home/diego/ns3/ns-3-dev/scratch/LoRaMESH-sim/validation_results/"
        "pdr_sfrange_n9_spacing247_20260310"
    )
    for topo in ["pueyo_grid", "pueyo_random_equiv"]:
        for variant in ["sf7_12", "sf7_9", "sf8_9"]:
            rows.append(
                extract_sweep_from_summary(
                    base / f"{topo}_{variant}" / "mesh_dv_summary.json",
                    topology=topo,
                    spacing_m=247,
                    n_nodes=9,
                )
            )

    # n=25, spacing=177
    df = pd.read_csv(
        "/home/diego/ns3/ns-3-dev/scratch/LoRaMESH-sim/validation_results/"
        "pdr_sfrange_n25_20260309/sf_range_n25_summary.csv"
    )
    rows.extend(df.to_dict(orient="records"))

    # n=25, spacing=247
    df = pd.read_csv(
        "/home/diego/ns3/ns-3-dev/scratch/LoRaMESH-sim/validation_results/"
        "pdr_sfrange_n25_spacing247_20260309/sf_range_n25_spacing247_summary.csv"
    )
    rows.extend(df.to_dict(orient="records"))

    sweep = pd.DataFrame(rows)
    sweep = sweep.loc[
        :,
        [
            "topology",
            "spacing_m",
            "n_nodes",
            "load",
            "sf_min",
            "sf_max",
            "delivery_ratio",
            "goodput_bps",
            "routes_total",
            "drop_no_route_src",
            "rx_scan_miss_before_lock",
            "rx_post_lock_interference_fail",
        ],
    ].copy()
    sweep = sweep.sort_values(
        ["topology", "spacing_m", "n_nodes", "sf_min", "sf_max"]
    ).reset_index(drop=True)
    return sweep


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--base-campaign",
        default=str(BASE_CAMPAIGN),
        help="Directory with results_runs.csv/results_agg.csv",
    )
    ap.add_argument(
        "--outdir",
        default="/home/diego/ns3/ns-3-dev/scratch/LoRaMESH-sim/validation_results/"
        "fig11_fig12_report_pack_20260309",
        help="Output directory",
    )
    args = ap.parse_args()

    base_dir = Path(args.base_campaign)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    tidy = build_tidy_csv(base_dir)
    preflight = build_preflight_csv(base_dir)
    zero = build_pdr_zero_csv(base_dir)
    sweep = build_sweep_csv()

    tidy.to_csv(outdir / "fig11_fig12_tidy.csv", index=False)
    preflight.to_csv(outdir / "fig11_fig12_preflight_checks.csv", index=False)
    zero.to_csv(outdir / "pdr_zero_diagnosis.csv", index=False)
    sweep.to_csv(outdir / "toa_sf_range_sweep.csv", index=False)

    manifest = {
        "base_campaign": str(base_dir),
        "outdir": str(outdir),
        "files": [
            "fig11_fig12_tidy.csv",
            "fig11_fig12_preflight_checks.csv",
            "pdr_zero_diagnosis.csv",
            "toa_sf_range_sweep.csv",
        ],
        "rows": {
            "fig11_fig12_tidy": int(len(tidy)),
            "fig11_fig12_preflight_checks": int(len(preflight)),
            "pdr_zero_diagnosis": int(len(zero)),
            "toa_sf_range_sweep": int(len(sweep)),
        },
    }
    with (outdir / "manifest.json").open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)


if __name__ == "__main__":
    main()
