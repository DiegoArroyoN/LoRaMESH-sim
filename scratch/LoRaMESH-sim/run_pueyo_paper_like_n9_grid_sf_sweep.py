#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import json
import math
import argparse
import statistics
import subprocess
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"

PROFILE_SEMANTICS = "pueyo2024_paper_like"
TOPOLOGY = "pueyo_grid"
SPACING_M = 177
GRID_SIDE = 3
N_NODES = GRID_SIDE * GRID_SIDE
AREA = (GRID_SIDE - 1) * SPACING_M
SEEDS = [1, 2, 3]
DATA_START_SEC = 300.0
DRAIN_SEC = 600.0
PDR_END_WINDOW_SEC = 600.0
SF_RANGES = [
    (7, 8),
    (7, 9),
    (7, 10),
    (7, 11),
    (7, 12),
    (8, 9),
    (8, 10),
    (8, 11),
    (8, 12),
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_command(cmd: list[str], cwd: Path, log_path: Path) -> subprocess.CompletedProcess[str]:
    ensure_dir(log_path.parent)
    with log_path.open("w", encoding="utf-8") as f:
        return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def mean_std_ci95(values: list[float]) -> tuple[float, float, float, float]:
    if not values:
        return 0.0, 0.0, 0.0, 0.0
    if len(values) == 1:
        x = float(values[0])
        return x, 0.0, x, x
    m = float(statistics.mean(values))
    s = float(statistics.stdev(values))
    h = 1.96 * (s / math.sqrt(len(values)))
    return m, s, m - h, m + h


def dict_to_cli(args: dict[str, object]) -> str:
    return " ".join(f"--{k}={v}" for k, v in args.items())


def pkts_per_node() -> int:
    return 100 * (N_NODES - 1)


def expected_generated_total() -> int:
    return N_NODES * pkts_per_node()


def data_stop_sec() -> float:
    return DATA_START_SEC + pkts_per_node() * 100.0


def stop_sec() -> float:
    return data_stop_sec() + DRAIN_SEC


def make_cli_args(sf_min: int, sf_max: int, seed: int) -> dict[str, object]:
    return {
        "profile": "extended",
        "enableCsma": "false",
        "enableDuty": "false",
        "dutyLimit": 1.0,
        "routeMetricMode": "toa_only",
        "sfLinkMode": "deterministic_sensitivity",
        "sfLinkMarginDb": 0.0,
        "wireFormat": "pueyo7b",
        "txPowerDbm": 20,
        "preambleSymbols": 16,
        "initTtl": 63,
        "useProbabilisticSfForBeacons": "true",
        "interferenceModel": "pueyo_fixed_capture",
        "shadowingSigmaDb": 3.57,
        "trafficMode": "pueyo_all_to_all",
        "pueyoPacketsPerPair": 100,
        "beaconIntervalWarmSec": 60,
        "beaconIntervalStableSec": 60,
        "routeTimeoutFactor": 5,
        "routeSwitchMinDeltaX100": 0,
        "dvPayloadMaxBytes": 251,
        "routeAdvertPolicy": "cost_weighted",
        "maxRoutesPerDestination": 2,
        "maxTotalRoutes": 1024,
        "costEncoding": "cost255",
        "beaconLatestOnly": "false",
        "prioritizeBeacons": "false",
        "pueyoStrictQueueScheduler": "true",
        "controlBackoffFactor": 1.0,
        "dataBackoffFactor": 10.0,
        "enablePcap": "false",
        "verboseLogs": "false",
        "dataPeriodJitterMaxSec": 0.0,
        "sfScanResetOnNewSignal": "true",
        "sfScanEdThresholdDbm": -120.0,
        "dataPayloadSizeBytes": 20,
        "enableNs3EnergyFramework": "false",
        "pueyoFloraLikeRx": "true",
        "enableSfScanRx": "false",
        "nodePlacementMode": TOPOLOGY,
        "pueyoGridSide": GRID_SIDE,
        "pueyoGridSpacingM": SPACING_M,
        "nEd": N_NODES,
        "areaWidth": AREA,
        "areaHeight": AREA,
        "trafficLoad": "low",
        "dataStartSec": DATA_START_SEC,
        "dataStopSec": data_stop_sec(),
        "stopSec": stop_sec(),
        "pdrEndWindowSec": PDR_END_WINDOW_SEC,
        "rngRun": seed,
        "sfMin": sf_min,
        "sfMax": sf_max,
    }


def extract_row(summary: dict, sf_min: int, sf_max: int, seed: int, run_dir: Path, elapsed_s: float) -> dict:
    cp = summary["control_plane"]
    return {
        "profile_reference": PROFILE_SEMANTICS,
        "profile_runtime": summary["simulation"]["profile"],
        "topology": TOPOLOGY,
        "spacing_m": SPACING_M,
        "n_nodes": N_NODES,
        "load": "low",
        "sf_range": f"{sf_min}-{sf_max}",
        "sf_min": sf_min,
        "sf_max": sf_max,
        "seed": seed,
        "generated_count": summary["pdr"]["total_data_generated"],
        "source_first_tx_count": summary["tx_attempts"]["source_first_tx_count"],
        "delivered_count": summary["pdr"]["delivered"],
        "delivery_ratio": summary["pdr"]["delivery_ratio"],
        "delay_avg_s": summary["delay"]["avg_s"],
        "delay_p95_s": summary["delay"]["p95_s"],
        "beacon_tx_sent": cp["beacon_tx_sent"],
        "beacon_rx_ok": cp["beacon_rx_ok"],
        "data_tx_sent": cp["data_tx_sent"],
        "forwarded_unique_count": summary["forwarding"]["forwarded_unique_count"],
        "routes_total": summary["routes"]["routes_total"],
        "rx_post_lock_interference_fail": cp["rx_post_lock_interference_fail"],
        "rx_no_more_demodulators": cp["rx_no_more_demodulators"],
        "rx_scan_attempts": cp["rx_scan_attempts"],
        "rx_scan_locks": cp["rx_scan_locks"],
        "run_dir": str(run_dir),
        "elapsed_s": elapsed_s,
    }


def aggregate_rows(rows: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(str(row["sf_range"]), []).append(row)

    metrics = [
        "generated_count",
        "source_first_tx_count",
        "delivered_count",
        "delivery_ratio",
        "delay_avg_s",
        "delay_p95_s",
        "beacon_tx_sent",
        "beacon_rx_ok",
        "data_tx_sent",
        "forwarded_unique_count",
        "routes_total",
        "rx_post_lock_interference_fail",
        "rx_no_more_demodulators",
    ]

    out = []
    for sf_range, rr in sorted(grouped.items(), key=lambda item: tuple(map(int, item[0].split("-")))):
        row = {
            "profile_reference": PROFILE_SEMANTICS,
            "topology": TOPOLOGY,
            "spacing_m": SPACING_M,
            "n_nodes": N_NODES,
            "load": "low",
            "sf_range": sf_range,
            "sf_min": rr[0]["sf_min"],
            "sf_max": rr[0]["sf_max"],
            "seed_set": "1,2,3",
            "n": len(rr),
            "expected_generated_total": expected_generated_total(),
        }
        for metric in metrics:
            values = [float(x[metric]) for x in rr]
            m, s, lo, hi = mean_std_ci95(values)
            row[f"{metric}_mean"] = m
            row[f"{metric}_std"] = s
            row[f"{metric}_ci95_lo"] = lo
            row[f"{metric}_ci95_hi"] = hi
        row["workload_completed_fraction_mean"] = (
            row["generated_count_mean"] / row["expected_generated_total"]
            if row["expected_generated_total"] > 0
            else 0.0
        )
        out.append(row)
    return out


def build_summary(outdir: Path, agg_rows: list[dict]) -> None:
    by_range = {row["sf_range"]: row for row in agg_rows}
    baseline = by_range["7-8"]
    best = max(agg_rows, key=lambda row: (row["delivery_ratio_mean"], -row["delay_avg_s_mean"]))
    summary = {
        "outdir": str(outdir),
        "profile_reference": PROFILE_SEMANTICS,
        "runtime_profile_semantics": "extended clone matching pueyo2024_paper_like except sfMin/sfMax",
        "case": {
            "topology": TOPOLOGY,
            "spacing_m": SPACING_M,
            "n_nodes": N_NODES,
            "load": "low",
            "data_start_sec": DATA_START_SEC,
            "data_stop_sec": data_stop_sec(),
            "stop_sec": stop_sec(),
            "expected_generated_total": expected_generated_total(),
        },
        "baseline_7_8": {
            "pdr_mean": baseline["delivery_ratio_mean"],
            "delay_avg_s_mean": baseline["delay_avg_s_mean"],
        },
        "best_sf_range": {
            "sf_range": best["sf_range"],
            "pdr_mean": best["delivery_ratio_mean"],
            "delay_avg_s_mean": best["delay_avg_s_mean"],
            "delta_vs_7_8": best["delivery_ratio_mean"] - baseline["delivery_ratio_mean"],
        },
    }
    with (outdir / "pueyo_paper_like_n9_grid_sf_sweep_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def build_report(outdir: Path, agg_rows: list[dict]) -> None:
    by_range = {row["sf_range"]: row for row in agg_rows}
    baseline = by_range["7-8"]
    lines = []
    lines.append("# Barrido SF sobre caso `pueyo2024_paper_like` N=9 grid low full-workload\n\n")
    lines.append("- Caso fijo: `pueyo_grid`, `N=9`, `spacing=177 m`, `load=low`, `seeds=1,2,3`\n")
    lines.append("- Horizonte temporal: `dataStart=300 s`, `dataStop=80300 s`, `stopSec=80900 s`\n")
    lines.append("- Semántica fija: clon de `pueyo2024_paper_like` con `wireFormat=pueyo7b`, beacon Pueyo, `toa_only`, `pueyoFloraLikeRx=true`, `EnableSfScanRx=false`, `enableNs3EnergyFramework=false`\n")
    lines.append("- Lo único que varía: `sfMin/sfMax`\n\n")
    lines.append("## Referencia base\n\n")
    lines.append(
        f"- `7-8`: `PDR={baseline['delivery_ratio_mean']:.6f}`, "
        f"`delay_avg_s={baseline['delay_avg_s_mean']:.6f}`, "
        f"`delivered_mean={baseline['delivered_count_mean']:.1f}`\n\n"
    )
    lines.append("## Resultados\n\n")
    lines.append("| sf_range | PDR_mean | delta_vs_7_8 | delay_avg_s | delivered_mean | post_lock_fail_mean | no_more_demod_mean |\n")
    lines.append("|---|---:|---:|---:|---:|---:|---:|\n")
    for row in agg_rows:
        delta = row["delivery_ratio_mean"] - baseline["delivery_ratio_mean"]
        lines.append(
            f"| {row['sf_range']} | {row['delivery_ratio_mean']:.6f} | {delta:+.6f} | "
            f"{row['delay_avg_s_mean']:.6f} | {row['delivered_count_mean']:.1f} | "
            f"{row['rx_post_lock_interference_fail_mean']:.1f} | {row['rx_no_more_demodulators_mean']:.1f} |\n"
        )
    lines.append("\n")
    best = max(agg_rows, key=lambda row: (row["delivery_ratio_mean"], -row["delay_avg_s_mean"]))
    worst = min(agg_rows, key=lambda row: row["delivery_ratio_mean"])
    lines.append("## Lectura rápida\n\n")
    lines.append(
        f"- Mejor rango por PDR: `{best['sf_range']}` con `PDR={best['delivery_ratio_mean']:.6f}` "
        f"(`delta_vs_7_8={best['delivery_ratio_mean'] - baseline['delivery_ratio_mean']:+.6f}`)\n"
    )
    lines.append(
        f"- Peor rango por PDR: `{worst['sf_range']}` con `PDR={worst['delivery_ratio_mean']:.6f}`\n"
    )
    lines.append(
        "- Si abrir más SF mejora el PDR, la mejora debería venir acompañada de menos "
        "`rx_post_lock_interference_fail` y/o menos `rx_no_more_demodulators`; si empeora, "
        "significa que el mayor tiempo en aire dominó sobre cualquier beneficio de desacoplar tráfico.\n"
    )
    (outdir / "pueyo_paper_like_n9_grid_sf_sweep_report.md").write_text("".join(lines), encoding="utf-8")


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=None)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    outdir = args.outdir or (
        BASE_DIR / "validation_results" / f"pueyo_paper_like_n9_grid_sf_sweep_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    ensure_dir(outdir)

    rows = []
    total = len(SF_RANGES) * len(SEEDS)
    idx = 0
    for sf_min, sf_max in SF_RANGES:
        for seed in SEEDS:
            idx += 1
            run_dir = outdir / f"sf_{sf_min}_{sf_max}" / f"seed_{seed}"
            cached_summary = run_dir / "mesh_dv_summary.json"
            if args.resume and cached_summary.exists():
                print(f"[{idx:02d}/{total}] reuse sf={sf_min}-{sf_max} seed={seed}", flush=True)
                summary = load_json(cached_summary)
                rows.append(extract_row(summary, sf_min, sf_max, seed, run_dir, 0.0))
            else:
                cfg = make_cli_args(sf_min, sf_max, seed)
                sim_args = "mesh_dv_baseline " + dict_to_cli(cfg)
                cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
                print(f"[{idx:02d}/{total}] sf={sf_min}-{sf_max} seed={seed}", flush=True)
                t0 = time.time()
                proc = run_command(cmd, NS3_DIR, run_dir / "run.log")
                if proc.returncode != 0:
                    raise SystemExit(f"run failed rc={proc.returncode} at {run_dir}")
                summary_path = NS3_DIR / "mesh_dv_summary.json"
                if not summary_path.exists():
                    raise SystemExit(f"missing mesh_dv_summary.json after {run_dir}")
                (run_dir / "mesh_dv_summary.json").write_bytes(summary_path.read_bytes())
                summary = load_json(run_dir / "mesh_dv_summary.json")
                rows.append(extract_row(summary, sf_min, sf_max, seed, run_dir, time.time() - t0))
            write_csv(outdir / "pueyo_paper_like_n9_grid_sf_sweep_results_raw.csv", rows, list(rows[0].keys()))

    agg_rows = aggregate_rows(rows)
    write_csv(
        outdir / "pueyo_paper_like_n9_grid_sf_sweep_results.csv",
        agg_rows,
        list(agg_rows[0].keys()),
    )
    build_summary(outdir, agg_rows)
    build_report(outdir, agg_rows)
    print(outdir)


if __name__ == "__main__":
    main()
