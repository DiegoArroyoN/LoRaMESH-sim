#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import shutil
import statistics
import subprocess
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"

PROFILE = "pueyo2024_paper_like"
TOPOLOGY = "pueyo_grid"
LOAD = "low"
SPACING_M = 177
SIDES = [3, 4, 5, 6, 7, 8]  # N = 9,16,25,36,49,64
SEEDS = [1, 2, 3]
DATA_START_SEC = 300.0
DRAIN_SEC = 600.0
PDR_END_WINDOW_SEC = DRAIN_SEC

REUSE_DIRS = [
    BASE_DIR / "validation_results" / "pueyo_low_full_workload_n9_only_20260311",
    BASE_DIR / "validation_results" / "pueyo_fig11a_trend_replication_20260320_energy_off",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_command(cmd: list[str], cwd: Path, log_path: Path) -> subprocess.CompletedProcess[str]:
    ensure_dir(log_path.parent)
    with log_path.open("w", encoding="utf-8") as f:
        return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames or list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


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


def packets_per_node(n_nodes: int) -> int:
    return 100 * (n_nodes - 1)


def expected_generated_total(n_nodes: int) -> int:
    return n_nodes * packets_per_node(n_nodes)


def data_stop_sec(n_nodes: int) -> float:
    return DATA_START_SEC + packets_per_node(n_nodes) * 100.0


def stop_sec(n_nodes: int) -> float:
    return data_stop_sec(n_nodes) + DRAIN_SEC


def dict_to_cli(args: dict[str, object]) -> str:
    return " ".join(f"--{k}={v}" for k, v in args.items())


def build_args(side: int, seed: int) -> dict[str, object]:
    n_nodes = side * side
    area = (side - 1) * SPACING_M
    return {
        "profile": PROFILE,
        "nodePlacementMode": TOPOLOGY,
        "pueyoGridSide": side,
        "pueyoGridSpacingM": SPACING_M,
        "nEd": n_nodes,
        "areaWidth": area,
        "areaHeight": area,
        "trafficLoad": LOAD,
        "trafficMode": "pueyo_all_to_all",
        "pueyoPacketsPerPair": 100,
        "dataStartSec": DATA_START_SEC,
        "dataStopSec": data_stop_sec(n_nodes),
        "stopSec": stop_sec(n_nodes),
        "pdrEndWindowSec": PDR_END_WINDOW_SEC,
        "enablePcap": "false",
        "verboseLogs": "false",
        "enableNs3EnergyFramework": "false",
        "rngRun": seed,
    }


def summary_matches_expected(summary: dict, side: int) -> bool:
    sim = summary.get("simulation", {})
    n_nodes = side * side
    return (
        sim.get("profile") == PROFILE
        and sim.get("topology") == TOPOLOGY
        and int(sim.get("n_nodes", -1)) == n_nodes
        and int(sim.get("sf_min", -1)) == 7
        and int(sim.get("sf_max", -1)) == 8
        and bool(sim.get("pueyo_flora_like_rx", False)) is True
        and bool(sim.get("enable_sf_scan_rx", True)) is False
        and bool(sim.get("enable_ns3_energy_framework", True)) is False
        and sim.get("wire_format") == "pueyo7b"
        and int(sim.get("channel_count", -1)) == 1
        and int(sim.get("reception_paths", -1)) == 1
        and abs(float(sim.get("data_stop_sec", -1)) - data_stop_sec(n_nodes)) < 1e-9
    )


def find_reusable_run(side: int, seed: int) -> Path | None:
    n_nodes = side * side
    candidates = [
        REUSE_DIRS[0] / "runs" / "full_workload" / TOPOLOGY / f"n{n_nodes}" / f"seed_{seed}",
        REUSE_DIRS[1] / "runs" / "phase1" / TOPOLOGY / f"n{n_nodes}" / f"seed_{seed}",
    ]
    for candidate in candidates:
        summary_path = candidate / "mesh_dv_summary.json"
        if not summary_path.exists():
            continue
        try:
            summary = load_json(summary_path)
        except Exception:
            continue
        if summary_matches_expected(summary, side):
            return candidate
    return None


def classify_damage(row: dict) -> str:
    first_tx = max(1.0, float(row["source_first_tx_count_mean"]))
    busy = float(row["rx_no_more_demodulators_mean"]) / first_tx
    interf = float(row["rx_post_lock_interference_fail_mean"]) / first_tx
    no_route = float(row["drop_no_route_mean"]) / first_tx
    backlog = float(row["queued_packets_end_mean"])

    if no_route > 0.02:
        return "routing/no-route visible"
    if backlog > 1.0:
        return "queue/backlog visible"
    if busy > interf * 1.5:
        return "single active RX saturation dominates"
    if interf > busy * 1.2:
        return "post-lock interference dominates"
    return "mixed RX saturation + interference"


def extract_raw_row(summary: dict, side: int, seed: int, run_dir: Path, elapsed_s: float, reused_from: str = "") -> dict:
    sim = summary["simulation"]
    pdr = summary["pdr"]
    delay = summary["delay"]
    cp = summary["control_plane"]
    fwd = summary["forwarding"]
    q = summary["queue_backlog"]
    routes = summary["routes"]
    drops = summary["drops"]
    n_nodes = side * side
    expected = expected_generated_total(n_nodes)
    generated = int(pdr["total_data_generated"])
    return {
        "profile": sim["profile"],
        "topology": TOPOLOGY,
        "spacing_m": SPACING_M,
        "grid_side": side,
        "n_nodes": n_nodes,
        "load": LOAD,
        "seed": seed,
        "data_start_sec": sim["data_start_sec"],
        "data_stop_sec": sim["data_stop_sec"],
        "stop_sec": sim["stop_sec"],
        "workload_target": expected,
        "generated_count": generated,
        "workload_completed_fraction": (float(generated) / float(expected)) if expected > 0 else 0.0,
        "tx_sent_count": int(cp["data_tx_sent"]),
        "delivered_count": int(pdr["delivered"]),
        "pdr": float(pdr["delivery_ratio"]),
        "delay_avg_s": float(delay["avg_s"]),
        "delay_p95_s": float(delay["p95_s"]),
        "beacons_tx": int(cp["beacon_tx_sent"]),
        "beacons_rx": int(cp["beacon_rx_ok"]),
        "forwarded_unique_count": int(fwd["forwarded_unique_count"]),
        "routes_total": int(routes["routes_total"]),
        "routes_primary_total": int(routes["routes_primary_total"]),
        "routes_backup_total": int(routes["routes_backup_total"]),
        "drop_no_route": int(drops["drop_no_route"]),
        "drop_no_route_src": int(drops["drop_no_route_src"]),
        "drop_no_route_relay": int(drops["drop_no_route_relay"]),
        "rx_post_lock_interference_fail": int(cp["rx_post_lock_interference_fail"]),
        "rx_no_more_demodulators": int(cp["rx_no_more_demodulators"]),
        "rx_scan_miss_before_lock": int(cp["rx_scan_miss_before_lock"]),
        "pueyo_same_sf_overlap_events": int(cp["pueyo_same_sf_overlap_events"]),
        "pueyo_destructive_overlap_drops": int(cp["pueyo_destructive_overlap_drops"]),
        "pueyo_capture_or_timing_survivals": int(cp["pueyo_capture_or_timing_survivals"]),
        "queued_packets_at_data_stop": int(q["queued_packets_at_data_stop"]),
        "queued_packets_end": int(q["queued_packets_end"]),
        "source_first_tx_count": int(pdr["source_first_tx_count"]),
        "wire_format": sim["wire_format"],
        "sf_min": int(sim["sf_min"]),
        "sf_max": int(sim["sf_max"]),
        "pueyo_flora_like_rx": bool(sim["pueyo_flora_like_rx"]),
        "enable_sf_scan_rx": bool(sim["enable_sf_scan_rx"]),
        "enable_ns3_energy_framework": bool(sim["enable_ns3_energy_framework"]),
        "interference_model": sim["interference_model"],
        "channel_count": int(sim["channel_count"]),
        "reception_paths": int(sim["reception_paths"]),
        "run_dir": str(run_dir),
        "elapsed_s": elapsed_s,
        "reused_from": reused_from,
    }


def aggregate_rows(rows: list[dict]) -> list[dict]:
    grouped: dict[int, list[dict]] = {}
    for row in rows:
        grouped.setdefault(int(row["n_nodes"]), []).append(row)

    metrics = [
        "generated_count",
        "workload_completed_fraction",
        "tx_sent_count",
        "delivered_count",
        "pdr",
        "delay_avg_s",
        "delay_p95_s",
        "beacons_tx",
        "beacons_rx",
        "forwarded_unique_count",
        "routes_total",
        "drop_no_route",
        "drop_no_route_src",
        "drop_no_route_relay",
        "rx_post_lock_interference_fail",
        "rx_no_more_demodulators",
        "pueyo_same_sf_overlap_events",
        "pueyo_destructive_overlap_drops",
        "pueyo_capture_or_timing_survivals",
        "queued_packets_end",
        "source_first_tx_count",
    ]

    out = []
    for n_nodes, rr in sorted(grouped.items()):
        row = {
            "profile_reference": PROFILE,
            "topology": TOPOLOGY,
            "spacing_m": SPACING_M,
            "n_nodes": n_nodes,
            "seed_set": "1,2,3",
            "n": len(rr),
            "workload_target": expected_generated_total(n_nodes),
        }
        for metric in metrics:
            vals = [float(x[metric]) for x in rr]
            m, s, lo, hi = mean_std_ci95(vals)
            row[f"{metric}_mean"] = m
            row[f"{metric}_std"] = s
            row[f"{metric}_ci95_lo"] = lo
            row[f"{metric}_ci95_hi"] = hi
        row["damage_diagnosis"] = classify_damage(row)
        out.append(row)
    return out


def build_report(outdir: Path, agg_rows: list[dict]) -> None:
    lines = []
    lines.append("# pueyo2024_paper_like grid low SF7-8 N sweep\n\n")
    lines.append("- Caso fijo: `pueyo_grid`, `spacing=177 m`, `load=low`, `SF7-8`, `seeds=1,2,3`\n")
    lines.append("- Perfil: `pueyo2024_paper_like`\n")
    lines.append("- Horizonte temporal por N: full-workload (`100 packets/pair`, `100 s` entre paquetes) + `600 s` de drenaje\n\n")
    lines.append("## Average PDR vs Number of nodes\n\n")
    lines.append("| N | PDR_mean | PDR_std | delay_avg_s | delivered_mean | diagnosis |\n")
    lines.append("|---|---:|---:|---:|---:|---|\n")
    for row in agg_rows:
        lines.append(
            f"| {row['n_nodes']} | {row['pdr_mean']:.6f} | {row['pdr_std']:.6f} | "
            f"{row['delay_avg_s_mean']:.6f} | {row['delivered_count_mean']:.1f} | "
            f"{row['damage_diagnosis']} |\n"
        )
    lines.append("\n## Daño dominante al PDR por N\n\n")
    for row in agg_rows:
        first_tx = max(1.0, float(row["source_first_tx_count_mean"]))
        lines.append(f"### N={row['n_nodes']}\n\n")
        lines.append(f"- PDR medio: `{row['pdr_mean']:.6f}`\n")
        lines.append(f"- Datos generados: `{row['generated_count_mean']:.1f}` / `{row['workload_target']}`\n")
        lines.append(f"- Datos enviados al aire: `{row['tx_sent_count_mean']:.1f}`\n")
        lines.append(f"- Datos entregados: `{row['delivered_count_mean']:.1f}`\n")
        lines.append(f"- `rx_no_more_demodulators`: `{row['rx_no_more_demodulators_mean']:.1f}` (`{row['rx_no_more_demodulators_mean']/first_tx:.3f}` por first-tx)\n")
        lines.append(f"- `rx_post_lock_interference_fail`: `{row['rx_post_lock_interference_fail_mean']:.1f}` (`{row['rx_post_lock_interference_fail_mean']/first_tx:.3f}` por first-tx)\n")
        lines.append(f"- `pueyo_destructive_overlap_drops`: `{row['pueyo_destructive_overlap_drops_mean']:.1f}`\n")
        lines.append(f"- `drop_no_route`: `{row['drop_no_route_mean']:.1f}`\n")
        lines.append(f"- Diagnóstico: `{row['damage_diagnosis']}`\n\n")
    (outdir / "pueyo_paper_like_grid_n_sweep_report.md").write_text("".join(lines), encoding="utf-8")


def build_summary(outdir: Path, agg_rows: list[dict], raw_rows: list[dict]) -> None:
    summary = {
        "outdir": str(outdir),
        "profile_reference": PROFILE,
        "topology": TOPOLOGY,
        "spacing_m": SPACING_M,
        "load": LOAD,
        "sf_range": "7-8",
        "n_nodes": [row["n_nodes"] for row in agg_rows],
        "completed_runs": len(raw_rows),
        "expected_runs": len(SIDES) * len(SEEDS),
        "average_pdr_vs_n": [
            {
                "n_nodes": row["n_nodes"],
                "pdr_mean": row["pdr_mean"],
                "pdr_std": row["pdr_std"],
                "damage_diagnosis": row["damage_diagnosis"],
            }
            for row in agg_rows
        ],
    }
    with (outdir / "pueyo_paper_like_grid_n_sweep_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=None)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    outdir = args.outdir or (
        BASE_DIR / "validation_results" / f"pueyo_paper_like_grid_n_sweep_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    ensure_dir(outdir)

    rows: list[dict] = []
    total = len(SIDES) * len(SEEDS)
    idx = 0
    for side in SIDES:
        n_nodes = side * side
        for seed in SEEDS:
            idx += 1
            run_dir = outdir / "runs" / f"n{n_nodes}" / f"seed_{seed}"
            summary_path = run_dir / "mesh_dv_summary.json"
            reused_from = ""
            if args.resume and summary_path.exists():
                summary = load_json(summary_path)
                rows.append(extract_raw_row(summary, side, seed, run_dir, 0.0))
                print(f"[{idx:02d}/{total}] reuse-local N={n_nodes} seed={seed}", flush=True)
            else:
                reusable = find_reusable_run(side, seed)
                if reusable is not None:
                    ensure_dir(run_dir)
                    shutil.copy2(reusable / "mesh_dv_summary.json", summary_path)
                    if (reusable / "run.log").exists():
                        shutil.copy2(reusable / "run.log", run_dir / "run.log")
                    summary = load_json(summary_path)
                    reused_from = str(reusable)
                    rows.append(extract_raw_row(summary, side, seed, run_dir, 0.0, reused_from))
                    print(f"[{idx:02d}/{total}] reuse-cache N={n_nodes} seed={seed}", flush=True)
                else:
                    cfg = build_args(side, seed)
                    sim_args = "mesh_dv_baseline " + dict_to_cli(cfg)
                    cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
                    print(
                        f"[{idx:02d}/{total}] run N={n_nodes} seed={seed} "
                        f"dataStop={data_stop_sec(n_nodes):.0f}s stop={stop_sec(n_nodes):.0f}s",
                        flush=True,
                    )
                    t0 = time.time()
                    proc = run_command(cmd, NS3_DIR, run_dir / "run.log")
                    if proc.returncode != 0:
                        raise SystemExit(f"run failed rc={proc.returncode} at {run_dir}")
                    root_summary = NS3_DIR / "mesh_dv_summary.json"
                    if not root_summary.exists():
                        raise SystemExit(f"missing mesh_dv_summary.json after {run_dir}")
                    ensure_dir(run_dir)
                    shutil.copy2(root_summary, summary_path)
                    summary = load_json(summary_path)
                    rows.append(extract_raw_row(summary, side, seed, run_dir, time.time() - t0))
            write_csv(outdir / "pueyo_paper_like_grid_n_sweep_results_raw.csv", rows, list(rows[0].keys()))

    agg_rows = aggregate_rows(rows)
    write_csv(outdir / "pueyo_paper_like_grid_n_sweep_results.csv", agg_rows, list(agg_rows[0].keys()))
    build_summary(outdir, agg_rows, rows)
    build_report(outdir, agg_rows)
    print(outdir)


if __name__ == "__main__":
    main()
