#!/usr/bin/env python3
"""
Run a paper-comparable DV-ToA campaign for Pueyo Fig.11-12:
- exact Pueyo all-to-all workload: 100*(n_nodes-1) packets per node
- fixed generation period per load
- full workload completion per node before dataStopSec
- SF-range sweep and best-of selection per (topology, spacing, n_nodes, load)

Outputs:
- results_runs.csv          : per-seed per-SF-range
- results_agg.csv           : aggregated per point per SF-range
- fig11_fig12_paper_ready.csv : best-of dataset for the paper
- point_logs/*.json         : one mini-log JSON per best-of point
- campaign_summary.json
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple


BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Pueyo Fig.11-12 DV-ToA best-of SF-range campaign")
    p.add_argument("--build", action="store_true", default=False)
    p.add_argument("--resume", action="store_true", default=True)
    p.add_argument("--no-resume", dest="resume", action="store_false")
    p.add_argument("--sides", type=str, default="3 4 5 6 7 8")
    p.add_argument("--spacings", type=str, default="177 178 246 247")
    p.add_argument("--topologies", type=str, default="pueyo_grid pueyo_random_equiv")
    p.add_argument("--loads", type=str, default="low medium")
    p.add_argument("--seeds", type=str, default="1 2 3 4 5")
    p.add_argument(
        "--sf-ranges",
        type=str,
        default="7-8 7-9 8-9 7-10 8-10 9-10 7-11 8-11 9-11 10-11 7-12 8-12 9-12 10-12 11-12",
    )
    p.add_argument("--data-start-sec", type=float, default=300.0)
    p.add_argument("--drain-sec", type=float, default=600.0)
    p.add_argument("--shadowing-sigma-db", type=float, default=3.57)
    p.add_argument(
        "--sf-scan-ed-threshold-dbm",
        type=float,
        default=-140.0,
        help="ED gate for RX-scan. Use -140 to make the gate effectively inactive across candidate SF ranges.",
    )
    p.add_argument("--outdir", type=str, default=None)
    p.add_argument("--max-runs", type=int, default=0, help="0 = run all planned runs")
    return p.parse_args()


def parse_int_list(spec: str) -> List[int]:
    vals = [int(tok) for tok in spec.replace(",", " ").split() if tok.strip()]
    if not vals:
        raise ValueError("empty int list")
    return vals


def parse_str_list(spec: str) -> List[str]:
    vals = [tok.strip() for tok in spec.replace(",", " ").split() if tok.strip()]
    if not vals:
        raise ValueError("empty str list")
    return vals


def parse_sf_ranges(spec: str) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    for tok in spec.replace(",", " ").split():
        lo_s, hi_s = tok.strip().split("-")
        lo = int(lo_s)
        hi = int(hi_s)
        if not (7 <= lo <= hi <= 12):
            raise ValueError(f"invalid SF range {tok}")
        ranges.append((lo, hi))
    if not ranges:
        raise ValueError("empty sf range list")
    return ranges


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_command(cmd: List[str], cwd: Path, log_path: Path | None = None) -> subprocess.CompletedProcess:
    if log_path is not None:
        ensure_dir(log_path.parent)
        with log_path.open("w", encoding="utf-8") as f:
            return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)
    return subprocess.run(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)


def get_git_commit_hash(repo_dir: Path) -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return (proc.stdout.strip() if proc.returncode == 0 else "") or "unknown"


def dict_to_cli(args: Dict[str, object]) -> str:
    return " ".join([f"--{k}={v}" for k, v in args.items()])


def load_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def mean_std_ci95(values: List[float]) -> Tuple[float, float, Tuple[float, float]]:
    if not values:
        return 0.0, 0.0, (0.0, 0.0)
    if len(values) == 1:
        x = float(values[0])
        return x, 0.0, (x, x)
    m = float(statistics.mean(values))
    s = float(statistics.stdev(values))
    h = 1.96 * (s / math.sqrt(len(values)))
    return m, s, (m - h, m + h)


def traffic_interval_from_load(load: str) -> float:
    normalized = load.strip().lower()
    if normalized == "low":
        return 100.0
    if normalized == "medium":
        return 10.0
    if normalized == "high":
        return 1.0
    if normalized == "saturation":
        return 0.1
    raise ValueError(f"unsupported load {load}")


def required_run_artifacts(run_dir: Path) -> bool:
    return (
        (run_dir / "mesh_dv_summary.json").exists()
        and (run_dir / "run.log").exists()
        and (run_dir / "meta.json").exists()
    )


def run_one(run_dir: Path, sim_args: str, resume: bool, git_commit: str) -> Dict:
    ensure_dir(run_dir)
    summary_dst = run_dir / "mesh_dv_summary.json"
    log_path = run_dir / "run.log"
    meta_path = run_dir / "meta.json"

    if resume and required_run_artifacts(run_dir):
        meta = load_json(meta_path)
        if not str(meta.get("git_commit", "")).strip():
            meta["git_commit"] = git_commit
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return {"summary": load_json(summary_dst), "summary_path": summary_dst, "meta": meta, "resumed": True}

    cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
    t0 = time.time()
    proc = run_command(cmd, NS3_DIR, log_path=log_path)
    elapsed = time.time() - t0
    if proc.returncode != 0:
        raise RuntimeError(f"simulation failed rc={proc.returncode} run_dir={run_dir}")

    summary_src = NS3_DIR / "mesh_dv_summary.json"
    if not summary_src.exists():
        raise RuntimeError("mesh_dv_summary.json missing after run")
    summary_dst.write_bytes(summary_src.read_bytes())

    meta = {
        "cmd": " ".join(cmd),
        "sim_args": sim_args,
        "elapsed_s": elapsed,
        "timestamp": dt.datetime.now().isoformat(),
        "git_commit": git_commit,
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return {"summary": load_json(summary_dst), "summary_path": summary_dst, "meta": meta, "resumed": False}


def topology_label(topology: str) -> str:
    if topology == "pueyo_grid":
        return "grid"
    if topology == "pueyo_random_equiv":
        return "random_equiv"
    return topology


def validate_run(summary: Dict, expected_generated_total: int, side: int, spacing: int, topology: str, sigma: float) -> Dict:
    sim = summary.get("simulation", {})
    pdr = summary.get("pdr", {})
    area_expected = float((side - 1) * spacing)

    checks = {
        "check_area_formula": int(
            abs(float(sim.get("area_w_m", float("nan"))) - area_expected) < 1e-9
            and abs(float(sim.get("area_h_m", float("nan"))) - area_expected) < 1e-9
        ),
        "check_n_nodes_square": int(int(sim.get("n_nodes", 0)) == side * side),
        "check_shadowing_sigma": int(abs(float(sim.get("shadowing_sigma_db", float("nan"))) - sigma) < 1e-9),
        "check_wire_pueyo7b": int(str(sim.get("wire_format", "")) == "pueyo7b"),
        "check_payload_27b": int(
            int(sim.get("data_header_bytes", 0)) + int(sim.get("payload_bytes", 0)) == 27
        ),
        "check_generated_total": int(int(pdr.get("total_data_generated", 0)) == expected_generated_total),
        "check_traffic_mode": int(str(sim.get("traffic_mode", "")) == "pueyo_all_to_all"),
    }
    checks["preflight_ok"] = int(all(v == 1 for v in checks.values()))
    checks["generated_count"] = int(pdr.get("total_data_generated", 0))
    return checks


def extract_row(
    summary: Dict,
    run_info: Dict,
    side: int,
    spacing: int,
    topology: str,
    load: str,
    seed: int,
    sf_min: int,
    sf_max: int,
    expected_generated_total: int,
    run_dir: Path,
    sigma: float,
) -> Dict:
    sim = summary.get("simulation", {})
    pdr = summary.get("pdr", {})
    delay = summary.get("delay", {})
    tput = summary.get("throughput", {})
    routes = summary.get("routes", {})
    cp = summary.get("control_plane", {})
    drops = summary.get("drops", {})
    meta = run_info.get("meta", {})
    checks = validate_run(summary, expected_generated_total, side, spacing, topology, sigma)

    n_nodes = int(sim.get("n_nodes", side * side))
    return {
        "run_id": f"toa_{topology}_s{side}_d{spacing}_{load}_sf{sf_min}_{sf_max}_seed{seed}",
        "topology": topology_label(topology),
        "topology_internal": topology,
        "grid_side": side,
        "spacing_m": spacing,
        "n_nodes": n_nodes,
        "load": load,
        "routing": "ToA",
        "sf_range": f"{sf_min}-{sf_max}",
        "sf_min": sf_min,
        "sf_max": sf_max,
        "seed": seed,
        "generated_count": int(pdr.get("total_data_generated", 0)),
        "delivered_count": int(pdr.get("delivered", 0)),
        "delivery_ratio": float(pdr.get("delivery_ratio", 0.0)),
        "goodput_bps": float(tput.get("goodput_bps", 0.0)),
        "delay_p95_s": float(delay.get("p95_s", 0.0)),
        "data_tx_sent": int(cp.get("data_tx_sent", 0)),
        "control_tx_sent": int(cp.get("control_tx_sent", cp.get("beacon_tx_sent", 0))),
        "routes_total": int(routes.get("routes_total", 0)),
        "rx_scan_miss_before_lock": int(cp.get("rx_scan_miss_before_lock", 0)),
        "rx_post_lock_interference_fail": int(cp.get("rx_post_lock_interference_fail", 0)),
        "shadowing_sigma_db": float(sim.get("shadowing_sigma_db", float("nan"))),
        "sf_scan_ed_threshold_dbm": float(sim.get("sf_scan_ed_threshold_dbm", float("nan"))),
        "expected_generated_total": expected_generated_total,
        "git_commit": str(meta.get("git_commit", "")),
        "sim_args": str(meta.get("sim_args", "")),
        "run_dir": str(run_dir),
        **checks,
    }


def aggregate(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[Tuple[str, str, int, int, str, int, int], List[Dict]] = {}
    for r in rows:
        key = (
            str(r["topology"]),
            str(r["topology_internal"]),
            int(r["spacing_m"]),
            int(r["n_nodes"]),
            str(r["load"]),
            int(r["sf_min"]),
            int(r["sf_max"]),
        )
        grouped.setdefault(key, []).append(r)

    metrics = [
        "delivery_ratio",
        "goodput_bps",
        "delay_p95_s",
        "generated_count",
        "data_tx_sent",
        "control_tx_sent",
        "routes_total",
        "rx_scan_miss_before_lock",
        "rx_post_lock_interference_fail",
    ]

    out = []
    for (topology, topology_internal, spacing, n_nodes, load, sf_min, sf_max), rr in sorted(grouped.items()):
        row = {
            "topology": topology,
            "topology_internal": topology_internal,
            "spacing_m": spacing,
            "n_nodes": n_nodes,
            "load": load,
            "routing": "ToA",
            "sf_range": f"{sf_min}-{sf_max}",
            "sf_min": sf_min,
            "sf_max": sf_max,
            "seed_set": ",".join(str(int(x["seed"])) for x in sorted(rr, key=lambda x: int(x["seed"]))),
            "n_seeds": len(rr),
            "shadowing_sigma_db": rr[0]["shadowing_sigma_db"],
            "sf_scan_ed_threshold_dbm": rr[0]["sf_scan_ed_threshold_dbm"],
            "all_preflight_ok": int(all(int(x["preflight_ok"]) == 1 for x in rr)),
        }
        for m in metrics:
            vals = [float(x[m]) for x in rr]
            mu, sd, (lo, hi) = mean_std_ci95(vals)
            row[f"{m}_mean"] = mu
            row[f"{m}_std"] = sd
            row[f"{m}_ci95_lo"] = lo
            row[f"{m}_ci95_hi"] = hi
            row[f"{m}_ci95"] = max(mu - lo, hi - mu)
        out.append(row)
    return out


def choose_best_points(agg_rows: List[Dict]) -> List[Dict]:
    grouped: Dict[Tuple[str, int, str], List[Dict]] = {}
    for r in agg_rows:
        key = (str(r["topology"]), int(r["spacing_m"]), int(r["n_nodes"]), str(r["load"]))
        grouped.setdefault(key, []).append(r)

    out = []
    for key, rr in sorted(grouped.items()):
        best = max(
            rr,
            key=lambda x: (
                float(x["delivery_ratio_mean"]),
                float(x["goodput_bps_mean"]),
                -int(x["sf_min"]),
                -int(x["sf_max"]),
            ),
        )
        out.append(
            {
                "topology": best["topology"],
                "spacing_m": best["spacing_m"],
                "n_nodes": best["n_nodes"],
                "load": best["load"],
                "routing": "ToA",
                "sf_range_best": best["sf_range"],
                "pdr_mean": best["delivery_ratio_mean"],
                "pdr_ci95": best["delivery_ratio_ci95"],
                "sf_scan_ed_threshold_dbm": best["sf_scan_ed_threshold_dbm"],
                "shadowing_sigma_db": best["shadowing_sigma_db"],
                "seed_set": best["seed_set"],
                "n_seeds": best["n_seeds"],
            }
        )
    return out


def write_csv(path: Path, rows: List[Dict]) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_point_logs(outdir: Path, runs: List[Dict], best_rows: List[Dict]) -> None:
    runs_by_point: Dict[Tuple[str, int, int, str, str], List[Dict]] = {}
    for r in runs:
        key = (str(r["topology"]), int(r["spacing_m"]), int(r["n_nodes"]), str(r["load"]), str(r["sf_range"]))
        runs_by_point.setdefault(key, []).append(r)

    point_dir = outdir / "point_logs"
    ensure_dir(point_dir)
    for row in best_rows:
        key = (
            str(row["topology"]),
            int(row["spacing_m"]),
            int(row["n_nodes"]),
            str(row["load"]),
            str(row["sf_range_best"]),
        )
        rr = sorted(runs_by_point.get(key, []), key=lambda x: int(x["seed"]))
        payload = {
            "topology": row["topology"],
            "spacing_m": row["spacing_m"],
            "n_nodes": row["n_nodes"],
            "load": row["load"],
            "routing": "ToA",
            "sf_range_best": row["sf_range_best"],
            "seed_set": row["seed_set"],
            "n_seeds": row["n_seeds"],
            "shadowing_sigma_db": row["shadowing_sigma_db"],
            "sf_scan_ed_threshold_dbm": row["sf_scan_ed_threshold_dbm"],
            "per_seed": [
                {
                    "seed": int(r["seed"]),
                    "generated_count": int(r["generated_count"]),
                    "data_tx_sent": int(r["data_tx_sent"]),
                    "control_tx_sent": int(r["control_tx_sent"]),
                    "routes_total": int(r["routes_total"]),
                    "rx_scan_miss_before_lock": int(r["rx_scan_miss_before_lock"]),
                    "rx_post_lock_interference_fail": int(r["rx_post_lock_interference_fail"]),
                }
                for r in rr
            ],
            "aggregate_mean": {
                "generated_count": statistics.mean([float(r["generated_count"]) for r in rr]) if rr else 0.0,
                "data_tx_sent": statistics.mean([float(r["data_tx_sent"]) for r in rr]) if rr else 0.0,
                "control_tx_sent": statistics.mean([float(r["control_tx_sent"]) for r in rr]) if rr else 0.0,
                "routes_total": statistics.mean([float(r["routes_total"]) for r in rr]) if rr else 0.0,
                "rx_scan_miss_before_lock": statistics.mean([float(r["rx_scan_miss_before_lock"]) for r in rr]) if rr else 0.0,
                "rx_post_lock_interference_fail": statistics.mean([float(r["rx_post_lock_interference_fail"]) for r in rr]) if rr else 0.0,
            },
        }
        fname = f"{row['topology']}_dx{row['spacing_m']}_n{row['n_nodes']}_{row['load']}.json"
        (point_dir / fname).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    sides = parse_int_list(args.sides)
    spacings = parse_int_list(args.spacings)
    topologies = parse_str_list(args.topologies)
    loads = parse_str_list(args.loads)
    seeds = parse_int_list(args.seeds)
    sf_ranges = parse_sf_ranges(args.sf_ranges)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else BASE_DIR / "validation_results" / f"fig11_fig12_toa_bestof_{ts}"
    ensure_dir(outdir)

    if args.build:
        proc = run_command([str(NS3_BIN), "build"], NS3_DIR)
        if proc.returncode != 0:
            print(proc.stdout)
            raise RuntimeError("build failed")
    git_commit = get_git_commit_hash(NS3_DIR)

    common = {
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
        "shadowingSigmaDb": args.shadowing_sigma_db,
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
        "sfScanEdThresholdDbm": args.sf_scan_ed_threshold_dbm,
        "dataPayloadSizeBytes": 20,
        "dataStartSec": args.data_start_sec,
        "pdrEndWindowSec": args.drain_sec,
    }

    jobs = []
    for side in sides:
        n_nodes = side * side
        packets_per_node = 100 * (n_nodes - 1)
        for spacing in spacings:
            area = (side - 1) * spacing
            for topology in topologies:
                for load in loads:
                    interval_s = traffic_interval_from_load(load)
                    data_duration = packets_per_node * interval_s
                    data_stop = args.data_start_sec + data_duration
                    stop_sec = data_stop + args.drain_sec
                    expected_generated_total = n_nodes * packets_per_node
                    for sf_min, sf_max in sf_ranges:
                        for seed in seeds:
                            cfg = dict(common)
                            cfg.update(
                                {
                                    "nodePlacementMode": topology,
                                    "pueyoGridSide": side,
                                    "pueyoGridSpacingM": spacing,
                                    "nEd": n_nodes,
                                    "areaWidth": area,
                                    "areaHeight": area,
                                    "trafficLoad": load,
                                    "dataStopSec": data_stop,
                                    "stopSec": stop_sec,
                                    "rngRun": seed,
                                    "sfMin": sf_min,
                                    "sfMax": sf_max,
                                }
                            )
                            jobs.append(
                                (
                                    side,
                                    spacing,
                                    topology,
                                    load,
                                    seed,
                                    sf_min,
                                    sf_max,
                                    expected_generated_total,
                                    cfg,
                                )
                            )

    if args.max_runs > 0:
        jobs = jobs[: args.max_runs]

    total = len(jobs)
    rows: List[Dict] = []
    preflight_failures: List[Dict] = []

    for idx, (side, spacing, topology, load, seed, sf_min, sf_max, expected_generated_total, cfg) in enumerate(jobs, start=1):
        run_dir = (
            outdir
            / topology
            / f"side_{side}"
            / f"spacing_{spacing}"
            / load
            / f"sf_{sf_min}_{sf_max}"
            / f"seed_{seed}"
        )
        sim_args = "mesh_dv_baseline " + dict_to_cli(cfg)
        print(
            f"[{idx:04d}/{total}] {topology} side={side} spacing={spacing} load={load} sf={sf_min}-{sf_max} seed={seed}"
        )
        res = run_one(run_dir, sim_args, resume=args.resume, git_commit=git_commit)
        row = extract_row(
            res["summary"],
            res,
            side,
            spacing,
            topology,
            load,
            seed,
            sf_min,
            sf_max,
            expected_generated_total,
            run_dir,
            args.shadowing_sigma_db,
        )
        rows.append(row)
        if int(row["preflight_ok"]) != 1:
            preflight_failures.append(
                {
                    "run_id": row["run_id"],
                    "generated_count": row["generated_count"],
                    "expected_generated_total": expected_generated_total,
                    "check_area_formula": row["check_area_formula"],
                    "check_n_nodes_square": row["check_n_nodes_square"],
                    "check_shadowing_sigma": row["check_shadowing_sigma"],
                    "check_wire_pueyo7b": row["check_wire_pueyo7b"],
                    "check_payload_27b": row["check_payload_27b"],
                    "check_generated_total": row["check_generated_total"],
                    "run_dir": row["run_dir"],
                }
            )
            write_csv(outdir / "results_runs.csv", rows)
            write_csv(outdir / "results_agg.csv", aggregate(rows))
            (outdir / "campaign_summary.json").write_text(
                json.dumps(
                    {
                        "outdir": str(outdir),
                        "git_commit": git_commit,
                        "total_runs": len(rows),
                        "preflight_failures": preflight_failures,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            raise RuntimeError(f"preflight failed for {row['run_id']}")

        if (idx % 10 == 0) or (idx == total):
            write_csv(outdir / "results_runs.csv", rows)
            agg_rows = aggregate(rows)
            write_csv(outdir / "results_agg.csv", agg_rows)
            write_csv(outdir / "fig11_fig12_paper_ready.csv", choose_best_points(agg_rows))

    agg_rows = aggregate(rows)
    best_rows = choose_best_points(agg_rows)
    write_csv(outdir / "results_runs.csv", rows)
    write_csv(outdir / "results_agg.csv", agg_rows)
    write_csv(outdir / "fig11_fig12_paper_ready.csv", best_rows)
    write_point_logs(outdir, rows, best_rows)

    summary = {
        "outdir": str(outdir),
        "git_commit": git_commit,
        "total_runs": len(rows),
        "passed_preflight_runs": sum(1 for r in rows if int(r["preflight_ok"]) == 1),
        "failed_preflight_runs": len(preflight_failures),
        "matrix": {
            "sides": sides,
            "spacings": spacings,
            "topologies": topologies,
            "loads": loads,
            "seeds": seeds,
            "sf_ranges": [f"{lo}-{hi}" for lo, hi in sf_ranges],
        },
        "sf_scan_ed_threshold_dbm": args.sf_scan_ed_threshold_dbm,
        "data_start_sec": args.data_start_sec,
        "drain_sec": args.drain_sec,
        "preflight_failures": preflight_failures,
    }
    (outdir / "campaign_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("[OK] Pueyo Fig11/Fig12 DV-ToA best-of campaign completed")
    print(f"OUTDIR {outdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
