#!/usr/bin/env python3
"""
Campaign runner for Pueyo-like Fig. 11-12 family:
- topology in {pueyo_grid, pueyo_random_equiv}
- side in {3,4,5,6,7,8} so N = side^2
- spacing in {177,178,246,247}
- profile in {pueyo2024, proposal_pueyo_like}
- load in {low,medium,high,saturation}
- seeds configurable (default 1..5)

The random topology uses the same equivalent area as the grid:
  area_w = area_h = (side - 1) * spacing
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"

METRIC_FILES = [
    "mesh_dv_metrics_tx.csv",
    "mesh_dv_metrics_rx.csv",
    "mesh_dv_metrics_routes.csv",
    "mesh_dv_metrics_routes_used.csv",
    "mesh_dv_metrics_delay.csv",
    "mesh_dv_metrics_overhead.csv",
    "mesh_dv_metrics_duty.csv",
    "mesh_dv_metrics_energy.csv",
    "mesh_dv_metrics_lifetime.csv",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Pueyo Fig.11/Fig.12 area-coverage campaign")
    p.add_argument("--build", action="store_true", default=False)
    p.add_argument("--resume", action="store_true", default=True)
    p.add_argument("--no-resume", dest="resume", action="store_false")
    p.add_argument("--sides", type=str, default="3 4 5 6 7 8")
    p.add_argument("--spacings", type=str, default="177 178 246 247")
    p.add_argument("--topologies", type=str, default="pueyo_grid pueyo_random_equiv")
    p.add_argument("--profiles", type=str, default="pueyo2024 proposal_pueyo_like")
    p.add_argument("--loads", type=str, default="low medium high saturation")
    p.add_argument("--seeds", type=str, default="1 2 3 4 5")
    p.add_argument("--outdir", type=str, default=None)
    p.add_argument("--max-runs", type=int, default=0, help="0 = run all")
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
    if proc.returncode != 0:
        return "unknown"
    return proc.stdout.strip() or "unknown"


def dict_to_cli(args: Dict[str, object]) -> str:
    return " ".join([f"--{k}={v}" for k, v in args.items()])


def load_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def required_run_artifacts(run_dir: Path) -> bool:
    return (
        (run_dir / "mesh_dv_summary.json").exists()
        and (run_dir / "run.log").exists()
        and (run_dir / "meta.json").exists()
    )


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
    shutil.copy2(summary_src, summary_dst)
    for name in METRIC_FILES:
        src = NS3_DIR / name
        if src.exists():
            shutil.copy2(src, run_dir / name)

    meta = {
        "cmd": " ".join(cmd),
        "sim_args": sim_args,
        "elapsed_s": elapsed,
        "timestamp": dt.datetime.now().isoformat(),
        "git_commit": git_commit,
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return {"summary": load_json(summary_dst), "summary_path": summary_dst, "meta": meta, "resumed": False}


def extract_row(
    summary: Dict,
    run_info: Dict,
    side: int,
    spacing: int,
    topology: str,
    profile: str,
    load: str,
    seed: int,
    run_dir: Path,
) -> Dict:
    sim = summary.get("simulation", {})
    pdr = summary.get("pdr", {})
    delay = summary.get("delay", {})
    tput = summary.get("throughput", {})
    cp = summary.get("control_plane", {})
    drops = summary.get("drops", {})
    meta = run_info.get("meta", {})

    n_nodes = int(sim.get("n_nodes", side * side))
    area_expected = float((side - 1) * spacing)
    area_w = float(sim.get("area_w_m", float("nan")))
    area_h = float(sim.get("area_h_m", float("nan")))
    shadowing_sigma = float(sim.get("shadowing_sigma_db", float("nan")))
    wire_format = str(sim.get("wire_format", ""))
    payload_bytes = int(sim.get("payload_bytes", 0))
    data_header_bytes = int(sim.get("data_header_bytes", 0))
    topology_runtime = str(sim.get("topology", ""))

    check_area = abs(area_w - area_expected) < 1e-9 and abs(area_h - area_expected) < 1e-9
    check_n_nodes = n_nodes == side * side
    check_topology = topology_runtime == topology
    check_shadowing = abs(shadowing_sigma - 3.57) < 1e-9
    check_wire = wire_format == "pueyo7b"
    check_payload_27b = (data_header_bytes + payload_bytes) == 27
    preflight_ok = all(
        [check_area, check_n_nodes, check_topology, check_shadowing, check_wire, check_payload_27b]
    )

    return {
        "run_id": f"{profile}_{topology}_s{side}_d{spacing}_{load}_seed{seed}",
        "profile": profile,
        "topology": topology_runtime,
        "grid_side": side,
        "grid_spacing_m": spacing,
        "N": side * side,
        "n_nodes": n_nodes,
        "area_w_m": area_w,
        "area_h_m": area_h,
        "load": load,
        "seed": seed,
        "wire_format": wire_format,
        "tx_power_dbm": float(sim.get("tx_power_dbm", float("nan"))),
        "preamble_symbols": int(sim.get("preamble_symbols", 0)),
        "coding_rate": str(sim.get("coding_rate", "")),
        "shadowing_sigma_db": shadowing_sigma,
        "delivery_ratio": float(pdr.get("delivery_ratio", 0.0)),
        "delay_p95_s": float(delay.get("p95_s", 0.0)),
        "delay_avg_s": float(delay.get("avg_s", 0.0)),
        "throughput_bps": float(tput.get("throughput_bps", 0.0)),
        "goodput_bps": float(tput.get("goodput_bps", 0.0)),
        "control_tx_sent": int(cp.get("control_tx_sent", cp.get("beacon_tx_sent", 0))),
        "data_tx_sent": int(cp.get("data_tx_sent", 0)),
        "control_to_data_tx_ratio": float(cp.get("control_to_data_tx_ratio", 0.0)),
        "drop_no_route": int(drops.get("drop_no_route", 0)),
        "drop_no_route_src": int(drops.get("drop_no_route_src", 0)),
        "drop_no_route_relay": int(drops.get("drop_no_route_relay", 0)),
        "check_area_formula": int(check_area),
        "check_n_nodes_square": int(check_n_nodes),
        "check_topology_mode": int(check_topology),
        "check_shadowing_sigma": int(check_shadowing),
        "check_wire_pueyo7b": int(check_wire),
        "check_payload_27b": int(check_payload_27b),
        "preflight_ok": int(preflight_ok),
        "git_commit": str(meta.get("git_commit", "")),
        "cli_full": str(meta.get("cmd", "")),
        "sim_args": str(meta.get("sim_args", "")),
        "run_dir": str(run_dir),
    }


def aggregate(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[Tuple[str, int, int, int, str, str], List[Dict]] = {}
    for r in rows:
        key = (
            str(r["topology"]),
            int(r["grid_side"]),
            int(r["grid_spacing_m"]),
            int(r["N"]),
            str(r["load"]),
            str(r["profile"]),
        )
        grouped.setdefault(key, []).append(r)

    metrics = [
        "delivery_ratio",
        "delay_p95_s",
        "delay_avg_s",
        "throughput_bps",
        "goodput_bps",
        "control_to_data_tx_ratio",
        "drop_no_route",
        "drop_no_route_src",
        "drop_no_route_relay",
    ]

    out = []
    for (topology, side, spacing, n_nodes, load, profile), rr in sorted(grouped.items()):
        row = {
            "topology": topology,
            "grid_side": side,
            "grid_spacing_m": spacing,
            "N": n_nodes,
            "n_nodes": n_nodes,
            "load": load,
            "profile": profile,
            "area_w_m": float((side - 1) * spacing),
            "area_h_m": float((side - 1) * spacing),
            "seed_set": ",".join(str(int(x["seed"])) for x in sorted(rr, key=lambda x: int(x["seed"]))),
            "n": len(rr),
        }
        for m in metrics:
            vals = [float(x[m]) for x in rr]
            mu, sd, (lo, hi) = mean_std_ci95(vals)
            row[f"{m}_mean"] = mu
            row[f"{m}_std"] = sd
            row[f"{m}_ci95_lo"] = lo
            row[f"{m}_ci95_hi"] = hi
        row["all_preflight_ok"] = int(all(int(x["preflight_ok"]) == 1 for x in rr))
        out.append(row)
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


def main() -> int:
    args = parse_args()
    sides = parse_int_list(args.sides)
    spacings = parse_int_list(args.spacings)
    topologies = parse_str_list(args.topologies)
    profiles = parse_str_list(args.profiles)
    loads = parse_str_list(args.loads)
    seeds = parse_int_list(args.seeds)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else BASE_DIR / "validation_results" / f"pueyo_fig11_fig12_{ts}"
    ensure_dir(outdir)

    if args.build:
        proc = run_command([str(NS3_BIN), "build"], NS3_DIR)
        if proc.returncode != 0:
            print(proc.stdout)
            raise RuntimeError("build failed")
    git_commit = get_git_commit_hash(NS3_DIR)

    common = {
        "wireFormat": "pueyo7b",
        "txPowerDbm": 20,
        "preambleSymbols": 16,
        "sfMin": 7,
        "sfMax": 12,
        "interferenceModel": "pueyo_fixed_capture",
        "shadowingSigmaDb": 3.57,
        "trafficMode": "pueyo_all_to_all",
        "pueyoPacketsPerPair": 100,
        "beaconIntervalWarmSec": 60,
        "beaconIntervalStableSec": 60,
        "routeTimeoutFactor": 5,
        "dvPayloadMaxBytes": 251,
        "maxRoutesPerDestination": 2,
        "maxTotalRoutes": 1024,
        "dataStartSec": 300,
        "dataStopSec": 3900,
        "stopSec": 4500,
        "pdrEndWindowSec": 600,
        "enablePcap": "false",
        "verboseLogs": "false",
    }

    jobs = []
    for side in sides:
        n_nodes = side * side
        for spacing in spacings:
            area = (side - 1) * spacing
            for topology in topologies:
                for profile in profiles:
                    for load in loads:
                        for seed in seeds:
                            cfg = dict(common)
                            cfg.update(
                                {
                                    "profile": profile,
                                    "nodePlacementMode": topology,
                                    "pueyoGridSide": side,
                                    "pueyoGridSpacingM": spacing,
                                    "nEd": n_nodes,
                                    "areaWidth": area,
                                    "areaHeight": area,
                                    "trafficLoad": load,
                                    "rngRun": seed,
                                }
                            )
                            jobs.append((side, spacing, topology, profile, load, seed, cfg))

    if args.max_runs > 0:
        jobs = jobs[: args.max_runs]

    total = len(jobs)
    rows: List[Dict] = []
    preflight_failures: List[Dict] = []

    for idx, (side, spacing, topology, profile, load, seed, cfg) in enumerate(jobs, start=1):
        run_dir = outdir / profile / topology / f"side_{side}" / f"spacing_{spacing}" / load / f"seed_{seed}"
        sim_args = "mesh_dv_baseline " + dict_to_cli(cfg)
        print(f"[{idx:04d}/{total}] {profile} {topology} side={side} spacing={spacing} load={load} seed={seed}")
        res = run_one(run_dir, sim_args, resume=args.resume, git_commit=git_commit)
        row = extract_row(res["summary"], res, side, spacing, topology, profile, load, seed, run_dir)
        rows.append(row)
        if int(row["preflight_ok"]) != 1:
            preflight_failures.append(
                {
                    "run_id": row["run_id"],
                    "check_area_formula": row["check_area_formula"],
                    "check_n_nodes_square": row["check_n_nodes_square"],
                    "check_topology_mode": row["check_topology_mode"],
                    "check_shadowing_sigma": row["check_shadowing_sigma"],
                    "check_wire_pueyo7b": row["check_wire_pueyo7b"],
                    "check_payload_27b": row["check_payload_27b"],
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
            write_csv(outdir / "results_agg.csv", aggregate(rows))

    agg_rows = aggregate(rows)
    write_csv(outdir / "results_runs.csv", rows)
    write_csv(outdir / "results_agg.csv", agg_rows)
    (outdir / "results.json").write_text(json.dumps({"runs": rows, "agg": agg_rows}, indent=2), encoding="utf-8")

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
            "profiles": profiles,
            "loads": loads,
            "seeds": seeds,
        },
        "preflight_failures": preflight_failures,
    }
    (outdir / "campaign_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("[OK] pueyo fig11/fig12 campaign completed")
    print(f"OUTDIR {outdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
