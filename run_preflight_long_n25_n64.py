#!/usr/bin/env python3
"""
Long preflight before full matrix:
- N in {25, 64}
- load in {low, medium}
- duty in {off, on(1%)}
- seeds in {1,3,5}
- timing: dataStart=300, dataStop=3900, stop=4500, pdrEndWindow=600
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
    p = argparse.ArgumentParser(description="Run long preflight campaign (N=25/64, low/medium, duty off/on).")
    p.add_argument("--build", action="store_true", default=False)
    p.add_argument("--resume", action="store_true", default=True)
    p.add_argument("--no-resume", dest="resume", action="store_false")
    p.add_argument("--seeds", type=str, default="1 3 5")
    p.add_argument("--outdir", type=str, default=None)
    return p.parse_args()


def parse_seed_list(spec: str) -> List[int]:
    out = []
    for tok in spec.replace(",", " ").split():
        tok = tok.strip()
        if tok:
            out.append(int(tok))
    if not out:
        raise ValueError("seed list is empty")
    return out


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_command(cmd: List[str], cwd: Path, log_path: Path | None = None) -> subprocess.CompletedProcess:
    if log_path is not None:
        ensure_dir(log_path.parent)
        with log_path.open("w", encoding="utf-8") as f:
            return subprocess.run(
                cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False
            )
    return subprocess.run(
        cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False
    )


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
    return (run_dir / "mesh_dv_summary.json").exists() and (run_dir / "run.log").exists() and (
        run_dir / "meta.json"
    ).exists()


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
        commit_val = str(meta.get("git_commit", "")).strip().lower()
        if (not commit_val) or (commit_val == "unknown"):
            meta["git_commit"] = git_commit
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return {
            "summary": load_json(summary_dst),
            "summary_path": summary_dst,
            "meta": meta,
            "resumed": True,
        }

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


def extract_row(summary: Dict, run_info: Dict, n_nodes: int, load: str, duty_name: str, seed: int, run_dir: Path, resumed: bool) -> Dict:
    sim = summary.get("simulation", {})
    pdr = summary.get("pdr", {})
    cp = summary.get("control_plane", {})
    routes = summary.get("routes", {})
    q = summary.get("queue_backlog", {})
    drops = summary.get("drops", {})
    delay = summary.get("delay", {})
    meta = run_info.get("meta", {})

    delivery_ratio = float(pdr.get("delivery_ratio", float("nan")))
    source_first_tx_ratio = float(pdr.get("source_first_tx_ratio", float("nan")))
    delay_p95_s = float(delay.get("p95_s", float("nan")))
    beacon_tx_sent = float(cp.get("beacon_tx_sent", float("nan")))
    beacon_scheduled = float(cp.get("beacon_scheduled", float("nan")))
    tx_attempts_per_generated = float(
        pdr.get("tx_attempts_per_generated", pdr.get("admission_ratio", 0.0))
    )
    delivered_count = int(pdr.get("delivered", 0))
    generated_count = int(pdr.get("total_data_generated", 0))

    c_delivery = int(not math.isnan(delivery_ratio))
    c_beacon = int((not math.isnan(beacon_tx_sent)) and (not math.isnan(beacon_scheduled)) and (beacon_tx_sent <= beacon_scheduled))
    c_src_first = int((not math.isnan(source_first_tx_ratio)) and (source_first_tx_ratio <= 1.0))
    c_delay = int((not math.isnan(delay_p95_s)) and math.isfinite(delay_p95_s))

    return {
        "run_id": f"n{n_nodes}_{load}_{duty_name}_seed{seed}",
        "N": n_nodes,
        "n_nodes": n_nodes,
        "load": load,
        "duty": duty_name,
        "duty_profile": duty_name,
        "seed": seed,
        "resumed": int(bool(resumed)),
        "enable_duty": int(bool(sim.get("enable_duty", duty_name == "duty_on"))),
        "delivery_ratio": delivery_ratio,
        "tx_attempts_per_generated": tx_attempts_per_generated,
        "source_first_tx_ratio": source_first_tx_ratio,
        "delivered_count": delivered_count,
        "generated_count": generated_count,
        "delay_p95_s": delay_p95_s,
        "delay_avg_s": float(delay.get("avg_s", 0.0)),
        "beacon_tx_sent": beacon_tx_sent,
        "beacon_scheduled": beacon_scheduled,
        "beacon_tx_sent_over_scheduled": (beacon_tx_sent / beacon_scheduled) if beacon_scheduled > 0 else 0.0,
        "beacon_blocked_by_duty": int(cp.get("beacon_blocked_by_duty", 0)),
        "dv_route_expire_events": int(routes.get("dv_route_expire_events", 0)),
        "drop_no_route": int(drops.get("drop_no_route", 0)),
        "cad_busy_events": int(q.get("cad_busy_events", 0)),
        "duty_blocked_events": int(q.get("duty_blocked_events", 0)),
        "check_delivery_not_nan": c_delivery,
        "check_beacon_sent_le_sched": c_beacon,
        "check_source_first_tx_le_1": c_src_first,
        "check_delay_p95_finite": c_delay,
        "git_commit": str(meta.get("git_commit", "")),
        "cli_full": str(meta.get("cmd", "")),
        "sim_args": str(meta.get("sim_args", "")),
        "run_dir": str(run_dir),
    }


def aggregate(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[Tuple[int, str, str], List[Dict]] = {}
    for r in rows:
        grouped.setdefault((int(r["n_nodes"]), str(r["load"]), str(r["duty_profile"])), []).append(r)

    metrics = [
        "delivery_ratio",
        "delay_p95_s",
        "beacon_tx_sent",
        "beacon_scheduled",
        "beacon_tx_sent_over_scheduled",
        "dv_route_expire_events",
        "drop_no_route",
        "tx_attempts_per_generated",
        "source_first_tx_ratio",
        "delivered_count",
        "generated_count",
        "beacon_blocked_by_duty",
        "duty_blocked_events",
    ]

    out = []
    for (n_nodes, load, duty_profile), rr in sorted(grouped.items()):
        row = {"n_nodes": n_nodes, "load": load, "duty_profile": duty_profile, "n": len(rr)}
        for m in metrics:
            vals = [float(x[m]) for x in rr]
            mu, sd, (lo, hi) = mean_std_ci95(vals)
            row[f"{m}_mean"] = mu
            row[f"{m}_std"] = sd
            row[f"{m}_ci95_lo"] = lo
            row[f"{m}_ci95_hi"] = hi
        row["all_delivery_not_nan"] = int(all(int(x["check_delivery_not_nan"]) == 1 for x in rr))
        row["all_beacon_sent_le_sched"] = int(all(int(x["check_beacon_sent_le_sched"]) == 1 for x in rr))
        row["all_source_first_tx_le_1"] = int(all(int(x["check_source_first_tx_le_1"]) == 1 for x in rr))
        row["all_delay_p95_finite"] = int(all(int(x["check_delay_p95_finite"]) == 1 for x in rr))
        out.append(row)
    return out


def write_csv(path: Path, rows: List[Dict]) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    args = parse_args()
    seeds = parse_seed_list(args.seeds)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else BASE_DIR / "validation_results" / f"preflight_long_n25_n64_{ts}"
    ensure_dir(outdir)

    if args.build:
        proc = run_command([str(NS3_BIN), "build"], NS3_DIR)
        if proc.returncode != 0:
            print(proc.stdout)
            raise RuntimeError("build failed")
    git_commit = get_git_commit_hash(NS3_DIR)

    common = {
        "wireFormat": "v2",
        "nodePlacementMode": "random",
        "areaWidth": 1000,
        "areaHeight": 1000,
        "enableCsma": "true",
        "interferenceModel": "puello",
        "txPowerDbm": 20,
        "beaconIntervalWarmSec": 10,
        "beaconIntervalStableSec": 120,
        "routeTimeoutFactor": 7,
        "dutyWindowSec": 3600,
        "dataStartSec": 300,
        "dataStopSec": 3900,
        "stopSec": 4500,
        "pdrEndWindowSec": 600,
        "enablePcap": "false",
    }

    loads = ["low", "medium"]
    duty_cases = [
        ("duty_off", {"enableDuty": "false"}),
        ("duty_on", {"enableDuty": "true", "dutyLimit": 0.01}),
    ]
    n_values = [25, 64]

    jobs = []
    for n in n_values:
        for load in loads:
            for duty_name, duty_args in duty_cases:
                for seed in seeds:
                    cfg = dict(common)
                    cfg["nEd"] = n
                    cfg["trafficLoad"] = load
                    cfg["rngRun"] = seed
                    cfg.update(duty_args)
                    jobs.append((n, load, duty_name, seed, cfg))

    total = len(jobs)
    rows: List[Dict] = []
    for idx, (n_nodes, load, duty_name, seed, cfg) in enumerate(jobs, start=1):
        run_id = f"n{n_nodes}_{load}_{duty_name}_seed{seed}"
        run_dir = outdir / f"n{n_nodes}" / load / duty_name / f"seed_{seed}"
        sim_args = "mesh_dv_baseline " + dict_to_cli(cfg)
        print(f"[{idx:02d}/{total}] {run_id}")
        res = run_one(run_dir, sim_args, resume=args.resume, git_commit=git_commit)
        row = extract_row(res["summary"], res, n_nodes, load, duty_name, seed, run_dir, bool(res["resumed"]))
        rows.append(row)
        print(
            f"  ok={int(row['check_delivery_not_nan'] and row['check_beacon_sent_le_sched'] and row['check_source_first_tx_le_1'] and row['check_delay_p95_finite'])} "
            f"delivery={row['delivery_ratio']:.4f} bSent/bSched={int(row['beacon_tx_sent'])}/{int(row['beacon_scheduled'])} "
            f"srcFirst={row['source_first_tx_ratio']:.4f} delayP95={row['delay_p95_s']:.4f} "
            f"dutyBlk={row['duty_blocked_events']} beaconDutyBlk={row['beacon_blocked_by_duty']} "
            f"{'(resume)' if row['resumed'] else ''}"
        )

    agg_rows = aggregate(rows)
    runs_csv = outdir / "results_runs.csv"
    agg_csv = outdir / "results_agg.csv"
    write_csv(runs_csv, rows)
    write_csv(agg_csv, agg_rows)
    (outdir / "results.json").write_text(json.dumps({"runs": rows, "agg": agg_rows}, indent=2), encoding="utf-8")

    summary = {
        "outdir": str(outdir),
        "total_runs": len(rows),
        "git_commit": git_commit,
        "passed_runs": sum(
            1
            for r in rows
            if int(r["check_delivery_not_nan"]) == 1
            and int(r["check_beacon_sent_le_sched"]) == 1
            and int(r["check_source_first_tx_le_1"]) == 1
            and int(r["check_delay_p95_finite"]) == 1
        ),
    }
    summary["failed_runs"] = summary["total_runs"] - summary["passed_runs"]
    (outdir / "preflight_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("[OK] preflight completed")
    print(f"OUTDIR {outdir}")
    print(f"RUNS_CSV {runs_csv}")
    print(f"AGG_CSV {agg_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
