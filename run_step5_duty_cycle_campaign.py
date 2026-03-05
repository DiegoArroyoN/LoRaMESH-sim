#!/usr/bin/env python3
"""
Step 5 duty-cycle campaign (final frozen spec).

Fixed configuration:
- wireFormat=v2
- N=25, random 1 km^2
- CSMA ON
- interferenceModel=puello
- txPower=20 dBm
- dutyWindow=3600
- beaconIntervalStableSec=120
- routeTimeoutFactor=7

Timing:
- dataStartSec=300
- dataStopSec=3900
- stopSec=4500
- pdrEndWindowSec=600

Loads:
- low (100s), medium (10s)
Profiles:
- duty OFF, duty ON (1%)
Seeds:
- default 1..10
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
    p = argparse.ArgumentParser(description="Run Step 5 duty-cycle campaign")
    p.add_argument("--build", action="store_true", default=True)
    p.add_argument("--no-build", dest="build", action="store_false")
    p.add_argument("--resume", action="store_true", default=True)
    p.add_argument("--no-resume", dest="resume", action="store_false")
    p.add_argument("--seeds", type=str, default="1 2 3 4 5 6 7 8 9 10")
    p.add_argument("--outdir", type=str, default=None)
    return p.parse_args()


def parse_seed_list(spec: str) -> List[int]:
    vals = []
    for tok in spec.replace(",", " ").split():
        tok = tok.strip()
        if tok:
            vals.append(int(tok))
    if not vals:
        raise ValueError("seed list is empty")
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


def mean_std_ci95(values: List[float]) -> Tuple[float, float, Tuple[float, float]]:
    if not values:
        return 0.0, 0.0, (0.0, 0.0)
    if len(values) == 1:
        x = values[0]
        return x, 0.0, (x, x)
    m = statistics.mean(values)
    s = statistics.stdev(values)
    h = 1.96 * (s / math.sqrt(len(values)))
    return m, s, (m - h, m + h)


def required_run_artifacts(run_dir: Path) -> bool:
    return (run_dir / "mesh_dv_summary.json").exists() and (run_dir / "meta.json").exists() and (run_dir / "run.log").exists()


def validate_summary_schema(summary: Dict) -> None:
    sim = summary.get("simulation", {})
    pdr = summary.get("pdr", {})
    routes = summary.get("routes", {})
    cp = summary.get("control_plane", {})
    q = summary.get("queue_backlog", {})
    delay = summary.get("delay", {})

    required_sim = [
        "rng_run",
        "enable_csma",
        "enable_duty",
        "duty_limit",
        "duty_window_sec",
        "data_start_sec",
        "data_stop_sec",
        "stop_sec",
        "pdr_end_window_sec",
        "beacon_interval_stable_s",
        "route_timeout_factor",
        "route_timeout_sec",
        "interference_model",
        "tx_power_dbm",
        "wire_format",
    ]
    required_routes = ["dv_route_expire_events"]
    required_cp = [
        "beacon_scheduled",
        "beacon_tx_sent",
        "beacon_blocked_by_duty",
        "beacon_delay_s_mean",
        "beacon_delay_s_p95",
    ]
    required_q = ["txQueue_len_end_total"]
    required_pdr = ["delivery_ratio"]
    required_delay = ["p50_s", "p95_s"]

    for k in required_sim:
        if k not in sim:
            raise RuntimeError(f"summary missing simulation.{k}")
    for k in required_routes:
        if k not in routes:
            raise RuntimeError(f"summary missing routes.{k}")
    for k in required_cp:
        if k not in cp:
            raise RuntimeError(f"summary missing control_plane.{k}")
    for k in required_q:
        if k not in q:
            raise RuntimeError(f"summary missing queue_backlog.{k}")
    for k in required_pdr:
        if k not in pdr:
            raise RuntimeError(f"summary missing pdr.{k}")
    if "tx_attempts_per_generated" not in pdr and "admission_ratio" not in pdr:
        raise RuntimeError("summary missing pdr.tx_attempts_per_generated (or legacy pdr.admission_ratio)")
    for k in required_delay:
        if k not in delay:
            raise RuntimeError(f"summary missing delay.{k}")


def run_one(run_dir: Path, sim_args: str, resume: bool, git_commit: str) -> Dict:
    ensure_dir(run_dir)
    log_path = run_dir / "run.log"
    meta_path = run_dir / "meta.json"
    summary_dst = run_dir / "mesh_dv_summary.json"

    if resume and required_run_artifacts(run_dir):
        summary = load_json(summary_dst)
        validate_summary_schema(summary)
        meta = load_json(meta_path)
        commit_val = str(meta.get("git_commit", "")).strip().lower()
        if (not commit_val) or (commit_val == "unknown"):
            meta["git_commit"] = git_commit
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return {
            "rc": 0,
            "elapsed_s": float(meta.get("elapsed_s", 0.0)),
            "summary": summary,
            "summary_path": summary_dst,
            "log_path": log_path,
            "meta_path": meta_path,
            "meta": meta,
            "resumed": True,
        }

    cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
    t0 = time.time()
    proc = run_command(cmd, NS3_DIR, log_path=log_path)
    elapsed = time.time() - t0

    summary_src = NS3_DIR / "mesh_dv_summary.json"
    if summary_src.exists():
        shutil.copy2(summary_src, summary_dst)

    for name in METRIC_FILES:
        src = NS3_DIR / name
        if src.exists():
            shutil.copy2(src, run_dir / name)

    meta = {
        "cmd": " ".join(cmd),
        "sim_args": sim_args,
        "elapsed_s": elapsed,
        "return_code": proc.returncode,
        "timestamp": dt.datetime.now().isoformat(),
        "git_commit": git_commit,
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    if proc.returncode != 0:
        raise RuntimeError(f"simulation failed rc={proc.returncode} run_dir={run_dir}")
    if not summary_dst.exists():
        raise RuntimeError(f"summary missing in {run_dir}")

    summary = load_json(summary_dst)
    validate_summary_schema(summary)
    return {
        "rc": proc.returncode,
        "elapsed_s": elapsed,
        "summary": summary,
        "summary_path": summary_dst,
        "log_path": log_path,
        "meta_path": meta_path,
        "meta": meta,
        "resumed": False,
    }


def extract_row(summary: Dict, run_info: Dict, load: str, duty_profile: str, seed: int, run_dir: Path) -> Dict:
    sim = summary.get("simulation", {})
    pdr = summary.get("pdr", {})
    routes = summary.get("routes", {})
    cp = summary.get("control_plane", {})
    q = summary.get("queue_backlog", {})
    delay = summary.get("delay", {})
    drops = summary.get("drops", {})
    meta = run_info.get("meta", {})

    tx_attempts_per_generated = float(
        pdr.get("tx_attempts_per_generated", pdr.get("admission_ratio", 0.0))
    )
    source_first_tx_count = int(pdr.get("source_first_tx_count", 0))
    source_first_tx_ratio = float(pdr.get("source_first_tx_ratio", 0.0))

    return {
        "N": 25,
        "n_nodes": 25,
        "load": load,
        "duty": duty_profile,
        "duty_profile": duty_profile,
        "seed": seed,
        "return_code": run_info["rc"],
        "elapsed_s": round(float(run_info["elapsed_s"]), 4),
        "wire_format": sim.get("wire_format", ""),
        "beacon_interval_stable_s": float(sim.get("beacon_interval_stable_s", 0.0)),
        "route_timeout_factor": float(sim.get("route_timeout_factor", 0.0)),
        "route_timeout_sec": float(sim.get("route_timeout_sec", 0.0)),
        "duty_window_sec": float(sim.get("duty_window_sec", 0.0)),
        "traffic_interval_s": float(sim.get("traffic_interval_s", 0.0)),
        "total_data_generated": int(pdr.get("total_data_generated", 0)),
        "delivered": int(pdr.get("delivered", 0)),
        "delivered_count": int(pdr.get("delivered", 0)),
        "delivery_ratio": float(pdr.get("delivery_ratio", pdr.get("pdr", 0.0))),
        "tx_attempts_per_generated": tx_attempts_per_generated,
        "admission_ratio_legacy": float(pdr.get("admission_ratio", tx_attempts_per_generated)),
        "source_first_tx_count": source_first_tx_count,
        "source_first_tx_ratio": source_first_tx_ratio,
        "delivered_per_tx_attempt": float(pdr.get("delivered_per_tx_attempt", 0.0)),
        "end_window_generated": int(pdr.get("end_window_generated", 0)),
        "dv_route_expire_events": int(routes.get("dv_route_expire_events", 0)),
        "beacon_scheduled": int(cp.get("beacon_scheduled", 0)),
        "beacon_tx_sent": int(cp.get("beacon_tx_sent", 0)),
        "beacon_blocked_by_duty": int(cp.get("beacon_blocked_by_duty", 0)),
        "beacon_delay_s_mean": float(cp.get("beacon_delay_s_mean", 0.0)),
        "beacon_delay_s_p95": float(cp.get("beacon_delay_s_p95", 0.0)),
        "txQueue_len_end_total": int(q.get("txQueue_len_end_total", 0)),
        "cad_busy_events": int(q.get("cad_busy_events", 0)),
        "duty_blocked_events": int(q.get("duty_blocked_events", 0)),
        "drop_no_route": int(drops.get("drop_no_route", 0)),
        "delay_p50_s": float(delay.get("p50_s", 0.0)),
        "delay_p95_s": float(delay.get("p95_s", 0.0)),
        "git_commit": str(meta.get("git_commit", "")),
        "cli_full": str(meta.get("cmd", "")),
        "sim_args": str(meta.get("sim_args", "")),
        "run_dir": str(run_dir),
    }


def aggregate(rows: List[Dict]) -> Dict:
    grouped: Dict[Tuple[str, str], List[Dict]] = {}
    for r in rows:
        grouped.setdefault((r["load"], r["duty_profile"]), []).append(r)

    metrics = [
        "delivery_ratio",
        "delivered_count",
        "tx_attempts_per_generated",
        "source_first_tx_ratio",
        "delivered_per_tx_attempt",
        "end_window_generated",
        "dv_route_expire_events",
        "drop_no_route",
        "beacon_scheduled",
        "beacon_tx_sent",
        "beacon_blocked_by_duty",
        "beacon_delay_s_mean",
        "beacon_delay_s_p95",
        "txQueue_len_end_total",
        "cad_busy_events",
        "duty_blocked_events",
        "delay_p50_s",
        "delay_p95_s",
    ]

    out = {}
    for key, items in grouped.items():
        load, duty = key
        stats = {"n": len(items)}
        for m in metrics:
            vals = [float(x[m]) for x in items]
            mean, std, (lo, hi) = mean_std_ci95(vals)
            stats[m] = {"mean": mean, "std": std, "ci95_lo": lo, "ci95_hi": hi}
        out[f"{load}__{duty}"] = stats
    return out


def write_rows_csv(path: Path, rows: List[Dict]) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    cols = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def write_agg_csv(path: Path, grouped: Dict) -> None:
    ensure_dir(path.parent)
    rows = []
    for key in sorted(grouped.keys()):
        load, duty_profile = key.split("__")
        g = grouped[key]
        row = {
            "load": load,
            "duty_profile": duty_profile,
            "n": g["n"],
        }
        for metric in [
            "delivery_ratio",
            "delay_p95_s",
            "delivered_count",
            "beacon_tx_sent",
            "beacon_scheduled",
            "dv_route_expire_events",
            "drop_no_route",
            "tx_attempts_per_generated",
            "source_first_tx_ratio",
        ]:
            row[f"{metric}_mean"] = g[metric]["mean"]
            row[f"{metric}_std"] = g[metric]["std"]
            row[f"{metric}_ci95_lo"] = g[metric]["ci95_lo"]
            row[f"{metric}_ci95_hi"] = g[metric]["ci95_hi"]
        sched = g["beacon_scheduled"]["mean"]
        sent = g["beacon_tx_sent"]["mean"]
        row["beacon_tx_sent_over_scheduled_mean"] = (sent / sched) if sched > 0 else 0.0
        rows.append(row)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def write_report(path: Path, rows: List[Dict], grouped: Dict) -> None:
    lines = []
    lines.append("# REPORTE STEP5 DUTY-CYCLE (BLOQUE A OPERATIVO)")
    lines.append("")
    lines.append(f"- Fecha: {dt.datetime.now().isoformat()}")
    lines.append("- Config fijada: N=25, random 1km2, CSMA=on, wire=v2, stable=120s, rtf=7, txPower=20dBm, Puello")
    lines.append("- Ventanas: dataStart=300, dataStop=3900, stop=4500, pdrEndWindow=600")
    lines.append("- Semantica: `beacon_blocked_by_duty` cuenta eventos de bloqueo/deferral, no beacons unicos.")
    lines.append("- Reportar `delay_p95_s` junto con `delivered_count` para interpretar percentiles con muestra suficiente.")
    lines.append(f"- Corridas completadas: {len(rows)}")
    lines.append("")

    lines.append("## Resumen")
    lines.append("")
    lines.append("| load | duty | n | delivery_ratio mean+-std | tx_attempts_per_generated mean+-std | source_first_tx_ratio mean+-std | dv_route_expire_events mean | beacon_blocked_by_duty mean | delay p50 mean | delay p95 mean |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for key in sorted(grouped.keys()):
        load, duty = key.split("__")
        g = grouped[key]
        lines.append(
            f"| {load} | {duty} | {g['n']} | "
            f"{g['delivery_ratio']['mean']:.4f} +- {g['delivery_ratio']['std']:.4f} | "
            f"{g['tx_attempts_per_generated']['mean']:.4f} +- {g['tx_attempts_per_generated']['std']:.4f} | "
            f"{g['source_first_tx_ratio']['mean']:.4f} +- {g['source_first_tx_ratio']['std']:.4f} | "
            f"{g['dv_route_expire_events']['mean']:.2f} | "
            f"{g['beacon_blocked_by_duty']['mean']:.2f} | "
            f"{g['delay_p50_s']['mean']:.4f} | {g['delay_p95_s']['mean']:.4f} |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    seeds = parse_seed_list(args.seeds)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else BASE_DIR / "validation_results" / f"step5_duty_cycle_{ts}"
    ensure_dir(outdir)

    if args.build:
        proc = run_command([str(NS3_BIN), "build"], NS3_DIR)
        if proc.returncode != 0:
            print(proc.stdout)
            raise RuntimeError("build failed")
    git_commit = get_git_commit_hash(NS3_DIR)

    base_args: Dict[str, object] = {
        "wireFormat": "v2",
        "nEd": 25,
        "nodePlacementMode": "random",
        "areaWidth": 1000,
        "areaHeight": 1000,
        "enableCsma": "true",
        "interferenceModel": "puello",
        "txPowerDbm": 20,
        "dutyWindowSec": 3600,
        "dataStartSec": 300,
        "dataStopSec": 3900,
        "stopSec": 4500,
        "pdrEndWindowSec": 600,
        "beaconIntervalStableSec": 120,
        "routeTimeoutFactor": 7,
        "enablePcap": "false",
    }

    loads = ["low", "medium"]
    profiles = [
        ("duty_off", {"enableDuty": "false"}),
        ("duty_on", {"enableDuty": "true", "dutyLimit": 0.01}),
    ]

    run_defs = []
    for load in loads:
        for duty_name, duty_args in profiles:
            for seed in seeds:
                run_defs.append((load, duty_name, duty_args, seed))

    rows: List[Dict] = []
    total = len(run_defs)
    for i, (load, duty_name, duty_args, seed) in enumerate(run_defs, start=1):
        sim_args = dict(base_args)
        sim_args.update(duty_args)
        sim_args["trafficLoad"] = load
        sim_args["rngRun"] = seed

        arg_string = dict_to_cli(sim_args)
        cmd_string = f"mesh_dv_baseline {arg_string}"

        run_dir = outdir / "block_a_step5" / load / duty_name / f"seed_{seed}"
        t0 = time.time()
        result = run_one(run_dir, cmd_string, resume=args.resume, git_commit=git_commit)
        elapsed = time.time() - t0

        summary = result["summary"]
        row = extract_row(summary, result, load, duty_name, seed, run_dir)
        rows.append(row)

        canonical = BASE_DIR / (
            f"mesh_dv_summary_step5_n25_random_{load}_csma_on_"
            f"duty_{'on' if duty_name == 'duty_on' else 'off'}_stable120_rtf7_seed{seed}.json"
        )
        shutil.copy2(result["summary_path"], canonical)

        print(
            f"[{i:02d}/{total}] load={load} duty={duty_name} seed={seed} "
            f"pdr={row['delivery_ratio']:.4f} tx_attempts/gen={row['tx_attempts_per_generated']:.4f} "
            f"src_first_tx_ratio={row['source_first_tx_ratio']:.4f} "
            f"expire={row['dv_route_expire_events']} bSched={row['beacon_scheduled']} "
            f"elapsed={elapsed:.2f}s {'(resume)' if result.get('resumed') else ''}"
        )

    grouped = aggregate(rows)

    write_rows_csv(outdir / "results_step5_runs.csv", rows)
    write_rows_csv(outdir / "results_runs.csv", rows)
    (outdir / "results_step5.json").write_text(json.dumps({"runs": rows, "grouped": grouped}, indent=2), encoding="utf-8")
    write_agg_csv(outdir / "results_agg.csv", grouped)
    write_report(BASE_DIR / f"REPORTE_STEP5_DUTY_CYCLE_{dt.date.today().isoformat()}.md", rows, grouped)

    print(f"[OK] Step5 campaign completed. Output: {outdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
