#!/usr/bin/env python3
"""
Duty-cycle campaign runner.

Implements Block A of the agreed plan:
- N=25, random 1km^2, wire v2, CSMA on, Puello, txPower=20 dBm
- dataStart=300, dataStop=3900, stop=4500, pdrEndWindow=600
- loads: low/medium/high
- duty profiles: off / on(1%)
- seeds: default 1..10
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
from typing import Dict, Iterable, List, Tuple

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
    p = argparse.ArgumentParser(description="Run LoRaMESH duty-cycle campaign.")
    p.add_argument("--block", choices=["a", "b", "c"], default="a")
    p.add_argument("--build", action="store_true", default=True)
    p.add_argument("--no-build", dest="build", action="store_false")
    p.add_argument("--resume", action="store_true", default=True)
    p.add_argument("--no-resume", dest="resume", action="store_false")
    p.add_argument("--seeds", type=str, default="1 2 3 4 5 6 7 8 9 10")
    p.add_argument(
        "--outdir",
        type=str,
        default=None,
        help="Output root. Default: validation_results/duty_cycle_campaign_<timestamp>",
    )
    return p.parse_args()


def parse_seed_list(seed_spec: str) -> List[int]:
    seeds = []
    for tok in seed_spec.replace(",", " ").split():
        tok = tok.strip()
        if not tok:
            continue
        seeds.append(int(tok))
    if not seeds:
        raise ValueError("seed list is empty")
    return seeds


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_command(cmd: List[str], cwd: Path, log_path: Path | None = None) -> subprocess.CompletedProcess:
    if log_path is not None:
        ensure_dir(log_path.parent)
        with log_path.open("w", encoding="utf-8") as f:
            return subprocess.run(
                cmd,
                cwd=str(cwd),
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
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


def required_run_artifacts(run_dir: Path) -> bool:
    if not (run_dir / "mesh_dv_summary.json").exists():
        return False
    if not (run_dir / "meta.json").exists():
        return False
    if not (run_dir / "run.log").exists():
        return False
    return True


def dict_to_cli(args: Dict[str, object]) -> str:
    parts = []
    for k, v in args.items():
        parts.append(f"--{k}={v}")
    return " ".join(parts)


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


def build_if_requested(do_build: bool) -> None:
    if not do_build:
        return
    proc = run_command([str(NS3_BIN), "build"], NS3_DIR)
    if proc.returncode != 0:
        print(proc.stdout)
        raise RuntimeError("build failed")


def run_smoke() -> None:
    smoke_args = (
        "mesh_dv_baseline "
        "--wireFormat=v2 --nEd=5 --nodePlacementMode=random --areaWidth=300 --areaHeight=300 "
        "--enableCsma=true --interferenceModel=puello --txPowerDbm=20 "
        "--enableDuty=true --dutyLimit=0.01 --dutyWindowSec=3600 "
        "--dataStartSec=30 --dataStopSec=80 --stopSec=120 --pdrEndWindowSec=40 "
        "--trafficLoad=medium --enablePcap=false --rngRun=1"
    )
    proc = run_command([str(NS3_BIN), "run", "--no-build", smoke_args], NS3_DIR)
    if proc.returncode != 0:
        print(proc.stdout)
        raise RuntimeError("smoke run failed")
    summary = NS3_DIR / "mesh_dv_summary.json"
    if not summary.exists():
        raise RuntimeError("smoke did not produce mesh_dv_summary.json")


def run_one(run_dir: Path, sim_args: str, resume: bool, git_commit: str) -> Dict:
    ensure_dir(run_dir)
    log_path = run_dir / "run.log"
    meta_path = run_dir / "meta.json"
    summary_dst = run_dir / "mesh_dv_summary.json"

    if resume and required_run_artifacts(run_dir):
        summary = load_json(summary_dst)
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

    return {
        "rc": proc.returncode,
        "elapsed_s": elapsed,
        "summary": load_json(summary_dst),
        "summary_path": summary_dst,
        "log_path": log_path,
        "meta_path": meta_path,
        "meta": meta,
        "resumed": False,
    }


def extract_row(summary: Dict, run_info: Dict, load: str, duty_profile: str, seed: int, run_dir: Path) -> Dict:
    sim = summary.get("simulation", {})
    pdr = summary.get("pdr", {})
    q = summary.get("queue_backlog", {})
    drops = summary.get("drops", {})
    routes = summary.get("routes", {})
    cp = summary.get("control_plane", {})
    delay = summary.get("delay", {})
    meta = run_info.get("meta", {})
    tx_attempts_per_generated = float(
        pdr.get("tx_attempts_per_generated", pdr.get("admission_ratio", 0.0))
    )
    source_first_tx_ratio = float(pdr.get("source_first_tx_ratio", 0.0))

    row = {
        "N": 25,
        "n_nodes": 25,
        "load": load,
        "duty": duty_profile,
        "duty_profile": duty_profile,
        "seed": seed,
        "return_code": run_info["rc"],
        "elapsed_s": round(float(run_info["elapsed_s"]), 4),
        "wire_format": sim.get("wire_format", ""),
        "total_data_generated": int(pdr.get("total_data_generated", 0)),
        "delivered": int(pdr.get("delivered", 0)),
        "delivered_count": int(pdr.get("delivered", 0)),
        "delivery_ratio": float(pdr.get("delivery_ratio", pdr.get("pdr", 0.0))),
        "admission_ratio": tx_attempts_per_generated,
        "tx_attempts_per_generated": tx_attempts_per_generated,
        "source_first_tx_ratio": source_first_tx_ratio,
        "delivered_per_tx_attempt": float(pdr.get("delivered_per_tx_attempt", 0.0)),
        "pdr_e2e_generated_eligible": float(pdr.get("pdr_e2e_generated_eligible", 0.0)),
        "end_window_generated": int(pdr.get("end_window_generated", 0)),
        "delay_p95_s": float(delay.get("p95_s", 0.0)),
        "beacon_scheduled": int(cp.get("beacon_scheduled", 0)),
        "beacon_tx_sent": int(cp.get("beacon_tx_sent", 0)),
        "dv_route_expire_events": int(routes.get("dv_route_expire_events", 0)),
        "cad_busy_events": int(q.get("cad_busy_events", 0)),
        "duty_blocked_events": int(q.get("duty_blocked_events", 0)),
        "queued_packets_end": int(q.get("queued_packets_end", 0)),
        "txQueue_len_end_total": int(q.get("txQueue_len_end_total", 0)),
        "drop_no_route": int(drops.get("drop_no_route", 0)),
        "drop_ttl_expired": int(drops.get("drop_ttl_expired", 0)),
        "drop_queue_overflow": int(drops.get("drop_queue_overflow", 0)),
        "drop_backtrack": int(drops.get("drop_backtrack", 0)),
        "drop_other": int(drops.get("drop_other", 0)),
        "git_commit": str(meta.get("git_commit", "")),
        "cli_full": str(meta.get("cmd", "")),
        "sim_args": str(meta.get("sim_args", "")),
        "run_dir": str(run_dir),
    }
    return row


def aggregate(rows: List[Dict]) -> Dict:
    grouped: Dict[Tuple[str, str], List[Dict]] = {}
    for r in rows:
        grouped.setdefault((r["load"], r["duty_profile"]), []).append(r)

    metrics = [
        "delivery_ratio",
        "delivered_count",
        "admission_ratio",
        "tx_attempts_per_generated",
        "source_first_tx_ratio",
        "delivered_per_tx_attempt",
        "pdr_e2e_generated_eligible",
        "end_window_generated",
        "delay_p95_s",
        "beacon_scheduled",
        "beacon_tx_sent",
        "dv_route_expire_events",
        "cad_busy_events",
        "duty_blocked_events",
        "queued_packets_end",
        "txQueue_len_end_total",
        "drop_no_route",
        "drop_ttl_expired",
        "drop_queue_overflow",
        "drop_backtrack",
        "drop_other",
    ]

    out = {}
    for key, items in grouped.items():
        load, duty = key
        stats = {"n": len(items)}
        for m in metrics:
            vals = [float(x[m]) for x in items]
            mean, std, (lo, hi) = mean_std_ci95(vals)
            stats[m] = {
                "mean": mean,
                "std": std,
                "ci95_lo": lo,
                "ci95_hi": hi,
            }
        out[f"{load}__{duty}"] = stats
    return out


def write_agg_csv(path: Path, grouped: Dict) -> None:
    if not grouped:
        path.write_text("", encoding="utf-8")
        return
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
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def write_rows_csv(path: Path, rows: List[Dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    ensure_dir(path.parent)
    cols = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def write_report(path: Path, rows: List[Dict], grouped: Dict) -> None:
    lines = []
    lines.append("# REPORTE BLOQUE A DUTY-CYCLE")
    lines.append("")
    lines.append(f"- Fecha: {dt.datetime.now().isoformat()}")
    lines.append("- Escenario base: N=25, random 1km2, CSMA=on, interferencia=puello, txPower=20 dBm, wire=v2")
    lines.append("- Ventanas: dataStart=300, dataStop=3900, stop=4500, pdrEndWindow=600")
    lines.append("- Semantica: `beacon_blocked_by_duty` cuenta eventos de bloqueo/deferral, no beacons unicos.")
    lines.append("- Reportar `delay_p95_s` junto con `delivered_count` para interpretar percentiles con muestra suficiente.")
    lines.append(f"- Corridas completadas: {len(rows)}")
    lines.append("")

    lines.append("## Resumen por carga y duty")
    lines.append("")
    lines.append("| load | duty | n | delivery_ratio mean±std | admission_ratio mean±std | delivered_per_tx_attempt mean±std |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for key in sorted(grouped.keys()):
        load, duty = key.split("__")
        g = grouped[key]
        dr = g["delivery_ratio"]
        ar = g["admission_ratio"]
        da = g["delivered_per_tx_attempt"]
        lines.append(
            f"| {load} | {duty} | {g['n']} | {dr['mean']:.4f} +- {dr['std']:.4f} | "
            f"{ar['mean']:.4f} +- {ar['std']:.4f} | {da['mean']:.4f} +- {da['std']:.4f} |"
        )

    lines.append("")
    lines.append("## Causalidad backlog/drops")
    lines.append("")
    lines.append("| load | duty | cad_busy mean | duty_blocked mean | queued_packets_end mean | drop_no_route mean | drop_other mean |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for key in sorted(grouped.keys()):
        load, duty = key.split("__")
        g = grouped[key]
        lines.append(
            f"| {load} | {duty} | {g['cad_busy_events']['mean']:.2f} | {g['duty_blocked_events']['mean']:.2f} | "
            f"{g['queued_packets_end']['mean']:.2f} | {g['drop_no_route']['mean']:.2f} | {g['drop_other']['mean']:.2f} |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_block_a(args: argparse.Namespace, outdir: Path, seeds: List[int], git_commit: str) -> None:
    block_dir = outdir / "block_a"
    ensure_dir(block_dir)

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
        "enablePcap": "false",
    }

    loads: List[Tuple[str, float]] = [("low", 100.0), ("medium", 10.0), ("high", 1.0)]
    profiles = [
        ("duty_off", {"enableDuty": "false"}),
        ("duty_on", {"enableDuty": "true", "dutyLimit": 0.01}),
    ]

    run_defs = []
    for load_name, _ in loads:
        for duty_name, duty_args in profiles:
            for seed in seeds:
                run_defs.append((load_name, duty_name, duty_args, seed))

    rows: List[Dict] = []
    total = len(run_defs)
    for i, (load_name, duty_name, duty_args, seed) in enumerate(run_defs, start=1):
        sim_args = dict(base_args)
        sim_args.update(duty_args)
        sim_args["trafficLoad"] = load_name
        sim_args["rngRun"] = seed

        arg_string = dict_to_cli(sim_args)
        cmd_string = f"mesh_dv_baseline {arg_string}"

        run_dir = block_dir / load_name / duty_name / f"seed_{seed}"
        t0 = time.time()
        result = run_one(run_dir, cmd_string, resume=args.resume, git_commit=git_commit)
        elapsed = time.time() - t0

        summary = result["summary"]
        row = extract_row(summary, result, load_name, duty_name, seed, run_dir)
        rows.append(row)

        canonical = BASE_DIR / (
            f"mesh_dv_summary_n25_random_{load_name}_csma_on_"
            f"duty_{'on' if duty_name == 'duty_on' else 'off'}_seed{seed}.json"
        )
        shutil.copy2(result["summary_path"], canonical)

        print(
            f"[{i:02d}/{total}] load={load_name} duty={duty_name} seed={seed} "
            f"pdr={row['delivery_ratio']:.4f} admission={row['admission_ratio']:.4f} "
            f"elapsed={elapsed:.2f}s {'(resume)' if result.get('resumed') else ''}"
        )

    grouped = aggregate(rows)

    write_rows_csv(outdir / "results_block_a.csv", rows)
    write_rows_csv(outdir / "results_runs.csv", rows)
    write_agg_csv(outdir / "results_agg.csv", grouped)
    (outdir / "results_block_a.json").write_text(
        json.dumps({"runs": rows, "grouped": grouped}, indent=2), encoding="utf-8"
    )
    write_report(BASE_DIR / f"REPORTE_BLOQUE_A_DUTY_CYCLE_{dt.date.today().isoformat()}.md", rows, grouped)


def main() -> int:
    args = parse_args()
    seeds = parse_seed_list(args.seeds)

    if args.block != "a":
        raise RuntimeError("Only block 'a' is implemented in this stage.")

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else BASE_DIR / "validation_results" / f"duty_cycle_campaign_{ts}"
    ensure_dir(outdir)

    build_if_requested(args.build)
    git_commit = get_git_commit_hash(NS3_DIR)
    run_smoke()
    run_block_a(args, outdir, seeds, git_commit)

    print(f"[OK] Block A completed. Output: {outdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
