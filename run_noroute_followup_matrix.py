#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import statistics
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Follow-up no-route sensitivity runs")
    p.add_argument("--build", action="store_true", default=False)
    p.add_argument("--resume", action="store_true", default=True)
    p.add_argument("--no-resume", dest="resume", action="store_false")
    p.add_argument("--seeds", type=str, default="1 3 5")
    p.add_argument("--outdir", type=str, default=None)
    return p.parse_args()


def parse_seeds(spec: str) -> List[int]:
    toks = spec.replace(",", " ").split()
    out = [int(t) for t in toks if t.strip()]
    if not out:
        raise ValueError("empty seeds")
    return out


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def run_command(cmd: List[str], cwd: Path, log_path: Path | None = None) -> int:
    if log_path is not None:
        ensure_dir(log_path.parent)
        with log_path.open("w", encoding="utf-8") as f:
            proc = subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True)
            return proc.returncode
    proc = subprocess.run(cmd, cwd=str(cwd))
    return proc.returncode


def dict_to_cli(d: Dict[str, object]) -> str:
    return " ".join([f"--{k}={v}" for k, v in d.items()])


def load_json(p: Path) -> Dict:
    return json.loads(p.read_text(encoding="utf-8"))


def mean_std_ci95(values: List[float]) -> Tuple[float, float, Tuple[float, float]]:
    if not values:
        return 0.0, 0.0, (0.0, 0.0)
    if len(values) == 1:
        v = float(values[0])
        return v, 0.0, (v, v)
    m = float(statistics.mean(values))
    s = float(statistics.stdev(values))
    h = 1.96 * (s / math.sqrt(len(values)))
    return m, s, (m - h, m + h)


def copy_metrics(run_dir: Path) -> None:
    for name in [
        "mesh_dv_metrics_tx.csv",
        "mesh_dv_metrics_rx.csv",
        "mesh_dv_metrics_routes.csv",
        "mesh_dv_metrics_routes_used.csv",
        "mesh_dv_metrics_delay.csv",
        "mesh_dv_metrics_overhead.csv",
        "mesh_dv_metrics_duty.csv",
        "mesh_dv_metrics_energy.csv",
        "mesh_dv_metrics_lifetime.csv",
    ]:
        src = NS3_DIR / name
        if src.exists():
            shutil.copy2(src, run_dir / name)


def parse_route_actions(routes_csv: Path) -> Dict[str, int]:
    out = {"NEW": 0, "UPDATE": 0, "POISON": 0, "EXPIRE": 0, "PURGE": 0}
    if not routes_csv.exists():
        return out
    with routes_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            act = row.get("action", "")
            if act in out:
                out[act] += 1
    return out


def run_one(run_dir: Path, sim_args: str, resume: bool) -> Dict:
    ensure_dir(run_dir)
    summary_p = run_dir / "mesh_dv_summary.json"
    log_p = run_dir / "run.log"
    meta_p = run_dir / "meta.json"
    if resume and summary_p.exists() and log_p.exists() and meta_p.exists():
        return {"summary": load_json(summary_p), "resumed": True}

    cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
    t0 = time.time()
    rc = run_command(cmd, NS3_DIR, log_p)
    dt_s = time.time() - t0
    if rc != 0:
        raise RuntimeError(f"run failed rc={rc}: {sim_args}")

    src_summary = NS3_DIR / "mesh_dv_summary.json"
    if not src_summary.exists():
        raise RuntimeError("missing mesh_dv_summary.json")

    shutil.copy2(src_summary, summary_p)
    copy_metrics(run_dir)
    meta = {
        "cmd": " ".join(cmd),
        "sim_args": sim_args,
        "elapsed_s": dt_s,
        "timestamp": dt.datetime.now().isoformat(),
    }
    meta_p.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return {"summary": load_json(summary_p), "resumed": False}


def main() -> int:
    args = parse_args()
    seeds = parse_seeds(args.seeds)

    if args.build:
        rc = run_command([str(NS3_BIN), "build"], NS3_DIR)
        if rc != 0:
            raise RuntimeError("build failed")

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else BASE_DIR / "validation_results" / f"duty_noroute_followup_{ts}"
    ensure_dir(outdir)

    base = {
        "wireFormat": "v2",
        "nEd": 25,
        "nodePlacementMode": "random",
        "areaWidth": 1000,
        "areaHeight": 1000,
        "enableCsma": "true",
        "interferenceModel": "puello",
        "txPowerDbm": 20,
        "dutyLimit": 0.01,
        "dutyWindowSec": 3600,
        "dataStartSec": 300,
        "dataStopSec": 3900,
        "stopSec": 4500,
        "pdrEndWindowSec": 600,
        "trafficLoad": "medium",
        "enablePcap": "false",
    }

    jobs: List[Tuple[str, Dict[str, object]]] = []

    # Block 1: DC-OFF full matrix
    for stable in [60, 90, 120]:
        for rtf in [6, 10, 14]:
            for seed in seeds:
                j = dict(base)
                j.update({
                    "enableDuty": "false",
                    "beaconIntervalStableSec": stable,
                    "routeTimeoutFactor": rtf,
                    "rngRun": seed,
                })
                jobs.append(("dc_off_matrix", j))

    # Block 2: DC-ON constant-timeout comparison points
    const_points = [(60, 10), (120, 5), (60, 14), (120, 7)]
    for stable, rtf in const_points:
        for seed in seeds:
            j = dict(base)
            j.update({
                "enableDuty": "true",
                "beaconIntervalStableSec": stable,
                "routeTimeoutFactor": rtf,
                "rngRun": seed,
            })
            jobs.append(("dc_on_const_timeout", j))

    rows = []
    total = len(jobs)
    for idx, (group, cfg) in enumerate(jobs, start=1):
        stable = int(cfg["beaconIntervalStableSec"])
        rtf = int(cfg["routeTimeoutFactor"])
        seed = int(cfg["rngRun"])
        duty_on = str(cfg["enableDuty"]).lower() == "true"
        run_dir = outdir / group / f"stable_{stable}" / f"rtf_{rtf}" / f"seed_{seed}"
        sim_args = "mesh_dv_baseline " + dict_to_cli(cfg)

        res = run_one(run_dir, sim_args, resume=args.resume)
        s = res["summary"]
        pdr = s.get("pdr", {})
        drops = s.get("drops", {})
        routes = s.get("routes", {})
        delay = s.get("delay", {})
        cp = s.get("control_plane", {})
        route_actions = parse_route_actions(run_dir / "mesh_dv_metrics_routes.csv")
        dv_route_expire_events_summary = routes.get("dv_route_expire_events", None)
        if isinstance(dv_route_expire_events_summary, (int, float)):
            dv_route_expire_events = int(dv_route_expire_events_summary)
        else:
            dv_route_expire_events = int(route_actions.get("EXPIRE", 0))
        tx_attempts_per_generated = float(
            pdr.get("tx_attempts_per_generated", pdr.get("admission_ratio", 0.0))
        )

        row = {
            "group": group,
            "duty_on": int(duty_on),
            "stable": stable,
            "rtf": rtf,
            "route_timeout_sec": stable * rtf,
            "seed": seed,
            "delivery_ratio": float(pdr.get("delivery_ratio", pdr.get("pdr", 0.0))),
            "admission_ratio": float(pdr.get("admission_ratio", 0.0)),
            "tx_attempts_per_generated": tx_attempts_per_generated,
            "source_first_tx_ratio": float(pdr.get("source_first_tx_ratio", 0.0)),
            "delay_p95_s": float(delay.get("p95_s", 0.0)),
            "drop_no_route": int(drops.get("drop_no_route", 0)),
            "drop_ttl_expired": int(drops.get("drop_ttl_expired", 0)),
            "drop_backtrack": int(drops.get("drop_backtrack", 0)),
            "drop_other": int(drops.get("drop_other", 0)),
            "beacon_scheduled": int(cp.get("beacon_scheduled", 0)),
            "beacon_tx_sent": int(cp.get("beacon_tx_sent", 0)),
            # Keep summary counters for traceability, but do not use them as
            # primary expire metric in this study.
            "route_expired_counter_summary": int(routes.get("expired", 0)),
            "route_poisoned_counter_summary": int(routes.get("poisoned", 0)),
            # Primary, unambiguous metric for this study (from routes CSV).
            "dv_route_expire_events": dv_route_expire_events,
            "dv_route_purge_events": int(route_actions.get("PURGE", 0)),
            "dv_route_poison_events": int(route_actions.get("POISON", 0)),
            "run_dir": str(run_dir),
        }
        rows.append(row)

        print(
            f"[{idx:02d}/{total}] {group} duty={'ON' if duty_on else 'OFF'} "
            f"stable={stable} rtf={rtf} seed={seed} "
            f"drop_no_route={row['drop_no_route']} pdr={row['delivery_ratio']:.4f} "
            f"{'(resume)' if res['resumed'] else ''}"
        )

        # canonical copy for quick access
        canonical = BASE_DIR / (
            f"mesh_dv_summary_n25_random_medium_csma_on_"
            f"duty_{'on' if duty_on else 'off'}_stable{stable}_rtf{rtf}_seed{seed}.json"
        )
        shutil.copy2(run_dir / "mesh_dv_summary.json", canonical)

    runs_csv = outdir / "results_runs.csv"
    with runs_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    # aggregate by group,duty,stable,rtf
    agg_map: Dict[Tuple[str, int, int, int], List[Dict]] = {}
    for r in rows:
        k = (r["group"], r["duty_on"], r["stable"], r["rtf"])
        agg_map.setdefault(k, []).append(r)

    agg_rows = []
    for k, rs in sorted(agg_map.items()):
        group, duty_on, stable, rtf = k
        row = {
            "group": group,
            "duty_on": duty_on,
            "stable": stable,
            "rtf": rtf,
            "route_timeout_sec": stable * rtf,
            "n": len(rs),
        }
        metrics = [
            "delivery_ratio",
            "admission_ratio",
            "tx_attempts_per_generated",
            "source_first_tx_ratio",
            "delay_p95_s",
            "drop_no_route",
            "dv_route_expire_events",
            "route_expired_counter_summary",
            "drop_other",
            "beacon_scheduled",
            "beacon_tx_sent",
        ]
        for metric in metrics:
            vals = [float(x[metric]) for x in rs]
            m, s, (lo, hi) = mean_std_ci95(vals)
            row[f"{metric}_mean"] = m
            row[f"{metric}_std"] = s
            row[f"{metric}_ci95_lo"] = lo
            row[f"{metric}_ci95_hi"] = hi
        ratio_vals = []
        for x in rs:
            sched = float(x["beacon_scheduled"])
            sent = float(x["beacon_tx_sent"])
            ratio_vals.append((sent / sched) if sched > 0 else 0.0)
        rm, rsd, (rlo, rhi) = mean_std_ci95(ratio_vals)
        row["beacon_tx_sent_over_scheduled_mean"] = rm
        row["beacon_tx_sent_over_scheduled_std"] = rsd
        row["beacon_tx_sent_over_scheduled_ci95_lo"] = rlo
        row["beacon_tx_sent_over_scheduled_ci95_hi"] = rhi
        agg_rows.append(row)

    agg_csv = outdir / "results_agg.csv"
    with agg_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(agg_rows[0].keys()))
        w.writeheader()
        w.writerows(agg_rows)

    out_json = outdir / "results.json"
    out_json.write_text(json.dumps({"runs": rows, "agg": agg_rows}, indent=2), encoding="utf-8")

    report = BASE_DIR / f"REPORTE_FOLLOWUP_NO_ROUTE_DUTY_{dt.date.today().isoformat()}.md"
    lines = []
    lines.append("# Follow-up No-Route Duty")
    lines.append("")
    lines.append(f"- Fecha: {dt.datetime.now().isoformat()}")
    lines.append(f"- Outdir: `{outdir}`")
    lines.append(f"- Seeds: `{seeds}`")
    lines.append("")
    lines.append("## Agregado")
    lines.append("")
    lines.append("| group | duty_on | stable | rtf | route_timeout_sec | n | drop_no_route_mean | dv_route_expire_events_mean | delivery_ratio_mean |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for a in agg_rows:
        lines.append(
            f"| {a['group']} | {a['duty_on']} | {a['stable']} | {a['rtf']} | {a['route_timeout_sec']} | {a['n']} | "
            f"{a['drop_no_route_mean']:.2f} | {a['dv_route_expire_events_mean']:.2f} | {a['delivery_ratio_mean']:.4f} |"
        )
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"[OK] outdir: {outdir}")
    print(f"[OK] report: {report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
