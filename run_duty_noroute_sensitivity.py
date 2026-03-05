#!/usr/bin/env python3
"""
Micro-study: sensitivity of no-route drops under duty ON.

Scenario fixed:
- N=25, random 1km^2
- wireFormat=v2
- CSMA/CAD ON
- interference=puello
- txPower=20 dBm
- duty ON, dutyLimit=1%, dutyWindow=3600s
- trafficLoad=medium (10s)
- dataStart=300, dataStop=3900, stop=4500, pdrEndWindow=600

Sweep:
- beaconIntervalStableSec in {60,90,120}
- routeTimeoutFactor in {6,10,14}
- seeds default {1,3,5}
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
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

NOROUTE_REASON_RE = re.compile(r"FWDTRACE DATA_NOROUTE .* reason=([A-Za-z0-9_]+)")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run no-route sensitivity study under duty ON.")
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
        raise ValueError("seed list is empty")
    return out


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_command(cmd: List[str], cwd: Path, log_path: Path | None = None) -> subprocess.CompletedProcess:
    if log_path is not None:
        ensure_dir(log_path.parent)
        with log_path.open("w", encoding="utf-8") as f:
            return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)
    return subprocess.run(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)


def dict_to_cli(d: Dict[str, object]) -> str:
    return " ".join([f"--{k}={v}" for k, v in d.items()])


def load_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def copy_metrics(run_dir: Path) -> None:
    for name in METRIC_FILES:
        src = NS3_DIR / name
        if src.exists():
            shutil.copy2(src, run_dir / name)


def parse_noroute_reasons(log_path: Path) -> Dict[str, int]:
    text = log_path.read_text(encoding="utf-8", errors="replace")
    counts: Dict[str, int] = {}
    for m in NOROUTE_REASON_RE.finditer(text):
        reason = m.group(1)
        counts[reason] = counts.get(reason, 0) + 1
    return counts


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
    summary_dst = run_dir / "mesh_dv_summary.json"
    log_path = run_dir / "run.log"
    meta_path = run_dir / "meta.json"

    if resume and summary_dst.exists() and log_path.exists() and meta_path.exists():
        return {
            "summary": load_json(summary_dst),
            "log_path": log_path,
            "meta": load_json(meta_path),
            "resumed": True,
        }

    cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
    t0 = time.time()
    proc = run_command(cmd, NS3_DIR, log_path)
    elapsed = time.time() - t0

    if proc.returncode != 0:
        raise RuntimeError(f"run failed rc={proc.returncode}: {sim_args}")

    src_summary = NS3_DIR / "mesh_dv_summary.json"
    if not src_summary.exists():
        raise RuntimeError("mesh_dv_summary.json missing")
    shutil.copy2(src_summary, summary_dst)
    copy_metrics(run_dir)

    meta = {
        "cmd": " ".join(cmd),
        "sim_args": sim_args,
        "elapsed_s": elapsed,
        "timestamp": dt.datetime.now().isoformat(),
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return {
        "summary": load_json(summary_dst),
        "log_path": log_path,
        "meta": meta,
        "resumed": False,
    }


def mean_std(values: List[float]) -> Tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], 0.0
    return statistics.mean(values), statistics.stdev(values)


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


def main() -> int:
    args = parse_args()
    seeds = parse_seeds(args.seeds)

    if args.build:
        proc = run_command([str(NS3_BIN), "build"], NS3_DIR)
        if proc.returncode != 0:
            print(proc.stdout)
            raise RuntimeError("build failed")

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else BASE_DIR / "validation_results" / f"duty_noroute_sensitivity_{ts}"
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
        "enableDuty": "true",
        "dutyLimit": 0.01,
        "dutyWindowSec": 3600,
        "dataStartSec": 300,
        "dataStopSec": 3900,
        "stopSec": 4500,
        "pdrEndWindowSec": 600,
        "trafficLoad": "medium",
        "enablePcap": "false",
    }

    stable_vals = [60, 90, 120]
    rtf_vals = [6, 10, 14]

    run_rows: List[Dict] = []
    total = len(stable_vals) * len(rtf_vals) * len(seeds)
    idx = 0

    for stable in stable_vals:
        for rtf in rtf_vals:
            for seed in seeds:
                idx += 1
                sim = dict(base)
                sim["beaconIntervalStableSec"] = stable
                sim["routeTimeoutFactor"] = rtf
                sim["rngRun"] = seed

                sim_args = "mesh_dv_baseline " + dict_to_cli(sim)
                run_dir = outdir / f"stable_{stable}" / f"rtf_{rtf}" / f"seed_{seed}"

                result = run_one(run_dir, sim_args, resume=args.resume)
                summary = result["summary"]
                pdr = summary.get("pdr", {})
                q = summary.get("queue_backlog", {})
                drops = summary.get("drops", {})
                delay = summary.get("delay", {})
                cp = summary.get("control_plane", {})
                reasons = parse_noroute_reasons(result["log_path"])
                route_actions = parse_route_actions(run_dir / "mesh_dv_metrics_routes.csv")
                tx_attempts_per_generated = float(
                    pdr.get("tx_attempts_per_generated", pdr.get("admission_ratio", 0.0))
                )

                row = {
                    "stable": stable,
                    "rtf": rtf,
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
                    "cad_busy_events": int(q.get("cad_busy_events", 0)),
                    "duty_blocked_events": int(q.get("duty_blocked_events", 0)),
                    "beacon_scheduled": int(cp.get("beacon_scheduled", 0)),
                    "beacon_tx_sent": int(cp.get("beacon_tx_sent", 0)),
                    "no_route_v2": int(reasons.get("no_route_v2", 0)),
                    "no_route_rx": int(reasons.get("no_route_rx", 0)),
                    "no_link_addr_for_unicast_v2": int(reasons.get("no_link_addr_for_unicast_v2", 0))
                    + int(reasons.get("no_mac_for_unicast_v2", 0)),
                    "route_EXPIRE": int(route_actions.get("EXPIRE", 0)),
                    "route_PURGE": int(route_actions.get("PURGE", 0)),
                    "route_POISON": int(route_actions.get("POISON", 0)),
                    "run_dir": str(run_dir),
                }
                run_rows.append(row)

                print(
                    f"[{idx:02d}/{total}] stable={stable}s rtf={rtf} seed={seed} "
                    f"drop_no_route={row['drop_no_route']} no_route_v2={row['no_route_v2']} "
                    f"no_linkaddr={row['no_link_addr_for_unicast_v2']} expire={row['route_EXPIRE']} "
                    f"pdr={row['delivery_ratio']:.4f} {'(resume)' if result['resumed'] else ''}"
                )

    # Aggregate by (stable, rtf)
    grouped: Dict[Tuple[int, int], List[Dict]] = {}
    for r in run_rows:
        grouped.setdefault((r["stable"], r["rtf"]), []).append(r)

    agg_rows: List[Dict] = []
    for key, rows in sorted(grouped.items()):
        stable, rtf = key
        agg = {"stable": stable, "rtf": rtf, "n": len(rows)}
        for metric in [
            "delivery_ratio",
            "admission_ratio",
            "tx_attempts_per_generated",
            "source_first_tx_ratio",
            "delay_p95_s",
            "drop_no_route",
            "no_route_v2",
            "no_route_rx",
            "no_link_addr_for_unicast_v2",
            "route_EXPIRE",
            "route_PURGE",
            "route_POISON",
            "cad_busy_events",
            "duty_blocked_events",
            "beacon_scheduled",
            "beacon_tx_sent",
        ]:
            vals = [float(x[metric]) for x in rows]
            m, s, (lo, hi) = mean_std_ci95(vals)
            agg[f"{metric}_mean"] = m
            agg[f"{metric}_std"] = s
            agg[f"{metric}_ci95_lo"] = lo
            agg[f"{metric}_ci95_hi"] = hi
        ratio_vals = []
        for x in rows:
            sched = float(x["beacon_scheduled"])
            sent = float(x["beacon_tx_sent"])
            ratio_vals.append((sent / sched) if sched > 0 else 0.0)
        rm, rs, (rlo, rhi) = mean_std_ci95(ratio_vals)
        agg["beacon_tx_sent_over_scheduled_mean"] = rm
        agg["beacon_tx_sent_over_scheduled_std"] = rs
        agg["beacon_tx_sent_over_scheduled_ci95_lo"] = rlo
        agg["beacon_tx_sent_over_scheduled_ci95_hi"] = rhi
        # Canonical alias for comparability across campaigns
        agg["dv_route_expire_events_mean"] = agg.get("route_EXPIRE_mean", 0.0)
        agg["dv_route_expire_events_std"] = agg.get("route_EXPIRE_std", 0.0)
        agg["dv_route_expire_events_ci95_lo"] = agg.get("route_EXPIRE_ci95_lo", 0.0)
        agg["dv_route_expire_events_ci95_hi"] = agg.get("route_EXPIRE_ci95_hi", 0.0)
        agg_rows.append(agg)

    # CSV outputs
    run_csv = outdir / "results_runs.csv"
    agg_csv = outdir / "results_agg.csv"
    with run_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(run_rows[0].keys()))
        w.writeheader()
        w.writerows(run_rows)
    with agg_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(agg_rows[0].keys()))
        w.writeheader()
        w.writerows(agg_rows)

    result_json = outdir / "results.json"
    result_json.write_text(json.dumps({"runs": run_rows, "agg": agg_rows}, indent=2), encoding="utf-8")

    # Markdown report
    report_path = BASE_DIR / f"ANALISIS_SENSIBILIDAD_NO_ROUTE_DUTY_ON_{dt.date.today().isoformat()}.md"
    lines = []
    lines.append("# Analisis Sensibilidad No-Route con Duty ON")
    lines.append("")
    lines.append(f"- Fecha: {dt.datetime.now().isoformat()}")
    lines.append(f"- Outdir: `{outdir}`")
    lines.append(f"- Seeds: `{seeds}`")
    lines.append("- Escenario fijo: N=25, random 1km2, medium, CSMA ON, duty ON(1%), ventana duty 1h, wire=v2")
    lines.append("")

    lines.append("## Resumen agregado por configuracion")
    lines.append("")
    lines.append("| stable (s) | rtf | n | drop_no_route mean+-std | no_route_v2 mean | no_link_addr_for_unicast_v2 mean | route_EXPIRE mean | delivery_ratio mean |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for a in agg_rows:
        lines.append(
            f"| {a['stable']} | {a['rtf']} | {a['n']} | "
            f"{a['drop_no_route_mean']:.2f} +- {a['drop_no_route_std']:.2f} | "
            f"{a['no_route_v2_mean']:.2f} | {a['no_link_addr_for_unicast_v2_mean']:.2f} | "
            f"{a['route_EXPIRE_mean']:.2f} | {a['delivery_ratio_mean']:.4f} |"
        )

    lines.append("")
    lines.append("## Lectura causal")
    lines.append("")
    lines.append("- `drop_no_route` se descompone principalmente en `reason=no_route_v2` (sin ruta DV vigente) y `reason=no_link_addr_for_unicast_v2`.")
    lines.append("- Si al aumentar `beaconIntervalStableSec` y/o bajar frescura efectiva (`routeTimeoutFactor` relativo a beacon) sube `route_EXPIRE` y sube `no_route_v2`, hay sensibilidad clara de expiracion DV bajo restriccion duty.")
    lines.append("- `duty_blocked_events` alto confirma presion de duty sobre TX, que puede retrasar propagacion de beacons/updates y degradar vigencia de rutas.")
    lines.append("")
    lines.append("## Archivos")
    lines.append("")
    lines.append(f"- Corridas por seed: `{outdir}`")
    lines.append(f"- Detalle por corrida: `{run_csv}`")
    lines.append(f"- Agregado por configuracion: `{agg_csv}`")

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"[OK] Completed. Outdir: {outdir}")
    print(f"[OK] Report: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
