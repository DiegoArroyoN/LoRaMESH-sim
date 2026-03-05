#!/usr/bin/env python3
import csv
import datetime as dt
import json
import math
import os
import pathlib
import statistics
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent
NS3_DIR = ROOT.parent.parent

SEEDS = [1, 3, 5]
MODELS = ["puello", "goursaud"]
DUTY_CASES = [
    ("duty_off", {"enableDuty": "false"}),
    ("duty_on", {"enableDuty": "true", "dutyLimit": "0.01", "dutyWindowSec": "3600"}),
]

BASE_ARGS = {
    "wireFormat": "v2",
    "nEd": "25",
    "nodePlacementMode": "random",
    "areaWidth": "1000",
    "areaHeight": "1000",
    "enableCsma": "true",
    "txPowerDbm": "20",
    "beaconIntervalWarmSec": "10",
    "beaconIntervalStableSec": "120",
    "routeTimeoutFactor": "7",
    "dataStartSec": "300",
    "dataStopSec": "900",
    "stopSec": "1200",
    "pdrEndWindowSec": "300",
    "trafficLoad": "medium",
    "enablePcap": "false",
}

def to_run_string(args: dict) -> str:
    parts = ["mesh_dv_baseline"]
    for k, v in args.items():
        parts.append(f"--{k}={v}")
    return " ".join(parts)


def mean_std(vals):
    if not vals:
        return 0.0, 0.0
    if len(vals) == 1:
        return float(vals[0]), 0.0
    return float(statistics.mean(vals)), float(statistics.stdev(vals))


def mean_std_ci95(vals):
    if not vals:
        return 0.0, 0.0, (0.0, 0.0)
    if len(vals) == 1:
        v = float(vals[0])
        return v, 0.0, (v, v)
    mu = float(statistics.mean(vals))
    sd = float(statistics.stdev(vals))
    h = 1.96 * (sd / math.sqrt(len(vals)))
    return mu, sd, (mu - h, mu + h)


def main() -> int:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = ROOT / "validation_results" / f"interference_ab_compare_{ts}"
    outdir.mkdir(parents=True, exist_ok=True)

    rows = []
    total = len(SEEDS) * len(MODELS) * len(DUTY_CASES)
    done = 0

    for model in MODELS:
        for duty_name, duty_args in DUTY_CASES:
            for seed in SEEDS:
                done += 1
                run_args = dict(BASE_ARGS)
                run_args["interferenceModel"] = model
                run_args.update(duty_args)
                run_args["rngRun"] = str(seed)

                run_id = f"n25_random_medium_csma_on_{duty_name}_{model}_seed{seed}"
                run_dir = outdir / run_id
                run_dir.mkdir(parents=True, exist_ok=True)

                cmd = ["./ns3", "run", to_run_string(run_args)]
                log_path = run_dir / "run.log"
                meta_path = run_dir / "meta.json"
                summary_src = NS3_DIR / "mesh_dv_summary.json"
                summary_dst = ROOT / f"mesh_dv_summary_{run_id}.json"
                summary_copy = run_dir / f"mesh_dv_summary_{run_id}.json"

                meta = {
                    "run_id": run_id,
                    "model": model,
                    "duty": duty_name,
                    "seed": seed,
                    "args": run_args,
                    "cmd": " ".join(cmd),
                }
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                print(f"[{done}/{total}] {run_id}")
                with open(log_path, "w", encoding="utf-8") as logf:
                    proc = subprocess.run(
                        cmd,
                        cwd=NS3_DIR,
                        stdout=logf,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                if proc.returncode != 0:
                    print(f"ERROR run failed: {run_id}")
                    rows.append(
                        {
                            "run_id": run_id,
                            "model": model,
                            "duty": duty_name,
                            "seed": seed,
                            "ok": 0,
                            "delivery_ratio": 0.0,
                            "pdr_eligible": 0.0,
                            "source_first_tx_ratio": 0.0,
                            "tx_attempts_per_generated": 0.0,
                            "delay_p95_s": 0.0,
                            "delivered_per_tx_attempt": 0.0,
                            "beacon_scheduled": 0,
                            "beacon_tx_sent": 0,
                            "dv_route_expire_events": 0,
                            "cad_busy_events": 0,
                            "duty_blocked_events": 0,
                            "drop_no_route": 0,
                            "drop_ttl_expired": 0,
                            "drop_backtrack": 0,
                            "drop_other": 0,
                            "queued_packets_end": 0,
                        }
                    )
                    continue

                if not summary_src.exists():
                    print(f"ERROR summary not found: {summary_src}")
                    continue

                # Canonical copies
                summary_copy.write_bytes(summary_src.read_bytes())
                summary_dst.write_bytes(summary_src.read_bytes())

                data = json.loads(summary_src.read_text(encoding="utf-8"))
                pdr = data.get("pdr", {})
                routes = data.get("routes", {})
                q = data.get("queue_backlog", {})
                drops = data.get("drops", {})
                delay = data.get("delay", {})
                cp = data.get("control_plane", {})
                tx_attempts_per_generated = float(
                    pdr.get("tx_attempts_per_generated", pdr.get("admission_ratio", 0.0))
                )

                rows.append(
                    {
                        "run_id": run_id,
                        "model": model,
                        "duty": duty_name,
                        "seed": seed,
                        "ok": 1,
                        "delivery_ratio": pdr.get("delivery_ratio", 0.0),
                        "pdr_eligible": pdr.get("pdr_e2e_generated_eligible", 0.0),
                        "source_first_tx_ratio": pdr.get("source_first_tx_ratio", 0.0),
                        "tx_attempts_per_generated": tx_attempts_per_generated,
                        "delay_p95_s": delay.get("p95_s", 0.0),
                        "delivered_per_tx_attempt": pdr.get("delivered_per_tx_attempt", 0.0),
                        "beacon_scheduled": cp.get("beacon_scheduled", 0),
                        "beacon_tx_sent": cp.get("beacon_tx_sent", 0),
                        "dv_route_expire_events": routes.get("dv_route_expire_events", 0),
                        "cad_busy_events": q.get("cad_busy_events", 0),
                        "duty_blocked_events": q.get("duty_blocked_events", 0),
                        "drop_no_route": drops.get("drop_no_route", 0),
                        "drop_ttl_expired": drops.get("drop_ttl_expired", 0),
                        "drop_backtrack": drops.get("drop_backtrack", 0),
                        "drop_other": drops.get("drop_other", 0),
                        "queued_packets_end": q.get("queued_packets_end", 0),
                    }
                )

    runs_csv = outdir / "results_runs.csv"
    with open(runs_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["run_id"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Aggregate by (model,duty)
    agg = []
    groups = {}
    for r in rows:
        if r.get("ok", 0) != 1:
            continue
        groups.setdefault((r["model"], r["duty"]), []).append(r)

    metrics = [
        "delivery_ratio",
        "delay_p95_s",
        "pdr_eligible",
        "source_first_tx_ratio",
        "tx_attempts_per_generated",
        "delivered_per_tx_attempt",
        "beacon_scheduled",
        "beacon_tx_sent",
        "dv_route_expire_events",
        "cad_busy_events",
        "duty_blocked_events",
        "drop_no_route",
        "queued_packets_end",
    ]

    for (model, duty), rr in sorted(groups.items()):
        row = {"model": model, "duty": duty, "n": len(rr)}
        for m in metrics:
            vals = [float(x[m]) for x in rr]
            mu, sd, (lo, hi) = mean_std_ci95(vals)
            row[f"{m}_mean"] = mu
            row[f"{m}_std"] = sd
            row[f"{m}_ci95_lo"] = lo
            row[f"{m}_ci95_hi"] = hi
        ratio_vals = []
        for x in rr:
            sched = float(x["beacon_scheduled"])
            sent = float(x["beacon_tx_sent"])
            ratio_vals.append((sent / sched) if sched > 0 else 0.0)
        rmu, rsd, (rlo, rhi) = mean_std_ci95(ratio_vals)
        row["beacon_tx_sent_over_scheduled_mean"] = rmu
        row["beacon_tx_sent_over_scheduled_std"] = rsd
        row["beacon_tx_sent_over_scheduled_ci95_lo"] = rlo
        row["beacon_tx_sent_over_scheduled_ci95_hi"] = rhi
        agg.append(row)

    agg_csv = outdir / "results_agg.csv"
    with open(agg_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(agg[0].keys()) if agg else ["model", "duty", "n"])
        w.writeheader()
        for r in agg:
            w.writerow(r)

    rep = outdir / f"REPORTE_AB_INTERFERENCIA_{dt.date.today().isoformat()}.md"
    lines = []
    lines.append("# Reporte A/B InterferenceModel (puello vs goursaud)")
    lines.append("")
    lines.append(f"- Fecha: {dt.datetime.now().isoformat()}")
    lines.append(f"- Outdir: `{outdir}`")
    lines.append("- Escenario fijo: N=25, random 1km2, wire=v2, CSMA ON, traffic=medium, seeds=1/3/5")
    lines.append("- Timing: dataStart=300, dataStop=900, stop=1200, pdrEndWindow=300")
    lines.append("")
    lines.append("## Agregado por modelo/duty")
    lines.append("")
    lines.append("| model | duty | n | delivery_ratio mean±std | delay_p95_s mean±std | tx_attempts_per_generated mean±std | source_first_tx_ratio mean±std | beacon_tx_sent/scheduled mean | dv_route_expire_events mean±std | drop_no_route mean±std |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for a in agg:
        lines.append(
            f"| {a['model']} | {a['duty']} | {a['n']} | "
            f"{a['delivery_ratio_mean']:.4f} ± {a['delivery_ratio_std']:.4f} | "
            f"{a['delay_p95_s_mean']:.4f} ± {a['delay_p95_s_std']:.4f} | "
            f"{a['tx_attempts_per_generated_mean']:.4f} ± {a['tx_attempts_per_generated_std']:.4f} | "
            f"{a['source_first_tx_ratio_mean']:.4f} ± {a['source_first_tx_ratio_std']:.4f} | "
            f"{a['beacon_tx_sent_over_scheduled_mean']:.4f} | "
            f"{a['dv_route_expire_events_mean']:.2f} ± {a['dv_route_expire_events_std']:.2f} | "
            f"{a['drop_no_route_mean']:.2f} ± {a['drop_no_route_std']:.2f} | "
        )
    lines.append("")
    rep.write_text("\n".join(lines), encoding="utf-8")

    print(f"DONE: {outdir}")
    print(f"RUNS_CSV: {runs_csv}")
    print(f"AGG_CSV: {agg_csv}")
    print(f"REPORT: {rep}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
