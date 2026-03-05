#!/usr/bin/env python3
import csv
import datetime as dt
import json
import re
import shutil
import statistics
import subprocess
import time
from collections import defaultdict
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
NS3_DIR = THIS_DIR.parents[1]  # ns-3-dev
NS3_BIN = NS3_DIR / "ns3"

TIMESTAMP = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
OUTDIR = THIS_DIR / "validation_results" / f"campaign_h3_interval_up_{TIMESTAMP}"
OUTDIR.mkdir(parents=True, exist_ok=True)

NEDS = [8, 16, 24, 32]
LOADS = ["low", "medium"]
SEEDS = [1, 2, 3, 5, 8]

COMMON_ARGS = {
    "nodePlacementMode": "random",
    "areaWidth": 1000,
    "areaHeight": 1000,
    "stopSec": 300,
    "dataStartSec": 90,
    "dataStopSec": -1,
    "enableCsma": "true",
    "enableDuty": "true",
    "dutyLimit": 0.01,
    "enableControlGuard": "false",
    "interferenceModel": "puello",
    "enablePcap": "false",
    "enableDataSlots": "false",
}

# H3 plus higher mean interval via bigger per-period jitter.
# Current model uses nextDelay = basePeriod + U[0, jitterMax],
# so E[nextDelay] = basePeriod + jitterMax/2.
H3_INTERVAL_UP_ARGS = {
    "dataPeriodJitterMaxSec": 6.0,  # raises mean interval by +3.0s
    "minBackoffSlots": 4,
    "backoffStep": 2,
    "controlBackoffFactor": 0.8,
    "dataBackoffFactor": 0.6,
    "beaconIntervalStableSec": 90,
    "extraDvBeaconMaxPerWindow": 1,
    "disableExtraAfterWarmup": "true",
    "prioritizeBeacons": "true",
}


def dict_to_args(d):
    return " ".join(f"--{k}={v}" for k, v in d.items())


def parse_log_metrics(log_text):
    app_send = len(re.findall(r"APP_SEND_DATA", log_text))
    delivered = len(re.findall(r"FWDTRACE deliver", log_text))
    cad_busy = len(re.findall(r"CAD_RESULT detail: node=.*busy=1", log_text))
    duty_block = len(re.findall(r"reason=duty_block", log_text))
    no_route = len(re.findall(r"FWDTRACE DATA_NOROUTE .*reason=no_route\\b", log_text))

    q_sizes = [int(x) for x in re.findall(r"CSMA: Paquete en cola \(size=(\d+)\)", log_text)]
    queue_max = max(q_sizes) if q_sizes else 0
    queue_last = q_sizes[-1] if q_sizes else 0

    tx_data_ok_origin = 0
    for m in re.finditer(
        r"CSMA_TX_QUEUE_OUT node=(\d+).*?tagDst=(\d+).*?tagSrc=(\d+).*?ok=(\w+)",
        log_text,
    ):
        node = int(m.group(1))
        tag_dst = int(m.group(2))
        tag_src = int(m.group(3))
        ok = m.group(4).lower() in ("1", "true")
        if not ok:
            continue
        if tag_dst != 65535 and node == tag_src:
            tx_data_ok_origin += 1

    pdr_real = (delivered / app_send) if app_send > 0 else 0.0

    return {
        "app_generated": app_send,
        "fwd_delivered": delivered,
        "cad_busy": cad_busy,
        "duty_block": duty_block,
        "no_route": no_route,
        "queue_max": queue_max,
        "queue_last": queue_last,
        "tx_data_ok_origin": tx_data_ok_origin,
        "pdr_real": pdr_real,
    }


def parse_per_node_pdr(log_text):
    gen = defaultdict(int)
    deliv = defaultdict(int)

    for m in re.finditer(r"APP_SEND_DATA src=(\d+) dst=(\d+) seq=(\d+)", log_text):
        src = int(m.group(1))
        gen[src] += 1

    for m in re.finditer(r"FWDTRACE deliver .* src=(\d+) dst=(\d+) seq=(\d+)", log_text):
        src = int(m.group(1))
        deliv[src] += 1

    rows = []
    for src in sorted(set(gen.keys()) | set(deliv.keys())):
        g = gen[src]
        d = deliv[src]
        p = (d / g) if g > 0 else 0.0
        rows.append({"src": src, "generated": g, "delivered": d, "pdr": p})
    return rows


def mean_std(values):
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return float(values[0]), 0.0
    return float(statistics.mean(values)), float(statistics.stdev(values))


raw_rows = []
scenario_node_acc = defaultdict(lambda: defaultdict(lambda: {"generated": 0, "delivered": 0}))

TOTAL_RUNS = len(NEDS) * len(LOADS) * len(SEEDS)
idx = 0

for n_ed in NEDS:
    for load in LOADS:
        for seed in SEEDS:
            idx += 1
            scenario = f"h3_up_n{n_ed}_{load}_seed{seed}"
            run_dir = OUTDIR / scenario
            run_dir.mkdir(parents=True, exist_ok=True)

            args = {}
            args.update(COMMON_ARGS)
            args.update(H3_INTERVAL_UP_ARGS)
            args["nEd"] = n_ed
            args["trafficLoad"] = load
            args["rngRun"] = seed

            cmd = [str(NS3_BIN), "run", f"mesh_dv_baseline {dict_to_args(args)}"]
            t0 = time.time()
            proc = subprocess.run(
                cmd,
                cwd=NS3_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            elapsed = time.time() - t0
            log_text = proc.stdout
            (run_dir / "run.log").write_text(log_text)

            summary_src_candidates = [
                NS3_DIR / "mesh_dv_summary.json",
                THIS_DIR / "mesh_dv_summary.json",
            ]
            summary_src = None
            for c in summary_src_candidates:
                if c.exists():
                    summary_src = c
                    break
            if summary_src is None:
                raise RuntimeError(f"No summary JSON found after run: {scenario}")

            summary_dst = run_dir / "mesh_dv_summary.json"
            shutil.copy2(summary_src, summary_dst)
            summary = json.loads(summary_dst.read_text())

            lm = parse_log_metrics(log_text)
            per_node = parse_per_node_pdr(log_text)

            with (run_dir / "per_node_pdr.csv").open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["src", "generated", "delivered", "pdr"])
                w.writeheader()
                w.writerows(per_node)

            scen_key = (n_ed, load)
            for r in per_node:
                src = int(r["src"])
                scenario_node_acc[scen_key][src]["generated"] += int(r["generated"])
                scenario_node_acc[scen_key][src]["delivered"] += int(r["delivered"])

            row = {
                "profile": "h3_interval_up",
                "nEd": n_ed,
                "load": load,
                "seed": seed,
                "exit_code": proc.returncode,
                "elapsed_s": round(elapsed, 3),
                "summary_total_data_tx": int(summary.get("pdr", {}).get("total_data_tx", 0)),
                "summary_delivered": int(summary.get("pdr", {}).get("delivered", 0)),
                "summary_pdr": float(summary.get("pdr", {}).get("pdr", 0.0)),
            }
            row.update(lm)
            raw_rows.append(row)

            status = "OK" if proc.returncode == 0 else f"FAIL({proc.returncode})"
            print(
                f"[{idx:02d}/{TOTAL_RUNS}] {scenario} {status} "
                f"pdr_summary={row['summary_pdr']:.4f} pdr_real={row['pdr_real']:.4f} "
                f"cad_busy={row['cad_busy']}"
            )

raw_csv = OUTDIR / "campaign_raw.csv"
with raw_csv.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(raw_rows[0].keys()))
    w.writeheader()
    w.writerows(raw_rows)

# Global aggregate by scenario
agg_rows = []
for n_ed in NEDS:
    for load in LOADS:
        sub = [r for r in raw_rows if r["nEd"] == n_ed and r["load"] == load]
        if not sub:
            continue

        def m(field):
            return mean_std([float(x[field]) for x in sub])

        pdr_s_m, pdr_s_std = m("summary_pdr")
        pdr_r_m, pdr_r_std = m("pdr_real")
        cad_m, cad_std = m("cad_busy")
        notx_m, notx_std = m("app_generated")
        qmax_m, qmax_std = m("queue_max")

        generated_total = int(sum(int(x["app_generated"]) for x in sub))
        delivered_total = int(sum(int(x["fwd_delivered"]) for x in sub))
        pdr_real_total = (delivered_total / generated_total) if generated_total > 0 else 0.0

        agg_rows.append(
            {
                "profile": "h3_interval_up",
                "nEd": n_ed,
                "load": load,
                "runs": len(sub),
                "generated_total": generated_total,
                "delivered_total": delivered_total,
                "pdr_real_total": pdr_real_total,
                "summary_pdr_mean": pdr_s_m,
                "summary_pdr_std": pdr_s_std,
                "pdr_real_mean": pdr_r_m,
                "pdr_real_std": pdr_r_std,
                "cad_busy_mean": cad_m,
                "cad_busy_std": cad_std,
                "queue_max_mean": qmax_m,
                "queue_max_std": qmax_std,
            }
        )

agg_csv = OUTDIR / "campaign_global_aggregate.csv"
with agg_csv.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(agg_rows[0].keys()))
    w.writeheader()
    w.writerows(agg_rows)

# Per-node aggregate by scenario
node_rows = []
for (n_ed, load), per_src in sorted(scenario_node_acc.items()):
    for src in sorted(per_src.keys()):
        g = per_src[src]["generated"]
        d = per_src[src]["delivered"]
        p = (d / g) if g > 0 else 0.0
        node_rows.append(
            {
                "profile": "h3_interval_up",
                "nEd": n_ed,
                "load": load,
                "src": src,
                "generated_total": g,
                "delivered_total": d,
                "pdr": p,
            }
        )

node_csv = OUTDIR / "campaign_per_node_aggregate.csv"
with node_csv.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(node_rows[0].keys()))
    w.writeheader()
    w.writerows(node_rows)

print(f"DONE outdir={OUTDIR}")
