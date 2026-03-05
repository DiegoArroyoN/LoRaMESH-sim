#!/usr/bin/env python3
import csv
import datetime as dt
import json
import re
import shutil
import statistics
import subprocess
import time
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
NS3_DIR = THIS_DIR.parents[1]  # ns-3-dev
NS3_BIN = NS3_DIR / "ns3"

TIMESTAMP = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
OUTDIR = THIS_DIR / "validation_results" / f"campaign_hybrid_cad_{TIMESTAMP}"
OUTDIR.mkdir(parents=True, exist_ok=True)

NEDS = [16, 32]
LOADS = ["low", "medium", "high", "saturation"]
SEEDS = [1, 3]

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

PROFILES = {
    "baseline": {
        "dataPeriodJitterMaxSec": 0.5,
        "minBackoffSlots": 8,
        "backoffStep": 4,
        "controlBackoffFactor": 0.5,
        "dataBackoffFactor": 1.0,
        "beaconIntervalStableSec": 60,
        "extraDvBeaconMaxPerWindow": 2,
        "disableExtraAfterWarmup": "false",
        "prioritizeBeacons": "true",
    },
    "h1_jitter": {
        "dataPeriodJitterMaxSec": 3.0,
        "minBackoffSlots": 8,
        "backoffStep": 4,
        "controlBackoffFactor": 0.5,
        "dataBackoffFactor": 1.0,
        "beaconIntervalStableSec": 60,
        "extraDvBeaconMaxPerWindow": 2,
        "disableExtraAfterWarmup": "false",
        "prioritizeBeacons": "true",
    },
    "h2_jitter_backoff": {
        "dataPeriodJitterMaxSec": 3.0,
        "minBackoffSlots": 4,
        "backoffStep": 2,
        "controlBackoffFactor": 0.8,
        "dataBackoffFactor": 0.6,
        "beaconIntervalStableSec": 60,
        "extraDvBeaconMaxPerWindow": 2,
        "disableExtraAfterWarmup": "false",
        "prioritizeBeacons": "true",
    },
    "h3_jitter_backoff_overhead": {
        "dataPeriodJitterMaxSec": 3.0,
        "minBackoffSlots": 4,
        "backoffStep": 2,
        "controlBackoffFactor": 0.8,
        "dataBackoffFactor": 0.6,
        "beaconIntervalStableSec": 90,
        "extraDvBeaconMaxPerWindow": 1,
        "disableExtraAfterWarmup": "true",
        "prioritizeBeacons": "true",
    },
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

    tx_data_ok = 0
    tx_data_ok_origin = 0
    tx_beacon_ok = 0
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
        if tag_dst == 65535:
            tx_beacon_ok += 1
        else:
            tx_data_ok += 1
            if node == tag_src:
                tx_data_ok_origin += 1

    pdr_real = (delivered / app_send) if app_send > 0 else 0.0
    not_tx_origin = max(app_send - tx_data_ok_origin, 0)
    tx_not_delivered = max(tx_data_ok_origin - delivered, 0)

    return {
        "app_generated": app_send,
        "fwd_delivered": delivered,
        "cad_busy": cad_busy,
        "duty_block": duty_block,
        "no_route": no_route,
        "queue_max": queue_max,
        "queue_last": queue_last,
        "tx_data_ok": tx_data_ok,
        "tx_data_ok_origin": tx_data_ok_origin,
        "tx_beacon_ok": tx_beacon_ok,
        "pdr_real": pdr_real,
        "not_tx_origin": not_tx_origin,
        "tx_not_delivered": tx_not_delivered,
    }


def mean_std(values):
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return float(values[0]), 0.0
    return float(statistics.mean(values)), float(statistics.stdev(values))


rows = []
total_runs = len(NEDS) * len(LOADS) * len(SEEDS) * len(PROFILES)
run_idx = 0

for profile_name, profile_args in PROFILES.items():
    for n_ed in NEDS:
        for load in LOADS:
            for seed in SEEDS:
                run_idx += 1
                scenario = f"{profile_name}_n{n_ed}_{load}_seed{seed}"
                run_dir = OUTDIR / scenario
                run_dir.mkdir(parents=True, exist_ok=True)

                args = {}
                args.update(COMMON_ARGS)
                args.update(profile_args)
                args["nEd"] = n_ed
                args["trafficLoad"] = load
                args["rngRun"] = seed

                ns3_args = dict_to_args(args)
                cmd = [str(NS3_BIN), "run", f"mesh_dv_baseline {ns3_args}"]

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

                log_metrics = parse_log_metrics(log_text)

                row = {
                    "profile": profile_name,
                    "nEd": n_ed,
                    "load": load,
                    "seed": seed,
                    "exit_code": proc.returncode,
                    "elapsed_s": round(elapsed, 3),
                    "summary_total_data_tx": int(summary.get("pdr", {}).get("total_data_tx", 0)),
                    "summary_delivered": int(summary.get("pdr", {}).get("delivered", 0)),
                    "summary_pdr": float(summary.get("pdr", {}).get("pdr", 0.0)),
                }
                row.update(log_metrics)
                rows.append(row)

                status = "OK" if proc.returncode == 0 else f"FAIL({proc.returncode})"
                print(
                    f"[{run_idx:02d}/{total_runs}] {scenario} {status} "
                    f"pdr={row['summary_pdr']:.4f} real={row['pdr_real']:.4f} "
                    f"cad_busy={row['cad_busy']} not_tx_origin={row['not_tx_origin']}"
                )

raw_csv = OUTDIR / "campaign_raw.csv"
with raw_csv.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

# Aggregate by profile, nEd, load
agg_rows = []
for profile_name in PROFILES.keys():
    for n_ed in NEDS:
        for load in LOADS:
            subset = [r for r in rows if r["profile"] == profile_name and r["nEd"] == n_ed and r["load"] == load]
            if not subset:
                continue

            def m(field):
                return mean_std([float(x[field]) for x in subset])

            pdr_m, pdr_s = m("summary_pdr")
            real_m, real_s = m("pdr_real")
            cad_m, cad_s = m("cad_busy")
            ntx_m, ntx_s = m("not_tx_origin")
            qmax_m, qmax_s = m("queue_max")
            txo_m, txo_s = m("tx_data_ok_origin")
            deliv_m, deliv_s = m("summary_delivered")
            bcn_m, bcn_s = m("tx_beacon_ok")

            agg_rows.append(
                {
                    "profile": profile_name,
                    "nEd": n_ed,
                    "load": load,
                    "runs": len(subset),
                    "pdr_mean": pdr_m,
                    "pdr_std": pdr_s,
                    "pdr_real_mean": real_m,
                    "pdr_real_std": real_s,
                    "cad_busy_mean": cad_m,
                    "cad_busy_std": cad_s,
                    "not_tx_origin_mean": ntx_m,
                    "not_tx_origin_std": ntx_s,
                    "queue_max_mean": qmax_m,
                    "queue_max_std": qmax_s,
                    "tx_data_ok_origin_mean": txo_m,
                    "tx_data_ok_origin_std": txo_s,
                    "delivered_mean": deliv_m,
                    "delivered_std": deliv_s,
                    "beacon_tx_ok_mean": bcn_m,
                    "beacon_tx_ok_std": bcn_s,
                }
            )

agg_csv = OUTDIR / "campaign_aggregate.csv"
with agg_csv.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(agg_rows[0].keys()))
    writer.writeheader()
    writer.writerows(agg_rows)

print(f"DONE outdir={OUTDIR}")
