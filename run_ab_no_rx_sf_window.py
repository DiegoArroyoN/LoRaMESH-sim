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
NS3_DIR = THIS_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"
TIMESTAMP = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
OUTDIR = THIS_DIR / "validation_results" / f"ab_no_rx_sf_window_{TIMESTAMP}"
OUTDIR.mkdir(parents=True, exist_ok=True)

SEEDS = [1, 2, 3]
STOP_SEC = 300.0

COMMON_ARGS = {
    "nEd": 32,
    "nodePlacementMode": "random",
    "areaWidth": 1000,
    "areaHeight": 1000,
    "stopSec": int(STOP_SEC),
    "dataStartSec": 90,
    "trafficLoad": "medium",
    "enablePcap": "false",
    "enableDuty": "true",
    "dutyLimit": 0.01,
    "enableCsma": "true",
    "interferenceModel": "puello",
    "dataPeriodJitterMaxSec": 3.0,
    "minBackoffSlots": 4,
    "backoffStep": 2,
    "controlBackoffFactor": 0.8,
    "dataBackoffFactor": 0.6,
    "beaconIntervalStableSec": 90,
    "extraDvBeaconMaxPerWindow": 1,
    "disableExtraAfterWarmup": "true",
    "enableControlGuard": "false",
}

PROFILES = {
    "A_legacy_timeout": {"neighborLinkTimeoutSec": 100000},
    "B_recent_window": {"neighborLinkTimeoutSec": 90},
}


def dict_to_args(d):
    return " ".join(f"--{k}={v}" for k, v in d.items())


def parse_app_packets(log_text):
    app = {}
    for m in re.finditer(r"APP_SEND_DATA src=(\d+) dst=(\d+) seq=(\d+) time=([0-9eE+\-.]+)", log_text):
        src = int(m.group(1))
        dst = int(m.group(2))
        seq = int(m.group(3))
        t = float(m.group(4))
        app[(src, dst, seq)] = t
    return app


def parse_fwd_rx(log_text):
    rx = defaultdict(list)
    pat = re.compile(r"FWDTRACE rx time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)")
    for m in pat.finditer(log_text):
        t = float(m.group(1))
        node = int(m.group(2))
        src = int(m.group(3))
        dst = int(m.group(4))
        seq = int(m.group(5))
        rx[(src, dst, seq)].append((node, t))
    return rx


def parse_fwd_fwd(log_text):
    fwd = defaultdict(list)
    pat = re.compile(r"FWDTRACE fwd time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)")
    for m in pat.finditer(log_text):
        t = float(m.group(1))
        node = int(m.group(2))
        src = int(m.group(3))
        dst = int(m.group(4))
        seq = int(m.group(5))
        fwd[(src, dst, seq)].append((node, t))
    return fwd


def parse_deliver(log_text):
    delivered = {}
    pat = re.compile(r"FWDTRACE deliver time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)")
    for m in pat.finditer(log_text):
        t = float(m.group(1))
        node = int(m.group(2))
        src = int(m.group(3))
        dst = int(m.group(4))
        seq = int(m.group(5))
        delivered[(src, dst, seq)] = (node, t)
    return delivered


def parse_origin_tx_times_from_csv(tx_csv_path, keys_set):
    tx_origin = {}
    if not tx_csv_path.exists():
        return tx_origin
    with tx_csv_path.open() as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                t = float(row["timestamp(s)"])
                node = int(row["nodeId"])
                seq = int(row["seq"])
                dst = int(row["dst"])
                ok = int(row["ok"]) == 1
            except Exception:
                continue
            if not ok or dst == 65535:
                continue
            key = (node, dst, seq)
            if key not in keys_set:
                continue
            if key not in tx_origin or t < tx_origin[key]:
                tx_origin[key] = t
    return tx_origin


def classify_packets(app, tx_origin, rx, fwd, delivered):
    cls = defaultdict(int)
    for key, gen_t in app.items():
        src, _, _ = key
        tx_t = tx_origin.get(key)
        deliv = key in delivered
        rx_list = rx.get(key, [])
        fwd_list = fwd.get(key, [])

        if deliv:
            cls["delivered"] += 1
            continue

        if tx_t is None:
            cls["no_tx_origin"] += 1
            continue

        cls["tx_not_delivered"] += 1
        if gen_t >= (STOP_SEC - 1.0) or tx_t >= (STOP_SEC - 1.0):
            cls["tx_not_delivered_end_window"] += 1
            continue

        relay_fwd = any(node != src for node, _ in fwd_list)
        any_rx_other = any(node != src for node, _ in rx_list)

        if relay_fwd:
            cls["tx_not_delivered_relay_forward_stall"] += 1
        elif any_rx_other:
            cls["tx_not_delivered_rx_no_relay_forward"] += 1
        else:
            cls["tx_not_delivered_no_rx_after_tx"] += 1

    return cls


def run_one(profile, seed):
    run_name = f"{profile}_seed{seed}"
    run_dir = OUTDIR / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    args = {}
    args.update(COMMON_ARGS)
    args.update(PROFILES[profile])
    args["rngRun"] = seed

    cmd = [str(NS3_BIN), "run", "--no-build", f"mesh_dv_baseline {dict_to_args(args)}"]
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=NS3_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    elapsed = time.time() - t0

    log_text = proc.stdout
    (run_dir / "run.log").write_text(log_text)

    to_copy = [
        "mesh_dv_summary.json",
        "mesh_dv_metrics_tx.csv",
        "mesh_dv_metrics_rx.csv",
        "mesh_dv_metrics_routes_used.csv",
    ]
    for fname in to_copy:
        src = NS3_DIR / fname
        if src.exists():
            shutil.copy2(src, run_dir / fname)

    summary = {}
    sfile = run_dir / "mesh_dv_summary.json"
    if sfile.exists():
        summary = json.loads(sfile.read_text())

    app = parse_app_packets(log_text)
    rx = parse_fwd_rx(log_text)
    fwd = parse_fwd_fwd(log_text)
    delivered = parse_deliver(log_text)
    tx_origin = parse_origin_tx_times_from_csv(run_dir / "mesh_dv_metrics_tx.csv", set(app.keys()))
    cls = classify_packets(app, tx_origin, rx, fwd, delivered)

    generated = len(app)
    delivered_n = cls.get("delivered", 0)
    pdr_real = delivered_n / generated if generated > 0 else 0.0

    row = {
        "profile": profile,
        "seed": seed,
        "exit_code": proc.returncode,
        "elapsed_s": round(elapsed, 3),
        "generated": generated,
        "delivered": delivered_n,
        "pdr_real": pdr_real,
        "no_tx_origin": cls.get("no_tx_origin", 0),
        "tx_not_delivered": cls.get("tx_not_delivered", 0),
        "tx_not_delivered_end_window": cls.get("tx_not_delivered_end_window", 0),
        "tx_not_delivered_no_rx_after_tx": cls.get("tx_not_delivered_no_rx_after_tx", 0),
        "tx_not_delivered_rx_no_relay_forward": cls.get("tx_not_delivered_rx_no_relay_forward", 0),
        "tx_not_delivered_relay_forward_stall": cls.get("tx_not_delivered_relay_forward_stall", 0),
        "summary_pdr": float(summary.get("pdr", {}).get("pdr", 0.0)),
        "summary_total_data_tx": int(summary.get("pdr", {}).get("total_data_tx", 0)),
        "summary_delivered": int(summary.get("pdr", {}).get("delivered", 0)),
    }
    return row


def mean_std(vals):
    if not vals:
        return (0.0, 0.0)
    if len(vals) == 1:
        return (float(vals[0]), 0.0)
    return (float(statistics.mean(vals)), float(statistics.stdev(vals)))


def main():
    rows = []
    total = len(PROFILES) * len(SEEDS)
    idx = 0

    for profile in ["A_legacy_timeout", "B_recent_window"]:
        for seed in SEEDS:
            idx += 1
            row = run_one(profile, seed)
            rows.append(row)
            status = "OK" if row["exit_code"] == 0 else f"FAIL({row['exit_code']})"
            print(
                f"[{idx:02d}/{total}] {profile} seed={seed} {status} "
                f"pdr_real={row['pdr_real']:.4f} "
                f"no_rx_after_tx={row['tx_not_delivered_no_rx_after_tx']}"
            )

    raw_csv = OUTDIR / "ab_raw.csv"
    with raw_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    agg_rows = []
    for profile in ["A_legacy_timeout", "B_recent_window"]:
        sub = [r for r in rows if r["profile"] == profile]
        entry = {
            "profile": profile,
            "runs": len(sub),
            "generated_total": sum(r["generated"] for r in sub),
            "delivered_total": sum(r["delivered"] for r in sub),
        }
        for field in [
            "pdr_real",
            "tx_not_delivered_no_rx_after_tx",
            "tx_not_delivered_rx_no_relay_forward",
            "tx_not_delivered_relay_forward_stall",
            "no_tx_origin",
        ]:
            m, s = mean_std([float(r[field]) for r in sub])
            entry[f"{field}_mean"] = m
            entry[f"{field}_std"] = s
        agg_rows.append(entry)

    agg_csv = OUTDIR / "ab_summary.csv"
    with agg_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(agg_rows[0].keys()))
        writer.writeheader()
        writer.writerows(agg_rows)

    md = OUTDIR / "AB_NO_RX_AFTER_TX.md"
    with md.open("w") as f:
        f.write("# A/B `no_rx_after_tx` por ventana SF empírica\n\n")
        f.write(f"- Fecha: {dt.datetime.now().isoformat()}\n")
        f.write("- Escenario: random 1km^2, nEd=32, traffic=medium, stop=300s, dataStart=90s\n")
        f.write("- Control: A = `neighborLinkTimeoutSec=100000` (pegado/legado)\n")
        f.write("- Tratamiento: B = `neighborLinkTimeoutSec=90` (SF reciente)\n")
        f.write("- Seeds: 1,2,3\n\n")
        f.write("| Profile | PDR mean | no_rx_after_tx mean | rx_no_relay mean | relay_stall mean |\n")
        f.write("|---|---:|---:|---:|---:|\n")
        for row in agg_rows:
            f.write(
                f"| {row['profile']} | {row['pdr_real_mean']:.4f} | "
                f"{row['tx_not_delivered_no_rx_after_tx_mean']:.2f} | "
                f"{row['tx_not_delivered_rx_no_relay_forward_mean']:.2f} | "
                f"{row['tx_not_delivered_relay_forward_stall_mean']:.2f} |\n"
            )
        f.write("\n")
        f.write("## Archivos\n")
        f.write(f"- Raw CSV: `{raw_csv}`\n")
        f.write(f"- Summary CSV: `{agg_csv}`\n")
        f.write(f"- Runs: `{OUTDIR}`\n")

    print(f"DONE outdir={OUTDIR}")


if __name__ == "__main__":
    main()

