#!/usr/bin/env python3
import csv
import datetime as dt
import json
import math
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
OUTDIR = THIS_DIR / "validation_results" / f"pdr_deep_dive_{TIMESTAMP}"
OUTDIR.mkdir(parents=True, exist_ok=True)

SEEDS = [1, 3]
BIN_SEC = 600.0
RX_WINDOW_SEC = 15.0
BEACON_RECENCY_SEC = 60.0

COMMON_ARGS = {
    "enablePcap": "false",
    "enableDuty": "true",
    "dutyLimit": 0.01,
    "enableCsma": "true",
    "interferenceModel": "puello",
    "dataStartSec": 90,
    "neighborLinkTimeoutSec": 60,
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

SCENARIOS = [
    {
        "id": "D1_line_n6_low_7200_nodrain",
        "desc": "Control lineal n6 low, larga, sin drenaje",
        "args": {
            "nEd": 6,
            "nodePlacementMode": "line",
            "spacing": 180,
            "trafficLoad": "low",
            "stopSec": 7200,
        },
    },
    {
        "id": "D2_line_n6_low_7200_drain900",
        "desc": "Control lineal n6 low, larga, con drenaje 900s",
        "args": {
            "nEd": 6,
            "nodePlacementMode": "line",
            "spacing": 180,
            "trafficLoad": "low",
            "stopSec": 7200,
            "dataStopSec": 6300,
        },
    },
    {
        "id": "D3_random_n6_low_1km2_7200_nodrain",
        "desc": "Random 1km2 n6 low, larga, sin drenaje",
        "args": {
            "nEd": 6,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "low",
            "stopSec": 7200,
        },
    },
    {
        "id": "D4_random_n6_low_1km2_7200_drain900",
        "desc": "Random 1km2 n6 low, larga, con drenaje 900s",
        "args": {
            "nEd": 6,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "low",
            "stopSec": 7200,
            "dataStopSec": 6300,
        },
    },
    {
        "id": "D5_random_n8_low_1km2_7200_nodrain",
        "desc": "Random 1km2 n8 low, larga, sin drenaje",
        "args": {
            "nEd": 8,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "low",
            "stopSec": 7200,
        },
    },
    {
        "id": "D6_random_n8_low_1km2_7200_drain900",
        "desc": "Random 1km2 n8 low, larga, con drenaje 900s",
        "args": {
            "nEd": 8,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "low",
            "stopSec": 7200,
            "dataStopSec": 6300,
        },
    },
    {
        "id": "D7_random_n8_medium_1km2_3600_nodrain",
        "desc": "Random 1km2 n8 medium, sin drenaje",
        "args": {
            "nEd": 8,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "medium",
            "stopSec": 3600,
        },
    },
    {
        "id": "D8_random_n8_medium_1km2_3600_drain900",
        "desc": "Random 1km2 n8 medium, con drenaje 900s",
        "args": {
            "nEd": 8,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "medium",
            "stopSec": 3600,
            "dataStopSec": 2700,
        },
    },
]


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
    pat = re.compile(
        r"FWDTRACE fwd time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+).*nextHop=(\d+)"
    )
    for m in pat.finditer(log_text):
        t = float(m.group(1))
        node = int(m.group(2))
        src = int(m.group(3))
        dst = int(m.group(4))
        seq = int(m.group(5))
        next_hop = int(m.group(6))
        fwd[(src, dst, seq)].append((node, t, next_hop))
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


def parse_data_tx_detail(log_text):
    attempts = []
    pat = re.compile(
        r"DATA_TX detail: node=(\d+) src=(\d+) dst=(\d+) seq=(\d+) time=([0-9eE+\-.]+)s "
        r"nextHop=(\d+) sf=(\d+)"
    )
    for m in pat.finditer(log_text):
        attempts.append(
            {
                "tx_node": int(m.group(1)),
                "src": int(m.group(2)),
                "dst": int(m.group(3)),
                "seq": int(m.group(4)),
                "time": float(m.group(5)),
                "next_hop": int(m.group(6)),
                "sf": int(m.group(7)),
            }
        )
    return attempts


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


def classify_packets(app, tx_origin, rx, fwd, delivered, stop_sec):
    cls = defaultdict(int)
    labels = {}

    for key, gen_t in app.items():
        src, _, _ = key
        tx_t = tx_origin.get(key)
        deliv = key in delivered
        rx_list = rx.get(key, [])
        fwd_list = fwd.get(key, [])

        if deliv:
            cls["delivered"] += 1
            labels[key] = "delivered"
            continue

        if tx_t is None:
            cls["no_tx_origin"] += 1
            labels[key] = "no_tx_origin"
            continue

        cls["tx_not_delivered"] += 1
        if gen_t >= (stop_sec - 1.0) or tx_t >= (stop_sec - 1.0):
            cls["tx_not_delivered_end_window"] += 1
            labels[key] = "end_window"
            continue

        relay_fwd = any(node != src for node, _, _ in fwd_list)
        any_rx_other = any(node != src for node, _ in rx_list)

        if not any_rx_other:
            cls["first_hop_no_rx"] += 1
            labels[key] = "first_hop_no_rx"
        elif not relay_fwd:
            cls["rx_but_no_relay_forward"] += 1
            labels[key] = "rx_but_no_relay_forward"
        else:
            cls["relay_forward_stall"] += 1
            labels[key] = "relay_forward_stall"

    return cls, labels


def has_recent_beacon(beacon_rx_times, tx_node, next_hop, tx_time, window_sec):
    times = beacon_rx_times.get((tx_node, next_hop), [])
    if not times:
        return False
    lo = tx_time - window_sec
    for t in reversed(times):
        if t < lo:
            return False
        if t <= tx_time:
            return True
    return False


def make_time_bins(stop_sec, bin_sec):
    bins = []
    t = 0.0
    while t < stop_sec:
        bins.append((t, min(t + bin_sec, stop_sec)))
        t += bin_sec
    return bins


def bin_index(t, bins):
    for i, (a, b) in enumerate(bins):
        if a <= t < b:
            return i
    return None


def run_one(scenario, seed):
    scenario_id = scenario["id"]
    run_dir = OUTDIR / scenario_id / f"seed_{seed}"
    run_dir.mkdir(parents=True, exist_ok=True)

    args = dict(COMMON_ARGS)
    args.update(scenario["args"])
    args["rngRun"] = seed
    stop_sec = float(args["stopSec"])

    cmd = [str(NS3_BIN), "run", "--no-build", f"mesh_dv_baseline {dict_to_args(args)}"]
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=NS3_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    elapsed = time.time() - t0
    log_text = proc.stdout
    (run_dir / "run.log").write_text(log_text)

    for fname in ["mesh_dv_summary.json", "mesh_dv_metrics_tx.csv", "mesh_dv_metrics_rx.csv"]:
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
    tx_attempts = parse_data_tx_detail(log_text)

    tx_origin = parse_origin_tx_times_from_csv(run_dir / "mesh_dv_metrics_tx.csv", set(app.keys()))
    cls, labels = classify_packets(app, tx_origin, rx, fwd, delivered, stop_sec)

    beacon_rx_times = defaultdict(list)
    for key, lst in rx.items():
        src, dst, _ = key
        if dst != 65535:
            continue
        for node, t in lst:
            beacon_rx_times[(node, src)].append(t)
    for k in beacon_rx_times:
        beacon_rx_times[k].sort()

    expected_hit = 0
    expected_miss = 0
    miss_no_recent_beacon = 0
    miss_with_recent_beacon = 0

    for a in tx_attempts:
        key = (a["src"], a["dst"], a["seq"])
        tx_t = a["time"]
        next_hop = a["next_hop"]
        tx_node = a["tx_node"]
        rx_list = rx.get(key, [])
        hit = any((n == next_hop and tx_t <= t <= (tx_t + RX_WINDOW_SEC)) for n, t in rx_list)
        if hit:
            expected_hit += 1
        else:
            expected_miss += 1
            recent = has_recent_beacon(beacon_rx_times, tx_node, next_hop, tx_t, BEACON_RECENCY_SEC)
            if recent:
                miss_with_recent_beacon += 1
            else:
                miss_no_recent_beacon += 1

    # Per-node source PDR
    gen_by_src = defaultdict(int)
    deliv_by_src = defaultdict(int)
    for (src, _, _), _ in app.items():
        gen_by_src[src] += 1
    for (src, _, _), _ in delivered.items():
        deliv_by_src[src] += 1
    pdr_src_vals = []
    for src, g in gen_by_src.items():
        if g > 0:
            pdr_src_vals.append(deliv_by_src.get(src, 0) / g)

    bins = make_time_bins(stop_sec, BIN_SEC)
    gen_bin = [0] * len(bins)
    deliv_bin = [0] * len(bins)
    no_tx_bin = [0] * len(bins)
    no_rx_bin = [0] * len(bins)
    for key, t in app.items():
        i = bin_index(t, bins)
        if i is None:
            continue
        gen_bin[i] += 1
        lab = labels.get(key, "")
        if lab == "delivered":
            deliv_bin[i] += 1
        elif lab == "no_tx_origin":
            no_tx_bin[i] += 1
        elif lab == "first_hop_no_rx":
            no_rx_bin[i] += 1

    pdr_first_bin = 0.0
    pdr_last_bin = 0.0
    no_tx_last_bin_ratio = 0.0
    if gen_bin:
        pdr_first_bin = (deliv_bin[0] / gen_bin[0]) if gen_bin[0] > 0 else 0.0
        last_idx = len(gen_bin) - 1
        pdr_last_bin = (deliv_bin[last_idx] / gen_bin[last_idx]) if gen_bin[last_idx] > 0 else 0.0
        no_tx_last_bin_ratio = (no_tx_bin[last_idx] / gen_bin[last_idx]) if gen_bin[last_idx] > 0 else 0.0

    generated = len(app)
    delivered_n = cls.get("delivered", 0)
    pdr_real = delivered_n / generated if generated > 0 else 0.0

    result = {
        "scenario_id": scenario_id,
        "seed": seed,
        "exit_code": proc.returncode,
        "elapsed_s": round(elapsed, 3),
        "generated": generated,
        "delivered": delivered_n,
        "pdr_real": pdr_real,
        "summary_pdr": float(summary.get("pdr", {}).get("pdr", 0.0)),
        "summary_total_data_tx": int(summary.get("pdr", {}).get("total_data_tx", 0)),
        "summary_delivered": int(summary.get("pdr", {}).get("delivered", 0)),
        "no_tx_origin": cls.get("no_tx_origin", 0),
        "first_hop_no_rx": cls.get("first_hop_no_rx", 0),
        "rx_but_no_relay_forward": cls.get("rx_but_no_relay_forward", 0),
        "relay_forward_stall": cls.get("relay_forward_stall", 0),
        "tx_not_delivered_end_window": cls.get("tx_not_delivered_end_window", 0),
        "expected_next_hop_hit": expected_hit,
        "expected_next_hop_miss": expected_miss,
        "miss_no_recent_beacon": miss_no_recent_beacon,
        "miss_with_recent_beacon": miss_with_recent_beacon,
        "pdr_first_bin": pdr_first_bin,
        "pdr_last_bin": pdr_last_bin,
        "no_tx_last_bin_ratio": no_tx_last_bin_ratio,
        "pdr_src_min": min(pdr_src_vals) if pdr_src_vals else 0.0,
        "pdr_src_max": max(pdr_src_vals) if pdr_src_vals else 0.0,
        "pdr_src_mean": statistics.mean(pdr_src_vals) if pdr_src_vals else 0.0,
        "run_dir": str(run_dir),
    }
    return result


def mean_std(values):
    if not values:
        return (0.0, 0.0)
    if len(values) == 1:
        return (float(values[0]), 0.0)
    return (float(statistics.mean(values)), float(statistics.stdev(values)))


def main():
    rows = []
    total = len(SCENARIOS) * len(SEEDS)
    idx = 0
    for sc in SCENARIOS:
        for seed in SEEDS:
            idx += 1
            row = run_one(sc, seed)
            rows.append(row)
            status = "OK" if row["exit_code"] == 0 else f"FAIL({row['exit_code']})"
            print(
                f"[{idx:02d}/{total}] {sc['id']} seed={seed} {status} "
                f"pdr_real={row['pdr_real']:.4f} no_tx={row['no_tx_origin']} "
                f"no_rx={row['first_hop_no_rx']} miss={row['expected_next_hop_miss']}"
            )

    raw_csv = OUTDIR / "deep_raw_runs.csv"
    with raw_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    summary_rows = []
    for sc in SCENARIOS:
        sid = sc["id"]
        sub = [r for r in rows if r["scenario_id"] == sid]
        out = {"scenario_id": sid, "runs": len(sub)}
        for field in [
            "pdr_real",
            "summary_pdr",
            "generated",
            "delivered",
            "summary_total_data_tx",
            "no_tx_origin",
            "first_hop_no_rx",
            "relay_forward_stall",
            "expected_next_hop_miss",
            "miss_no_recent_beacon",
            "miss_with_recent_beacon",
            "pdr_first_bin",
            "pdr_last_bin",
            "no_tx_last_bin_ratio",
            "pdr_src_min",
            "pdr_src_max",
            "pdr_src_mean",
        ]:
            m, s = mean_std([float(r[field]) for r in sub])
            out[f"{field}_mean"] = m
            out[f"{field}_std"] = s
        summary_rows.append(out)

    summary_csv = OUTDIR / "deep_scenario_summary.csv"
    with summary_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        w.writeheader()
        w.writerows(summary_rows)

    report_md = THIS_DIR / "ANALISIS_EXHAUSTIVO_PDR_DEEP_2026-02-16.md"
    with report_md.open("w") as f:
        f.write("# Analisis Exhaustivo PDR Deep Dive (corridas largas, foco causa raiz)\n\n")
        f.write(f"- Fecha: {dt.datetime.now().isoformat()}\n")
        f.write(f"- Seeds: {SEEDS}\n")
        f.write("- Config fija: CSMA+CAD ON, Duty ON (1%), PHY=puello, TxPower=20 dBm, ")
        f.write("NeighborLinkTimeout=60s, dataStart=90s.\n")
        f.write("- Comparativa clave: escenarios sin drenaje vs con drenaje (dataStopSec).\n\n")

        f.write("## Escenarios\n")
        for sc in SCENARIOS:
            f.write(f"- `{sc['id']}`: {sc['desc']} args={sc['args']}\n")
        f.write("\n")

        f.write("## Resumen (media seeds)\n")
        f.write("| Escenario | pdr_real | summary_pdr_json | no_tx_origin | first_hop_no_rx | nextHop_miss | pdr_bin_inicio | pdr_bin_final | no_tx_ratio_bin_final |\n")
        f.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for r in summary_rows:
            f.write(
                f"| {r['scenario_id']} | {r['pdr_real_mean']:.4f} | {r['summary_pdr_mean']:.4f} | "
                f"{r['no_tx_origin_mean']:.2f} | {r['first_hop_no_rx_mean']:.2f} | "
                f"{r['expected_next_hop_miss_mean']:.2f} | {r['pdr_first_bin_mean']:.3f} | "
                f"{r['pdr_last_bin_mean']:.3f} | {r['no_tx_last_bin_ratio_mean']:.3f} |\n"
            )
        f.write("\n")

        f.write("## Lectura tecnica\n")
        f.write("- Si `pdr_bin_inicio` es alto y `pdr_bin_final` cae fuerte junto con `no_tx_ratio_bin_final`, el cuello es backlog MAC/duty (paquetes generados que no alcanzan TX origen a tiempo).\n")
        f.write("- `first_hop_no_rx` y `nextHop_miss` miden perdida en salto real (no backlog).\n")
        f.write("- Diferencia sistematica entre `pdr_real` y `summary_pdr_json` indica que el KPI del JSON no es PDR extremo-a-extremo por paquetes generados.\n")
        f.write("- `pdr_src_min/max` refleja desigualdad por nodo origen (algunos nodos mucho peor que otros).\n\n")

        f.write("## Artefactos\n")
        f.write(f"- Raw por corrida: `{raw_csv}`\n")
        f.write(f"- Resumen por escenario: `{summary_csv}`\n")
        f.write(f"- Carpeta completa: `{OUTDIR}`\n")

    print(f"DONE outdir={OUTDIR}")
    print(f"REPORT {report_md}")


if __name__ == "__main__":
    main()
