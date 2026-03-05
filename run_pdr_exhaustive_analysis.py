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
OUTDIR = THIS_DIR / "validation_results" / f"pdr_exhaustive_{TIMESTAMP}"
OUTDIR.mkdir(parents=True, exist_ok=True)

SEEDS = [1, 3, 5]
BIN_SEC = 300.0
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
    # H3-like tuning already validated in prior runs
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
        "id": "S1_line_n6_low",
        "desc": "Control lineal, pocos nodos, baja carga",
        "args": {
            "nEd": 6,
            "nodePlacementMode": "line",
            "spacing": 180,
            "trafficLoad": "low",
            "stopSec": 3600,
        },
    },
    {
        "id": "S2_random_n6_low_1km2",
        "desc": "Random 1km2, n6, baja carga",
        "args": {
            "nEd": 6,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "low",
            "stopSec": 3600,
        },
    },
    {
        "id": "S3_random_n8_low_1km2",
        "desc": "Random 1km2, n8, baja carga",
        "args": {
            "nEd": 8,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "low",
            "stopSec": 3600,
        },
    },
    {
        "id": "S4_random_n8_low_500m",
        "desc": "Random 500x500m, n8, baja carga (densidad mayor)",
        "args": {
            "nEd": 8,
            "nodePlacementMode": "random",
            "areaWidth": 500,
            "areaHeight": 500,
            "trafficLoad": "low",
            "stopSec": 3600,
        },
    },
    {
        "id": "S5_random_n8_medium_1km2",
        "desc": "Random 1km2, n8, carga media (comparativo)",
        "args": {
            "nEd": 8,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "medium",
            "stopSec": 1800,
        },
    },
    {
        "id": "S6_random_n12_low_1km2",
        "desc": "Random 1km2, n12, baja carga (escalado leve)",
        "args": {
            "nEd": 12,
            "nodePlacementMode": "random",
            "areaWidth": 1000,
            "areaHeight": 1000,
            "trafficLoad": "low",
            "stopSec": 3600,
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
    # key=(src,dst,seq) -> list[(node,time)]
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
    # key=(src,dst,seq) -> list[(node,time,nextHop)]
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


def parse_drop_reasons(log_text):
    # key=(src,dst,seq) -> set(reason)
    reasons = defaultdict(set)
    for m in re.finditer(
        r"FWDTRACE [^\n]* src=(\d+) dst=(\d+) seq=(\d+)[^\n]*reason=([A-Za-z0-9_]+)",
        log_text,
    ):
        src = int(m.group(1))
        dst = int(m.group(2))
        seq = int(m.group(3))
        reason = m.group(4)
        reasons[(src, dst, seq)].add(reason)
    return reasons


def parse_data_tx_detail(log_text):
    # list of dict attempts
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
    # first successful tx at origin for each (src,dst,seq)
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
    run_name = f"{scenario_id}_seed{seed}"
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
    reasons = parse_drop_reasons(log_text)
    tx_attempts = parse_data_tx_detail(log_text)

    tx_origin = parse_origin_tx_times_from_csv(run_dir / "mesh_dv_metrics_tx.csv", set(app.keys()))
    cls, labels = classify_packets(app, tx_origin, rx, fwd, delivered, stop_sec)

    # Beacons observed by node from src (dst=65535 in FWDTRACE rx)
    beacon_rx_times = defaultdict(list)  # key=(node, beacon_src) -> [times]
    for key, lst in rx.items():
        src, dst, _ = key
        if dst != 65535:
            continue
        for node, t in lst:
            beacon_rx_times[(node, src)].append(t)
    for k in beacon_rx_times:
        beacon_rx_times[k].sort()

    # Tx-attempt level next-hop hit/miss
    expected_hit = 0
    expected_miss = 0
    expected_miss_others_heard = 0
    miss_no_recent_beacon = 0
    miss_with_recent_beacon = 0
    tx_attempt_origin = 0
    tx_attempt_relay = 0
    tx_attempt_origin_miss = 0
    tx_attempt_relay_miss = 0
    sf_hist = defaultdict(int)

    for a in tx_attempts:
        key = (a["src"], a["dst"], a["seq"])
        tx_node = a["tx_node"]
        tx_t = a["time"]
        next_hop = a["next_hop"]
        sf_hist[a["sf"]] += 1

        if tx_node == a["src"]:
            tx_attempt_origin += 1
        else:
            tx_attempt_relay += 1

        rx_list = rx.get(key, [])
        hit = any((n == next_hop and tx_t <= t <= (tx_t + RX_WINDOW_SEC)) for n, t in rx_list)
        others_heard = any((n != next_hop and n != tx_node and tx_t <= t <= (tx_t + RX_WINDOW_SEC))
                           for n, t in rx_list)

        if hit:
            expected_hit += 1
        else:
            expected_miss += 1
            if others_heard:
                expected_miss_others_heard += 1
            if tx_node == a["src"]:
                tx_attempt_origin_miss += 1
            else:
                tx_attempt_relay_miss += 1

            recent = has_recent_beacon(beacon_rx_times, tx_node, next_hop, tx_t, BEACON_RECENCY_SEC)
            if recent:
                miss_with_recent_beacon += 1
            else:
                miss_no_recent_beacon += 1

    # Per-node metrics
    gen_by_src = defaultdict(int)
    for (src, _, _), _ in app.items():
        gen_by_src[src] += 1

    deliv_by_src = defaultdict(int)
    deliv_by_dst = defaultdict(int)
    for (src, dst, _), _ in delivered.items():
        deliv_by_src[src] += 1
        deliv_by_dst[dst] += 1

    fwd_by_node = defaultdict(int)
    for _, lst in fwd.items():
        for node, _, _ in lst:
            fwd_by_node[node] += 1

    per_node_file = run_dir / "per_node.csv"
    node_ids = sorted(set(gen_by_src) | set(deliv_by_src) | set(deliv_by_dst) | set(fwd_by_node))
    with per_node_file.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["node", "generated_src", "delivered_src", "pdr_src", "delivered_as_dst", "forwarded"])
        for n in node_ids:
            g = gen_by_src.get(n, 0)
            dsrc = deliv_by_src.get(n, 0)
            pdr_src = (dsrc / g) if g > 0 else 0.0
            ddst = deliv_by_dst.get(n, 0)
            fw = fwd_by_node.get(n, 0)
            w.writerow([n, g, dsrc, f"{pdr_src:.6f}", ddst, fw])

    # PDR progression by generation-time bins
    bins = make_time_bins(stop_sec, BIN_SEC)
    gen_bin = [0] * len(bins)
    deliv_bin = [0] * len(bins)
    no_tx_bin = [0] * len(bins)
    no_rx_bin = [0] * len(bins)
    relay_stall_bin = [0] * len(bins)

    for key, t in app.items():
        bi = bin_index(t, bins)
        if bi is None:
            continue
        gen_bin[bi] += 1
        label = labels.get(key, "")
        if label == "delivered":
            deliv_bin[bi] += 1
        elif label == "no_tx_origin":
            no_tx_bin[bi] += 1
        elif label == "first_hop_no_rx":
            no_rx_bin[bi] += 1
        elif label == "relay_forward_stall":
            relay_stall_bin[bi] += 1

    progression_file = run_dir / "pdr_progression.csv"
    with progression_file.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "bin_start_s",
                "bin_end_s",
                "generated",
                "delivered",
                "pdr_gen_bin",
                "no_tx_origin",
                "first_hop_no_rx",
                "relay_forward_stall",
            ]
        )
        for i, (a, b) in enumerate(bins):
            g = gen_bin[i]
            d = deliv_bin[i]
            p = (d / g) if g > 0 else 0.0
            w.writerow([f"{a:.1f}", f"{b:.1f}", g, d, f"{p:.6f}", no_tx_bin[i], no_rx_bin[i], relay_stall_bin[i]])

    # Save top failed packet traces
    failed_keys = []
    for key, label in labels.items():
        if label in ("first_hop_no_rx", "rx_but_no_relay_forward", "relay_forward_stall", "no_tx_origin"):
            failed_keys.append(key)
    failed_keys = sorted(failed_keys, key=lambda k: app.get(k, math.inf))[:40]

    packet_trace_file = run_dir / "failed_packet_samples.csv"
    with packet_trace_file.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "src",
                "dst",
                "seq",
                "gen_time",
                "label",
                "origin_tx_time",
                "rx_nodes_times",
                "fwd_nodes_times_nextHop",
                "reasons",
            ]
        )
        for key in failed_keys:
            src, dst, seq = key
            gen_t = app.get(key, -1)
            tx_t = tx_origin.get(key, -1)
            rx_str = ";".join(f"{n}@{t:.1f}" for n, t in sorted(rx.get(key, []), key=lambda x: x[1])[:20])
            fwd_str = ";".join(
                f"{n}@{t:.1f}->nh{nh}" for n, t, nh in sorted(fwd.get(key, []), key=lambda x: x[1])[:20]
            )
            rs = ",".join(sorted(reasons.get(key, set())))
            w.writerow([src, dst, seq, f"{gen_t:.3f}", labels.get(key, ""), f"{tx_t:.3f}", rx_str, fwd_str, rs])

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
        "no_tx_origin": cls.get("no_tx_origin", 0),
        "first_hop_no_rx": cls.get("first_hop_no_rx", 0),
        "rx_but_no_relay_forward": cls.get("rx_but_no_relay_forward", 0),
        "relay_forward_stall": cls.get("relay_forward_stall", 0),
        "tx_not_delivered_end_window": cls.get("tx_not_delivered_end_window", 0),
        "tx_attempts_total": len(tx_attempts),
        "tx_attempt_origin": tx_attempt_origin,
        "tx_attempt_relay": tx_attempt_relay,
        "expected_next_hop_hit": expected_hit,
        "expected_next_hop_miss": expected_miss,
        "expected_miss_others_heard": expected_miss_others_heard,
        "miss_no_recent_beacon": miss_no_recent_beacon,
        "miss_with_recent_beacon": miss_with_recent_beacon,
        "origin_miss": tx_attempt_origin_miss,
        "relay_miss": tx_attempt_relay_miss,
        "sf7_tx": sf_hist.get(7, 0),
        "sf8_tx": sf_hist.get(8, 0),
        "sf9_tx": sf_hist.get(9, 0),
        "sf10_tx": sf_hist.get(10, 0),
        "sf11_tx": sf_hist.get(11, 0),
        "sf12_tx": sf_hist.get(12, 0),
        "summary_pdr": float(summary.get("pdr", {}).get("pdr", 0.0)),
        "summary_total_data_tx": int(summary.get("pdr", {}).get("total_data_tx", 0)),
        "summary_delivered": int(summary.get("pdr", {}).get("delivered", 0)),
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
                f"pdr={row['pdr_real']:.4f} no_tx={row['no_tx_origin']} "
                f"firstHopNoRx={row['first_hop_no_rx']} relayStall={row['relay_forward_stall']}"
            )

    raw_csv = OUTDIR / "exhaustive_raw_runs.csv"
    with raw_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    summary_rows = []
    for sc in SCENARIOS:
        sid = sc["id"]
        sub = [r for r in rows if r["scenario_id"] == sid]
        out = {
            "scenario_id": sid,
            "runs": len(sub),
            "generated_total": sum(r["generated"] for r in sub),
            "delivered_total": sum(r["delivered"] for r in sub),
        }
        for field in [
            "pdr_real",
            "no_tx_origin",
            "first_hop_no_rx",
            "rx_but_no_relay_forward",
            "relay_forward_stall",
            "expected_next_hop_miss",
            "expected_miss_others_heard",
            "miss_no_recent_beacon",
            "miss_with_recent_beacon",
            "origin_miss",
            "relay_miss",
        ]:
            m, s = mean_std([float(r[field]) for r in sub])
            out[f"{field}_mean"] = m
            out[f"{field}_std"] = s
        summary_rows.append(out)

    summary_csv = OUTDIR / "exhaustive_scenario_summary.csv"
    with summary_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        w.writeheader()
        w.writerows(summary_rows)

    # Build markdown report
    report_md = THIS_DIR / "ANALISIS_EXHAUSTIVO_PDR_2026-02-16.md"
    with report_md.open("w") as f:
        f.write("# Analisis Exhaustivo PDR (pocos nodos, baja carga, corridas largas)\n\n")
        f.write(f"- Fecha: {dt.datetime.now().isoformat()}\n")
        f.write("- Configuracion fija: CSMA+CAD ON, Duty ON (1%), PHY=puello, TxPower=20 dBm, ")
        f.write("NeighborLinkTimeout=60s, dataStart=90s.\n")
        f.write(f"- Seeds: {SEEDS}\n")
        f.write(f"- Bins temporales para progresion: {int(BIN_SEC)} s\n\n")

        f.write("## Escenarios\n")
        for sc in SCENARIOS:
            f.write(f"- `{sc['id']}`: {sc['desc']} args={sc['args']}\n")
        f.write("\n")

        f.write("## Resumen por escenario (media de seeds)\n")
        f.write(
            "| Escenario | PDR | no_tx_origin | first_hop_no_rx | relay_forward_stall | "
            "nextHop_miss | miss_sin_beacon_reciente | miss_con_beacon_reciente |\n"
        )
        f.write("|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for r in summary_rows:
            f.write(
                f"| {r['scenario_id']} | {r['pdr_real_mean']:.4f} | {r['no_tx_origin_mean']:.2f} | "
                f"{r['first_hop_no_rx_mean']:.2f} | {r['relay_forward_stall_mean']:.2f} | "
                f"{r['expected_next_hop_miss_mean']:.2f} | {r['miss_no_recent_beacon_mean']:.2f} | "
                f"{r['miss_with_recent_beacon_mean']:.2f} |\n"
            )
        f.write("\n")

        # Scenario-level quick diagnosis
        f.write("## Patrones observados\n")
        f.write(
            "- `no_tx_origin` alto indica que una fraccion relevante del trafico generado no sale del nodo origen "
            "(contencion CAD/backoff + duty + fin de simulacion).\n"
        )
        f.write(
            "- `first_hop_no_rx` y `nextHop_miss` capturan perdidas en primer salto: el origen/relevo transmite, "
            "pero el `nextHop` esperado no recibe en ventana.\n"
        )
        f.write(
            "- `relay_forward_stall` indica paquetes que alcanzan relay(es) pero no llegan al destino final.\n"
        )
        f.write(
            "- `miss_no_recent_beacon` vs `miss_with_recent_beacon` separa decisiones de next-hop sin evidencia "
            "reciente de enlace vs fallas pese a beacon reciente.\n\n"
        )

        f.write("## Hallazgos de implementacion relevantes\n")
        f.write(
            "- El trafico originado usa SF empirico por enlace (`GetDataSfForNeighbor`), pero en forwarding "
            "intermedio se usa `sfForRoute = route->sf` sin aplicar SF empirico local del relay. "
            "Esto puede degradar saltos intermedios en multi-hop.\n"
        )
        f.write("- Referencia de codigo:\n")
        f.write("  - origen: `mesh_dv_app.cc` (SendData) usa `GetDataSfForNeighbor(route->nextHop)`\n")
        f.write("  - relay: `mesh_dv_app.cc` (ForwardWithTtl) usa `route->sf` para `sfForRoute`\n\n")

        f.write("## Artefactos\n")
        f.write(f"- Raw por corrida: `{raw_csv}`\n")
        f.write(f"- Resumen por escenario: `{summary_csv}`\n")
        f.write(f"- Carpeta completa: `{OUTDIR}`\n")
        f.write("\n")

    print(f"DONE outdir={OUTDIR}")
    print(f"REPORT {report_md}")


if __name__ == "__main__":
    main()

