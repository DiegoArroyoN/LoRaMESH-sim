#!/usr/bin/env python3
import csv
import datetime as dt
import json
import re
import shutil
import statistics
import subprocess
import time
from collections import Counter, defaultdict
from pathlib import Path


THIS_DIR = Path(__file__).resolve().parent
NS3_DIR = THIS_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"
TS = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
DATE_STR = dt.datetime.now().strftime("%Y-%m-%d")
OUTDIR = THIS_DIR / "validation_results" / f"pdr_post_queue_{TS}"
OUTDIR.mkdir(parents=True, exist_ok=True)

SEEDS = [1, 3, 5]
RX_WINDOW_SEC = 15.0

COMMON_ARGS = {
    "nEd": 8,
    "nodePlacementMode": "random",
    "areaWidth": 1000,
    "areaHeight": 1000,
    "enablePcap": "false",
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
    {"id": "low", "args": {"trafficLoad": "low", "stopSec": 900}},
    {"id": "medium", "args": {"trafficLoad": "medium", "stopSec": 600}},
]

PROFILES = [
    {"id": "csma_on_duty_on", "args": {"enableCsma": "true", "enableDuty": "true", "dutyLimit": 0.01}},
    {"id": "csma_on_duty_off", "args": {"enableCsma": "true", "enableDuty": "false"}},
    {"id": "csma_off_duty_on", "args": {"enableCsma": "false", "enableDuty": "true", "dutyLimit": 0.01}},
    {"id": "csma_off_duty_off", "args": {"enableCsma": "false", "enableDuty": "false"}},
]

PAT_APP = re.compile(r"APP_SEND_DATA src=(\d+) dst=(\d+) seq=(\d+) time=([0-9eE+\-.]+)")
PAT_TX_DETAIL = re.compile(
    r"DATA_TX detail: node=(\d+) src=(\d+) dst=(\d+) seq=(\d+) time=([0-9eE+\-.]+)s nextHop=(\d+) sf=(\d+)"
)
PAT_RX = re.compile(
    r"FWDTRACE rx time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)"
)
PAT_FWD = re.compile(
    r"FWDTRACE fwd time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)"
)
PAT_DELIVER = re.compile(
    r"FWDTRACE deliver time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)"
)
PAT_DROP_SEEN = re.compile(
    r"FWDTRACE drop_seen_once time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)"
)
PAT_BACKTRACK_DROP = re.compile(
    r"FWDTRACE backtrack_drop time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)"
)
PAT_NOROUTE_NO_MAC = re.compile(
    r"FWDTRACE DATA_NOROUTE time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+).*reason=(?:no_link_addr_for_unicast|no_mac_for_unicast)"
)
PAT_NOROUTE = re.compile(
    r"FWDTRACE DATA_NOROUTE time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+).*reason=no_route"
)
PAT_PENDING_END = re.compile(
    r"FWDTRACE ORIGIN_PENDING_END time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+) "
    r"sf=(\d+) reason=([a-zA-Z0-9_]+) deferCount=(\d+) queueSize=(\d+)"
)


def dict_to_args(d):
    return " ".join(f"--{k}={v}" for k, v in d.items())


def _mean(vals):
    return statistics.mean(vals) if vals else 0.0


def parse_log(log_text):
    app = {}
    tx_source = {}
    tx_all = defaultdict(list)
    rx_map = defaultdict(list)
    fwd_map = defaultdict(list)
    delivered = {}
    no_mac_keys = set()
    no_route_keys = set()
    pending_end = {}
    drop_seen = set()
    backtrack = set()

    for m in PAT_APP.finditer(log_text):
        key = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        app[key] = float(m.group(4))

    for m in PAT_TX_DETAIL.finditer(log_text):
        tx_node = int(m.group(1))
        src = int(m.group(2))
        key = (src, int(m.group(3)), int(m.group(4)))
        item = {
            "tx_node": tx_node,
            "time": float(m.group(5)),
            "next_hop": int(m.group(6)),
            "sf": int(m.group(7)),
        }
        tx_all[key].append(item)
        if tx_node == src:
            prev = tx_source.get(key)
            if prev is None or item["time"] < prev["time"]:
                tx_source[key] = item

    for m in PAT_RX.finditer(log_text):
        key = (int(m.group(3)), int(m.group(4)), int(m.group(5)))
        rx_map[key].append((int(m.group(2)), float(m.group(1))))

    for m in PAT_FWD.finditer(log_text):
        key = (int(m.group(3)), int(m.group(4)), int(m.group(5)))
        fwd_map[key].append((int(m.group(2)), float(m.group(1))))

    for m in PAT_DELIVER.finditer(log_text):
        key = (int(m.group(3)), int(m.group(4)), int(m.group(5)))
        delivered[key] = {"node": int(m.group(2)), "time": float(m.group(1))}

    for m in PAT_NOROUTE_NO_MAC.finditer(log_text):
        no_mac_keys.add((int(m.group(3)), int(m.group(4)), int(m.group(5))))

    for m in PAT_NOROUTE.finditer(log_text):
        no_route_keys.add((int(m.group(3)), int(m.group(4)), int(m.group(5))))

    for m in PAT_PENDING_END.finditer(log_text):
        key = (int(m.group(3)), int(m.group(4)), int(m.group(5)))
        pending_end[key] = {
            "reason": m.group(7),
            "deferCount": int(m.group(8)),
            "queueSize": int(m.group(9)),
        }

    for m in PAT_DROP_SEEN.finditer(log_text):
        drop_seen.add((int(m.group(3)), int(m.group(4)), int(m.group(5))))

    for m in PAT_BACKTRACK_DROP.finditer(log_text):
        backtrack.add((int(m.group(3)), int(m.group(4)), int(m.group(5))))

    return {
        "app": app,
        "tx_source": tx_source,
        "tx_all": tx_all,
        "rx_map": rx_map,
        "fwd_map": fwd_map,
        "delivered": delivered,
        "no_mac_keys": no_mac_keys,
        "no_route_keys": no_route_keys,
        "pending_end": pending_end,
        "drop_seen": drop_seen,
        "backtrack": backtrack,
    }


def classify_packets(parsed, stop_sec):
    app = parsed["app"]
    tx_source = parsed["tx_source"]
    rx_map = parsed["rx_map"]
    fwd_map = parsed["fwd_map"]
    delivered = parsed["delivered"]
    no_mac_keys = parsed["no_mac_keys"]
    no_route_keys = parsed["no_route_keys"]
    pending_end = parsed["pending_end"]
    drop_seen = parsed["drop_seen"]
    backtrack = parsed["backtrack"]

    cls = Counter()
    details = []
    stop_edge = stop_sec - 1.0

    for key, gen_t in app.items():
        src, dst, seq = key

        if key in delivered:
            cls["delivered"] += 1
            details.append((src, dst, seq, "delivered"))
            continue

        src_tx = tx_source.get(key)
        if src_tx is None:
            if key in pending_end:
                reason = pending_end[key]["reason"]
                label = f"no_tx_origin_pending_{reason}"
            elif key in no_mac_keys:
                label = "no_tx_origin_no_link_addr_for_unicast"
            elif key in no_route_keys:
                label = "no_tx_origin_no_route"
            else:
                label = "no_tx_origin_other"
            cls[label] += 1
            cls["no_tx_origin_total"] += 1
            details.append((src, dst, seq, label))
            continue

        tx_t = src_tx["time"]
        if gen_t >= stop_edge or tx_t >= stop_edge:
            cls["tx_no_deliver_end_window"] += 1
            details.append((src, dst, seq, "tx_no_deliver_end_window"))
            continue

        next_hop = src_tx["next_hop"]
        first_hop_hit = any(
            node == next_hop and tx_t <= rt <= (tx_t + RX_WINDOW_SEC)
            for node, rt in rx_map.get(key, [])
        )
        if not first_hop_hit:
            cls["first_hop_no_rx"] += 1
            details.append((src, dst, seq, "first_hop_no_rx"))
            continue

        relay_rx = [node for node, _ in rx_map.get(key, []) if node not in (src, dst)]
        relay_fwd = [node for node, _ in fwd_map.get(key, []) if node not in (src, dst)]
        if relay_rx and not relay_fwd:
            cls["relay_stall"] += 1
            details.append((src, dst, seq, "relay_stall"))
            continue

        if key in drop_seen:
            cls["rx_but_no_relay_seen"] += 1
            details.append((src, dst, seq, "rx_but_no_relay_seen"))
            continue

        if key in backtrack:
            cls["rx_but_no_relay_backtrack"] += 1
            details.append((src, dst, seq, "rx_but_no_relay_backtrack"))
            continue

        cls["downstream_loss_after_first_hop"] += 1
        details.append((src, dst, seq, "downstream_loss_after_first_hop"))

    return cls, details


def run_one(scenario, profile, seed):
    args = dict(COMMON_ARGS)
    args.update(scenario["args"])
    args.update(profile["args"])
    args["rngRun"] = seed
    stop_sec = float(args["stopSec"])
    drain_sec = 120.0
    args["dataStopSec"] = max(0.0, stop_sec - drain_sec)
    args["pdrEndWindowSec"] = drain_sec

    run_dir = OUTDIR / scenario["id"] / profile["id"] / f"seed_{seed}"
    run_dir.mkdir(parents=True, exist_ok=True)

    cmd = [str(NS3_BIN), "run", "--no-build", f"mesh_dv_baseline {dict_to_args(args)}"]
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=NS3_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    elapsed = time.time() - t0
    (run_dir / "run.log").write_text(proc.stdout)

    summary = {}
    summary_path = NS3_DIR / "mesh_dv_summary.json"
    if proc.returncode == 0 and summary_path.exists():
        shutil.copy2(summary_path, run_dir / "mesh_dv_summary.json")
        summary = json.loads((run_dir / "mesh_dv_summary.json").read_text())

    for suffix in ["tx", "rx", "routes", "routes_used", "delay", "overhead", "duty", "energy"]:
        src = NS3_DIR / f"mesh_dv_metrics_{suffix}.csv"
        if src.exists():
            shutil.copy2(src, run_dir / src.name)

    parsed = parse_log(proc.stdout)
    cls, details = classify_packets(parsed, stop_sec)
    details_path = run_dir / "packet_classification.csv"
    with details_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["src", "dst", "seq", "label"])
        for row in details:
            w.writerow(row)

    pdr = summary.get("pdr", {})
    pdr_by_src = summary.get("pdr_by_source", [])
    src_pdr_values = [float(x.get("pdr", 0.0)) for x in pdr_by_src]

    row = {
        "scenario": scenario["id"],
        "profile": profile["id"],
        "seed": seed,
        "exit_code": proc.returncode,
        "elapsed_s": round(elapsed, 3),
        "generated_total": int(pdr.get("total_data_generated", 0)),
        "generated_eligible": int(pdr.get("total_data_generated_eligible", pdr.get("total_data_generated", 0))),
        "delivered_total": int(pdr.get("delivered", 0)),
        "delivered_eligible": int(pdr.get("delivered_eligible", pdr.get("delivered", 0))),
        "pdr_total": float(pdr.get("pdr", 0.0)),
        "pdr_eligible": float(pdr.get("pdr_e2e_generated_eligible", pdr.get("pdr", 0.0))),
        "end_window_generated": int(pdr.get("end_window_generated", 0)),
        "pdr_src_min": min(src_pdr_values) if src_pdr_values else 0.0,
        "pdr_src_mean": _mean(src_pdr_values),
        "pdr_src_max": max(src_pdr_values) if src_pdr_values else 0.0,
        "no_tx_origin_total": int(cls.get("no_tx_origin_total", 0)),
        "no_tx_origin_pending_cad_busy_backoff": int(cls.get("no_tx_origin_pending_cad_busy_backoff", 0)),
        "no_tx_origin_pending_duty_wait_queue": int(cls.get("no_tx_origin_pending_duty_wait_queue", 0)),
        "no_tx_origin_pending_phy_tx_busy_queue": int(cls.get("no_tx_origin_pending_phy_tx_busy_queue", 0)),
        "no_tx_origin_pending_inflight_pending_end": int(cls.get("no_tx_origin_pending_inflight_pending_end", 0)),
        "no_tx_origin_no_link_addr_for_unicast": int(cls.get("no_tx_origin_no_link_addr_for_unicast", 0)),
        "no_tx_origin_no_mac_for_unicast": int(cls.get("no_tx_origin_no_link_addr_for_unicast", 0)),
        "no_tx_origin_no_route": int(cls.get("no_tx_origin_no_route", 0)),
        "no_tx_origin_other": int(cls.get("no_tx_origin_other", 0)),
        "first_hop_no_rx": int(cls.get("first_hop_no_rx", 0)),
        "relay_stall": int(cls.get("relay_stall", 0)),
        "rx_but_no_relay_seen": int(cls.get("rx_but_no_relay_seen", 0)),
        "rx_but_no_relay_backtrack": int(cls.get("rx_but_no_relay_backtrack", 0)),
        "downstream_loss_after_first_hop": int(cls.get("downstream_loss_after_first_hop", 0)),
        "tx_no_deliver_end_window": int(cls.get("tx_no_deliver_end_window", 0)),
        "run_dir": str(run_dir),
    }
    return row


def main():
    rows = []
    total = len(SCENARIOS) * len(PROFILES) * len(SEEDS)
    idx = 0
    for sc in SCENARIOS:
        for pf in PROFILES:
            for seed in SEEDS:
                idx += 1
                row = run_one(sc, pf, seed)
                rows.append(row)
                status = "OK" if row["exit_code"] == 0 else f"FAIL({row['exit_code']})"
                print(
                    f"[{idx:02d}/{total}] {sc['id']} {pf['id']} seed={seed} {status} "
                    f"pdr_total={row['pdr_total']:.4f} pdr_eligible={row['pdr_eligible']:.4f} "
                    f"no_tx={row['no_tx_origin_total']} firstHopNoRx={row['first_hop_no_rx']}"
                )

    raw_csv = OUTDIR / "pdr_post_queue_raw.csv"
    with raw_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    grouped = defaultdict(list)
    for r in rows:
        grouped[(r["scenario"], r["profile"])].append(r)

    summary_rows = []
    for (scenario, profile), items in sorted(grouped.items()):
        out = {"scenario": scenario, "profile": profile, "runs": len(items)}
        keys = [
            "pdr_total",
            "pdr_eligible",
            "generated_total",
            "generated_eligible",
            "delivered_total",
            "delivered_eligible",
            "end_window_generated",
            "pdr_src_min",
            "pdr_src_mean",
            "pdr_src_max",
            "no_tx_origin_total",
            "no_tx_origin_pending_cad_busy_backoff",
            "no_tx_origin_pending_duty_wait_queue",
            "no_tx_origin_pending_phy_tx_busy_queue",
            "no_tx_origin_pending_inflight_pending_end",
            "no_tx_origin_no_link_addr_for_unicast",
            "no_tx_origin_no_route",
            "no_tx_origin_other",
            "first_hop_no_rx",
            "relay_stall",
            "rx_but_no_relay_seen",
            "rx_but_no_relay_backtrack",
            "downstream_loss_after_first_hop",
            "tx_no_deliver_end_window",
        ]
        for k in keys:
            out[k] = _mean([float(x[k]) for x in items])
        summary_rows.append(out)

    summary_csv = OUTDIR / "pdr_post_queue_summary.csv"
    with summary_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        w.writeheader()
        w.writerows(summary_rows)

    report = THIS_DIR / f"REPORTE_PDR_POST_COLAS_{DATE_STR}.md"
    with report.open("w") as f:
        f.write("# Reporte PDR Post-Fix de Colas (CSMA/Backoff/Duty)\n\n")
        f.write(f"- Fecha: {dt.datetime.now().isoformat()}\n")
        f.write(f"- Seeds: {SEEDS}\n")
        f.write("- Topología: random 1km2, nEd=8\n")
        f.write("- Cargas: low y medium\n")
        f.write("- Perfiles: csma on/off x duty on/off\n")
        f.write("- Drenaje: dataStopSec=stopSec-120, pdrEndWindowSec=120\n\n")

        f.write("## Resumen por Escenario\n")
        f.write(
            "| Escenario | Perfil | PDR total | PDR elegible | no_tx_origin | "
            "first_hop_no_rx | relay_stall | downstream_loss |\n"
        )
        f.write("|---|---|---:|---:|---:|---:|---:|---:|\n")
        for r in summary_rows:
            f.write(
                f"| {r['scenario']} | {r['profile']} | {r['pdr_total']:.4f} | {r['pdr_eligible']:.4f} | "
                f"{r['no_tx_origin_total']:.2f} | {r['first_hop_no_rx']:.2f} | "
                f"{r['relay_stall']:.2f} | {r['downstream_loss_after_first_hop']:.2f} |\n"
            )

        f.write("\n## Desglose de `no_tx_origin`\n")
        f.write(
            "| Escenario | Perfil | cad_busy_backoff | duty_wait_queue | phy_tx_busy_queue | "
            "inflight_pending_end | no_link_addr_for_unicast | no_route | other |\n"
        )
        f.write("|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for r in summary_rows:
            f.write(
                f"| {r['scenario']} | {r['profile']} | "
                f"{r['no_tx_origin_pending_cad_busy_backoff']:.2f} | "
                f"{r['no_tx_origin_pending_duty_wait_queue']:.2f} | "
                f"{r['no_tx_origin_pending_phy_tx_busy_queue']:.2f} | "
                f"{r['no_tx_origin_pending_inflight_pending_end']:.2f} | "
                f"{r['no_tx_origin_no_link_addr_for_unicast']:.2f} | "
                f"{r['no_tx_origin_no_route']:.2f} | "
                f"{r['no_tx_origin_other']:.2f} |\n"
            )

        f.write("\n## Interpretación rápida\n")
        f.write("- `no_tx_origin` ahora refleja backlog real en cola cuando termina la simulación y bloqueos locales por canal/duty.\n")
        f.write("- `first_hop_no_rx` representa pérdidas de primer salto (el origen sí transmitió, pero el next-hop esperado no recibió a tiempo).\n")
        f.write("- `downstream_loss_after_first_hop` representa pérdidas posteriores al primer salto (forwarding/canal en hops intermedios).\n")

        f.write("\n## Artefactos\n")
        f.write(f"- Raw CSV: `{raw_csv}`\n")
        f.write(f"- Summary CSV: `{summary_csv}`\n")
        f.write(f"- Carpeta corridas: `{OUTDIR}`\n")

    print(f"DONE outdir={OUTDIR}")
    print(f"REPORT {report}")


if __name__ == "__main__":
    main()
