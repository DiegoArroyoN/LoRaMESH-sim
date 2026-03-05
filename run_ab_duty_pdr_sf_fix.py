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
TS = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
OUTDIR = THIS_DIR / "validation_results" / f"ab_duty_pdr_sf_fix_{TS}"
OUTDIR.mkdir(parents=True, exist_ok=True)

SEEDS = [1, 3, 5]

COMMON_ARGS = {
    "nEd": 8,
    "nodePlacementMode": "random",
    "areaWidth": 1000,
    "areaHeight": 1000,
    "enablePcap": "false",
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
    {"id": "S_low", "args": {"trafficLoad": "low", "stopSec": 3600}},
    {"id": "S_medium", "args": {"trafficLoad": "medium", "stopSec": 1800}},
]

PROFILES = [
    {"id": "duty_on", "args": {"enableDuty": "true", "dutyLimit": 0.01}},
    {"id": "duty_off", "args": {"enableDuty": "false"}},
]

RX_WINDOW_SEC = 15.0


def dict_to_args(d):
    return " ".join(f"--{k}={v}" for k, v in d.items())


def parse_attempts_and_rx(log_text):
    attempts = []
    pat_tx = re.compile(
        r"DATA_TX detail: node=(\d+) src=(\d+) dst=(\d+) seq=(\d+) time=([0-9eE+\-.]+)s "
        r"nextHop=(\d+) sf=(\d+)"
    )
    for m in pat_tx.finditer(log_text):
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

    rx = defaultdict(list)
    pat_rx = re.compile(r"FWDTRACE rx time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)")
    for m in pat_rx.finditer(log_text):
        t = float(m.group(1))
        node = int(m.group(2))
        src = int(m.group(3))
        dst = int(m.group(4))
        seq = int(m.group(5))
        rx[(src, dst, seq)].append((node, t))

    return attempts, rx


def compute_next_hop_miss(attempts, rx_map):
    total = 0
    miss = 0
    origin_total = 0
    origin_miss = 0
    relay_total = 0
    relay_miss = 0
    for a in attempts:
        total += 1
        is_origin = (a["tx_node"] == a["src"])
        if is_origin:
            origin_total += 1
        else:
            relay_total += 1

        key = (a["src"], a["dst"], a["seq"])
        tx_t = a["time"]
        nh = a["next_hop"]
        hit = any((node == nh and tx_t <= t <= (tx_t + RX_WINDOW_SEC)) for node, t in rx_map.get(key, []))
        if not hit:
            miss += 1
            if is_origin:
                origin_miss += 1
            else:
                relay_miss += 1

    return {
        "next_hop_attempts": total,
        "next_hop_miss": miss,
        "next_hop_miss_rate": (miss / total) if total > 0 else 0.0,
        "origin_attempts": origin_total,
        "origin_miss": origin_miss,
        "origin_miss_rate": (origin_miss / origin_total) if origin_total > 0 else 0.0,
        "relay_attempts": relay_total,
        "relay_miss": relay_miss,
        "relay_miss_rate": (relay_miss / relay_total) if relay_total > 0 else 0.0,
    }


def run_one(scenario, profile, seed):
    args = dict(COMMON_ARGS)
    args.update(scenario["args"])
    args.update(profile["args"])
    args["rngRun"] = seed
    run_id = f"{scenario['id']}__{profile['id']}__seed{seed}"
    run_dir = OUTDIR / scenario["id"] / profile["id"] / f"seed_{seed}"
    run_dir.mkdir(parents=True, exist_ok=True)

    cmd = [str(NS3_BIN), "run", "--no-build", f"mesh_dv_baseline {dict_to_args(args)}"]
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=NS3_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    elapsed = time.time() - t0
    log_text = proc.stdout
    (run_dir / "run.log").write_text(log_text)

    summary = {}
    src_summary = NS3_DIR / "mesh_dv_summary.json"
    if proc.returncode == 0 and src_summary.exists():
        shutil.copy2(src_summary, run_dir / "mesh_dv_summary.json")
        summary = json.loads((run_dir / "mesh_dv_summary.json").read_text())

    attempts, rx_map = parse_attempts_and_rx(log_text)
    miss = compute_next_hop_miss(attempts, rx_map)

    pdr = summary.get("pdr", {})
    pdr_by_src = summary.get("pdr_by_source", [])
    src_pdr_values = [float(x.get("pdr", 0.0)) for x in pdr_by_src]

    row = {
        "scenario": scenario["id"],
        "profile": profile["id"],
        "seed": seed,
        "exit_code": proc.returncode,
        "elapsed_s": round(elapsed, 3),
        "pdr_e2e_global": float(pdr.get("pdr", 0.0)),
        "generated_e2e": int(pdr.get("total_data_generated", 0)),
        "delivered_e2e": int(pdr.get("delivered", 0)),
        "legacy_total_data_tx_attempts": int(pdr.get("legacy_total_data_tx_attempts", 0)),
        "legacy_pdr_tx_based": float(pdr.get("legacy_pdr_tx_based", 0.0)),
        "pdr_src_min": min(src_pdr_values) if src_pdr_values else 0.0,
        "pdr_src_mean": statistics.mean(src_pdr_values) if src_pdr_values else 0.0,
        "pdr_src_max": max(src_pdr_values) if src_pdr_values else 0.0,
        **miss,
        "run_dir": str(run_dir),
    }
    return row


def mean(v):
    return statistics.mean(v) if v else 0.0


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
                    f"pdr={row['pdr_e2e_global']:.4f} miss={row['next_hop_miss_rate']:.3f}"
                )

    raw_csv = OUTDIR / "ab_raw.csv"
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
        for key in [
            "pdr_e2e_global",
            "generated_e2e",
            "delivered_e2e",
            "pdr_src_min",
            "pdr_src_mean",
            "pdr_src_max",
            "next_hop_miss_rate",
            "origin_miss_rate",
            "relay_miss_rate",
            "next_hop_attempts",
        ]:
            out[key] = mean([float(x[key]) for x in items])
        summary_rows.append(out)

    summary_csv = OUTDIR / "ab_summary.csv"
    with summary_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        w.writeheader()
        w.writerows(summary_rows)

    report = THIS_DIR / "REPORTE_AB_DUTY_PDR_SF_FIX_2026-02-16.md"
    with report.open("w") as f:
        f.write("# Reporte A/B Duty Cycle y SF Forward (PDR E2E)\n\n")
        f.write(f"- Fecha: {dt.datetime.now().isoformat()}\n")
        f.write(f"- Seeds: {SEEDS}\n")
        f.write("- Escenarios: random 1km2, nEd=8, low y medium.\n")
        f.write("- Código ya incluye: PDR E2E corregido + SF empírico en forwarding.\n\n")

        f.write("## Resumen\n")
        f.write("| Escenario | Perfil | PDR E2E global | PDR src min/mean/max | nextHop miss total | miss origen | miss relay |\n")
        f.write("|---|---|---:|---:|---:|---:|---:|\n")
        for r in summary_rows:
            f.write(
                f"| {r['scenario']} | {r['profile']} | {r['pdr_e2e_global']:.4f} | "
                f"{r['pdr_src_min']:.4f}/{r['pdr_src_mean']:.4f}/{r['pdr_src_max']:.4f} | "
                f"{r['next_hop_miss_rate']:.4f} | {r['origin_miss_rate']:.4f} | {r['relay_miss_rate']:.4f} |\n"
            )

        f.write("\n## Artefactos\n")
        f.write(f"- Raw: `{raw_csv}`\n")
        f.write(f"- Resumen: `{summary_csv}`\n")
        f.write(f"- Carpeta corridas: `{OUTDIR}`\n")

    print(f"DONE outdir={OUTDIR}")
    print(f"REPORT {report}")


if __name__ == "__main__":
    main()
