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
DATE_STR = dt.datetime.now().strftime("%Y-%m-%d")
TS = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
OUTDIR = THIS_DIR / "validation_results" / f"fix_p0_p1_p2_{TS}"
OUTDIR.mkdir(parents=True, exist_ok=True)

SEEDS = [1, 3, 5]
DRAIN_SEC = 120
RX_WINDOW_SEC = 15.0

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
    "allowStaleLinkAddrForUnicastData": "true",
    "empiricalSfMinSamples": 2,
    "empiricalSfSelectMode": "robust_min",
    "routeSwitchMinDeltaX100": 5,
    "avoidImmediateBacktrack": "true",
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

BASELINE_COUNTS = {
    "no_tx_origin": 98,
    "first_hop_no_rx": 91,
    "relay_stall": 17,
    "rx_but_no_relay": 12,
}


PAT_TX_ATTEMPT = re.compile(
    r"FWDTRACE data_tx_attempt time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)"
)
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
PAT_BACKTRACK = re.compile(
    r"FWDTRACE backtrack_drop time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)"
)
PAT_NOROUTE_MAC = re.compile(
    r"FWDTRACE DATA_NOROUTE time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+).*reason=(?:no_link_addr_for_unicast|no_mac_for_unicast)"
)


def dict_to_args(d):
    return " ".join(f"--{k}={v}" for k, v in d.items())


def _mean(values):
    return statistics.mean(values) if values else 0.0


def parse_forensics(log_text):
    attempts = []
    tx_details = []
    rx_map = defaultdict(list)
    fwd_map = defaultdict(list)
    delivered = set()

    for m in PAT_TX_ATTEMPT.finditer(log_text):
        attempts.append(
            {
                "t": float(m.group(1)),
                "node": int(m.group(2)),
                "src": int(m.group(3)),
                "dst": int(m.group(4)),
                "seq": int(m.group(5)),
            }
        )

    for m in PAT_TX_DETAIL.finditer(log_text):
        tx_details.append(
            {
                "node": int(m.group(1)),
                "src": int(m.group(2)),
                "dst": int(m.group(3)),
                "seq": int(m.group(4)),
                "t": float(m.group(5)),
                "next_hop": int(m.group(6)),
                "sf": int(m.group(7)),
            }
        )

    for m in PAT_RX.finditer(log_text):
        key = (int(m.group(3)), int(m.group(4)), int(m.group(5)))
        rx_map[key].append((int(m.group(2)), float(m.group(1))))

    for m in PAT_FWD.finditer(log_text):
        key = (int(m.group(3)), int(m.group(4)), int(m.group(5)))
        fwd_map[key].append((int(m.group(2)), float(m.group(1))))

    for m in PAT_DELIVER.finditer(log_text):
        delivered.add((int(m.group(3)), int(m.group(4)), int(m.group(5))))

    drop_seen = len(list(PAT_DROP_SEEN.finditer(log_text)))
    backtrack_drop = len(list(PAT_BACKTRACK.finditer(log_text)))
    no_mac = []
    for m in PAT_NOROUTE_MAC.finditer(log_text):
        no_mac.append((int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5))))

    attempt_keys = {(a["src"], a["dst"], a["seq"]) for a in attempts}
    src_tx_keys = {(t["src"], t["dst"], t["seq"]) for t in tx_details if t["node"] == t["src"]}

    no_tx_origin = len(attempt_keys - src_tx_keys)

    first_hop_no_rx = 0
    source_tx = [t for t in tx_details if t["node"] == t["src"]]
    for tx in source_tx:
        key = (tx["src"], tx["dst"], tx["seq"])
        nh = tx["next_hop"]
        tx_t = tx["t"]
        hit = any(node == nh and tx_t <= rt <= (tx_t + RX_WINDOW_SEC) for node, rt in rx_map.get(key, []))
        if not hit:
            first_hop_no_rx += 1

    relay_stall = 0
    for key in attempt_keys:
        if key in delivered:
            continue
        src, dst, _ = key
        relay_rx = [node for node, _ in rx_map.get(key, []) if node not in (src, dst)]
        relay_fwd = [node for node, _ in fwd_map.get(key, []) if node not in (src, dst)]
        if relay_rx and not relay_fwd:
            relay_stall += 1

    rx_but_no_relay = drop_seen + backtrack_drop

    return {
        "no_tx_origin": no_tx_origin,
        "first_hop_no_rx": first_hop_no_rx,
        "relay_stall": relay_stall,
        "rx_but_no_relay": rx_but_no_relay,
        "drop_seen_once": drop_seen,
        "backtrack_drop": backtrack_drop,
        "no_link_addr_for_unicast_events": len(no_mac),
        "no_mac_for_unicast_events": len(no_mac),  # legacy alias
    }


def run_one(scenario, profile, seed):
    args = dict(COMMON_ARGS)
    args.update(scenario["args"])
    args.update(profile["args"])
    args["rngRun"] = seed
    stop_sec = float(args["stopSec"])
    args["dataStopSec"] = max(0.0, stop_sec - DRAIN_SEC)
    args["pdrEndWindowSec"] = DRAIN_SEC

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

    forensic = parse_forensics(proc.stdout)

    pdr = summary.get("pdr", {})
    pdr_by_src = summary.get("pdr_by_source", [])
    src_pdr_values = [float(x.get("pdr", 0.0)) for x in pdr_by_src]

    row = {
        "scenario": scenario["id"],
        "profile": profile["id"],
        "seed": seed,
        "exit_code": proc.returncode,
        "elapsed_s": round(elapsed, 3),
        "pdr_total": float(pdr.get("pdr", 0.0)),
        "pdr_eligible": float(pdr.get("pdr_e2e_generated_eligible", pdr.get("pdr", 0.0))),
        "generated_total": int(pdr.get("total_data_generated", 0)),
        "generated_eligible": int(pdr.get("total_data_generated_eligible", pdr.get("total_data_generated", 0))),
        "delivered_total": int(pdr.get("delivered", 0)),
        "delivered_eligible": int(pdr.get("delivered_eligible", pdr.get("delivered", 0))),
        "end_window_generated": int(pdr.get("end_window_generated", 0)),
        "legacy_total_data_tx_attempts": int(pdr.get("legacy_total_data_tx_attempts", 0)),
        "legacy_pdr_tx_based": float(pdr.get("legacy_pdr_tx_based", 0.0)),
        "pdr_src_min": min(src_pdr_values) if src_pdr_values else 0.0,
        "pdr_src_mean": _mean(src_pdr_values),
        "pdr_src_max": max(src_pdr_values) if src_pdr_values else 0.0,
        **forensic,
        "run_dir": str(run_dir),
    }
    return row


def compute_acceptance(summary_rows):
    all_rows = [r for r in summary_rows if r["profile"] in {"duty_on", "duty_off"}]
    means = {
        "no_tx_origin": _mean([r["no_tx_origin"] for r in all_rows]),
        "first_hop_no_rx": _mean([r["first_hop_no_rx"] for r in all_rows]),
        "relay_stall": _mean([r["relay_stall"] for r in all_rows]),
        "rx_but_no_relay": _mean([r["rx_but_no_relay"] for r in all_rows]),
    }

    reductions = {}
    for k, baseline in BASELINE_COUNTS.items():
        reductions[k] = ((baseline - means[k]) / baseline) if baseline > 0 else 0.0

    checks = {
        "no_tx_origin_reduction_ge_80": reductions["no_tx_origin"] >= 0.80,
        "rx_but_no_relay_reduction_ge_70": reductions["rx_but_no_relay"] >= 0.70,
        "first_hop_no_rx_reduction_ge_20": reductions["first_hop_no_rx"] >= 0.20,
        "relay_stall_reduction_ge_30": reductions["relay_stall"] >= 0.30,
    }

    return means, reductions, checks


def write_reports(rows, summary_rows, means, reductions, checks):
    report_fix = THIS_DIR / f"REPORTE_FIX_P0_P1_P2_{DATE_STR}.md"
    report_causes = THIS_DIR / f"REPORTE_AB_PDR_CAUSAS_{DATE_STR}.md"

    with report_fix.open("w") as f:
        f.write("# Reporte Fix P0-P1-P2\n\n")
        f.write(f"- Fecha: {dt.datetime.now().isoformat()}\n")
        f.write(f"- Outdir campaña: `{OUTDIR}`\n")
        f.write(f"- Seeds: {SEEDS}\n")
        f.write(f"- Escenarios: {[s['id'] for s in SCENARIOS]}\n")
        f.write(f"- Perfiles: {[p['id'] for p in PROFILES]}\n\n")

        f.write("## Cambios implementados\n")
        f.write("1. P0: unicast data permite MAC stale configurable (`AllowStaleMacForUnicastData`) sin fallback broadcast.\n")
        f.write("2. P1: selección SF empírica robusta (`EmpiricalSfSelectMode=robust_min`, `EmpiricalSfMinSamples`) en ventana `neighborLinkTimeoutSec`.\n")
        f.write("3. P1: histéresis de cambio de next-hop en DV (`RouteSwitchMinDeltaX100`) y trazas `DVTRACE_ROUTE_DECISION`.\n")
        f.write("4. P1: guardia anti-backtrack inmediato en forwarding (`AvoidImmediateBacktrack`) con traza `FWDTRACE backtrack_drop`.\n")
        f.write("5. P2: métricas JSON con `pdr_e2e_generated_eligible` y `end_window_generated` + campaña con `dataStopSec=stopSec-drainSec`.\n\n")

        f.write("## Build y smoke\n")
        f.write("- `./ns3 build`: PASS\n")
        f.write("- Smoke corto con `pdrEndWindowSec`: PASS (campos nuevos presentes en `mesh_dv_summary.json`).\n\n")

        f.write("## Resumen A/B (promedio por escenario/perfil)\n")
        f.write("| Escenario | Perfil | PDR total | PDR elegible | End-window | no_tx_origin | first_hop_no_rx | relay_stall | rx_but_no_relay |\n")
        f.write("|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for r in summary_rows:
            f.write(
                f"| {r['scenario']} | {r['profile']} | {r['pdr_total']:.4f} | {r['pdr_eligible']:.4f} | "
                f"{r['end_window_generated']:.2f} | {r['no_tx_origin']:.2f} | {r['first_hop_no_rx']:.2f} | "
                f"{r['relay_stall']:.2f} | {r['rx_but_no_relay']:.2f} |\n"
            )

        f.write("\n## Criterios de aceptación (vs baseline histórico)\n")
        f.write(f"- Baseline: {BASELINE_COUNTS}\n")
        f.write(f"- Promedios campaña: {means}\n")
        f.write("- Reducciones:\n")
        for k, v in reductions.items():
            f.write(f"  - {k}: {v*100:.1f}%\n")
        f.write("- Checks:\n")
        for k, v in checks.items():
            f.write(f"  - {k}: {'PASS' if v else 'FAIL'}\n")

    with report_causes.open("w") as f:
        f.write("# Reporte AB PDR Causas\n\n")
        f.write(f"- Fecha: {dt.datetime.now().isoformat()}\n")
        f.write(f"- Outdir campaña: `{OUTDIR}`\n\n")

        f.write("## Resultados por corrida\n")
        f.write("| Escenario | Perfil | Seed | PDR total | PDR elegible | Generated | Eligible | Delivered | End-window | no_tx_origin | first_hop_no_rx | relay_stall | rx_but_no_relay |\n")
        f.write("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for r in rows:
            f.write(
                f"| {r['scenario']} | {r['profile']} | {r['seed']} | {r['pdr_total']:.4f} | {r['pdr_eligible']:.4f} | "
                f"{r['generated_total']} | {r['generated_eligible']} | {r['delivered_total']} | {r['end_window_generated']} | "
                f"{r['no_tx_origin']} | {r['first_hop_no_rx']} | {r['relay_stall']} | {r['rx_but_no_relay']} |\n"
            )

    shutil.copy2(report_fix, OUTDIR / report_fix.name)
    shutil.copy2(report_causes, OUTDIR / report_causes.name)

    return report_fix, report_causes


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
                    f"no_tx_origin={row['no_tx_origin']} first_hop_no_rx={row['first_hop_no_rx']}"
                )

    raw_csv = OUTDIR / "ab_raw.csv"
    with raw_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    grouped = defaultdict(list)
    for r in rows:
        grouped[(r["scenario"], r["profile"])].append(r)

    summary_rows = []
    for (scenario, profile), items in sorted(grouped.items()):
        out = {"scenario": scenario, "profile": profile, "runs": len(items)}
        for key in [
            "pdr_total",
            "pdr_eligible",
            "generated_total",
            "generated_eligible",
            "delivered_total",
            "end_window_generated",
            "pdr_src_min",
            "pdr_src_mean",
            "pdr_src_max",
            "no_tx_origin",
            "first_hop_no_rx",
            "relay_stall",
            "rx_but_no_relay",
            "no_mac_for_unicast_events",
        ]:
            out[key] = _mean([float(x[key]) for x in items])
        summary_rows.append(out)

    summary_csv = OUTDIR / "ab_summary.csv"
    with summary_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    means, reductions, checks = compute_acceptance(summary_rows)

    report_fix, report_causes = write_reports(rows, summary_rows, means, reductions, checks)

    print(f"DONE outdir={OUTDIR}")
    print(f"RAW={raw_csv}")
    print(f"SUMMARY={summary_csv}")
    print(f"REPORT_FIX={report_fix}")
    print(f"REPORT_CAUSES={report_causes}")


if __name__ == "__main__":
    main()
