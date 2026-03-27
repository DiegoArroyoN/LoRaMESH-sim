#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import json
import statistics
import subprocess
import time
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"

SPACING = 177
DATA_START = 300.0
DATA_STOP = 3900.0
STOP = 4500.0
PDR_END = 600.0
SIDES = [3, 5]
TOPOLOGIES = ["pueyo_grid", "pueyo_random_equiv"]
LOADS = ["low", "high"]
SEEDS = [1, 2, 3]


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def run(cmd, cwd: Path, log: Path):
    ensure_dir(log.parent)
    with log.open("w", encoding="utf-8") as f:
        return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)


def git_commit() -> str:
    p = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(NS3_DIR), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.stdout.strip() if p.returncode == 0 else "unknown"


def cli(args: dict[str, object]) -> str:
    return " ".join(f"--{k}={v}" for k, v in args.items())


def mean_std_ci95(vals):
    if not vals:
        return 0.0, 0.0, 0.0, 0.0
    if len(vals) == 1:
        x = float(vals[0])
        return x, 0.0, x, x
    m = float(statistics.mean(vals))
    s = float(statistics.stdev(vals))
    h = 1.96 * (s / (len(vals) ** 0.5))
    return m, s, m - h, m + h


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


COMMON = {
    "profile": "extended",
    "enableCsma": "false",
    "enableDuty": "false",
    "dutyLimit": 1.0,
    "routeMetricMode": "toa_only",
    "sfLinkMode": "deterministic_sensitivity",
    "sfLinkMarginDb": 0.0,
    "wireFormat": "pueyo7b",
    "txPowerDbm": 20,
    "preambleSymbols": 16,
    "initTtl": 63,
    "useProbabilisticSfForBeacons": "true",
    "interferenceModel": "pueyo_fixed_capture",
    "shadowingSigmaDb": 3.57,
    "trafficMode": "pueyo_all_to_all",
    "pueyoPacketsPerPair": 100,
    "beaconIntervalWarmSec": 60,
    "beaconIntervalStableSec": 60,
    "routeTimeoutFactor": 5,
    "routeSwitchMinDeltaX100": 0,
    "dvPayloadMaxBytes": 251,
    "routeAdvertPolicy": "cost_weighted",
    "maxRoutesPerDestination": 2,
    "maxTotalRoutes": 1024,
    "costEncoding": "cost255",
    "beaconLatestOnly": "false",
    "prioritizeBeacons": "false",
    "pueyoStrictQueueScheduler": "true",
    "controlBackoffFactor": 1.0,
    "dataBackoffFactor": 10.0,
    "enablePcap": "false",
    "verboseLogs": "false",
    "dataPeriodJitterMaxSec": 0.0,
    "sfScanResetOnNewSignal": "true",
    "sfScanEdThresholdDbm": -120.0,
    "dataPayloadSizeBytes": 20,
    "dataStartSec": DATA_START,
    "dataStopSec": DATA_STOP,
    "stopSec": STOP,
    "pdrEndWindowSec": PDR_END,
    "sfMin": 7,
    "sfMax": 8,
}

VARIANTS = [
    {
        "variant": "baseline_scan_lock",
        "variant_label": "Baseline actual (single-demod + RX-scan/lock)",
        "enableSfScanRx": "true",
        "ablation_note": "Default SimpleGatewayLoraPhy behaviour with pending queue, scan and lock.",
    },
    {
        "variant": "ablation_immediate_lock",
        "variant_label": "Ablacion simplificada (lock inmediato viable, sin scan pre-lock)",
        "enableSfScanRx": "false",
        "ablation_note": "Same PHY and one demod path, but immediate true-SF lock without pending scan.",
    },
]


def main():
    outdir = BASE_DIR / "validation_results" / f"pueyo_phy_ablation_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ensure_dir(outdir)
    gith = git_commit()
    rows = []
    total = len(VARIANTS) * len(LOADS) * len(TOPOLOGIES) * len(SIDES) * len(SEEDS)
    idx = 0

    for variant in VARIANTS:
        for load in LOADS:
            for topo in TOPOLOGIES:
                for side in SIDES:
                    n = side * side
                    area = (side - 1) * SPACING
                    for seed in SEEDS:
                        idx += 1
                        cfg = dict(COMMON)
                        cfg.update(
                            {
                                "enableSfScanRx": variant["enableSfScanRx"],
                                "trafficLoad": load,
                                "nodePlacementMode": topo,
                                "pueyoGridSide": side,
                                "pueyoGridSpacingM": SPACING,
                                "nEd": n,
                                "areaWidth": area,
                                "areaHeight": area,
                                "rngRun": seed,
                            }
                        )
                        run_dir = outdir / variant["variant"] / load / topo / f"n{n}" / f"seed_{seed}"
                        sim_args = "mesh_dv_baseline " + cli(cfg)
                        cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
                        print(f"[{idx:03d}/{total}] {variant['variant']} load={load} {topo} n={n} seed={seed}", flush=True)
                        t0 = time.time()
                        proc = run(cmd, NS3_DIR, run_dir / "run.log")
                        if proc.returncode != 0:
                            raise SystemExit(f"run failed rc={proc.returncode} at {run_dir}")
                        summary_src = NS3_DIR / "mesh_dv_summary.json"
                        (run_dir / "mesh_dv_summary.json").write_bytes(summary_src.read_bytes())
                        (run_dir / "meta.json").write_text(
                            json.dumps(
                                {
                                    "git_commit": gith,
                                    "sim_args": sim_args,
                                    "elapsed_s": time.time() - t0,
                                    "variant_label": variant["variant_label"],
                                    "ablation_note": variant["ablation_note"],
                                },
                                indent=2,
                            ),
                            encoding="utf-8",
                        )
                        j = load_json(run_dir / "mesh_dv_summary.json")
                        cp = j["control_plane"]
                        row = {
                            "variant": variant["variant"],
                            "variant_label": variant["variant_label"],
                            "load": load,
                            "topology": topo,
                            "spacing_m": SPACING,
                            "n_nodes": n,
                            "seed": seed,
                            "profile_semantics": "pueyo2024",
                            "runtime_profile": j["simulation"]["profile"],
                            "sf_range": "7-8",
                            "pdr": j["pdr"]["delivery_ratio"],
                            "delay_avg_s": j["delay"]["avg_s"],
                            "delay_p95_s": j["delay"]["p95_s"],
                            "beacon_tx_sent": cp["beacon_tx_sent"],
                            "beacon_rx_ok": cp["beacon_rx_ok"],
                            "data_tx_sent": cp["data_tx_sent"],
                            "delivered_count": j["pdr"]["delivered"],
                            "forwarded_unique_count": j["forwarding"]["forwarded_unique_count"],
                            "routes_total": j["routes"]["routes_total"],
                            "rx_scan_attempts": cp["rx_scan_attempts"],
                            "rx_scan_locks": cp["rx_scan_locks"],
                            "rx_scan_miss_before_lock": cp["rx_scan_miss_before_lock"],
                            "rx_post_lock_interference_fail": cp["rx_post_lock_interference_fail"],
                            "rx_no_more_demodulators": cp["rx_no_more_demodulators"],
                            "rx_scan_time_total_s": cp["rx_scan_time_total_s"],
                            "sf_scan_ed_threshold_dbm": j["simulation"]["sf_scan_ed_threshold_dbm"],
                            "enable_sf_scan_rx": variant["enableSfScanRx"],
                            "wire_format": j["simulation"]["wire_format"],
                            "run_dir": str(run_dir),
                        }
                        rows.append(row)
                        if idx % 6 == 0 or idx == total:
                            with (outdir / "pueyo_phy_ablation_results_raw.csv").open("w", newline="", encoding="utf-8") as f:
                                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                                w.writeheader()
                                w.writerows(rows)

    grouped = {}
    for r in rows:
        key = (r["variant"], r["variant_label"], r["load"], r["topology"], r["spacing_m"], r["n_nodes"])
        grouped.setdefault(key, []).append(r)

    agg = []
    for key, rr in sorted(grouped.items()):
        variant, variant_label, load, topo, spacing, n = key
        pdrs = [float(x["pdr"]) for x in rr]
        davg = [float(x["delay_avg_s"]) for x in rr]
        dp95 = [float(x["delay_p95_s"]) for x in rr]
        btx = [float(x["beacon_tx_sent"]) for x in rr]
        brx = [float(x["beacon_rx_ok"]) for x in rr]
        dtx = [float(x["data_tx_sent"]) for x in rr]
        ddel = [float(x["delivered_count"]) for x in rr]
        fwd = [float(x["forwarded_unique_count"]) for x in rr]
        routes = [float(x["routes_total"]) for x in rr]
        scanAttempts = [float(x["rx_scan_attempts"]) for x in rr]
        scanLocks = [float(x["rx_scan_locks"]) for x in rr]
        prelock = [float(x["rx_scan_miss_before_lock"]) for x in rr]
        postlock = [float(x["rx_post_lock_interference_fail"]) for x in rr]
        nodemod = [float(x["rx_no_more_demodulators"]) for x in rr]
        scantime = [float(x["rx_scan_time_total_s"]) for x in rr]
        mu, sd, lo, hi = mean_std_ci95(pdrs)
        agg.append(
            {
                "variant": variant,
                "variant_label": variant_label,
                "load": load,
                "topology": topo,
                "spacing_m": spacing,
                "n_nodes": n,
                "seed_set": "1,2,3",
                "n": len(rr),
                "pdr_mean": mu,
                "pdr_std": sd,
                "pdr_ci95_lo": lo,
                "pdr_ci95_hi": hi,
                "delay_avg_s_mean": statistics.mean(davg),
                "delay_p95_s_mean": statistics.mean(dp95),
                "beacon_tx_sent_mean": statistics.mean(btx),
                "beacon_rx_ok_mean": statistics.mean(brx),
                "data_tx_sent_mean": statistics.mean(dtx),
                "delivered_count_mean": statistics.mean(ddel),
                "forwarded_unique_count_mean": statistics.mean(fwd),
                "routes_total_mean": statistics.mean(routes),
                "rx_scan_attempts_mean": statistics.mean(scanAttempts),
                "rx_scan_locks_mean": statistics.mean(scanLocks),
                "rx_scan_miss_before_lock_mean": statistics.mean(prelock),
                "rx_post_lock_interference_fail_mean": statistics.mean(postlock),
                "rx_no_more_demodulators_mean": statistics.mean(nodemod),
                "rx_scan_time_total_s_mean": statistics.mean(scantime),
                "goodput_bps_mean": statistics.mean([float(x["delivered_count"]) * 20 * 8 / (DATA_STOP - DATA_START) for x in rr]),
            }
        )

    with (outdir / "pueyo_phy_ablation_results.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(agg[0].keys()))
        w.writeheader()
        w.writerows(agg)

    summary = {
        "outdir": str(outdir),
        "git_commit": gith,
        "profile_semantics": "pueyo2024",
        "runtime_profile": "extended_clone_for_phy_ablation",
        "why_clone_is_needed": "strict pueyo2024 is cloned to keep semantics fixed while toggling only EnableSfScanRx",
        "sf_range": "7-8",
        "spacing_m": SPACING,
        "loads": LOADS,
        "n_runs": len(rows),
        "variants": VARIANTS,
    }
    with (outdir / "pueyo_phy_ablation_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(outdir)


if __name__ == "__main__":
    main()
