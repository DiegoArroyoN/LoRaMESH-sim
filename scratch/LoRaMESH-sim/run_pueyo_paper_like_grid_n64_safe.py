#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shlex
import shutil
import subprocess
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
SIM_BIN = NS3_DIR / "build" / "scratch" / "LoRaMESH-sim" / "ns3-dev-mesh_dv_baseline-default"

PROFILE = "pueyo2024_paper_like"
TOPOLOGY = "pueyo_grid"
LOAD = "low"
SPACING_M = 177
SIDE = 8
N_NODES = SIDE * SIDE
SEEDS = [1, 2, 3]
TEMPORAL_POLICY = "policy1_phase_only"
TEMPORAL_ALLOW_VARIANT = True
TEMPORAL_START_PHASE_MAX_SEC = 100.0
TEMPORAL_PERIOD_JITTER_MAX_SEC = 0.0
TEMPORAL_PERIOD_JITTER_SYMMETRIC = False
TEMPORAL_ENABLE_DATA_SLOTS = False
DATA_START_SEC = 300.0
DRAIN_SEC = 600.0
PDR_END_WINDOW_SEC = DRAIN_SEC
ROOT_OUTPUTS = [
    "mesh_dv_summary.json",
    "mesh_dv_metrics_tx.csv",
    "mesh_dv_metrics_rx.csv",
    "mesh_dv_metrics_routes.csv",
    "mesh_dv_metrics_routes_used.csv",
    "mesh_dv_metrics_delay.csv",
    "mesh_dv_metrics_overhead.csv",
    "mesh_dv_metrics_duty.csv",
    "mesh_dv_metrics_energy.csv",
    "mesh_dv_metrics_lifetime.csv",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def packets_per_node(n_nodes: int) -> int:
    return 100 * (n_nodes - 1)


def expected_generated_total(n_nodes: int) -> int:
    return n_nodes * packets_per_node(n_nodes)


def data_stop_sec(n_nodes: int) -> float:
    return DATA_START_SEC + packets_per_node(n_nodes) * 100.0


def stop_sec(n_nodes: int) -> float:
    return data_stop_sec(n_nodes) + DRAIN_SEC


def build_args(seed: int, forced_dst: int, only_src: int) -> dict[str, object]:
    area = (SIDE - 1) * SPACING_M
    args = {
        "profile": PROFILE,
        "nodePlacementMode": TOPOLOGY,
        "pueyoGridSide": SIDE,
        "pueyoGridSpacingM": SPACING_M,
        "nEd": N_NODES,
        "areaWidth": area,
        "areaHeight": area,
        "trafficLoad": LOAD,
        "trafficMode": "pueyo_all_to_all",
        "pueyoPacketsPerPair": 100,
        "dataStartSec": DATA_START_SEC,
        "dataStopSec": data_stop_sec(N_NODES),
        "stopSec": stop_sec(N_NODES),
        "pdrEndWindowSec": PDR_END_WINDOW_SEC,
        "enablePcap": "false",
        "verboseLogs": "false",
        "enableNs3EnergyFramework": "false",
        "enableMetricsEssentialOnly": "true",
        "allowTemporalDesyncVariant": str(TEMPORAL_ALLOW_VARIANT).lower(),
        "dataStartPhaseMaxSec": TEMPORAL_START_PHASE_MAX_SEC,
        "dataPeriodJitterMaxSec": TEMPORAL_PERIOD_JITTER_MAX_SEC,
        "dataPeriodJitterSymmetric": str(TEMPORAL_PERIOD_JITTER_SYMMETRIC).lower(),
        "enableDataSlots": str(TEMPORAL_ENABLE_DATA_SLOTS).lower(),
        "rngRun": seed,
    }
    if forced_dst >= 0:
        args["forcedDataDestinationId"] = forced_dst
    if only_src >= 0:
        args["onlyGenerateFromNodeId"] = only_src
    return args


def move_root_outputs(dst_dir: Path, bucket: str) -> list[str]:
    moved: list[str] = []
    target = dst_dir / bucket
    ensure_dir(target)
    for name in ROOT_OUTPUTS:
        src = NS3_DIR / name
        if src.exists():
            shutil.move(str(src), str(target / name))
            moved.append(name)
    return moved


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def rebuild_row(run_dir: Path, seed: int, elapsed_s: float, forced_dst: int, only_src: int) -> dict:
    summary = load_json(run_dir / "root_outputs" / "mesh_dv_summary.json")

    sim = summary["simulation"]
    pdr = summary["pdr"]
    cp = summary["control_plane"]
    routes = summary["routes"]
    forwarding = summary["forwarding"]
    delay = summary["delay"]
    drops = summary["drops"]
    q = summary["queue_backlog"]
    expected = expected_generated_total(N_NODES)
    generated = int(pdr["total_data_generated"])
    delivered_count = int(pdr["delivered"])
    delay_avg_s = float(delay["avg_s"])
    delay_p95_s = float(delay["p95_s"])
    delivery_ratio = float(pdr["delivery_ratio"])
    source_first_tx_count = int(pdr["source_first_tx_count"])
    forwarded_unique_count = int(forwarding["forwarded_unique_count"])

    row = {
        "profile": sim["profile"],
        "topology": TOPOLOGY,
        "spacing_m": SPACING_M,
        "grid_side": SIDE,
        "n_nodes": N_NODES,
        "load": LOAD,
        "temporal_policy": TEMPORAL_POLICY,
        "seed": seed,
        "data_start_sec": sim["data_start_sec"],
        "data_stop_sec": sim["data_stop_sec"],
        "stop_sec": sim["stop_sec"],
        "workload_target": expected,
        "generated_count": generated,
        "workload_completed_fraction": (float(generated) / float(expected)) if expected > 0 else 0.0,
        "tx_sent_count": int(cp["data_tx_sent"]),
        "delivered_count": delivered_count,
        "pdr": delivery_ratio,
        "delay_avg_s": delay_avg_s,
        "delay_p95_s": delay_p95_s,
        "beacons_tx": int(cp["beacon_tx_sent"]),
        "beacons_rx": int(cp["beacon_rx_ok"]),
        "forwarded_unique_count": forwarded_unique_count,
        "routes_total": int(routes["routes_total"]),
        "routes_primary_total": int(routes["routes_primary_total"]),
        "routes_backup_total": int(routes["routes_backup_total"]),
        "drop_no_route": int(drops["drop_no_route"]),
        "drop_no_route_src": int(drops["drop_no_route_src"]),
        "drop_no_route_relay": int(drops["drop_no_route_relay"]),
        "rx_post_lock_interference_fail": int(cp["rx_post_lock_interference_fail"]),
        "rx_no_more_demodulators": int(cp["rx_no_more_demodulators"]),
        "rx_scan_miss_before_lock": int(cp["rx_scan_miss_before_lock"]),
        "pueyo_same_sf_overlap_events": int(cp["pueyo_same_sf_overlap_events"]),
        "pueyo_destructive_overlap_drops": int(cp["pueyo_destructive_overlap_drops"]),
        "pueyo_capture_or_timing_survivals": int(cp["pueyo_capture_or_timing_survivals"]),
        "queued_packets_at_data_stop": int(q["queued_packets_at_data_stop"]),
        "queued_packets_end": int(q["queued_packets_end"]),
        "source_first_tx_count": source_first_tx_count,
        "wire_format": sim["wire_format"],
        "sf_min": int(sim["sf_min"]),
        "sf_max": int(sim["sf_max"]),
        "pueyo_flora_like_rx": bool(sim["pueyo_flora_like_rx"]),
        "enable_sf_scan_rx": bool(sim["enable_sf_scan_rx"]),
        "enable_ns3_energy_framework": bool(sim["enable_ns3_energy_framework"]),
        "interference_model": sim["interference_model"],
        "channel_count": int(sim["channel_count"]),
        "reception_paths": int(sim["reception_paths"]),
        "run_dir": str(run_dir),
        "elapsed_s": elapsed_s,
        "reused_from": "",
        "metrics_periodic_flush": False,
        "metrics_flush_interval_sec": 0.0,
        "temporal_allow_variant": TEMPORAL_ALLOW_VARIANT,
        "data_start_phase_max_sec": TEMPORAL_START_PHASE_MAX_SEC,
        "data_period_jitter_max_sec": TEMPORAL_PERIOD_JITTER_MAX_SEC,
        "data_period_jitter_symmetric": TEMPORAL_PERIOD_JITTER_SYMMETRIC,
        "enable_data_slots": TEMPORAL_ENABLE_DATA_SLOTS,
        "forced_data_destination_id": forced_dst,
        "only_generate_from_node_id": only_src,
    }
    return row


def run_seed(outdir: Path, seed: int, forced_dst: int, only_src: int) -> dict:
    run_dir = outdir / "runs_safe" / "n64" / f"seed_{seed}"
    done_row_path = run_dir / "rebuilt_row.json"
    if done_row_path.exists():
        return load_json(done_row_path)

    ensure_dir(run_dir)
    moved_before = move_root_outputs(run_dir, "root_outputs_preexisting")
    meta = {
        "seed": seed,
        "profile": PROFILE,
        "topology": TOPOLOGY,
        "n_nodes": N_NODES,
        "temporal_policy": TEMPORAL_POLICY,
        "data_start_sec": DATA_START_SEC,
        "data_stop_sec": data_stop_sec(N_NODES),
        "stop_sec": stop_sec(N_NODES),
        "metrics_periodic_flush": False,
        "metrics_flush_interval_sec": 0.0,
        "metrics_essential_only": True,
        "temporal_allow_variant": TEMPORAL_ALLOW_VARIANT,
        "data_start_phase_max_sec": TEMPORAL_START_PHASE_MAX_SEC,
        "data_period_jitter_max_sec": TEMPORAL_PERIOD_JITTER_MAX_SEC,
        "data_period_jitter_symmetric": TEMPORAL_PERIOD_JITTER_SYMMETRIC,
        "enable_data_slots": TEMPORAL_ENABLE_DATA_SLOTS,
        "forced_data_destination_id": forced_dst,
        "only_generate_from_node_id": only_src,
        "moved_root_outputs_before_run": moved_before,
    }
    (run_dir / "run_meta_before.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    sim_args = build_args(seed, forced_dst, only_src)
    cmd = [str(SIM_BIN)] + [f"--{k}={v}" for k, v in sim_args.items()]
    t0 = time.time()
    with (run_dir / "run_command.txt").open("w", encoding="utf-8") as f:
        f.write(shlex.join(cmd) + "\n")
    proc = subprocess.run(
        cmd,
        cwd=str(NS3_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    elapsed = time.time() - t0
    meta["returncode"] = proc.returncode
    meta["elapsed_s"] = elapsed
    (run_dir / "run_meta_after.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    if proc.returncode != 0:
        raise SystemExit(f"run failed rc={proc.returncode} at {run_dir}")

    moved_after = move_root_outputs(run_dir, "root_outputs")
    if "mesh_dv_summary.json" not in moved_after:
        raise SystemExit(f"missing root summary after run at {run_dir}")
    row = rebuild_row(run_dir, seed, elapsed, forced_dst, only_src)
    done_row_path.write_text(json.dumps(row, indent=2), encoding="utf-8")
    return row


def write_rows_csv(path: Path, rows: list[dict]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outdir",
        type=Path,
        default=BASE_DIR / "validation_results" / "pueyo_paper_like_grid_n64_safe_20260323",
    )
    parser.add_argument("--seeds", default="1,2,3")
    parser.add_argument("--forced-dst", type=int, default=-1)
    parser.add_argument("--only-src", type=int, default=-1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    outdir = args.outdir
    ensure_dir(outdir)
    seeds = [int(x) for x in args.seeds.split(",") if x.strip()]
    forced_dst = int(args.forced_dst)
    only_src = int(args.only_src)

    rows: list[dict] = []
    for seed in seeds:
        print(
            f"[N64 safe] seed={seed} dataStop={data_stop_sec(N_NODES):.0f}s stop={stop_sec(N_NODES):.0f}s "
            f"forcedDst={forced_dst} onlySrc={only_src}",
            flush=True,
        )
        row = run_seed(outdir, seed, forced_dst, only_src)
        rows.append(row)
        write_rows_csv(outdir / "pueyo_paper_like_grid_n64_safe_results_raw.csv", rows)

    print(outdir)


if __name__ == "__main__":
    main()
