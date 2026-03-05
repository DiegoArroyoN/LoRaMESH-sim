#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path


ROOT = Path("/home/diego/ns3/ns-3-dev")
SCRATCH = ROOT / "scratch" / "LoRaMESH-sim"


def run_cmd(cmd: str, cwd: Path, log_path: Path) -> int:
    with log_path.open("w", encoding="utf-8") as logf:
        proc = subprocess.run(cmd, cwd=cwd, shell=True, stdout=logf, stderr=subprocess.STDOUT)
        return proc.returncode


def parse_summary(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    pdr = data.get("pdr", {})
    sim = data.get("simulation", {})
    delay = data.get("delay", {})
    energy = data.get("energy", {})
    return {
        "wire_format_meta": sim.get("wire_format", ""),
        "data_header_bytes": sim.get("data_header_bytes", 0),
        "beacon_header_bytes": sim.get("beacon_header_bytes", 0),
        "dv_entry_bytes": sim.get("dv_entry_bytes", 0),
        "generated": pdr.get("total_data_generated", 0),
        "generated_eligible": pdr.get("total_data_generated_eligible", 0),
        "delivered": pdr.get("delivered", 0),
        "delivered_eligible": pdr.get("delivered_eligible", 0),
        "pdr": pdr.get("pdr", 0.0),
        "pdr_eligible": pdr.get("pdr_e2e_generated_eligible", 0.0),
        "end_window_generated": pdr.get("end_window_generated", 0),
        "avg_delay_s": delay.get("avg_s", 0.0),
        "energy_used_j": energy.get("total_used_j", 0.0),
    }


def parse_log_causes(path: Path) -> dict:
    out = {
        "data_noroute": 0,
        "origin_pending_end": 0,
        "first_hop_no_rx": 0,
        "relay_stall": 0,
        "rx_but_no_relay": 0,
        "tx_attempt_no_ok": 0,
    }
    patt = {
        "data_noroute": re.compile(r"DATA_NOROUTE"),
        "origin_pending_end": re.compile(r"ORIGIN_PENDING_END"),
        "first_hop_no_rx": re.compile(r"first_hop_no_rx"),
        "relay_stall": re.compile(r"relay_stall"),
        "rx_but_no_relay": re.compile(r"rx_but_no_relay"),
        "tx_attempt_no_ok": re.compile(r"tx_attempt_no_ok"),
    }
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            for k, rgx in patt.items():
                if rgx.search(line):
                    out[k] += 1
    return out


def build_scenario_args(scenario: str, seed: int, wire: str) -> str:
    common = (
        f"--trafficLoad=medium --enablePcap=false --rngRun={seed} --wireFormat={wire} "
        f"--beaconIntervalWarmSec=10 --beaconIntervalStableSec=60 "
        f"--dataStartSec=90 --dataStopSec=330 --stopSec=420 "
        f"--nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 "
        f"--nEd=25 --enableCsma=true --interferenceModel=puello"
    )
    if scenario == "n25_random_medium_csma_on_duty_off":
        return common + " --enableDuty=false"
    if scenario == "n25_random_medium_csma_on_duty_on":
        return common + " --enableDuty=true --dutyLimit=0.01"
    if scenario == "line_n5_multihop":
        return (
            f"--trafficLoad=medium --enablePcap=false --rngRun={seed} --wireFormat={wire} "
            f"--beaconIntervalWarmSec=10 --beaconIntervalStableSec=60 "
            f"--dataStartSec=90 --dataStopSec=330 --stopSec=420 "
            f"--nodePlacementMode=line --spacing=250 --nEd=5 "
            f"--enableCsma=true --enableDuty=false --interferenceModel=puello"
        )
    if scenario == "line_n8_multihop":
        return (
            f"--trafficLoad=medium --enablePcap=false --rngRun={seed} --wireFormat={wire} "
            f"--beaconIntervalWarmSec=10 --beaconIntervalStableSec=60 "
            f"--dataStartSec=90 --dataStopSec=330 --stopSec=420 "
            f"--nodePlacementMode=line --spacing=180 --nEd=8 "
            f"--enableCsma=true --enableDuty=false --interferenceModel=puello"
        )
    raise ValueError(f"unknown scenario: {scenario}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default="")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir) if args.outdir else SCRATCH / "validation_results" / f"wire_v2_migration_{ts}"
    outdir.mkdir(parents=True, exist_ok=True)

    # Campaign matrix
    runs = []
    for scenario in [
        "n25_random_medium_csma_on_duty_off",
        "n25_random_medium_csma_on_duty_on",
    ]:
        for wire in ["v1", "v2"]:
            for seed in [1, 3, 5]:
                runs.append((scenario, wire, seed))
    for scenario in ["line_n5_multihop", "line_n8_multihop"]:
        for wire in ["v1", "v2"]:
            runs.append((scenario, wire, 1))

    rows = []
    for scenario, wire, seed in runs:
        run_name = f"{scenario}__{wire}__seed{seed}"
        run_dir = outdir / scenario / wire / f"seed_{seed}"
        run_dir.mkdir(parents=True, exist_ok=True)
        done_marker = run_dir / "done.ok"
        summary_dst = run_dir / "mesh_dv_summary.json"
        log_path = run_dir / "run.log"
        meta_path = run_dir / "meta.json"

        if args.resume and done_marker.exists() and summary_dst.exists():
            summary = parse_summary(summary_dst)
            causes = parse_log_causes(log_path) if log_path.exists() else {}
            rows.append(
                {
                    "scenario": scenario,
                    "wire": wire,
                    "seed": seed,
                    **summary,
                    **causes,
                    "exit_code": 0,
                    "run_name": run_name,
                }
            )
            continue

        cli = build_scenario_args(scenario, seed, wire)
        cmd = f"./ns3 run {shlex.quote('mesh_dv_baseline ' + cli)}"
        exit_code = run_cmd(cmd, ROOT, log_path)

        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "scenario": scenario,
                    "wire": wire,
                    "seed": seed,
                    "cmd": cmd,
                    "exit_code": exit_code,
                },
                f,
                indent=2,
            )

        if exit_code == 0:
            summary_src = ROOT / "mesh_dv_summary.json"
            if summary_src.exists():
                shutil.copy2(summary_src, summary_dst)
            done_marker.write_text("ok\n", encoding="utf-8")
            summary = parse_summary(summary_dst)
            causes = parse_log_causes(log_path)
        else:
            summary = {
                "wire_format_meta": "",
                "data_header_bytes": 0,
                "beacon_header_bytes": 0,
                "dv_entry_bytes": 0,
                "generated": 0,
                "generated_eligible": 0,
                "delivered": 0,
                "delivered_eligible": 0,
                "pdr": 0.0,
                "pdr_eligible": 0.0,
                "end_window_generated": 0,
                "avg_delay_s": 0.0,
                "energy_used_j": 0.0,
            }
            causes = parse_log_causes(log_path)

        rows.append(
            {
                "scenario": scenario,
                "wire": wire,
                "seed": seed,
                **summary,
                **causes,
                "exit_code": exit_code,
                "run_name": run_name,
            }
        )

    csv_path = outdir / "ab_results.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "run_name",
                "scenario",
                "wire",
                "seed",
                "exit_code",
                "wire_format_meta",
                "data_header_bytes",
                "beacon_header_bytes",
                "dv_entry_bytes",
                "generated",
                "generated_eligible",
                "delivered",
                "delivered_eligible",
                "pdr",
                "pdr_eligible",
                "end_window_generated",
                "avg_delay_s",
                "energy_used_j",
                "data_noroute",
                "origin_pending_end",
                "first_hop_no_rx",
                "relay_stall",
                "rx_but_no_relay",
                "tx_attempt_no_ok",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Aggregate by scenario + wire
    agg = {}
    for row in rows:
        key = (row["scenario"], row["wire"])
        agg.setdefault(key, []).append(row)

    summary_out = []
    for (scenario, wire), items in sorted(agg.items()):
        ok = [x for x in items if x["exit_code"] == 0]
        n = len(ok)
        if n == 0:
            summary_out.append(
                {
                    "scenario": scenario,
                    "wire": wire,
                    "runs": len(items),
                    "ok_runs": 0,
                }
            )
            continue
        summary_out.append(
            {
                "scenario": scenario,
                "wire": wire,
                "runs": len(items),
                "ok_runs": n,
                "avg_pdr": sum(x["pdr"] for x in ok) / n,
                "avg_pdr_eligible": sum(x["pdr_eligible"] for x in ok) / n,
                "avg_generated": sum(x["generated"] for x in ok) / n,
                "avg_delivered": sum(x["delivered"] for x in ok) / n,
                "avg_end_window_generated": sum(x["end_window_generated"] for x in ok) / n,
                "avg_data_noroute": sum(x["data_noroute"] for x in ok) / n,
                "avg_origin_pending_end": sum(x["origin_pending_end"] for x in ok) / n,
                "avg_first_hop_no_rx": sum(x["first_hop_no_rx"] for x in ok) / n,
            }
        )

    with (outdir / "ab_summary.json").open("w", encoding="utf-8") as f:
        json.dump({"outdir": str(outdir), "summary": summary_out}, f, indent=2)

    print(str(outdir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

