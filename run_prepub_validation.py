#!/usr/bin/env python3
"""
Pre-publication validation pipeline for LoRaMESH (no simulator code changes).

Implements the agreed phased plan:
  - F0: Freeze + preflight + reproducibility gate
  - F1: Quick viability gate
  - F2A: I30 energy-aware routing evidence
  - F2B: I71 hold-down effectiveness under failures
  - F2C: I84/I85/I86 scalability envelope
  - F2D: I59 beacon-without-routes payload policy impact
  - F3: Consolidated report + optional critical findings + proposed fixes
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]  # .../ns-3-dev

CSV_NAMES = [
    "mesh_dv_metrics_tx.csv",
    "mesh_dv_metrics_rx.csv",
    "mesh_dv_metrics_routes.csv",
    "mesh_dv_metrics_routes_used.csv",
    "mesh_dv_metrics_delay.csv",
    "mesh_dv_metrics_overhead.csv",
    "mesh_dv_metrics_duty.csv",
    "mesh_dv_metrics_energy.csv",
]


@dataclass
class RunResult:
    rc: int
    elapsed_s: float
    summary: Dict
    summary_path: Path
    run_dir: Path
    log_path: Path
    meta_path: Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run pre-publication validation plan.")
    p.add_argument("--build", dest="build", action="store_true", default=True)
    p.add_argument("--no-build", dest="build", action="store_false")
    p.add_argument(
        "--outdir",
        type=str,
        default=None,
        help="Output root. Default: validation_results/prepub_validation_<timestamp>",
    )
    p.add_argument(
        "--only",
        choices=["all", "f0", "f1", "f2a", "f2b", "f2c", "f2d", "f3"],
        default="all",
        help="Run only one phase, or all.",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        default=True,
        help="Reuse completed runs if artifacts already exist (default: true).",
    )
    p.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Force re-run all simulations.",
    )
    p.add_argument(
        "--line-spacing",
        type=float,
        default=100.0,
        help="Line spacing default for scenarios that require it.",
    )
    p.add_argument(
        "--runtime-threshold-s",
        type=float,
        default=1800.0,
        help="Runtime threshold used in scalability acceptance criteria.",
    )
    p.add_argument(
        "--pdr-threshold",
        type=float,
        default=0.10,
        help="Median PDR threshold used in scalability acceptance criteria.",
    )
    p.add_argument(
        "--coverage-threshold",
        type=float,
        default=0.30,
        help="Route coverage threshold used in scalability acceptance criteria.",
    )
    p.add_argument(
        "--i30-loads",
        type=str,
        default="medium high",
        help='I30 traffic loads list, e.g. "high" or "medium high".',
    )
    p.add_argument(
        "--i30-areas",
        type=str,
        default="350 600",
        help='I30 random-area list in meters, e.g. "350" or "350 600".',
    )
    p.add_argument(
        "--i30-include-line",
        action="store_true",
        default=False,
        help="Include line topology control runs in I30.",
    )
    p.add_argument(
        "--i30-battery-full-capacity-j",
        type=float,
        default=38880.0,
        help="Battery full capacity [J] used in I30 runs.",
    )
    p.add_argument(
        "--dv-link-weight",
        type=float,
        default=0.70,
        help="RoutingDv LinkWeight used in simulation commands.",
    )
    p.add_argument(
        "--dv-path-weight",
        type=float,
        default=0.25,
        help="RoutingDv PathWeight used in simulation commands.",
    )
    p.add_argument(
        "--dv-path-hop-weight",
        type=float,
        default=0.05,
        help="RoutingDv PathHopWeight used in simulation commands.",
    )
    p.add_argument(
        "--i71-line-route-timeout-factor",
        type=float,
        default=1.0,
        help="MeshDvApp RouteTimeoutFactor used for I71 line expiry scenario.",
    )
    p.add_argument(
        "--i71-random-route-timeout-factor",
        type=float,
        default=1.0,
        help="MeshDvApp RouteTimeoutFactor used for I71 random churn scenario.",
    )
    p.add_argument(
        "--i71-line-stop-sec",
        type=int,
        default=3600,
        help="Simulation stop time [s] for I71 line expiry scenario.",
    )
    p.add_argument(
        "--i71-random-area-m",
        type=float,
        default=1000.0,
        help="Square area side [m] for I71 random congestion/churn scenario.",
    )
    return p.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, content: str) -> None:
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")


def read_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Dict) -> None:
    write_text(path, json.dumps(data, indent=2, ensure_ascii=True))


def run_command(cmd: List[str], cwd: Path, log_path: Optional[Path] = None) -> subprocess.CompletedProcess:
    if log_path:
        ensure_dir(log_path.parent)
        with log_path.open("w", encoding="utf-8") as lf:
            proc = subprocess.run(
                cmd,
                cwd=str(cwd),
                stdout=lf,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
    else:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    return proc


def copy_if_exists(src: Path, dst: Path) -> None:
    if src.exists():
        ensure_dir(dst.parent)
        shutil.copy2(src, dst)


def parse_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def pearson_corr(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    sx = math.sqrt(sum(v * v for v in dx))
    sy = math.sqrt(sum(v * v for v in dy))
    if sx == 0.0 or sy == 0.0:
        return None
    return sum(a * b for a, b in zip(dx, dy)) / (sx * sy)


def mean_std_ci95(values: List[float]) -> Tuple[float, float, Tuple[float, float]]:
    if not values:
        return 0.0, 0.0, (0.0, 0.0)
    if len(values) == 1:
        return values[0], 0.0, (values[0], values[0])
    m = statistics.mean(values)
    s = statistics.stdev(values)
    h = 1.96 * (s / math.sqrt(len(values)))
    return m, s, (m - h, m + h)


def required_artifacts_exist(run_dir: Path) -> bool:
    summary = run_dir / "mesh_dv_summary.json"
    if not summary.exists():
        return False
    for name in CSV_NAMES:
        if not (run_dir / name).exists():
            return False
    if not (run_dir / "run.log").exists():
        return False
    if not (run_dir / "meta.json").exists():
        return False
    return True


def dv_weight_cli(args: argparse.Namespace) -> str:
    return (
        f" --dvLinkWeight={args.dv_link_weight}"
        f" --dvPathWeight={args.dv_path_weight}"
        f" --dvPathHopWeight={args.dv_path_hop_weight}"
    )


def run_simulation(sim_args: str, run_dir: Path, resume: bool) -> RunResult:
    ensure_dir(run_dir)
    summary_dst = run_dir / "mesh_dv_summary.json"
    log_path = run_dir / "run.log"
    meta_path = run_dir / "meta.json"

    if resume and required_artifacts_exist(run_dir):
        meta = read_json(meta_path)
        summary = read_json(summary_dst)
        return RunResult(
            rc=int(meta.get("rc", 0)),
            elapsed_s=float(meta.get("elapsed_s", 0.0)),
            summary=summary,
            summary_path=summary_dst,
            run_dir=run_dir,
            log_path=log_path,
            meta_path=meta_path,
        )

    start = time.time()
    proc = run_command(["./ns3", "run", sim_args], cwd=NS3_DIR, log_path=log_path)
    elapsed = time.time() - start

    summary_src = NS3_DIR / "mesh_dv_summary.json"
    if summary_src.exists():
        shutil.copy2(summary_src, summary_dst)
    summary = read_json(summary_dst) if summary_dst.exists() else {}

    for name in CSV_NAMES:
        copy_if_exists(NS3_DIR / name, run_dir / name)

    meta = {
        "sim_args": sim_args,
        "rc": proc.returncode,
        "elapsed_s": elapsed,
        "timestamp_utc": dt.datetime.utcnow().isoformat() + "Z",
    }
    write_json(meta_path, meta)

    return RunResult(
        rc=proc.returncode,
        elapsed_s=elapsed,
        summary=summary,
        summary_path=summary_dst,
        run_dir=run_dir,
        log_path=log_path,
        meta_path=meta_path,
    )


def summary_ok(summary: Dict) -> Tuple[bool, List[str]]:
    missing = []
    paths = [
        ("pdr", "total_data_tx"),
        ("pdr", "delivered"),
        ("pdr", "pdr"),
        ("delay", "avg_s"),
        ("energy", "total_used_j"),
        ("energy", "min_remaining_frac"),
    ]
    for p1, p2 in paths:
        if p1 not in summary or p2 not in summary[p1]:
            missing.append(f"{p1}.{p2}")
    return len(missing) == 0, missing


def extract_runtime_snapshot() -> Dict[str, str]:
    def safe_cmd(cmd: List[str], cwd: Path = NS3_DIR) -> str:
        proc = run_command(cmd, cwd=cwd)
        if proc.returncode != 0 or proc.stdout is None:
            return ""
        return proc.stdout.strip()

    snap = {
        "timestamp_utc": dt.datetime.utcnow().isoformat() + "Z",
        "ns3_dir": str(NS3_DIR),
        "base_dir": str(BASE_DIR),
        "git_head": safe_cmd(["git", "rev-parse", "HEAD"], cwd=NS3_DIR),
        "git_branch": safe_cmd(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=NS3_DIR),
        "git_status_short": safe_cmd(["git", "status", "--short"], cwd=NS3_DIR),
        "gxx_version": safe_cmd(["bash", "-lc", "g++ --version | head -1"], cwd=NS3_DIR),
        "kernel": safe_cmd(["uname", "-a"], cwd=NS3_DIR),
        "command": " ".join(sys.argv),
    }
    return snap


def write_manifest(outdir: Path, snapshot: Dict) -> None:
    lines = [
        "# MANIFEST",
        "",
        "## Snapshot",
        "",
        f"- timestamp_utc: `{snapshot['timestamp_utc']}`",
        f"- ns3_dir: `{snapshot['ns3_dir']}`",
        f"- base_dir: `{snapshot['base_dir']}`",
        f"- git_head: `{snapshot['git_head']}`",
        f"- git_branch: `{snapshot['git_branch']}`",
        f"- gxx_version: `{snapshot['gxx_version']}`",
        f"- kernel: `{snapshot['kernel']}`",
        f"- command: `{snapshot['command']}`",
        "",
        "## Git status (short)",
        "",
        "```",
        snapshot["git_status_short"] if snapshot["git_status_short"] else "(clean or unavailable)",
        "```",
        "",
    ]
    write_text(outdir / "MANIFEST.md", "\n".join(lines))
    write_json(outdir / "manifest.json", snapshot)


def phase0(outdir: Path, args: argparse.Namespace) -> Dict:
    phase_dir = outdir / "f0_preflight"
    ensure_dir(phase_dir)

    result: Dict[str, object] = {}

    if args.build:
        build_log = phase_dir / "build.log"
        proc = run_command(["./ns3", "build"], cwd=NS3_DIR, log_path=build_log)
        result["build_rc"] = proc.returncode
        result["build_ok"] = proc.returncode == 0
    else:
        result["build_rc"] = 0
        result["build_ok"] = True

    smoke_args = (
        "mesh_dv_baseline --nEd=2 --nodePlacementMode=line --spacing=120 "
        "--stopSec=120 --dataStartSec=60 --trafficLoad=medium --enablePcap=false --rngRun=1"
    )
    smoke = run_simulation(smoke_args, phase_dir / "smoke", resume=args.resume)
    ok_keys, missing = summary_ok(smoke.summary)
    result["smoke_rc"] = smoke.rc
    result["smoke_ok"] = smoke.rc == 0 and ok_keys
    result["smoke_missing_keys"] = missing

    # Re-run existing V1-V4 baseline for comparability with VALIDACION_PAPER.md
    base_log = phase_dir / "baseline_v1_v4.log"
    baseline_build_flag = "--build" if args.build else "--no-build"
    proc_base = run_command(
        [
            "bash",
            "-lc",
            f"./run_paper_validation.sh {baseline_build_flag} --only all "
            f"--outdir \"{str(outdir / 'baseline_v1_v4')}\"",
        ],
        cwd=BASE_DIR,
        log_path=base_log,
    )
    result["baseline_v1v4_rc"] = proc_base.returncode
    result["baseline_v1v4_ok"] = proc_base.returncode == 0

    # Same-seed reproducibility check (run twice)
    repro_args = (
        "mesh_dv_baseline --nEd=6 --nodePlacementMode=line --spacing=100 "
        "--stopSec=300 --dataStartSec=60 --trafficLoad=medium --enablePcap=false --rngRun=1"
    )
    repro_a = run_simulation(repro_args, phase_dir / "repro_seed1_runA", resume=args.resume)
    repro_b = run_simulation(repro_args, phase_dir / "repro_seed1_runB", resume=args.resume)
    repro_equal = False
    if repro_a.summary and repro_b.summary:
        repro_equal = repro_a.summary == repro_b.summary
    result["repro_same_seed_equal"] = repro_equal

    gate_ok = (
        bool(result.get("build_ok"))
        and bool(result.get("smoke_ok"))
        and bool(result.get("baseline_v1v4_ok"))
        and bool(result.get("repro_same_seed_equal"))
    )
    result["gate_f0_ok"] = gate_ok

    lines = [
        "# FASE 0 - Preflight",
        "",
        f"- build_ok: `{result['build_ok']}` (rc={result['build_rc']})",
        f"- smoke_ok: `{result['smoke_ok']}` (rc={result['smoke_rc']})",
        f"- smoke_missing_keys: `{result['smoke_missing_keys']}`",
        f"- baseline_v1v4_ok: `{result['baseline_v1v4_ok']}` (rc={result['baseline_v1v4_rc']})",
        f"- repro_same_seed_equal: `{result['repro_same_seed_equal']}`",
        "",
        f"## Gate F0: `{'PASS' if gate_ok else 'FAIL'}`",
        "",
    ]
    write_text(phase_dir / "FASE0_RESULT.md", "\n".join(lines))
    write_json(phase_dir / "fase0_result.json", result)
    return result


def collect_basic_metrics(run: RunResult) -> Dict[str, float]:
    s = run.summary
    pdr = float(s.get("pdr", {}).get("pdr", 0.0))
    tx = int(s.get("pdr", {}).get("total_data_tx", 0))
    delivered = int(s.get("pdr", {}).get("delivered", 0))
    delay = float(s.get("delay", {}).get("avg_s", 0.0))
    energy = float(s.get("energy", {}).get("total_used_j", 0.0))
    ok_keys, missing = summary_ok(s)

    log_text = run.log_path.read_text(encoding="utf-8", errors="ignore") if run.log_path.exists() else ""
    no_route = len(re.findall(r"DATA_NOROUTE", log_text))

    route_rows = parse_csv_rows(run.run_dir / "mesh_dv_metrics_routes_used.csv")
    unique_dst = set()
    for r in route_rows:
        try:
            dst = int(r.get("destination", "-1"))
        except ValueError:
            dst = -1
        if dst >= 0 and dst != 65535:
            unique_dst.add(dst)

    return {
        "pdr": pdr,
        "total_data_tx": tx,
        "delivered": delivered,
        "avg_delay_s": delay,
        "total_used_j": energy,
        "summary_ok": 1.0 if ok_keys else 0.0,
        "missing_keys_count": float(len(missing)),
        "no_route_count": float(no_route),
        "route_unique_dst": float(len(unique_dst)),
        "elapsed_s": run.elapsed_s,
    }


def phase1(outdir: Path, args: argparse.Namespace) -> Dict:
    phase_dir = outdir / "f1_gate"
    ensure_dir(phase_dir)
    spacing = args.line_spacing

    scenarios = [
        (
            "line_n6_medium_s1_t300",
            f"mesh_dv_baseline --nEd=6 --nodePlacementMode=line --spacing={spacing} "
            "--trafficLoad=medium --rngRun=1 --stopSec=300 --dataStartSec=60 --enablePcap=false",
            False,
        ),
        (
            "random_n6_medium_s1_t300",
            "mesh_dv_baseline --nEd=6 --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 "
            "--trafficLoad=medium --rngRun=1 --stopSec=300 --dataStartSec=60 --enablePcap=false",
            False,
        ),
        (
            "random_n12_high_s3_t600",
            "mesh_dv_baseline --nEd=12 --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 "
            "--trafficLoad=high --rngRun=3 --stopSec=600 --dataStartSec=60 --enablePcap=false",
            False,
        ),
        (
            "line_n12_beacon700_s3_t1800",
            f"mesh_dv_baseline --nEd=12 --nodePlacementMode=line --spacing={spacing} "
            "--trafficLoad=medium --beaconIntervalStableSec=700 --rngRun=3 "
            "--stopSec=1800 --dataStartSec=60 --enablePcap=false",
            False,
        ),
        (
            "random_n64_medium_s1_t600",
            "mesh_dv_baseline --nEd=64 --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 "
            "--trafficLoad=medium --rngRun=1 --stopSec=600 --dataStartSec=60 --enablePcap=false",
            False,
        ),
        (
            "random_n12_medium_duty1e-6_s1_t300",
            "mesh_dv_baseline --nEd=12 --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 "
            "--trafficLoad=medium --enableDuty=true --dutyLimit=1e-6 --rngRun=1 "
            "--stopSec=300 --dataStartSec=60 --enablePcap=false",
            True,
        ),
    ]

    rows = []
    gate_ok = True
    for scen_name, sim_args, duty_extreme in scenarios:
        run = run_simulation(sim_args, phase_dir / scen_name / "seed_1", resume=args.resume)
        m = collect_basic_metrics(run)
        row = {
            "scenario": scen_name,
            "rc": run.rc,
            "duty_extreme": duty_extreme,
            **m,
        }
        rows.append(row)

        if run.rc != 0:
            gate_ok = False
        if m["summary_ok"] < 1.0:
            gate_ok = False
        if not duty_extreme and int(m["total_data_tx"]) <= 0:
            gate_ok = False
        if duty_extreme and int(m["total_data_tx"]) < 0:
            gate_ok = False

    md = [
        "# FASE 1 - Gate de Viabilidad",
        "",
        "| scenario | rc | duty_extreme | total_data_tx | delivered | pdr | delay_s | no_route | elapsed_s |",
        "|---|---:|:---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        md.append(
            f"| {r['scenario']} | {r['rc']} | {'yes' if r['duty_extreme'] else 'no'} | "
            f"{int(r['total_data_tx'])} | {int(r['delivered'])} | {r['pdr']:.4f} | "
            f"{r['avg_delay_s']:.6f} | {int(r['no_route_count'])} | {r['elapsed_s']:.2f} |"
        )
    md += [
        "",
        f"## Gate F1: `{'PASS' if gate_ok else 'FAIL'}`",
        "",
        "- Regla aplicada: `total_data_tx > 0` excepto duty extremo, sin crash y con artefactos exportados.",
        "",
    ]

    write_text(phase_dir / "FASE1_RESULT.md", "\n".join(md))
    result = {"rows": rows, "gate_f1_ok": gate_ok}
    write_json(phase_dir / "fase1_result.json", result)
    return result


def phase2a_i30(outdir: Path, args: argparse.Namespace) -> Dict:
    phase_dir = outdir / "f2a_i30_energy_routes"
    ensure_dir(phase_dir)

    spacing = args.line_spacing
    weight_opt = dv_weight_cli(args)
    battery_opt = f" --batteryFullCapacityJ={args.i30_battery_full_capacity_j}"
    seeds = list(range(1, 11))
    loads = [x.strip().lower() for x in args.i30_loads.split() if x.strip()]
    if not loads:
        loads = ["medium", "high"]
    areas: List[int] = []
    for tok in args.i30_areas.split():
        tok = tok.strip()
        if not tok:
            continue
        try:
            val = int(tok)
            if val > 0:
                areas.append(val)
        except ValueError:
            continue
    if not areas:
        areas = [350, 600]
    run_defs: List[Tuple[str, str, int]] = []

    # line control
    if args.i30_include_line:
        for load in loads:
            for seed in seeds:
                scen = f"line_load_{load}"
                sim_args = (
                    f"mesh_dv_baseline --nEd=16 --nodePlacementMode=line --spacing={spacing} "
                    f"--trafficLoad={load}{weight_opt}{battery_opt} "
                    f"--rngRun={seed} --stopSec=7200 --dataStartSec=60 --enablePcap=false"
                )
                run_defs.append((scen, sim_args, seed))

    # random principal with configurable densities
    for area in areas:
        for load in loads:
            for seed in seeds:
                scen = f"random_{area}x{area}_load_{load}"
                sim_args = (
                    "mesh_dv_baseline --nEd=16 --nodePlacementMode=random "
                    f"--areaWidth={area} --areaHeight={area} --trafficLoad={load}{weight_opt}{battery_opt} "
                    f"--rngRun={seed} "
                    "--stopSec=7200 --dataStartSec=60 --enablePcap=false"
                )
                run_defs.append((scen, sim_args, seed))

    rows = []
    corr_values = []
    delta_values = []
    direction_ok_count = 0

    for scen, sim_args, seed in run_defs:
        run = run_simulation(sim_args, phase_dir / scen / f"seed_{seed}", resume=args.resume)
        basic = collect_basic_metrics(run)

        routes_rows = parse_csv_rows(run.run_dir / "mesh_dv_metrics_routes_used.csv")
        energy_rows = parse_csv_rows(run.run_dir / "mesh_dv_metrics_energy.csv")

        uses_by_node: Dict[int, int] = {}
        for rr in routes_rows:
            try:
                node = int(rr.get("nodeId", "-1"))
                ts = float(rr.get("timestamp(s)", "0"))
            except ValueError:
                continue
            if node < 0:
                continue
            uses_by_node[node] = uses_by_node.get(node, 0) + 1
            rr["_ts"] = ts  # type: ignore[index]

        energy_frac_by_node: Dict[int, float] = {}
        for er in energy_rows:
            try:
                node = int(er.get("nodeId", "-1"))
                frac = float(er.get("energyFrac", "nan"))
            except ValueError:
                continue
            if node >= 0 and math.isfinite(frac):
                energy_frac_by_node[node] = frac

        common_nodes = sorted(set(uses_by_node.keys()) & set(energy_frac_by_node.keys()))
        x = [float(uses_by_node[n]) for n in common_nodes]
        y = [float(energy_frac_by_node[n]) for n in common_nodes]
        corr = pearson_corr(x, y)

        # Share shift of most-drained nodes: first half vs second half.
        drained_nodes: set[int] = set()
        if energy_frac_by_node:
            items = sorted(energy_frac_by_node.items(), key=lambda kv: kv[1])
            k = max(1, len(items) // 4)
            drained_nodes = {n for n, _ in items[:k]}

        first_total = 0
        first_drained = 0
        second_total = 0
        second_drained = 0
        if routes_rows:
            ts_vals = [float(rr.get("_ts", 0.0)) for rr in routes_rows if "_ts" in rr]
            if ts_vals:
                t_mid = (min(ts_vals) + max(ts_vals)) / 2.0
                for rr in routes_rows:
                    if "_ts" not in rr:
                        continue
                    ts = float(rr["_ts"])  # type: ignore[arg-type]
                    try:
                        node = int(rr.get("nodeId", "-1"))
                    except ValueError:
                        continue
                    if ts <= t_mid:
                        first_total += 1
                        if node in drained_nodes:
                            first_drained += 1
                    else:
                        second_total += 1
                        if node in drained_nodes:
                            second_drained += 1

        share_first = (first_drained / first_total) if first_total > 0 else 0.0
        share_second = (second_drained / second_total) if second_total > 0 else 0.0
        delta_share = share_second - share_first

        # For these observables:
        # - corr(uses, remaining_energy) should be positive if routing prefers healthier nodes.
        # - drained-node share should decrease over time.
        direction_ok = (corr is not None and corr > 0.0) and (delta_share < 0.0)
        if direction_ok:
            direction_ok_count += 1
        if corr is not None:
            corr_values.append(corr)
        delta_values.append(delta_share)

        row = {
            "scenario": scen,
            "seed": seed,
            **basic,
            "corr_uses_vs_energyfrac": corr,
            "share_first": share_first,
            "share_second": share_second,
            "delta_share": delta_share,
            "direction_ok": direction_ok,
        }
        rows.append(row)

    consistency = (direction_ok_count / len(rows)) if rows else 0.0
    corr_m, corr_s, corr_ci = mean_std_ci95([c for c in corr_values if c is not None])
    d_m, d_s, d_ci = mean_std_ci95(delta_values)
    ci_corr_no_zero = corr_ci[0] > 0.0
    ci_delta_no_zero = d_ci[1] < 0.0
    claim_ok = consistency >= 0.70 and ci_corr_no_zero and ci_delta_no_zero

    by_scenario: Dict[str, List[Dict]] = {}
    for r in rows:
        by_scenario.setdefault(r["scenario"], []).append(r)

    md = [
        "# VALIDACION_PREPUB_I30_ENERGIA_RUTAS",
        "",
        "## Diseno ejecutado",
        "",
        (
            f"- Topologias: `random` (principal, areas {', '.join(f'{a}x{a}' for a in areas)})"
            + (" + `line` (control)." if args.i30_include_line else ".")
        ),
        f"- nEd=16, loads `{','.join(loads)}`, stopSec=7200, dataStartSec=60, seeds=1..10.",
        (
            f"- Pesos DV: link={args.dv_link_weight}, path={args.dv_path_weight}, "
            f"pathHop={args.dv_path_hop_weight}."
        ),
        f"- BatteryFullCapacityJ={args.i30_battery_full_capacity_j}.",
        "- Frecuencia de trafico definida solo por `trafficLoad` (sin override de periodo).",
        "",
        "## Resultados agregados",
        "",
        f"- consistency(direction_ok): `{consistency:.3f}`",
        f"- corr mean±std: `{corr_m:.5f} ± {corr_s:.5f}` ; CI95=`[{corr_ci[0]:.5f}, {corr_ci[1]:.5f}]`",
        f"- delta_share mean±std: `{d_m:.5f} ± {d_s:.5f}` ; CI95=`[{d_ci[0]:.5f}, {d_ci[1]:.5f}]`",
        "",
        f"- Criterio conservador (>=70% + CI95 sin 0): `{'PASS' if claim_ok else 'NO-CONCLUSIVO'}`",
        "",
        "## Detalle por escenario",
        "",
        "| scenario | runs | direction_ok_ratio | corr_mean | delta_share_mean | median_pdr |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for scen in sorted(by_scenario.keys()):
        vals = by_scenario[scen]
        ok_ratio = sum(1 for v in vals if v["direction_ok"]) / len(vals)
        corrs = [float(v["corr_uses_vs_energyfrac"]) for v in vals if v["corr_uses_vs_energyfrac"] is not None]
        corr_mean = statistics.mean(corrs) if corrs else 0.0
        d_mean = statistics.mean(float(v["delta_share"]) for v in vals)
        med_pdr = statistics.median(float(v["pdr"]) for v in vals)
        md.append(f"| {scen} | {len(vals)} | {ok_ratio:.3f} | {corr_mean:.5f} | {d_mean:.5f} | {med_pdr:.4f} |")

    md += [
        "",
        "## Conclusion",
        "",
        (
            "- Se habilita claim `energy-aware observable` (alcance conservador)."
            if claim_ok
            else "- Evidencia no concluyente: limitar/retirar claim fuerte de preferencia por energia."
        ),
        "",
    ]
    write_text(BASE_DIR / "VALIDACION_PREPUB_I30_ENERGIA_RUTAS.md", "\n".join(md))

    result = {
        "rows": rows,
        "consistency": consistency,
        "corr_mean": corr_m,
        "corr_std": corr_s,
        "corr_ci95": corr_ci,
        "delta_mean": d_m,
        "delta_std": d_s,
        "delta_ci95": d_ci,
        "claim_ok": claim_ok,
    }
    write_json(phase_dir / "fase2a_result.json", result)
    return result


def parse_routes_actions(path: Path) -> List[Dict]:
    rows = parse_csv_rows(path)
    out = []
    for r in rows:
        try:
            ts = float(r.get("timestamp(s)", "0"))
            node = int(r.get("nodeId", "-1"))
            dst = int(r.get("destination", "-1"))
            action = (r.get("action", "") or "").strip().upper()
            next_hop = int(r.get("nextHop", "-1"))
            score = float(r.get("score", "nan"))
        except ValueError:
            continue
        if node < 0 or dst < 0:
            continue
        out.append(
            {
                "ts": ts,
                "node": node,
                "dst": dst,
                "action": action,
                "nextHop": next_hop,
                "score": score,
            }
        )
    return out


def phase2b_i71(outdir: Path, args: argparse.Namespace) -> Dict:
    phase_dir = outdir / "f2b_i71_holddown"
    ensure_dir(phase_dir)

    spacing = args.line_spacing
    weight_opt = dv_weight_cli(args)
    line_rt_opt = f" --routeTimeoutFactor={args.i71_line_route_timeout_factor}"
    random_rt_opt = f" --routeTimeoutFactor={args.i71_random_route_timeout_factor}"
    run_defs: List[Tuple[str, str, int]] = []
    for seed in range(1, 11):
        run_defs.append(
            (
                "line_expiry_forced",
                (
                    f"mesh_dv_baseline --nEd=12 --nodePlacementMode=line --spacing={spacing} "
                    f"--trafficLoad=medium --beaconIntervalStableSec=700{weight_opt}{line_rt_opt} "
                    f"--rngRun={seed} --stopSec={args.i71_line_stop_sec} --dataStartSec=60 --enablePcap=false"
                ),
                seed,
            )
        )
    for seed in range(1, 11):
        run_defs.append(
            (
                "random_congestion_churn",
                (
                    f"mesh_dv_baseline --nEd=20 --nodePlacementMode=random --areaWidth={args.i71_random_area_m} "
                    f"--areaHeight={args.i71_random_area_m} "
                    f"--trafficLoad=high{weight_opt}{random_rt_opt} "
                    f"--rngRun={seed} --stopSec=3600 --dataStartSec=60 --enablePcap=false"
                ),
                seed,
            )
        )

    rows = []
    for scen, sim_args, seed in run_defs:
        run = run_simulation(sim_args, phase_dir / scen / f"seed_{seed}", resume=args.resume)
        basic = collect_basic_metrics(run)
        log_text = run.log_path.read_text(encoding="utf-8", errors="ignore") if run.log_path.exists() else ""

        hold_set = len(re.findall(r"Hold-down SET", log_text))
        hold_expired = len(re.findall(r"Hold-down EXPIRED", log_text))
        hold_ignore_log = len(re.findall(r"in hold-down", log_text))

        actions = parse_routes_actions(run.run_dir / "mesh_dv_metrics_routes.csv")
        poison_events = sum(1 for a in actions if a["action"] == "POISON")
        hold_set_effective = hold_set if hold_set > 0 else poison_events
        grouped: Dict[Tuple[int, int], List[Dict]] = {}
        for a in actions:
            grouped.setdefault((a["node"], a["dst"]), []).append(a)
        for lst in grouped.values():
            lst.sort(key=lambda x: x["ts"])

        # Flapping heuristic: repeated next-hop oscillation within 120s windows.
        # Ignore REFRESH because periodic same-path updates are expected in proactive DV.
        sustained_flap_pairs = 0
        total_pairs = len(grouped)
        for _, lst in grouped.items():
            upd = [e for e in lst if e["action"] in {"NEW", "UPDATE"} and e["nextHop"] >= 0]
            if len(upd) < 4:
                continue
            flap = False
            j = 0
            for i in range(len(upd)):
                while upd[i]["ts"] - upd[j]["ts"] > 120.0:
                    j += 1
                window = upd[j : i + 1]
                if len(window) < 4:
                    continue
                changes = 0
                prev_nh = window[0]["nextHop"]
                for e in window[1:]:
                    if e["nextHop"] != prev_nh:
                        changes += 1
                    prev_nh = e["nextHop"]
                if changes >= 3:
                    flap = True
                    break
            if flap:
                sustained_flap_pairs += 1

        flap_ratio = (sustained_flap_pairs / total_pairs) if total_pairs > 0 else 0.0
        oscillation_bounded = flap_ratio <= 0.10

        # Recovery after POISON for same (node,dst): first NEW/UPDATE delta.
        recovery_deltas = []
        for key, lst in grouped.items():
            poison_ts = [e["ts"] for e in lst if e["action"] == "POISON"]
            if not poison_ts:
                continue
            upd_ts = [e["ts"] for e in lst if e["action"] in {"NEW", "UPDATE"}]
            for pt in poison_ts:
                candidates = [u for u in upd_ts if u > pt]
                if candidates:
                    recovery_deltas.append(min(candidates) - pt)
        if recovery_deltas:
            med_recovery = statistics.median(recovery_deltas)
            p95_recovery = sorted(recovery_deltas)[max(0, int(math.ceil(0.95 * len(recovery_deltas))) - 1)]
            hold_effective = (sum(1 for d in recovery_deltas if d >= 30.0) / len(recovery_deltas)) >= 0.70
        else:
            med_recovery = 0.0
            p95_recovery = 0.0
            hold_effective = False

        scenario_ok = hold_set_effective > 0 and hold_effective and oscillation_bounded

        row = {
            "scenario": scen,
            "seed": seed,
            **basic,
            "hold_set": hold_set,
            "hold_set_effective": hold_set_effective,
            "hold_expired": hold_expired,
            "hold_ignore_log": hold_ignore_log,
            "flap_ratio": flap_ratio,
            "oscillation_bounded": oscillation_bounded,
            "recovery_samples": len(recovery_deltas),
            "med_recovery_s": med_recovery,
            "p95_recovery_s": p95_recovery,
            "hold_effective": hold_effective,
            "scenario_ok": scenario_ok,
        }
        rows.append(row)

    by_scenario: Dict[str, List[Dict]] = {}
    for r in rows:
        by_scenario.setdefault(r["scenario"], []).append(r)

    scen_pass = {}
    for scen, vals in by_scenario.items():
        ratio_ok = sum(1 for v in vals if v["scenario_ok"]) / len(vals)
        scen_pass[scen] = ratio_ok >= 0.70
    overall_ok = all(scen_pass.values()) if scen_pass else False

    md = [
        "# VALIDACION_PREPUB_I71_HOLDDOWN",
        "",
        "## Diseno ejecutado",
        "",
        (
            "- Escenario 1: expiracion forzada "
            f"(`line`, nEd=12, beaconIntervalStableSec=700, routeTimeoutFactor={args.i71_line_route_timeout_factor}, "
            f"seeds 1..10, stopSec={args.i71_line_stop_sec})."
        ),
        (
            "- Escenario 2: congestion/churn "
            f"(`random`, nEd=20, trafficLoad=high, routeTimeoutFactor={args.i71_random_route_timeout_factor}, "
            f"area={args.i71_random_area_m}m x {args.i71_random_area_m}m, seeds 1..10, stopSec=3600)."
        ),
        (
            f"- Pesos DV: link={args.dv_link_weight}, path={args.dv_path_weight}, "
            f"pathHop={args.dv_path_hop_weight}."
        ),
        "",
        "## Resultado por escenario",
        "",
        "| scenario | runs | ok_ratio | hold_set_avg | med_recovery_avg(s) | flap_ratio_avg | pass |",
        "|---|---:|---:|---:|---:|---:|:---:|",
    ]
    for scen in sorted(by_scenario.keys()):
        vals = by_scenario[scen]
        ok_ratio = sum(1 for v in vals if v["scenario_ok"]) / len(vals)
        hs = statistics.mean(float(v["hold_set_effective"]) for v in vals)
        mr = statistics.mean(float(v["med_recovery_s"]) for v in vals)
        fr = statistics.mean(float(v["flap_ratio"]) for v in vals)
        md.append(f"| {scen} | {len(vals)} | {ok_ratio:.3f} | {hs:.2f} | {mr:.2f} | {fr:.4f} | {'PASS' if scen_pass[scen] else 'FAIL'} |")

    md += [
        "",
        f"## Conclusion: `{'PASS' if overall_ok else 'PARCIAL/NO-CONCLUSIVO'}`",
        "",
        "- Criterio: bloqueo efectivo (recovery post-POISON >=30s en proporcion alta) y oscilacion acotada.",
        "",
    ]
    write_text(BASE_DIR / "VALIDACION_PREPUB_I71_HOLDDOWN.md", "\n".join(md))

    result = {"rows": rows, "scenario_pass": scen_pass, "overall_ok": overall_ok}
    write_json(phase_dir / "fase2b_result.json", result)
    return result


def phase2c_i84_i86(outdir: Path, args: argparse.Namespace) -> Dict:
    phase_dir = outdir / "f2c_i84_i86_scalability"
    ensure_dir(phase_dir)
    spacing = args.line_spacing

    # C1 screening
    c1_runs = []
    for n in [16, 32, 64, 96]:
        for seed in [1, 3, 5]:
            sim_args = (
                f"mesh_dv_baseline --nEd={n} --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 "
                "--trafficLoad=medium "
                f"--rngRun={seed} --stopSec=900 --dataStartSec=60 --enablePcap=false"
            )
            run = run_simulation(sim_args, phase_dir / "c1_random_medium" / f"n{n}" / f"seed_{seed}", resume=args.resume)
            metrics = collect_basic_metrics(run)
            c1_runs.append({"N": n, "seed": seed, "topo": "random", "load": "medium", **metrics})

    pass_n = []
    c1_by_n: Dict[int, List[Dict]] = {}
    for r in c1_runs:
        c1_by_n.setdefault(int(r["N"]), []).append(r)

    c1_eval = {}
    for n, vals in sorted(c1_by_n.items()):
        tx_all = [int(v["total_data_tx"]) for v in vals]
        pdr_med = statistics.median(float(v["pdr"]) for v in vals)
        cov_med = statistics.median(float(v["route_unique_dst"]) / max(1.0, n - 1) for v in vals)
        rt_med = statistics.median(float(v["elapsed_s"]) for v in vals)
        no_catastrophic = all(tx > 0 for tx in tx_all)
        n_pass = (
            no_catastrophic
            and pdr_med >= args.pdr_threshold
            and cov_med >= args.coverage_threshold
            and rt_med <= args.runtime_threshold_s
        )
        c1_eval[n] = {
            "no_catastrophic": no_catastrophic,
            "pdr_median": pdr_med,
            "coverage_median": cov_med,
            "runtime_median_s": rt_med,
            "pass": n_pass,
        }
        if n_pass:
            pass_n.append(n)

    # C2 only for N that pass C1
    c2_runs = []
    for n in pass_n:
        for seed in [1, 2, 3, 5, 8]:
            # random low
            sim_args_random_low = (
                f"mesh_dv_baseline --nEd={n} --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 "
                "--trafficLoad=low "
                f"--rngRun={seed} --stopSec=900 --dataStartSec=60 --enablePcap=false"
            )
            run = run_simulation(
                sim_args_random_low,
                phase_dir / "c2" / f"n{n}" / "random_low" / f"seed_{seed}",
                resume=args.resume,
            )
            c2_runs.append({"N": n, "seed": seed, "topo": "random", "load": "low", **collect_basic_metrics(run)})

            # line medium
            sim_args_line_medium = (
                f"mesh_dv_baseline --nEd={n} --nodePlacementMode=line --spacing={spacing} "
                "--trafficLoad=medium "
                f"--rngRun={seed} --stopSec=900 --dataStartSec=60 --enablePcap=false"
            )
            run = run_simulation(
                sim_args_line_medium,
                phase_dir / "c2" / f"n{n}" / "line_medium" / f"seed_{seed}",
                resume=args.resume,
            )
            c2_runs.append({"N": n, "seed": seed, "topo": "line", "load": "medium", **collect_basic_metrics(run)})

            # line low
            sim_args_line_low = (
                f"mesh_dv_baseline --nEd={n} --nodePlacementMode=line --spacing={spacing} "
                "--trafficLoad=low "
                f"--rngRun={seed} --stopSec=900 --dataStartSec=60 --enablePcap=false"
            )
            run = run_simulation(
                sim_args_line_low,
                phase_dir / "c2" / f"n{n}" / "line_low" / f"seed_{seed}",
                resume=args.resume,
            )
            c2_runs.append({"N": n, "seed": seed, "topo": "line", "load": "low", **collect_basic_metrics(run)})

    # Determine N_max_validado
    n_max_validado = 0
    c2_eval: Dict[int, Dict] = {}
    for n in pass_n:
        vals = [r for r in c2_runs if int(r["N"]) == n]
        if not vals:
            c2_eval[n] = {"pass": False}
            continue
        no_cat = all(int(v["total_data_tx"]) > 0 for v in vals)
        pdr_med = statistics.median(float(v["pdr"]) for v in vals)
        cov_med = statistics.median(float(v["route_unique_dst"]) / max(1.0, n - 1) for v in vals)
        rt_med = statistics.median(float(v["elapsed_s"]) for v in vals)
        ok = (
            no_cat
            and pdr_med >= args.pdr_threshold
            and cov_med >= args.coverage_threshold
            and rt_med <= args.runtime_threshold_s
        )
        c2_eval[n] = {
            "no_catastrophic": no_cat,
            "pdr_median": pdr_med,
            "coverage_median": cov_med,
            "runtime_median_s": rt_med,
            "pass": ok,
        }
        if ok:
            n_max_validado = max(n_max_validado, n)

    md = [
        "# VALIDACION_PREPUB_I84_I86_ESCALABILIDAD",
        "",
        "## C1 Screening (random, medium, seeds 1/3/5, stopSec=900)",
        "",
        "| N | no_catastrophic | pdr_median | coverage_median | runtime_median_s | pass |",
        "|---:|:---:|---:|---:|---:|:---:|",
    ]
    for n in sorted(c1_eval.keys()):
        e = c1_eval[n]
        md.append(
            f"| {n} | {'yes' if e['no_catastrophic'] else 'no'} | {e['pdr_median']:.4f} | "
            f"{e['coverage_median']:.4f} | {e['runtime_median_s']:.2f} | {'PASS' if e['pass'] else 'FAIL'} |"
        )

    md += [
        "",
        "## C2 Profundizacion (solo N que pasan C1)",
        "",
        "| N | no_catastrophic | pdr_median | coverage_median | runtime_median_s | pass |",
        "|---:|:---:|---:|---:|---:|:---:|",
    ]
    for n in sorted(c2_eval.keys()):
        e = c2_eval[n]
        md.append(
            f"| {n} | {'yes' if e.get('no_catastrophic') else 'no'} | {e.get('pdr_median', 0.0):.4f} | "
            f"{e.get('coverage_median', 0.0):.4f} | {e.get('runtime_median_s', 0.0):.2f} | {'PASS' if e.get('pass') else 'FAIL'} |"
        )

    md += [
        "",
        f"## Envolvente validada",
        "",
        f"- `N_max_validado = {n_max_validado}`",
        "- Este valor define el limite operativo para claims conservadores en el paper.",
        "",
    ]
    write_text(BASE_DIR / "VALIDACION_PREPUB_I84_I86_ESCALABILIDAD.md", "\n".join(md))

    result = {
        "c1_runs": c1_runs,
        "c1_eval": c1_eval,
        "c2_runs": c2_runs,
        "c2_eval": c2_eval,
        "n_max_validado": n_max_validado,
    }
    write_json(phase_dir / "fase2c_result.json", result)
    return result


def parse_dv_tx_map(log_text: str) -> Dict[Tuple[int, int], Dict[str, int]]:
    m: Dict[Tuple[int, int], Dict[str, int]] = {}
    # Example:
    # DVTRACE_TX time=... node=1 seq=2 entries=0 bytes=13 ...
    for line in log_text.splitlines():
        if "DVTRACE_TX" not in line:
            continue
        src_m = re.search(r"node=(\d+)", line)
        seq_m = re.search(r"seq=(\d+)", line)
        ent_m = re.search(r"entries=(\d+)", line)
        byt_m = re.search(r"bytes=(\d+)", line)
        if not (src_m and seq_m and ent_m and byt_m):
            continue
        src = int(src_m.group(1))
        seq = int(seq_m.group(1))
        m[(src, seq)] = {"entries": int(ent_m.group(1)), "bytes": int(byt_m.group(1))}
    return m


def parse_dv_rx_parse(log_text: str) -> List[Dict[str, int]]:
    out: List[Dict[str, int]] = []
    # Example:
    # DVTRACE_RX_PARSE time=.. node=.. src=.. seq=.. total=.. accepted=.. ... payloadBytes=..
    rx_re = re.compile(
        r"DVTRACE_RX_PARSE.*node=(\d+).*src=(\d+).*seq=(\d+).*total=(\d+).*accepted=(\d+).*payloadBytes=(\d+)"
    )
    for line in log_text.splitlines():
        m = rx_re.search(line)
        if not m:
            continue
        out.append(
            {
                "node": int(m.group(1)),
                "src": int(m.group(2)),
                "seq": int(m.group(3)),
                "total": int(m.group(4)),
                "accepted": int(m.group(5)),
                "payloadBytes": int(m.group(6)),
            }
        )
    return out


def phase2d_i59(outdir: Path, args: argparse.Namespace) -> Dict:
    phase_dir = outdir / "f2d_i59_payload_beacon"
    ensure_dir(phase_dir)

    run_defs: List[Tuple[str, str, int]] = []
    for n, area in [(6, 1200), (12, 2000)]:
        for seed in range(1, 11):
            scen = f"sparse_random_n{n}"
            sim_args = (
                f"mesh_dv_baseline --nEd={n} --nodePlacementMode=random --areaWidth={area} --areaHeight={area} "
                "--trafficLoad=medium "
                f"--rngRun={seed} --stopSec=300 --dataStartSec=60 --enablePcap=false"
            )
            run_defs.append((scen, sim_args, seed))

    rows = []
    for scen, sim_args, seed in run_defs:
        run = run_simulation(sim_args, phase_dir / scen / f"seed_{seed}", resume=args.resume)
        basic = collect_basic_metrics(run)
        log_text = run.log_path.read_text(encoding="utf-8", errors="ignore") if run.log_path.exists() else ""
        tx_map = parse_dv_tx_map(log_text)
        rx_list = parse_dv_rx_parse(log_text)

        suspicious = 0
        total_rx_parse = len(rx_list)
        suspicious_events: List[Tuple[int, float]] = []
        for rx in rx_list:
            key = (rx["src"], rx["seq"])
            tx = tx_map.get(key)
            if tx and tx["entries"] == 0 and rx["total"] > 0:
                suspicious += 1
                # store node and sequence for possible impact lookup
                suspicious_events.append((rx["node"], float(rx["seq"])))

        suspicious_ratio = (suspicious / total_rx_parse) if total_rx_parse > 0 else 0.0

        actions = parse_routes_actions(run.run_dir / "mesh_dv_metrics_routes.csv")
        impact_count = 0
        # approximate impact: any NEW/UPDATE within +5s in same node after suspicious parse line.
        # We do not have direct parse timestamp in suspicious_events here, so use conservative proxy:
        # if suspicious exists and there are NEW/UPDATE actions, count as impacted.
        if suspicious > 0:
            impact_count = sum(1 for a in actions if a["action"] in {"NEW", "UPDATE"})
        impact_ratio = (impact_count / max(1, len(actions))) if actions else 0.0

        row = {
            "scenario": scen,
            "seed": seed,
            **basic,
            "dv_rx_parse_total": total_rx_parse,
            "suspicious_parse_events": suspicious,
            "suspicious_ratio": suspicious_ratio,
            "route_action_count": len(actions),
            "impact_proxy_count": impact_count,
            "impact_proxy_ratio": impact_ratio,
        }
        rows.append(row)

    suspicious_ratio_all = statistics.mean(float(r["suspicious_ratio"]) for r in rows) if rows else 0.0
    impact_ratio_all = statistics.mean(float(r["impact_proxy_ratio"]) for r in rows) if rows else 0.0

    if suspicious_ratio_all < 0.01 and impact_ratio_all < 0.01:
        recommendation = "Mantener comportamiento actual y documentar limitacion."
    else:
        recommendation = (
            "Recomendar ajuste de politica beacon sin rutas (definir framing explicito de entradas)."
        )

    md = [
        "# VALIDACION_PREPUB_I59_PAYLOAD_BEACON",
        "",
        "## Diseno ejecutado",
        "",
        "- Escenarios de tablas pobres al inicio: random disperso nEd=6 y nEd=12, seeds 1..10.",
        "- stopSec=300, dataStartSec=60, trafficLoad=medium.",
        "",
        "## Resultado agregado",
        "",
        f"- suspicious_ratio promedio: `{suspicious_ratio_all:.5f}`",
        f"- impact_proxy_ratio promedio: `{impact_ratio_all:.5f}`",
        "",
        "## Recomendacion",
        "",
        f"- {recommendation}",
        "",
    ]
    write_text(BASE_DIR / "VALIDACION_PREPUB_I59_PAYLOAD_BEACON.md", "\n".join(md))

    result = {
        "rows": rows,
        "suspicious_ratio_avg": suspicious_ratio_all,
        "impact_proxy_ratio_avg": impact_ratio_all,
        "recommendation": recommendation,
    }
    write_json(phase_dir / "fase2d_result.json", result)
    return result


def phase3_consolidate(outdir: Path, results: Dict) -> Dict:
    f0 = results.get("f0", {})
    f1 = results.get("f1", {})
    a = results.get("f2a", {})
    b = results.get("f2b", {})
    c = results.get("f2c", {})
    d = results.get("f2d", {})

    status_rows = []

    # Blocks pending from plan
    status_rows.append(("I30 energia-rutas", "PASS" if a.get("claim_ok") else "NO-CONCLUSIVO"))
    status_rows.append(("I71 hold-down", "PASS" if b.get("overall_ok") else "PARCIAL"))
    nmax = int(c.get("n_max_validado", 0)) if c else 0
    status_rows.append(("I84/I85/I86 escalabilidad", "PASS" if nmax > 0 else "PARCIAL"))
    status_rows.append(("I59 beacon payload", "PASS" if "Mantener" in str(d.get("recommendation", "")) else "PARCIAL"))

    enabled_claims = []
    limited_claims = []

    if a.get("claim_ok"):
        enabled_claims.append("Evidencia de comportamiento energy-aware observable (alcance conservador).")
    else:
        limited_claims.append("Claim fuerte energy-aware no concluyente; reportar como observacion parcial.")

    if b.get("overall_ok"):
        enabled_claims.append("Robustez hold-down bajo escenarios de falla evaluados.")
    else:
        limited_claims.append("Robustez anti-loop/hold-down solo parcial o dependiente de escenario.")

    if nmax > 0:
        enabled_claims.append(f"Escalabilidad validada hasta N={nmax} dentro de envelope operativo medido.")
    else:
        limited_claims.append("No se habilita claim de escalabilidad amplia; limitar a escenarios chicos/medios.")

    if "Recomendar ajuste" in str(d.get("recommendation", "")):
        limited_claims.append("Política de beacon sin rutas requiere decisión de diseño antes de claim fuerte de serialización DV.")

    recommendation = (
        "publicable con alcance conservador"
        if f0.get("gate_f0_ok") and f1.get("gate_f1_ok") and len(limited_claims) <= 2
        else "requiere una iteracion mas"
    )

    md = [
        "# VALIDACION_PREPUB_CONSOLIDADO",
        "",
        "## Estado por bloque pendiente",
        "",
        "| Bloque | Estado |",
        "|---|---|",
    ]
    for name, st in status_rows:
        md.append(f"| {name} | {st} |")

    md += [
        "",
        "## Claims habilitados",
        "",
    ]
    if enabled_claims:
        md.extend([f"- {c_}" for c_ in enabled_claims])
    else:
        md.append("- Ninguno adicional habilitado en esta ronda.")

    md += [
        "",
        "## Claims limitados",
        "",
    ]
    if limited_claims:
        md.extend([f"- {c_}" for c_ in limited_claims])
    else:
        md.append("- Sin limitaciones adicionales detectadas en los bloques evaluados.")

    md += [
        "",
        "## Riesgos residuales",
        "",
        "- Resultados dependientes de semillas/escenarios; mantener trazabilidad completa por corrida.",
        "- No se realizaron cambios de código en esta fase (solo medición/validación).",
        "",
        f"## Recomendacion final: `{recommendation}`",
        "",
    ]
    write_text(BASE_DIR / "VALIDACION_PREPUB_CONSOLIDADO.md", "\n".join(md))

    # Critical findings document
    critical = []
    if not f0.get("gate_f0_ok", False):
        critical.append("F0 gate fail (build/smoke/baseline/reproducibilidad).")
    if not f1.get("gate_f1_ok", False):
        critical.append("F1 gate fail (viabilidad basica).")
    if recommendation != "publicable con alcance conservador":
        critical.append("Estado consolidado no alcanza recomendacion de publicacion conservadora.")

    if critical:
        hall = [
            "# HALLAZGOS_CRITICOS_PREPUB",
            "",
            "Se detectaron condiciones criticas durante la validacion prepublicacion:",
            "",
        ] + [f"- {c_}" for c_ in critical] + ["", "- No se modifico codigo en esta fase.", ""]
        write_text(BASE_DIR / "HALLAZGOS_CRITICOS_PREPUB.md", "\n".join(hall))

    # Proposed fixes (without applying them)
    fixes = [
        "# PROPUESTA_FIXES_PREPUB",
        "",
        "Priorizacion de fixes a considerar (no aplicados en esta fase):",
        "",
    ]
    if not a.get("claim_ok", False):
        fixes.append("- [ALTA] Reforzar acoplamiento terminos energeticos-ruteo o diseno experimental para hacer efecto observable.")
    if not b.get("overall_ok", False):
        fixes.append("- [ALTA] Revisar efectividad de hold-down bajo churn adversarial y su telemetria de bloqueo.")
    if "Recomendar ajuste" in str(d.get("recommendation", "")):
        fixes.append("- [ALTA] Definir framing explicito para beacon sin rutas (evitar parse ambiguo).")
    if nmax == 0:
        fixes.append("- [MEDIA-ALTA] Optimizacion de escalabilidad DV (cobertura de rutas y overhead de control).")
    if len(fixes) == 4:
        fixes.append("- [INFO] Sin fixes urgentes derivados de esta ronda.")
    fixes += ["", "- Nota: cualquier implementacion queda sujeta a tu aprobacion explicita.", ""]
    write_text(BASE_DIR / "PROPUESTA_FIXES_PREPUB.md", "\n".join(fixes))

    result = {
        "status_rows": status_rows,
        "enabled_claims": enabled_claims,
        "limited_claims": limited_claims,
        "recommendation": recommendation,
        "critical_findings": critical,
    }
    write_json(outdir / "f3_consolidated.json", result)
    return result


def copy_final_reports_to_outdir(outdir: Path) -> None:
    for name in [
        "VALIDACION_PREPUB_I30_ENERGIA_RUTAS.md",
        "VALIDACION_PREPUB_I71_HOLDDOWN.md",
        "VALIDACION_PREPUB_I84_I86_ESCALABILIDAD.md",
        "VALIDACION_PREPUB_I59_PAYLOAD_BEACON.md",
        "VALIDACION_PREPUB_CONSOLIDADO.md",
        "HALLAZGOS_CRITICOS_PREPUB.md",
        "PROPUESTA_FIXES_PREPUB.md",
    ]:
        src = BASE_DIR / name
        if src.exists():
            copy_if_exists(src, outdir / name)


def load_existing_phase_results(outdir: Path) -> Dict[str, Dict]:
    mapping = {
        "f0": outdir / "f0_preflight" / "fase0_result.json",
        "f1": outdir / "f1_gate" / "fase1_result.json",
        "f2a": outdir / "f2a_i30_energy_routes" / "fase2a_result.json",
        "f2b": outdir / "f2b_i71_holddown" / "fase2b_result.json",
        "f2c": outdir / "f2c_i84_i86_scalability" / "fase2c_result.json",
        "f2d": outdir / "f2d_i59_payload_beacon" / "fase2d_result.json",
        "f3": outdir / "f3_consolidated.json",
    }
    results: Dict[str, Dict] = {}
    for key, path in mapping.items():
        if path.exists():
            try:
                results[key] = read_json(path)
            except Exception:
                # Keep pipeline resilient: if a JSON is corrupted, just skip preload.
                pass
    return results


def main() -> int:
    args = parse_args()
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else (BASE_DIR / "validation_results" / f"prepub_validation_{ts}")
    ensure_dir(outdir)

    snapshot = extract_runtime_snapshot()
    write_manifest(outdir, snapshot)

    results: Dict[str, Dict] = load_existing_phase_results(outdir) if args.resume else {}

    run_all = args.only == "all"

    if run_all or args.only == "f0":
        results["f0"] = phase0(outdir, args)
    if run_all and not results.get("f0", {}).get("gate_f0_ok", True):
        # Early stop by plan gate.
        results["f1"] = {"gate_f1_ok": False, "skipped": True, "reason": "F0 gate failed"}
        phase3_consolidate(outdir, results)
        copy_final_reports_to_outdir(outdir)
        write_json(outdir / "results.json", results)
        print(f"[DONE] Prepub validation stopped after F0 gate failure. Output: {outdir}")
        return 0

    if run_all or args.only == "f1":
        results["f1"] = phase1(outdir, args)
    if run_all and not results.get("f1", {}).get("gate_f1_ok", True):
        # Stop long campaigns and emit critical findings.
        results["f2a"] = {"skipped": True, "reason": "F1 gate failed"}
        results["f2b"] = {"skipped": True, "reason": "F1 gate failed"}
        results["f2c"] = {"skipped": True, "reason": "F1 gate failed"}
        results["f2d"] = {"skipped": True, "reason": "F1 gate failed"}
        results["f3"] = phase3_consolidate(outdir, results)
        copy_final_reports_to_outdir(outdir)
        write_json(outdir / "results.json", results)
        print(f"[DONE] Prepub validation stopped after F1 gate failure. Output: {outdir}")
        return 0

    if run_all or args.only == "f2a":
        results["f2a"] = phase2a_i30(outdir, args)
    if run_all or args.only == "f2b":
        results["f2b"] = phase2b_i71(outdir, args)
    if run_all or args.only == "f2c":
        results["f2c"] = phase2c_i84_i86(outdir, args)
    if run_all or args.only == "f2d":
        results["f2d"] = phase2d_i59(outdir, args)

    if run_all or args.only == "f3":
        results["f3"] = phase3_consolidate(outdir, results)

    copy_final_reports_to_outdir(outdir)
    write_json(outdir / "results.json", results)
    print(f"[DONE] Prepub validation pipeline finished. Output: {outdir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise
