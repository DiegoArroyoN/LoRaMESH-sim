#!/usr/bin/env python3
"""
Pipeline reproducible de validacion paper-ready para LoRaMESH.

Genera:
  - VALIDACION_RANGOS.md
  - VALIDACION_REPRODUCIBILIDAD.md
  - VALIDACION_ENERGIA.md
  - VALIDACION_PAPER.md
  - JSONs canonicos por seed y simulacion larga
  - Copias de trazabilidad en validation_results/paper_validation_<timestamp>/
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


BASE_DIR = Path(__file__).resolve().parent  # .../scratch/LoRaMESH-sim
NS3_DIR = BASE_DIR.parents[1]  # .../ns-3-dev


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run paper-ready validation pipeline V1-V4.")
    parser.add_argument(
        "--build",
        dest="build",
        action="store_true",
        default=True,
        help="Run ./ns3 build before validations (default).",
    )
    parser.add_argument(
        "--no-build",
        dest="build",
        action="store_false",
        help="Skip ./ns3 build.",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default=None,
        help="Output directory for raw validation artifacts.",
    )
    parser.add_argument(
        "--v1-seeds",
        type=int,
        default=10,
        help="Number of seeds for V1 range statistical check (default: 10).",
    )
    parser.add_argument(
        "--v2-seeds",
        type=str,
        default="1 2 3 5 8",
        help='Seed list for V2 reproducibility, e.g. "1 2 3 5 8".',
    )
    parser.add_argument(
        "--only",
        type=str,
        choices=["v1", "v2", "v3", "v4", "all"],
        default="all",
        help="Run only selected phase or all phases.",
    )
    return parser.parse_args()


def parse_seed_list(seed_spec: str) -> List[int]:
    tokens = re.split(r"[,\s]+", seed_spec.strip())
    seeds = [int(t) for t in tokens if t]
    if not seeds:
        raise ValueError("v2 seed list is empty")
    return seeds


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_command(
    cmd: List[str],
    cwd: Path,
    log_path: Path | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess:
    if log_path:
        ensure_dir(log_path.parent)
        with open(log_path, "w", encoding="utf-8") as lf:
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
    if check and proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc


def run_ns3_sim(sim_args: str, log_path: Path, check: bool = True) -> int:
    cmd = ["./ns3", "run", sim_args]
    proc = run_command(cmd, cwd=NS3_DIR, log_path=log_path, check=False)
    if check and proc.returncode != 0:
        raise RuntimeError(f"Simulation failed ({proc.returncode}): {sim_args}")
    return proc.returncode


def read_json(path: Path) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_text(path: Path, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def copy_file(src: Path, dst: Path) -> None:
    ensure_dir(dst.parent)
    shutil.copy2(src, dst)


def mean_std(values: Iterable[float]) -> Tuple[float, float]:
    vals = list(values)
    if not vals:
        return 0.0, 0.0
    if len(vals) == 1:
        return vals[0], 0.0
    return statistics.mean(vals), statistics.stdev(vals)


def extract_runtime_parameters() -> Dict:
    baseline = (BASE_DIR / "mesh_dv_baseline.cc").read_text(encoding="utf-8")
    helper = (NS3_DIR / "src/loramesh/helper/loramesh-helper.cc").read_text(encoding="utf-8")
    phy = (NS3_DIR / "src/lorawan/model/simple-gateway-lora-phy.cc").read_text(encoding="utf-8")

    def grab(pattern: str, text: str, cast=float, default=None):
        m = re.search(pattern, text)
        if not m:
            if default is None:
                raise RuntimeError(f"Pattern not found: {pattern}")
            return default
        return cast(m.group(1))

    n = grab(r"double\s+pathLossExponent\s*=\s*([0-9.]+)\s*;", baseline)
    d0 = grab(r"double\s+referenceDistance\s*=\s*([0-9.]+)\s*;", baseline)
    pl0 = grab(r"double\s+referenceLossDb\s*=\s*([0-9.]+)\s*;", baseline)
    tx_current = grab(
        r'loraEnergyHelper\.Set\("TxCurrentA",\s*DoubleValue\(([0-9.]+)\)\);',
        baseline,
    )
    tx_power = grab(
        r'MeshLoraNetDevice::TxPowerDbm",\s*DoubleValue\(([0-9.]+)\)',
        baseline,
        default=14.0,
    )

    sens_match = re.search(r"sensitivity\[6\]\s*=\s*\{([^}]+)\};", phy, re.DOTALL)
    if not sens_match:
        raise RuntimeError("Could not parse SimpleGatewayLoraPhy sensitivity table")
    sens_values = [
        float(x.strip()) for x in sens_match.group(1).replace("\n", " ").split(",") if x.strip()
    ]
    if len(sens_values) != 6:
        raise RuntimeError(f"Unexpected sensitivity table length: {len(sens_values)}")

    sigma = None
    sigma_mul = re.search(
        r'Variance",\s*DoubleValue\(\s*([0-9.]+)\s*\*\s*([0-9.]+)\s*\)',
        helper,
    )
    if sigma_mul:
        sigma = math.sqrt(float(sigma_mul.group(1)) * float(sigma_mul.group(2)))
    else:
        sigma_var = re.search(r'Variance",\s*DoubleValue\(\s*([0-9.]+)\s*\)', helper)
        if sigma_var:
            sigma = math.sqrt(float(sigma_var.group(1)))
    if sigma is None:
        sigma = 3.57

    return {
        "path_loss_exponent": n,
        "reference_distance_m": d0,
        "reference_loss_db": pl0,
        "tx_power_dbm": tx_power,
        "tx_current_a": tx_current,
        "shadowing_sigma_db": sigma,
        "sensitivity_dbm": sens_values,
    }


def validate_summary_json_keys(summary: Dict) -> Tuple[bool, List[str]]:
    missing: List[str] = []
    required_paths = [
        ("pdr", "total_data_tx"),
        ("pdr", "delivered"),
        ("pdr", "pdr"),
        ("delay", "avg_s"),
        ("energy", "total_used_j"),
        ("energy", "min_remaining_frac"),
    ]
    for p1, p2 in required_paths:
        if p1 not in summary or p2 not in summary[p1]:
            missing.append(f"{p1}.{p2}")
    return len(missing) == 0, missing


def load_current_summary() -> Dict:
    path = NS3_DIR / "mesh_dv_summary.json"
    if not path.exists():
        raise RuntimeError("mesh_dv_summary.json not found after simulation")
    return read_json(path)


def preflight(args: argparse.Namespace, outdir: Path, runtime: Dict) -> Dict:
    logs = outdir / "preflight"
    ensure_dir(logs)
    result = {"build_ok": False, "smoke_ok": False, "runtime": runtime}

    if args.build:
        build_log = logs / "build.log"
        proc = run_command(["./ns3", "build"], cwd=NS3_DIR, log_path=build_log, check=False)
        result["build_ok"] = proc.returncode == 0
        result["build_rc"] = proc.returncode
        if proc.returncode != 0:
            raise RuntimeError(f"Build failed. See {build_log}")
    else:
        result["build_ok"] = True
        result["build_rc"] = 0

    smoke_args = (
        "mesh_dv_baseline --nEd=2 --nodePlacementMode=line --spacing=120 "
        "--stopSec=90 --dataStartSec=30 --enablePcap=false --rngRun=1"
    )
    smoke_log = logs / "smoke.log"
    smoke_rc = run_ns3_sim(smoke_args, smoke_log, check=False)
    result["smoke_rc"] = smoke_rc
    result["smoke_ok"] = smoke_rc == 0
    if smoke_rc != 0:
        raise RuntimeError(f"Smoke test failed. See {smoke_log}")

    summary = load_current_summary()
    ok_keys, missing = validate_summary_json_keys(summary)
    result["smoke_json_ok"] = ok_keys
    result["smoke_missing_keys"] = missing
    if not ok_keys:
        raise RuntimeError(f"Smoke JSON missing keys: {missing}")

    copy_file(NS3_DIR / "mesh_dv_summary.json", logs / "smoke_summary.json")
    return result


def run_v1(args: argparse.Namespace, outdir: Path, runtime: Dict) -> Dict:
    v1_dir = outdir / "v1"
    ensure_dir(v1_dir)

    n = runtime["path_loss_exponent"]
    d0 = runtime["reference_distance_m"]
    pl0 = runtime["reference_loss_db"]
    tx = runtime["tx_power_dbm"]
    sensitivities = runtime["sensitivity_dbm"]

    sf_rows = []
    dmax_by_sf = {}
    for idx, sf in enumerate(range(7, 13)):
        sens = sensitivities[idx]
        pl_max = tx - sens
        d_max = d0 * (10 ** ((pl_max - pl0) / (10 * n)))
        dmax_by_sf[sf] = d_max
        sf_rows.append(
            {
                "sf": sf,
                "sensitivity": sens,
                "pl_max": pl_max,
                "d_max_m": d_max,
            }
        )

    d90 = int(round(0.9 * dmax_by_sf[12]))
    d110 = int(round(1.1 * dmax_by_sf[12]))

    dist_results = {}
    for d in [d90, d110]:
        pdrs = []
        delivered_vals = []
        success_count = 0
        total_data_vals = []
        for seed in range(1, args.v1_seeds + 1):
            sim_args = (
                "mesh_dv_baseline --nEd=2 --nodePlacementMode=line "
                f"--spacing={d} --stopSec=240 --dataStartSec=60 --enablePcap=false --rngRun={seed}"
            )
            log_file = v1_dir / f"run_d{d}_seed{seed}.log"
            run_ns3_sim(sim_args, log_file, check=True)
            summary = load_current_summary()
            ok_keys, missing = validate_summary_json_keys(summary)
            if not ok_keys:
                raise RuntimeError(f"V1 summary missing keys {missing} for d={d}, seed={seed}")
            summary_copy = v1_dir / f"mesh_dv_summary_line_d{d}_seed{seed}.json"
            copy_file(NS3_DIR / "mesh_dv_summary.json", summary_copy)

            pdr = float(summary["pdr"]["pdr"])
            delivered = int(summary["pdr"]["delivered"])
            total_data_tx = int(summary["pdr"]["total_data_tx"])
            pdrs.append(pdr)
            delivered_vals.append(delivered)
            total_data_vals.append(total_data_tx)
            if delivered > 0:
                success_count += 1

        dist_results[d] = {
            "success_rate": success_count / float(args.v1_seeds),
            "avg_pdr": statistics.mean(pdrs) if pdrs else 0.0,
            "avg_delivered": statistics.mean(delivered_vals) if delivered_vals else 0.0,
            "avg_total_data_tx": statistics.mean(total_data_vals) if total_data_vals else 0.0,
        }

    criterion_ok = (
        dist_results[d90]["avg_pdr"] > dist_results[d110]["avg_pdr"]
        and dist_results[d90]["success_rate"] >= dist_results[d110]["success_rate"]
    )

    md = []
    md.append("# VALIDACION_RANGOS")
    md.append("")
    md.append("## Alcance teorico por SF (parametros reales implementados)")
    md.append("")
    md.append(
        f"- Modelo: `PL(d)=PL(d0)+10*n*log10(d/d0)` con `n={n}`, `d0={d0}m`, `PL(d0)={pl0} dB`"
    )
    md.append(
        f"- Potencia TX usada para calculo de rango: `{tx} dBm` (presupuesto de enlace nominal)"
    )
    md.append(f"- Sensibilidad (SF7..SF12): `{sensitivities}` dBm")
    md.append("")
    md.append("| SF | Sensibilidad (dBm) | PL_max (dB) | d_max teorico (m) |")
    md.append("|---:|-------------------:|------------:|-------------------:|")
    for row in sf_rows:
        md.append(
            f"| {row['sf']} | {row['sensitivity']:.1f} | {row['pl_max']:.2f} | {row['d_max_m']:.2f} |"
        )
    md.append("")
    md.append("## Verificacion estadistica en simulacion (sin forzar SF)")
    md.append("")
    md.append(
        f"- Distancias evaluadas: `d90={d90}m` (0.9*d_max_SF12) y `d110={d110}m` (1.1*d_max_SF12)"
    )
    md.append(f"- Seeds por distancia: `{args.v1_seeds}`")
    md.append("")
    md.append("| Distancia | success_rate (delivered>0) | avg_pdr | avg_delivered | avg_total_data_tx |")
    md.append("|---------:|-----------------------------:|--------:|--------------:|------------------:|")
    for d in [d90, d110]:
        r = dist_results[d]
        md.append(
            f"| {d} m | {r['success_rate']:.3f} | {r['avg_pdr']:.4f} | {r['avg_delivered']:.3f} | {r['avg_total_data_tx']:.3f} |"
        )
    md.append("")
    md.append("## Criterio V1")
    md.append("")
    md.append(
        f"- Regla: `avg_pdr(d90) > avg_pdr(d110)` y `success_rate(d90) >= success_rate(d110)`"
    )
    md.append(f"- Resultado: `{'PASS' if criterion_ok else 'FAIL'}`")
    md.append("")
    md.append("## Limitacion metodologica")
    md.append("")
    md.append(
        "- En esta validacion no se fuerza SF fijo por CLI; se mide alcance efectivo del stack actual."
    )
    md.append(
        "- Por shadowing y seleccion probabilistica de SF, el comportamiento es estadistico, no determinista."
    )
    md.append("")

    write_text(BASE_DIR / "VALIDACION_RANGOS.md", "\n".join(md))

    return {
        "sf_rows": sf_rows,
        "dmax_by_sf": dmax_by_sf,
        "d90": d90,
        "d110": d110,
        "dist_results": dist_results,
        "criterion_ok": criterion_ok,
    }


def choose_operational_geometry(outdir: Path) -> Dict:
    pilot_dir = outdir / "v2" / "pilot"
    ensure_dir(pilot_dir)

    line_candidates = [80, 100, 120, 140, 160]
    line_rows = []
    for spacing in line_candidates:
        sim_args = (
            "mesh_dv_baseline --nEd=6 --nodePlacementMode=line "
            f"--spacing={spacing} --stopSec=300 --dataStartSec=60 --enablePcap=false --rngRun=3"
        )
        log_file = pilot_dir / f"pilot_line_spacing{spacing}.log"
        run_ns3_sim(sim_args, log_file, check=True)
        summary = load_current_summary()
        pdr = float(summary["pdr"]["pdr"])
        delay = float(summary["delay"]["avg_s"])
        line_rows.append({"spacing": spacing, "pdr": pdr, "delay": delay})
        copy_file(NS3_DIR / "mesh_dv_summary.json", pilot_dir / f"pilot_line_spacing{spacing}.json")

    line_rows_sorted = sorted(line_rows, key=lambda x: (-x["pdr"], x["delay"]))
    spacing_line = int(line_rows_sorted[0]["spacing"])

    a0 = spacing_line * 5
    random_candidates = sorted({int(round(0.75 * a0)), int(round(1.0 * a0)), int(round(1.25 * a0))})
    random_rows = []
    for area in random_candidates:
        sim_args = (
            "mesh_dv_baseline --nEd=6 --nodePlacementMode=random "
            f"--areaWidth={area} --areaHeight={area} --stopSec=300 --dataStartSec=60 "
            "--enablePcap=false --rngRun=3"
        )
        log_file = pilot_dir / f"pilot_random_area{area}.log"
        run_ns3_sim(sim_args, log_file, check=True)
        summary = load_current_summary()
        pdr = float(summary["pdr"]["pdr"])
        delay = float(summary["delay"]["avg_s"])
        random_rows.append({"area": area, "pdr": pdr, "delay": delay})
        copy_file(NS3_DIR / "mesh_dv_summary.json", pilot_dir / f"pilot_random_area{area}.json")

    random_rows_sorted = sorted(random_rows, key=lambda x: (-x["pdr"], x["delay"]))
    area_random = int(random_rows_sorted[0]["area"])

    return {
        "line_candidates": line_rows,
        "random_candidates": random_rows,
        "spacing_line": spacing_line,
        "a0": a0,
        "area_random": area_random,
    }


def run_v2(args: argparse.Namespace, outdir: Path, geom: Dict) -> Dict:
    v2_dir = outdir / "v2"
    ensure_dir(v2_dir)

    seeds = parse_seed_list(args.v2_seeds)
    spacing_line = geom["spacing_line"]
    area_random = geom["area_random"]

    runs = {"line": [], "random": []}
    for topo in ["line", "random"]:
        for seed in seeds:
            if topo == "line":
                sim_args = (
                    "mesh_dv_baseline --nEd=6 --nodePlacementMode=line "
                    f"--spacing={spacing_line} --stopSec=600 --dataStartSec=60 "
                    f"--enablePcap=false --rngRun={seed}"
                )
            else:
                sim_args = (
                    "mesh_dv_baseline --nEd=6 --nodePlacementMode=random "
                    f"--areaWidth={area_random} --areaHeight={area_random} --stopSec=600 "
                    f"--dataStartSec=60 --enablePcap=false --rngRun={seed}"
                )

            log_file = v2_dir / f"run_{topo}_seed{seed}.log"
            run_ns3_sim(sim_args, log_file, check=True)
            summary = load_current_summary()
            ok_keys, missing = validate_summary_json_keys(summary)
            if not ok_keys:
                raise RuntimeError(
                    f"V2 summary missing keys {missing} for topo={topo}, seed={seed}"
                )

            dst_summary = v2_dir / f"mesh_dv_summary_{topo}_seed{seed}.json"
            copy_file(NS3_DIR / "mesh_dv_summary.json", dst_summary)

            if topo == "line":
                canonical = BASE_DIR / f"mesh_dv_summary_seed{seed}.json"
                copy_file(NS3_DIR / "mesh_dv_summary.json", canonical)

            run_row = {
                "seed": seed,
                "pdr": float(summary["pdr"]["pdr"]),
                "avg_delay": float(summary["delay"]["avg_s"]),
                "total_used_j": float(summary["energy"]["total_used_j"]),
                "total_data_tx": int(summary["pdr"]["total_data_tx"]),
                "delivered": int(summary["pdr"]["delivered"]),
            }
            runs[topo].append(run_row)

    stats = {}
    for topo in ["line", "random"]:
        pdr_m, pdr_s = mean_std([r["pdr"] for r in runs[topo]])
        delay_m, delay_s = mean_std([r["avg_delay"] for r in runs[topo]])
        energy_m, energy_s = mean_std([r["total_used_j"] for r in runs[topo]])
        tx_m, tx_s = mean_std([float(r["total_data_tx"]) for r in runs[topo]])
        deliv_m, deliv_s = mean_std([float(r["delivered"]) for r in runs[topo]])
        catastrophic = any(r["total_data_tx"] <= 0 for r in runs[topo])
        stats[topo] = {
            "pdr_mean": pdr_m,
            "pdr_std": pdr_s,
            "delay_mean": delay_m,
            "delay_std": delay_s,
            "energy_mean": energy_m,
            "energy_std": energy_s,
            "tx_mean": tx_m,
            "tx_std": tx_s,
            "delivered_mean": deliv_m,
            "delivered_std": deliv_s,
            "catastrophic_seed": catastrophic,
        }

    md = []
    md.append("# VALIDACION_REPRODUCIBILIDAD")
    md.append("")
    md.append("## Seleccion de geometria operativa (pilot seed=3)")
    md.append("")
    md.append("### Topologia line")
    md.append("")
    md.append("| spacing (m) | pdr | avg_delay (s) |")
    md.append("|------------:|----:|--------------:|")
    for row in sorted(geom["line_candidates"], key=lambda x: x["spacing"]):
        md.append(f"| {row['spacing']} | {row['pdr']:.4f} | {row['delay']:.6f} |")
    md.append("")
    md.append(f"- Seleccionado `SPACING_LINE = {geom['spacing_line']} m`")
    md.append("")
    md.append("### Topologia random")
    md.append("")
    md.append(f"- `A0 = SPACING_LINE*(nEd-1) = {geom['a0']} m`")
    md.append("| area (m x m) | pdr | avg_delay (s) |")
    md.append("|-------------:|----:|--------------:|")
    for row in sorted(geom["random_candidates"], key=lambda x: x["area"]):
        md.append(f"| {row['area']} | {row['pdr']:.4f} | {row['delay']:.6f} |")
    md.append("")
    md.append(f"- Seleccionado `AREA_RANDOM = {geom['area_random']} m`")
    md.append("")
    md.append("## Corridas por seed")
    md.append("")
    for topo in ["line", "random"]:
        md.append(f"### {topo}")
        md.append("")
        md.append("| seed | pdr | avg_delay (s) | total_used_j | total_data_tx | delivered |")
        md.append("|-----:|----:|--------------:|-------------:|--------------:|----------:|")
        for r in runs[topo]:
            md.append(
                f"| {r['seed']} | {r['pdr']:.4f} | {r['avg_delay']:.6f} | {r['total_used_j']:.4f} | {r['total_data_tx']} | {r['delivered']} |"
            )
        md.append("")

    md.append("## Estadistica (media y desviacion estandar)")
    md.append("")
    md.append(
        "| Topologia | pdr mean±std | delay mean±std (s) | energy mean±std (J) | total_data_tx mean±std | delivered mean±std |"
    )
    md.append(
        "|----------|--------------|--------------------|---------------------|------------------------|--------------------|"
    )
    for topo in ["line", "random"]:
        s = stats[topo]
        md.append(
            f"| {topo} | {s['pdr_mean']:.4f} ± {s['pdr_std']:.4f} | {s['delay_mean']:.6f} ± {s['delay_std']:.6f} | {s['energy_mean']:.4f} ± {s['energy_std']:.4f} | {s['tx_mean']:.2f} ± {s['tx_std']:.2f} | {s['delivered_mean']:.2f} ± {s['delivered_std']:.2f} |"
        )
    md.append("")
    md.append("## Criterio de seeds catastroficas")
    md.append("")
    md.append(
        f"- Line: `{'FAIL' if stats['line']['catastrophic_seed'] else 'PASS'}` (`total_data_tx > 0` en todas las seeds)"
    )
    md.append(
        f"- Random: `{'FAIL' if stats['random']['catastrophic_seed'] else 'PASS'}` (`total_data_tx > 0` en todas las seeds)"
    )
    md.append("")

    write_text(BASE_DIR / "VALIDACION_REPRODUCIBILIDAD.md", "\n".join(md))
    return {"seeds": seeds, "runs": runs, "stats": stats}


def run_v3(outdir: Path, geom: Dict) -> Dict:
    v3_dir = outdir / "v3"
    ensure_dir(v3_dir)

    spacing_line = geom["spacing_line"]
    area_random = geom["area_random"]

    scenarios = {
        "line": (
            "mesh_dv_baseline --nEd=6 --nodePlacementMode=line "
            f"--spacing={spacing_line} --stopSec=3600 --dataStartSec=60 --enablePcap=false --rngRun=42"
        ),
        "random": (
            "mesh_dv_baseline --nEd=6 --nodePlacementMode=random "
            f"--areaWidth={area_random} --areaHeight={area_random} --stopSec=3600 --dataStartSec=60 "
            "--enablePcap=false --rngRun=42"
        ),
    }

    rows = {}
    for topo, sim_args in scenarios.items():
        log_file = v3_dir / f"run_long_{topo}.log"
        run_ns3_sim(sim_args, log_file, check=True)
        summary = load_current_summary()
        ok_keys, missing = validate_summary_json_keys(summary)
        if not ok_keys:
            raise RuntimeError(f"V3 summary missing keys {missing} for topo={topo}")

        out_json = v3_dir / f"mesh_dv_summary_long_{topo}.json"
        copy_file(NS3_DIR / "mesh_dv_summary.json", out_json)
        copy_file(NS3_DIR / "mesh_dv_summary.json", BASE_DIR / f"mesh_dv_summary_long_{topo}.json")

        if topo == "line":
            copy_file(NS3_DIR / "mesh_dv_summary.json", BASE_DIR / "mesh_dv_summary_long.json")

        total_used_j = float(summary["energy"]["total_used_j"])
        min_remaining = float(summary["energy"]["min_remaining_frac"])
        used_frac_per_node = (total_used_j / 6.0) / 38880.0
        used_percent_per_node = used_frac_per_node * 100.0
        in_expected = 0.01 <= used_percent_per_node <= 0.1
        rows[topo] = {
            "total_used_j": total_used_j,
            "min_remaining_frac": min_remaining,
            "used_frac_per_node": used_frac_per_node,
            "used_percent_per_node": used_percent_per_node,
            "in_expected_range": in_expected,
            "pdr": float(summary["pdr"]["pdr"]),
            "total_data_tx": int(summary["pdr"]["total_data_tx"]),
            "delivered": int(summary["pdr"]["delivered"]),
        }

    md = []
    md.append("# VALIDACION_ENERGIA")
    md.append("")
    md.append("## Configuracion simulacion larga")
    md.append("")
    md.append("- Duracion: `3600 s`")
    md.append("- Nodos: `6`")
    md.append("- Seed: `42`")
    md.append(f"- Topologia line: `spacing={spacing_line} m`")
    md.append(f"- Topologia random: `area={area_random} x {area_random} m`")
    md.append("")
    md.append("## Referencia teorica")
    md.append("")
    md.append("- Capacidad por nodo: `38880 J` (10.8 Wh @ 3.6V)")
    md.append("- Rango esperado de consumo por hora: `0.01% - 0.1%` por nodo")
    md.append("")
    md.append("## Resultado simulado")
    md.append("")
    md.append(
        "| Topologia | total_used_j (red) | min_remaining_frac | used_frac_per_node | used_percent_per_node | rango esperado |"
    )
    md.append(
        "|----------|--------------------:|-------------------:|-------------------:|----------------------:|---------------|"
    )
    for topo in ["line", "random"]:
        r = rows[topo]
        md.append(
            f"| {topo} | {r['total_used_j']:.4f} | {r['min_remaining_frac']:.6f} | {r['used_frac_per_node']:.8f} | {r['used_percent_per_node']:.4f}% | {'PASS' if r['in_expected_range'] else 'FAIL'} |"
        )
    md.append("")
    md.append("## Comparacion teorica vs simulada")
    md.append("")
    for topo in ["line", "random"]:
        r = rows[topo]
        md.append(
            f"- {topo}: consumo por nodo `{r['used_percent_per_node']:.4f}%` en 1 hora -> "
            f"{'dentro' if r['in_expected_range'] else 'fuera'} del rango esperado."
        )
    md.append("")

    write_text(BASE_DIR / "VALIDACION_ENERGIA.md", "\n".join(md))
    return rows


def run_no_regression(outdir: Path) -> Dict:
    verif_dir = outdir / "verification"
    ensure_dir(verif_dir)
    sim_args = (
        "mesh_dv_baseline --nEd=5 --spacing=450 --stopSec=300 "
        "--dataStartSec=60 --enablePcap=false --rngRun=3"
    )
    log_file = verif_dir / "no_regression.log"
    rc = run_ns3_sim(sim_args, log_file, check=False)
    summary = None
    if rc == 0 and (NS3_DIR / "mesh_dv_summary.json").exists():
        summary = load_current_summary()
        copy_file(NS3_DIR / "mesh_dv_summary.json", verif_dir / "no_regression_summary.json")
    return {"rc": rc, "summary_present": summary is not None}


def write_v4_consolidated(
    outdir: Path,
    pre: Dict,
    v1: Dict | None,
    v2: Dict | None,
    v3: Dict | None,
    no_reg: Dict,
    runtime: Dict,
) -> Dict:
    checks = {
        "build_ok": bool(pre.get("build_ok", False)),
        "smoke_ok": bool(pre.get("smoke_ok", False)) and bool(pre.get("smoke_json_ok", False)),
        "v1_trend_ok": bool(v1 and v1.get("criterion_ok", False)),
        "v2_no_catastrophic_line": bool(v2 and not v2["stats"]["line"]["catastrophic_seed"]),
        "v2_no_catastrophic_random": bool(v2 and not v2["stats"]["random"]["catastrophic_seed"]),
        "v3_energy_line_ok": bool(v3 and v3["line"]["in_expected_range"]),
        "v3_energy_random_ok": bool(v3 and v3["random"]["in_expected_range"]),
        "no_regression_ok": no_reg.get("rc", 1) == 0,
    }
    paper_ready = all(checks.values())

    md = []
    md.append("# VALIDACION_PAPER")
    md.append("")
    md.append("## 1) Parametros reales del simulador")
    md.append("")
    md.append(f"- Path loss exponent: `{runtime['path_loss_exponent']}`")
    md.append(f"- Reference distance: `{runtime['reference_distance_m']} m`")
    md.append(f"- Reference loss: `{runtime['reference_loss_db']} dB`")
    md.append(f"- Shadowing sigma: `{runtime['shadowing_sigma_db']:.2f} dB`")
    md.append(f"- Sensibilidad SF7..SF12: `{runtime['sensitivity_dbm']}` dBm")
    md.append(f"- TxPowerDbm (default net device): `{runtime['tx_power_dbm']} dBm`")
    md.append(f"- TxCurrentA en baseline: `{runtime['tx_current_a']} A`")
    md.append("")
    md.append("## 2) Rango por SF (V1)")
    md.append("")
    if v1:
        md.append(f"- d90: `{v1['d90']} m`")
        md.append(f"- d110: `{v1['d110']} m`")
        md.append(f"- Criterio de tendencia: `{'PASS' if v1['criterion_ok'] else 'FAIL'}`")
    else:
        md.append("- No ejecutado en esta corrida (`--only` excluyo V1).")
    md.append("")
    md.append("## 3) Reproducibilidad (V2)")
    md.append("")
    if v2:
        line = v2["stats"]["line"]
        rnd = v2["stats"]["random"]
        md.append(
            f"- Line pdr mean±std: `{line['pdr_mean']:.4f} ± {line['pdr_std']:.4f}`; catastrophic seeds: "
            f"`{'YES' if line['catastrophic_seed'] else 'NO'}`"
        )
        md.append(
            f"- Random pdr mean±std: `{rnd['pdr_mean']:.4f} ± {rnd['pdr_std']:.4f}`; catastrophic seeds: "
            f"`{'YES' if rnd['catastrophic_seed'] else 'NO'}`"
        )
    else:
        md.append("- No ejecutado en esta corrida (`--only` excluyo V2).")
    md.append("")
    md.append("## 4) Consumo energetico (V3)")
    md.append("")
    if v3:
        md.append(
            f"- Line used_percent_per_node: `{v3['line']['used_percent_per_node']:.4f}%` "
            f"-> `{'PASS' if v3['line']['in_expected_range'] else 'FAIL'}`"
        )
        md.append(
            f"- Random used_percent_per_node: `{v3['random']['used_percent_per_node']:.4f}%` "
            f"-> `{'PASS' if v3['random']['in_expected_range'] else 'FAIL'}`"
        )
    else:
        md.append("- No ejecutado en esta corrida (`--only` excluyo V3).")
    md.append("")
    md.append("## 5) Checklist final")
    md.append("")
    for k, v in checks.items():
        md.append(f"- {k}: `{'PASS' if v else 'FAIL'}`")
    md.append("")
    md.append(f"## Conclusion")
    md.append("")
    md.append(f"- Estado global paper-ready: `{'PASS' if paper_ready else 'PENDIENTES'}`")
    if not paper_ready:
        md.append("- Pendientes:")
        for k, v in checks.items():
            if not v:
                md.append(f"  - {k}")
    md.append("")
    md.append("## Trazabilidad")
    md.append("")
    md.append(f"- Artefactos crudos: `{outdir}`")
    md.append("")

    write_text(BASE_DIR / "VALIDACION_PAPER.md", "\n".join(md))
    return {"checks": checks, "paper_ready": paper_ready}


def copy_reports_to_outdir(outdir: Path) -> None:
    for name in [
        "VALIDACION_RANGOS.md",
        "VALIDACION_REPRODUCIBILIDAD.md",
        "VALIDACION_ENERGIA.md",
        "VALIDACION_PAPER.md",
    ]:
        src = BASE_DIR / name
        if src.exists():
            copy_file(src, outdir / name)


def main() -> int:
    args = parse_args()
    if args.v1_seeds < 1:
        raise RuntimeError("--v1-seeds must be >= 1")

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else BASE_DIR / "validation_results" / f"paper_validation_{timestamp}"
    ensure_dir(outdir)

    runtime = extract_runtime_parameters()
    pre = preflight(args, outdir, runtime)

    v1_result = None
    v2_result = None
    v3_result = None
    geom = None

    if args.only in ("v1", "all"):
        v1_result = run_v1(args, outdir, runtime)

    if args.only in ("v2", "v3", "all"):
        geom = choose_operational_geometry(outdir)

    if args.only in ("v2", "all"):
        if geom is None:
            geom = choose_operational_geometry(outdir)
        v2_result = run_v2(args, outdir, geom)

    if args.only in ("v3", "all"):
        if geom is None:
            geom = choose_operational_geometry(outdir)
        v3_result = run_v3(outdir, geom)

    no_reg = run_no_regression(outdir)

    if args.only in ("v4", "all"):
        write_v4_consolidated(outdir, pre, v1_result, v2_result, v3_result, no_reg, runtime)

    copy_reports_to_outdir(outdir)

    results = {
        "outdir": str(outdir),
        "runtime": runtime,
        "preflight": pre,
        "v1": v1_result,
        "geometry": geom,
        "v2": v2_result,
        "v3": v3_result,
        "no_regression": no_reg,
        "args": {
            "build": args.build,
            "v1_seeds": args.v1_seeds,
            "v2_seeds": args.v2_seeds,
            "only": args.only,
        },
    }
    write_text(outdir / "results.json", json.dumps(results, indent=2))

    print(f"[OK] Validation pipeline finished. Output: {outdir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise
