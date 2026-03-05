#!/usr/bin/env python3
"""
Run multi-scenario PDR diagnosis campaigns and attribute packet loss causes.

Outputs:
  - validation_results/pdr_diagnosis_<timestamp>/
  - DIAGNOSTICO_PDR.md (copied to scratch/LoRaMESH-sim root)
"""

from __future__ import annotations

import datetime as dt
import json
import re
import shutil
import statistics
import subprocess
from pathlib import Path
from typing import Dict, List


BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]  # .../ns-3-dev


def run_cmd(cmd: List[str], cwd: Path, log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as lf:
        p = subprocess.run(cmd, cwd=str(cwd), stdout=lf, stderr=subprocess.STDOUT, text=True, check=False)
    return p.returncode


def read_json(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_log_counts(log_text: str) -> Dict[str, int]:
    no_route = len(re.findall(r"FWDTRACE DATA_NOROUTE|FWDTRACE drop_noroute", log_text))
    duty_data_drop = len(re.findall(r"FWDTRACE drop_duty", log_text))
    duty_beacon_drop = len(re.findall(r"BEACON DROP", log_text))
    interference_drop = len(re.findall(r"PHY_INTERFERENCE_DROP", log_text))
    ttl_drop = len(re.findall(r"FWDTRACE drop_ttl", log_text))
    duplicate_drop = len(re.findall(r"FWDTRACE drop_seen_once|FWDTRACE drop_dup_sink_delivered", log_text))
    cad_total = len(re.findall(r"CAD_RESULT detail:", log_text))
    cad_busy = len(re.findall(r"CAD_RESULT detail:.*busy=1", log_text))
    return {
        "no_route_drop": no_route,
        "duty_data_drop": duty_data_drop,
        "duty_beacon_drop": duty_beacon_drop,
        "phy_interference_drop": interference_drop,
        "ttl_drop": ttl_drop,
        "duplicate_drop": duplicate_drop,
        "cad_total": cad_total,
        "cad_busy": cad_busy,
    }


def dominant_cause(row: Dict) -> str:
    tx = max(1, int(row["total_data_tx"]))
    candidates = {
        "no_route": row["no_route_drop"] / tx,
        "interference_phy": row["phy_interference_drop"] / tx,
        "duty_data": row["duty_data_drop"] / tx,
        "ttl": row["ttl_drop"] / tx,
        "duplicate": row["duplicate_drop"] / tx,
    }
    return max(candidates.items(), key=lambda kv: kv[1])[0]


def scenario_matrix() -> List[Dict[str, str]]:
    base = "--nEd=12 --enablePcap=false --stopSec=300"
    return [
        {
            "name": "baseline_random_medium",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 --trafficLoad=medium --dataStartSec=90 --enableDuty=true --enableCsma=true",
        },
        {
            "name": "random_medium_no_duty",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 --trafficLoad=medium --dataStartSec=90 --enableDuty=false --enableCsma=true",
        },
        {
            "name": "random_medium_no_csma",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 --trafficLoad=medium --dataStartSec=90 --enableDuty=true --enableCsma=false",
        },
        {
            "name": "random_medium_dataStart_30",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 --trafficLoad=medium --dataStartSec=30 --enableDuty=true --enableCsma=true",
        },
        {
            "name": "random_medium_dataStart_150",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 --trafficLoad=medium --dataStartSec=150 --enableDuty=true --enableCsma=true",
        },
        {
            "name": "random_low",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 --trafficLoad=low --dataStartSec=90 --enableDuty=true --enableCsma=true",
        },
        {
            "name": "random_high",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=random --areaWidth=1000 --areaHeight=1000 --trafficLoad=high --dataStartSec=90 --enableDuty=true --enableCsma=true",
        },
        {
            "name": "line_spacing_100_medium",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=line --spacing=100 --trafficLoad=medium --dataStartSec=90 --enableDuty=true --enableCsma=true",
        },
        {
            "name": "line_spacing_160_medium",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=line --spacing=160 --trafficLoad=medium --dataStartSec=90 --enableDuty=true --enableCsma=true",
        },
        {
            "name": "random_dense_600_medium",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=random --areaWidth=600 --areaHeight=600 --trafficLoad=medium --dataStartSec=90 --enableDuty=true --enableCsma=true",
        },
        {
            "name": "random_sparse_1400_medium",
            "args": f"mesh_dv_baseline {base} --nodePlacementMode=random --areaWidth=1400 --areaHeight=1400 --trafficLoad=medium --dataStartSec=90 --enableDuty=true --enableCsma=true",
        },
    ]


def main() -> int:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = BASE_DIR / "validation_results" / f"pdr_diagnosis_{ts}"
    outdir.mkdir(parents=True, exist_ok=True)

    seeds = [1, 2, 3]
    scenarios = scenario_matrix()
    rows: List[Dict] = []

    # Build once
    build_log = outdir / "build.log"
    rc_build = run_cmd(["./ns3", "build"], NS3_DIR, build_log)
    if rc_build != 0:
        raise RuntimeError(f"Build failed. See {build_log}")

    for scen in scenarios:
        for seed in seeds:
            run_dir = outdir / scen["name"] / f"seed_{seed}"
            run_dir.mkdir(parents=True, exist_ok=True)
            log_path = run_dir / "run.log"
            cmd = ["./ns3", "run", f"{scen['args']} --rngRun={seed}"]
            rc = run_cmd(cmd, NS3_DIR, log_path)

            # Copy artifacts if run produced them
            summary_src = NS3_DIR / "mesh_dv_summary.json"
            tx_src = NS3_DIR / "mesh_dv_metrics_tx.csv"
            rx_src = NS3_DIR / "mesh_dv_metrics_rx.csv"
            routes_src = NS3_DIR / "mesh_dv_metrics_routes.csv"
            for src in [summary_src, tx_src, rx_src, routes_src]:
                if src.exists():
                    shutil.copy2(src, run_dir / src.name)

            if rc != 0 or not (run_dir / "mesh_dv_summary.json").exists():
                rows.append(
                    {
                        "scenario": scen["name"],
                        "seed": seed,
                        "rc": rc,
                        "pdr": 0.0,
                        "total_data_tx": 0,
                        "delivered": 0,
                        "avg_delay_s": 0.0,
                        "no_route_drop": 0,
                        "duty_data_drop": 0,
                        "duty_beacon_drop": 0,
                        "phy_interference_drop": 0,
                        "ttl_drop": 0,
                        "duplicate_drop": 0,
                        "cad_total": 0,
                        "cad_busy": 0,
                        "dominant_cause": "run_error",
                    }
                )
                continue

            summary = read_json(run_dir / "mesh_dv_summary.json")
            log_text = log_path.read_text(encoding="utf-8", errors="ignore")
            counts = parse_log_counts(log_text)
            row = {
                "scenario": scen["name"],
                "seed": seed,
                "rc": rc,
                "pdr": float(summary.get("pdr", {}).get("pdr", 0.0)),
                "total_data_tx": int(summary.get("pdr", {}).get("total_data_tx", 0)),
                "delivered": int(summary.get("pdr", {}).get("delivered", 0)),
                "avg_delay_s": float(summary.get("delay", {}).get("avg_s", 0.0)),
                **counts,
            }
            row["dominant_cause"] = dominant_cause(row)
            rows.append(row)

    # Aggregate by scenario
    by_scenario: Dict[str, List[Dict]] = {}
    for r in rows:
        by_scenario.setdefault(r["scenario"], []).append(r)

    scen_rows = []
    for name in sorted(by_scenario):
        vals = by_scenario[name]
        ok_vals = [v for v in vals if v["rc"] == 0]
        if not ok_vals:
            continue
        mean = lambda k: statistics.mean(float(v[k]) for v in ok_vals)
        dom_counts: Dict[str, int] = {}
        for v in ok_vals:
            dom_counts[v["dominant_cause"]] = dom_counts.get(v["dominant_cause"], 0) + 1
        dom = max(dom_counts.items(), key=lambda kv: kv[1])[0]
        scen_rows.append(
            {
                "scenario": name,
                "runs": len(ok_vals),
                "pdr_avg": mean("pdr"),
                "tx_avg": mean("total_data_tx"),
                "delivered_avg": mean("delivered"),
                "delay_avg_s": mean("avg_delay_s"),
                "no_route_drop_avg": mean("no_route_drop"),
                "interference_drop_avg": mean("phy_interference_drop"),
                "duty_data_drop_avg": mean("duty_data_drop"),
                "duty_beacon_drop_avg": mean("duty_beacon_drop"),
                "ttl_drop_avg": mean("ttl_drop"),
                "duplicate_drop_avg": mean("duplicate_drop"),
                "cad_busy_ratio_avg": (mean("cad_busy") / max(1.0, mean("cad_total"))),
                "dominant_cause_mode": dom,
            }
        )

    result = {"outdir": str(outdir), "rows": rows, "scenario_summary": scen_rows}
    (outdir / "diagnosis_result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")

    # Build markdown report
    md = [
        "# DIAGNOSTICO_PDR",
        "",
        f"Campana: `{outdir}`",
        "",
        "## Resumen por escenario",
        "",
        "| escenario | runs | pdr_avg | tx_avg | delivered_avg | no_route_avg | interf_avg | duty_data_avg | duty_beacon_avg | cad_busy_ratio | causa dominante |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for s in sorted(scen_rows, key=lambda x: x["pdr_avg"]):
        md.append(
            f"| {s['scenario']} | {s['runs']} | {s['pdr_avg']:.4f} | {s['tx_avg']:.1f} | "
            f"{s['delivered_avg']:.1f} | {s['no_route_drop_avg']:.1f} | {s['interference_drop_avg']:.1f} | "
            f"{s['duty_data_drop_avg']:.1f} | {s['duty_beacon_drop_avg']:.1f} | {s['cad_busy_ratio_avg']:.3f} | "
            f"{s['dominant_cause_mode']} |"
        )

    # Global insight
    if scen_rows:
        worst = min(scen_rows, key=lambda x: x["pdr_avg"])
        best = max(scen_rows, key=lambda x: x["pdr_avg"])
        md += [
            "",
            "## Hallazgos clave",
            "",
            f"- Mejor PDR: `{best['scenario']}` con `pdr_avg={best['pdr_avg']:.4f}`.",
            f"- Peor PDR: `{worst['scenario']}` con `pdr_avg={worst['pdr_avg']:.4f}`.",
            "- El conteo de causas proviene de `run.log` (diagnóstico operacional).",
            "- `no_route` alto sugiere convergencia insuficiente o conectividad pobre.",
            "- `phy_interference_drop` alto sugiere colisiones/interferencia (especialmente con carga alta o sin CSMA).",
            "- `duty_beacon_drop` alto sugiere saturación del ciclo de duty por control/overhead.",
            "",
            "## Criterio de interpretación",
            "",
            "- `PDR` se toma de `mesh_dv_summary.json`.",
            "- Las causas no son mutuamente excluyentes y no suman exactamente pérdidas E2E.",
            "- El objetivo es identificar factores dominantes por escenario, no contabilidad exacta por paquete.",
            "",
        ]

    report_path = outdir / "DIAGNOSTICO_PDR.md"
    report_path.write_text("\n".join(md), encoding="utf-8")
    shutil.copy2(report_path, BASE_DIR / "DIAGNOSTICO_PDR.md")
    print(f"[DONE] PDR diagnosis finished: {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

