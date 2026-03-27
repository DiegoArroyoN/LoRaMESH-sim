#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import shutil
import statistics
import subprocess
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"

PROFILE = "pueyo2024_paper_like"
LOAD = "low"
SPACING_M = 177
TOPOLOGIES = ["pueyo_grid", "pueyo_random_equiv"]
PHASE1_SIDES = [3, 5, 8]  # N = 9, 25, 64
PHASE2_SIDES = [4, 6, 7]  # N = 16, 36, 49
SEEDS = [1, 2, 3]
DATA_START_SEC = 300.0
DRAIN_SEC = 600.0
PDR_END_WINDOW_SEC = DRAIN_SEC
REUSE_DIRS = [
    BASE_DIR / "validation_results" / "pueyo_low_full_workload_20260311_090717",
    BASE_DIR / "validation_results" / "pueyo_low_full_workload_n9_only_20260311",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replicate qualitatively the Pueyo Fig. 11(a) PDR trend.")
    parser.add_argument("--outdir", type=str, default=None, help="Existing or new output directory.")
    parser.add_argument("--resume", action="store_true", default=False, help="Reuse finished runs already present in outdir.")
    parser.add_argument("--phase1-only", action="store_true", default=False, help="Run only phase 1 (N=9,25,64).")
    parser.add_argument(
        "--topologies",
        type=str,
        default=" ".join(TOPOLOGIES),
        help="Subset of topologies to run, space or comma separated.",
    )
    return parser.parse_args()


def parse_topologies(spec: str) -> list[str]:
    vals = [tok.strip() for tok in spec.replace(",", " ").split() if tok.strip()]
    if not vals:
        raise ValueError("empty topology list")
    invalid = [tok for tok in vals if tok not in TOPOLOGIES]
    if invalid:
        raise ValueError(f"invalid topologies: {', '.join(invalid)}")
    return vals


def run_command(cmd: list[str], cwd: Path, log_path: Path) -> subprocess.CompletedProcess[str]:
    ensure_dir(log_path.parent)
    with log_path.open("w", encoding="utf-8") as f:
        return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames or list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def mean_std_ci95(values: list[float]) -> tuple[float, float, float, float]:
    if not values:
        return 0.0, 0.0, 0.0, 0.0
    if len(values) == 1:
        x = float(values[0])
        return x, 0.0, x, x
    m = float(statistics.mean(values))
    s = float(statistics.stdev(values))
    h = 1.96 * (s / math.sqrt(len(values)))
    return m, s, m - h, m + h


def packets_per_node(n_nodes: int) -> int:
    return 100 * (n_nodes - 1)


def expected_generated_total(n_nodes: int) -> int:
    return n_nodes * packets_per_node(n_nodes)


def data_duration_sec(n_nodes: int) -> float:
    return float(packets_per_node(n_nodes) * 100)


def data_stop_sec(n_nodes: int) -> float:
    return DATA_START_SEC + data_duration_sec(n_nodes)


def stop_sec(n_nodes: int) -> float:
    return data_stop_sec(n_nodes) + DRAIN_SEC


def dict_to_cli(args: dict[str, object]) -> str:
    return " ".join(f"--{k}={v}" for k, v in args.items())


def build_args(topology: str, side: int, seed: int) -> dict[str, object]:
    n_nodes = side * side
    area = (side - 1) * SPACING_M
    return {
        "profile": PROFILE,
        "nodePlacementMode": topology,
        "pueyoGridSide": side,
        "pueyoGridSpacingM": SPACING_M,
        "nEd": n_nodes,
        "areaWidth": area,
        "areaHeight": area,
        "trafficLoad": LOAD,
        "trafficMode": "pueyo_all_to_all",
        "pueyoPacketsPerPair": 100,
        "dataStartSec": DATA_START_SEC,
        "dataStopSec": data_stop_sec(n_nodes),
        "stopSec": stop_sec(n_nodes),
        "pdrEndWindowSec": PDR_END_WINDOW_SEC,
        "enablePcap": "false",
        "verboseLogs": "false",
        "enableNs3EnergyFramework": "false",
        "rngRun": seed,
    }


def find_reusable_run(topology: str, side: int, seed: int) -> Path | None:
    n_nodes = side * side
    if n_nodes != 9:
        return None
    for reuse_dir in REUSE_DIRS:
        candidate = reuse_dir / "runs" / "full_workload" / topology / f"n{n_nodes}" / f"seed_{seed}"
        summary_path = candidate / "mesh_dv_summary.json"
        if summary_path.exists() and (candidate / "run.log").exists():
            try:
                summary = load_json(summary_path)
            except Exception:
                continue
            if summary_matches_expected(summary):
                return candidate
    return None


def summary_matches_expected(summary: dict) -> bool:
    sim = summary.get("simulation", {})
    return (
        sim.get("profile") == PROFILE
        and int(sim.get("sf_min", -1)) == 7
        and int(sim.get("sf_max", -1)) == 8
        and bool(sim.get("pueyo_flora_like_rx", False)) is True
        and bool(sim.get("enable_sf_scan_rx", True)) is False
        and bool(sim.get("enable_ns3_energy_framework", True)) is False
        and sim.get("wire_format") == "pueyo7b"
        and int(sim.get("channel_count", -1)) == 1
        and int(sim.get("reception_paths", -1)) == 1
    )


def extract_raw_row(
    summary: dict,
    phase: str,
    topology: str,
    side: int,
    seed: int,
    run_dir: Path,
    elapsed_s: float,
    reused_from: str = "",
) -> dict:
    sim = summary["simulation"]
    pdr = summary["pdr"]
    delay = summary["delay"]
    cp = summary["control_plane"]
    fwd = summary["forwarding"]
    q = summary["queue_backlog"]
    txa = summary["tx_attempts"]
    routes = summary["routes"]
    n_nodes = side * side
    expected = expected_generated_total(n_nodes)
    generated = int(pdr["total_data_generated"])
    delivered = int(pdr["delivered"])
    return {
        "phase": phase,
        "profile": sim["profile"],
        "topology": topology,
        "spacing_m": SPACING_M,
        "grid_side": side,
        "n_nodes": n_nodes,
        "load": LOAD,
        "seed": seed,
        "data_start_sec": sim["data_start_sec"],
        "data_stop_sec": sim["data_stop_sec"],
        "stop_sec": sim["stop_sec"],
        "workload_target": expected,
        "generated_count": generated,
        "workload_completed_fraction": (float(generated) / float(expected)) if expected > 0 else 0.0,
        "tx_sent_count": int(cp["data_tx_sent"]),
        "delivered_count": delivered,
        "pdr": float(pdr["delivery_ratio"]),
        "pdr_post_convergence": float(pdr["pdr_post_convergence"]),
        "pdr_no_drain": float(pdr["pdr_no_drain"]),
        "delay_avg_s": float(delay["avg_s"]),
        "delay_p95_s": float(delay["p95_s"]),
        "beacons_tx": int(cp["beacon_tx_sent"]),
        "beacons_rx": int(cp["beacon_rx_ok"]),
        "forwarded_unique_count": int(fwd["forwarded_unique_count"]),
        "routes_total": int(routes["routes_total"]),
        "routes_at_data_start": int(cp["routes_at_data_start"]),
        "routes_at_midpoint": int(cp["routes_at_midpoint"]),
        "first_usable_route_time_s_mean": float(cp["first_usable_route_time_s_mean"]),
        "coverage80_route_time_s_mean": float(cp["coverage80_route_time_s_mean"]),
        "rx_scan_miss_before_lock": int(cp["rx_scan_miss_before_lock"]),
        "rx_post_lock_interference_fail": int(cp["rx_post_lock_interference_fail"]),
        "rx_no_more_demodulators": int(cp["rx_no_more_demodulators"]),
        "pueyo_same_sf_overlap_events": int(cp["pueyo_same_sf_overlap_events"]),
        "pueyo_destructive_overlap_drops": int(cp["pueyo_destructive_overlap_drops"]),
        "pueyo_capture_or_timing_survivals": int(cp["pueyo_capture_or_timing_survivals"]),
        "queued_packets_at_data_stop": int(q["queued_packets_at_data_stop"]),
        "queued_packets_end": int(q["queued_packets_end"]),
        "source_first_tx_count": int(pdr["source_first_tx_count"]),
        "generated_before_first_route": int(pdr["generated_before_first_route"]),
        "generated_after_first_route": int(pdr["generated_after_first_route"]),
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
        "reused_from": reused_from,
    }


def aggregate_rows(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, int], list[dict]] = {}
    for row in rows:
        grouped.setdefault((row["topology"], int(row["n_nodes"])), []).append(row)

    metrics = [
        "pdr",
        "delay_avg_s",
        "delay_p95_s",
        "generated_count",
        "tx_sent_count",
        "delivered_count",
        "forwarded_unique_count",
        "beacons_tx",
        "beacons_rx",
        "routes_at_data_start",
        "routes_at_midpoint",
        "routes_total",
        "first_usable_route_time_s_mean",
        "coverage80_route_time_s_mean",
        "rx_scan_miss_before_lock",
        "rx_post_lock_interference_fail",
        "rx_no_more_demodulators",
        "pueyo_same_sf_overlap_events",
        "pueyo_destructive_overlap_drops",
        "pueyo_capture_or_timing_survivals",
        "queued_packets_at_data_stop",
        "queued_packets_end",
        "workload_completed_fraction",
    ]

    out = []
    for (topology, n_nodes), rr in sorted(grouped.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        row = {
            "topology": topology,
            "spacing_m": SPACING_M,
            "n_nodes": n_nodes,
            "grid_side": int(math.isqrt(n_nodes)),
            "load": LOAD,
            "profile": rr[0]["profile"],
            "seed_set": ",".join(str(x["seed"]) for x in sorted(rr, key=lambda x: x["seed"])),
            "n": len(rr),
            "workload_target": rr[0]["workload_target"],
            "sf_min": rr[0]["sf_min"],
            "sf_max": rr[0]["sf_max"],
            "pueyo_flora_like_rx": rr[0]["pueyo_flora_like_rx"],
            "enable_sf_scan_rx": rr[0]["enable_sf_scan_rx"],
            "enable_ns3_energy_framework": rr[0]["enable_ns3_energy_framework"],
            "interference_model": rr[0]["interference_model"],
        }
        for metric in metrics:
            vals = [float(x[metric]) for x in rr]
            mean_v, std_v, lo, hi = mean_std_ci95(vals)
            row[f"{metric}_mean"] = mean_v
            row[f"{metric}_std"] = std_v
            row[f"{metric}_ci95_lo"] = lo
            row[f"{metric}_ci95_hi"] = hi
        row["seed_min_pdr"] = min(float(x["pdr"]) for x in rr)
        row["seed_max_pdr"] = max(float(x["pdr"]) for x in rr)
        out.append(row)
    return out


def detect_strong_collapse(agg_rows: list[dict]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    for row in agg_rows:
        n_nodes = int(row["n_nodes"])
        pdr_mean = float(row["pdr_mean"])
        if n_nodes in {25, 64} and pdr_mean < 0.001:
            reasons.append(
                f"{row['topology']} N={n_nodes} tiene PDR medio {pdr_mean:.6f} (<0.001), colapso fuerte"
            )
    return (len(reasons) > 0, reasons)


def build_diagnostic_rows(agg_rows: list[dict], topologies: list[str]) -> list[dict]:
    out = []
    for topology in topologies:
        topo_rows = [r for r in agg_rows if r["topology"] == topology]
        topo_rows.sort(key=lambda r: int(r["n_nodes"]))
        prev_pdr = None
        for row in topo_rows:
            pdr_mean = float(row["pdr_mean"])
            pdr_std = float(row["pdr_std"])
            n_nodes = int(row["n_nodes"])
            if prev_pdr is None:
                obs = "Inicio de curva."
            elif pdr_mean < prev_pdr:
                obs = "Caida progresiva respecto al N previo."
            elif math.isclose(pdr_mean, prev_pdr, rel_tol=0.05, abs_tol=0.01):
                obs = "Meseta aproximada respecto al N previo."
            else:
                obs = "Aumento no monotono; revisar si es dispersión o efecto topologico."
            if pdr_std > max(0.05, 0.5 * max(pdr_mean, 1e-9)):
                obs += " Dispersión alta entre seeds."
            out.append(
                {
                    "topology": topology,
                    "n_nodes": n_nodes,
                    "pdr_mean": pdr_mean,
                    "pdr_std": pdr_std,
                    "seed_min_pdr": row["seed_min_pdr"],
                    "seed_max_pdr": row["seed_max_pdr"],
                    "observation": obs,
                }
            )
            prev_pdr = pdr_mean
    return out


def make_plot(outdir: Path, agg_rows: list[dict], topologies: list[str]) -> list[str]:
    plot_paths: list[str] = []
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return plot_paths

    plt.figure(figsize=(7.5, 4.8))
    styles = {
        "pueyo_grid": ("#0b5fff", "o", "Grid"),
        "pueyo_random_equiv": ("#c73e1d", "s", "Random equiv"),
    }
    for topology in topologies:
        topo_rows = sorted((r for r in agg_rows if r["topology"] == topology), key=lambda r: int(r["n_nodes"]))
        xs = [int(r["n_nodes"]) for r in topo_rows]
        ys = [float(r["pdr_mean"]) for r in topo_rows]
        errs = [max(0.0, float(r["pdr_ci95_hi"]) - float(r["pdr_mean"])) for r in topo_rows]
        color, marker, label = styles[topology]
        plt.errorbar(xs, ys, yerr=errs, color=color, marker=marker, linewidth=2, capsize=4, label=label)

    plt.xlabel("N nodes")
    plt.ylabel("PDR mean")
    plt.title("Fig. 11(a) qualitative trend replication - ToA SF7-8 low")
    plt.xticks(sorted({int(r["n_nodes"]) for r in agg_rows}))
    plt.ylim(bottom=0.0)
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()

    png_path = outdir / "pueyo_fig11a_trend_replication_pdr_vs_n.png"
    pdf_path = outdir / "pueyo_fig11a_trend_replication_pdr_vs_n.pdf"
    plt.savefig(png_path, dpi=180)
    plt.savefig(pdf_path)
    plt.close()
    plot_paths.extend([str(png_path), str(pdf_path)])
    return plot_paths


def build_summary(
    outdir: Path,
    phase1_rows: list[dict],
    all_rows: list[dict],
    collapse: bool,
    collapse_reasons: list[str],
    plot_paths: list[str],
    topologies: list[str],
    phase1_only: bool,
) -> dict:
    agg = aggregate_rows(all_rows)
    by_topology: dict[str, list[dict]] = {}
    for topology in topologies:
        by_topology[topology] = [r for r in agg if r["topology"] == topology]
        by_topology[topology].sort(key=lambda r: int(r["n_nodes"]))

    final_answers = {
        "progressive_drop": None,
        "unreasonable_collapse": None,
        "topology_relative_behavior": None,
        "seed_dispersion_reasonable": None,
        "qualitative_replication": None,
    }

    summary = {
        "campaign_dir": str(outdir),
        "profile": PROFILE,
        "load": LOAD,
        "spacing_m": SPACING_M,
        "topologies": topologies,
        "phase1_sizes": [9, 25, 64],
        "phase2_sizes": [] if phase1_only or collapse else [16, 36, 49],
        "seeds": SEEDS,
        "data_start_sec": DATA_START_SEC,
        "drain_sec": DRAIN_SEC,
        "full_workload_rule": {
            "packets_per_pair": 100,
            "interval_low_sec": 100.0,
            "pkts_per_node_formula": "100*(N-1)",
            "data_duration_formula_sec": "100*(N-1)*100",
            "data_stop_formula_sec": "dataStartSec + dataDuration",
            "stop_formula_sec": "dataStopSec + 600",
        },
        "phase1_only": phase1_only,
        "requested_phase1_only": phase1_only,
        "collapse_reasons": collapse_reasons,
        "plot_paths": plot_paths,
        "final_answers": final_answers,
        "per_topology": by_topology,
    }
    return summary


def build_report(
    outdir: Path,
    raw_rows: list[dict],
    agg_rows: list[dict],
    diag_rows: list[dict],
    collapse: bool,
    collapse_reasons: list[str],
    plot_paths: list[str],
    phase1_only: bool,
) -> None:
    lines: list[str] = []
    lines.append("# Replicación cualitativa de la Fig. 11(a)\n\n")
    lines.append("## Configuración fija\n\n")
    lines.append(f"- Perfil: `{PROFILE}`\n")
    lines.append("- Routing: ToA\n")
    lines.append("- SF-range: `7-8`\n")
    lines.append("- Beacon path: Pueyo corregido\n")
    lines.append("- Receive-start: FLoRa-like (`pueyoFloraLikeRx=true`)\n")
    lines.append("- Canal: 1\n")
    lines.append("- Reception paths: 1\n")
    lines.append(f"- Load: `{LOAD}`\n")
    lines.append(f"- Spacing: `{SPACING_M} m`\n")
    lines.append(f"- Seeds: `{','.join(str(x) for x in SEEDS)}`\n\n")

    lines.append("## Horizonte temporal correcto por tamaño\n\n")
    for n in sorted({int(r['n_nodes']) for r in agg_rows}):
        lines.append(
            f"- `N={n}`: `pkts_per_node={packets_per_node(n)}`, `generated_total={expected_generated_total(n)}`, "
            f"`dataDuration={data_duration_sec(n):.0f} s`, `dataStop={data_stop_sec(n):.0f} s`, `stopSec={stop_sec(n):.0f} s`\n"
        )
    lines.append("\n")

    if phase1_only:
        lines.append("## Resultado de Fase 1\n\n")
        lines.append("Se ejecutó solo Fase 1 por pedido explícito, sin entrar a la interpolación de Fase 2.\n\n")
    elif collapse:
        lines.append("## Resultado de Fase 1\n\n")
        lines.append("Se detectó colapso fuerte y la campaña se detuvo antes de Fase 2.\n\n")
        for reason in collapse_reasons:
            lines.append(f"- {reason}\n")
        lines.append("\n")
    else:
        lines.append("## Resultado de Fase 1\n\n")
        lines.append("No se detectó colapso fuerte en `N=9,25,64`, por lo que se ejecutó Fase 2 con `N=16,36,49`.\n\n")

    lines.append("## Tabla diagnóstica por N\n\n")
    lines.append("| topology | N | PDR mean | PDR std | min seed | max seed | observación |\n")
    lines.append("|---|---:|---:|---:|---:|---:|---|\n")
    for row in diag_rows:
        lines.append(
            f"| {row['topology']} | {row['n_nodes']} | {row['pdr_mean']:.6f} | {row['pdr_std']:.6f} | "
            f"{row['seed_min_pdr']:.6f} | {row['seed_max_pdr']:.6f} | {row['observation']} |\n"
        )
    lines.append("\n")

    lines.append("## Agregado por caso\n\n")
    lines.append("| topology | N | PDR mean | delay_avg_s | delay_p95_s | generated | tx_sent | delivered | forwarded | beacons_tx | beacons_rx |\n")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    for row in sorted(agg_rows, key=lambda r: (r["topology"], int(r["n_nodes"]))):
        lines.append(
            f"| {row['topology']} | {row['n_nodes']} | {row['pdr_mean']:.6f} | {row['delay_avg_s_mean']:.3f} | "
            f"{row['delay_p95_s_mean']:.3f} | {row['generated_count_mean']:.1f} | {row['tx_sent_count_mean']:.1f} | "
            f"{row['delivered_count_mean']:.1f} | {row['forwarded_unique_count_mean']:.1f} | "
            f"{row['beacons_tx_mean']:.1f} | {row['beacons_rx_mean']:.1f} |\n"
        )
    lines.append("\n")

    if plot_paths:
        lines.append("## Figura generada\n\n")
        for path in plot_paths:
            lines.append(f"- `{path}`\n")
        lines.append("\n")

    lines.append("## Diagnóstico de replicación cualitativa de la Fig. 11(a)\n\n")
    lines.append("1. **¿Se reproduce la tendencia general del paper?** Ver respuesta consolidada en `summary.json` a partir de la monotonicidad y del orden relativo entre topologías.\n")
    lines.append("2. **¿Qué tan estable es la curva al crecer N?** Ver tabla diagnóstica por `PDR std` y rango min/max entre seeds.\n")
    lines.append("3. **¿Dónde aparece la mayor desviación respecto del comportamiento esperado?** Se identifica en los puntos con no-monotonicidad o dispersión alta.\n")
    lines.append("4. **¿La implementación ya es apta para afirmar que reproduce cualitativamente la referencia ToA?** Queda contestado en el consolidado final, no por inspección vaga.\n")
    lines.append("5. **¿Qué siguiente paso mínimo harías si aún quieres acercarte más?** Solo uno: contrastar la misma curva contra la referencia visual del paper sin introducir nuevas sensibilidades.\n")
    (outdir / "pueyo_fig11a_trend_replication_report.md").write_text("".join(lines), encoding="utf-8")


def run_phase(outdir: Path, phase_name: str, sides: list[int], resume: bool, topologies: list[str]) -> list[dict]:
    raw_rows: list[dict] = []
    total_runs = len(sides) * len(topologies) * len(SEEDS)
    idx = 0
    for topology in topologies:
        for side in sides:
            n_nodes = side * side
            for seed in SEEDS:
                idx += 1
                run_dir = outdir / "runs" / phase_name / topology / f"n{n_nodes}" / f"seed_{seed}"
                ensure_dir(run_dir)
                if resume and (run_dir / "mesh_dv_summary.json").exists():
                    summary = load_json(run_dir / "mesh_dv_summary.json")
                    if summary_matches_expected(summary):
                        print(
                            f"[{phase_name} {idx:02d}/{total_runs}] topo={topology} N={n_nodes} seed={seed} resume={run_dir}",
                            flush=True,
                        )
                        raw_rows.append(
                            extract_raw_row(summary, phase_name, topology, side, seed, run_dir, 0.0, "")
                        )
                        continue
                    print(
                        f"[{phase_name} {idx:02d}/{total_runs}] topo={topology} N={n_nodes} seed={seed} "
                        f"resume-incompatible -> rerun {run_dir}",
                        flush=True,
                    )
                reused_from = ""
                reuse_dir = find_reusable_run(topology, side, seed)
                if reuse_dir is not None:
                    print(
                        f"[{phase_name} {idx:02d}/{total_runs}] topo={topology} N={n_nodes} seed={seed} "
                        f"reusing={reuse_dir}",
                        flush=True,
                    )
                    shutil.copy2(reuse_dir / "mesh_dv_summary.json", run_dir / "mesh_dv_summary.json")
                    shutil.copy2(reuse_dir / "run.log", run_dir / "run.log")
                    elapsed_s = 0.0
                    reused_from = str(reuse_dir)
                else:
                    args = build_args(topology, side, seed)
                    cmdline = "mesh_dv_baseline " + dict_to_cli(args)
                    cmd = [str(NS3_BIN), "run", "--no-build", cmdline]
                    print(
                        f"[{phase_name} {idx:02d}/{total_runs}] topo={topology} N={n_nodes} seed={seed} "
                        f"dataStop={args['dataStopSec']:.0f}s stop={args['stopSec']:.0f}s",
                        flush=True,
                    )
                    t0 = time.time()
                    proc = run_command(cmd, NS3_DIR, run_dir / "run.log")
                    elapsed_s = time.time() - t0
                    if proc.returncode != 0:
                        raise RuntimeError(f"simulation failed rc={proc.returncode} for {run_dir}")
                    summary_src = NS3_DIR / "mesh_dv_summary.json"
                    if not summary_src.exists():
                        raise RuntimeError("mesh_dv_summary.json missing after run")
                    shutil.copy2(summary_src, run_dir / "mesh_dv_summary.json")
                summary = load_json(run_dir / "mesh_dv_summary.json")
                raw_rows.append(
                    extract_raw_row(summary, phase_name, topology, side, seed, run_dir, elapsed_s, reused_from)
                )
    return raw_rows


def main() -> None:
    args = parse_args()
    selected_topologies = parse_topologies(args.topologies)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.outdir).resolve() if args.outdir else BASE_DIR / "validation_results" / f"pueyo_fig11a_trend_replication_{ts}"
    ensure_dir(outdir)

    phase1_raw = run_phase(outdir, "phase1", PHASE1_SIDES, args.resume, selected_topologies)
    phase1_agg = aggregate_rows(phase1_raw)
    collapse, collapse_reasons = detect_strong_collapse(phase1_agg)

    all_raw = list(phase1_raw)
    if not args.phase1_only and not collapse:
        all_raw.extend(run_phase(outdir, "phase2", PHASE2_SIDES, args.resume, selected_topologies))

    agg_rows = aggregate_rows(all_raw)
    diag_rows = build_diagnostic_rows(agg_rows, selected_topologies)
    plot_paths = make_plot(outdir, agg_rows, selected_topologies)

    raw_csv = outdir / "pueyo_fig11a_trend_replication_results_raw.csv"
    agg_csv = outdir / "pueyo_fig11a_trend_replication_results.csv"
    diag_csv = outdir / "pueyo_fig11a_trend_replication_diagnostic_table.csv"
    write_csv(raw_csv, all_raw)
    write_csv(agg_csv, agg_rows)
    write_csv(diag_csv, diag_rows)

    summary = build_summary(
        outdir,
        phase1_raw,
        all_raw,
        collapse,
        collapse_reasons,
        plot_paths,
        selected_topologies,
        args.phase1_only,
    )

    # Fill final answers from aggregate data.
    by_topo = {
        topo: sorted([r for r in agg_rows if r["topology"] == topo], key=lambda r: int(r["n_nodes"]))
        for topo in selected_topologies
    }
    progressive_drop = all(
        float(rows[i]["pdr_mean"]) <= float(rows[i - 1]["pdr_mean"]) + 0.02
        for rows in by_topo.values()
        for i in range(1, len(rows))
    )
    unreasonable_collapse = collapse
    topology_relative_behavior = None
    if "pueyo_grid" in by_topo and "pueyo_random_equiv" in by_topo and all(by_topo.values()):
        grid_rows = {int(r["n_nodes"]): r for r in by_topo["pueyo_grid"]}
        rnd_rows = {int(r["n_nodes"]): r for r in by_topo["pueyo_random_equiv"]}
        common_ns = sorted(set(grid_rows) & set(rnd_rows))
        rnd_better = sum(1 for n in common_ns if float(rnd_rows[n]["pdr_mean"]) >= float(grid_rows[n]["pdr_mean"]))
        topology_relative_behavior = rnd_better >= max(1, len(common_ns) - 1)
    seed_dispersion_reasonable = all(
        float(r["pdr_std"]) <= max(0.08, 0.6 * max(float(r["pdr_mean"]), 1e-9)) for r in agg_rows
    )
    qualitative_replication = progressive_drop and (not unreasonable_collapse) and bool(topology_relative_behavior)
    summary["final_answers"] = {
        "progressive_drop": progressive_drop,
        "unreasonable_collapse": unreasonable_collapse,
        "topology_relative_behavior": topology_relative_behavior,
        "seed_dispersion_reasonable": seed_dispersion_reasonable,
        "qualitative_replication": qualitative_replication,
    }

    (outdir / "pueyo_fig11a_trend_replication_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    build_report(outdir, all_raw, agg_rows, diag_rows, collapse, collapse_reasons, plot_paths, args.phase1_only)
    print(outdir)


if __name__ == "__main__":
    main()
