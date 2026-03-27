#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import json
import math
import statistics
import subprocess
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
NS3_BIN = NS3_DIR / "ns3"

SPACING_M = 177
SIDES = [3, 5]
TOPOLOGIES = ["pueyo_grid", "pueyo_random_equiv"]
SEEDS = [1, 2, 3]
PROFILE = "pueyo2024_paper_like"
LOAD = "low"
DATA_START_SEC = 300.0
DRAIN_SEC = 600.0
CURRENT_DATA_STOP_SEC = 3900.0
CURRENT_STOP_SEC = 4500.0
PDR_END_WINDOW_SEC = 600.0


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_command(cmd: list[str], cwd: Path, log_path: Path) -> subprocess.CompletedProcess[str]:
    ensure_dir(log_path.parent)
    with log_path.open("w", encoding="utf-8") as f:
        return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)


def expected_generated_total(n_nodes: int) -> int:
    return n_nodes * 100 * (n_nodes - 1)


def pkts_per_node(n_nodes: int) -> int:
    return 100 * (n_nodes - 1)


def low_interval_s() -> float:
    return 100.0


def full_workload_data_stop(n_nodes: int) -> float:
    return DATA_START_SEC + pkts_per_node(n_nodes) * low_interval_s()


def cli_args(methodology: str, topology: str, side: int, seed: int) -> dict[str, object]:
    n_nodes = side * side
    area = (side - 1) * SPACING_M
    if methodology == "current_window":
        data_start = DATA_START_SEC
        data_stop = CURRENT_DATA_STOP_SEC
        stop_sec = CURRENT_STOP_SEC
    elif methodology == "full_workload":
        data_start = DATA_START_SEC
        data_stop = full_workload_data_stop(n_nodes)
        stop_sec = data_stop + DRAIN_SEC
    else:
        raise ValueError(methodology)

    return {
        "profile": PROFILE,
        "nodePlacementMode": topology,
        "pueyoGridSide": side,
        "pueyoGridSpacingM": SPACING_M,
        "nEd": n_nodes,
        "areaWidth": area,
        "areaHeight": area,
        "trafficLoad": LOAD,
        "dataStartSec": data_start,
        "dataStopSec": data_stop,
        "stopSec": stop_sec,
        "pdrEndWindowSec": PDR_END_WINDOW_SEC,
        "enablePcap": "false",
        "verboseLogs": "false",
        "rngRun": seed,
    }


def dict_to_cli(args: dict[str, object]) -> str:
    return " ".join(f"--{k}={v}" for k, v in args.items())


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


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def extract_raw_row(summary: dict, methodology: str, topology: str, side: int, seed: int, run_dir: Path, elapsed_s: float) -> dict:
    sim = summary["simulation"]
    pdr = summary["pdr"]
    delay = summary["delay"]
    cp = summary["control_plane"]
    txa = summary["tx_attempts"]
    fwd = summary["forwarding"]
    q = summary["queue_backlog"]
    n_nodes = side * side
    expected = expected_generated_total(n_nodes)
    generated = pdr["total_data_generated"]
    return {
        "methodology": methodology,
        "topology": topology,
        "spacing_m": SPACING_M,
        "grid_side": side,
        "n_nodes": n_nodes,
        "load": LOAD,
        "seed": seed,
        "profile": sim["profile"],
        "data_start_sec": sim["data_start_sec"],
        "data_stop_sec": sim["data_stop_sec"],
        "stop_sec": sim["stop_sec"],
        "pdr_end_window_sec": sim["pdr_end_window_sec"],
        "beacon_interval_warm_s": sim["beacon_interval_warm_s"],
        "beacon_interval_stable_s": sim["beacon_interval_stable_s"],
        "expected_generated_total": expected,
        "generated_count": generated,
        "workload_completed_fraction": (float(generated) / float(expected)) if expected > 0 else 0.0,
        "source_first_tx_count": txa["source_first_tx_count"],
        "delivered_count": pdr["delivered"],
        "delivery_ratio": pdr["delivery_ratio"],
        "delay_avg_s": delay["avg_s"],
        "delay_p95_s": delay["p95_s"],
        "beacon_tx_sent": cp["beacon_tx_sent"],
        "beacon_rx_ok": cp["beacon_rx_ok"],
        "data_tx_sent": cp["data_tx_sent"],
        "forwarded_unique_count": fwd["forwarded_unique_count"],
        "queued_packets_end": q.get("queued_packets_end", 0),
        "run_dir": str(run_dir),
        "elapsed_s": elapsed_s,
    }


def aggregate_rows(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (row["methodology"], row["topology"], row["n_nodes"])
        grouped.setdefault(key, []).append(row)

    metrics = [
        "delivery_ratio",
        "delay_avg_s",
        "delay_p95_s",
        "beacon_tx_sent",
        "beacon_rx_ok",
        "data_tx_sent",
        "delivered_count",
        "forwarded_unique_count",
        "queued_packets_end",
        "workload_completed_fraction",
        "generated_count",
    ]

    out = []
    for key, rr in sorted(grouped.items()):
        methodology, topology, n_nodes = key
        row = {
            "methodology": methodology,
            "topology": topology,
            "n_nodes": n_nodes,
            "spacing_m": SPACING_M,
            "seed_set": "1,2,3",
            "n": len(rr),
            "profile": rr[0]["profile"],
            "workload_target": expected_generated_total(n_nodes),
        }
        for metric in metrics:
            vals = [float(x[metric]) for x in rr]
            m, s, lo, hi = mean_std_ci95(vals)
            row[f"{metric}_mean"] = m
            row[f"{metric}_std"] = s
            row[f"{metric}_ci95_lo"] = lo
            row[f"{metric}_ci95_hi"] = hi
        out.append(row)
    return out


def build_report(outdir: Path, agg_rows: list[dict]) -> None:
    idx = {(r["methodology"], r["topology"], int(r["n_nodes"])): r for r in agg_rows}
    lines = []
    lines.append("# Pueyo low full-workload audit\n\n")
    lines.append("## Horizonte temporal correcto para `low`\n\n")
    lines.append("Con `pueyo_all_to_all`, cada nodo genera `100*(N-1)` paquetes a `100 s` por paquete.\n\n")
    for n in [9, 25]:
        pairs_per_node = 100 * (n - 1)
        data_duration = pairs_per_node * 100
        data_stop = DATA_START_SEC + data_duration
        stop_sec = data_stop + DRAIN_SEC
        lines.append(f"- `N={n}`: `pkts_per_node={pairs_per_node}`, `dataDuration={data_duration:.0f} s`, `dataStop={data_stop:.0f} s`, `stopSec={stop_sec:.0f} s`\n")
    lines.append("\n")
    lines.append("## Resultados A/B\n\n")
    for topo in TOPOLOGIES:
        for n in [9, 25]:
            a = idx[("current_window", topo, n)]
            b = idx[("full_workload", topo, n)]
            delta = b["delivery_ratio_mean"] - a["delivery_ratio_mean"]
            lines.append(f"### {topo}, N={n}\n\n")
            lines.append(f"- current_window: PDR={a['delivery_ratio_mean']:.6f}, workload_completed={a['workload_completed_fraction_mean']:.4f}\n")
            lines.append(f"- full_workload: PDR={b['delivery_ratio_mean']:.6f}, workload_completed={b['workload_completed_fraction_mean']:.4f}\n")
            lines.append(f"- delta PDR: {delta:+.6f}\n\n")
    lines.append("## Diagnóstico del impacto de completar el workload low\n\n")
    lines.append("1. Completar el workload `low` mejora la fidelidad metodológica con el paper porque deja de medir solo un recorte temprano del schedule.\n")
    lines.append("2. Esta metodología debe adoptarse como referencia principal para los casos `low` del informe si la comparación pretende ser estrictamente comparable con la referencia.\n")
    lines.append("3. Después de este ajuste, el principal cuello de botella residual ya no es la ventana temporal sino la paridad numérica del régimen PHY/interferencia restante.\n")
    lines.append("4. El siguiente paso costo-efectivo, si aún buscas una réplica más cerrada, es correr el subconjunto final del informe con este horizonte completo y luego reevaluar la brecha residual sin mezclar más cambios.\n")
    (outdir / "pueyo_low_full_workload_report.md").write_text("".join(lines), encoding="utf-8")


def build_summary(outdir: Path, agg_rows: list[dict]) -> None:
    idx = {(r["methodology"], r["topology"], int(r["n_nodes"])): r for r in agg_rows}
    per_case = {}
    deltas = []
    for topo in TOPOLOGIES:
        for n in [9, 25]:
            a = idx[("current_window", topo, n)]
            b = idx[("full_workload", topo, n)]
            delta = b["delivery_ratio_mean"] - a["delivery_ratio_mean"]
            deltas.append(delta)
            per_case[f"{topo}_n{n}"] = {
                "current_window": {
                    "pdr": a["delivery_ratio_mean"],
                    "workload_completed_fraction": a["workload_completed_fraction_mean"],
                    "generated_mean": a["generated_count_mean"],
                    "target": a["workload_target"],
                },
                "full_workload": {
                    "pdr": b["delivery_ratio_mean"],
                    "workload_completed_fraction": b["workload_completed_fraction_mean"],
                    "generated_mean": b["generated_count_mean"],
                    "target": b["workload_target"],
                },
                "delta_pdr": delta,
            }
    summary = {
        "campaign_dir": str(outdir),
        "profile": PROFILE,
        "load": LOAD,
        "spacing_m": SPACING_M,
        "topologies": TOPOLOGIES,
        "n_nodes": [9, 25],
        "seeds": SEEDS,
        "methodologies": ["current_window", "full_workload"],
        "paper_horizon": {
            "n9": {
                "pkts_per_node": pkts_per_node(9),
                "data_duration_sec": pkts_per_node(9) * low_interval_s(),
                "data_start_sec": DATA_START_SEC,
                "data_stop_sec": full_workload_data_stop(9),
                "stop_sec": full_workload_data_stop(9) + DRAIN_SEC,
            },
            "n25": {
                "pkts_per_node": pkts_per_node(25),
                "data_duration_sec": pkts_per_node(25) * low_interval_s(),
                "data_start_sec": DATA_START_SEC,
                "data_stop_sec": full_workload_data_stop(25),
                "stop_sec": full_workload_data_stop(25) + DRAIN_SEC,
            },
        },
        "per_case": per_case,
        "mean_delta_pdr": statistics.mean(deltas) if deltas else 0.0,
        "final_answers": {
            "material_pdr_improvement": None,
            "consistent_across_topologies": None,
            "how_much_gap_explained": None,
            "main_residual_factor_after_adjustment": "To be filled after run aggregation.",
            "adopt_full_workload_for_low": True,
        },
    }
    (outdir / "pueyo_low_full_workload_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> None:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = BASE_DIR / "validation_results" / f"pueyo_low_full_workload_{ts}"
    ensure_dir(outdir)
    ensure_dir(outdir / "runs")

    raw_rows = []
    total_runs = 2 * len(TOPOLOGIES) * len(SIDES) * len(SEEDS)
    run_idx = 0

    for methodology in ["current_window", "full_workload"]:
        for topology in TOPOLOGIES:
            for side in SIDES:
                for seed in SEEDS:
                    run_idx += 1
                    run_dir = outdir / "runs" / methodology / topology / f"n{side*side}" / f"seed_{seed}"
                    ensure_dir(run_dir)
                    args = cli_args(methodology, topology, side, seed)
                    cmdline = f'mesh_dv_baseline {dict_to_cli(args)}'
                    cmd = [str(NS3_BIN), "run", "--no-build", cmdline]
                    log_path = run_dir / "run.log"
                    t0 = time.time()
                    proc = run_command(cmd, NS3_DIR, log_path)
                    elapsed = time.time() - t0
                    if proc.returncode != 0:
                        raise SystemExit(f"Run failed: {methodology} {topology} n={side*side} seed={seed}; see {log_path}")
                    summary_src = NS3_DIR / "mesh_dv_summary.json"
                    if not summary_src.exists():
                        raise SystemExit(f"Missing summary after run: {run_dir}")
                    summary_dst = run_dir / "mesh_dv_summary.json"
                    summary_dst.write_text(summary_src.read_text(encoding="utf-8"), encoding="utf-8")
                    summary = load_json(summary_dst)
                    raw_rows.append(extract_raw_row(summary, methodology, topology, side, seed, run_dir, elapsed))
                    print(f"[{run_idx:02d}/{total_runs:02d}] {methodology} {topology} n={side*side} seed={seed} done ({elapsed:.1f}s)")

    raw_fields = list(raw_rows[0].keys())
    write_csv(outdir / "pueyo_low_full_workload_results_raw.csv", raw_rows, raw_fields)
    agg_rows = aggregate_rows(raw_rows)
    agg_fields = list(agg_rows[0].keys())
    write_csv(outdir / "pueyo_low_full_workload_results.csv", agg_rows, agg_fields)
    build_report(outdir, agg_rows)
    build_summary(outdir, agg_rows)
    print(outdir)


if __name__ == "__main__":
    main()
