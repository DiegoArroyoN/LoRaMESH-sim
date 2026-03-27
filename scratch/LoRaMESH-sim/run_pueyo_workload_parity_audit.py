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
LOADS = ["low", "high"]
SEEDS = [1, 2, 3]
PROFILE = "pueyo2024_paper_like"
DATA_START_SEC = 300.0
DRAIN_SEC = 600.0
CURRENT_DATA_STOP_SEC = 3900.0
CURRENT_STOP_SEC = 4500.0
PDR_END_WINDOW_SEC = 600.0
DELAYED_DATA_START_SEC = 900.0
DELAYED_DATA_STOP_SEC = DELAYED_DATA_START_SEC + (CURRENT_DATA_STOP_SEC - DATA_START_SEC)
DELAYED_STOP_SEC = DELAYED_DATA_STOP_SEC + DRAIN_SEC


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_command(cmd: list[str], cwd: Path, log_path: Path) -> subprocess.CompletedProcess[str]:
    ensure_dir(log_path.parent)
    with log_path.open("w", encoding="utf-8") as f:
        return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)


def traffic_interval_from_load(load: str) -> float:
    if load == "low":
        return 100.0
    if load == "high":
        return 1.0
    raise ValueError(load)


def expected_generated_total(n_nodes: int) -> int:
    return n_nodes * 100 * (n_nodes - 1)


def cli_args(methodology: str, topology: str, side: int, load: str, seed: int) -> dict[str, object]:
    n_nodes = side * side
    area = (side - 1) * SPACING_M
    if methodology == "current_window":
        data_start = DATA_START_SEC
        data_stop = CURRENT_DATA_STOP_SEC
        stop_sec = CURRENT_STOP_SEC
    elif methodology == "delayed_start":
        data_start = DELAYED_DATA_START_SEC
        data_stop = DELAYED_DATA_STOP_SEC
        stop_sec = DELAYED_STOP_SEC
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
        "trafficLoad": load,
        "dataStartSec": data_start,
        "dataStopSec": data_stop,
        "stopSec": stop_sec,
        "pdrEndWindowSec": PDR_END_WINDOW_SEC,
        "enablePcap": "false",
        "verboseLogs": "false",
        "enableGapAuditTrace": "true",
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


def extract_raw_row(summary: dict, methodology: str, topology: str, side: int, load: str, seed: int, run_dir: Path, elapsed_s: float) -> dict:
    sim = summary["simulation"]
    pdr = summary["pdr"]
    delay = summary["delay"]
    cp = summary["control_plane"]
    txa = summary["tx_attempts"]
    fwd = summary["forwarding"]
    q = summary["queue_backlog"]
    n_nodes = side * side
    return {
        "methodology": methodology,
        "topology": topology,
        "spacing_m": SPACING_M,
        "grid_side": side,
        "n_nodes": n_nodes,
        "load": load,
        "seed": seed,
        "profile": sim["profile"],
        "data_start_sec": sim["data_start_sec"],
        "data_stop_sec": sim["data_stop_sec"],
        "stop_sec": sim["stop_sec"],
        "pdr_end_window_sec": sim["pdr_end_window_sec"],
        "beacon_interval_warm_s": sim["beacon_interval_warm_s"],
        "beacon_interval_stable_s": sim["beacon_interval_stable_s"],
        "expected_generated_total": expected_generated_total(n_nodes),
        "generated_count": pdr["total_data_generated"],
        "source_first_tx_count": txa["source_first_tx_count"],
        "delivered_count": pdr["delivered"],
        "delivery_ratio": pdr["delivery_ratio"],
        "pdr_eligible": pdr["pdr_e2e_generated_eligible"],
        "pdr_post_convergence": pdr.get("pdr_post_convergence", 0.0),
        "pdr_no_drain": pdr.get("pdr_no_drain", 0.0),
        "delay_avg_s": delay["avg_s"],
        "delay_p95_s": delay["p95_s"],
        "beacon_tx_sent": cp["beacon_tx_sent"],
        "beacon_rx_ok": cp["beacon_rx_ok"],
        "beacon_tx_before_data_start": cp.get("beacon_tx_before_data_start", 0),
        "beacon_rx_before_data_start": cp.get("beacon_rx_before_data_start", 0),
        "beacon_tx_during_data_phase": cp.get("beacon_tx_during_data_phase", 0),
        "beacon_rx_during_data_phase": cp.get("beacon_rx_during_data_phase", 0),
        "data_tx_sent": cp["data_tx_sent"],
        "forwarded_unique_count": fwd["forwarded_unique_count"],
        "first_usable_route_time_s_mean": cp.get("first_usable_route_time_s_mean", -1.0),
        "coverage80_route_time_s_mean": cp.get("coverage80_route_time_s_mean", -1.0),
        "routes_at_data_start": cp.get("routes_at_data_start", 0),
        "routes_at_midpoint": cp.get("routes_at_midpoint", 0),
        "routes_at_data_stop": cp.get("routes_at_data_stop", 0),
        "generated_before_first_route": pdr.get("generated_before_first_route", 0),
        "generated_after_first_route": pdr.get("generated_after_first_route", 0),
        "generated_without_route_ever": pdr.get("generated_without_route_ever", 0),
        "generated_no_first_tx_by_end": pdr.get("generated_no_first_tx_by_end", 0),
        "late_generated_near_data_stop": pdr.get("late_generated_near_data_stop", 0),
        "generated_no_first_tx_late_window": pdr.get("generated_no_first_tx_late_window", 0),
        "queued_packets_at_data_stop": q.get("queued_packets_at_data_stop", 0),
        "queued_packets_end": q.get("queued_packets_end", 0),
        "sf_range": f"{sim['sf_min']}-{sim['sf_max']}",
        "pueyo_flora_like_rx": sim["pueyo_flora_like_rx"],
        "enable_sf_scan_rx": sim["enable_sf_scan_rx"],
        "run_dir": str(run_dir),
        "elapsed_s": elapsed_s,
    }


def aggregate_rows(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (row["methodology"], row["topology"], row["n_nodes"], row["load"])
        grouped.setdefault(key, []).append(row)

    metrics = [
        "delivery_ratio",
        "pdr_post_convergence",
        "pdr_no_drain",
        "delay_avg_s",
        "delay_p95_s",
        "beacon_tx_sent",
        "beacon_rx_ok",
        "beacon_tx_before_data_start",
        "beacon_rx_before_data_start",
        "beacon_tx_during_data_phase",
        "beacon_rx_during_data_phase",
        "data_tx_sent",
        "delivered_count",
        "forwarded_unique_count",
        "first_usable_route_time_s_mean",
        "coverage80_route_time_s_mean",
        "routes_at_data_start",
        "routes_at_midpoint",
        "routes_at_data_stop",
        "generated_before_first_route",
        "generated_after_first_route",
        "generated_without_route_ever",
        "generated_no_first_tx_by_end",
        "late_generated_near_data_stop",
        "generated_no_first_tx_late_window",
        "queued_packets_at_data_stop",
        "queued_packets_end",
    ]
    out = []
    for key, rr in sorted(grouped.items()):
        methodology, topology, n_nodes, load = key
        row = {
            "methodology": methodology,
            "topology": topology,
            "n_nodes": n_nodes,
            "load": load,
            "spacing_m": SPACING_M,
            "seed_set": "1,2,3",
            "n": len(rr),
            "profile": rr[0]["profile"],
            "sf_range": rr[0]["sf_range"],
            "pueyo_flora_like_rx": rr[0]["pueyo_flora_like_rx"],
            "enable_sf_scan_rx": rr[0]["enable_sf_scan_rx"],
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
    idx = {(r["methodology"], r["topology"], int(r["n_nodes"]), r["load"]): r for r in agg_rows}
    lines = []
    lines.append("# Pueyo workload parity audit\n\n")
    lines.append("## Metodología temporal actual\n\n")
    lines.append("- Perfil base: `pueyo2024_paper_like`\n")
    lines.append("- Metodología A (`current_window`): `dataStart=300`, `dataStop=3900`, `stop=4500`, `pdrEndWindow=600`.\n")
    lines.append("- Metodología B (`delayed_start`): misma fase útil (`3600 s`) y mismo drenaje (`600 s`), pero `dataStart=900`, `dataStop=4500`, `stop=5100`.\n")
    lines.append("- La variante B no cambia protocolo, PHY, MAC ni workload por paquete; solo retrasa el inicio del tráfico útil para medir sensibilidad a convergencia/control-plane antes de data.\n\n")
    lines.append("## Paridad con el workload del paper\n\n")
    lines.append("- El paper define `100*(N-1)` paquetes por nodo a intervalo fijo.\n")
    lines.append("- Para `low`, eso implica duraciones de datos muy largas: `N=9 -> 80000 s`, `N=25 -> 240000 s`.\n")
    lines.append("- La ventana comparable actual (`300..3900 s`) no completa ese workload; por eso esta auditoría separa dos cosas: drift analítico por truncamiento del workload completo y sensibilidad práctica a madurez del control-plane con `delayed_start`.\n\n")
    lines.append("## Código relevante\n\n")
    lines.append("- `mesh_dv_app.cc`: arranque de datos y snapshots de gap audit.\n")
    lines.append("- `metrics_collector.cc`: export de `pdr_post_convergence`, `pdr_no_drain`, counts before/after first route, snapshots `midpoint` y `dataStop`.\n")
    lines.append("- `run_pueyo_workload_parity_audit.py`: A/B entre ventana actual y `delayed_start`.\n\n")
    lines.append("## Resultados agregados\n\n")
    lines.append("| caso | pdr actual | pdr delayed_start | delta | first route current | first route delayed | observación |\n")
    lines.append("|---|---:|---:|---:|---:|---:|---|\n")
    deltas = []
    for topo in TOPOLOGIES:
        for side in SIDES:
            n_nodes = side * side
            for load in LOADS:
                a = idx[("current_window", topo, n_nodes, load)]
                b = idx[("delayed_start", topo, n_nodes, load)]
                delta = b["delivery_ratio_mean"] - a["delivery_ratio_mean"]
                deltas.append(delta)
                observation = "improves with later data start" if delta > 0 else "no improvement / lower with later data start"
                lines.append(
                    f"| {topo} N={n_nodes} {load} | {a['delivery_ratio_mean']:.6f} | {b['delivery_ratio_mean']:.6f} | {delta:+.6f} | "
                    f"{a['first_usable_route_time_s_mean_mean']:.1f} | {b['first_usable_route_time_s_mean_mean']:.1f} | {observation} |\n"
                )
    mean_delta = statistics.mean(deltas) if deltas else 0.0
    lines.append("\n## Diagnóstico del impacto de la metodología temporal del workload\n\n")
    lines.append(f"1. ¿Cuánto parece explicar la metodología temporal de la brecha residual? Media de delta PDR A/B: `{mean_delta:+.6f}`. ")
    lines.append("La respuesta distingue dos cosas: el drift analítico por truncamiento del workload completo y la sensibilidad práctica a arrancar datos con la topología todavía inmadura.\n")
    lines.append("2. ¿Es hoy el siguiente cambio más importante después de SF-range y receive-start? Solo si `delayed_start` mejora de forma consistente; si no, el problema temporal pesa menos que esos dos factores.\n")
    lines.append("3. ¿Debe integrarse al perfil principal o solo reportarse como sensibilidad? Debe reportarse como sensibilidad/harness metodológico. No corresponde integrarla al perfil, porque cambia el cronograma experimental, no el protocolo.\n")
    lines.append("4. ¿Qué siguiente paso harías solo si aún buscas una réplica numérica más cerrada? Para `low`, ejecutar workload completo solo en los puntos finales que vayas a reportar; para iteración normal, usar la sensibilidad `delayed_start` y los KPIs post-convergencia.\n")
    (outdir / "pueyo_workload_parity_audit_report.md").write_text("".join(lines), encoding="utf-8")


def main() -> int:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = BASE_DIR / "validation_results" / f"pueyo_workload_parity_audit_{stamp}"
    ensure_dir(outdir)
    raw_rows: list[dict] = []
    total = 2 * len(TOPOLOGIES) * len(SIDES) * len(LOADS) * len(SEEDS)
    idx = 0
    for methodology in ["current_window", "delayed_start"]:
        for topology in TOPOLOGIES:
            for side in SIDES:
                for load in LOADS:
                    for seed in SEEDS:
                        idx += 1
                        args = cli_args(methodology, topology, side, load, seed)
                        run_dir = outdir / "runs" / methodology / topology / f"n{side*side}" / load / f"seed_{seed}"
                        sim_args = "mesh_dv_baseline " + dict_to_cli(args)
                        cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
                        print(f"[{idx:02d}/{total}] {methodology} {topology} n={side*side} load={load} seed={seed}", flush=True)
                        t0 = time.time()
                        proc = run_command(cmd, NS3_DIR, run_dir / "run.log")
                        elapsed = time.time() - t0
                        if proc.returncode != 0:
                            raise SystemExit(f"run failed rc={proc.returncode} at {run_dir}")
                        summary_src = NS3_DIR / "mesh_dv_summary.json"
                        summary_dst = run_dir / "mesh_dv_summary.json"
                        ensure_dir(run_dir)
                        summary_dst.write_bytes(summary_src.read_bytes())
                        summary = load_json(summary_dst)
                        raw_rows.append(extract_raw_row(summary, methodology, topology, side, load, seed, run_dir, elapsed))

    raw_fieldnames = list(raw_rows[0].keys()) if raw_rows else []
    raw_csv = outdir / "pueyo_workload_parity_audit_results_raw.csv"
    write_csv(raw_csv, raw_rows, raw_fieldnames)
    agg_rows = aggregate_rows(raw_rows)
    agg_fieldnames = list(agg_rows[0].keys()) if agg_rows else []
    agg_csv = outdir / "pueyo_workload_parity_audit_results.csv"
    write_csv(agg_csv, agg_rows, agg_fieldnames)
    build_report(outdir, agg_rows)
    summary = {
        "outdir": str(outdir),
        "profile": PROFILE,
        "spacing_m": SPACING_M,
        "loads": LOADS,
        "topologies": TOPOLOGIES,
        "sizes": [s * s for s in SIDES],
        "seeds": SEEDS,
        "methodologies": {
            "current_window": {
                "data_start_sec": DATA_START_SEC,
                "data_stop_sec": CURRENT_DATA_STOP_SEC,
                "stop_sec": CURRENT_STOP_SEC,
                "pdr_end_window_sec": PDR_END_WINDOW_SEC,
            },
            "delayed_start": {
                "data_start_sec": DELAYED_DATA_START_SEC,
                "data_stop_sec": DELAYED_DATA_STOP_SEC,
                "stop_sec": DELAYED_STOP_SEC,
                "pdr_end_window_sec": PDR_END_WINDOW_SEC,
                "formula": "same active traffic duration and drain as current_window, but dataStart shifted later",
            },
            "paper_workload_reference": {
                "formula": "dataStop = dataStart + 100*(N-1)*T(load), stop = dataStop + drain",
                "low_n9_data_stop_sec": DATA_START_SEC + (100 * (9 - 1) * traffic_interval_from_load('low')),
                "low_n25_data_stop_sec": DATA_START_SEC + (100 * (25 - 1) * traffic_interval_from_load('low')),
                "high_n9_data_stop_sec": DATA_START_SEC + (100 * (9 - 1) * traffic_interval_from_load('high')),
                "high_n25_data_stop_sec": DATA_START_SEC + (100 * (25 - 1) * traffic_interval_from_load('high')),
            },
        },
        "runs_total": len(raw_rows),
        "results_csv": str(agg_csv),
        "results_raw_csv": str(raw_csv),
        "report_md": str(outdir / "pueyo_workload_parity_audit_report.md"),
    }
    (outdir / "pueyo_workload_parity_audit_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
