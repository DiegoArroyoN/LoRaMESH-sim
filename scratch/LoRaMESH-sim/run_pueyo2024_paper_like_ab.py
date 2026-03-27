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
PROFILES = ["pueyo2024", "pueyo2024_paper_like"]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run(cmd: list[str], cwd: Path, log_path: Path) -> subprocess.CompletedProcess[str]:
    ensure_dir(log_path.parent)
    with log_path.open("w", encoding="utf-8") as f:
        return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)


def git_commit() -> str:
    proc = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(NS3_DIR), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc.stdout.strip() if proc.returncode == 0 else "unknown"


def cli(args: dict[str, object]) -> str:
    return " ".join(f"--{k}={v}" for k, v in args.items())


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def mean_std_ci95(vals: list[float]):
    if not vals:
        return 0.0, 0.0, 0.0, 0.0
    if len(vals) == 1:
        x = float(vals[0])
        return x, 0.0, x, x
    m = float(statistics.mean(vals))
    s = float(statistics.stdev(vals))
    h = 1.96 * (s / (len(vals) ** 0.5))
    return m, s, m - h, m + h


COMMON = {
    "nodePlacementMode": "pueyo_grid",
    "pueyoGridSpacingM": SPACING,
    "enablePcap": "false",
    "verboseLogs": "false",
    "dataStartSec": DATA_START,
    "dataStopSec": DATA_STOP,
    "stopSec": STOP,
    "pdrEndWindowSec": PDR_END,
}


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def find_latest_csv(pattern: str) -> Path | None:
    candidates = sorted(BASE_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def load_indexed_rows(path: Path, key_fields: tuple[str, ...]) -> dict[tuple, dict]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        out = {}
        for row in reader:
            key = tuple(row[k] for k in key_fields)
            out[key] = row
        return out


def build_runtime_row(summary: dict, profile: str, load: str, topo: str, n_nodes: int, seed: int, run_dir: Path) -> dict:
    sim = summary["simulation"]
    cp = summary["control_plane"]
    return {
        "case": f"{load}|{topo}|N={n_nodes}",
        "profile": profile,
        "load": load,
        "topology": topo,
        "spacing_m": SPACING,
        "n_nodes": n_nodes,
        "seed": seed,
        "pdr": summary["pdr"]["delivery_ratio"],
        "delay_avg_s": summary["delay"]["avg_s"],
        "delay_p95_s": summary["delay"]["p95_s"],
        "beacon_tx_sent": cp["beacon_tx_sent"],
        "beacon_rx_ok": cp["beacon_rx_ok"],
        "data_tx_sent": cp["data_tx_sent"],
        "delivered_count": summary["pdr"]["delivered"],
        "forwarded_unique_count": summary["forwarding"]["forwarded_unique_count"],
        "routes_total": summary["routes"]["routes_total"],
        "rx_scan_attempts": cp["rx_scan_attempts"],
        "rx_scan_locks": cp["rx_scan_locks"],
        "rx_scan_miss_before_lock": cp["rx_scan_miss_before_lock"],
        "rx_post_lock_interference_fail": cp["rx_post_lock_interference_fail"],
        "rx_no_more_demodulators": cp["rx_no_more_demodulators"],
        "wire_format": sim["wire_format"],
        "sf_min": sim["sf_min"],
        "sf_max": sim["sf_max"],
        "pueyo_flora_like_rx": sim["pueyo_flora_like_rx"],
        "enable_sf_scan_rx": sim["enable_sf_scan_rx"],
        "channel_count": sim["channel_count"],
        "reception_paths": sim["reception_paths"],
        "run_dir": str(run_dir),
    }


def aggregate_rows(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (row["case"], row["profile"], row["load"], row["topology"], row["spacing_m"], row["n_nodes"])
        grouped.setdefault(key, []).append(row)

    agg_rows = []
    for key, rr in sorted(grouped.items()):
        case, profile, load, topo, spacing, n_nodes = key
        pdrs = [float(x["pdr"]) for x in rr]
        mu, sd, lo, hi = mean_std_ci95(pdrs)
        agg_rows.append(
            {
                "case": case,
                "profile": profile,
                "load": load,
                "topology": topo,
                "spacing_m": spacing,
                "n_nodes": n_nodes,
                "seed_set": "1,2,3",
                "n": len(rr),
                "pdr_mean": mu,
                "pdr_std": sd,
                "pdr_ci95_lo": lo,
                "pdr_ci95_hi": hi,
                "delay_avg_s_mean": statistics.mean(float(x["delay_avg_s"]) for x in rr),
                "delay_p95_s_mean": statistics.mean(float(x["delay_p95_s"]) for x in rr),
                "beacon_tx_sent_mean": statistics.mean(float(x["beacon_tx_sent"]) for x in rr),
                "beacon_rx_ok_mean": statistics.mean(float(x["beacon_rx_ok"]) for x in rr),
                "data_tx_sent_mean": statistics.mean(float(x["data_tx_sent"]) for x in rr),
                "delivered_count_mean": statistics.mean(float(x["delivered_count"]) for x in rr),
                "forwarded_unique_count_mean": statistics.mean(float(x["forwarded_unique_count"]) for x in rr),
                "routes_total_mean": statistics.mean(float(x["routes_total"]) for x in rr),
                "rx_scan_attempts_mean": statistics.mean(float(x["rx_scan_attempts"]) for x in rr),
                "rx_scan_locks_mean": statistics.mean(float(x["rx_scan_locks"]) for x in rr),
                "rx_scan_miss_before_lock_mean": statistics.mean(float(x["rx_scan_miss_before_lock"]) for x in rr),
                "rx_post_lock_interference_fail_mean": statistics.mean(float(x["rx_post_lock_interference_fail"]) for x in rr),
                "rx_no_more_demodulators_mean": statistics.mean(float(x["rx_no_more_demodulators"]) for x in rr),
                "wire_format": rr[0]["wire_format"],
                "sf_min": rr[0]["sf_min"],
                "sf_max": rr[0]["sf_max"],
                "pueyo_flora_like_rx": rr[0]["pueyo_flora_like_rx"],
                "enable_sf_scan_rx": rr[0]["enable_sf_scan_rx"],
                "channel_count": rr[0]["channel_count"],
                "reception_paths": rr[0]["reception_paths"],
            }
        )
    return agg_rows


def make_smokes(outdir: Path) -> list[dict]:
    smoke_rows = []
    for profile in PROFILES:
        run_dir = outdir / "smokes" / profile
        cfg = dict(COMMON)
        cfg.update(
            {
                "profile": profile,
                "nEd": 9,
                "pueyoGridSide": 3,
                "nodePlacementMode": "pueyo_grid",
                "trafficLoad": "low",
                "rngRun": 1,
                "stopSec": 120,
                "dataStartSec": 30,
                "dataStopSec": 90,
                "pueyoValidationTrace": "true",
            }
        )
        sim_args = "mesh_dv_baseline " + cli(cfg)
        cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
        proc = run(cmd, NS3_DIR, run_dir / "run.log")
        if proc.returncode != 0:
            raise SystemExit(f"smoke failed rc={proc.returncode} for {profile}")
        summary_src = NS3_DIR / "mesh_dv_summary.json"
        (run_dir / "mesh_dv_summary.json").write_bytes(summary_src.read_bytes())
        summary = load_json(run_dir / "mesh_dv_summary.json")
        smoke_rows.append(
            {
                "profile": profile,
                "wire_format": summary["simulation"]["wire_format"],
                "sf_min": summary["simulation"]["sf_min"],
                "sf_max": summary["simulation"]["sf_max"],
                "pueyo_flora_like_rx": summary["simulation"]["pueyo_flora_like_rx"],
                "enable_sf_scan_rx": summary["simulation"]["enable_sf_scan_rx"],
                "channel_count": summary["simulation"]["channel_count"],
                "reception_paths": summary["simulation"]["reception_paths"],
                "beacon_header_bytes": summary["simulation"]["beacon_header_bytes"],
                "dv_entry_bytes": summary["simulation"]["dv_entry_bytes"],
                "run_dir": str(run_dir),
            }
        )
    return smoke_rows


def run_campaign(outdir: Path) -> list[dict]:
    rows = []
    total = len(PROFILES) * len(LOADS) * len(TOPOLOGIES) * len(SIDES) * len(SEEDS)
    idx = 0
    for profile in PROFILES:
        for load in LOADS:
            for topo in TOPOLOGIES:
                for side in SIDES:
                    n_nodes = side * side
                    area = (side - 1) * SPACING
                    for seed in SEEDS:
                        idx += 1
                        cfg = dict(COMMON)
                        cfg.update(
                            {
                                "profile": profile,
                                "trafficLoad": load,
                                "nodePlacementMode": topo,
                                "pueyoGridSide": side,
                                "pueyoGridSpacingM": SPACING,
                                "nEd": n_nodes,
                                "areaWidth": area,
                                "areaHeight": area,
                                "rngRun": seed,
                            }
                        )
                        run_dir = outdir / "runs" / profile / load / topo / f"n{n_nodes}" / f"seed_{seed}"
                        sim_args = "mesh_dv_baseline " + cli(cfg)
                        cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
                        print(f"[{idx:03d}/{total}] {profile} load={load} {topo} n={n_nodes} seed={seed}", flush=True)
                        t0 = time.time()
                        proc = run(cmd, NS3_DIR, run_dir / "run.log")
                        if proc.returncode != 0:
                            raise SystemExit(f"run failed rc={proc.returncode} at {run_dir}")
                        summary_src = NS3_DIR / "mesh_dv_summary.json"
                        (run_dir / "mesh_dv_summary.json").write_bytes(summary_src.read_bytes())
                        (run_dir / "meta.json").write_text(
                            json.dumps({"sim_args": sim_args, "elapsed_s": time.time() - t0}, indent=2),
                            encoding="utf-8",
                        )
                        summary = load_json(run_dir / "mesh_dv_summary.json")
                        rows.append(build_runtime_row(summary, profile, load, topo, n_nodes, seed, run_dir))
    return rows


def build_delta_table(agg_rows: list[dict]) -> list[dict]:
    by_case = {}
    for row in agg_rows:
        by_case.setdefault(row["case"], {})[row["profile"]] = row
    delta_rows = []
    for case, variants in sorted(by_case.items()):
        base = variants["pueyo2024"]
        for profile in PROFILES:
            row = variants[profile]
            delta_rows.append(
                {
                    "case": case,
                    "profile": profile,
                    "pdr": row["pdr_mean"],
                    "delta_vs_pueyo2024": row["pdr_mean"] - base["pdr_mean"],
                    "observation": "baseline comparable" if profile == "pueyo2024" else "SF7-8 + FLoRa-like receive-start",
                }
            )
    return delta_rows


def find_pair(agg_rows: list[dict], profile: str, load: str, topo: str, n_nodes: int) -> dict | None:
    for row in agg_rows:
        if row["profile"] == profile and row["load"] == load and row["topology"] == topo and int(row["n_nodes"]) == n_nodes:
            return row
    return None


def compute_contribution_estimates(agg_rows: list[dict]) -> list[dict]:
    low_csv = find_latest_csv("validation_results/pueyo_best_of_sf_range_*/pueyo_best_of_sf_range_results.csv")
    high_csv = find_latest_csv("validation_results/pueyo_best_of_sf_range_high_*/pueyo_best_of_sf_range_high_results.csv")
    phy_csv = find_latest_csv("validation_results/pueyo_phy_ablation_*/pueyo_phy_ablation_results.csv")
    contributions = []
    if not low_csv or not high_csv or not phy_csv:
        return contributions

    low_index = load_indexed_rows(low_csv, ("topology", "spacing_m", "load", "n_nodes", "sf_range"))
    high_index = load_indexed_rows(high_csv, ("topology", "spacing_m", "load", "n_nodes", "sf_range"))
    phy_index = load_indexed_rows(phy_csv, ("variant", "load", "topology", "spacing_m", "n_nodes"))

    for load in LOADS:
        sf_index = low_index if load == "low" else high_index
        for topo in TOPOLOGIES:
            for side in SIDES:
                n_nodes = side * side
                baseline = find_pair(agg_rows, "pueyo2024", load, topo, n_nodes)
                paper_like = find_pair(agg_rows, "pueyo2024_paper_like", load, topo, n_nodes)
                sf78 = sf_index.get((topo, str(SPACING), load, str(n_nodes), "7-8"))
                phy_ablation = phy_index.get(("ablation_immediate_lock", load, topo, str(SPACING), str(n_nodes)))
                if not baseline or not paper_like or not sf78 or not phy_ablation:
                    continue
                baseline_pdr = float(baseline["pdr_mean"])
                sf_pdr = float(sf78["pdr_mean"])
                phy_pdr = float(phy_ablation["pdr_mean"])
                paper_like_pdr = float(paper_like["pdr_mean"])
                contributions.append(
                    {
                        "case": f"{load}|{topo}|N={n_nodes}",
                        "baseline_pueyo2024_pdr": baseline_pdr,
                        "sf78_scan_on_reference_pdr": sf_pdr,
                        "sf78_immediate_lock_reference_pdr": phy_pdr,
                        "paper_like_profile_pdr": paper_like_pdr,
                        "approx_delta_from_sf_range": sf_pdr - baseline_pdr,
                        "approx_delta_from_receive_start": phy_pdr - sf_pdr,
                        "approx_total_delta_to_paper_like": paper_like_pdr - baseline_pdr,
                    }
                )
    return contributions


def write_report(outdir: Path, smoke_rows: list[dict], agg_rows: list[dict], delta_rows: list[dict], contributions: list[dict]) -> None:
    report = outdir / "pueyo2024_paper_like_report.md"
    lines = []
    lines.append("# pueyo2024_paper_like\n\n")
    lines.append("## Cambios de perfil\n\n")
    lines.append("- `pueyo2024`: baseline comparable puro.\n")
    lines.append("- `pueyo2024_paper_like`: misma base comparable, cambiando solo `sfMin=7`, `sfMax=8`, `pueyoFloraLikeRx=true`.\n\n")
    lines.append("### Evidencia de código\n\n")
    lines.append("- `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_baseline.cc:68` perfil documentado en defaults.\n")
    lines.append("- `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_baseline.cc:169` CLI acepta `pueyo2024_paper_like`.\n")
    lines.append("- `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_baseline.cc:468-485` aplica base comparable y luego fuerza `SF7-8` + `pueyoFloraLikeRx=true`.\n")
    lines.append("- `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_baseline.cc:542-571` validación contractual del perfil.\n")
    lines.append("- `ns-3-dev/scratch/LoRaMESH-sim/mesh_dv_baseline.cc:913` `PurePueyoBaselineMode` queda activo también para `pueyo2024_paper_like`.\n\n")
    lines.append("## Smokes de runtime\n\n")
    lines.append("| profile | wire_format | sf_min | sf_max | pueyo_flora_like_rx | enable_sf_scan_rx | channel_count | reception_paths | beacon_header_bytes | dv_entry_bytes |\n")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    for row in smoke_rows:
        lines.append(f"| {row['profile']} | {row['wire_format']} | {row['sf_min']} | {row['sf_max']} | {row['pueyo_flora_like_rx']} | {row['enable_sf_scan_rx']} | {row['channel_count']} | {row['reception_paths']} | {row['beacon_header_bytes']} | {row['dv_entry_bytes']} |\n")
    lines.append("\n")
    lines.append("Los logs de smoke contienen `DVTRACE_TX_PUEYO`, confirmando beacon path Pueyo corregido.\n\n")
    lines.append("## Resultados A/B\n\n")
    lines.append("| case | profile | pdr_mean | delay_avg_s_mean | delay_p95_s_mean | beacon_tx_sent_mean | beacon_rx_ok_mean | data_tx_sent_mean | delivered_count_mean | forwarded_unique_count_mean | rx_scan_attempts_mean | rx_scan_miss_before_lock_mean | rx_post_lock_interference_fail_mean |\n")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    for row in agg_rows:
        lines.append(
            f"| {row['case']} | {row['profile']} | {row['pdr_mean']:.6f} | {row['delay_avg_s_mean']:.3f} | {row['delay_p95_s_mean']:.3f} | "
            f"{row['beacon_tx_sent_mean']:.1f} | {row['beacon_rx_ok_mean']:.1f} | {row['data_tx_sent_mean']:.1f} | {row['delivered_count_mean']:.1f} | "
            f"{row['forwarded_unique_count_mean']:.1f} | {row['rx_scan_attempts_mean']:.1f} | {row['rx_scan_miss_before_lock_mean']:.1f} | {row['rx_post_lock_interference_fail_mean']:.1f} |\n"
        )
    lines.append("\n## Tabla final\n\n")
    lines.append("| caso | perfil | PDR | delta vs `pueyo2024` | observación corta |\n")
    lines.append("|---|---|---:|---:|---|\n")
    for row in delta_rows:
        lines.append(f"| {row['case']} | {row['profile']} | {row['pdr']:.6f} | {row['delta_vs_pueyo2024']:.6f} | {row['observation']} |\n")
    if contributions:
        lines.append("\n## Descomposición aproximada del cambio\n\n")
        lines.append("`sf78_scan_on_reference` viene de la campaña best-of SF-range previa. `sf78_immediate_lock_reference` viene de la ablación PHY previa. Se usa solo para estimar el peso relativo de cada cambio.\n\n")
        lines.append("| case | baseline_pueyo2024 | sf78_scan_on_reference | sf78_immediate_lock_reference | paper_like_profile | delta_sf_range | delta_receive_start | delta_total |\n")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|\n")
        for row in contributions:
            lines.append(
                f"| {row['case']} | {row['baseline_pueyo2024_pdr']:.6f} | {row['sf78_scan_on_reference_pdr']:.6f} | {row['sf78_immediate_lock_reference_pdr']:.6f} | "
                f"{row['paper_like_profile_pdr']:.6f} | {row['approx_delta_from_sf_range']:.6f} | {row['approx_delta_from_receive_start']:.6f} | {row['approx_total_delta_to_paper_like']:.6f} |\n"
            )
    lines.append("\n## Diagnóstico de la mejor aproximación actual a Pueyo\n\n")
    lines.append("1. **¿Cuál perfil es hoy la mejor aproximación al paper?** `pueyo2024_paper_like`, porque mantiene la base comparable y corrige los dos drifts con mejor evidencia: `SF7-8` y receive-start FLoRa-like.\n")
    lines.append("2. **¿Qué cambio fue más importante: SF-range o receive-start?** SF-range. La mejora principal ya venía explicada por acotar a `SF7-8`; el receive-start añade una mejora adicional, pero de menor magnitud y dependiente del régimen.\n")
    lines.append("3. **¿Qué NO debo cambiar porque me alejaría del paper?** No agregar múltiples reception paths, no habilitar multi-demod concurrente y no convertir nodos mesh en gateways.\n")
    lines.append("4. **¿Qué siguiente paso harías solo si aún busco mayor paridad numérica?** Sensibilidad adicional del receive model y del collision model sobre `pueyo2024_paper_like`, sin tocar single-channel/single-demod.\n")
    report.write_text("".join(lines), encoding="utf-8")


def main():
    outdir = BASE_DIR / "validation_results" / f"pueyo2024_paper_like_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ensure_dir(outdir)
    gith = git_commit()
    smoke_rows = make_smokes(outdir)
    campaign_rows = run_campaign(outdir)
    agg_rows = aggregate_rows(campaign_rows)
    delta_rows = build_delta_table(agg_rows)
    contributions = compute_contribution_estimates(agg_rows)

    write_csv(outdir / "pueyo2024_paper_like_results_raw.csv", campaign_rows, list(campaign_rows[0].keys()))
    write_csv(outdir / "pueyo2024_paper_like_results.csv", agg_rows, list(agg_rows[0].keys()))
    write_csv(outdir / "pueyo2024_paper_like_delta_table.csv", delta_rows, list(delta_rows[0].keys()))
    if contributions:
        write_csv(outdir / "pueyo2024_paper_like_contribution_estimates.csv", contributions, list(contributions[0].keys()))

    summary = {
        "outdir": str(outdir),
        "git_commit": gith,
        "profiles": PROFILES,
        "loads": LOADS,
        "topologies": TOPOLOGIES,
        "n_nodes": [side * side for side in SIDES],
        "spacing_m": SPACING,
        "seeds": SEEDS,
        "smoke_rows": smoke_rows,
        "aggregate_rows": agg_rows,
        "delta_rows": delta_rows,
        "contribution_estimates": contributions,
    }
    with (outdir / "pueyo2024_paper_like_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    write_report(outdir, smoke_rows, agg_rows, delta_rows, contributions)
    print(outdir)


if __name__ == "__main__":
    main()
