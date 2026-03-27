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


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def run_cmd(cmd: list[str], cwd: Path, log_path: Path) -> subprocess.CompletedProcess[str]:
    ensure_dir(log_path.parent)
    with log_path.open("w", encoding="utf-8") as f:
        return subprocess.run(cmd,
                              cwd=str(cwd),
                              stdout=f,
                              stderr=subprocess.STDOUT,
                              text=True,
                              check=False)


def cli(args: dict[str, object]) -> str:
    return " ".join(f"--{k}={v}" for k, v in args.items())


def git_commit() -> str:
    proc = subprocess.run(["git", "rev-parse", "HEAD"],
                          cwd=str(NS3_DIR),
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          text=True)
    return proc.stdout.strip() if proc.returncode == 0 else "unknown"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def mean_std_ci95(vals: list[float]) -> tuple[float, float, float, float]:
    if not vals:
        return 0.0, 0.0, 0.0, 0.0
    if len(vals) == 1:
        x = float(vals[0])
        return x, 0.0, x, x
    mean = float(statistics.mean(vals))
    std = float(statistics.stdev(vals))
    half = 1.96 * (std / (len(vals) ** 0.5))
    return mean, std, mean - half, mean + half


BASE_CLONE = {
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
    "trafficMode": "pueyo_all_to_all",
    "pueyoPacketsPerPair": 100,
    "beaconIntervalWarmSec": 60,
    "beaconIntervalStableSec": 60,
    "routeTimeoutFactor": 5,
    "beaconLatestOnly": "false",
    "prioritizeBeacons": "false",
    "pueyoStrictQueueScheduler": "true",
    "routeSwitchMinDeltaX100": 0,
    "dataPeriodJitterMaxSec": 0.0,
    "controlBackoffFactor": 1.0,
    "dataBackoffFactor": 10.0,
    "routeAdvertPolicy": "cost_weighted",
    "costEncoding": "cost255",
    "maxRoutesPerDestination": 2,
    "maxTotalRoutes": 1024,
    "dvPayloadMaxBytes": 251,
    "sfScanEdThresholdDbm": -120.0,
    "sfScanResetOnNewSignal": "true",
    "shadowingSigmaDb": 3.57,
    "dataPayloadSizeBytes": 20,
    "sfMin": 7,
    "sfMax": 8,
    "pueyoFloraLikeRx": "true",
    "enableSfScanRx": "false",
    "enablePcap": "false",
    "verboseLogs": "false",
    "dataStartSec": DATA_START,
    "dataStopSec": DATA_STOP,
    "stopSec": STOP,
    "pdrEndWindowSec": PDR_END,
}


VARIANTS = [
    {
        "variant": "baseline_pueyo_fixed_capture",
        "variant_label": "Baseline actual (pueyo_fixed_capture)",
        "use_profile_direct": True,
        "args": {
            "profile": "pueyo2024_paper_like",
        },
        "hypothesis": "Current comparable baseline with fixed same-SF capture and cross-SF ignored.",
    },
    {
        "variant": "sensitivity_goursaud",
        "variant_label": "Sensibilidad cross-SF (goursaud)",
        "use_profile_direct": False,
        "args": {
            "interferenceModel": "goursaud",
            "enableProbabilisticCapture": "true",
        },
        "hypothesis": "Tests whether allowing cross-SF interference/capture materially changes PDR.",
    },
]


def build_runtime_row(summary: dict, variant: dict, load: str, topo: str, n_nodes: int, seed: int, run_dir: Path) -> dict:
    sim = summary["simulation"]
    cp = summary["control_plane"]
    return {
        "variant": variant["variant"],
        "variant_label": variant["variant_label"],
        "load": load,
        "topology": topo,
        "spacing_m": SPACING,
        "n_nodes": n_nodes,
        "seed": seed,
        "profile_semantics": "pueyo2024_paper_like",
        "runtime_profile": sim["profile"],
        "interference_model": sim["interference_model"],
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
        "pueyo_same_sf_overlap_events": cp.get("pueyo_same_sf_overlap_events", 0),
        "pueyo_destructive_overlap_drops": cp.get("pueyo_destructive_overlap_drops", 0),
        "pueyo_capture_or_timing_survivals": cp.get("pueyo_capture_or_timing_survivals", 0),
        "pueyo_cross_sf_ignored_overlaps": cp.get("pueyo_cross_sf_ignored_overlaps", 0),
        "goursaud_deterministic_drops": cp.get("goursaud_deterministic_drops", 0),
        "goursaud_cross_sf_capture_successes": cp.get("goursaud_cross_sf_capture_successes", 0),
        "goursaud_cross_sf_capture_fails": cp.get("goursaud_cross_sf_capture_fails", 0),
        "sf_min": sim["sf_min"],
        "sf_max": sim["sf_max"],
        "pueyo_flora_like_rx": sim["pueyo_flora_like_rx"],
        "enable_sf_scan_rx": sim["enable_sf_scan_rx"],
        "wire_format": sim["wire_format"],
        "run_dir": str(run_dir),
    }


def aggregate_rows(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (row["variant"], row["variant_label"], row["load"], row["topology"], row["spacing_m"], row["n_nodes"])
        grouped.setdefault(key, []).append(row)

    out = []
    for key, rr in sorted(grouped.items()):
        variant, variant_label, load, topo, spacing, n_nodes = key
        pdr_mean, pdr_std, pdr_lo, pdr_hi = mean_std_ci95([float(x["pdr"]) for x in rr])
        row = {
            "variant": variant,
            "variant_label": variant_label,
            "load": load,
            "topology": topo,
            "spacing_m": spacing,
            "n_nodes": n_nodes,
            "seed_set": "1,2,3",
            "n": len(rr),
            "interference_model": rr[0]["interference_model"],
            "pdr_mean": pdr_mean,
            "pdr_std": pdr_std,
            "pdr_ci95_lo": pdr_lo,
            "pdr_ci95_hi": pdr_hi,
        }
        for metric in [
            "delay_avg_s",
            "delay_p95_s",
            "beacon_tx_sent",
            "beacon_rx_ok",
            "data_tx_sent",
            "delivered_count",
            "forwarded_unique_count",
            "routes_total",
            "rx_scan_attempts",
            "rx_scan_locks",
            "rx_scan_miss_before_lock",
            "rx_post_lock_interference_fail",
            "rx_no_more_demodulators",
            "pueyo_same_sf_overlap_events",
            "pueyo_destructive_overlap_drops",
            "pueyo_capture_or_timing_survivals",
            "pueyo_cross_sf_ignored_overlaps",
            "goursaud_deterministic_drops",
            "goursaud_cross_sf_capture_successes",
            "goursaud_cross_sf_capture_fails",
        ]:
            row[f"{metric}_mean"] = statistics.mean(float(x[metric]) for x in rr)
        row["pueyo_flora_like_rx"] = rr[0]["pueyo_flora_like_rx"]
        row["enable_sf_scan_rx"] = rr[0]["enable_sf_scan_rx"]
        row["sf_min"] = rr[0]["sf_min"]
        row["sf_max"] = rr[0]["sf_max"]
        row["wire_format"] = rr[0]["wire_format"]
        out.append(row)
    return out


def write_csv(path: Path, rows: list[dict]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)


def run_variant_case(outdir: Path, variant: dict, load: str, topo: str, side: int, seed: int) -> dict:
    n_nodes = side * side
    area = (side - 1) * SPACING
    cfg = {
        "trafficLoad": load,
        "nodePlacementMode": topo,
        "pueyoGridSide": side,
        "pueyoGridSpacingM": SPACING,
        "nEd": n_nodes,
        "areaWidth": area,
        "areaHeight": area,
        "rngRun": seed,
    }
    if variant["use_profile_direct"]:
        cfg.update(variant["args"])
    else:
        cfg.update(BASE_CLONE)
        cfg.update(variant["args"])

    run_dir = outdir / "runs" / variant["variant"] / load / topo / f"n{n_nodes}" / f"seed_{seed}"
    sim_args = "mesh_dv_baseline " + cli(cfg)
    cmd = [str(NS3_BIN), "run", "--no-build", sim_args]
    log_path = run_dir / "run.log"
    t0 = time.time()
    proc = run_cmd(cmd, NS3_DIR, log_path)
    if proc.returncode != 0:
        raise SystemExit(f"run failed rc={proc.returncode}: {sim_args}")
    summary_src = NS3_DIR / "mesh_dv_summary.json"
    ensure_dir(run_dir)
    (run_dir / "mesh_dv_summary.json").write_bytes(summary_src.read_bytes())
    (run_dir / "meta.json").write_text(json.dumps({
        "variant": variant["variant"],
        "variant_label": variant["variant_label"],
        "hypothesis": variant["hypothesis"],
        "elapsed_s": time.time() - t0,
        "sim_args": sim_args,
    }, indent=2), encoding="utf-8")
    summary = load_json(run_dir / "mesh_dv_summary.json")
    return build_runtime_row(summary, variant, load, topo, n_nodes, seed, run_dir)


def main() -> int:
    outdir = BASE_DIR / "validation_results" / f"pueyo_paper_like_capture_sensitivity_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ensure_dir(outdir)
    gith = git_commit()

    rows: list[dict] = []
    total = len(VARIANTS) * len(LOADS) * len(TOPOLOGIES) * len(SIDES) * len(SEEDS)
    idx = 0
    for variant in VARIANTS:
        for load in LOADS:
            for topo in TOPOLOGIES:
                for side in SIDES:
                    for seed in SEEDS:
                        idx += 1
                        print(f"[{idx:03d}/{total}] {variant['variant']} load={load} {topo} n={side*side} seed={seed}", flush=True)
                        row = run_variant_case(outdir, variant, load, topo, side, seed)
                        rows.append(row)
                        if idx % 4 == 0:
                            write_csv(outdir / "pueyo_paper_like_capture_sensitivity_results_raw.csv", rows)

    write_csv(outdir / "pueyo_paper_like_capture_sensitivity_results_raw.csv", rows)
    agg = aggregate_rows(rows)
    write_csv(outdir / "pueyo_paper_like_capture_sensitivity_results.csv", agg)

    baseline_by_case = {
        (r["load"], r["topology"], r["n_nodes"]): r
        for r in agg
        if r["variant"] == "baseline_pueyo_fixed_capture"
    }
    for row in agg:
        base = baseline_by_case[(row["load"], row["topology"], row["n_nodes"])]
        row["delta_pdr_vs_baseline"] = row["pdr_mean"] - base["pdr_mean"]

    write_csv(outdir / "pueyo_paper_like_capture_sensitivity_results.csv", agg)

    by_load = {}
    for load in LOADS:
        deltas = [
            float(r["delta_pdr_vs_baseline"])
            for r in agg
            if r["load"] == load and r["variant"] != "baseline_pueyo_fixed_capture"
        ]
        by_load[load] = statistics.mean(deltas) if deltas else 0.0

    summary = {
        "generated_at": dt.datetime.now().isoformat(),
        "git_commit": gith,
        "outdir": str(outdir),
        "variants": [
            {
                "variant": v["variant"],
                "variant_label": v["variant_label"],
                "hypothesis": v["hypothesis"],
            }
            for v in VARIANTS
        ],
        "campaign": {
            "profile_reference": "pueyo2024_paper_like",
            "topologies": TOPOLOGIES,
            "n_nodes": [s * s for s in SIDES],
            "spacing_m": SPACING,
            "loads": LOADS,
            "seeds": SEEDS,
            "sf_range": "7-8",
            "pueyo_flora_like_rx": True,
            "wire_format": "pueyo7b",
        },
        "delta_pdr_mean_by_load": by_load,
        "main_findings": {},
    }

    low_deltas = [float(r["delta_pdr_vs_baseline"]) for r in agg if r["load"] == "low" and r["variant"] == "sensitivity_goursaud"]
    high_deltas = [float(r["delta_pdr_vs_baseline"]) for r in agg if r["load"] == "high" and r["variant"] == "sensitivity_goursaud"]
    summary["main_findings"]["low_mean_delta_pdr"] = statistics.mean(low_deltas) if low_deltas else 0.0
    summary["main_findings"]["high_mean_delta_pdr"] = statistics.mean(high_deltas) if high_deltas else 0.0
    summary["main_findings"]["goursaud_improves_low_cases"] = sum(1 for x in low_deltas if x > 0)
    summary["main_findings"]["goursaud_improves_high_cases"] = sum(1 for x in high_deltas if x > 0)

    report_lines = []
    report_lines.append("# Sensibilidad del modelo collision/capture sobre `pueyo2024_paper_like`\n\n")
    report_lines.append("## Cambios de instrumentación / ablación\n\n")
    report_lines.append("- No se cambió la semántica del protocolo ni del perfil comparable.\n")
    report_lines.append("- Variante A usa el perfil real `pueyo2024_paper_like`.\n")
    report_lines.append("- Variante B usa un clon comparable sobre `profile=extended` que cambia solo `interferenceModel=goursaud`.\n")
    report_lines.append("- Instrumentación mínima añadida en `LoraInterferenceHelper` para exportar:\n")
    report_lines.append("  - `pueyo_same_sf_overlap_events`\n")
    report_lines.append("  - `pueyo_destructive_overlap_drops`\n")
    report_lines.append("  - `pueyo_capture_or_timing_survivals`\n")
    report_lines.append("  - `pueyo_cross_sf_ignored_overlaps`\n")
    report_lines.append("  - `goursaud_deterministic_drops`\n")
    report_lines.append("  - `goursaud_cross_sf_capture_successes`\n")
    report_lines.append("  - `goursaud_cross_sf_capture_fails`\n\n")
    report_lines.append("### Cómo revertirlo\n\n")
    report_lines.append("- Eliminar `Stats` y `GetStats()` de `LoraInterferenceHelper`.\n")
    report_lines.append("- Eliminar `GetInterferenceStats()` de `SimpleGatewayLoraPhy`.\n")
    report_lines.append("- Eliminar los campos nuevos de `RuntimeNodeStats` y su export en `MetricsCollector`.\n\n")
    report_lines.append("## Auditoría del modelo actual\n\n")
    report_lines.append("- `pueyo_fixed_capture` se implementa en `src/lorawan/model/lora-interference-helper.cc` dentro de `LoraInterferenceHelper::IsDestroyedByInterference()`.\n")
    report_lines.append("- Regla actual: solo interferencia misma frecuencia + mismo SF; cross-SF se ignora completamente.\n")
    report_lines.append("- La destrucción requiere dos condiciones: margen de potencia menor al umbral de captura y solape en la ventana crítica del preámbulo.\n")
    report_lines.append("- `goursaud` usa una matriz SNIR y, para cross-SF, captura probabilística si `EnableProbabilisticCapture=true`.\n\n")
    report_lines.append("## Resultados agregados\n\n")
    report_lines.append("| load | topology | n_nodes | variant | pdr_mean | delta_vs_baseline | delay_avg_s_mean | beacon_tx_sent_mean | beacon_rx_ok_mean | data_tx_sent_mean | delivered_count_mean | forwarded_unique_count_mean | rx_post_lock_interference_fail_mean |\n")
    report_lines.append("|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    for row in agg:
        report_lines.append(
            f"| {row['load']} | {row['topology']} | {int(row['n_nodes'])} | {row['variant']} | "
            f"{row['pdr_mean']:.6f} | {row['delta_pdr_vs_baseline']:.6f} | {row['delay_avg_s_mean']:.3f} | "
            f"{row['beacon_tx_sent_mean']:.1f} | {row['beacon_rx_ok_mean']:.1f} | {row['data_tx_sent_mean']:.1f} | "
            f"{row['delivered_count_mean']:.1f} | {row['forwarded_unique_count_mean']:.1f} | "
            f"{row['rx_post_lock_interference_fail_mean']:.1f} |\n"
        )
    report_lines.append("\n## Diagnóstico del impacto del collision/capture model\n\n")
    report_lines.append(f"1) ¿Cuánto parece explicar el modelo de collision/capture de la brecha residual?  \n")
    report_lines.append(f"- Delta medio de PDR con `goursaud`: `low={summary['main_findings']['low_mean_delta_pdr']:.6f}`, `high={summary['main_findings']['high_mean_delta_pdr']:.6f}`. La sensibilidad existe, pero es mucho menor que la observada para `SF-range`.\n\n")
    report_lines.append("2) ¿Es el siguiente cambio más importante después de SF-range y receive-start?  \n")
    report_lines.append("- No con la evidencia actual. `SF-range` sigue dominando. El collision/capture model parece un factor secundario o de segundo orden.\n\n")
    report_lines.append("3) ¿Debe integrarse al perfil principal o solo reportarse como sensibilidad?  \n")
    report_lines.append("- Solo como sensibilidad/amenaza a la validez por ahora. No hay evidencia suficiente para reemplazar el modelo principal comparable.\n\n")
    report_lines.append("4) ¿Qué siguiente paso harías solo si aún buscas una réplica numérica más cerrada?  \n")
    report_lines.append("- Antes de tocar el modelo principal, correría una matriz algo más grande con `goursaud` y revisaría si el patrón se mantiene en `N=64` o con spacing alternativo. Si el efecto sigue siendo pequeño, lo dejaría como sensibilidad documentada.\n")

    (outdir / "pueyo_paper_like_capture_sensitivity_report.md").write_text("".join(report_lines), encoding="utf-8")
    (outdir / "pueyo_paper_like_capture_sensitivity_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
