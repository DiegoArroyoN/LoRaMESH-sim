#!/usr/bin/env python3
"""
Genera un reporte automático a partir de los CSV producidos por el simulador LoRaMesh.

Lee:
  - mesh_dv_metrics_tx.csv (timestamp(s),nodeId,seq,dst,...,sf,energyJ,energyFrac,ok)
  - mesh_dv_metrics_rx.csv (timestamp(s),nodeId,src,dst,...,sf,energyJ,energyFrac,forwarded)
  - mesh_dv_metrics_routes.csv
  - mesh_dv_metrics_duty.csv (nodeId,dutyUsed,txCount,backoffCount)
  - mesh_dv_metrics_delay.csv (timestamp(s),src,dst,seq,hops,delay(s),bytes,sf,delivered)
  - mesh_dv_metrics_energy.csv (nodeId,energyInitialJ,energyConsumedJ,energyRemainingJ,energyFrac)

Produce:
  - analysis/results/<label>/summary_metrics.csv
  - figuras PNG en analysis/results/<label>/figs/
"""

import argparse
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def load_csv(path):
    if not Path(path).exists():
        print(f"[WARN] No se encontró {path}, se omite.")
        return None
    return pd.read_csv(path)


def ensure_dirs(label):
    base = Path("analysis/results") / label
    figs = base / "figs"
    base.mkdir(parents=True, exist_ok=True)
    figs.mkdir(parents=True, exist_ok=True)
    return base, figs


def compute_pdr(tx, rx):
    if tx is None or rx is None:
        return {}, {}
    data_tx = tx[(tx["dst"] != 65535) & (tx["ok"] == 1)] if "dst" in tx else tx.copy()
    delivered = rx[rx["dst"] == rx["nodeId"]] if "nodeId" in rx else rx.copy()

    tx_pairs = data_tx[["nodeId", "seq"]].drop_duplicates()
    rx_pairs = delivered[["src", "seq"]].drop_duplicates().rename(columns={"src": "nodeId"})

    sent_total = len(tx_pairs)
    recv_total = len(rx_pairs)
    pdr_global = recv_total / sent_total if sent_total else 0.0

    tx_per_src = tx_pairs.groupby("nodeId").size().rename("tx_unique").reset_index()
    rx_per_src = rx_pairs.groupby("nodeId").size().rename("rx_unique").reset_index()
    merged = pd.merge(tx_per_src, rx_per_src, how="left", on="nodeId").fillna(0)
    merged["pdr"] = merged["rx_unique"] / merged["tx_unique"]
    return {"global": pdr_global}, merged[["nodeId", "tx_unique", "rx_unique", "pdr"]]


def compute_delay_stats(delay_df):
    if delay_df is None or delay_df.empty:
        return {}
    d = delay_df[delay_df["delivered"] == 1]["delay(s)"]
    if d.empty:
        return {}
    return {
        "delay_mean_s": d.mean(),
        "delay_median_s": d.median(),
        "delay_p50_s": d.quantile(0.50),
        "delay_p90_s": d.quantile(0.90),
        "delay_p99_s": d.quantile(0.99),
    }


def compute_hops(rx_df):
    if rx_df is None or rx_df.empty:
        return {}, None
    delivered = rx_df[rx_df["dst"] == rx_df["nodeId"]]
    hop_stats = {
        "hops_avg_delivered": delivered["hops"].mean() if not delivered.empty else 0.0,
        "hops_avg_all_rx": rx_df["hops"].mean() if not rx_df.empty else 0.0,
    }
    per_pkt = delivered[["src", "dst", "seq", "hops"]]
    return hop_stats, per_pkt


def compute_sf_distribution(df, label):
    if df is None or "sf" not in df.columns:
        return None
    return df["sf"].value_counts().sort_index()


def plot_hist(series, title, xlabel, outfile, cumulative=False, bins=50):
    plt.figure()
    if cumulative:
        series.sort_values().plot(drawstyle="steps-post")
    else:
        series.plot(kind="hist", bins=bins)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(outfile)
    plt.close()


def plot_bar(series, title, xlabel, ylabel, outfile):
    plt.figure()
    series.plot(kind="bar")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(outfile)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Análisis automático de métricas LoRaMesh")
    parser.add_argument("--label", required=True, help="Nombre de carpeta de salida (results/<label>/)")
    parser.add_argument("--duty-limit", type=float, default=0.01, help="Duty cycle esperado (p.ej. 0.01 para 1%)")
    args = parser.parse_args()

    base, figs = ensure_dirs(args.label)

    tx = load_csv("mesh_dv_metrics_tx.csv")
    rx = load_csv("mesh_dv_metrics_rx.csv")
    routes = load_csv("mesh_dv_metrics_routes.csv")
    duty = load_csv("mesh_dv_metrics_duty.csv")
    delay = load_csv("mesh_dv_metrics_delay.csv")
    energy = load_csv("mesh_dv_metrics_energy.csv")

    summary_rows = []

    pdr_global, pdr_per_node = compute_pdr(tx, rx)
    summary_rows.append({"metric": "pdr_global", "value": pdr_global.get("global", 0.0)})
    if pdr_per_node is not None:
        pdr_per_node.to_csv(base / "pdr_per_node.csv", index=False)
        plot_bar(pdr_per_node.set_index("nodeId")["pdr"], "PDR por nodo", "nodeId", "PDR", figs / "pdr_per_node.png")

    delay_stats = compute_delay_stats(delay)
    for k, v in delay_stats.items():
        summary_rows.append({"metric": k, "value": v})
    if delay is not None and not delay.empty:
        plot_hist(delay["delay(s)"], "CDF de retardo (ordenado)", "delay (s)", figs / "delay_cdf.png", cumulative=True)

    hop_stats, hops_per_pkt = compute_hops(rx if rx is not None else None)
    for k, v in hop_stats.items():
        summary_rows.append({"metric": k, "value": v})
    if hops_per_pkt is not None and not hops_per_pkt.empty:
        plot_hist(hops_per_pkt["hops"], "Histograma de hops entregados", "hops", figs / "hops_hist.png", bins=range(0, int(hops_per_pkt["hops"].max()) + 2))

    sf_series = compute_sf_distribution(rx if rx is not None else None, "rx")
    if sf_series is not None:
        sf_series.to_csv(base / "sf_distribution.csv", header=["count"])
        plot_bar(sf_series, "Distribución de SF (RX)", "SF", "Count", figs / "sf_distribution.png")

    if duty is not None and not duty.empty:
        duty["dutyLimit"] = args.duty_limit
        duty.to_csv(base / "duty_metrics.csv", index=False)
        plot_bar(duty.set_index("nodeId")["dutyUsed"], "Duty usado por nodo", "nodeId", "dutyUsed", figs / "duty_used_per_node.png")
        summary_rows.append({"metric": "duty_max", "value": duty["dutyUsed"].max()})

    if energy is not None and not energy.empty:
        energy.to_csv(base / "energy_metrics.csv", index=False)
        plot_bar(energy.set_index("nodeId")["energyRemainingJ"], "Energía restante por nodo", "nodeId", "Joules", figs / "energy_remaining_per_node.png")
        summary_rows.append({"metric": "energy_total_consumed", "value": energy["energyConsumedJ"].sum()})

    if routes is not None:
        routes.to_csv(base / "routes_raw.csv", index=False)

    pd.DataFrame(summary_rows).to_csv(base / "summary_metrics.csv", index=False)
    print(f"[OK] Resultados guardados en {base}")


if __name__ == "__main__":
    main()
