#!/usr/bin/env python3
import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROFILE_LABELS = {
    "pueyo2024": "Referencia ToA-only",
    "proposal_pueyo_like": "Propuesta cross-layer",
    "proposal_pueyo_like_observed": "Propuesta SF observado",
}

TOPO_LABELS = {
    "pueyo_grid": "Grid",
    "pueyo_random_equiv": "Aleatoria (area equivalente)",
}

SPACING_STYLES = {
    177: {"linestyle": "-", "marker": "o"},
    247: {"linestyle": "--", "marker": "s"},
}

PROFILE_COLORS = {
    "pueyo2024": "#1f77b4",
    "proposal_pueyo_like": "#d62728",
    "proposal_pueyo_like_observed": "#2ca02c",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--agg", required=True, help="Path a results_agg.csv")
    ap.add_argument("--outdir", required=True, help="Directorio de salida")
    ap.add_argument("--loads", nargs="+", default=["low", "medium"])
    ap.add_argument("--spacings", nargs="+", type=int, default=[177, 247])
    ap.add_argument("--profiles", nargs="+", default=["pueyo2024", "proposal_pueyo_like"])
    ap.add_argument("--topologies", nargs="+", default=["pueyo_grid", "pueyo_random_equiv"])
    return ap.parse_args()


def format_series_label(profile: str, spacing_m: int) -> str:
    return f"{PROFILE_LABELS.get(profile, profile)} | {spacing_m} m"


def plot_topology(df: pd.DataFrame, topology: str, loads: list[str], spacings: list[int], profiles: list[str], outpath: str) -> None:
    ncols = len(loads)
    fig, axes = plt.subplots(1, ncols, figsize=(6.0 * ncols, 4.2), sharey=True)
    if ncols == 1:
        axes = [axes]

    topo_title = TOPO_LABELS.get(topology, topology)
    plotted_any = False

    for ax, load in zip(axes, loads):
        sub = df[(df["topology"] == topology) & (df["load"] == load)]
        if sub.empty:
            ax.set_visible(False)
            continue

        for profile in profiles:
            for spacing_m in spacings:
                g = sub[(sub["profile"] == profile) & (sub["grid_spacing_m"] == spacing_m)].copy()
                if g.empty:
                    continue

                g = g.sort_values("n_nodes")
                x = g["n_nodes"].astype(int).to_numpy()
                y = g["delivery_ratio_mean"].astype(float).to_numpy()
                lo = g["delivery_ratio_ci95_lo"].astype(float).to_numpy()
                hi = g["delivery_ratio_ci95_hi"].astype(float).to_numpy()
                yerr = np.vstack([np.maximum(y - lo, 0.0), np.maximum(hi - y, 0.0)])

                style = SPACING_STYLES.get(int(spacing_m), {"linestyle": "-", "marker": "o"})
                color = PROFILE_COLORS.get(profile)
                ax.errorbar(
                    x,
                    y,
                    yerr=yerr,
                    label=format_series_label(profile, int(spacing_m)),
                    color=color,
                    linestyle=style["linestyle"],
                    marker=style["marker"],
                    linewidth=1.2,
                    markersize=4.5,
                    capsize=3,
                )
                plotted_any = True

        ax.set_title(f"{topo_title} | carga={load}")
        ax.set_xlabel("Numero de nodos (N)")
        ax.set_ylim(0.0, 1.0)
        ax.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.7)

    axes[0].set_ylabel("PDR promedio E2E")

    handles, labels = [], []
    for ax in axes:
        h, l = ax.get_legend_handles_labels()
        for hh, ll in zip(h, l):
            if ll not in labels:
                handles.append(hh)
                labels.append(ll)
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2, fontsize=8, frameon=False)

    fig.suptitle(f"PDR promedio E2E vs N - {topo_title}", y=1.02)
    fig.tight_layout()
    fig.subplots_adjust(top=0.78 if handles else 0.88)
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    fig.savefig(outpath, bbox_inches="tight")
    plt.close(fig)

    if not plotted_any:
        raise RuntimeError(f"No hay datos para topology={topology}")


def main() -> None:
    args = parse_args()
    df = pd.read_csv(args.agg)

    required = {
        "topology",
        "load",
        "profile",
        "grid_spacing_m",
        "n_nodes",
        "delivery_ratio_mean",
        "delivery_ratio_ci95_lo",
        "delivery_ratio_ci95_hi",
    }
    missing = required.difference(df.columns)
    if missing:
        raise RuntimeError(f"Faltan columnas en results_agg.csv: {sorted(missing)}")

    df = df[
        df["topology"].isin(args.topologies)
        & df["load"].isin(args.loads)
        & df["profile"].isin(args.profiles)
        & df["grid_spacing_m"].isin(args.spacings)
    ].copy()

    if df.empty:
        raise RuntimeError("No hay filas tras aplicar filtros")

    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    for topology in args.topologies:
        outname = "fig11_pdr_pueyo_grid.pdf" if topology == "pueyo_grid" else "fig12_pdr_pueyo_random_equiv.pdf"
        plot_topology(
            df,
            topology=topology,
            loads=args.loads,
            spacings=args.spacings,
            profiles=args.profiles,
            outpath=os.path.join(outdir, outname),
        )


if __name__ == "__main__":
    main()
