#!/usr/bin/env python3
"""
plot_paper_figures.py — Generate all 12 PDR boxplot sub-figures for Pueyo 2024 replication.

Figures produced (saved to OUTDIR):
  Fig 11 (Grid, area coverage):
    fig11a_grid_177m_low.png
    fig11b_grid_177m_high.png
    fig11c_grid_248m_low.png
    fig11d_grid_248m_high.png

  Fig 12 (Random equiv, area coverage):
    fig12a_rand_177m_low.png
    fig12b_rand_177m_high.png
    fig12c_rand_248m_low.png
    fig12d_rand_248m_high.png

  Fig 13 (Grid, fixed 500x500m density):
    fig13a_density_grid_low.png
    fig13b_density_grid_high.png

  Fig 14 (Random, fixed 500x500m density):
    fig14a_density_random_low.png
    fig14b_density_random_high.png

Usage:
  python3 plot_paper_figures.py [--outdir /path/to/output]
"""

import json, sys, os, glob, argparse
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# ── paths ──────────────────────────────────────────────────────────────────
BASE_GRID    = "/home/diego/ns3-runs/campaigns_csmacad"
BASE_RANDOM  = "/home/diego/ns3-runs/campaigns_random_csmacad"
BASE_DENSITY = "/home/diego/ns3-runs/campaigns_density_csmacad"

NS    = [9, 16, 25, 36, 49, 64]
SEEDS = list(range(1, 11))

# ── helpers ────────────────────────────────────────────────────────────────
def load_pdr(path):
    """Load pdr.pdr from a mesh_dv_summary.json. Returns float or None."""
    try:
        with open(path) as f:
            return float(json.load(f)['pdr']['pdr'])
    except Exception:
        return None

def collect_grid(spacing_m, load):
    """Fig 11: grid, fixed spacing, area grows with N."""
    data = {}
    for N in NS:
        vals = []
        for s in SEEDS:
            p = f"{BASE_GRID}/s{spacing_m}m/{load}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"
            v = load_pdr(p)
            if v is not None:
                vals.append(v)
        data[N] = vals
    return data

def collect_random_equiv(spacing_m, load):
    """Fig 12: pueyo_random_equiv, equivalent area to grid."""
    data = {}
    for N in NS:
        vals = []
        for s in SEEDS:
            p = f"{BASE_RANDOM}/s{spacing_m}m_equiv/{load}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"
            v = load_pdr(p)
            if v is not None:
                vals.append(v)
        data[N] = vals
    return data

def collect_density(topo, load):
    """Figs 13+14: fixed 500x500m, grid or random."""
    data = {}
    for N in NS:
        vals = []
        for s in SEEDS:
            p = f"{BASE_DENSITY}/{topo}/{load}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"
            v = load_pdr(p)
            if v is not None:
                vals.append(v)
        data[N] = vals
    return data

# ── plot ───────────────────────────────────────────────────────────────────
def make_boxplot(data, outpath, xlabel="Number of nodes", ylabel="Average PDR"):
    """
    data: dict {N: [pdr_seed1, ..., pdr_seed10]}
    outpath: full path for saved PNG
    """
    ns_present = [N for N in NS if len(data.get(N, [])) > 0]
    if not ns_present:
        print(f"  [WARN] no data for {outpath} — skipping")
        return

    box_data = [data[N] for N in ns_present]
    missing  = [N for N in NS if len(data.get(N, [])) < len(SEEDS)]
    if missing:
        print(f"  [WARN] incomplete data for N={missing} in {os.path.basename(outpath)}")

    all_vals = [v for vs in box_data for v in vs]
    y_min = max(0.0, np.min(all_vals) - 0.05)
    y_max = min(1.0, np.max(all_vals) + 0.05)

    fig, ax = plt.subplots(figsize=(7, 4.5))

    # Boxplot
    bp = ax.boxplot(
        box_data,
        positions=range(len(ns_present)),
        widths=0.5,
        patch_artist=True,
        medianprops=dict(color='black', linewidth=1.5),
        boxprops=dict(facecolor='#AED6F1', linewidth=1),
        whiskerprops=dict(linewidth=1),
        capprops=dict(linewidth=1),
        flierprops=dict(marker='', markersize=0),
    )

    # Jitter points
    rng = np.random.default_rng(42)
    for i, vals in enumerate(box_data):
        jitter = rng.uniform(-0.18, 0.18, len(vals))
        ax.scatter(
            [i + j for j in jitter], vals,
            s=18, color='#2471A3', alpha=0.65, zorder=3
        )

    ax.set_xticks(range(len(ns_present)))
    ax.set_xticklabels([str(N) for N in ns_present], fontsize=11)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_ylim(y_min, y_max)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%.2f'))
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    ax.tick_params(axis='both', labelsize=10)

    fig.tight_layout()
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    fig.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close(fig)
    n_points = sum(len(v) for v in box_data)
    print(f"  [OK] {os.path.basename(outpath)}  ({n_points} data points)")

# ── main ───────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--outdir', default='/tmp/ns3_paper_figures')
    args = ap.parse_args()
    OUT = args.outdir
    os.makedirs(OUT, exist_ok=True)

    print(f"Output directory: {OUT}")
    print()

    # ── Fig 11: Grid, fixed spacing, area grows with N ──
    print("=== Fig 11: Grid topology, area coverage ===")
    for spacing in [177, 248]:
        for load in ['low', 'high']:
            tag = f"fig11{'a' if spacing==177 and load=='low' else 'b' if spacing==177 and load=='high' else 'c' if spacing==248 and load=='low' else 'd'}"
            fname = f"{tag}_grid_{spacing}m_{load}.png"
            data = collect_grid(spacing, load)
            make_boxplot(data, os.path.join(OUT, fname))

    # ── Fig 12: Random equiv, fixed spacing, area grows with N ──
    print()
    print("=== Fig 12: Random topology, area coverage ===")
    for spacing in [177, 248]:
        for load in ['low', 'high']:
            tag = f"fig12{'a' if spacing==177 and load=='low' else 'b' if spacing==177 and load=='high' else 'c' if spacing==248 and load=='low' else 'd'}"
            fname = f"{tag}_rand_{spacing}m_{load}.png"
            data = collect_random_equiv(spacing, load)
            make_boxplot(data, os.path.join(OUT, fname))

    # ── Fig 13: Grid, fixed 500x500m, density ──
    print()
    print("=== Fig 13: Grid topology, fixed-area density ===")
    for load in ['low', 'high']:
        tag = f"fig13{'a' if load=='low' else 'b'}"
        fname = f"{tag}_density_grid_{load}.png"
        data = collect_density('grid', load)
        make_boxplot(data, os.path.join(OUT, fname))

    # ── Fig 14: Random, fixed 500x500m, density ──
    print()
    print("=== Fig 14: Random topology, fixed-area density ===")
    for load in ['low', 'high']:
        tag = f"fig14{'a' if load=='low' else 'b'}"
        fname = f"{tag}_density_random_{load}.png"
        data = collect_density('random', load)
        make_boxplot(data, os.path.join(OUT, fname))

    print()
    print(f"Done. All figures saved to {OUT}")

if __name__ == '__main__':
    main()
