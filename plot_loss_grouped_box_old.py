#!/usr/bin/env python3
"""
plot_loss_grouped_box.py
Grouped boxplots per N: one box per loss cause, showing distribution over 10 seeds.
Style inspired by Pueyo 2024 reference figure.
"""

import json, os, argparse
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

BASE_GRID    = "/home/diego/ns3-runs/campaigns"
BASE_RANDOM  = "/home/diego/ns3-runs/campaigns_random"
BASE_DENSITY = "/home/diego/ns3-runs/campaigns_density"

NS    = [9, 16, 25, 36, 49, 64]
SEEDS = list(range(1, 11))

CATS   = ['collision', 'rx_busy', 'no_route', 'ttl', 'queue_full', 'other']
COLORS = ['#C0392B', '#E67E22', '#2471A3', '#8E44AD', '#1E8449', '#95A5A6']
LABELS = ['Air collision', 'Receiver busy', 'No route', 'TTL expired',
          'Queue full', 'Other']

def extract(path):
    try:
        with open(path) as f:
            j = json.load(f)
    except Exception:
        return None

    gen  = j['pdr']['total_data_generated']
    delv = j['pdr']['delivered']
    if gen == 0:
        return None
    total_loss = gen - delv

    d = j['drops']
    rt_no_route = (d.get('drop_no_route', 0)
                   + d.get('drop_no_route_src', 0)
                   + d.get('drop_no_route_relay', 0))
    rt_ttl   = d.get('drop_ttl_expired', 0)
    rt_queue = d.get('drop_queue_overflow', 0)
    rt_other = d.get('drop_backtrack', 0) + d.get('drop_other', 0)
    routing  = rt_no_route + rt_ttl + rt_queue + rt_other

    phy_loss = max(0, total_loss - routing)
    cp = j['control_plane']
    n_col  = cp.get('pueyo_destructive_overlap_drops', 0)
    n_busy = cp.get('rx_no_more_demodulators', 0)
    denom  = n_col + n_busy
    if denom > 0:
        col_loss  = phy_loss * n_col  / denom
        busy_loss = phy_loss * n_busy / denom
    else:
        col_loss, busy_loss = 0.0, float(phy_loss)

    residual = max(0, total_loss - col_loss - busy_loss - routing)

    return {
        'collision':  col_loss  / gen,
        'rx_busy':    busy_loss / gen,
        'no_route':   rt_no_route / gen,
        'ttl':        rt_ttl  / gen,
        'queue_full': rt_queue / gen,
        'other':      (rt_other + residual) / gen,
    }

def collect(path_fn):
    """{N: {cat: [val_seed1..seed10]}}"""
    out = {}
    for N in NS:
        rows = [extract(path_fn(N, s)) for s in SEEDS]
        rows = [r for r in rows if r is not None]
        if rows:
            out[N] = {cat: [r[cat] for r in rows] for cat in CATS}
    return out

def make_grouped_box(data, outpath):
    ns = [N for N in NS if N in data]
    if not ns:
        print(f"  [WARN] no data — {outpath}")
        return

    n_cats = len(CATS)
    n_groups = len(ns)

    # spacing
    group_gap   = 1.0          # distance between group centres
    group_width = 0.75         # total width occupied by boxes in one group
    box_w       = group_width / n_cats
    offsets     = np.linspace(-group_width/2 + box_w/2,
                               group_width/2 - box_w/2, n_cats)
    x_centres   = np.arange(n_groups) * group_gap

    fig, ax = plt.subplots(figsize=(8, 4.5))

    for j, (cat, color, label) in enumerate(zip(CATS, COLORS, LABELS)):
        positions = x_centres + offsets[j]
        box_data  = [data[N][cat] for N in ns]

        bp = ax.boxplot(
            box_data,
            positions=positions,
            widths=box_w * 0.82,
            patch_artist=True,
            manage_ticks=False,
            medianprops=dict(color='black', linewidth=1.4),
            boxprops=dict(facecolor=color, linewidth=0.8, alpha=0.85),
            whiskerprops=dict(linewidth=0.8, color='#333333'),
            capprops=dict(linewidth=0.8, color='#333333'),
            flierprops=dict(marker='o', markersize=3,
                            markerfacecolor=color, markeredgewidth=0,
                            alpha=0.6),
            label=label,
        )

    # x-axis ticks at group centres
    ax.set_xticks(x_centres)
    ax.set_xticklabels([str(N) for N in ns], fontsize=11)
    ax.set_xlim(x_centres[0] - group_gap*0.6,
                x_centres[-1] + group_gap*0.6)

    ax.set_xlabel("Number of nodes", fontsize=12)
    ax.set_ylabel("Damage to PDR", fontsize=12)

    # y limits: from 0 to just above the max whisker/outlier
    all_vals = [v for N in ns for cat in CATS for v in data[N][cat]]
    y_max = min(1.0, max(all_vals) + 0.04)
    ax.set_ylim(0, y_max)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.2f'))
    ax.grid(axis='y', linestyle='--', alpha=0.4, zorder=0)
    ax.tick_params(axis='both', labelsize=10)

    # legend — use one patch per category (boxplot label trick)
    handles = [
        matplotlib.patches.Patch(facecolor=c, edgecolor='#333333',
                                  linewidth=0.8, alpha=0.85, label=l)
        for c, l in zip(COLORS, LABELS)
    ]
    ax.legend(handles=handles, loc='upper right', fontsize=9,
              framealpha=0.92, edgecolor='#AAAAAA', frameon=True)

    fig.tight_layout()
    os.makedirs(os.path.dirname(os.path.abspath(outpath)), exist_ok=True)
    fig.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  [OK] {os.path.basename(outpath)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--outdir', default='/tmp/ns3_loss_grouped')
    args = ap.parse_args()
    OUT = args.outdir
    os.makedirs(OUT, exist_ok=True)
    print(f"Output: {OUT}\n")

    TAG11 = {(177,'low'):'fig11a',(177,'high'):'fig11b',
             (248,'low'):'fig11c',(248,'high'):'fig11d'}
    TAG12 = {(177,'low'):'fig12a',(177,'high'):'fig12b',
             (248,'low'):'fig12c',(248,'high'):'fig12d'}

    def pg(sp,ld): return lambda N,s: f"{BASE_GRID}/s{sp}m/{ld}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"
    def pr(sp,ld): return lambda N,s: f"{BASE_RANDOM}/s{sp}m_equiv/{ld}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"
    def pd(tp,ld): return lambda N,s: f"{BASE_DENSITY}/{tp}/{ld}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"

    print("=== Fig 11: Grid, area coverage ===")
    for sp in [177, 248]:
        for ld in ['low', 'high']:
            make_grouped_box(collect(pg(sp,ld)),
                             f"{OUT}/{TAG11[(sp,ld)]}_loss_grid_{sp}m_{ld}.png")

    print("\n=== Fig 12: Random equiv, area coverage ===")
    for sp in [177, 248]:
        for ld in ['low', 'high']:
            make_grouped_box(collect(pr(sp,ld)),
                             f"{OUT}/{TAG12[(sp,ld)]}_loss_rand_{sp}m_{ld}.png")

    print("\n=== Fig 13: Grid density ===")
    for ld in ['low', 'high']:
        tag = 'fig13a' if ld == 'low' else 'fig13b'
        make_grouped_box(collect(pd('grid',ld)),
                         f"{OUT}/{tag}_loss_density_grid_{ld}.png")

    print("\n=== Fig 14: Random density ===")
    for ld in ['low', 'high']:
        tag = 'fig14a' if ld == 'low' else 'fig14b'
        make_grouped_box(collect(pd('random',ld)),
                         f"{OUT}/{tag}_loss_density_random_{ld}.png")

    print(f"\nDone → {OUT}")

if __name__ == '__main__':
    main()
