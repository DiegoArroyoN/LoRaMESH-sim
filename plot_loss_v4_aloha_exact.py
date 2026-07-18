#!/usr/bin/env python3
"""
plot_loss_v4_aloha_exact.py  — loss breakdown for ALOHA exact-counter runs
Same logic as plot_loss_v4_aloha.py but reads from *_aloha_exact dirs
(runs with the post-§7 binary that has data_collision_drops / data_busy_drops).
"""

import json, os, argparse
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.patches as mpatches

BASE_GRID    = "/home/diego/ns3-runs/campaigns_aloha_exact"
BASE_RANDOM  = "/home/diego/ns3-runs/campaigns_random_aloha_exact"
BASE_DENSITY = "/home/diego/ns3-runs/campaigns_density_aloha_exact"

NS    = [9, 16, 25, 36, 49, 64]
SEEDS = list(range(1, 11))

ALL_CATS   = ['collision', 'rx_busy', 'no_route', 'ttl', 'queue_full',
              'max_retries', 'other']
ALL_COLORS = ['#C0392B', '#E67E22', '#2471A3', '#8E44AD', '#1E8449',
              '#00838F', '#95A5A6']
ALL_LABELS = ['Air collision', 'Receiver busy', 'No route', 'TTL expired',
              'Queue full', 'Max CSMA retries', 'Other']

ACTIVE_THRESHOLD = 0.005   # hide category if max across all seeds/N < 0.5%


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

    # ── routing losses (exact counters, always available) ────────────────
    d = j['drops']
    rt_no_route = (d.get('drop_no_route', 0)
                   + d.get('drop_no_route_src', 0)
                   + d.get('drop_no_route_relay', 0))
    rt_ttl   = d.get('drop_ttl_expired', 0)
    rt_queue = d.get('drop_queue_overflow', 0)
    rt_max_retries = d.get('drop_max_csma_retries', 0)
    rt_other = d.get('drop_backtrack', 0) + d.get('drop_other', 0)
    routing  = rt_no_route + rt_ttl + rt_queue + rt_max_retries + rt_other

    # ── PHY losses ───────────────────────────────────────────────────────
    cp = j['control_plane']

    # v4 exact counters (present in post-§7 binary)
    exact_col  = cp.get('data_collision_drops')
    exact_busy = cp.get('data_busy_drops')

    if exact_col is not None and exact_busy is not None:
        col_loss  = exact_col
        busy_loss = exact_busy
        method = 'exact'
    else:
        # Fallback: legacy estimation
        phy_loss = max(0, total_loss - routing)
        n_col  = cp.get('pueyo_destructive_overlap_drops', 0)
        n_busy = cp.get('rx_no_more_demodulators', 0)
        denom  = n_col + n_busy
        if denom > 0:
            col_loss  = phy_loss * n_col  / denom
            busy_loss = phy_loss * n_busy / denom
        else:
            col_loss, busy_loss = 0.0, float(phy_loss)
        method = 'estimated'

    residual = max(0, total_loss - col_loss - busy_loss - routing)
    return {
        'collision':  col_loss  / gen,
        'rx_busy':    busy_loss / gen,
        'no_route':   rt_no_route / gen,
        'ttl':        rt_ttl  / gen,
        'queue_full': rt_queue / gen,
        'max_retries': rt_max_retries / gen,
        'other':      (rt_other + residual) / gen,
        '_method':    method,
    }


def collect(path_fn):
    out = {}
    methods = set()
    for N in NS:
        rows = [extract(path_fn(N, s)) for s in SEEDS]
        rows = [r for r in rows if r is not None]
        if rows:
            out[N] = {cat: [r[cat] for r in rows] for cat in ALL_CATS}
            for r in rows:
                methods.add(r['_method'])
    return out, methods


def active_categories(data):
    cats, colors, labels = [], [], []
    for cat, color, label in zip(ALL_CATS, ALL_COLORS, ALL_LABELS):
        max_val = max(max(data[N][cat]) for N in data if data[N][cat])
        if max_val >= ACTIVE_THRESHOLD:
            cats.append(cat)
            colors.append(color)
            labels.append(label)
    return cats, colors, labels


def make_grouped_box(data, methods, outpath):
    ns = [N for N in NS if N in data]
    if not ns:
        print(f"  [WARN] no data — {outpath}")
        return

    cats, colors, labels = active_categories(data)
    n_cats = len(cats)

    group_gap   = 1.0
    group_width = 0.78
    box_w       = group_width / n_cats
    offsets     = np.linspace(-group_width/2 + box_w/2,
                               group_width/2 - box_w/2, n_cats)
    x_centres   = np.arange(len(ns)) * group_gap

    fig, ax = plt.subplots(figsize=(8, 4.8))

    for j, (cat, color, label) in enumerate(zip(cats, colors, labels)):
        positions = x_centres + offsets[j]
        box_data  = [data[N][cat] for N in ns]
        ax.boxplot(
            box_data,
            positions=positions,
            widths=box_w * 0.88,
            patch_artist=True,
            manage_ticks=False,
            medianprops=dict(color='black', linewidth=1.5),
            boxprops=dict(facecolor=color, linewidth=0.8, alpha=0.88),
            whiskerprops=dict(linewidth=0.9, color='#222222'),
            capprops=dict(linewidth=0.9, color='#222222'),
            flierprops=dict(marker='o', markersize=3.5,
                            markerfacecolor=color, markeredgewidth=0,
                            alpha=0.65),
        )

    ax.set_xticks(x_centres)
    ax.set_xticklabels([str(N) for N in ns], fontsize=11)
    ax.set_xlim(x_centres[0] - group_gap * 0.6,
                x_centres[-1] + group_gap * 0.6)
    ax.set_xlabel("Number of nodes", fontsize=12)
    ax.set_ylabel("Drops per generated packet", fontsize=12)

    all_vals = [v for N in ns for cat in cats for v in data[N][cat]]
    y_max = max(all_vals) + 0.04
    ax.set_ylim(0, y_max)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.2f'))
    ax.grid(axis='y', linestyle='--', alpha=0.4, zorder=0)
    ax.tick_params(axis='both', labelsize=10)

    handles = [
        mpatches.Patch(facecolor=c, edgecolor='#333333',
                       linewidth=0.8, alpha=0.88, label=l)
        for c, l in zip(colors, labels)
    ]
    ncol = min(3, n_cats)
    ax.legend(handles=handles,
              loc='upper center',
              bbox_to_anchor=(0.5, -0.17),
              ncol=ncol, fontsize=9,
              framealpha=0.92, edgecolor='#AAAAAA', frameon=True)

    fig.tight_layout(rect=[0, 0.12, 1, 1])
    os.makedirs(os.path.dirname(os.path.abspath(outpath)), exist_ok=True)
    fig.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close(fig)
    method_tag = '+'.join(sorted(methods))
    print(f"  [OK] {os.path.basename(outpath)}  ({n_cats} cats: {cats})  [{method_tag}]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--outdir', default='/tmp/ns3_loss_aloha_exact')
    args = ap.parse_args()
    OUT = args.outdir
    os.makedirs(OUT, exist_ok=True)
    print(f"Output: {OUT}\n")

    TAG11 = {(177,'low'):'fig11a',(177,'high'):'fig11b',
             (248,'low'):'fig11c',(248,'high'):'fig11d'}
    TAG12 = {(177,'low'):'fig12a',(177,'high'):'fig12b',
             (248,'low'):'fig12c',(248,'high'):'fig12d'}

    def pg(sp, ld): return lambda N, s: f"{BASE_GRID}/s{sp}m/{ld}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"
    def pr(sp, ld): return lambda N, s: f"{BASE_RANDOM}/s{sp}m_equiv/{ld}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"
    def pd(tp, ld): return lambda N, s: f"{BASE_DENSITY}/{tp}/{ld}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"

    print("=== Fig 11: Grid, area coverage ===")
    for sp in [177, 248]:
        for ld in ['low', 'high']:
            d, m = collect(pg(sp, ld))
            make_grouped_box(d, m, f"{OUT}/{TAG11[(sp,ld)]}_loss_grid_{sp}m_{ld}.png")

    print("\n=== Fig 12: Random equiv ===")
    for sp in [177, 248]:
        for ld in ['low', 'high']:
            d, m = collect(pr(sp, ld))
            make_grouped_box(d, m, f"{OUT}/{TAG12[(sp,ld)]}_loss_rand_{sp}m_{ld}.png")

    print("\n=== Fig 13: Grid density ===")
    for ld in ['low', 'high']:
        tag = 'fig13a' if ld == 'low' else 'fig13b'
        d, m = collect(pd('grid', ld))
        make_grouped_box(d, m, f"{OUT}/{tag}_loss_density_grid_{ld}.png")

    print("\n=== Fig 14: Random density ===")
    for ld in ['low', 'high']:
        tag = 'fig14a' if ld == 'low' else 'fig14b'
        d, m = collect(pd('random', ld))
        make_grouped_box(d, m, f"{OUT}/{tag}_loss_density_random_{ld}.png")

    print(f"\nDone -> {OUT}")


if __name__ == '__main__':
    main()
