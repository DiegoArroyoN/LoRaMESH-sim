#!/usr/bin/env python3
"""
plot_loss_breakdown_v2.py
Stacked bars (mean over seeds) showing loss cause composition,
overlaid with jitter scatter for the TOTAL loss per seed
— consistent with the PDR boxplot style.
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
        col_loss, busy_loss = 0, phy_loss

    residual = max(0, total_loss - col_loss - busy_loss - routing)

    return {
        'collision':  col_loss  / gen,
        'rx_busy':    busy_loss / gen,
        'no_route':   rt_no_route / gen,
        'ttl':        rt_ttl  / gen,
        'queue_full': rt_queue / gen,
        'other':      (rt_other + residual) / gen,
        'total':      total_loss / gen,
    }

def collect(path_fn):
    """Returns {N: {'means': {cat: float}, 'total_per_seed': [float×10]}}"""
    out = {}
    for N in NS:
        rows = [extract(path_fn(N, s)) for s in SEEDS]
        rows = [r for r in rows if r is not None]
        if rows:
            out[N] = {
                'means': {k: float(np.mean([r[k] for r in rows])) for k in rows[0]},
                'total_per_seed': [r['total'] for r in rows],
            }
    return out

def make_bar(data, outpath):
    ns = [N for N in NS if N in data]
    if not ns:
        print(f"  [WARN] no data — {outpath}")
        return

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = np.arange(len(ns))

    # ── stacked bars (mean) ──────────────────────────────────────────
    bottoms = np.zeros(len(ns))
    for cat, color, label in zip(CATS, COLORS, LABELS):
        vals = np.array([data[N]['means'][cat] for N in ns])
        ax.bar(x, vals, bottom=bottoms,
               color=color, label=label,
               edgecolor='white', linewidth=0.5, zorder=2)
        bottoms += vals

    # ── jitter: total loss per seed ──────────────────────────────────
    rng = np.random.default_rng(42)
    for i, N in enumerate(ns):
        seeds_total = data[N]['total_per_seed']
        jitter = rng.uniform(-0.22, 0.22, len(seeds_total))
        ax.scatter(
            [i + j for j in jitter], seeds_total,
            s=20, color='#1A1A1A', alpha=0.55,
            zorder=4, linewidths=0,
        )

    # ── axes ─────────────────────────────────────────────────────────
    ax.set_xticks(x)
    ax.set_xticklabels([str(N) for N in ns], fontsize=11)
    ax.set_xlabel("Number of nodes", fontsize=12)
    ax.set_ylabel("Damage to PDR", fontsize=12)

    all_totals = [v for N in ns for v in data[N]['total_per_seed']]
    y_max = min(1.0, max(all_totals) + 0.05)
    ax.set_ylim(0, y_max)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.2f'))
    ax.grid(axis='y', linestyle='--', alpha=0.4, zorder=1)
    ax.tick_params(axis='both', labelsize=10)

    ax.legend(loc='upper right', fontsize=9,
              framealpha=0.92, edgecolor='#AAAAAA', frameon=True)

    fig.tight_layout()
    os.makedirs(os.path.dirname(os.path.abspath(outpath)), exist_ok=True)
    fig.savefig(outpath, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  [OK] {os.path.basename(outpath)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--outdir', default='/tmp/ns3_loss_v2')
    args = ap.parse_args()
    OUT = args.outdir
    os.makedirs(OUT, exist_ok=True)
    print(f"Output: {OUT}\n")

    TAG11 = {(177,'low'):'fig11a',(177,'high'):'fig11b',
             (248,'low'):'fig11c',(248,'high'):'fig11d'}
    TAG12 = {(177,'low'):'fig12a',(177,'high'):'fig12b',
             (248,'low'):'fig12c',(248,'high'):'fig12d'}

    def pg(sp, ld): return lambda N,s: f"{BASE_GRID}/s{sp}m/{ld}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"
    def pr(sp, ld): return lambda N,s: f"{BASE_RANDOM}/s{sp}m_equiv/{ld}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"
    def pd(tp, ld): return lambda N,s: f"{BASE_DENSITY}/{tp}/{ld}/N{N:02d}/seed{s:02d}/mesh_dv_summary.json"

    print("=== Fig 11: Grid, area coverage ===")
    for sp in [177, 248]:
        for ld in ['low', 'high']:
            make_bar(collect(pg(sp, ld)), f"{OUT}/{TAG11[(sp,ld)]}_loss_grid_{sp}m_{ld}.png")

    print("\n=== Fig 12: Random equiv, area coverage ===")
    for sp in [177, 248]:
        for ld in ['low', 'high']:
            make_bar(collect(pr(sp, ld)), f"{OUT}/{TAG12[(sp,ld)]}_loss_rand_{sp}m_{ld}.png")

    print("\n=== Fig 13: Grid density ===")
    for ld in ['low', 'high']:
        tag = 'fig13a' if ld == 'low' else 'fig13b'
        make_bar(collect(pd('grid', ld)), f"{OUT}/{tag}_loss_density_grid_{ld}.png")

    print("\n=== Fig 14: Random density ===")
    for ld in ['low', 'high']:
        tag = 'fig14a' if ld == 'low' else 'fig14b'
        make_bar(collect(pd('random', ld)), f"{OUT}/{tag}_loss_density_random_{ld}.png")

    print(f"\nDone → {OUT}")

if __name__ == '__main__':
    main()
