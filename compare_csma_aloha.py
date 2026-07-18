#!/usr/bin/env python3
"""compare_csma_aloha.py — side-by-side comparison of CSMA/CAD vs ALOHA
across all 4 paper campaigns (Fig 11/12/13/14).

For each (campaign, axis-cell) computes mean PDR for both MACs and the
relative delta. Prints a per-figure table and a summary of how many cells
improved / regressed / stayed flat.
"""
import json
import glob
from collections import defaultdict

# (label, base_aloha, base_csma)
CAMPAIGNS = [
    ("Fig 11 — Grid, area coverage",
     "/home/diego/ns3-runs/campaigns",
     "/home/diego/ns3-runs/campaigns_csmacad",
     "spacing-load-N"),
    ("Fig 12 — Random, area coverage",
     "/home/diego/ns3-runs/campaigns_random",
     "/home/diego/ns3-runs/campaigns_random_csmacad",
     "spacing-load-N"),
    ("Fig 13/14 — Fixed-area density (grid+random)",
     "/home/diego/ns3-runs/campaigns_density",
     "/home/diego/ns3-runs/campaigns_density_csmacad",
     "topology-load-N"),
]
SEEDS = list(range(1, 11))
NS = [9, 16, 25, 36, 49, 64]


def read_pdr(path):
    try:
        with open(path) as f:
            j = json.load(f)
        p = j.get("pdr", {})
        g = p.get("total_data_generated", 0) or 0
        d = p.get("delivered", 0) or 0
        return d / g if g > 0 else None
    except Exception:
        return None


def collect(base, axis_kind):
    """axis_kind:
         'spacing-load-N'  → key = (spacing_str, load, N)  (Fig 11/12)
         'topology-load-N' → key = (topology_str, load, N) (Fig 13/14)
    """
    cells = defaultdict(list)
    pattern = f"{base}/**/mesh_dv_summary.json"
    for path in glob.glob(pattern, recursive=True):
        rel = path.replace(base + "/", "")
        parts = rel.split("/")
        if axis_kind == "spacing-load-N":
            # campaigns/s177m/low/N09/seed01/mesh_dv_summary.json
            # campaigns_random/s177m_equiv/low/N09/seed01/...
            if len(parts) < 4:
                continue
            spacing, load, N = parts[0], parts[1], parts[2]
            key = (spacing, load, N)
        elif axis_kind == "topology-load-N":
            # campaigns_density/grid/low/N09/seed01/...
            if len(parts) < 4:
                continue
            topo, load, N = parts[0], parts[1], parts[2]
            key = (topo, load, N)
        else:
            continue
        v = read_pdr(path)
        if v is not None:
            cells[key].append(v)
    return cells


def cell_label(k, axis_kind):
    if axis_kind == "spacing-load-N":
        spacing, load, N = k
        return f"{spacing:>14} {load:>4} {N:>3}"
    else:
        topo, load, N = k
        return f"{topo:>14} {load:>4} {N:>3}"


def fmt_pdr(p):
    return f"{p*100:6.2f}%" if p is not None else "  n/a "


print("=" * 96)
print("CSMA/CAD vs ALOHA — comparative summary across the 4 paper campaigns")
print("=" * 96)

improved = 0
regressed = 0
flat = 0
total_cells = 0
big_wins = []  # (delta_rel, label)
big_losses = []

for label, base_aloha, base_csma, axis_kind in CAMPAIGNS:
    print(f"\n--- {label} ---")
    print(f"{'axis':>26} {'PDR_ALOHA':>10} {'PDR_CSMA':>10} {'delta_rel':>11} {'verdict':>8}")
    print("-" * 76)
    aloha = collect(base_aloha, axis_kind)
    csma = collect(base_csma, axis_kind)
    for k in sorted(set(aloha) | set(csma)):
        a = aloha.get(k, [])
        c = csma.get(k, [])
        ma = sum(a) / len(a) if a else None
        mc = sum(c) / len(c) if c else None
        if ma is None or mc is None:
            verdict = "N/A"
            drel = None
        elif ma <= 0 and mc <= 0:
            verdict = "FLAT"
            drel = 0
        elif ma <= 0:
            drel = float("inf")
            verdict = "+++"
        else:
            drel = (mc - ma) / ma * 100
            if drel > 5:
                verdict = "WIN"
                improved += 1
            elif drel < -5:
                verdict = "LOSS"
                regressed += 1
            else:
                verdict = "flat"
                flat += 1
            total_cells += 1
            if drel > 0:
                big_wins.append((drel, cell_label(k, axis_kind).strip()))
            else:
                big_losses.append((drel, cell_label(k, axis_kind).strip()))
        drel_s = f"{drel:+8.1f}%" if drel is not None and drel != float("inf") else "  +inf  "
        print(f"{cell_label(k, axis_kind)}  {fmt_pdr(ma)}  {fmt_pdr(mc)}  {drel_s}   {verdict:>5}")

print("\n" + "=" * 96)
print(f"AGGREGATE: total_cells={total_cells}  WIN(>+5%)={improved}  "
      f"LOSS(<-5%)={regressed}  flat={flat}")
print("=" * 96)

if big_wins:
    big_wins.sort(reverse=True)
    print("\nTop 8 wins:")
    for d, lbl in big_wins[:8]:
        print(f"  {d:+8.1f}%   {lbl}")

if big_losses:
    big_losses.sort()
    print("\nTop 8 losses:")
    for d, lbl in big_losses[:8]:
        print(f"  {d:+8.1f}%   {lbl}")

# Aggregate per load
per_load = defaultdict(lambda: {"a": [], "c": []})
for label, base_aloha, base_csma, axis_kind in CAMPAIGNS:
    aloha = collect(base_aloha, axis_kind)
    csma = collect(base_csma, axis_kind)
    for k, vals in aloha.items():
        load = k[1]
        per_load[load]["a"].extend(vals)
    for k, vals in csma.items():
        load = k[1]
        per_load[load]["c"].extend(vals)

print("\n=== Aggregate by load (across all campaigns and N) ===")
print(f"{'load':>10} {'n_runs_A':>10} {'n_runs_C':>10} {'mean_A':>9} {'mean_C':>9} {'delta_rel':>11}")
for load in ("low", "high"):
    a = per_load[load]["a"]
    c = per_load[load]["c"]
    ma = sum(a) / len(a) if a else 0
    mc = sum(c) / len(c) if c else 0
    drel = (mc - ma) / ma * 100 if ma > 0 else 0
    print(f"{load:>10} {len(a):>10} {len(c):>10} {ma*100:8.2f}% {mc*100:8.2f}% {drel:+10.1f}%")
