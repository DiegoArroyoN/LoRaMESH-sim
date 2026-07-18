#!/usr/bin/env python3
"""sanity_check_csmacad.py — quick sanity probe of the 720 CSMA/CAD runs."""
import json
import glob
from collections import defaultdict

CAMPAIGNS = {
    "campaigns_csmacad":         "/home/diego/ns3-runs/campaigns_csmacad",
    "campaigns_random_csmacad":  "/home/diego/ns3-runs/campaigns_random_csmacad",
    "campaigns_density_csmacad": "/home/diego/ns3-runs/campaigns_density_csmacad",
}


def read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def metrics(j):
    if j is None:
        return None
    p = j.get("pdr") or {}
    g = p.get("total_data_generated", 0) or 0
    d = p.get("delivered", 0) or 0
    cp = j.get("control_plane") or {}
    cad = (cp.get("cad_busy_events_local_power", 0) or
           cp.get("cad_busy_events", 0) or 0)
    issues = []
    if g <= 0:
        issues.append("zero_generated")
    if d < 0 or d > g:
        issues.append("delivered_invalid")
    pdr = (d / g) if g > 0 else None
    if pdr is not None and (pdr < 0.0 or pdr > 1.0):
        issues.append("pdr_out_of_range")
    return {"g": g, "d": d, "pdr": pdr, "cad": cad, "issues": issues}


print("=" * 78)
print("CSMA/CAD CAMPAIGN SANITY CHECK (720 runs)")
print("=" * 78)

for camp_name, base in CAMPAIGNS.items():
    files = sorted(glob.glob(f"{base}/**/mesh_dv_summary.json", recursive=True))
    print(f"\n--- {camp_name} ({len(files)} runs) ---")
    if not files:
        print("  NO RUNS FOUND")
        continue
    rows = [metrics(read_json(p)) for p in files]
    rows = [r for r in rows if r]
    pdrs = [r["pdr"] for r in rows if r["pdr"] is not None]
    cads = [r["cad"] for r in rows]
    issues = defaultdict(int)
    for r in rows:
        for i in r["issues"]:
            issues[i] += 1
    if pdrs:
        pdrs_sorted = sorted(pdrs)
        n = len(pdrs)
        print(f"  PDR n={n} mean={sum(pdrs)/n*100:.2f}% "
              f"median={pdrs_sorted[n//2]*100:.2f}% "
              f"min={pdrs_sorted[0]*100:.2f}% max={pdrs_sorted[-1]*100:.2f}%")
    if cads:
        print(f"  cad_busy_events mean={sum(cads)/len(cads):.0f} max={max(cads)}")
    if issues:
        print(f"  ISSUES: {dict(issues)}")
    else:
        print("  ISSUES: none")

# Per-(spacing, load, N) breakdown — campaigns_csmacad
print("\n" + "=" * 78)
print("Breakdown campaigns_csmacad (grid 177/248m)")
print("=" * 78)
print(f"{'spacing':>8} {'load':>6} {'N':>3} {'mean_PDR':>10} {'CAD_busy':>14}")
print("-" * 50)
cells = defaultdict(list)
for path in sorted(glob.glob(f"{CAMPAIGNS['campaigns_csmacad']}/**/mesh_dv_summary.json",
                              recursive=True)):
    rel = path.replace(CAMPAIGNS["campaigns_csmacad"] + "/", "")
    parts = rel.split("/")
    if len(parts) < 4:
        continue
    spacing, load, N = parts[0], parts[1], parts[2]
    j = read_json(path)
    m = metrics(j)
    if m and m["pdr"] is not None:
        cells[(spacing, load, N)].append((m["pdr"], m["cad"]))
for k in sorted(cells.keys()):
    spacing, load, N = k
    pdrs = [c[0] for c in cells[k]]
    cads = [c[1] for c in cells[k]]
    print(f"{spacing:>8} {load:>6} {N:>3} {sum(pdrs)/len(pdrs)*100:9.2f}% "
          f"{sum(cads)/len(cads):14.0f}")

print()
