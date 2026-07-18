#!/usr/bin/env python3
"""analyze_cbf_dbf.py — compare (cbf, dbf) combos at N=25 high."""
import json, glob, os
BASE = "/home/diego/ns3-runs/sweep_cbf_dbf"


def load(p):
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return None


def metrics(j):
    p = (j or {}).get("pdr", {}) or {}
    g = p.get("total_data_generated", 0) or 0
    d = p.get("delivered", 0) or 0
    cp = (j or {}).get("control_plane", {}) or {}
    dr = (j or {}).get("drops", {}) or {}
    return {
        "pdr": d / g if g > 0 else 0.0,
        "delv": d, "gen": g,
        "coll": cp.get("data_collision_drops", 0) or 0,
        "retries_drop": dr.get("drop_max_csma_retries", 0) or 0,
        "qfull": dr.get("drop_queue_overflow", 0) or 0,
    }


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else 0.0


print(f"{'combo':>16} | {'PDR':>7} | {'delv':>7} | {'gen':>7} | "
      f"{'retries':>8} | {'qfull':>7} | {'vs_Pueyo':>9}")
print("-" * 90)

baseline_pdr = None
combos = sorted(glob.glob(f"{BASE}/cbf*_dbf*/"))
results = []
for d in combos:
    name = os.path.basename(d.rstrip("/"))
    runs = [load(f"{d}seed{s:02d}/mesh_dv_summary.json") for s in (1, 2, 3)]
    runs = [r for r in runs if r]
    if not runs:
        continue
    ms = [metrics(r) for r in runs]
    pdr = mean([m["pdr"] for m in ms])
    delv = mean([m["delv"] for m in ms])
    gen = mean([m["gen"] for m in ms])
    rd = mean([m["retries_drop"] for m in ms])
    qf = mean([m["qfull"] for m in ms])
    if name == "cbf1_0_dbf10_0":
        baseline_pdr = pdr
    results.append((name, pdr, delv, gen, rd, qf))

# print, with delta vs baseline (Pueyo)
for name, pdr, delv, gen, rd, qf in results:
    if baseline_pdr is not None and baseline_pdr > 0 and name != "cbf1_0_dbf10_0":
        d_rel = (pdr - baseline_pdr) / baseline_pdr * 100
        delta = f"{d_rel:+.1f}%"
    else:
        delta = "(baseline)" if name == "cbf1_0_dbf10_0" else "n/a"
    print(f"{name:>16} | {pdr*100:6.2f}% | {delv:7.0f} | {gen:7.0f} | "
          f"{rd:8.0f} | {qf:7.0f} | {delta:>9}")

# Best combo
if results:
    best = max(results, key=lambda r: r[1])
    print(f"\n>>> Best PDR: {best[0]} = {best[1]*100:.2f}%")
    if baseline_pdr and baseline_pdr > 0:
        d_rel = (best[1] - baseline_pdr) / baseline_pdr * 100
        print(f">>> vs Pueyo (1.0, 10.0): {d_rel:+.1f}% relative")
