#!/usr/bin/env python3
"""
analyze_tuning.py - compare Fase 2A/2B/2C/3D sweep configs vs ALOHA baseline
at N=25 high. Finds if any config beats ALOHA collisions (< 80% = C3 pass).
"""
import json, os, glob

BASE = "/home/diego/ns3-runs/validate_csmacad"


def load_metrics(d):
    try:
        with open(f"{d}/mesh_dv_summary.json") as f:
            j = json.load(f)
    except Exception:
        return None
    pdr = j["pdr"]
    g = pdr.get("total_data_generated", 0) or 0
    dv = pdr.get("delivered", 0) or 0
    cp = j.get("control_plane", {}) or {}
    dr = j.get("drops", {}) or {}
    return {
        "gen": g, "delv": dv, "pdr": (dv / g if g > 0 else 0.0),
        "coll": cp.get("data_collision_drops", 0) or 0,
        "busy_drops": cp.get("data_busy_drops", 0) or 0,
        "retries_drop": dr.get("drop_max_csma_retries", 0) or 0,
        "qfull": dr.get("drop_queue_overflow", 0) or 0,
    }


def mean_list(xs):
    xs = [x for x in xs if x is not None]
    return (sum(xs) / len(xs)) if xs else 0.0


# ALOHA baseline (N=25 high) from Fase 1
aloha_runs = [load_metrics(f"{BASE}/fase1/pueyo2024_paper_like/N25/high/seed{s:02d}")
              for s in (1, 2, 3)]
aloha_runs = [r for r in aloha_runs if r]
aloha_coll = mean_list([r["coll"] for r in aloha_runs])
aloha_pdr = mean_list([r["pdr"] for r in aloha_runs])
print(f"=== ALOHA baseline N=25 high: coll={aloha_coll:.0f}  PDR={aloha_pdr:.4f} ===\n")

# Default-CSMA baseline (N=25 high) from Fase 1
csma_default = [load_metrics(f"{BASE}/fase1/pueyo2024_paper_like_csmacad/N25/high/seed{s:02d}")
                for s in (1, 2, 3)]
csma_default = [r for r in csma_default if r]
csma_def_coll = mean_list([r["coll"] for r in csma_default])
csma_def_pdr = mean_list([r["pdr"] for r in csma_default])
print(f"=== CSMA default (retries=8, qmax=32, margin=6, cbf=?/dbf=?) N=25 high: "
      f"coll={csma_def_coll:.0f} ({csma_def_coll/aloha_coll*100:.1f}% of ALOHA)  "
      f"PDR={csma_def_pdr:.4f} ===\n")


def sweep_report(fase, param_name):
    print(f"--- {fase} ({param_name} sweep) vs ALOHA coll={aloha_coll:.0f} ---")
    print(f"{param_name:>14} | coll     | coll/aloha | PDR    | retries_drop | qfull_drop | verdict")
    best_coll = None
    best_cfg = None
    for subd in sorted(glob.glob(f"{BASE}/{fase}/*/")):
        name = os.path.basename(os.path.dirname(subd))
        seeds_data = [load_metrics(f"{subd.rstrip('/')}/seed{s:02d}") for s in (1, 2, 3)]
        seeds_data = [x for x in seeds_data if x]
        if not seeds_data:
            continue
        mc = mean_list([s["coll"] for s in seeds_data])
        mp = mean_list([s["pdr"] for s in seeds_data])
        mr = mean_list([s["retries_drop"] for s in seeds_data])
        mq = mean_list([s["qfull"] for s in seeds_data])
        ratio = mc / aloha_coll if aloha_coll > 0 else 0.0
        if ratio < 0.80:
            v = "BEATS ALOHA (C3 PASS)"
        elif ratio < 1.00:
            v = "worse than ALOHA (but < 100%)"
        else:
            v = "WORSE than ALOHA"
        print(f"{name:>14} | {mc:8.0f} | {ratio*100:8.1f}%  | {mp:.4f} | "
              f"{mr:12.1f} | {mq:10.1f} | {v}")
        if best_coll is None or mc < best_coll:
            best_coll = mc
            best_cfg = name
    print(f">>> best in {fase}: {best_cfg}  coll={best_coll:.0f}  "
          f"({best_coll/aloha_coll*100:.1f}% of ALOHA)\n")


sweep_report("fase2a", "retries")
sweep_report("fase2b", "qmax")
sweep_report("fase2c", "margin_dB")
sweep_report("fase3d", "cbf_dbf")
