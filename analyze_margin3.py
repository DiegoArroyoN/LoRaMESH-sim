#!/usr/bin/env python3
"""analyze_margin3.py — compare margin=3 vs margin=6 (default) baseline.

Decision criteria:
  C-A: PDR(margin=3) >= PDR(margin=6) in N=9 high AND N=49 high
  C-B: |relative delta_PDR| < 15% across all low-load scenarios
  C-C: (drop_max_csma_retries + drop_queue_overflow) / generated < 30% in all high
"""
import json
import os
import sys

BASE_M3 = "/home/diego/ns3-runs/validate_margin3"
BASE_M6 = "/home/diego/ns3-runs/validate_csmacad/fase1/pueyo2024_paper_like_csmacad"
SEEDS = [1, 2, 3]
SCENARIOS = [(N, load) for N in (9, 25, 49) for load in ("low", "high")]


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def pdr(j):
    p = (j or {}).get("pdr", {}) or {}
    g = p.get("total_data_generated", 0) or 0
    d = p.get("delivered", 0) or 0
    return (d / g) if g > 0 else 0.0


def gen(j):
    return ((j or {}).get("pdr", {}) or {}).get("total_data_generated", 0) or 0


def drop_total(j):
    d = (j or {}).get("drops", {}) or {}
    return (d.get("drop_max_csma_retries", 0) or 0) + (d.get("drop_queue_overflow", 0) or 0)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return (sum(xs) / len(xs)) if xs else 0.0


def main():
    print(f"{'scenario':>12} | {'PDR_m6':>9} | {'PDR_m3':>9} | "
          f"{'delta_rel':>10} | {'drop_pct_m3':>11}")
    print("-" * 70)

    rows = []
    for N, ld in SCENARIOS:
        m6 = [load_run(BASE_M6, N, ld, s) for s in SEEDS]
        m3 = [load_run(BASE_M3, N, ld, s) for s in SEEDS]

        p6 = mean([pdr(j) for j in m6])
        p3 = mean([pdr(j) for j in m3])
        drel = ((p3 - p6) / p6 * 100) if p6 > 0 else 0.0

        dpct_m3 = mean([
            (drop_total(j) / gen(j) * 100) if gen(j) > 0 else 0.0 for j in m3
        ])

        print(f"   N={N:2d} {ld:>4} | {p6:9.4f} | {p3:9.4f} | "
              f"{drel:+9.1f}% | {dpct_m3:10.2f}%")
        rows.append({"N": N, "load": ld, "p6": p6, "p3": p3,
                     "drel": drel, "dpct_m3": dpct_m3})

    # Verdicts
    print("\n=== DECISION CRITERIA ===")

    # C-A: PDR(m3) >= PDR(m6) in N=9 high AND N=49 high
    a9 = next(r for r in rows if r["N"] == 9 and r["load"] == "high")
    a49 = next(r for r in rows if r["N"] == 49 and r["load"] == "high")
    c_a = (a9["p3"] >= a9["p6"]) and (a49["p3"] >= a49["p6"])
    print(f"C-A: PDR(m3)>=PDR(m6) in N=9 high & N=49 high  -> "
          f"{'PASS' if c_a else 'FAIL'} "
          f"[N=9: {a9['drel']:+.1f}%, N=49: {a49['drel']:+.1f}%]")

    # C-B: |delta_rel| < 15% in all low-load
    low_rows = [r for r in rows if r["load"] == "low"]
    c_b = all(abs(r["drel"]) < 15 for r in low_rows)
    print(f"C-B: |delta_PDR| < 15% in all low-load        -> "
          f"{'PASS' if c_b else 'FAIL'} "
          f"[max |delta|={max(abs(r['drel']) for r in low_rows):.1f}%]")

    # C-C: drop% < 30% in all high
    high_rows = [r for r in rows if r["load"] == "high"]
    c_c = all(r["dpct_m3"] < 30 for r in high_rows)
    print(f"C-C: drop%<30% in all high (margin=3)         -> "
          f"{'PASS' if c_c else 'FAIL'} "
          f"[max drop%={max(r['dpct_m3'] for r in high_rows):.2f}%]")

    overall = c_a and c_b and c_c
    print(f"\n=== VERDICT: {'CHANGE DEFAULT TO 3 dB' if overall else 'KEEP DEFAULT 6 dB'} ===")
    sys.exit(0 if overall else 1)


def load_run(base, N, ld, seed):
    return load(f"{base}/N{N:02d}/{ld}/seed{seed:02d}/mesh_dv_summary.json")


if __name__ == "__main__":
    main()
