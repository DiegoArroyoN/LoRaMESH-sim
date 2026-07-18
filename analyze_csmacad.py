#!/usr/bin/env python3
"""
analyze_csmacad.py — Fase 1 verdict.
Reads /home/diego/ns3-runs/validate_csmacad/fase1/ and checks 5 criteria:
  C1: Profile ran without crashes (no FAILED marker, summary JSON exists)
  C2: Low-load PDR within +/-15% (ALOHA vs CSMA) per N
  C3: CSMA high-load collisions < 80% of ALOHA per N
  C4: max_retries + queue_overflow < 30% of generated in CSMA high per N
  C5: cad_busy_events > 0 in high CSMA runs per N
"""
import json, os, sys

BASE = '/home/diego/ns3-runs/validate_csmacad/fase1'
PROFILES = ['pueyo2024_paper_like', 'pueyo2024_paper_like_csmacad']
NS = [9, 25, 49]
LOADS = ['low', 'high']
SEEDS = [1, 2, 3]


def safe_load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def scan():
    out = {}
    for prof in PROFILES:
        for N in NS:
            for load in LOADS:
                for seed in SEEDS:
                    outdir = f"{BASE}/{prof}/N{N:02d}/{load}/seed{seed:02d}"
                    summary = f"{outdir}/mesh_dv_summary.json"
                    failed = os.path.exists(f"{outdir}/FAILED")
                    j = safe_load(summary)
                    out[(prof, N, load, seed)] = {
                        'dir': outdir, 'failed': failed, 'j': j
                    }
    return out


def pdr(j):
    p = (j or {}).get('pdr', {}) or {}
    g = p.get('total_data_generated', 0) or 0
    d = p.get('delivered', 0) or 0
    return (d / g) if g > 0 else 0.0


def collisions(j):
    cp = (j or {}).get('control_plane', {}) or {}
    # Prefer exact v4 counter if present; else fall back to raw event count.
    v = cp.get('data_collision_drops')
    if v is not None:
        return v
    return cp.get('pueyo_destructive_overlap_drops', 0) or 0


def drop_frac(j, key):
    p = (j or {}).get('pdr', {}) or {}
    g = p.get('total_data_generated', 0) or 0
    if g == 0:
        return 0.0
    d = ((j or {}).get('drops', {}) or {}).get(key, 0) or 0
    return d / g


def cad_busy(j):
    """Best-effort: CAD-busy event count. Different builds expose it differently."""
    if j is None:
        return 0
    cp = j.get('control_plane', {}) or {}
    for key in ('cad_busy_events', 'csma_cad_busy_events', 'data_busy_drops'):
        v = cp.get(key)
        if v is not None:
            return v
    # Also check csma section if present
    csma = j.get('csma', {}) or {}
    for key in ('cad_busy_events', 'busy_events'):
        v = csma.get(key)
        if v is not None:
            return v
    return 0


def mean(xs):
    xs = [x for x in xs if x is not None]
    return (sum(xs) / len(xs)) if xs else 0.0


def main():
    data = scan()
    results = []

    # C1
    fails = [k for k, v in data.items() if v['failed'] or v['j'] is None]
    total = len(data)
    results.append((
        'C1 no crashes',
        len(fails) == 0,
        f"{total - len(fails)}/{total} OK" if not fails
        else f"{len(fails)} missing/FAILED (first 5: {fails[:5]})"
    ))

    # C2: low-load PDR within +/-15%
    c2_ok = True
    c2_details = []
    for N in NS:
        pa = mean([pdr(data[('pueyo2024_paper_like', N, 'low', s)]['j']) for s in SEEDS])
        pc = mean([pdr(data[('pueyo2024_paper_like_csmacad', N, 'low', s)]['j']) for s in SEEDS])
        rel = abs(pc - pa) / pa if pa > 0 else (1.0 if pc > 0 else 0.0)
        ok = rel <= 0.15
        c2_ok = c2_ok and ok
        c2_details.append(f"N={N}: aloha={pa:.3f} csma={pc:.3f} |delta|={rel*100:.1f}%")
    results.append(('C2 low-load PDR +/-15%', c2_ok, ' | '.join(c2_details)))

    # C3: CSMA high-load collisions < 80% of ALOHA
    c3_ok = True
    c3_details = []
    for N in NS:
        ca = mean([collisions(data[('pueyo2024_paper_like', N, 'high', s)]['j']) for s in SEEDS])
        cc = mean([collisions(data[('pueyo2024_paper_like_csmacad', N, 'high', s)]['j']) for s in SEEDS])
        frac = (cc / ca) if ca > 0 else (1.0 if cc > 0 else 0.0)
        ok = frac < 0.80
        c3_ok = c3_ok and ok
        c3_details.append(f"N={N}: aloha={ca:.0f} csma={cc:.0f} ratio={frac*100:.1f}%")
    results.append(('C3 CSMA collisions < 80% ALOHA (high)', c3_ok, ' | '.join(c3_details)))

    # C4: max_retries + queue_overflow < 30% of generated in CSMA high
    c4_ok = True
    c4_details = []
    for N in NS:
        fracs = []
        for s in SEEDS:
            j = data[('pueyo2024_paper_like_csmacad', N, 'high', s)]['j']
            if j is not None:
                fracs.append(
                    drop_frac(j, 'drop_max_csma_retries')
                    + drop_frac(j, 'drop_queue_overflow')
                )
        m = mean(fracs)
        ok = m < 0.30
        c4_ok = c4_ok and ok
        c4_details.append(f"N={N}: {m*100:.1f}%")
    results.append(('C4 max_retries+qfull < 30% (CSMA high)', c4_ok, ' | '.join(c4_details)))

    # C5: cad_busy_events > 0 in CSMA high
    c5_ok = True
    c5_details = []
    for N in NS:
        evts = [cad_busy(data[('pueyo2024_paper_like_csmacad', N, 'high', s)]['j']) for s in SEEDS]
        m = mean(evts)
        ok = m > 0
        c5_ok = c5_ok and ok
        c5_details.append(f"N={N}: avg={m:.0f}")
    results.append(('C5 CAD busy events > 0 (CSMA high)', c5_ok, ' | '.join(c5_details)))

    # Emit
    print("=" * 78)
    print("FASE 1 VERDICT - ALOHA vs CSMA comparative (36 runs)")
    print("=" * 78)
    all_ok = True
    for name, ok, detail in results:
        mark = 'PASS' if ok else 'FAIL'
        all_ok = all_ok and ok
        print(f"[{mark}] {name}")
        print(f"        {detail}")
    print("=" * 78)
    print(f"OVERALL: {'PASS' if all_ok else 'FAIL'}")
    sys.exit(0 if all_ok else 1)


if __name__ == '__main__':
    main()
