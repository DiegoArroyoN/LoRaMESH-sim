#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Verifica el claim de robustez del paper contra las campanas 6B:
"en ninguna celda la linea base supera significativamente a DV-CL".

Barre los manifests de shadow/pathloss/interference/cadmargin (los mas
recientes, = re-run 6B jul-2026), agrupa por celda (parametros del sweep
x topologia x N), y hace Welch cmp_csma vs toa_aloha por celda. Cuenta:
  - celdas donde DV-CL > baseline significativo (p<0.05)
  - celdas donde baseline > DV-CL significativo  <-- debe ser 0
Salida: /home/diego/sim/_robcheck6b_result.txt
"""
import csv, glob, math, os

def welch_p(a, b):
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return 1.0
    ma = sum(a)/na; mb = sum(b)/nb
    va = sum((x-ma)**2 for x in a)/(na-1); vb = sum((x-mb)**2 for x in b)/(nb-1)
    if va == 0 and vb == 0:
        return 1.0 if ma == mb else 0.0
    t = (ma-mb)/math.sqrt(va/na+vb/nb)
    df = (va/na+vb/nb)**2/((va/na)**2/(na-1)+(vb/nb)**2/(nb-1))
    # aproximacion normal para p (df>=8 en la practica con 10 seeds)
    from math import erf
    p = 2*(1-0.5*(1+erf(abs(t)/math.sqrt(2))))
    return p, ma, mb

def newest(pat):
    ds = sorted(glob.glob("/home/diego/sim/results_archive/"+pat))
    return ds[-1] if ds else None

out = open("/home/diego/sim/_robcheck6b_result.txt", "w")
tot_cells = 0; dv_wins_sig = 0; base_wins_sig = 0; worst = []
for camp in ["campaign_shadow_sweep_2026*", "campaign_pathloss_sweep_2026*",
             "campaign_interference_sweep_2026*", "campaign_cadmargin_sweep_2026*"]:
    d = newest(camp)
    if not d:
        out.write("NO ENCONTRADA: %s\n" % camp); continue
    man = os.path.join(d, "campaign_manifest.csv")
    rows = list(csv.DictReader(open(man)))
    cols = rows[0].keys()
    # detectar columnas: variante, pdr, seed; el resto de parametros define la celda
    vcol = next((c for c in cols if "variant" in c.lower()), None)
    pcol = next((c for c in cols if c.lower() == "pdr"), None)
    scol = next((c for c in cols if "seed" in c.lower() or "rng" in c.lower()), None)
    if not (vcol and pcol and scol):
        out.write("%s: columnas no detectadas (%s)\n" % (os.path.basename(d), ",".join(cols)))
        continue
    ignore = {vcol, pcol, scol, "run_id", "status", "rc", "elapsed_s",
              "dead_nodes", "avg_hops", "duration_s", "timestamp", "out_dir", "cmd"}
    keycols = [c for c in cols if c not in ignore and not c.startswith("json_")
               and all(k not in c.lower() for k in
                       ("pdr", "delay", "energy", "t50", "fnd", "dead", "hops"))]
    cells = {}
    for r in rows:
        if r.get("status", "OK") not in ("OK", "", None):
            continue
        try:
            pdr = float(r[pcol])
        except Exception:
            continue
        key = tuple(r.get(c, "") for c in keycols)
        cells.setdefault(key, {}).setdefault(r[vcol], []).append(pdr)
    camp_cells = 0
    for key, per in cells.items():
        a = per.get("cmp_csma"); b = per.get("toa_aloha")
        if not a or not b:
            continue
        camp_cells += 1; tot_cells += 1
        p, ma, mb = welch_p(a, b)
        if p < 0.05 and ma > mb:
            dv_wins_sig += 1
        if p < 0.05 and mb > ma:
            base_wins_sig += 1
            worst.append((os.path.basename(d), key, ma, mb, p))
    out.write("%s: %d celdas comparadas\n" % (os.path.basename(d), camp_cells))
out.write("\nTOTAL celdas: %d | DV-CL>base sig: %d | base>DV-CL sig: %d\n"
          % (tot_cells, dv_wins_sig, base_wins_sig))
for w in worst[:10]:
    out.write("  VIOLACION: %s %s dv=%.4f base=%.4f p=%.3g\n" % w)
out.write("VEREDICTO: %s\n" % ("CLAIM SOSTENIDO en 6B (0 celdas donde baseline gana sig)"
          if base_wins_sig == 0 else "REVISAR: baseline gana en %d celdas" % base_wins_sig))
out.close()
print(open("/home/diego/sim/_robcheck6b_result.txt").read())
