#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""F1.5 — convergencia del DV contra Bellman-Ford offline.

Consume el mesh_dv_metrics_routes.csv de un run con
--enableMetricsEssentialOnly=false, reconstruye (a) el grafo de enlaces
directos (installs con hops==1: costo = 1 - score/100, cuantizado a
0.01) y (b) la tabla de rutas final por nodo (ultimo install por
(nodo,destino) antes del corte). Computa caminos minimos con
Bellman-Ford puro-Python sobre el grafo y compara el costo del camino
elegido por el simulador contra el optimo, con tolerancia:

  tol = histeresis (0.05) + holgura de cuantizacion (0.01 por salto x 2)

Reporta la fraccion de pares (nodo,destino) cuyo costo esta dentro de
tolerancia. Uso: bf_check.py <run_dir> [t_corte_s]
"""
import csv, sys, os
from collections import defaultdict

def main():
    d = sys.argv[1]
    tcut = float(sys.argv[2]) if len(sys.argv) > 2 else 1e18
    routes = os.path.join(d, "mesh_dv_metrics_routes.csv")
    edges = {}
    table = {}
    for r in csv.DictReader(open(routes)):
        t = float(r["timestamp(s)"])
        if t > tcut:
            continue
        node = int(r["nodeId"]); dst = int(r["destination"])
        nh = int(r["nextHop"]); hops = int(r["hops"]); score = int(r["score"])
        act = r.get("action", "install").lower()
        if "expire" in act or "remove" in act or score <= 0:
            table.pop((node, dst), None)
            continue
        table[(node, dst)] = (nh, hops, score)
        if hops == 1 and dst == nh:
            edges[(node, dst)] = 1.0 - score/100.0
    # grafo dirigido: costo de enlace
    nodes = set()
    for (a, b) in edges:
        nodes.add(a); nodes.add(b)
    # Bellman-Ford all-pairs (grafos chicos)
    INF = float("inf")
    dist = {}
    for s in nodes:
        dd = {n: INF for n in nodes}; dd[s] = 0.0
        for _ in range(len(nodes)-1):
            ch = False
            for (a, b), w in edges.items():
                if dd[a]+w < dd[b]-1e-12:
                    dd[b] = dd[a]+w; ch = True
            if not ch:
                break
        dist[s] = dd
    tot = 0; ok = 0; bad = []
    for (node, dst), (nh, hops, score) in table.items():
        if node == dst or dst not in nodes or node not in nodes:
            continue
        if dist[node].get(dst, INF) == INF:
            continue
        tot += 1
        sim_cost = 1.0 - score/100.0
        opt = dist[node][dst]
        tol = 0.05 + 0.02*max(hops, 1)
        if sim_cost <= opt + tol:
            ok += 1
        else:
            bad.append((node, dst, nh, sim_cost, opt, hops))
    print("F1.5 BF check: %s" % os.path.basename(d))
    print("  pares (nodo,destino) evaluados: %d" % tot)
    print("  dentro de tolerancia (opt + 0.05 + 0.02/salto): %d (%.1f%%)"
          % (ok, 100.0*ok/max(tot, 1)))
    for b in bad[:8]:
        print("  FUERA: node=%d dst=%d nh=%d sim=%.3f opt=%.3f hops=%d" % b)
    print("VEREDICTO: %s" % ("PASS" if ok == tot and tot > 0 else
                             ("REVISAR (%d fuera)" % len(bad) if tot else "SIN DATOS")))

if __name__ == "__main__":
    main()
