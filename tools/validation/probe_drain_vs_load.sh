#!/bin/bash
# probe_drain_vs_load.sh — ¿el consumo de un nodo depende de cuanto releva?
#
# Es la pregunta que hay detras de la intuicion de Diego: "si mando los paquetes
# por los nodos con mas bateria, deberia morir mas tarde el que menos tiene".
# Ese razonamiento presupone algo que nadie ha comprobado: que el consumo de un
# nodo DEPENDA de la carga que se le encamina. Si el gasto es casi fijo -- lo
# fijan la escucha y las balizas propias, no el trafico ajeno -- entonces
# ninguna decision de ruteo puede alargar la vida de nadie, y delta esta
# condenado por construccion y no por estar mal implementado.
#
# La revision del codigo del 31-jul descarta el fallo de implementacion: Psi usa
# BattMvToEFrac(link.batt_mV), o sea la bateria DEL VECINO, con signo correcto
# (penaliza al que menos tiene) y a precision completa en el primer salto. Asi
# que el mecanismo esta bien y hay que buscar la causa en la fisica.
#
# Se mide, nodo a nodo:
#   - energia total y sus cuatro componentes
#   - aire de relevo, de balizas y de trafico propio
# y se calcula cuanta de la VARIANZA del consumo explica el relevo. Si es
# despreciable, la respuesta esta cerrada.
#
#   bash probe_drain_vs_load.sh [ns3_dir]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/probe_drain
rm -rf "$OUT"; mkdir -p "$OUT"

corre() {  # corre <etiqueta> <flags extra...>
  local tag=$1; shift
  local d="$OUT/$tag"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 900 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=49 --stopSec=100000 --rngRun=1 \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --pueyoGridSpacingM=178 --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 \
      --allowBeaconOverride=true --beaconIntervalWarmSec=900 --beaconIntervalStableSec=900 \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel=static --enablePcap=false --verboseLogs=false "$@" > run.log 2>&1
  echo "  $tag rc=$?"
}

corre d00 --routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 --compositeWEnergy=0
corre d85 --routeMetricMode=composite_score --compositeWToa=0.00 --compositeWHop=0.15 --compositeWEnergy=0.85
corre toa --allowMetricModeOverride=true --routeMetricMode=toa_only

cd "$OUT" || exit 1
python3 - <<'PY'
import csv, os, statistics as st
base = os.path.expanduser("~/ns3-runs/probe_drain")

def leer(tag):
    d = os.path.join(base, tag)
    en = {}
    try:
        for r in csv.DictReader(open(os.path.join(d,"mesh_dv_metrics_energy_breakdown.csv"))):
            n = int(r["nodeId"])
            en[n] = dict(tx=float(r["txMah"]), rx=float(r["rxMah"]),
                         cad=float(r["cadMah"]), idle=float(r["idleMah"]))
            en[n]["tot"] = sum(en[n][k] for k in ("tx","rx","cad","idle"))
    except Exception as ex:
        print("  sin energia en", tag, ex); return None
    air = {n: dict(b=0.0, f=0.0, r=0.0) for n in en}
    try:
        for r in csv.DictReader(open(os.path.join(d,"mesh_dv_metrics_tx.csv"))):
            try: n=int(r["nodeId"]); t=float(r["toaUs"])
            except Exception: continue
            if n not in air: continue
            if r.get("dst")=="65535": air[n]["b"]+=t
            elif r.get("src")==r.get("nodeId"): air[n]["f"]+=t
            else: air[n]["r"]+=t
    except Exception: pass
    return en, air

def r2(xs, ys):
    if len(xs)<3: return float('nan')
    mx,my=st.mean(xs),st.mean(ys)
    sx=sum((a-mx)**2 for a in xs); sy=sum((b-my)**2 for b in ys)
    if sx<=0 or sy<=0: return float('nan')
    cov=sum((a-mx)*(b-my) for a,b in zip(xs,ys))
    return (cov/ (sx*sy)**0.5)**2

for tag in ("d00","d85","toa"):
    got = leer(tag)
    if not got: continue
    en, air = got
    ns = sorted(en)
    tot = [en[n]["tot"] for n in ns]
    rel = [air[n]["r"]/1e6 for n in ns]
    bal = [air[n]["b"]/1e6 for n in ns]
    fue = [air[n]["f"]/1e6 for n in ns]
    cv = st.stdev(tot)/st.mean(tot) if len(tot)>1 and st.mean(tot)>0 else 0
    print("\n=== %s : %d nodos ===" % (tag, len(ns)))
    print("  energia por nodo: media %.4f mAh  desv %.4f  CV %.4f  (max/min %.3f)" %
          (st.mean(tot), st.stdev(tot) if len(tot)>1 else 0, cv, max(tot)/min(tot)))
    print("  componentes (media): TX %.4f  RX %.4f  CAD %.4f  reposo %.4f" %
          (st.mean([en[n]["tx"] for n in ns]), st.mean([en[n]["rx"] for n in ns]),
           st.mean([en[n]["cad"] for n in ns]), st.mean([en[n]["idle"] for n in ns])))
    print("  reposo: CV %.5f   <- si es ~0, es un coste FIJO por nodo" %
          (st.stdev([en[n]["idle"] for n in ns])/st.mean([en[n]["idle"] for n in ns])
           if len(ns)>1 else 0))
    print("  varianza del consumo explicada por...")
    print("     aire de RELEVO   R2 = %.4f" % r2(rel, tot))
    print("     aire de BALIZAS  R2 = %.4f" % r2(bal, tot))
    print("     aire PROPIO      R2 = %.4f" % r2(fue, tot))
    mrel = st.mean(rel)
    if mrel>0:
        print("  relevo: media %.4f s  max %.4f s  (max/media %.2f)" % (mrel, max(rel), max(rel)/mrel))
    # cuanto costaria en energia el relevo del nodo que mas releva
    print("  el nodo que MAS releva lleva %.4f s de aire de relevo;" % max(rel))
    print("     su energia total es %.4f mAh y la media %.4f" %
          (en[ns[rel.index(max(rel))]]["tot"], st.mean(tot)))
PY
