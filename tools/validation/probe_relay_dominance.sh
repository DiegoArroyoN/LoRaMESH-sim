#!/bin/bash
# probe_relay_dominance.sh — ¿existe algun escenario donde el RELEVO domine el gasto?
#
# La conclusion "el ruteo es palanca de entrega, no de energia" se apoya en un
# numero: el relevo es el 1.61% de la energia total (4.83% del aire de TX por
# 33.23% que pesa el TX). Pero eso se midio en UN escenario -- rejilla a2a de
# 178 m con SF7-12 o SF7-8 y balizas cada 60 s -- y una conclusion de ese
# calibre no puede descansar en un punto del espacio.
#
# La pregunta de Diego es exactamente la correcta: si el relevo llegara a
# dominar, el termino de energia de la metrica tendria donde morder y delta=0
# dejaria de ser general. Asi que hay que ir a buscar ese escenario en serio,
# empujando las tres palancas que deberian hacer crecer el relevo:
#
#   ALCANCE CORTO   con SF7-8 el alcance es 350 m; separar la rejilla obliga a
#                   encadenar saltos en vez de llegar directo.
#   MAS NODOS       la cadena mas larga de una rejilla NxN crece con N.
#   MENOS BALIZAS   las balizas son el 65% del aire de TX. Si su cadencia baja,
#                   el relevo gana peso relativo aunque no crezca en absoluto.
#
#   CONVERGECAST    ademas, embudar todo hacia un sumidero concentra el relevo
#                   en los vecinos del sumidero, que es el caso que mas
#                   favorece al termino de energia.
#
# Se mide la cuota del relevo sobre la energia TOTAL, no sobre el aire: es la
# unica cifra que responde a "quien se lleva la bateria".
#
#   bash probe_relay_dominance.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-10}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/probe_relay
rm -rf "$OUT"; mkdir -p "$OUT/cells"
export BIN LD_LIBRARY_PATH OUT

celda() {  # celda <topo> <N> <separacion> <sfmax> <baliza>
  local topo=$1 n=$2 sp=$3 sfmax=$4 bcn=$5
  local id="${topo}_n${n}_s${sp}_sf7${sfmax}_b${bcn}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local tf bf
  case "$topo" in
    a2a)  tf="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=$sp" ;;
    conv) tf="--nodePlacementMode=pueyo_grid --pueyoGridSpacingM=$sp --forcedDataDestinationId=0" ;;
    *) return 1 ;;
  esac
  bf=""
  [ "$bcn" != "60" ] && bf="--allowBeaconOverride=true --beaconIntervalWarmSec=$bcn --beaconIntervalStableSec=$bcn"

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 600 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=20000 --rngRun=1 \
      --allowDutyOverride=true $tf \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=$sfmax \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel=static --enablePcap=false --verboseLogs=false \
      --routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 \
      --compositeWEnergy=0 $bf > run.log 2>&1
  local rc=$?

  T=$topo N=$n SP=$sp SF=$sfmax B=$bcn RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os
e=os.environ
pdr=hops=""
try:
    j=json.load(open("mesh_dv_summary.json"))
    pdr=j["pdr"]["pdr"]; hops=j["forwarding"]["avg_hops_delivered"]
except Exception: pass
tx=rx=cad=idle=0.0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tx+=float(r["txMah"]); rx+=float(r["rxMah"]); cad+=float(r["cadMah"]); idle+=float(r["idleMah"])
except Exception: pass
ab=af=ar=0.0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: t=float(r["toaUs"])
        except Exception: continue
        if r.get("dst")=="65535": ab+=t
        elif r.get("src")==r.get("nodeId"): af+=t
        else: ar+=t
except Exception: pass
aire=ab+af+ar
tot=tx+rx+cad+idle
# cuota del relevo sobre la energia TOTAL: (relevo/aire) * (TX/total)
cuota = (ar/aire)*(tx/tot)*100 if aire>0 and tot>0 else 0.0
f=lambda x: "%.4f" % x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["T"],e["N"],e["SP"],"7-"+e["SF"],e["B"],e["RC"],pdr,hops,
    f(100*ar/aire if aire else 0), f(100*ab/aire if aire else 0),
    f(100*tx/tot if tot else 0), f(100*idle/tot if tot else 0), f(cuota)])+"\n")
PY
  cd "$OUT" && rm -rf "$d"
}
export -f celda

{
  for topo in a2a conv; do
    for n in 25 49 100; do
      for sp in 178 350 600; do
        for sfmax in 8 12; do
          for bcn in 60 900; do echo "$topo $n $sp $sfmax $bcn"; done
        done
      done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4'

RES="$OUT/relay_dominance.csv"
{ echo "topo,N,sep_m,sf,baliza_s,rc,pdr,saltos,relevo_pct_aire,baliza_pct_aire,tx_pct_energia,reposo_pct_energia,RELEVO_PCT_ENERGIA_TOTAL"
  cat "$OUT"/cells/*.row 2>/dev/null | sort -t, -k13 -gr; } > "$RES"
echo "== PROBE_DONE: $(( $(wc -l < "$RES") - 1 )) celdas =="
echo ""
echo "== los 12 escenarios donde el relevo pesa MAS =="
head -13 "$RES" | column -t -s,
