#!/bin/bash
# run_e1e2.sh — bloques E1 (ranking de métricas) y E2 (interacción 2×2) del DoE.
# Corridas perf (duty-on 1%, carga Pueyo 100 paq/par, 40 ks). Paralelo N cores.
#
#   E1 (CSMA/CAD): {composite, toa, hops, rssi} × 3 escenarios × 8 N × 20 seeds
#   E2 (ALOHA):    {composite, toa}              × 3 escenarios × 8 N × 20 seeds
#
# Cada celda escribe UNA fila-resumen; los CSV pesados se descartan
# (--enableMetricsEssentialOnly + borrado por corrida) para acotar disco.
# Reanudable: salta celdas cuya fila ya existe.
#
#   bash run_e1e2.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-15}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e1e2
mkdir -p "$OUT/cells"
CSV="$OUT/e1e2_results.csv"
HDR="block,mac,metric,scenario,nEd,seed,rc,pdr,adm,fwd,delay_p50,delay_p95,gen,deliv,oh_ratio"

# --- una celda ---
cell() {
  local block=$1 mac=$2 metric=$3 scen=$4 n=$5 seed=$6
  local id="${block}_${mac}_${metric}_${scen}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0          # reanudable: ya hecho

  local prof scenflags metricflags
  case $mac in
    csmacad) prof=proposal_pueyo_like_csmacad ;;
    aloha)   prof=proposal_pueyo_like_aloha ;;
  esac
  case $scen in
    grid_a2a)   scenflags="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all" ;;
    rnd_conv1)  scenflags="--nodePlacementMode=random --forcedDataDestinationId=0" ;;
    rnd_msink4) scenflags="--nodePlacementMode=random --numSinks=4" ;;
  esac
  case $metric in
    composite) metricflags="--routeMetricMode=composite_score" ;;
    toa)       metricflags="--allowMetricModeOverride=true --routeMetricMode=toa_only" ;;
    hops)      metricflags="--compositeWToa=0 --compositeWHop=1 --compositeWEnergy=0" ;;
    rssi)      metricflags="--allowMetricModeOverride=true --routeMetricMode=rssi" ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile="$prof" --nEd="$n" --stopSec=40000 --rngRun="$seed" \
      --allowDutyOverride=true --enablePcap=false --verboseLogs=false \
      --enableMetricsEssentialOnly=true $scenflags $metricflags > run.log 2>&1) 2>/dev/null
  local rc=$?

  BLK=$block MAC=$mac MET=$metric SC=$scen N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import json,os
e=os.environ
def g(j,*ks,d=""):
    for k in ks:
        if isinstance(j,dict) and k in j: j=j[k]
        else: return d
    return j
pdr=adm=fwd=d50=d95=gen=deliv=cd=""
try:
    j=json.load(open("mesh_dv_summary.json"))
    pdr=g(j,"pdr","pdr"); gen=g(j,"pdr","total_data_generated"); deliv=g(j,"pdr","delivered")
    adm=g(j,"tx_attempts","admission_ratio"); fwd=g(j,"tx_attempts","delivered_per_tx_attempt")
    d50=g(j,"delay","p50_s"); d95=g(j,"delay","p95_s")
    cd=g(j,"overhead","ratio")
except Exception: pass
open(e["ROW"],"w").write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"%(
    e["BLK"],e["MAC"],e["MET"],e["SC"],e["N"],e["SEED"],e["RC"],
    pdr,adm,fwd,d50,d95,gen,deliv,cd))
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

# --- generar la lista de celdas ---
gen_cells() {
  local N="9 16 25 36 49 64 81 100"
  local SC="grid_a2a rnd_conv1 rnd_msink4"
  local S=$(seq 1 20)
  for scen in $SC; do for n in $N; do for seed in $S; do
    # E1: CSMA/CAD, 4 metricas (rssi anadida 2026-07-25)
    for m in composite toa hops rssi; do echo "E1 csmacad $m $scen $n $seed"; done
    # E2: ALOHA, 2 metricas
    for m in composite toa; do echo "E2 aloha $m $scen $n $seed"; done
  done; done; done
}

[ -f "$CSV" ] || echo "$HDR" > "$CSV"
echo "Lanzando E1+E2 con $JOBS jobs en paralelo..."
gen_cells | shuf | xargs -P "$JOBS" -L1 bash -c 'cell "$@"' _

# consolidar filas
{ echo "$HDR"; cat "$OUT"/cells/*.row 2>/dev/null; } > "$CSV"
echo "FIN. $(( $(wc -l < "$CSV") - 1 )) celdas en $CSV"
