#!/bin/bash
# run_e6.sh — bloque E6 del DoE: flooding gestionado como línea de referencia.
#
#   flooding × {grid_a2a, rnd_conv1} × N {9..100} × 20 seeds = 320 corridas perf
#   Comparable celda a celda con las filas de E1 (mismos escenarios, N y semillas).
#
#   bash run_e6.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-15}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e6
mkdir -p "$OUT/cells"
CSV="$OUT/e6_results.csv"
HDR="metric,scenario,nEd,seed,rc,pdr,adm,fwd,delay_p50,delay_p95,gen,deliv,oh_ratio,src_tx,relay_tx,hops"

# Sonda previa: el binario debe conocer --floodingMode antes de encolar 320
# corridas. La leccion del 25-jul: 480 celdas rssi se perdieron por lanzar
# contra un binario sin la funcion.
mkdir -p /tmp/e6check && cd /tmp/e6check
if ! "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=9 --stopSec=3000 --rngRun=1 \
        --allowDutyOverride=true --floodingMode=true \
        --enablePcap=false --verboseLogs=false >/dev/null 2>&1; then
    echo "ABORTA: el binario no acepta --floodingMode. Sincroniza y recompila antes."
    exit 1
fi
echo "sonda OK: el binario acepta --floodingMode"

cell() {
  local scen=$1 n=$2 seed=$3
  local id="flood_${scen}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0
  local scenflags
  case $scen in
    grid_a2a)  scenflags="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all" ;;
    rnd_conv1) scenflags="--nodePlacementMode=random --forcedDataDestinationId=0" ;;
  esac
  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=40000 --rngRun="$seed" \
      --allowDutyOverride=true --floodingMode=true \
      --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      $scenflags > run.log 2>&1) 2>/dev/null
  local rc=$?
  SC=$scen N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import json,os
e=os.environ
def g(j,*ks,d=""):
    for k in ks:
        if isinstance(j,dict) and k in j: j=j[k]
        else: return d
    return j
v={}
try:
    j=json.load(open("mesh_dv_summary.json"))
    v=dict(pdr=g(j,"pdr","pdr"), gen=g(j,"pdr","total_data_generated"), deliv=g(j,"pdr","delivered"),
           adm=g(j,"tx_attempts","admission_ratio"), fwd=g(j,"tx_attempts","delivered_per_tx_attempt"),
           d50=g(j,"delay","p50_s"), d95=g(j,"delay","p95_s"), oh=g(j,"overhead","ratio"),
           src=g(j,"tx_attempts","source_tx_sent_total"), rel=g(j,"forwarding","forward_tx_sent_total"),
           hops=g(j,"forwarding","avg_hops_delivered"))
except Exception: pass
open(e["ROW"],"w").write("flooding,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"%(
    e["SC"],e["N"],e["SEED"],e["RC"],v.get("pdr",""),v.get("adm",""),v.get("fwd",""),
    v.get("d50",""),v.get("d95",""),v.get("gen",""),v.get("deliv",""),v.get("oh",""),
    v.get("src",""),v.get("rel",""),v.get("hops","")))
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"
for scen in grid_a2a rnd_conv1; do
  for n in 9 16 25 36 49 64 81 100; do
    for seed in $(seq 1 20); do echo "$scen $n $seed"; done
  done
done | shuf | xargs -P "$JOBS" -L1 bash -c 'cell "$@"' _

{ echo "$HDR"; cat "$OUT"/cells/*.row 2>/dev/null; } > "$CSV"
echo "FIN E6. $(( $(wc -l < "$CSV") - 1 )) celdas en $CSV"
