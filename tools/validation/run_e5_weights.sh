#!/bin/bash
# run_e5_weights.sh — barrido de pesos alfa/beta/delta para MAXIMIZAR PDR.
#
# Objetivo declarado por Diego (2026-07-26): usar siempre la metrica compuesta
# con los pesos que den el mejor PDR.
#
# Este barrido REEMPLAZA al del 2026-07-22, que quedo invalidado: entonces el
# termino de ToA estaba saturado (T_hat=1 en el 90% de los enlaces), asi que
# alfa multiplicaba una constante y beta era otra constante -- ninguno podia
# hacer nada, y el barrido concluyo "alfa y beta son inertes". Con la
# normalizacion corregida (2026-07-25) si tienen efecto.
#
# Rejilla GRUESA primero; luego se refina alrededor del mejor. La region
# interesante de beta es la baja: por encima de ~0.134 el constante por salto
# aplasta al termino de ToA (VALIDATION.md 2026-07-25, hallazgo A).
#
#   alfa in {0.2, 0.6, 1.0}   beta in {0.0, 0.05, 0.15}   delta in {0, 0.25}
#   x 2 escenarios x N {16,25,49} x 10 semillas = 1080 corridas perf
#
#   bash run_e5_weights.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-15}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e5w
mkdir -p "$OUT/cells"
CSV="$OUT/e5_weights.csv"
HDR="alpha,beta,delta,scenario,nEd,seed,rc,pdr,adm,fwd,delay_p50,gen,deliv,oh_ratio,relay_tx,src_tx"

cell() {
  local a=$1 b=$2 dl=$3 scen=$4 n=$5 seed=$6
  local id="a${a}_b${b}_d${dl}_${scen}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  # El criterio es SUPERAR a toa_only, asi que la referencia se corre en las
  # mismas semillas, escenarios y tamanos que las combinaciones de pesos.
  local metricflags
  if [ "$a" = "toaref" ]; then
      metricflags="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      metricflags="--routeMetricMode=composite_score --compositeWToa=$a --compositeWHop=$b --compositeWEnergy=$dl"
  fi

  local scenflags
  case $scen in
    grid_a2a)  scenflags="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all" ;;
    rnd_conv1) scenflags="--nodePlacementMode=random --forcedDataDestinationId=0" ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=40000 --rngRun="$seed" \
      --allowDutyOverride=true \
      --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      $scenflags $metricflags > run.log 2>&1) 2>/dev/null
  local rc=$?

  A=$a B=$b D=$dl SC=$scen N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import json,os
e=os.environ
def g(j,*ks,d=""):
    for k in ks:
        if isinstance(j,dict) and k in j: j=j[k]
        else: return d
    return j
pdr=adm=fwd=d50=gen=deliv=oh=relay=srctx=""
try:
    j=json.load(open("mesh_dv_summary.json"))
    pdr=g(j,"pdr","pdr"); gen=g(j,"pdr","total_data_generated"); deliv=g(j,"pdr","delivered")
    adm=g(j,"tx_attempts","admission_ratio"); fwd=g(j,"tx_attempts","delivered_per_tx_attempt")
    d50=g(j,"delay","p50_s"); oh=g(j,"overhead","ratio")
    relay=g(j,"forwarding","forward_tx_sent_total"); srctx=g(j,"tx_attempts","source_tx_sent_total")
except Exception: pass
open(e["ROW"],"w").write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"%(
    e["A"],e["B"],e["D"],e["SC"],e["N"],e["SEED"],e["RC"],
    pdr,adm,fwd,d50,gen,deliv,oh,relay,srctx))
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"
echo "Barrido de pesos: rejilla gruesa, $JOBS jobs ($(date '+%F %H:%M'))"
{
  # referencia obligatoria: toa_only en las mismas celdas
  for scen in grid_a2a rnd_conv1; do
    for n in 16 25 49; do
      for seed in $(seq 1 10); do echo "toaref 0 0 $scen $n $seed"; done
    done
  done
  # Rejilla informada por el piloto en WSL (2026-07-26): la meseta esta en
  # beta >= 0.02; beta = 0 se conserva solo como control, porque con alfa bajo
  # degrada mucho (-5.5% de PDR y MAS relevo, no menos).
  for a in 0.2 0.6 1.0; do
    for b in 0.0 0.02 0.05 0.15; do
      for dl in 0.0 0.25; do
        for scen in grid_a2a rnd_conv1; do
          for n in 16 25 49; do
            for seed in $(seq 1 10); do echo "$a $b $dl $scen $n $seed"; done
          done
        done
      done
    done
  done
} | shuf | xargs -P "$JOBS" -L1 bash -c 'cell "$@"' _

{ echo "$HDR"; cat "$OUT"/cells/*.row 2>/dev/null; } > "$CSV"
echo "FIN E5W. $(( $(wc -l < "$CSV") - 1 )) celdas en $CSV"
