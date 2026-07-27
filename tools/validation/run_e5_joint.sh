#!/bin/bash
# run_e5_joint.sh — barrido CONJUNTO alfa/beta/delta: PDR *y* bateria.
#
# Criterio de Diego (2026-07-26): los pesos de la tesis son provisionales; se
# quieren los que superen a toa_only en PDR **y** en bateria.
#
# Por que corridas largas y no perf de 40 ks: Psi(b) vale 0 por encima de
# bHi=0.50, y a 40 ks solo 4 de 25 nodos bajan de ese umbral -- el termino
# delta*Psi es INERTE por construccion y barrer delta ahi no mide nada. A 300 ks
# todos acaban en 0, o sea Psi saturado al maximo para todos, que es otra
# constante. El regimen donde delta discrimina es el intermedio: a 150 ks el SoC
# esta repartido a lo largo de la rampa (min 0.11, p50 0.25, max 0.52).
# Medido 2026-07-26; ver VALIDATION.md.
#
# Se corre a 300 ks para poder medir FND (la bateria), sabiendo que delta actua
# sobre todo en la parte media de la corrida.
#
#   alfa {0.2,0.6,1.0} x beta {0.02,0.05,0.15} x delta {0,0.25,0.5} + toa_only
#   x grid_a2a x N {25,49} x 10 semillas
#
#   bash run_e5_joint.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-15}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e5j
mkdir -p "$OUT/cells"
CSV="$OUT/e5_joint.csv"
HDR="alpha,beta,delta,nEd,seed,rc,pdr,fnd_s,t50_s,deliv,gen,soc_min,soc_p10,relay_tx,src_tx,tx_mah"

cell() {
  local a=$1 b=$2 dl=$3 n=$4 seed=$5
  local id="a${a}_b${b}_d${dl}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$a" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=$a --compositeWHop=$b --compositeWEnergy=$dl"
  fi

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  A=$a B=$b D=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv,json,os
e=os.environ
def g(j,*ks,d=""):
    for k in ks:
        if isinstance(j,dict) and k in j: j=j[k]
        else: return d
    return j
pdr=deliv=gen=relay=srctx=""
try:
    j=json.load(open("mesh_dv_summary.json"))
    pdr=g(j,"pdr","pdr"); deliv=g(j,"pdr","delivered"); gen=g(j,"pdr","total_data_generated")
    relay=g(j,"forwarding","forward_tx_sent_total"); srctx=g(j,"tx_attempts","source_tx_sent_total")
except Exception: pass
fnd=t50=""
try:
    for r in csv.reader(open("mesh_dv_metrics_lifetime.csv")):
        if r and r[0]=="fnd_s": fnd=r[1]
        if r and r[0]=="t50_s": t50=r[1]
except Exception: pass
smin=sp10=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr: smin="%.6f"%fr[0]; sp10="%.6f"%fr[max(0,int(len(fr)*0.10))]
except Exception: pass
tx=0.0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")): tx+=float(r["txMah"])
except Exception: pass
open(e["ROW"],"w").write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%.1f\n"%(
    e["A"],e["B"],e["D"],e["N"],e["SEED"],e["RC"],pdr,fnd,t50,deliv,gen,smin,sp10,relay,srctx,tx))
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"
for n in 25 49; do
  echo "=== tanda N=$n ($(date '+%F %H:%M')) ==="
  {
    for seed in $(seq 1 10); do echo "toaref 0 0 $n $seed"; done
    for a in 0.2 0.6 1.0; do
      for b in 0.02 0.05 0.15; do
        for dl in 0.0 0.25 0.5; do
          for seed in $(seq 1 10); do echo "$a $b $dl $n $seed"; done
        done
      done
    done
  } | shuf | xargs -P "$JOBS" -L1 bash -c 'cell "$@"' _
  { echo "$HDR"; cat "$OUT"/cells/*.row 2>/dev/null; } > "$CSV"
  echo "  N=$n listo. filas: $(( $(wc -l < "$CSV") - 1 ))"
done
echo "FIN E5J. $(( $(wc -l < "$CSV") - 1 )) celdas en $CSV"
