#!/bin/bash
# run_e4.sh — bloque E4 del DoE: el barrido de duty cycle (pregunta Q5).
#
#   DC ∈ {off, 0.25, 0.5, 0.75, 1, 1.5, 2}% × δ ∈ {0.25, 0} × grid_a2a
#        × N {25, 49} × 20 seeds = 560 corridas lifetime (300 ks, sostenido).
#
# Niveles elegidos por sonda (2026-07-25), no por intuición: la demanda natural
# de la red es ~1.9% de duty, así que límites de 2% o más NO atan — dan el mismo
# duty usado y el mismo PDR que 'off'. El barrido original {off,1,2,5,10} habría
# tenido un solo punto informativo. La transición vive en [0, 2].
#
# Traza la transición del "ecualizador": sin duty δ compra ~9.7% de FND
# (2026-07-22 d), con duty 1% no compra nada (2026-07-23). Este barrido dice
# dónde y cómo de brusca es la transición.
#
# DC=off no pasa --allowDutyOverride (el perfil ya trae el duty desactivado);
# los demás niveles lo activan con --dutyOverridePct.
#
#   bash run_e4.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-15}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e4
mkdir -p "$OUT/cells"
CSV="$OUT/e4_results.csv"
HDR="dc,delta,nEd,seed,rc,secs,fnd_s,t50_s,pdr,gen,deliv,last_data_s,soc_min,soc_p10,soc_mean,tx_mah,idle_mah,rx_mah,relay_tx,src_tx,duty_used"

cell() {
  local dc=$1 delta=$2 n=$3 seed=$4
  local id="dc${dc}_d${delta}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local dutyflags=""
  if [ "$dc" != "off" ]; then
      dutyflags="--allowDutyOverride=true --dutyOverridePct=$dc"
  fi

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  local t0=$SECONDS
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --routeMetricMode=composite_score --compositeWEnergy="$delta" \
      --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      $dutyflags > run.log 2>&1) 2>/dev/null
  local rc=$?
  local secs=$(( SECONDS - t0 ))

  DC=$dc DELTA=$delta N=$n SEED=$seed RC=$rc SECS=$secs ROW="$row" python3 - <<'PY'
import csv,json,os
e=os.environ
def g(j,*ks,d=""):
    for k in ks:
        if isinstance(j,dict) and k in j: j=j[k]
        else: return d
    return j
pdr=gen=deliv=relay=srctx=""
try:
    j=json.load(open("mesh_dv_summary.json"))
    pdr=g(j,"pdr","pdr"); gen=g(j,"pdr","total_data_generated"); deliv=g(j,"pdr","delivered")
    relay=g(j,"forwarding","forward_tx_sent_total"); srctx=g(j,"tx_attempts","source_tx_sent_total")
except Exception: pass
fnd=t50=""
try:
    for r in csv.reader(open("mesh_dv_metrics_lifetime.csv")):
        if r and r[0]=="fnd_s": fnd=r[1]
        if r and r[0]=="t50_s": t50=r[1]
except Exception: pass
smin=sp10=smean=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin="%.6f"%fr[0]; sp10="%.6f"%fr[max(0,int(len(fr)*0.10))]
        smean="%.6f"%(sum(fr)/len(fr))
except Exception: pass
tx=idle=rx=0.0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tx+=float(r["txMah"]); idle+=float(r["idleMah"]); rx+=float(r["rxMah"])
except Exception: pass
# duty realmente usado: confirma que el nivel pedido se aplico
duty=""
try:
    ds=[float(r["dutyUsed"]) for r in csv.DictReader(open("mesh_dv_metrics_duty.csv"))]
    if ds: duty="%.6f"%(sum(ds)/len(ds))
except Exception: pass
last=""
try:
    mx=0.0
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        if r["dst"]!="65535":
            t=float(r["timestamp(s)"])
            if t>mx: mx=t
    last="%.0f"%mx
except Exception: pass
open(e["ROW"],"w").write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%.1f,%.1f,%.1f,%s,%s,%s\n"%(
    e["DC"],e["DELTA"],e["N"],e["SEED"],e["RC"],e["SECS"],fnd,t50,pdr,gen,deliv,last,
    smin,sp10,smean,tx,idle,rx,relay,srctx,duty))
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

for n in 25 49; do
  echo "=== tanda N=$n ($(date +%H:%M)) ==="
  for dc in off 0.25 0.5 0.75 1 1.5 2; do
    for delta in 0.25 0.0; do
      for seed in $(seq 1 20); do echo "$dc $delta $n $seed"; done
    done
  done | shuf | xargs -P "$JOBS" -L1 bash -c 'cell "$@"' _
  { echo "$HDR"; cat "$OUT"/cells/*.row 2>/dev/null; } > "$CSV"
  echo "  N=$n listo. filas: $(( $(wc -l < "$CSV") - 1 ))"
done
echo "FIN E4. $(( $(wc -l < "$CSV") - 1 )) celdas en $CSV"
