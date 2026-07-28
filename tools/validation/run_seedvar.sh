#!/bin/bash
# run_seedvar.sh — cuantas semillas hacen falta en cada brazo de canal.
#
# Con canal determinista (shadowingModel=none) la unica aleatoriedad que queda
# son las colisiones y el desfase de arranque, asi que la dispersion entre
# semillas deberia caer respecto del brazo con sombreado. Si cae bastante, 10
# semillas bastan ahi y se ahorra un cuarto del computo total.
#
# Lo que hay que medir NO es la varianza del PDR sino la del CONTRASTE pareado
# (compuesta menos toa_only), porque es esa la que fija cuantas semillas se
# necesitan: las comparaciones del paper son todas pareadas por semilla, y en un
# contraste pareado la varianza comun a las dos ramas se cancela. Un brazo puede
# tener PDR muy disperso y contraste muy estable.
#
# Dos sondas: rendimiento a 40 ks (cubre E1/E2/E6/E9) y vida util a 300 ks
# (cubre E3/E7), porque el FND puede dispersar de forma muy distinta al PDR.
#
#   bash run_seedvar.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/seedvar
mkdir -p "$OUT/cells"
CSV="$OUT/seedvar.csv"
HDR="probe,chan,metric,nEd,seed,rc,pdr,fnd_s,t50_s,adm,gen,deliv"

cell() {
  local probe=$1 chan=$2 met=$3 n=$4 seed=$5
  local id="${probe}_${chan}_${met}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf stop extra
  case "$met" in
    composite) mf="--routeMetricMode=composite_score" ;;
    toa)       mf="--allowMetricModeOverride=true --routeMetricMode=toa_only" ;;
    *) return 1 ;;
  esac
  if [ "$probe" = "perf" ]; then
      stop=40000; extra=""
  else
      stop=300000; extra="--allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400"
  fi

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=$stop --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --shadowingModel="$chan" --enablePcap=false --verboseLogs=false \
      --enableMetricsEssentialOnly=true $extra $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  P=$probe C=$chan M=$met N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os
e = os.environ
pdr = adm = gen = deliv = ""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr = j["pdr"]["pdr"]; gen = j["pdr"]["total_data_generated"]
    deliv = j["pdr"]["delivered"]; adm = j["tx_attempts"]["admission_ratio"]
except Exception:
    pass
fnd = t50 = ""
try:
    for r in csv.reader(open("mesh_dv_metrics_lifetime.csv")):
        if r and r[0] == "fnd_s": fnd = r[1]
        if r and r[0] == "t50_s": t50 = r[1]
except Exception:
    pass
open(e["ROW"], "w").write(",".join(str(x) for x in [
    e["P"], e["C"], e["M"], e["N"], e["SEED"], e["RC"], pdr, fnd, t50, adm, gen, deliv]) + "\n")
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

if ! "$BIN" --PrintHelp 2>&1 | grep -q "shadowingModel"; then
    echo "ABORTA: el binario no expone --shadowingModel."; exit 3
fi

{
  for chan in none static; do
    for met in composite toa; do
      for n in 25 49; do
        for seed in $(seq 1 20); do echo "perf $chan $met $n $seed"; done
      done
      for seed in $(seq 1 20); do echo "life $chan $met 25 $seed"; done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3 $4'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
echo "== SEEDVAR_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
