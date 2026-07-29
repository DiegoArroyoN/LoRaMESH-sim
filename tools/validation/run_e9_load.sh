#!/bin/bash
# run_e9_load.sh — barrido de carga: donde esta la rodilla y cual es el punto
# de operacion defendible.
#
# Todas las campañas hasta ahora corren a un solo caudal (el preajuste MEDIUM,
# un paquete cada 10 s por nodo) y salen con PDR de 0.006 a 0.48. La tasa de
# ADMISION explica por que: en rejilla all-to-all cae de 0.97 con 9 nodos a 0.23
# con 100, o sea que la red rechaza el 77% del trafico antes de emitirlo. Se
# esta midiendo un sistema dominado por la cola y la contencion, no por el
# ruteo, y parte de lo que atribuimos a la metrica podria ser comportamiento de
# cola: dPDR crece con N justo cuando la admision se desploma.
#
# Pueyo-Centelles reporta TRES regimenes (NotebookLM, 2026-07-28): low con PDR
# 0.95-0.98 en N^2=9 cayendo a 0.65-0.75 en N^2=64; high con 0.15-0.20 cayendo
# a 0.01-0.02; y saturacion con ~0.001 para metricas de un solo SF. Nuestras
# cifras caen entre su high y su low. Reportar una sola carga donde la
# referencia reporta tres es indefendible ante un revisor.
#
# Nueve caudales en escala casi logaritmica, de 2000 s por nodo (muy por debajo
# de la rodilla, donde el PDR deberia acercarse a su techo) hasta 5 s (bien
# dentro de saturacion). Las cuatro metricas, porque la pregunta no es solo
# donde satura la red sino si el ranking entre metricas SOBREVIVE fuera de
# saturacion: si el orden cambia al bajar la carga, entonces lo medido en E1 es
# un efecto de congestion y no una propiedad del ruteo.
#
#   bash run_e9_load.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
# Canal y semillas por entorno, como el resto de runners: mismo script
# para el brazo determinista y para el de sombreado, con salidas separadas.
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
# Exportar, no solo asignar: cell() corre en un bash nuevo lanzado por
# xargs, que hereda el entorno y no las variables de shell. Sin esto el
# valor por defecto no llega y --shadowingModel viaja vacio, que aborta
# cada corrida. Funcionaba al invocarlo como "CHAN=x bash runner" porque
# entonces la variable ya venia del entorno.
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e9_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e9_load.csv"
HDR="period,metric,nEd,seed,rc,pdr,adm,fwd,delay_p50,delay_p95,gen,deliv,oh_ratio,src_tx,relay_tx"

cell() {
  local per=$1 met=$2 n=$3 seed=$4
  local id="p${per}_${met}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  case "$met" in
    composite) mf="--routeMetricMode=composite_score" ;;
    toa)       mf="--allowMetricModeOverride=true --routeMetricMode=toa_only" ;;
    hops)      mf="--compositeWToa=0 --compositeWHop=1 --compositeWEnergy=0" ;;
    rssi)      mf="--allowMetricModeOverride=true --routeMetricMode=rssi" ;;
    *) return 1 ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=40000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --dataPeriodSec="$per" \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  PER=$per MET=$met N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import json, os
e = os.environ
def g(j, *ks, d=""):
    for k in ks:
        if isinstance(j, dict) and k in j: j = j[k]
        else: return d
    return j
pdr=adm=fwd=d50=d95=gen=deliv=oh=src=rel=""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr=g(j,"pdr","pdr"); gen=g(j,"pdr","total_data_generated"); deliv=g(j,"pdr","delivered")
    adm=g(j,"tx_attempts","admission_ratio"); fwd=g(j,"tx_attempts","delivered_per_tx_attempt")
    src=g(j,"tx_attempts","source_tx_sent_total"); rel=g(j,"forwarding","forward_tx_sent_total")
    d50=g(j,"delay","p50_s"); d95=g(j,"delay","p95_s"); oh=g(j,"overhead","ratio")
except Exception:
    pass
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["PER"], e["MET"], e["N"], e["SEED"], e["RC"], pdr, adm, fwd, d50, d95,
    gen, deliv, oh, src, rel]) + "\n")
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# El caudal tiene que MORDER. Si el binario fuese el viejo, --dataPeriodSec se
# aceptaria como flag desconocido o se ignoraria, las nueve columnas saldrian
# iguales y el barrido concluiria que la carga no influye -- justo lo contrario
# de lo que sabemos. Se comprueba por comportamiento: generar a 200 s tiene que
# dar mucho menos trafico que a 10 s.
probe=/tmp/e9probe; rm -rf $probe; mkdir -p $probe
for per in 200 10; do
  d=$probe/$per; mkdir -p "$d"; cd "$d" || exit 2
  "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=8000 --rngRun=1 \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --dataPeriodSec=$per --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --enablePcap=false --verboseLogs=false > run.log 2>&1
done
chk=$(cd $probe && python3 -c "
import json
def g(p):
    try: return float(json.load(open(p+'/mesh_dv_summary.json'))['pdr']['total_data_generated'])
    except Exception: return 0.0
a,b=g('200'),g('10')
print('generados a 200 s: %.0f   a 10 s: %.0f' % (a,b))
print('MUERDE' if b > 5*a > 0 else 'NO MUERDE')
" 2>/dev/null)
cd "$HOME" || exit 2
rm -rf $probe
echo "== sonda de caudal =="
echo "$chk"
if ! echo "$chk" | grep -q "^MUERDE"; then
    echo "ABORTA: --dataPeriodSec no cambia el trafico generado. Binario viejo?"
    exit 4
fi

for per in 2000 1000 500 200 100 50 20 10 5; do
  for met in composite toa hops rssi; do
    for n in 25 49; do
      for seed in $(seq 1 "$SEEDS"); do echo "$per $met $n $seed"; done
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
echo "== E9_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
