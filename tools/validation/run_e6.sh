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
# Canal y semillas por entorno: el mismo runner sirve para el brazo
# determinista (CHAN=none, la condicion de Pueyo-Centelles) y para el de
# sombreado por enlace (CHAN=static). La salida va a directorios distintos
# para que un brazo no se coma las celdas del otro por el cache de .row.
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
OUT=$HOME/ns3-runs/e6_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e6_results.csv"
HDR="metric,scenario,nEd,seed,rc,pdr,adm,fwd,delay_p50,delay_p95,gen,deliv,oh_ratio,src_tx,relay_tx,hops"

# Sonda previa: el binario debe conocer --floodingMode antes de encolar 320
# corridas. La leccion del 25-jul: 480 celdas rssi se perdieron por lanzar
# contra un binario sin la funcion.
mkdir -p /tmp/e6check && cd /tmp/e6check
if ! "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=9 --stopSec=3000 --rngRun=1 \
        --allowDutyOverride=true --floodingMode=true \
        --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false >/dev/null 2>&1; then
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
  # La configuracion efectiva viaja con la fila. Sin esto el unico rastro vive
  # en el JSON de la corrida, que se borra dos lineas mas abajo, y asi es como
  # sfMax=8 estuvo tres meses invisible.
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"

  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"
# La campaña DECLARA lo que cree medir y el binario lo confirma antes de gastar
# una sola celda. El sfMax=8 de los perfiles invalido cuatro campañas y
# sobrevivio tres meses porque nadie comparaba la configuracion efectiva contra
# la intencion; esto convierte ese defecto en un aborto en la primera celda.
# Solo se declaran INVARIANTES de la campaña: los factores que se barren
# (metrica, pesos, espaciado, caudal) cambian por celda y no se declaran aqui.
if [ -x "$HOME/assert_config.sh" ] || [ -f "$HOME/assert_config.sh" ]; then
    bash "$HOME/assert_config.sh" "$BIN" /tmp/cfgchk_$$         "--profile=proposal_pueyo_like_csmacad --nEd=16 --stopSec=6000 --rngRun=1          --allowDutyOverride=true --shadowingModel=$CHAN          --enablePcap=false --verboseLogs=false"         sfmin=7 sfmax=12 wire=pueyo7b hyst=1 shadow=$CHAN spacing=178 || { echo "ABORTA: la configuracion efectiva no es la declarada."; exit 5; }
    [ -s /tmp/cfgchk_$$/mesh_dv_effective_config.csv ] && echo "cell,$(head -1 /tmp/cfgchk_$$/mesh_dv_effective_config.csv)" > "$OUT/config_header.txt"
    rm -rf /tmp/cfgchk_$$
else
    echo "AVISO: falta assert_config.sh; la campaña corre SIN comprobar su configuracion."
fi

for scen in grid_a2a rnd_conv1; do
  for n in 9 16 25 36 49 64 81 100; do
    for seed in $(seq 1 "$SEEDS"); do echo "$scen $n $seed"; done
  done
done | shuf | xargs -P "$JOBS" -L1 bash -c 'cell "$@"' _

{ echo "$HDR"; cat "$OUT"/cells/*.row 2>/dev/null; } > "$CSV"
CFGCSV="${CSV%.csv}_config.csv"
{ cat "$OUT/config_header.txt" 2>/dev/null; cat "$OUT"/cells/*.cfg 2>/dev/null | sort; } > "$CFGCSV"
echo "FIN E6. $(( $(wc -l < "$CSV") - 1 )) celdas en $CSV"
