#!/bin/bash
# run_e3.sh — bloque E3 del DoE: vida útil (Q1 en su terreno propio).
#
#   {composite, toa} × {grid_a2a, rnd_conv1} × N {25,49,100} × 20 seeds = 240
#   Corridas lifetime: 300 ks, tráfico sostenido (1400 paq/par), duty-on 1%.
#
# Métricas: FND, T50, cola del SoC (min y p10 — el mecanismo del 22-jul),
# desglose de energía, y fracción de relevo. Todo del summary json y de dos
# CSV pequeños (uno por nodo); los trazados pesados se descartan.
#
# Ordenado por N ascendente: N=25 y N=49 dan resultados usables temprano, y el
# coste real de N=100 se mide antes de comprometerse con las 80 corridas.
# Reanudable: salta celdas cuya fila ya existe.
#
#   bash run_e3.sh [ns3_dir] [jobs]
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
OUT=$HOME/ns3-runs/e3_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e3_results.csv"
HDR="metric,scenario,nEd,seed,rc,secs,fnd_s,t50_s,pdr,gen,deliv,last_data_s,soc_min,soc_p10,soc_mean,tx_mah,idle_mah,rx_mah,relay_tx,src_tx"

cell() {
  local metric=$1 scen=$2 n=$3 seed=$4
  local id="${metric}_${scen}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  # Carga por escenario: `pueyoPacketsPerPair` cuenta paquetes POR PAR
  # origen-destino. En all-to-all cada nodo tiene N-1 destinos, así que emite
  # 1400*(N-1); en convergecast tiene uno solo y emitiría 1400, agotando el
  # tráfico a los ~15 ks de una corrida de 300 ks (medido) y dejando que el FND
  # midiera balizado en red vacía — el defecto de VALIDATION 2026-07-22 (b).
  # Se escala para que cada nodo emita el MISMO número de paquetes en ambos
  # escenarios, que es lo que iguala la tasa por nodo.
  local scenflags metricflags ppp
  case $scen in
    grid_a2a)  scenflags="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all"
               ppp=1400 ;;
    rnd_conv1) scenflags="--nodePlacementMode=random --forcedDataDestinationId=0"
               ppp=$(( 1400 * (n - 1) )) ;;
  esac
  case $metric in
    composite) metricflags="--routeMetricMode=composite_score" ;;
    toa)       metricflags="--allowMetricModeOverride=true --routeMetricMode=toa_only" ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  local t0=$SECONDS
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true --allowPacketsPerPairOverride=true --pueyoPacketsPerPair="$ppp" \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      $scenflags $metricflags > run.log 2>&1) 2>/dev/null
  local rc=$?
  local secs=$(( SECONDS - t0 ))

  MET=$metric SC=$scen N=$n SEED=$seed RC=$rc SECS=$secs ROW="$row" python3 - <<'PY'
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
# cola del SoC: minimo y percentil 10 entre nodos (el mecanismo de delta)
smin=sp10=smean=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin="%.6f"%fr[0]
        sp10="%.6f"%fr[max(0,int(len(fr)*0.10))]
        smean="%.6f"%(sum(fr)/len(fr))
except Exception: pass
tx=idle=rx=0.0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tx+=float(r["txMah"]); idle+=float(r["idleMah"]); rx+=float(r["rxMah"])
except Exception: pass
# ultimo dato transmitido: confirma que el trafico se sostuvo
last=""
try:
    mx=0.0
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        if r["dst"]!="65535":
            t=float(r["timestamp(s)"])
            if t>mx: mx=t
    last="%.0f"%mx
except Exception: pass
open(e["ROW"],"w").write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%.1f,%.1f,%.1f,%s,%s\n"%(
    e["MET"],e["SC"],e["N"],e["SEED"],e["RC"],e["SECS"],fnd,t50,pdr,gen,deliv,last,
    smin,sp10,smean,tx,idle,rx,relay,srctx))
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# Por tandas de N ascendente: la de N=25 termina pronto y ya es analizable.
for n in 25 49 100; do
  echo "=== tanda N=$n ($(date +%H:%M)) ==="
  for scen in grid_a2a rnd_conv1; do
    for met in composite toa; do
      for seed in $(seq 1 "$SEEDS"); do echo "$met $scen $n $seed"; done
    done
  done | shuf | xargs -P "$JOBS" -L1 bash -c 'cell "$@"' _
  { echo "$HDR"; cat "$OUT"/cells/*.row 2>/dev/null; } > "$CSV"
  echo "  N=$n listo. filas acumuladas: $(( $(wc -l < "$CSV") - 1 ))"
done
echo "FIN E3. $(( $(wc -l < "$CSV") - 1 )) celdas en $CSV"
