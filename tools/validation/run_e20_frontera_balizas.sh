#!/bin/bash
# run_e20_frontera_balizas.sh — la frontera vida util / entrega que SI existe.
#
# Pregunta de Diego el 31-jul, despues de que delta quedara descartado en las
# tres topologias: "¿como podria mejorar el FND y el T50?".
#
# El reparto de energia medido (E14, 320 celdas) ordena las palancas y deja poco
# lugar a la duda:
#
#     escucha continua (reposo)  45.7%   NO HAY KNOB: el radio nunca duerme
#     balizas                    21.7%   si, beaconInterval*
#     trafico propio              9.9%   lo fija la aplicacion
#     relevo (lo unico del ruteo)  1.6%   agotado, y delta no lo rescata
#
# La mayor con diferencia es la escucha, y no esta implementada: no existe sueño
# ni RX ciclado, el reposo se cobra a corriente constante y su CV entre nodos es
# 0.00003, o sea una constante. Atacarla es trabajo de MAC.
#
# La que SI tenemos hoy es la cadencia de balizas, y tiene un intercambio que
# hay que MAPEAR y no suponer: menos balizas es menos energia, pero tambien
# convergencia mas lenta, rutas mas viejas y peor entrega. Este barrido dibuja
# esa frontera para poder elegir el punto de operacion con datos.
#
# 300 ks para que el FND sea medible: a 100 ks nadie muere y fnd_s sale -1, que
# es lo que invalido la primera version de E18. Y guarda de 5400 s porque a 300
# ks con N=49 una corrida no cabe en 900 s -- el otro error de aquella version,
# que mato 244 de 320 celdas por tiempo sin que el CSV lo delatara.
#
# PREDICCION registrada:
#   1) FND crecera de forma monotona al espaciar las balizas, y sera el efecto
#      mas grande que hayamos medido sobre vida util -- mayor que cualquier
#      cambio de metrica.
#   2) el PDR caera, pero NO de forma monotona: hasta cierto punto las balizas
#      sobran y quitarlas libera aire (menos colisiones, menos duty consumido);
#      pasado ese punto las rutas envejecen y la entrega se hunde.
#   3) si (2) se cumple, existe un optimo interior en PDR y ahi esta el punto de
#      operacion que hay que reportar.
#
#
# EXTENSION 31-jul: el primer barrido (60..1800 s) salio MONOTONO en las tres
# metricas -- FND +8.76%, T50 +8.64% y PDR +238.93% al pasar de 60 a 1800 s --
# o sea que no aparecio el punto de giro y la prediccion 2 fallo: el PDR no cae,
# sube. A 60 s las balizas se comen el 74.93% del aire y asfixian los datos.
# Se extiende a 3600, 7200 y 14400 s para encontrar donde las rutas envejecen
# lo bastante como para que la entrega empiece a caer. Sin ese punto no se puede
# recomendar una cadencia, solo decir "mas alta que la actual".
#   bash run_e20_frontera_balizas.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-14}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e20_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e20_frontera_balizas.csv"
HDR="baliza_s,cfg,nEd,seed,rc,pdr,fnd_s,t50_s,soc_min,e_mean,e_max,n_baliza,air_baliza,air_dato,frac_baliza,hops,sf_mean,rel_n"

celda() {
  local bcn=$1 cfg=$2 n=$3 seed=$4
  local id="b${bcn}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 --compositeWEnergy=0"
  fi
  # 60 s es el valor del perfil; para el resto hace falta abrir la puerta.
  local bf=""
  [ "$bcn" != "60" ] && bf="--allowBeaconOverride=true --beaconIntervalWarmSec=$bcn --beaconIntervalStableSec=$bcn"

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 5400 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --pueyoGridSpacingM=178 $bf \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1
  local rc=$?

  B=$bcn C=$cfg N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os, statistics as st
e=os.environ
pdr=""
try: pdr=json.load(open("mesh_dv_summary.json"))["pdr"]["pdr"]
except Exception: pass
fnd=t50=""
try:
    for r in csv.reader(open("mesh_dv_metrics_lifetime.csv")):
        if r and r[0]=="fnd_s": fnd=r[1]
        if r and r[0]=="t50_s": t50=r[1]
except Exception: pass
tot=[]
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tot.append(float(r["txMah"])+float(r["rxMah"])+float(r["cadMah"])+float(r["idleMah"]))
except Exception: pass
smin=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr: smin="%.6f"%fr[0]
except Exception: pass
nb=0; ab=ad=0.0; sfsum=hops=dn=reln=0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: t=float(r["toaUs"]); sf=int(r["sf"])
        except Exception: continue
        if r.get("dst")=="65535":
            nb+=1; ab+=t
        else:
            dn+=1; ad+=t; sfsum+=sf
            try: hops+=int(r["hops"])
            except Exception: pass
            if r.get("src")!=r.get("nodeId"): reln+=1
except Exception: pass
aire=ab+ad
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["B"],e["C"],e["N"],e["SEED"],e["RC"],pdr,fnd,t50,smin,
    f4(st.mean(tot)) if tot else "", f4(max(tot)) if tot else "",
    nb, f4(ab/1e6), f4(ad/1e6), f4(ab/aire) if aire else "",
    f4(hops/dn) if dn else "", f4(sfsum/dn) if dn else "", reln])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

for bcn in 60 150 300 600 900 1800 3600 7200 14400; do
  for cfg in comp toaref; do
    for n in 25 49; do
      for seed in $(seq 1 "$SEEDS"); do echo "$bcn $cfg $n $seed"; done
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E20_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
