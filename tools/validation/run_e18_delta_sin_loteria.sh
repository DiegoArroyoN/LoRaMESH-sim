#!/bin/bash
# run_e18_delta_sin_loteria.sh — delta cuando TODOS arrancan con la misma bateria.
#
# La sonda probe_drain_vs_load.sh del 31-jul refuto la explicacion que yo venia
# dando. Yo sostenia que el consumo de un nodo no depende de la carga que se le
# encamina; los datos dicen lo contrario:
#
#     el aire de RELEVO explica el 79% de la varianza del consumo por nodo
#     el reposo es un coste FIJO (CV 0.00003: 44.44 mAh identicos en 49 nodos)
#
# O sea que el relevo SI es lo que separa a un nodo de otro, y el mecanismo de
# delta apunta al sitio correcto. La causa del fracaso es otra, y es esta:
#
#     dispersion por la LOTERIA de bateria inicial U[60,100%]  sigma 30.7 mAh
#     dispersion por diferencias de CONSUMO                    sigma  4.2 mAh
#
# El primero en morir es, siete veces de cada ocho, el que arranco mas bajo, no
# el que mas relevo. Delta esta optimizando una variable que aporta el 12% de la
# dispersion mientras el 88% lo pone un sorteo que el ruteo no puede tocar.
#
# Este experimento quita el sorteo: socInitMin = socInitMax, todos al mismo SoC.
# Entonces el FND lo decide EXCLUSIVAMENTE quien mas gasta, que es la condicion
# en la que delta deberia lucir. Es la prueba limpia de la intuicion de Diego:
# "mandar los paquetes por los nodos con mas bateria deberia alargar la red".
#
# Se arranca en 0.75 y no en 1.00 a proposito: arrancando al 100% los nodos
# quedarian por encima de la rampa de Psi (0.20, 0.50) durante casi toda la
# corrida -- el error que invalido el brazo conv de E13.
#
# HORIZONTE 300 ks, corregido el 31-jul. La primera version corrio a 100 ks y
# ahi los nodos terminan en ~0.42: Psi discrimina, pero NADIE MUERE, asi que
# fnd_s salio -1 (centinela de "sin muertes") en las 320 celdas y la campaña no
# pudo medir lo unico que de verdad queria medir. Error de diseño mio: elegi el
# horizonte para que Psi estuviera viva y me olvide de que el FND necesita
# muertes. A 33 puntos de SoC por cada 100 ks, desde 0.75 la primera muerte cae
# cerca de 227 ks; 300 ks la deja dentro con margen, y de paso los nodos cruzan
# la rampa de Psi por el camino en vez de terminar en ella.
#
# PREDICCION registrada: si delta sirve para algo, tiene que ser aqui. Si
# tampoco aqui mejora FND ni soc_min, entonces el termino de energia no rescata
# la vida util en ninguna condicion alcanzable y eso es un resultado, no un
# fracaso: dice que la vida util de estas redes se gobierna desde el plan de
# balizas y el ciclo de escucha, no desde el encaminamiento.
#
# GUARDA DE TIEMPO 5400 s, corregida el 31-jul. Al subir el horizonte de 100 ks
# a 300 ks se me quedo el timeout en 900 s: con el servidor cargado (E19 en
# paralelo) 244 de las 320 celdas murieron por tiempo -- las 160 de N=49
# enteras -- y la campaña devolvio rc=124 en tres cuartas partes. El sintoma es
# traicionero porque el CSV sale con sus 320 filas y solo el codigo de retorno
# delata que no midieron nada.
#
#   bash run_e18_delta_sin_loteria.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e18_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e18_delta_sin_loteria.csv"
HDR="bat,cfg,alpha,beta,delta,nEd,seed,rc,pdr,fnd_s,t50_s,soc_min,soc_p10,psi_dentro,e_max,e_mean,e_cv,rel_max,rel_mean,rel_cv,r2_rel"

# Escenario de maximo relevo (ganador de los 72 de probe_relay_dominance.sh).
ESC="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178
     --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8
     --allowBeaconOverride=true --beaconIntervalWarmSec=900 --beaconIntervalStableSec=900"
export ESC

celda() {
  local bat=$1 cfg=$2 al=$3 be=$4 dl=$5 n=$6 seed=$7
  local id="${bat}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf bf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=$al --compositeWHop=$be --compositeWEnergy=$dl"
  fi
  # brazo `sorteo`: el U[60,100%] de siempre. brazo `igual`: todos en 0.75.
  case "$bat" in
    sorteo) bf="" ;;
    igual)  bf="--socInitMin=0.75 --socInitMax=0.75" ;;
    *) return 1 ;;
  esac

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 5400 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true $ESC $bf \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1
  local rc=$?

  B=$bat C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
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
tot={}
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tot[int(r["nodeId"])]=float(r["txMah"])+float(r["rxMah"])+float(r["cadMah"])+float(r["idleMah"])
except Exception: pass
smin=sp10=dentro=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin="%.6f"%fr[0]; sp10="%.6f"%fr[max(0,int(len(fr)*0.10))]
        dentro=sum(1 for x in fr if 0.20<x<0.50)
except Exception: pass
rel={n:0.0 for n in tot}
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: nid=int(r["nodeId"]); t=float(r["toaUs"])
        except Exception: continue
        if nid in rel and r.get("dst")!="65535" and r.get("src")!=r.get("nodeId"): rel[nid]+=t
except Exception: pass
def cv(v):
    return st.stdev(v)/st.mean(v) if len(v)>1 and st.mean(v)>0 else 0.0
def r2(xs,ys):
    if len(xs)<3: return ""
    mx,my=st.mean(xs),st.mean(ys)
    sx=sum((a-mx)**2 for a in xs); sy=sum((b-my)**2 for b in ys)
    if sx<=0 or sy<=0: return ""
    c=sum((a-mx)*(b-my) for a,b in zip(xs,ys))
    return "%.4f"%((c/(sx*sy)**0.5)**2)
ns=sorted(tot); T=[tot[i] for i in ns]; R=[rel[i]/1e6 for i in ns]
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["B"],e["C"],e["AL"],e["BE"],e["DL"],e["N"],e["SEED"],e["RC"],pdr,fnd,t50,
    smin,sp10,dentro,
    f4(max(T)) if T else "", f4(st.mean(T)) if T else "", f4(cv(T)) if T else "",
    f4(max(R)) if R else "", f4(st.mean(R)) if R else "", f4(cv(R)) if R else "",
    r2(R,T)])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

CFGS="
d00    0.85 0.15 0.00
d25    0.60 0.15 0.25
d85    0.00 0.15 0.85
toaref 0    0    0
"

{
  for bat in igual sorteo; do
    echo "$CFGS" | while read -r cfg al be dl; do
      [ -z "$cfg" ] && continue
      for n in 25 49; do
        for seed in $(seq 1 "$SEEDS"); do echo "$bat $cfg $al $be $dl $n $seed"; done
      done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4 $5 $6'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
CFGCSV="${CSV%.csv}_config.csv"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "$CFGCSV"
echo "== E18_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
