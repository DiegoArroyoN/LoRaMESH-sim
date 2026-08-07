#!/bin/bash
# run_e19_mecanismo_delta.sh — el MECANISMO de delta, en las tres topologias y
# en los dos rangos de SF.
#
# Hueco que señalo Diego el 31-jul, y es justo el que faltaba. El veredicto
# "delta = 0" ya esta establecido de sobra:
#
#     tres topologias                 E15
#     cuota completa 0 -> 0.85        E17
#     los dos regimenes de SF         E14, E16
#
# Pero el MECANISMO -- que delta equilibra la carga ESTIRANDO los enlaces, o sea
# subiendo el SF -- se midio en un solo punto: rejilla all-to-all con SF7-8. Y
# ahi solo existen DOS niveles de SF, asi que la escalada es casi binaria.
#
# Por que importa el rango completo: con SF7-12 hay SEIS niveles y el ToA de
# SF12 es ~20x el de SF7 (2.4 s frente a 118 ms). Si el mecanismo es real, con
# rango completo deberia ser MUCHO mas caro: delta tendria margen para escalar
# hasta SF11 o SF12 y el peaje en aire crecer de forma no lineal. Si en cambio
# la escalada se detiene igual, entonces lo que manda no es delta sino el
# alcance disponible, y el mecanismo hay que reescribirlo.
#
# Por que importan las topologias: cada una concentra el relevo distinto. En
# all-to-all la carga sale casi uniforme; en convergecast se embuda hacia un
# sumidero; con cuatro sumideros se reparte en cuatro vecindarios. Si el
# mecanismo de "ensanchar el vecindario" es el correcto, deberia lucir MAS donde
# el embudo es mas estrecho, o sea en convergecast.
#
# La cadencia de balizas se mantiene en 900 s, que es el valor con el que la
# sonda de 72 escenarios encontro el maximo de relevo (8.21% de la energia frente
# al 1.61% habitual). No es la cadencia del paper: es la que le da al mecanismo
# el mayor margen posible para manifestarse. Si no se ve ahi, no se ve.
#
# PREDICCION registrada antes de mirar:
#   1) con SF7-12 el aire por relevo subira MAS que con SF7-8 (mas niveles que
#      escalar), y el PDR caera mas.
#   2) el efecto sobre rel_cv sera mayor en convergecast que en all-to-all.
#   3) e_max seguira empeorando en las seis combinaciones: el peaje siempre gana.
# Si (3) falla en alguna, hay una condicion donde delta SI sirve y hay que
# caracterizarla, no descartarla.
#
#   bash run_e19_mecanismo_delta.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-14}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e19_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e19_mecanismo.csv"
HDR="topo,rango,stop_s,cfg,alpha,beta,delta,nEd,seed,rc,pdr,soc_min,psi_dentro,e_max,e_mean,e_cv,rel_n,rel_air,rel_per,rel_cv,hops,sf_mean,sf7,sf8,sf9,sf10,sf11,sf12"
export CSV

flags_topo() {
  case "$1" in
    a2a)    echo "--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178" ;;
    conv1)  echo "--nodePlacementMode=random --forcedDataDestinationId=0" ;;
    msink4) echo "--nodePlacementMode=random --numSinks=4" ;;
  esac
}
flags_rango() {
  # SF7-12 es el default del perfil: NO se pasa la puerta.
  [ "$1" = "sf78" ] && echo "--allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8" || echo ""
}
# Horizonte POR TOPOLOGIA, calibrado el 31-jul. Con balizas cada 900 s el gasto
# baja mucho y cada topologia drena a ritmo distinto: a 100 ks all-to-all queda
# en p50=0.42-0.47 (dentro de la rampa) pero convergecast y multi-sink se quedan
# en 0.57, POR ENCIMA, con Psi viva en solo 8-12 de 49. La primera version de
# esta campaña aborto por eso, que es exactamente para lo que existe la
# precondicion. Medido: a 200 ks las dos bajan a p50=0.40-0.42 con 34-37 de 49
# dentro; a 400 ks ya estan muertas (p50=0.07-0.12, solo 9-14 dentro).
#
# El horizonte distinto entre topologias NO confunde el contraste: la
# comparacion es delta frente a delta=0 DENTRO de cada topologia, al mismo
# horizonte. Lo que no se puede es comparar valores absolutos entre topologias.
horizonte() {
  [ "$1" = "a2a" ] && echo 100000 || echo 200000
}
export -f flags_topo flags_rango horizonte

BEAC="--allowBeaconOverride=true --beaconIntervalWarmSec=900 --beaconIntervalStableSec=900"
export BEAC

celda() {
  local topo=$1 rango=$2 cfg=$3 al=$4 be=$5 dl=$6 n=$7 seed=$8
  local id="${topo}_${rango}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=$al --compositeWHop=$be --compositeWEnergy=$dl"
  fi
  local tf rf st
  st=$(horizonte "$topo")
  tf=$(flags_topo "$topo"); rf=$(flags_rango "$rango")

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 1200 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=$st --rngRun="$seed" \
      --allowDutyOverride=true $tf $rf $BEAC \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1
  local rc=$?

  T=$topo R=$rango ST=$st C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os, statistics as st
e=os.environ
pdr=""
try: pdr=json.load(open("mesh_dv_summary.json"))["pdr"]["pdr"]
except Exception: pass
tot={}
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tot[int(r["nodeId"])]=float(r["txMah"])+float(r["rxMah"])+float(r["cadMah"])+float(r["idleMah"])
except Exception: pass
smin=dentro=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin="%.6f"%fr[0]; dentro=sum(1 for x in fr if 0.20<x<0.50)
except Exception: pass
# El histograma de SF de los RELEVOS es lo que distingue "estirar el enlace" de
# "fragmentar la ruta". Sin el, el aire por relevo se puede confundir con carga.
rel={n:0.0 for n in tot}; reln=0; relair=0.0
sfh=[0]*6; sfsum=hops=dn=0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        if r.get("dst")=="65535": continue
        try:
            nid=int(r["nodeId"]); t=float(r["toaUs"]); sf=int(r["sf"])
        except Exception: continue
        dn+=1; sfsum+=sf
        if 7<=sf<=12: sfh[sf-7]+=1
        try: hops+=int(r["hops"])
        except Exception: pass
        if r.get("src")!=r.get("nodeId"):
            reln+=1; relair+=t
            if nid in rel: rel[nid]+=t
except Exception: pass
def cv(v):
    return st.stdev(v)/st.mean(v) if len(v)>1 and st.mean(v)>0 else 0.0
T=[tot[i] for i in sorted(tot)]; R=[rel[i]/1e6 for i in sorted(rel)]
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["T"],e["R"],e["ST"],e["C"],e["AL"],e["BE"],e["DL"],e["N"],e["SEED"],e["RC"],pdr,smin,dentro,
    f4(max(T)) if T else "", f4(st.mean(T)) if T else "", f4(cv(T)) if T else "",
    reln, f4(relair/1e6), f4(relair/reln/1000) if reln else "", f4(cv(R)) if R else "",
    f4(hops/dn) if dn else "", f4(sfsum/dn) if dn else "", *sfh])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# PRECONDICION por cada combinacion topologia x rango: Psi tiene que discriminar
# o la celda no dice nada sobre delta. Es lo que invalido el brazo conv de E13.
echo "== Psi viva en las SEIS combinaciones? =="
mal=0
for topo in a2a conv1 msink4; do
  for rango in sf78 sf712; do
    d=/tmp/p19_$$/${topo}_${rango}; rm -rf "$d"; mkdir -p "$d"; cd "$d" || exit 2
    # shellcheck disable=SC2086
    "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=49 --stopSec=$(horizonte $topo) --rngRun=1 \
        --allowDutyOverride=true $(flags_topo $topo) $(flags_rango $rango) $BEAC \
        --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
        --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
        --routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 \
        --compositeWEnergy=0 > run.log 2>&1
    v=$(python3 -c "
import csv
try: fr=sorted(float(r['energyFrac']) for r in csv.DictReader(open('mesh_dv_metrics_energy.csv')))
except Exception: fr=[]
if not fr: print('SIN DATOS 0 0')
else:
    d=sum(1 for x in fr if 0.20<x<0.50)
    print('%d %d %.4f' % (d, len(fr), fr[len(fr)//2]))
" 2>/dev/null)
    set -- $v
    if [ "$1" = "SIN" ]; then echo "  $topo/$rango  SIN DATOS"; mal=1; continue; fi
    echo "  $topo/$rango  dentro de la rampa: $1/$2   p50=$3"
    [ "$1" -lt $(( $2 / 3 )) ] && mal=1
  done
done
cd "$HOME" || exit 2; rm -rf /tmp/p19_$$
[ $mal -eq 0 ] || { echo "ABORTA: en alguna combinacion Psi no discrimina."; exit 6; }

CFGS="
d00    0.85 0.15 0.00
d25    0.60 0.15 0.25
d85    0.00 0.15 0.85
"

{
  for topo in a2a conv1 msink4; do
    for rango in sf78 sf712; do
      echo "$CFGS" | while read -r cfg al be dl; do
        [ -z "$cfg" ] && continue
        for n in 25 49; do
          for seed in $(seq 1 "$SEEDS"); do echo "$topo $rango $cfg $al $be $dl $n $seed"; done
        done
      done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4 $5 $6 $7'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E19_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
