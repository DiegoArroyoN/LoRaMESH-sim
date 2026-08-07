#!/bin/bash
# run_e25_delta_con_margen.sh — ¿cambia delta cuando el selector es coherente?
#
# Hueco que señalo Diego el 2026-08-03, y es real: E15, E17, E18 y E19 -- TODAS
# las campañas que establecieron "delta = 0" -- corrieron con sfLinkMarginDb=0,
# el valor que despues resulto hacer incoherentes al selector y al PHY. Y E24 si
# probo margen, pero con delta=0 fijo: comparaba compuesta contra toa_only, no
# el termino de energia.
#
# POR QUE PODRIA CAMBIAR. El mecanismo de delta que midio E19 es "subir el SF
# para ensanchar el vecindario": mas SF -> mas alcance -> mas nodos son relevos
# viables -> la carga se reparte. Con margen, el selector YA sube el SF por su
# cuenta, asi que el margen de maniobra de delta es distinto. Puede que:
#   (a) delta pierda su unica palanca (el SF ya esta arriba) y se vuelva inerte, o
#   (b) delta encuentre por fin un enlace estable sobre el que redistribuir y
#       empiece a rendir.
# No hay forma de saberlo sin medirlo, y (b) cambiaria una conclusion central.
#
# ESCENARIO. El mismo de E17/E18 -- rejilla a2a 178 m, SF7-8, balizas 900 s, el
# de maximo relevo de los 72 que barrio probe_relay_dominance.sh -- para que las
# celdas sean directamente comparables con lo ya medido.
#
# HORIZONTE 300 ks y guarda 5400 s: las dos lecciones de E18. A 100 ks nadie
# muere y fnd_s sale -1; y al subir el horizonte hay que subir el timeout o
# mueren tres cuartas partes de las celdas con rc=124 sin que el CSV lo delate.
#
# PREDICCION registrada antes de mirar: delta seguira sin mejorar FND ni soc_min
# tambien con margen, porque el techo estructural no cambia -- el relevo sigue
# siendo el 1.6% del presupuesto energetico y el margen no toca esa fraccion.
# Si me equivoco y delta mejora el FND con margen >= 1, entonces "delta = 0" era
# un artefacto de nuestra incoherencia y hay que rehacer E15, E17, E18 y E19.
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-14}
SEEDS=${SEEDS:-15}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e25_delta_margen
mkdir -p "$OUT/cells"
CSV="$OUT/e25_delta_margen.csv"
HDR="margen_db,cfg,alpha,beta,delta,nEd,seed,rc,pdr,fnd_s,t50_s,soc_min,psi_dentro,e_max,e_mean,rel_mean,rel_cv,sf_medio,hops"
ESC="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178
     --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8
     --allowBeaconOverride=true --beaconIntervalWarmSec=900 --beaconIntervalStableSec=900"
export ESC

celda() {
  local mg=$1 cfg=$2 al=$3 be=$4 dl=$5 n=$6 seed=$7
  local id="m${mg}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0
  local mf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=$al --compositeWHop=$be --compositeWEnergy=$dl"
  fi
  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 5400 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true $ESC \
      --allowSfMarginOverride=true --sfLinkMarginDb="$mg" \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel=static --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1
  local rc=$?
  MG=$mg C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
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
smin=dentro=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr: smin="%.6f"%fr[0]; dentro=sum(1 for x in fr if 0.20<x<0.50)
except Exception: pass
rel={n:0.0 for n in tot}; ssum=hops=dn=0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        if r.get("dst")=="65535": continue
        try: nid=int(r["nodeId"]); t=float(r["toaUs"]); sf=int(r["sf"])
        except Exception: continue
        dn+=1; ssum+=sf
        try: hops+=int(r["hops"])
        except Exception: pass
        if nid in rel and r.get("src")!=r.get("nodeId"): rel[nid]+=t
except Exception: pass
def cv(v):
    return st.stdev(v)/st.mean(v) if len(v)>1 and st.mean(v)>0 else 0.0
T=[tot[i] for i in sorted(tot)]; R=[rel[i]/1e6 for i in sorted(rel)]
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["MG"],e["C"],e["AL"],e["BE"],e["DL"],e["N"],e["SEED"],e["RC"],pdr,fnd,t50,smin,dentro,
    f4(max(T)) if T else "", f4(st.mean(T)) if T else "",
    f4(st.mean(R)) if R else "", f4(cv(R)) if R else "",
    f4(ssum/dn) if dn else "", f4(hops/dn) if dn else ""])+"\n")
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
 for mg in 0 1; do
  echo "$CFGS" | while read -r cfg al be dl; do
    [ -z "$cfg" ] && continue
    for n in 25 49; do
      for seed in $(seq 1 "$SEEDS"); do echo "$mg $cfg $al $be $dl $n $seed"; done
    done
  done
 done
} | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4 $5 $6'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E25_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
