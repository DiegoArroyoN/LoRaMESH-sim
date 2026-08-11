#!/bin/bash
# run_e31_delta_limpio.sh — E27 rehecha sin el confusor.
#
# ===========================================================================
# QUE ESTABA MAL EN E27
# ===========================================================================
# E27 sostiene la tesis del paper: delta funciona sin duty (FND +13.41%, 30/30
# semillas) y cambia de signo con duty (-1.01%). Pero sus brazos movian TRES
# cosas a la vez, porque los pesos se usaban como un simplex que suma 1:
#
#     brazo   alfa   beta   delta      H = (beta/alfa)*C
#     d00     0.85   0.15   0.00           1493 ms
#     d25     0.60   0.15   0.25           2116 ms
#     d85     0.00   0.15   0.85        INFINITO   <-- alfa=0: SIN termino de ToA
#
# En d85 no hay termino de ToA en absoluto: la metrica es saltos + energia. Asi
# que "delta=0.85 mejora el FND un 13.41%" mezcla el efecto de delta con el de
# haber borrado el ToA y mandado H a infinito.
#
# QUE SOBREVIVE Y QUE NO. El confusor es IDENTICO en los dos regimenes de duty,
# asi que el cambio de signo -- la interaccion, que es la tesis -- se mantiene.
# Lo que no esta limpio es atribuirselo a delta. Y E29 muestra que mas H cuesta
# FND, o sea que el confusor jugaba EN CONTRA del hallazgo: el efecto real de
# delta sin duty es probablemente mayor de 13.41%.
#
# ===========================================================================
# COMO SE ARREGLA
# ===========================================================================
# Con la parametrizacion desacoplada (alfa === 1, beta' = H/C, delta'
# independiente) mover delta ya NO mueve H. Todos los brazos comparten:
#
#     alfa = 1        beta' = 0.01524  (H = 129 ms)   paso = 0.005
#
# y lo unico que cambia entre ellos es delta'. Eso es lo que E27 creia estar
# haciendo.
#
# EQUIVALENCIA con los delta viejos, para que el barrido sea comparable. Lo que
# importa es la penalizacion maxima relativa al coste de un enlace:
#
#     delta viejo 0.25 -> 1.59x el enlace   ~   delta' 0.05 -> 2.18x
#     delta viejo 0.85 -> 5.41x el enlace   ~   delta' 0.15 -> 6.55x
#
# Barrido: delta' en 0, 0.005, 0.015, 0.05 y 0.15, o sea de "un quinto del
# enlace" a "seis veces y media el enlace".
#
# ===========================================================================
# EL ESCENARIO NO CAMBIA, Y ES DELIBERADO
# ===========================================================================
# Se mantiene SF7-8 y la rejilla de 178 m all-to-all: es el escenario de MAXIMO
# RELEVO, ganador de los 72 de probe_relay_dominance.sh, y el relevo es el
# terreno donde delta puede actuar. Restringir el SF reduce el alcance directo y
# fuerza el encaminamiento. Si delta no luce aqui, no luce en ningun sitio.
#
# (Ojo: esto NO es el error que tumbo a E30 v1. Alli el sfMax=8 anulaba el
# mecanismo bajo estudio -- H solo actua si la metrica puede elegir SF alto.
# Aqui el mecanismo es el reparto de relevo, que SF7-8 maximiza.)
#
# ===========================================================================
# PREDICCION registrada antes de mirar
# ===========================================================================
# El cambio de signo se mantendra y sera MAS LIMPIO que en E27, porque ya no lo
# contamina la desaparicion del ToA. Espero que el efecto sin duty sea >= 13.41%
# por la razon de arriba. Si el cambio de signo DESAPARECE al quitar el confusor,
# entonces la tesis del paper se cae y hay que decirlo el mismo dia.
#
#   bash run_e31_delta_limpio.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-6}
SEEDS=${SEEDS:-20}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e31_delta_limpio
mkdir -p "$OUT/cells"
CSV="$OUT/e31_delta_limpio.csv"
HDR="duty,cfg,alpha,beta,delta,nEd,seed,rc,pdr,fnd_s,t50_s,soc_min,e_max,e_mean,rel_mean,rel_cv,sf_medio,hops,air_dato,frac_baliza"

ESC="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178
     --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8
     --allowBeaconOverride=true --beaconIntervalWarmSec=900 --beaconIntervalStableSec=900"
export ESC

celda() {
  local duty=$1 cfg=$2 al=$3 be=$4 dl=$5 n=$6 seed=$7
  local id="${duty}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=$al --compositeWHop=$be --compositeWEnergy=$dl --compositeCostStep=0.005"
  fi
  # La base comparable deja el duty APAGADO; --allowDutyOverride=true es lo que
  # lo ENCIENDE al 1%. O sea que el brazo `off` es simplemente no pasar la puerta.
  local df=""
  [ "$duty" = "on" ] && df="--allowDutyOverride=true"

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 5400 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      $df $ESC --socInitMin=0.75 --socInitMax=0.75 \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel=static --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1
  local rc=$?

  D=$duty C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
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
smin=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr: smin="%.6f"%fr[0]
except Exception: pass
rel={k:0.0 for k in tot}; ab=ad=0.0; sfsum=hops=dn=0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: t=float(r["toaUs"])
        except Exception: continue
        if r.get("dst")=="65535": ab+=t; continue
        ad+=t; dn+=1
        try: sfsum+=int(r["sf"]); hops+=int(r["hops"])
        except Exception: pass
        try: nid=int(r["nodeId"])
        except Exception: continue
        if nid in rel and r.get("src")!=r.get("nodeId"): rel[nid]+=t
except Exception: pass
aire=ab+ad
def cv(v): return st.stdev(v)/st.mean(v) if len(v)>1 and st.mean(v)>0 else 0.0
T=[tot[i] for i in sorted(tot)]; R=[rel[i]/1e6 for i in sorted(rel)]
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["D"],e["C"],e["AL"],e["BE"],e["DL"],e["N"],e["SEED"],e["RC"],pdr,fnd,t50,smin,
    f4(max(T)) if T else "", f4(st.mean(T)) if T else "",
    f4(st.mean(R)) if R else "", f4(cv(R)) if R else "",
    f4(sfsum/dn) if dn else "", f4(hops/dn) if dn else "",
    f4(ad/1e6), f4(ab/aire) if aire else ""])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# PRECONDICION, dos comprobaciones y las dos son de las que ya nos han costado
# una campaña entera:
#   (a) el duty MUERDE. Si el brazo `off` simulara igual que el `on`, la campaña
#       compararia una cosa consigo misma y concluiria "el duty no explica nada".
#   (b) el FND es MEDIBLE en los dos brazos. E18 corrio 320 celdas a 100 ks y
#       fnd_s salio -1 (centinela de "sin muertes") en TODAS: la campaña no pudo
#       medir lo unico que queria medir. Sin duty el consumo sube y el FND se
#       adelanta, asi que hay que confirmarlo en el brazo lento, que es `on`.
echo "== ¿muerde el duty, y hay muertes que medir? =="
pk=/tmp/e27chk_$$; rm -rf $pk; prev=""
for duty in off on; do
  d=$pk/$duty; mkdir -p "$d"; cd "$d" || exit 2
  df=""; [ "$duty" = "on" ] && df="--allowDutyOverride=true"
  # shellcheck disable=SC2086
  timeout 5400 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=300000 --rngRun=1 \
      $df $ESC --socInitMin=0.75 --socInitMax=0.75 \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel=static --enablePcap=false --verboseLogs=false \
      --routeMetricMode=composite_score --compositeWToa=1.0 --compositeWHop=0.01524 \
      --compositeWEnergy=0 --compositeCostStep=0.005 > run.log 2>&1 || {
      echo "  ABORTA: brazo duty=$duty no corre (rc=$?)"; rm -rf $pk; exit 5; }
  read -r v fnd <<<"$(python3 -c "
import csv, json
d=dict(zip(*[r for r in list(csv.reader(open('mesh_dv_effective_config.csv')))[:2]]))
fnd='NA'
for r in csv.reader(open('mesh_dv_metrics_lifetime.csv')):
    if r and r[0]=='fnd_s': fnd=r[1]
print('%s|pdr=%.4f|margen=%s|alfa=%s|beta=%s|paso=%s' % (d.get('duty'), json.load(open('mesh_dv_summary.json'))['pdr']['pdr'], d.get('sfmargin'), d.get('alpha'), d.get('beta'), d.get('coststep')), fnd)" 2>/dev/null)"
  echo "  duty=$duty -> $v   fnd_s=$fnd"
  python3 -c "import sys; sys.exit(0 if float('$fnd') > 0 else 1)" 2>/dev/null || {
      echo "  ABORTA: fnd_s=$fnd en el brazo '$duty'. Nadie muere en 300 ks y la"
      echo "          campaña no podria medir vida util. Subir el horizonte."; rm -rf $pk; exit 6; }
  [ -n "$prev" ] && [ "$prev" = "$v" ] && {
      echo "  ABORTA: identico con duty on y off. La puerta del duty NO muerde."; rm -rf $pk; exit 4; }
  prev=$v
done
cd "$HOME" || exit 2; rm -rf $pk
echo "  el duty muerde y hay muertes que medir: se puede correr."

CFGS="
d000   1.0 0.01524 0.000
d005   1.0 0.01524 0.005
d015   1.0 0.01524 0.015
d050   1.0 0.01524 0.050
d150   1.0 0.01524 0.150
toaref 0   0       0
"

{
  for duty in off on; do
    echo "$CFGS" | while read -r cfg al be dl; do
      [ -z "$cfg" ] && continue
      for n in 25 49; do
        for seed in $(seq 1 "$SEEDS"); do echo "$duty $cfg $al $be $dl $n $seed"; done
      done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4 $5 $6'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E31_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
