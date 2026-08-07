#!/bin/bash
# run_e27_delta_sin_duty.sh — el ultimo hueco del claim sobre delta.
#
# LO QUE ESTA EN JUEGO. Todas las campañas que descartaron delta (E15, E17, E18,
# E19, E25) corrieron con duty al 1%. El viejo "+9.69% de FND sin duty" venia del
# binario roto y no vale. Asi que hoy no podemos distinguir entre:
#
#   (A) "el termino de energia no sirve"                 <- lo que decimos
#   (B) "la regulacion de duty cycle es lo que lo anula" <- mucho mas citable
#
# El mecanismo medido en E19 hace la pregunta legitima: delta reparte el relevo
# hasta un 22% mejor (40/40 celdas, p<0.0001) ESTIRANDO los enlaces -- sube el SF
# para que mas nodos queden al alcance. Cada transmision en SF mas alto dura el
# doble, y bajo duty al 1% el aire es el recurso que escasea, asi que el
# equilibrio que compra cuesta mas de lo que ahorra. Sin duty ese coste deberia
# desaparecer y el beneficio quedar al descubierto.
#
# DISEÑO. El duty entra como factor PAREADO, no como escenario aparte: las mismas
# semillas, el mismo escenario, la misma metrica, con y sin regulacion. Asi la
# comparacion no depende de nada externo.
#
#   4 configuraciones x 2 regimenes de duty x 2 tamaños x 15 semillas = 240 celdas
#
# Escenario de MAXIMO RELEVO, el mismo de E17/E18/E25 (ganador de los 72 de
# probe_relay_dominance.sh): rejilla all-to-all 178 m, SF7-8, balizas 900 s,
# 300 ks. Si delta no luce aqui, no luce en ningun sitio alcanzable.
#
# MARGEN: no se pasa. Desde el 2026-08-07 la base comparable vale 1.0 dB y este
# es el primer experimento que estrena ese valor por defecto. La columna
# `sfmargin` de mesh_dv_effective_config.csv lo deja archivado por celda.
#
# PREDICCION registrada antes de mirar:
#   1) si (B) es cierto, delta mejorara el FND en el brazo SIN duty y lo empeorara
#      en el brazo CON duty. El signo tiene que CAMBIAR entre brazos; que solo se
#      atenue no basta para sostener (B).
#   2) yo espero que NO cambie de signo, porque el techo estructural no lo pone el
#      duty sino el reparto de energia: el relevo es el 1.6% del presupuesto y el
#      reposo el 45.7%, y quitar el duty no mueve ninguna de las dos cifras.
#   3) sin duty el consumo sube y el FND se adelanta en los dos brazos. Eso es
#      esperado y no es el resultado; el resultado es el signo del contraste.
#
#   bash run_e27_delta_sin_duty.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-6}
SEEDS=${SEEDS:-15}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e27_sin_duty
mkdir -p "$OUT/cells"
CSV="$OUT/e27_delta_sin_duty.csv"
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
      mf="--routeMetricMode=composite_score --compositeWToa=$al --compositeWHop=$be --compositeWEnergy=$dl"
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
      --routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 \
      --compositeWEnergy=0 > run.log 2>&1 || {
      echo "  ABORTA: brazo duty=$duty no corre (rc=$?)"; rm -rf $pk; exit 5; }
  read -r v fnd <<<"$(python3 -c "
import csv, json
d=dict(zip(*[r for r in list(csv.reader(open('mesh_dv_effective_config.csv')))[:2]]))
fnd='NA'
for r in csv.reader(open('mesh_dv_metrics_lifetime.csv')):
    if r and r[0]=='fnd_s': fnd=r[1]
print('%s|pdr=%.4f|margen=%s' % (d.get('duty'), json.load(open('mesh_dv_summary.json'))['pdr']['pdr'], d.get('sfmargin')), fnd)" 2>/dev/null)"
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
d00    0.85 0.15 0.00
d25    0.60 0.15 0.25
d85    0.00 0.15 0.85
toaref 0    0    0
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
echo "== E27_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
