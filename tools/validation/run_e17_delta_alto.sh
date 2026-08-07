#!/bin/bash
# run_e17_delta_alto.sh — delta por encima de 0.25, y en su mejor escenario.
#
# Dos huecos que señalo Diego el 31-jul, los dos legitimos:
#
# HUECO 1 -- CUOTA DE DELTA. Todo lo que sostiene "delta = 0" se midio con
# delta <= 0.25 de cuota. E11 llego a delta=0.50 y 1.00, pero eran del brazo de
# ESCALA: alli alfa subia en la misma proporcion (0.60/0.15/0.25 por k), asi que
# el reparto relativo no cambiaba y no dicen nada sobre "mas peso a la energia".
# A suma constante, 0.25 es el maximo probado. Aqui se llega a 0.85, que es todo
# el presupuesto menos beta.
#
# HUECO 2 -- ESCENARIO. La conclusion se midio donde el relevo es el 1.61% de la
# energia. Preguntarle a delta que haga algo con el 1.6% del presupuesto es
# preguntarle lo imposible. La sonda probe_relay_dominance.sh barrio 72
# escenarios buscando donde el relevo pesa mas, y el ganador es este:
#
#     a2a, N=49, 178 m, SF7-8, balizas cada 900 s
#     relevo = 23.67% del aire y 8.21% de la energia (5x el caso base)
#
# Se corre AQUI. Si delta no aporta ni con el 85% del presupuesto ni en el
# escenario que mas relevo genera de 72 probados, entonces delta=0 deja de ser
# un resultado local y pasa a ser una propiedad del sistema.
#
# PREDICCION registrada antes de mirar: delta seguira sin mejorar FND ni soc_min,
# y el PDR caera de forma monotona con la cuota, porque quitarle presupuesto al
# termino de ToA empeora la eleccion de enlace. Si la prediccion falla y hay un
# optimo interior, el punto de operacion cambia y hay que rehacer E15/E16.
#
#   bash run_e17_delta_alto.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-10}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e17_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e17_delta_alto.csv"
HDR="cfg,alpha,beta,delta,nEd,seed,rc,pdr,soc_min,soc_p10,psi_dentro,tx_sum,tx_max,relay_tx,hops_mean,air_baliza,air_relevo,frac_relevo,relevo_cv,relevo_max,tot_cv"

# El escenario de maximo relevo, fijo para todas las celdas.
ESC="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178
     --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8
     --allowBeaconOverride=true --beaconIntervalWarmSec=900 --beaconIntervalStableSec=900"
export ESC

celda() {
  local cfg=$1 al=$2 be=$3 dl=$4 n=$5 seed=$6
  local id="${cfg}_n${n}_s${seed}"
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
  timeout 900 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=100000 --rngRun="$seed" \
      --allowDutyOverride=true $ESC \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1
  local rc=$?

  C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os, statistics
e=os.environ
pdr=relay=""
try:
    j=json.load(open("mesh_dv_summary.json"))
    pdr=j["pdr"]["pdr"]; relay=j["forwarding"]["forward_tx_sent_total"]
except Exception: pass
tx=[]; tot=[]
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        a,b,c,d=(float(r["txMah"]),float(r["rxMah"]),float(r["cadMah"]),float(r["idleMah"]))
        tx.append(a); tot.append(a+b+c+d)
except Exception: pass
smin=sp10=dentro=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin="%.6f"%fr[0]; sp10="%.6f"%fr[max(0,int(len(fr)*0.10))]
        dentro=sum(1 for x in fr if 0.20<x<0.50)
except Exception: pass
air={}; dh=dn=0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: nid=int(r["nodeId"]); toa=float(r["toaUs"])
        except Exception: continue
        a=air.setdefault(nid,{"b":0.0,"f":0.0,"r":0.0})
        if r.get("dst")=="65535": a["b"]+=toa
        else:
            dn+=1
            try: dh+=int(r["hops"])
            except Exception: pass
            if r.get("src")==r.get("nodeId"): a["f"]+=toa
            else: a["r"]+=toa
except Exception: pass
T={k:sum(a[k] for a in air.values()) for k in ("b","f","r")} if air else {"b":0,"f":0,"r":0}
ta=sum(T.values()) or 1.0
rel=[a["r"] for a in air.values()]
rcv=rmax=""
if len(rel)>1:
    mu=statistics.mean(rel)
    if mu>0:
        rcv="%.4f"%(statistics.stdev(rel)/mu); rmax="%.4f"%(max(rel)/mu)
tcv=""
if len(tot)>1 and statistics.mean(tot)>0:
    tcv="%.4f"%(statistics.stdev(tot)/statistics.mean(tot))
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["C"],e["AL"],e["BE"],e["DL"],e["N"],e["SEED"],e["RC"],pdr,smin,sp10,dentro,
    f4(sum(tx)) if tx else "", f4(max(tx)) if tx else "", relay,
    f4(dh/dn) if dn else "", f4(T["b"]/1e6), f4(T["r"]/1e6), f4(T["r"]/ta),
    rcv, rmax, tcv])+"\n")
PY
  # Rastro de configuracion por celda. Sin esto la campaña no puede demostrar
  # QUE simulo, solo afirmarlo -- que es justo lo que exigimos al resto y lo que
  # se me olvido aqui en la primera version.
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"

  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# PRECONDICION. Sin Psi discriminando, un delta inerte no distingue entre "el
# termino no sirve" y "Psi estaba fuera de su rampa", que es lo que invalido el
# brazo conv de E13. Se exige antes de encolar una sola celda.
echo "== Psi discrimina en este escenario a 100 ks? =="
pk=/tmp/psi17_$$; rm -rf $pk; mkdir -p $pk; cd $pk || exit 2
# shellcheck disable=SC2086
"$BIN" --profile=proposal_pueyo_like_csmacad --nEd=49 --stopSec=100000 --rngRun=1 \
    --allowDutyOverride=true $ESC --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
    --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
    --routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 \
    --compositeWEnergy=0 > run.log 2>&1
ver=$(python3 -c "
import csv
try: fr=sorted(float(r['energyFrac']) for r in csv.DictReader(open('mesh_dv_metrics_energy.csv')))
except Exception: fr=[]
if not fr: print('SIN DATOS')
else:
    d=sum(1 for x in fr if 0.20<x<0.50)
    print('  min=%.4f p50=%.4f max=%.4f  dentro de la rampa: %d/%d' % (fr[0],fr[len(fr)//2],fr[-1],d,len(fr)))
    print('VIVA' if d>=len(fr)/3 else 'MUERTA')
" 2>/dev/null)
cd "$HOME" || exit 2; rm -rf $pk
echo "$ver"
echo "$ver" | grep -q "^VIVA$" || { echo "ABORTA: Psi no discrimina aqui, la campaña no diria nada."; exit 6; }

# Suma constante 1, beta fijo en 0.15, delta se lo quita todo a alfa.
CFGS="
d00    0.85 0.15 0.00
d25    0.60 0.15 0.25
d40    0.45 0.15 0.40
d55    0.30 0.15 0.55
d70    0.15 0.15 0.70
d85    0.00 0.15 0.85
toaref 0    0    0
"

{
  echo "$CFGS" | while read -r cfg al be dl; do
    [ -z "$cfg" ] && continue
    for n in 25 49; do
      for seed in $(seq 1 "$SEEDS"); do echo "$cfg $al $be $dl $n $seed"; done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4 $5'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
CFGCSV="${CSV%.csv}_config.csv"
{ ls "$OUT"/cells/*.cfg >/dev/null 2>&1 && head -1 "$OUT"/config_header.txt 2>/dev/null
  cat "$OUT"/cells/*.cfg 2>/dev/null | sort; } > "$CFGCSV"
echo "== E17_DONE: $(( $(wc -l < "$CSV") - 1 )) filas, $(( $(wc -l < "$CFGCSV") )) con configuracion =="
