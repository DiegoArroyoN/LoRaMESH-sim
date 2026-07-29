#!/bin/bash
# run_e10_weights.sh — los pesos, medidos por fin sobre el arbol corregido.
#
# Toda la evidencia anterior sobre alfa/beta/delta (el barrido conjunto del
# 27-jul, el de beta bajo, y E8) corrio con sfMax=8 y con el canal modelado como
# desvanecimiento por paquete. Los dos defectos estan arreglados y sabemos que
# entre ambos multiplicaban por diez los efectos entre metricas, asi que
# ninguna de aquellas conclusiones sobre pesos se sostiene.
#
# Diseño: una variable cada vez alrededor de los valores de la tesis
# (alfa=0.60, beta=0.15, delta=0.25). Es interpretable y evita el problema del
# barrido factorial anterior, donde 27 combinaciones daban todas lo mismo y no
# se podia decir cual peso mandaba.
#
# PREDICCION, registrada antes de correr:
#
#   ALFA es la palanca. El mecanismo medido es que la compuesta cambia cadenas
#   de saltos cortos por saltos largos de SF alto, lo que sube el airtime y
#   acorta la vida. Alfa pondera justamente el termino de ToA, asi que subirlo
#   deberia penalizar el SF alto y acercar la compuesta a toa_only: PDR a la
#   baja y FND al alza, de forma monotona. Si alfa NO mueve nada entre 0.2 y
#   4.0, el termino de ToA sigue inerte y eso es un defecto que perseguir, no
#   un resultado.
#
#   BETA deberia empujar en sentido contrario (menos saltos = mas SF alto).
#   DELTA no deberia hacer nada en las corridas de PDR, porque a 40 ks casi
#   ningun nodo baja del umbral b_hi=0.50 donde Psi despierta.
#
# Dos sondas separadas porque PDR y FND piden regimenes opuestos:
#
#   perf: 40 ks a 1000 s por nodo, o sea SIN SATURAR (admision 1.30-1.47 segun
#         E9). Es donde el PDR mide ruteo y no cola.
#   life: 300 ks con trafico sostenido, que es lo unico que agota baterias.
#         Aqui el PDR no es interpretable, y viceversa: no hay un solo punto
#         donde las dos cosas se midan bien, y conviene decirlo en el paper.
#
#   bash run_e10_weights.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
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
OUT=$HOME/ns3-runs/e10_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e10_weights.csv"
HDR="probe,cfg,alpha,beta,delta,nEd,seed,rc,pdr,adm,fnd_s,t50_s,gen,deliv,tx_sum,soc_min,dtx,d_sf_mean,d_air_s,hops_mean,relay_tx"

cell() {
  local probe=$1 cfg=$2 al=$3 be=$4 dl=$5 n=$6 seed=$7
  local id="${probe}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=$al --compositeWHop=$be --compositeWEnergy=$dl"
  fi
  local stop extra
  if [ "$probe" = "perf" ]; then
      stop=40000;  extra="--dataPeriodSec=1000"
  else
      stop=300000; extra="--allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400"
  fi

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=$stop --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      --enableMetricsEssentialOnly=true $extra $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  P=$probe C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os
e = os.environ
pdr = adm = gen = deliv = relay = ""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr = j["pdr"]["pdr"]; gen = j["pdr"]["total_data_generated"]
    deliv = j["pdr"]["delivered"]; adm = j["tx_attempts"]["admission_ratio"]
    relay = j["forwarding"]["forward_tx_sent_total"]
except Exception:
    pass
fnd = t50 = ""
try:
    for r in csv.reader(open("mesh_dv_metrics_lifetime.csv")):
        if r and r[0] == "fnd_s": fnd = r[1]
        if r and r[0] == "t50_s": t50 = r[1]
except Exception:
    pass
tx = 0.0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tx += float(r["txMah"])
except Exception:
    pass
smin = ""
try:
    fr = sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr: smin = "%.6f" % fr[0]
except Exception:
    pass
dn = dair = dhops = dsfsum = 0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        if r.get("dst") == "65535": continue
        try: sf = int(r["sf"]); toa = float(r["toaUs"])
        except (ValueError, KeyError): continue
        dn += 1; dair += toa; dsfsum += sf
        try: dhops += int(r["hops"])
        except (ValueError, KeyError): pass
except Exception:
    pass
f4 = lambda x: "%.4f" % x
open(e["ROW"], "w").write(",".join(str(x) for x in [
    e["P"], e["C"], e["AL"], e["BE"], e["DL"], e["N"], e["SEED"], e["RC"],
    pdr, adm, fnd, t50, gen, deliv, f4(tx), smin, dn,
    f4(dsfsum/dn) if dn else "", f4(dair/1e6) if dn else "",
    f4(dhops/dn) if dn else "", relay]) + "\n")
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"
for f in shadowingModel dataPeriodSec compositeWToa; do
    "$BIN" --PrintHelp 2>&1 | grep -q -- "$f" || { echo "ABORTA: falta --$f"; exit 3; }
done

# alfa barrido; beta y delta a los valores de la tesis
CFGS="
a0.2 0.2 0.15 0.25
a0.6 0.6 0.15 0.25
a1.0 1.0 0.15 0.25
a2.0 2.0 0.15 0.25
a4.0 4.0 0.15 0.25
b0.0 0.6 0.00 0.25
b0.05 0.6 0.05 0.25
b0.30 0.6 0.30 0.25
d0.0 0.6 0.15 0.00
d0.5 0.6 0.15 0.50
d1.0 0.6 0.15 1.00
toaref 0 0 0
"

{
  for probe in perf life; do
    echo "$CFGS" | while read -r cfg al be dl; do
      [ -z "$cfg" ] && continue
      for n in 25 49; do
        for seed in $(seq 1 "$SEEDS"); do echo "$probe $cfg $al $be $dl $n $seed"; done
      done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3 $4 $5 $6'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
echo "== E10_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
