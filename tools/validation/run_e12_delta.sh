#!/bin/bash
# run_e12_delta.sh — delta medido, por primera vez, con arbol limpio Y Psi viva.
#
# Historia de por que esto no estaba hecho. Se ha concluido tres veces que delta
# no sirve, y ninguna de las tres podia concluirlo:
#
#   barrido conjunto (27-jul)  arbol roto (sfMax=8, canal, histeresis) + Psi saturada
#   E8 a 150 ks (28-jul)       arbol roto,                              Psi VIVA
#   E10 (29-jul)               arbol limpio,                            Psi saturada (soc_min=0.0000)
#
# Nunca las dos condiciones a la vez. En E10 se repitio el defecto identificado
# el 26-jul -- medir delta donde Psi es constante -- y el cero estaba en la tabla
# y se leyo como resultado en lugar de como invalidacion. La auditoria de mandos
# demostro ademas que delta SI muerde cuando Psi esta viva: el coste bruto medio
# pasa de 0.340 a 0.795 al subir delta de 0 a 1. El termino esta bien cableado.
#
# Dos decisiones de diseño que hacen esta medida distinta de las anteriores:
#
#  1) 100 ks, CALIBRADO y no supuesto. Psi es una rampa entre b_lo=0.20 y
#     b_hi=0.50: por encima vale 0, por debajo satura en 1, y en las dos puntas
#     es una CONSTANTE que no cambia ningun orden. La calibracion del 29-jul,
#     con la siembra por defecto U[60,100%], midio cuantos nodos caen dentro:
#
#         40 ks   5/25       100 ks  19/25   <- optimo
#         60 ks  11/25       120 ks  19/25   (pero el minimo ya baja de b_lo)
#         80 ks  17/25
#
#     A 300 ks todos acaban en 0, que es lo que invalido E10.
#
#  2) Siembra POR DEFECTO, U[60,100%]. El primer intento sembraba bimodal DENTRO
#     de la rampa (0.30 y 0.48) y salio peor: con trafico sostenido los nodos la
#     atraviesan y salen por abajo, acabando todos en 0.0000, y el guardian
#     aborto la campaña. Hay que arrancar por encima y dejar que el consumo los
#     deposite dentro.
#
#  3) La precondicion se COMPRUEBA y aborta. Si el SoC se sale de la rampa, la
#     campaña se detiene en vez de producir una tabla que no dice nada sobre
#     delta y que se leera como si dijera. Es la leccion de E10 convertida en
#     codigo.
#
# Suma constante 1: delta recibe su cuota a costa de alfa, respetando la
# convencion de la tesis. La pregunta correcta no es "delta hace algo" sino
# "delta se gana su parte del presupuesto frente a darsela al airtime".
#
#   bash run_e12_delta.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e12_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e12_delta.csv"
HDR="cfg,alpha,beta,delta,nEd,seed,rc,pdr,fnd_s,t50_s,soc_min,soc_p10,soc_p50,soc_max,psi_dentro,tx_sum,tx_max,relay_tx,dtx,d_sf_mean,d_air_s,hops_mean,q_raw_mean"


cell() {
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

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=100000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os
e = os.environ
def g(j, *ks, d=""):
    for k in ks:
        if isinstance(j, dict) and k in j: j = j[k]
        else: return d
    return j
pdr = relay = qraw = ""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr = g(j,"pdr","pdr"); relay = g(j,"forwarding","forward_tx_sent_total")
    qraw = g(j,"quantization","metric_raw_mean")
except Exception:
    pass
fnd = t50 = ""
try:
    for r in csv.reader(open("mesh_dv_metrics_lifetime.csv")):
        if r and r[0] == "fnd_s": fnd = r[1]
        if r and r[0] == "t50_s": t50 = r[1]
except Exception:
    pass
tx = []
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tx.append(float(r["txMah"]))
except Exception:
    pass
# psi_dentro: cuantos nodos caen DENTRO de la rampa (0.20, 0.50), que son los
# unicos sobre los que Psi puede discriminar. Es la variable que convierte esta
# campaña en interpretable: sin ella, un delta inerte no distingue entre "el
# termino no sirve" y "se volvio a medir fuera de su rampa".
smin = sp10 = sp50 = smax = dentro = ""
try:
    fr = sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin = "%.6f" % fr[0]; smax = "%.6f" % fr[-1]
        sp10 = "%.6f" % fr[max(0, int(len(fr)*0.10))]
        sp50 = "%.6f" % fr[len(fr)//2]
        dentro = sum(1 for x in fr if 0.20 < x < 0.50)
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
    e["C"], e["AL"], e["BE"], e["DL"], e["N"], e["SEED"], e["RC"], pdr, fnd, t50,
    smin, sp10, sp50, smax, dentro,
    f4(sum(tx)) if tx else "", f4(max(tx)) if tx else "", relay, dn,
    f4(dsfsum/dn) if dn else "", f4(dair/1e6) if dn else "",
    f4(dhops/dn) if dn else "", qraw]) + "\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"

  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

if [ -f "$HOME/assert_config.sh" ]; then
    bash "$HOME/assert_config.sh" "$BIN" /tmp/cfgchk_$$ \
        "--profile=proposal_pueyo_like_csmacad --nEd=16 --stopSec=6000 --rngRun=1 \
         --allowDutyOverride=true --shadowingModel=$CHAN \
         --enablePcap=false --verboseLogs=false" \
        sfmin=7 sfmax=12 wire=pueyo7b hyst=1 shadow=$CHAN socmin=0.60 socmax=1.00 \
        || { echo "ABORTA: la configuracion efectiva no es la declarada."; exit 5; }
    [ -s /tmp/cfgchk_$$/mesh_dv_effective_config.csv ] && echo "cell,$(head -1 /tmp/cfgchk_$$/mesh_dv_effective_config.csv)" > "$OUT/config_header.txt"
    rm -rf /tmp/cfgchk_$$
fi

# PRECONDICION DURA: Psi tiene que estar viva a 100 ks o la campaña no se lanza.
# Es la leccion de E10 en forma de codigo: alli soc_min valia 0.0000 y se
# concluyo sobre delta de todas formas.
echo "== comprobando que Psi esta viva a 100 ks =="
pchk=/tmp/psichk_$$; rm -rf $pchk; mkdir -p $pchk; cd $pchk || exit 2
"$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=100000 --rngRun=1 \
    --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
    --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
    --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
    --routeMetricMode=composite_score --compositeWToa=0.60 --compositeWHop=0.15 \
    --compositeWEnergy=0.25 > run.log 2>&1
veredicto=$(python3 -c "
import csv
try:
    fr = sorted(float(r['energyFrac']) for r in csv.DictReader(open('mesh_dv_metrics_energy.csv')))
except Exception:
    print('SIN DATOS'); raise SystemExit
if not fr:
    print('SIN DATOS'); raise SystemExit
dentro = sum(1 for x in fr if 0.20 < x < 0.50)
print('SoC min=%.4f p50=%.4f max=%.4f   dentro de la rampa: %d de %d' %
      (fr[0], fr[len(fr)//2], fr[-1], dentro, len(fr)))
# Se exige que al menos un tercio de los nodos discrimine. Con menos, delta
# actuaria sobre un puñado de enlaces y el resultado no seria concluyente.
print('VIVA' if dentro >= len(fr)/3 else 'MUERTA')
" 2>/dev/null)
cd "$HOME" || exit 2
rm -rf $pchk
echo "$veredicto"
if ! echo "$veredicto" | grep -q "^VIVA$"; then
    echo "ABORTA: Psi no discrimina en este regimen, asi que delta no puede morder"
    echo "  y la campaña produciria una tabla que no dice nada sobre delta."
    echo "  Ajustar stopSec o la siembra de SoC antes de relanzar."
    exit 6
fi

CFGS="
d00    0.85 0.15 0.00
d05    0.80 0.15 0.05
d15    0.70 0.15 0.15
d25    0.60 0.15 0.25
toaref 0    0    0
"

{
  echo "$CFGS" | while read -r cfg al be dl; do
    [ -z "$cfg" ] && continue
    for n in 25 49; do
      for seed in $(seq 1 "$SEEDS"); do echo "$cfg $al $be $dl $n $seed"; done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3 $4 $5'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
CFGCSV="${CSV%.csv}_config.csv"
{ cat "$OUT/config_header.txt" 2>/dev/null; cat "$OUT"/cells/*.cfg 2>/dev/null | sort; } > "$CFGCSV"
echo "== E12_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
