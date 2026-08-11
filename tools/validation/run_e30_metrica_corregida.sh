#!/bin/bash
# run_e30_metrica_corregida.sh — la metrica compuesta con H bien dimensionado.
#
# ===========================================================================
# POR QUE E16 NO SE PUEDE SIMPLEMENTE RELANZAR
# ===========================================================================
# E16 barria alfa/beta creyendo medir "el reparto entre ToA y saltos". En
# realidad barria H = (beta/alfa)*C, el aire que la metrica dice que vale un
# salto, y lo hacia en el rango equivocado ENTERO:
#
#     0.95/0.05 ->    445 ms       0.50/0.50 ->   8462 ms
#     0.85/0.15 ->   1493 ms       0.30/0.70 ->  19745 ms
#     0.70/0.30 ->   3627 ms
#
# Un paquete SF7 de 20 B ocupa 64.8 ms. O sea que el brazo MAS BARATO de E16 ya
# valoraba un salto en siete paquetes, y el mas caro en trescientos. E29 (440
# celdas) extendio el barrido hacia abajo y encontro que por debajo de 129 ms
# todo satura: SF medio 7.300, saltos 0.231 y PDR 0.204 identicos hasta el tercer
# decimal. La frontera "plana" de E16 era plana porque estaba entera fuera de la
# zona util.
#
# ===========================================================================
# EL ARREGLO: DESACOPLAR LOS TRES TERMINOS
# ===========================================================================
# La raiz del problema es que alfa, beta y delta se usaban como un simplex que
# suma 1, y eso los ACOPLA: tocar delta mueve alfa, y mover alfa mueve H. Por eso
# E27 salio confundida -- su brazo d85 tenia alfa=0, o sea NINGUN termino de ToA,
# y H infinito.
#
#     ANTES:  coste = alfa*T_hat + beta + delta*Psi(b)     con alfa+beta+delta=1
#     AHORA:  coste = T_hat + beta' + delta'*Psi(b)        alfa === 1
#
#   - alfa desaparece como grado de libertad. No se pierde nada: solo importaban
#     los cocientes entre terminos.
#   - beta' = H/C, con lectura fisica directa: beta'*C es el aire que vale un
#     salto. Para H=129 ms -> beta' = 0.01524.
#   - delta' queda independiente: cambiarlo ya NO mueve H.
#
# ===========================================================================
# Y EL PASO DE CUANTIZACION TAMBIEN CAMBIA, POR OBLIGACION
# ===========================================================================
# El coste se redondea a multiplos de `compositeCostStep` para caber en el byte
# del score. Dos condiciones se pelean:
#
#     (A) paso pequeño, o SF7 y SF8 caen en la misma unidad y el ToA se pierde
#     (B) paso grande, o un camino largo desborda las 255 unidades del byte
#
# Con el paso viejo (0.025) y los pesos viejos, SF7 (0.15651) y SF8 (0.16198)
# caian AMBOS en 6 unidades: el termino de ToA se redondeaba a nada al
# re-anunciar la ruta. Con la parametrizacion nueva y paso 0.005:
#
#     SF7 -> 5 unidades      SF10 -> 13        (A) se resuelven
#     SF8 -> 6 unidades      SF12 -> 40        (B) caben 55 saltos SF7
#
# Y el termino de ToA pasa de aportar el 4.2% del coste de enlace en SF7 al 33.4%.
#
# ===========================================================================
# QUE MIDE ESTA CAMPAÑA
# ===========================================================================
# Cinco configuraciones, las dos sondas de E16 (perf a 40 ks, life a 300 ks) y
# toa_only como referencia externa:
#
#     h43    beta'=0.00508   H =   43 ms
#     h129   beta'=0.01524   H =  129 ms   <- punto de operacion propuesto
#     h445   beta'=0.05260   H =  445 ms
#     h1493  alfa=0.85 beta=0.15 paso=0.025  <- LA VIEJA, tal cual, como control
#     toaref toa_only
#
# El brazo h1493 va con sus pesos y su paso originales a proposito: es el unico
# modo de que la comparacion sea "lo que teniamos" contra "lo corregido", no una
# version intermedia que no existio nunca.
#
# 5 x 2 sondas x 2 tamaños x 20 semillas = 400 celdas.
#
# PREDICCIONES registradas antes de mirar:
#  1) en la sonda `life` se reproducira E29: h129 y h43 iguales entre si y mejores
#     que h1493 en PDR (+12%) y FND (+11%). Si no se reproduce, una de las dos
#     campañas tiene un problema y hay que pararse.
#  2) en la sonda `perf`, que E29 no cubrio, espero que la ganancia sea MAYOR: a
#     40 ks el sistema esta en regimen de entrega y el aire desperdiciado en SF
#     alto pesa mas que en el horizonte largo.
#  3) toa_only seguira ganando en FND y perdiendo en PDR. El canje no desaparece
#     al corregir H: se recoloca.
#
# ===========================================================================
# CORRECCION 2026-08-11 -- LA PRIMERA PASADA NO MEDIA LO QUE DECIA
# ===========================================================================
# La v1 heredo el escenario de E16 sin comprobarlo, y ese escenario anula el
# efecto bajo estudio por dos vias independientes:
#
#   sfMax=8       con solo SF7 y SF8 disponibles la metrica NO PUEDE comprar
#                 enlaces de SF alto, que es justo el mecanismo que H controla.
#                 h1493 dio SF medio 7.280 y h129 7.309: sin diferencia, porque
#                 no habia nada que elegir.
#   balizas 60 s  el regimen saturado de control que E20 demostro mal
#                 dimensionado. E29 corrio a 900 s, el punto de operacion
#                 declarado, y por eso las dos campañas se contradecian.
#
# Se detecto comparando la configuracion efectiva ARCHIVADA de ambas: los dos
# ultimos campos daban beaconwarm=900 en E29 y 60 en E30. Mismo binario, 37
# columnas las dos, distinto escenario. Sin esas columnas -- añadidas el
# 2026-08-07 tras una pregunta de Diego -- la contradiccion habria sido
# indistinguible de un resultado real.
#
# De paso se elimina un backslash-n LITERAL heredado de E16: bash lo lee como un
# argumento suelto 'n' que se le colaba al binario. No rompia nada porque ns-3
# lo ignora, pero es el tercer script con el mismo defecto.
#
# La v2 fija SF7-12 y balizas 900 s, y assert_config los EXIGE en vez de
# registrarlos de paso: si el escenario vuelve a desviarse la campaña aborta
# antes de gastar una celda, en vez de producir 400 filas limpias que no miden
# nada.
#
#   bash run_e30_metrica_corregida.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
# Ver la nota de SUF en run_e14_energia.sh: relanzamiento a directorio limpio.
SUF=${SUF:-}
OUT=$HOME/ns3-runs/e30_${CHAN}${SUF}
mkdir -p "$OUT/cells"
CSV="$OUT/e30_metrica_corregida.csv"
HDR="probe,arm,cfg,alpha,beta,delta,wsum,nEd,seed,rc,pdr,adm,fnd_s,t50_s,gen,deliv,tx_sum,soc_min,dtx,d_sf_mean,d_air_s,hops_mean,relay_tx,q_raw_mean"

cell() {
  local probe=$1 arm=$2 cfg=$3 al=$4 be=$5 dl=$6 n=$7 seed=$8
  # paso 0.025 solo para el brazo viejo; el resto usa 0.005, que es el que
  # resuelve SF7 de SF8 a la escala nueva.
  local paso=0.005
  [ "$cfg" = "h1493" ] && paso=0.025
  local id="${probe}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=$al --compositeWHop=$be --compositeWEnergy=$dl --compositeCostStep=$paso"
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
      --shadowingModel="$CHAN" --allowBeaconOverride=true --beaconIntervalWarmSec=900 \
      --beaconIntervalStableSec=900 \
      --enablePcap=false --verboseLogs=false $extra $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  P=$probe A=$arm C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os
e = os.environ
def g(j, *ks, d=""):
    for k in ks:
        if isinstance(j, dict) and k in j: j = j[k]
        else: return d
    return j
pdr = adm = gen = deliv = relay = qraw = ""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr = g(j,"pdr","pdr"); gen = g(j,"pdr","total_data_generated")
    deliv = g(j,"pdr","delivered"); adm = g(j,"tx_attempts","admission_ratio")
    relay = g(j,"forwarding","forward_tx_sent_total")
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
wsum = float(e["AL"]) + float(e["BE"]) + float(e["DL"])
open(e["ROW"], "w").write(",".join(str(x) for x in [
    e["P"], e["A"], e["C"], e["AL"], e["BE"], e["DL"], f4(wsum), e["N"], e["SEED"], e["RC"],
    pdr, adm, fnd, t50, gen, deliv, f4(tx), smin, dn,
    f4(dsfsum/dn) if dn else "", f4(dair/1e6) if dn else "",
    f4(dhops/dn) if dn else "", relay, qraw]) + "\n")
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
         --allowDutyOverride=true --shadowingModel=$CHAN --enablePcap=false --verboseLogs=false --allowBeaconOverride=true --beaconIntervalWarmSec=900 --beaconIntervalStableSec=900" \
        sfmin=7 sfmax=12 wire=pueyo7b hyst=1 shadow=$CHAN spacing=178 mac=csmacad sfmargin=1 beaconwarm=900 \
        || { echo "ABORTA: la configuracion efectiva no es la declarada."; exit 5; }
    [ -s /tmp/cfgchk_$$/mesh_dv_effective_config.csv ] && echo "cell,$(head -1 /tmp/cfgchk_$$/mesh_dv_effective_config.csv)" > "$OUT/config_header.txt"
    rm -rf /tmp/cfgchk_$$
else
    echo "AVISO: falta assert_config.sh; la campaña corre SIN comprobar su configuracion."
fi

# arm  cfg      alpha  beta  delta      (suma)
CFGS="
A h43    1.0  0.00508 0.00
A h129   1.0  0.01524 0.00
A h445   1.0  0.05260 0.00
A h1493  0.85 0.15    0.00
R toaref 0    0       0
"

{
  for probe in perf life; do
    echo "$CFGS" | while read -r arm cfg al be dl; do
      [ -z "$arm" ] && continue
      for n in 25 49; do
        for seed in $(seq 1 "$SEEDS"); do echo "$probe $arm $cfg $al $be $dl $n $seed"; done
      done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3 $4 $5 $6 $7'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
CFGCSV="${CSV%.csv}_config.csv"
{ cat "$OUT/config_header.txt" 2>/dev/null; cat "$OUT"/cells/*.cfg 2>/dev/null | sort; } > "$CFGCSV"
echo "== E30_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
