#!/bin/bash
# run_e16_frontera_malla.sh — la frontera alfa/beta en regimen MALLA.
#
# E11 barrio la frontera alfa/beta con SF7-12, que es el regimen ESTRELLA: ahi
# la malla degenera porque el alcance de SF12 (1058 m) cubre casi todo el
# despliegue y casi nadie relega (saltos 0.07). El 29-jul quedo establecido que
# alfa es el mando malla-estrella, pero eso se midio en un solo regimen.
#
# Con SF7-8 el alcance baja a 350 m y la red ES malla por construccion (saltos
# 0.33). Cabe esperar que alfa tenga menos margen ahi y sature antes, igual que
# beta satura en 0.15 cuando la red ya es estrella: seria la simetria del mismo
# fenomeno. Si el optimo se mueve, el punto de operacion depende del regimen y
# hay que reportarlo asi, no como un valor unico.
#
# Se verifico antes de lanzar que el punto recomendado (0.85/0.15/0) sigue
# batiendo a toa_only en malla: +18.55% de PDR ganando 40 de 40 celdas (E14).
# Lo que NO se sabe es si sigue siendo el optimo, que es otra pregunta.
#
# Por que hace falta rehacer el barrido de E10. Los pesos de la tesis
# (alfa=0.60, beta=0.15, delta=0.25) suman exactamente 1.00: hay una convencion
# de combinacion convexa, aunque el codigo no la imponga (ComputeLinkCost
# devuelve alfa*T_hat + beta + delta*Psi sin normalizar). El barrido de E10 movio
# alfa de 0.2 a 4.0 dejando beta y delta fijos, lo que cambio DOS cosas a la vez:
#
#     alfa    suma    ratio alfa/beta
#      0.2    0.60    1.3
#      0.6    1.00    4.0
#      4.0    4.40    26.7
#
# La escala no es inocua: el cuantizador usa paso 0.025 y recorta en 255, asi que
# multiplicar todo por 7 cambia la resolucion efectiva del coste. "Alfa es la
# palanca" se sostiene en sentido laxo, pero E10 no puede separar "mas peso al
# airtime" de "cuantizador mas grueso", y su unico punto que respeta la
# convencion es alfa=0.60.
#
# Este barrido separa las dos cosas:
#
#   BRAZO A (reparto): suma fija en 1, moviendo el reparto entre alfa y beta con
#   delta=0. Mide el efecto del PESO RELATIVO sin tocar la escala.
#
#   BRAZO B (cuota de delta): suma fija en 1, dandole a delta 0, 0.05, 0.15 o
#   0.25 a costa de alfa. Responde a la pregunta correcta, que no es "delta hace
#   algo" sino "delta se gana su parte del presupuesto".
#
#   BRAZO C (escala): los pesos de la tesis multiplicados por k = 0.25, 0.5, 2,
#   4. El ORDEN de las rutas es invariante a k antes de cuantizar, asi que todo
#   efecto que aparezca aqui es del cuantizador y no de la metrica. Si k no mueve
#   nada, la cuantizacion es inocua y el brazo A se interpreta limpio; si mueve,
#   hay que reportar el paso de cuantizacion como parametro de diseño.
#
# La escala es la unica prediccion falsable aqui: si k mueve el resultado tanto
# como el reparto, entonces buena parte de lo que E10 atribuyo a alfa era
# resolucion de cuantizador.
#
#   bash run_e11_wsum.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e16_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e16_frontera_malla.csv"
HDR="probe,arm,cfg,alpha,beta,delta,wsum,nEd,seed,rc,pdr,adm,fnd_s,t50_s,gen,deliv,tx_sum,soc_min,dtx,d_sf_mean,d_air_s,hops_mean,relay_tx,q_raw_mean"

cell() {
  local probe=$1 arm=$2 cfg=$3 al=$4 be=$5 dl=$6 n=$7 seed=$8
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
      --shadowingModel="$CHAN" --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 \n      --enablePcap=false --verboseLogs=false $extra $mf > run.log 2>&1) 2>/dev/null
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
         --allowDutyOverride=true --shadowingModel=$CHAN --enablePcap=false --verboseLogs=false" \
        sfmin=7 wire=pueyo7b hyst=1 shadow=$CHAN spacing=178 coststep=0.025 mac=csmacad \
        || { echo "ABORTA: la configuracion efectiva no es la declarada."; exit 5; }
    [ -s /tmp/cfgchk_$$/mesh_dv_effective_config.csv ] && echo "cell,$(head -1 /tmp/cfgchk_$$/mesh_dv_effective_config.csv)" > "$OUT/config_header.txt"
    rm -rf /tmp/cfgchk_$$
else
    echo "AVISO: falta assert_config.sh; la campaña corre SIN comprobar su configuracion."
fi

# arm  cfg      alpha  beta  delta      (suma)
CFGS="
A r95_05 0.95 0.05 0.00
A r85_15 0.85 0.15 0.00
A r70_30 0.70 0.30 0.00
A r50_50 0.50 0.50 0.00
A r30_70 0.30 0.70 0.00
R toaref 0 0 0
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
echo "== E16_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
