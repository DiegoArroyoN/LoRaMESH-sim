#!/bin/bash
# run_e5_beta_low.sh — el intercambio saltos-por-SF, ¿lo manda beta o el paso?
#
# El 2x2 dejo claro que la perdida de vida util (-1.73% de FND) es de la
# metrica y no de la histeresis, y que el mecanismo es un intercambio: la
# compuesta cambia cadenas de saltos SF7 por saltos largos SF8 (-33% saltos,
# SF8 del 1.7% al 14.6%, +1.19% de airtime). Falta saber que palanca lo
# gobierna, porque de eso depende si existe un punto de operacion que no pague
# vida util.
#
# La rejilla original barrio beta en {0.02, 0.05, 0.15} y no vio ningun efecto
# marginal. La aritmetica dice por que: con normalizacion global el termino de
# ToA vale ~0.03 en SF7 y ~0.055 en SF8, y el cuantizador de COST255 usa paso
# 0.025. Con beta=0.02 un salto SF7 cuesta q=2 y uno SF8 q=3, asi que dos
# saltos SF7 (4) pierden contra un SF8 (3). Con beta=0.15 pierden 14 contra 8.
# En TODA la rejilla gana el salto largo, y por eso beta parecia inerte: se
# barrio entera por encima del punto de cambio.
#
# Dos brazos, una variable cada uno:
#   A) beta por debajo de la rejilla: {0, 0.005, 0.01, 0.02}, paso por defecto.
#      beta=0 con alfa=1 es ToA reescalado, o sea la costura contra toa_only:
#      deberia reproducirlo salvo por donde caen las fronteras del cuantizador.
#   B) paso mas fino a beta fijo: {0.010, 0.005}. Si el culpable es la
#      granularidad y no beta, aqui reaparece la discriminacion por ToA.
#      Riesgo a vigilar: el paso fino satura COST255 en caminos largos (un
#      camino de coste 0.8 con paso 0.0025 daria q=320 y se recorta a 255), lo
#      que degeneraria el ruteo en empates. Se controla mirando si el reparto
#      de SF se vuelve erratico.
#
#   bash run_e5_beta_low.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e5beta
mkdir -p "$OUT/cells"
CSV="$OUT/e5_beta_low.csv"
HDR="cfg,beta,step,nEd,seed,rc,pdr,fnd_s,t50_s,relay_tx,src_tx,tx_sum,soc_min,dtx,d_sf_mean,d_air_s,d_sf7,d_sf8,d_sf9,d_sf10,d_sf11,d_sf12,hops_mean"

cell() {
  local cfg=$1 bt=$2 st=$3 n=$4 seed=$5
  local id="${cfg}_b${bt}_s${st}_n${n}_sd${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=1.0 --compositeWHop=$bt \
          --compositeWEnergy=0.0 --compositeCostStep=$st"
  fi

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      --routeSwitchHysteresis=true $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  CFG=$cfg BT=$bt ST=$st N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os
e = os.environ
pdr = relay = srctx = ""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr = j["pdr"]["pdr"]; relay = j["forwarding"]["forward_tx_sent_total"]
    srctx = j["tx_attempts"]["source_tx_sent_total"]
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
dsf = [0]*6; dn = dair = dhops = dsfsum = 0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        if r.get("dst") == "65535": continue
        try: sf = int(r["sf"]); toa = float(r["toaUs"])
        except (ValueError, KeyError): continue
        dn += 1; dair += toa; dsfsum += sf
        if 7 <= sf <= 12: dsf[sf-7] += 1
        try: dhops += int(r["hops"])
        except (ValueError, KeyError): pass
except Exception:
    pass
f4 = lambda x: "%.4f" % x
open(e["ROW"], "w").write(",".join(str(x) for x in [
    e["CFG"], e["BT"], e["ST"], e["N"], e["SEED"], e["RC"], pdr, fnd, t50, relay, srctx,
    f4(tx), smin, dn, f4(dsfsum/dn) if dn else "", f4(dair/1e6) if dn else "",
    *dsf, f4(dhops/dn) if dn else ""]) + "\n")
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"
if ! "$BIN" --PrintHelp 2>&1 | grep -q "compositeCostStep"; then
    echo "ABORTA: el binario no expone --compositeCostStep."; exit 3
fi

{
  for seed in $(seq 1 8); do echo "toaref 0 0.025 25 $seed"; done
  for bt in 0.0 0.005 0.01 0.02; do
    for seed in $(seq 1 8); do echo "comp $bt 0.025 25 $seed"; done
  done
  for st in 0.010 0.005; do
    for seed in $(seq 1 8); do echo "comp 0.05 $st 25 $seed"; done
  done
} | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3 $4'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
echo "== E5BETA_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
