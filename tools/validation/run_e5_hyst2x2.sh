#!/bin/bash
# run_e5_hyst2x2.sh — separar la METRICA de la HISTERESIS de conmutacion.
#
# El barrido conjunto comparaba composite_score contra toa_only, pero hasta el
# commit de hoy la amortiguacion de cambio de ruta estaba condicionada al modo:
# `applyHysteresis = (m_metricMode != TOA_ONLY)`. O sea que la referencia era el
# unico modo que conmutaba con cualquier mejora bruta, mientras el resto exigia
# que la mejora sobreviviese a la cuantizacion del score. Las dos ramas diferian
# en DOS cosas a la vez, y la conclusion "la compuesta gana PDR y pierde vida
# util" podia ser de cualquiera de ellas.
#
# La pista de que el confusor mandaba: dPDR y dFND eran planos sobre toda la
# rejilla (dFND entre -1.69% y -2.23% para 12.5x de rango en beta). Un efecto
# que no responde a los pesos no lo causan los pesos.
#
# Este 2x2 cruza metrica {toa_only, compuesta} x histeresis {off, on}. Si el
# efecto lo causaba la amortiguacion, el contraste entre metricas se desploma al
# igualarla y aparece un efecto grande de la propia histeresis. Si lo causaba la
# metrica, el contraste sobrevive en las dos columnas.
#
# 300 ks para medir FND, que es la variable en disputa. Bateria U[60,100%], las
# condiciones de campaña.
#
#   bash run_e5_hyst2x2.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e5hyst
mkdir -p "$OUT/cells"
CSV="$OUT/e5_hyst2x2.csv"
HDR="hyst,cfg,nEd,seed,rc,pdr,fnd_s,t50_s,relay_tx,src_tx,tx_sum,tx_max,tx_cv,tx_gini,soc_min,dtx,d_sf_mean,d_air_s,d_air_per_tx,d_sf7,d_sf8,d_sf9,d_sf10,d_sf11,d_sf12,hops_mean,sw_primary"

cell() {
  local hy=$1 cfg=$2 n=$3 seed=$4
  local id="h${hy}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  case "$cfg" in
    toaref) mf="--allowMetricModeOverride=true --routeMetricMode=toa_only" ;;
    comp)   mf="--routeMetricMode=composite_score --compositeWToa=1.0 --compositeWHop=0.05 --compositeWEnergy=0.0" ;;
    *) return 1 ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      --routeSwitchHysteresis="$hy" $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  HY=$hy CFG=$cfg N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, math, os
e = os.environ

pdr = relay = srctx = swp = ""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr = j["pdr"]["pdr"]
    relay = j["forwarding"]["forward_tx_sent_total"]
    srctx = j["tx_attempts"]["source_tx_sent_total"]
    # cuantas veces se cambio de primaria: la variable que la histeresis toca
    # directamente, y por tanto la comprobacion de que el flag hizo algo.
    for k in ("route_switch_primary", "switch_primary", "route_switches"):
        if k in j.get("routing", {}):
            swp = j["routing"][k]
            break
except Exception:
    pass

fnd = t50 = ""
try:
    for r in csv.reader(open("mesh_dv_metrics_lifetime.csv")):
        if r and r[0] == "fnd_s":
            fnd = r[1]
        if r and r[0] == "t50_s":
            t50 = r[1]
except Exception:
    pass

tx = []
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tx.append(float(r["txMah"]))
except Exception:
    pass
ts = tmax = tcv = tgini = ""
if tx:
    tx.sort()
    s = sum(tx)
    m = s / len(tx)
    ts, tmax = "%.4f" % s, "%.4f" % tx[-1]
    if m > 0:
        sd = math.sqrt(sum((x - m) ** 2 for x in tx) / (len(tx) - 1)) if len(tx) > 1 else 0.0
        tcv = "%.6f" % (sd / m)
        tgini = "%.6f" % ((2.0 * sum((i + 1) * x for i, x in enumerate(tx)) / (len(tx) * s))
                          - (len(tx) + 1.0) / len(tx))

smin = ""
try:
    fr = sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin = "%.6f" % fr[0]
except Exception:
    pass

dsf = [0] * 6
dn = dair = dhops = dsfsum = 0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        if r.get("dst") == "65535":
            continue
        try:
            sf = int(r["sf"]); toa = float(r["toaUs"])
        except (ValueError, KeyError):
            continue
        dn += 1; dair += toa; dsfsum += sf
        if 7 <= sf <= 12:
            dsf[sf - 7] += 1
        try:
            dhops += int(r["hops"])
        except (ValueError, KeyError):
            pass
except Exception:
    pass
f4 = lambda x: "%.4f" % x
open(e["ROW"], "w").write(",".join(str(x) for x in [
    e["HY"], e["CFG"], e["N"], e["SEED"], e["RC"], pdr, fnd, t50, relay, srctx,
    ts, tmax, tcv, tgini, smin, dn,
    f4(dsfsum / dn) if dn else "", f4(dair / 1e6) if dn else "",
    f4(dair / dn) if dn else "", *dsf, f4(dhops / dn) if dn else "", swp]) + "\n")
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# El flag tiene que existir Y tiene que morder. Si el binario es viejo, las 32
# celdas saldrian identicas en las dos columnas y pareceria que la histeresis no
# hace nada, que es justo la conclusion contraria a la verdadera.
if ! "$BIN" --PrintHelp 2>&1 | grep -q "routeSwitchHysteresis"; then
    echo "ABORTA: el binario no expone --routeSwitchHysteresis. Falta recompilar."
    exit 3
fi

for hy in false true; do
  for cfg in toaref comp; do
    for seed in $(seq 1 8); do
      echo "$hy $cfg 25 $seed"
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
echo "== E5HYST_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
