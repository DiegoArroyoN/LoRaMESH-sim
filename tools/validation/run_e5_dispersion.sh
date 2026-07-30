#!/bin/bash
# run_e5_dispersion.sh — ¿la metrica compuesta CONCENTRA la carga?
#
# El barrido conjunto (2026-07-27) dejo una paradoja: la compuesta gasta MENOS
# energia de transmision en total (-0.4%), releva un 35-44% menos, y aun asi el
# primer nodo muere ~2% ANTES que con toa_only. Menos gasto agregado y peor FND
# solo cuadra si la carga se reparte peor: el FND no lo fija la suma, lo fija el
# nodo mas cargado.
#
# El CSV agregado no permite verlo porque suma txMah sobre todos los nodos. Aqui
# se conserva `mesh_dv_metrics_energy_breakdown.csv` (nodeId,txMah,rxMah,cadMah,
# idleMah) y se reducen a estadisticos de DISPERSION antes de borrar la corrida.
#
# Dos decisiones de diseño:
#
#  1) stopSec=150000, no 300000. A 300 ks todos los nodos acaban en SoC 0 y la
#     distribucion esta saturada: no se puede medir dispersion sobre una
#     constante. A 150 ks (medido 2026-07-26: SoC min 0.11, p50 0.25, max 0.52)
#     nadie ha muerto todavia, asi que el consumo por nodo es una medida limpia
#     del reparto de carga, sin el sesgo de supervivencia que introduce un nodo
#     muerto que deja de gastar.
#
#  2) Dos brazos de bateria. Con el U[60,100%] de las campañas, el primero en
#     morir es el peor combinado de (poca carga inicial, mucho trafico), asi que
#     el reparto de carga y la loteria de la bateria se confunden. El brazo
#     `fix` arranca a todos al 100%: ahi el FND lo decide SOLO quien mas gasta,
#     que es justo la hipotesis a contrastar. El brazo `unif` repite en las
#     condiciones de la campaña para comprobar que el patron sobrevive.
#
#   bash run_e5_dispersion.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e5disp
mkdir -p "$OUT/cells"
CSV="$OUT/e5_dispersion.csv"
HDR="bat,cfg,nEd,seed,rc,pdr,relay_tx,src_tx,tx_sum,tx_max,tx_mean,tx_cv,tx_gini,tx_top3,tot_sum,tot_max,tot_cv,tot_gini,soc_min,soc_p10,nz,dtx,d_sf_mean,d_air_s,d_air_per_tx,d_sf7,d_sf8,d_sf9,d_sf10,d_sf11,d_sf12,b_sf_mean,b_air_s,hops_mean"

cell() {
  local bat=$1 cfg=$2 n=$3 seed=$4
  local id="${bat}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  case "$cfg" in
    toaref) mf="--allowMetricModeOverride=true --routeMetricMode=toa_only" ;;
    d000)   mf="--routeMetricMode=composite_score --compositeWToa=1.0 --compositeWHop=0.05 --compositeWEnergy=0.0" ;;
    d050)   mf="--routeMetricMode=composite_score --compositeWToa=1.0 --compositeWHop=0.05 --compositeWEnergy=0.5" ;;
    *) return 1 ;;
  esac
  local bf
  case "$bat" in
    fix)  bf="--socInitMin=1.00 --socInitMax=1.00" ;;   # aisla el reparto de carga
    unif) bf="--socInitMin=0.60 --socInitMax=1.00" ;;   # condiciones de campaña
    *) return 1 ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=150000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      $bf $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  BAT=$bat CFG=$cfg N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, math, os
e = os.environ

def stats(v):
    """suma, max, media, CV, Gini y cuota de los 3 mayores."""
    v = sorted(x for x in v if x == x)
    n = len(v)
    if n == 0:
        return ("",) * 6
    s = sum(v)
    m = s / n
    if m <= 0:
        return ("%.4f" % s, "%.4f" % v[-1], "%.4f" % m, "", "", "")
    sd = math.sqrt(sum((x - m) ** 2 for x in v) / (n - 1)) if n > 1 else 0.0
    # Gini sobre la muestra ordenada
    gini = (2.0 * sum((i + 1) * x for i, x in enumerate(v)) / (n * s)) - (n + 1.0) / n
    top3 = sum(v[-3:]) / s
    return ("%.4f" % s, "%.4f" % v[-1], "%.4f" % m,
            "%.4f" % (sd / m), "%.4f" % gini, "%.4f" % top3)

pdr = relay = srctx = ""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr = j["pdr"]["pdr"]
    relay = j["forwarding"]["forward_tx_sent_total"]
    srctx = j["tx_attempts"]["source_tx_sent_total"]
except Exception:
    pass

tx, tot = [], []
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        t = float(r["txMah"])
        tx.append(t)
        tot.append(t + float(r["rxMah"]) + float(r["cadMah"]) + float(r["idleMah"]))
except Exception:
    pass
ts, tmax, tmean, tcv, tgini, ttop3 = stats(tx)
os_, omax, _, ocv, ogini, _ = stats(tot)

smin = sp10 = ""
try:
    fr = sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin = "%.6f" % fr[0]
        sp10 = "%.6f" % fr[max(0, int(len(fr) * 0.10))]
except Exception:
    pass

# Reparto no es lo mismo que coste. Si la compuesta hace MENOS transmisiones y
# aun asi gasta mas, es que cada transmision sale mas cara, o sea SF mas alto:
# rutas de menos saltos pero mas largos. El histograma de SF de los datos lo
# separa del reparto. Las balizas van aparte porque su SF lo rota una PMF
# geometrica ajena al ruteo, asi que sirven de control: deben coincidir entre
# brazos.
dsf = [0] * 6
dn = dair = dhops = 0
bn = bair = 0
bsfsum = dsfsum = 0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try:
            sf = int(r["sf"])
            toa = float(r["toaUs"])
        except (ValueError, KeyError):
            continue
        if r.get("dst") == "65535":
            bn += 1
            bair += toa
            bsfsum += sf
        else:
            dn += 1
            dair += toa
            dsfsum += sf
            if 7 <= sf <= 12:
                dsf[sf - 7] += 1
            try:
                dhops += int(r["hops"])
            except (ValueError, KeyError):
                pass
except Exception:
    pass
f6 = lambda x: "%.4f" % x
dstats = ([dn, f6(dsfsum / dn) if dn else "", f6(dair / 1e6) if dn else "",
           f6(dair / dn) if dn else "", *dsf, f6(bsfsum / bn) if bn else "",
           f6(bair / 1e6) if bn else "", f6(dhops / dn) if dn else ""])

open(e["ROW"], "w").write(",".join(str(x) for x in [
    e["BAT"], e["CFG"], e["N"], e["SEED"], e["RC"], pdr, relay, srctx,
    ts, tmax, tmean, tcv, tgini, ttop3, os_, omax, ocv, ogini,
    smin, sp10, len(tx), *dstats]) + "\n")
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# Comprobar que el binario acepta socInitMin antes de encolar 48 corridas: un
# flag no reconocido aborta cada celda y se descubre con la campaña ya perdida.
if ! "$BIN" --PrintHelp 2>&1 | grep -q "socInitMin"; then
    echo "ABORTA: el binario no expone --socInitMin. Binario desactualizado?"
    exit 3
fi

for bat in fix unif; do
  for cfg in toaref d000 d050; do
    for seed in $(seq 1 8); do
      echo "$bat $cfg 25 $seed"
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
echo "== E5DISP_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
