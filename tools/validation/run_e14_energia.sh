#!/bin/bash
# run_e14_energia.sh — de donde sale la energia, medido en serio.
#
# El desglose que sostiene las conclusiones mas fuertes del 29-jul (balizas
# 60.3% del TX, relevo 1.4%, reposo 42.6%) salio de UNA sola corrida, con una
# semilla y N=25. Es la base de dos afirmaciones grandes -- que el ruteo solo
# puede tocar el 0.43% de la energia, y que el trabajo util esta en el plan de
# balizas y no en el encaminamiento -- asi que no puede descansar en n=1.
#
# Ademas separa CUENTA de AIRTIME, que no son lo mismo y se confunden con
# facilidad: una baliza lleva entradas de ruta y rota por SF altos, asi que
# cuesta mas por transmision. Decir "emitimos mas balizas que datos" es una
# afirmacion sobre la cuenta; decir "las balizas gastan mas" es sobre el
# airtime. Pueden discrepar y aqui se miden las dos.
#
# El rango de SF entra como FACTOR y no como constante, porque el 29-jul quedo
# claro que gobierna la estructura de la red: con SF7-12 la malla degenera en
# estrella (saltos 0.07) y con SF7-8 se mantiene multisalto (saltos 0.33). El
# reparto de energia tiene que medirse en los dos.
#
#   bash run_e14_energia.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e14_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e14_energia.csv"
HDR="rango,metrica,nEd,seed,rc,pdr,hops_mean,e_total,e_tx,e_rx,e_cad,e_idle,n_baliza,n_fuente,n_relevo,air_baliza,air_fuente,air_relevo,tx_cv,tot_cv"

cell() {
  local rango=$1 met=$2 n=$3 seed=$4
  local id="${rango}_${met}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf rf
  case "$met" in
    comp) mf="--routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 --compositeWEnergy=0" ;;
    toa)  mf="--allowMetricModeOverride=true --routeMetricMode=toa_only" ;;
    *) return 1 ;;
  esac
  case "$rango" in
    sf712) rf="" ;;
    sf78)  rf="--allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8" ;;
    *) return 1 ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=100000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --pueyoGridSpacingM=178 --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      $rf $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  R=$rango M=$met N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os, statistics
e = os.environ
pdr = ""
try:
    pdr = json.load(open("mesh_dv_summary.json"))["pdr"]["pdr"]
except Exception:
    pass
tx = rx = cad = idle = 0.0
pernodo_tx, pernodo_tot = [], []
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        a, b, c, d = (float(r["txMah"]), float(r["rxMah"]),
                      float(r["cadMah"]), float(r["idleMah"]))
        tx += a; rx += b; cad += c; idle += d
        pernodo_tx.append(a); pernodo_tot.append(a+b+c+d)
except Exception:
    pass
# CUENTA y AIRTIME por separado: una baliza cuesta mas por transmision que un
# dato, asi que "cuantas se emiten" y "cuanto gastan" no coinciden.
nb = nf = nr = 0
ab = af = ar = 0.0
dh = dn = 0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: toa = float(r["toaUs"])
        except Exception: continue
        if r.get("dst") == "65535":
            nb += 1; ab += toa
        else:
            dn += 1
            try: dh += int(r["hops"])
            except Exception: pass
            if r.get("src") == r.get("nodeId"): nf += 1; af += toa
            else: nr += 1; ar += toa
except Exception:
    pass
f4 = lambda x: "%.4f" % x
cv = lambda v: "%.4f" % (statistics.stdev(v)/statistics.mean(v)) if len(v) > 1 and statistics.mean(v) > 0 else ""
open(e["ROW"], "w").write(",".join(str(x) for x in [
    e["R"], e["M"], e["N"], e["SEED"], e["RC"], pdr,
    f4(dh/dn) if dn else "",
    f4(tx+rx+cad+idle), f4(tx), f4(rx), f4(cad), f4(idle),
    nb, nf, nr, f4(ab/1e6), f4(af/1e6), f4(ar/1e6),
    cv(pernodo_tx), cv(pernodo_tot)]) + "\n")
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
        sfmin=7 sfmax=12 wire=pueyo7b hyst=1 shadow=$CHAN spacing=178 \
        || { echo "ABORTA: la configuracion efectiva no es la declarada."; exit 5; }
    [ -s /tmp/cfgchk_$$/mesh_dv_effective_config.csv ] && echo "cell,$(head -1 /tmp/cfgchk_$$/mesh_dv_effective_config.csv)" > "$OUT/config_header.txt"
    rm -rf /tmp/cfgchk_$$
fi

for rango in sf712 sf78; do
  for met in comp toa; do
    for n in 25 49; do
      for seed in $(seq 1 "$SEEDS"); do echo "$rango $met $n $seed"; done
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
CFGCSV="${CSV%.csv}_config.csv"
{ cat "$OUT/config_header.txt" 2>/dev/null; cat "$OUT"/cells/*.cfg 2>/dev/null | sort; } > "$CFGCSV"
echo "== E14_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
