#!/bin/bash
# run_e8_delta_regime.sh — delta medido en el unico regimen donde puede actuar.
#
# El termino de energia de la metrica es delta*Psi(b_j), y Psi es una rampa
# entre b_lo=0.20 y b_hi=0.50: vale 0 por encima de 0.50 y satura en 1 por
# debajo de 0.20. En las dos puntas es una CONSTANTE, asi que sumarla a todos
# los enlaces no cambia ningun orden y delta no puede hacer nada.
#
# Todo lo medido hasta ahora cae en una de las dos puntas:
#   - a 40 ks (E1) solo 4 de 25 nodos bajan de b_hi -> Psi=0 casi siempre.
#   - a 300 ks (E3, E5, E7) el SoC llega a 0 en TODAS las celdas -> Psi=1 para
#     todos, saturada.
# O sea que el termino que justifica llamar cross-layer a la metrica nunca se
# ha medido con vida. Medido 2026-07-26: a 150 ks el SoC se reparte a lo largo
# de la rampa (min 0.11, p50 0.25, max 0.52), que es la condicion buscada.
#
# Barre delta con alfa y beta en los valores de la tesis (0.60 y 0.15), mas
# toa_only como referencia pareada. Si delta no mueve nada AQUI, no lo hara en
# ningun sitio, y eso decide si el termino de energia se queda en el paper.
#
# Se registra soc_min/p10/p50 para poder afirmar que Psi estuvo viva, en vez de
# suponerlo: sin esa comprobacion un delta inerte no distingue entre "el
# termino no sirve" y "volvimos a medirlo fuera de su rampa".
#
#   bash run_e8_delta_regime.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e8
mkdir -p "$OUT/cells"
CSV="$OUT/e8_delta_regime.csv"
HDR="cfg,delta,scenario,nEd,seed,rc,pdr,fnd_s,t50_s,gen,deliv,relay_tx,src_tx,tx_sum,soc_min,soc_p10,soc_p50,soc_max,psi_activos,dtx,d_sf_mean,d_air_s,hops_mean"

cell() {
  local cfg=$1 dl=$2 scen=$3 n=$4 seed=$5
  local id="${cfg}_d${dl}_${scen}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=0.60 --compositeWHop=0.15 --compositeWEnergy=$dl"
  fi
  local scenflags
  case "$scen" in
    grid_a2a)  scenflags="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all" ;;
    rnd_conv1) scenflags="--nodePlacementMode=random --forcedDataDestinationId=0" ;;
    *) return 1 ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=150000 --rngRun="$seed" \
      --allowDutyOverride=true $scenflags \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  CFG=$cfg DL=$dl SC=$scen N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os
e = os.environ
pdr = gen = deliv = relay = srctx = ""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr = j["pdr"]["pdr"]; gen = j["pdr"]["total_data_generated"]; deliv = j["pdr"]["delivered"]
    relay = j["forwarding"]["forward_tx_sent_total"]; srctx = j["tx_attempts"]["source_tx_sent_total"]
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
# La comprobacion que hace interpretable el resultado: cuantos nodos caen
# DENTRO de la rampa [0.20, 0.50], que son los unicos sobre los que Psi puede
# discriminar. Si sale 0, delta volveria a estar medido fuera de su regimen.
smin = sp10 = sp50 = smax = act = ""
try:
    fr = sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin = "%.6f" % fr[0]; smax = "%.6f" % fr[-1]
        sp10 = "%.6f" % fr[max(0, int(len(fr)*0.10))]
        sp50 = "%.6f" % fr[len(fr)//2]
        act = sum(1 for x in fr if 0.20 < x < 0.50)
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
    e["CFG"], e["DL"], e["SC"], e["N"], e["SEED"], e["RC"], pdr, fnd, t50, gen, deliv,
    relay, srctx, f4(tx), smin, sp10, sp50, smax, act, dn,
    f4(dsfsum/dn) if dn else "", f4(dair/1e6) if dn else "", f4(dhops/dn) if dn else ""]) + "\n")
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

{
  for scen in grid_a2a rnd_conv1; do
    for n in 25 49; do
      for seed in $(seq 1 20); do
        echo "toaref 0 $scen $n $seed"
        for dl in 0.0 0.25 0.5; do echo "comp $dl $scen $n $seed"; done
      done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3 $4'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
echo "== E8_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
