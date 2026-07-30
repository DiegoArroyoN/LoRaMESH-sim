#!/bin/bash
# run_e15_delta_topologia.sh — delta en las TRES topologias del DoE.
#
# Hueco de alcance que señalo Diego el 29-jul. La conclusion "delta = 0" se
# apoyaba en:
#
#   rejilla all-to-all   E12 y E13, Psi viva en 24.9 de 25 nodos   SOLIDO
#   convergecast         E13 brazo conv, Psi viva en 8.2 de N      DEBIL
#   multi-sink (4)       nunca medido                              AUSENTE
#
# O sea que estaba establecida en una sola topologia. Y el 8.2 del convergecast
# no era culpa de la topologia sino de la configuracion que le puse a ese brazo
# (area de 3 km con SF7-9, que drena mucho mas rapido). Con la definicion
# estandar del escenario y 100 ks, la calibracion por topologia del 29-jul mide
# Psi viva en 19/25, 17/25 y 17/25 respectivamente: las tres son medibles.
#
# Por que la topologia podria cambiar la respuesta: delta reparte carga de
# RELEVO, y cada topologia concentra el relevo de forma distinta. En all-to-all
# el trafico va de todos a todos y la carga sale casi uniforme, asi que no hay
# nada que repartir. En convergecast todo se embuda hacia un sumidero y los
# nodos que lo rodean relegan a la fuerza. Con cuatro sumideros la concentracion
# se reparte entre cuatro vecindarios. Son tres geometrias de carga distintas y
# no hay razon para que delta rinda igual en las tres.
#
# PREDICCION registrada: si delta sirve en algun sitio, es donde la carga de
# relevo esta concentrada por la geometria, o sea convergecast primero y
# multi-sink despues. Si tampoco ahi, la conclusion delta=0 pasa a estar
# establecida en las tres topologias del DoE y no solo en una.
#
# Suma constante 1 (0.85/0.15/0 frente a 0.60/0.15/0.25): la comparacion es
# "delta frente a darle su cuota al airtime", que es la pregunta util.
#
#   bash run_e15_delta_topologia.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e15_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e15_delta_topologia.csv"
HDR="topologia,cfg,alpha,beta,delta,nEd,seed,rc,pdr,soc_min,soc_p10,psi_dentro,tx_sum,tx_max,relay_tx,src_tx,hops_mean,air_baliza,air_fuente,air_relevo,frac_relevo,relevo_cv,relevo_max"

cell() {
  local topo=$1 cfg=$2 al=$3 be=$4 dl=$5 n=$6 seed=$7
  local id="${topo}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf tf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=$al --compositeWHop=$be --compositeWEnergy=$dl"
  fi
  case "$topo" in
    a2a)    tf="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178" ;;
    conv1)  tf="--nodePlacementMode=random --forcedDataDestinationId=0" ;;
    msink4) tf="--nodePlacementMode=random --numSinks=4" ;;
    *) return 1 ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=100000 --rngRun="$seed" \
      --allowDutyOverride=true $tf \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  T=$topo C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os, statistics
e = os.environ
def g(j, *ks, d=""):
    for k in ks:
        if isinstance(j, dict) and k in j: j = j[k]
        else: return d
    return j
pdr = relay = srctx = ""
try:
    j = json.load(open("mesh_dv_summary.json"))
    pdr = g(j,"pdr","pdr"); relay = g(j,"forwarding","forward_tx_sent_total")
    srctx = g(j,"tx_attempts","source_tx_sent_total")
except Exception:
    pass
tx = []
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tx.append(float(r["txMah"]))
except Exception:
    pass
# psi_dentro se registra en CADA fila: es lo que hace interpretable el
# resultado. Sin ella, un delta inerte no distingue entre "el termino no sirve
# en esta topologia" y "en esta topologia Psi quedo fuera de su rampa", que es
# exactamente lo que invalido el brazo conv de E13.
smin = sp10 = dentro = ""
try:
    fr = sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin = "%.6f" % fr[0]
        sp10 = "%.6f" % fr[max(0, int(len(fr)*0.10))]
        dentro = sum(1 for x in fr if 0.20 < x < 0.50)
except Exception:
    pass
air = {}
dhops = dn = 0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try:
            nid = int(r["nodeId"]); toa = float(r["toaUs"])
        except Exception:
            continue
        a = air.setdefault(nid, {"b":0.0,"f":0.0,"r":0.0})
        if r.get("dst") == "65535":
            a["b"] += toa
        else:
            dn += 1
            try: dhops += int(r["hops"])
            except Exception: pass
            if r.get("src") == r.get("nodeId"): a["f"] += toa
            else: a["r"] += toa
except Exception:
    pass
T = {k: sum(a[k] for a in air.values()) for k in ("b","f","r")} if air else {"b":0,"f":0,"r":0}
ta = sum(T.values()) or 1.0
rel = [a["r"] for a in air.values()]
rcv = rmax = ""
if len(rel) > 1:
    mu = statistics.mean(rel)
    if mu > 0:
        rcv = "%.4f" % (statistics.stdev(rel)/mu)
        rmax = "%.4f" % (max(rel)/mu)
f4 = lambda x: "%.4f" % x
open(e["ROW"], "w").write(",".join(str(x) for x in [
    e["T"], e["C"], e["AL"], e["BE"], e["DL"], e["N"], e["SEED"], e["RC"],
    pdr, smin, sp10, dentro,
    f4(sum(tx)) if tx else "", f4(max(tx)) if tx else "", relay, srctx,
    f4(dhops/dn) if dn else "",
    f4(T["b"]/1e6), f4(T["f"]/1e6), f4(T["r"]/1e6), f4(T["r"]/ta), rcv, rmax]) + "\n")
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
        sfmin=7 sfmax=12 wire=pueyo7b hyst=1 shadow=$CHAN mac=csmacad socmin=0.60 socmax=1.00 \
        || { echo "ABORTA: la configuracion efectiva no es la declarada."; exit 5; }
    [ -s /tmp/cfgchk_$$/mesh_dv_effective_config.csv ] && echo "cell,$(head -1 /tmp/cfgchk_$$/mesh_dv_effective_config.csv)" > "$OUT/config_header.txt"
    rm -rf /tmp/cfgchk_$$
fi

# PRECONDICION POR TOPOLOGIA. E13 uso una sola duracion para las tres y en
# convergecast Psi quedo viva en 8.2 de N, o sea que aquella medida no decia
# nada. Aqui se exige que CADA topologia tenga Psi discriminando antes de
# encolar una sola celda.
echo "== Psi viva en las tres topologias a 100 ks? =="
pchk=/tmp/psitopo_$$; rm -rf $pchk; mkdir -p $pchk
for topo in a2a conv1 msink4; do
  case "$topo" in
    a2a)    tf="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178" ;;
    conv1)  tf="--nodePlacementMode=random --forcedDataDestinationId=0" ;;
    msink4) tf="--nodePlacementMode=random --numSinks=4" ;;
  esac
  d=$pchk/$topo; mkdir -p "$d"; cd "$d" || exit 2
  # shellcheck disable=SC2086
  "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=100000 --rngRun=1 \
      --allowDutyOverride=true $tf --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      --routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 \
      --compositeWEnergy=0 > run.log 2>&1
done
ver=$(cd $pchk && python3 -c "
import csv, os
ok = True
for t in ('a2a','conv1','msink4'):
    try:
        fr = sorted(float(r['energyFrac']) for r in csv.DictReader(open(os.path.join(t,'mesh_dv_metrics_energy.csv'))))
    except Exception:
        print('  %-8s sin datos' % t); ok = False; continue
    d = sum(1 for x in fr if 0.20 < x < 0.50)
    print('  %-8s min=%.4f p50=%.4f max=%.4f   dentro de la rampa: %d/%d' %
          (t, fr[0], fr[len(fr)//2], fr[-1], d, len(fr)))
    if d < len(fr)/3: ok = False
print('TODAS VIVAS' if ok else 'ALGUNA MUERTA')
" 2>/dev/null)
cd "$HOME" || exit 2
rm -rf $pchk
echo "$ver"
if ! echo "$ver" | grep -q "^TODAS VIVAS$"; then
    echo "ABORTA: en alguna topologia Psi no discrimina, asi que delta no puede"
    echo "  morder alli y la campaña produciria una tabla que no dice nada."
    exit 6
fi

CFGS="
d00    0.85 0.15 0.00
d25    0.60 0.15 0.25
toaref 0    0    0
"

{
  for topo in a2a conv1 msink4; do
    echo "$CFGS" | while read -r cfg al be dl; do
      [ -z "$cfg" ] && continue
      for n in 25 49; do
        for seed in $(seq 1 "$SEEDS"); do echo "$topo $cfg $al $be $dl $n $seed"; done
      done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3 $4 $5 $6'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
CFGCSV="${CSV%.csv}_config.csv"
{ cat "$OUT/config_header.txt" 2>/dev/null; cat "$OUT"/cells/*.cfg 2>/dev/null | sort; } > "$CFGCSV"
echo "== E15_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
