#!/bin/bash
# run_e13_delta_regimen.sh — delta donde el relevo SI pesa.
#
# El forense del 29-jul explica por que delta no movia el FND en rejilla
# all-to-all a 178 m, y la explicacion no es que delta falle:
#
#   reposo 42.6% | TX 31.3% | recepcion 21.3% | CAD 4.9%
#   y dentro del TX:  balizas 60.3%  datos propios 38.3%  RELEVO 1.4%
#
# El ruteo solo puede mover el relevo, que es el 0.43% de la energia. Delta SI
# lo reparte -- baja su CV un 12% y su maximo un 8% -- pero reparte algo que
# pesa nada. La causa raiz es que hops_mean ronda 0.07: a 178 m con SF7-12 casi
# cualquier nodo alcanza a cualquier otro directamente y la malla apenas relega.
#
# La conclusion correcta no es "delta no sirve" sino "delta esta fuera de su
# regimen". Esta campaña lo pone dentro:
#
# El primer intento aborto por su propia sonda de contraste: grid178 relegaba el
# 1.26% del TX, grid320 el 1.79% y el convergecast el 2.24%, o sea que NINGUNO
# era rico en relevo. La calibracion del 29-jul da la palanca correcta:
#
#   rejilla 178m SF7-12   relevo  1.26%   saltos 0.072
#   rejilla 900m SF7-12   relevo  4.38%   saltos 0.136   (pero el PDR se hunde a 0.009)
#   rejilla 178m SF7-9    relevo  5.24%   saltos 0.179
#   rejilla 178m SF7-8    relevo 10.08%   saltos 0.334
#   conv1 3km SF7-9       relevo  6.89%   saltos 0.163
#
# Lo que fuerza el multisalto NO es separar los nodos -- eso solo rompe la red --
# sino CERRAR el rango de SF. Con SF7-12 el alcance llega a 1058 m, casi
# cualquier nodo alcanza a cualquier otro de un salto y la malla degenera en
# estrella. Cerrando a SF7-8 el alcance baja a 350 m y el relevo se multiplica
# por ocho.
#
#   ref     rejilla 178 m SF7-12   relevo  1.3%   referencia pobre en relevo
#   rico    rejilla 178 m SF7-8    relevo 10.1%   ocho veces mas margen para delta
#   conv    conv1 3 km SF7-9       relevo  6.9%   otra topologia, mismo regimen
#
# PREDICCION, registrada antes de correr: delta ayudara al FND alli donde la
# fraccion de relevo sea alta, y no donde sea baja. Si NO ayuda ni siquiera con
# el relevo dominando, entonces el termino esta muerto y hay que quitarlo de la
# metrica en vez de defenderlo por regimen.
#
# Cada fila registra la fraccion de relevo sobre el TX, para que la
# interpretacion no dependa de recordar este comentario: la correlacion entre
# esa fraccion y el efecto de delta es el resultado.
#
# Suma constante 1 (0.85/0.15/0 frente a 0.60/0.15/0.25), asi que la comparacion
# es "delta frente a darle su cuota al airtime".
#
#   bash run_e13_delta_regimen.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e13_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e13_delta_regimen.csv"
HDR="escenario,cfg,alpha,beta,delta,nEd,seed,rc,pdr,fnd_s,t50_s,soc_min,psi_dentro,tx_sum,tx_max,relay_tx,src_tx,hops_mean,air_baliza,air_fuente,air_relevo,frac_relevo,relevo_cv,relevo_max"

cell() {
  local esc=$1 cfg=$2 al=$3 be=$4 dl=$5 n=$6 seed=$7
  local id="${esc}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf sf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=$al --compositeWHop=$be --compositeWEnergy=$dl"
  fi
  case "$esc" in
    ref)  sf="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178" ;;
    rico) sf="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178 --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8" ;;
    conv) sf="--nodePlacementMode=random --forcedDataDestinationId=0 --areaWidth=3000 --areaHeight=3000 --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=9" ;;
    *) return 1 ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=100000 --rngRun="$seed" \
      --allowDutyOverride=true $sf \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  E=$esc C=$cfg AL=$al BE=$be DL=$dl N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
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
smin = dentro = ""
try:
    fr = sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr:
        smin = "%.6f" % fr[0]
        dentro = sum(1 for x in fr if 0.20 < x < 0.50)
except Exception:
    pass
# El reparto del airtime por nodo separando balizas, datos propios y relevo. La
# fraccion de relevo es LA variable de esta campaña: mide cuanta energia puede
# tocar el ruteo, y por tanto cuanto margen tiene delta.
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
    e["E"], e["C"], e["AL"], e["BE"], e["DL"], e["N"], e["SEED"], e["RC"],
    pdr, fnd, t50, smin, dentro,
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
        sfmin=7 wire=pueyo7b hyst=1 shadow=$CHAN socmin=0.60 socmax=1.00 \
        || { echo "ABORTA: la configuracion efectiva no es la declarada."; exit 5; }
    [ -s /tmp/cfgchk_$$/mesh_dv_effective_config.csv ] && echo "cell,$(head -1 /tmp/cfgchk_$$/mesh_dv_effective_config.csv)" > "$OUT/config_header.txt"
    rm -rf /tmp/cfgchk_$$
fi

# El escenario tiene que producir de verdad mas relevo, o la campaña no mide lo
# que dice medir. Se comprueba antes de encolar: si grid320 y conv1 relegan lo
# mismo que grid178, no hay contraste y el resultado seria vacio.
echo "== comprobando que los escenarios difieren en fraccion de relevo =="
pr=/tmp/relchk_$$; rm -rf $pr; mkdir -p $pr
for esc in ref rico conv; do
  case "$esc" in
    ref)  sf="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178" ;;
    rico) sf="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178 --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8" ;;
    conv) sf="--nodePlacementMode=random --forcedDataDestinationId=0 --areaWidth=3000 --areaHeight=3000 --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=9" ;;
  esac
  d=$pr/$esc; mkdir -p "$d"; cd "$d" || exit 2
  # shellcheck disable=SC2086
  "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=20000 --rngRun=1 \
      --allowDutyOverride=true $sf --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false > run.log 2>&1
done
res=$(cd $pr && python3 -c "
import csv, os
def frac(esc):
    b=f=r=0.0
    try:
        for x in csv.DictReader(open(os.path.join(esc,'mesh_dv_metrics_tx.csv'))):
            try: toa=float(x['toaUs'])
            except Exception: continue
            if x.get('dst')=='65535': b+=toa
            elif x.get('src')==x.get('nodeId'): f+=toa
            else: r+=toa
    except Exception: return None
    t=b+f+r
    return r/t if t else None
v={e:frac(e) for e in ('ref','rico','conv')}
for k,x in v.items():
    print('  %-9s relevo = %s del TX' % (k, ('%.2f%%'%(100*x)) if x is not None else 'sin datos'))
ok = all(x is not None for x in v.values()) and max(v.values()) > 3*max(1e-9, v['ref'])
print('CONTRASTE' if ok else 'SIN CONTRASTE')
" 2>/dev/null)
cd "$HOME" || exit 2
rm -rf $pr
echo "$res"
if ! echo "$res" | grep -q "^CONTRASTE$"; then
    echo "ABORTA: los escenarios no difieren lo suficiente en fraccion de relevo."
    echo "  Sin contraste, la campaña no puede decir si delta depende del regimen."
    exit 6
fi

CFGS="
d00    0.85 0.15 0.00
d25    0.60 0.15 0.25
toaref 0    0    0
"

{
  for esc in ref rico conv; do
    echo "$CFGS" | while read -r cfg al be dl; do
      [ -z "$cfg" ] && continue
      for n in 25 49; do
        for seed in $(seq 1 "$SEEDS"); do echo "$esc $cfg $al $be $dl $n $seed"; done
      done
    done
  done
} | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3 $4 $5 $6'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
CFGCSV="${CSV%.csv}_config.csv"
{ cat "$OUT/config_header.txt" 2>/dev/null; cat "$OUT"/cells/*.cfg 2>/dev/null | sort; } > "$CFGCSV"
echo "== E13_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
