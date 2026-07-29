#!/bin/bash
# run_e7_spacing.sh — los cuatro espaciados de Pueyo-Centelles: ¿es un efecto
# de umbral o algo general?
#
# Sus espaciados no son arbitrarios y el paper lo dice: son pares que rodean un
# umbral de SF.
#
#   *"A spacing of 177m allows nodes using the shortest-range SF7 to
#   communicate with their adjacent nodes in horizontal, vertical, and
#   diagonal. When the spacing is increased by one unit, diagonal communication
#   with adjacent nodes is no longer possible with SF7, only vertically and
#   horizontally. Similarly, a 246m spacing or 247m allows communication
#   between adjacent nodes in diagonal with SF8 or requires using the slower
#   SF9."*
#
# A 177 m la diagonal mide 177*raiz(2) = 250.3 m, justo el alcance de SF7 (en
# nuestro simulador 251.0 m, verificado 2026-07-27), asi que cada nodo alcanza a
# sus 8 vecinos y NO hay dilema. A 178 m la diagonal se cae por 0.7 m y la red
# tiene que elegir entre un salto diagonal SF8 (107.01 ms) o dos saltos SF7
# (117.25 ms) por los vecinos H/V. Esa es exactamente la eleccion que separa a
# la compuesta de toa_only en todo lo medido hasta ahora.
#
# PREDICCION, para que esto sea una prueba y no una pesca:
#
#   - a 177 y 246 m (diagonal alcanzable) las dos metricas deberian CONVERGER:
#     sin dilema que resolver, dPDR y dFND se acercan a cero.
#   - a 178 y 247 m (diagonal recien caida) deberian DIVERGIR, y el reparto de
#     SF de la compuesta debe cargarse hacia el SF superior de cada par.
#
# Si a 177 m tambien divergen, la explicacion del mecanismo esta mal y hay que
# volver a mirarlo: seria señal de que el intercambio saltos-por-SF ocurre por
# otro motivo, no por el umbral diagonal.
#
# Matiz que hay que tener presente al leer el resultado: con sombreado
# sigma=3.57 dB el margen de 0.7 m son 0.025 dB, o sea nada. El umbral duro del
# diseño es blando aqui, asi que se espera un gradiente y no un escalon.
#
# 300 ks porque hacen falta PDR y FND a la vez. El area crece con el espaciado
# ((lado-1)*espaciado), igual que en su Tabla 7.
#
#   bash run_e7_spacing.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
# Canal y semillas por entorno: el mismo runner sirve para el brazo
# determinista (CHAN=none, la condicion de Pueyo-Centelles) y para el de
# sombreado por enlace (CHAN=static). La salida va a directorios distintos
# para que un brazo no se coma las celdas del otro por el cache de .row.
CHAN=${CHAN:-static}
SEEDS=${SEEDS:-20}
# Exportar, no solo asignar: cell() corre en un bash nuevo lanzado por
# xargs, que hereda el entorno y no las variables de shell. Sin esto el
# valor por defecto no llega y --shadowingModel viaja vacio, que aborta
# cada corrida. Funcionaba al invocarlo como "CHAN=x bash runner" porque
# entonces la variable ya venia del entorno.
export CHAN SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e7_$CHAN
mkdir -p "$OUT/cells"
CSV="$OUT/e7_spacing.csv"
HDR="spacing,cfg,nEd,seed,rc,pdr,fnd_s,t50_s,relay_tx,src_tx,tx_sum,soc_min,dtx,d_sf_mean,d_air_s,d_sf7,d_sf8,d_sf9,d_sf10,d_sf11,d_sf12,hops_mean"

cell() {
  local sp=$1 cfg=$2 n=$3 seed=$4
  local id="sp${sp}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  case "$cfg" in
    toaref) mf="--allowMetricModeOverride=true --routeMetricMode=toa_only" ;;
    comp)   mf="--routeMetricMode=composite_score" ;;   # pesos por defecto de la tesis
    *) return 1 ;;
  esac

  local d="$OUT/work/$id"; mkdir -p "$d"; cd "$d" || return 1
  ("$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --pueyoGridSpacingM="$sp" \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel="$CHAN" --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true \
      $mf > run.log 2>&1) 2>/dev/null
  local rc=$?

  SP=$sp CFG=$cfg N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
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
    e["SP"], e["CFG"], e["N"], e["SEED"], e["RC"], pdr, fnd, t50, relay, srctx,
    f4(tx), smin, dn, f4(dsfsum/dn) if dn else "", f4(dair/1e6) if dn else "",
    *dsf, f4(dhops/dn) if dn else ""]) + "\n")
PY
  cd "$OUT" && rm -rf "$d"
}
export -f cell
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# El espaciado tiene que MORDER, no solo estar aceptado: si el perfil lo pisara,
# las cuatro columnas saldrian identicas y el barrido concluiria justo lo
# contrario de la verdad.
probe=/tmp/e7probe; rm -rf $probe; mkdir -p $probe; cd $probe || exit 2
for sp in 177 247; do
  "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=8000 --rngRun=1 \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --pueyoGridSpacingM=$sp --allowDutyOverride=true \
      --enablePcap=false --verboseLogs=false > sp$sp.log 2>&1
  mv mesh_dv_metrics_tx.csv tx_$sp.csv 2>/dev/null
done
ok=$(python3 -c "
import csv,collections
def h(p):
    c=collections.Counter()
    try:
        for r in csv.DictReader(open(p)):
            if r.get('dst')!='65535':
                try: c[int(r['sf'])]+=1
                except: pass
    except Exception: return None
    return dict(sorted(c.items()))
a,b=h('tx_177.csv'),h('tx_247.csv')
print('177:',a,' 247:',b)
print('DISTINTO' if a!=b and a and b else 'IGUAL')
" 2>/dev/null)
echo "== sonda de espaciado =="
echo "$ok"
cd "$HOME" || exit 2
rm -rf $probe
if echo "$ok" | grep -q "IGUAL"; then
    echo "ABORTA: el espaciado no cambia el reparto de SF. El flag no muerde."
    exit 4
fi

for sp in 177 178 246 247; do
  for cfg in toaref comp; do
    for n in 25 49; do
      for seed in $(seq 1 "$SEEDS"); do echo "$sp $cfg $n $seed"; done
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'cell $0 $1 $2 $3'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
echo "== E7_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
