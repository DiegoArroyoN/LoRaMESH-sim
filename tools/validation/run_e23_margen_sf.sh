#!/bin/bash
# run_e23_margen_sf.sh — cuanta reserva necesita de verdad la seleccion de SF.
#
# HALLAZGO QUE LO MOTIVA (2026-08-03). Replicando la fig. 11c de Pueyo-Centelles
# (rejilla 248 m) obtuvimos PDR 0.0000 en las 240 celdas, con rc=0 y sin un solo
# error. Un barrido fino de separacion lo explico:
#
#   sep(m)  Prx enlace H/V   margen sobre -124 dBm   SF elegido   PDR
#    177       -120.84            +3.16 dB              SF7      0.547
#    200       -121.95            +2.05 dB              SF7      0.729
#    240       -123.60            +0.40 dB              SF7      0.000  <--
#    245       -123.78            +0.22 dB              SF7      0.000  <--
#    248       -123.89            +0.11 dB              SF7      0.000  <--
#    250       -123.96            +0.04 dB              SF7      0.000  <--
#    252       -124.04            -0.04 dB              SF8      0.689
#
# El selector NO tiene un bug de logica: ComputeMinSfBySensitivity elige el SF
# mas bajo cuya sensibilidad se cumple, que es correcto. El problema es el
# PARAMETRO: sfLinkMarginDb = 0 significa "usa el SF mas rapido que TEORICAMENTE
# alcanza", sin reserva ninguna. En 240-250 m el enlace queda con 0.04-0.40 dB de
# margen; el selector lo declara viable y no sobrevive a nada -- ni a una
# colision parcial, ni al ruido, ni a un interferente lejano. La red entrega
# cero en silencio.
#
# Y conecta con la tension que gobierna el paper: el termino de ToA empuja hacia
# SF bajos porque son mas rapidos, y con margen cero el selector le da la razon
# hasta el ultimo decibelio. La metrica y el selector se refuerzan en la
# direccion fragil.
#
# QUE MIDE ESTA CAMPAÑA:
#   1) si un margen > 0 elimina la banda muerta, y cuanto hace falta
#   2) que CUESTA el margen en separaciones seguras (177 m): mas margen obliga a
#      SF mas altos, mas airtime, menos entrega bajo carga
#   3) si la banda muerta es especifica de SF7-8 (con rango completo el selector
#      tiene a donde escapar) -- por eso el rango entra como factor
#
# ALCANCE DE LA CONTAMINACION, para que quede escrito: todo lo medido a 177-178 m
# tiene +3.1 dB de margen y esta LIMPIO (E14-E20, E22, y las celdas de 177 m y
# aleatorias de E21). La banda fragil es 240-251 m y solo muerde con SF7-8.
#
# PREDICCION registrada antes de mirar: con margen >= 1 dB la banda muerta
# desaparece porque el selector sube a SF8 antes de llegar al borde. El coste a
# 177 m sera pequeño hasta 2 dB (el enlace tiene 3.16 dB de reserva) y se
# disparara en 3 dB, cuando el umbral de SF7 baje de 251 m a 180 m y TODOS los
# enlaces de la rejilla pasen a SF8.
#
#   bash run_e23_margen_sf.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-14}
SEEDS=${SEEDS:-10}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e23_margen
mkdir -p "$OUT/cells"
CSV="$OUT/e23_margen_sf.csv"
HDR="margen_db,rango,spacing,nEd,seed,rc,generados,entregados,pdr,hops,sf7,sf8,sf9,sf10,sf11,sf12,sf_medio,air_dato,phy_drops"

celda() {
  local mg=$1 rango=$2 sp=$3 n=$4 seed=$5
  local id="m${mg}_${rango}_${sp}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local rf
  [ "$rango" = "sf78" ] && rf="--sfMin=7 --sfMax=8" || rf="--sfMin=7 --sfMax=12"

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 3600 "$BIN" --profile=pueyo2024_paper_like --nEd="$n" --stopSec=250000 --rngRun="$seed" \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM="$sp" \
      --allowPaperLikeSfRangeVariant=true $rf \
      --allowSfMarginOverride=true --sfLinkMarginDb="$mg" \
      --shadowingModel=none --enablePcap=false --verboseLogs=false --trafficLoad=low \
      > run.log 2>&1
  local rc=$?

  MG=$mg R=$rango SP=$sp N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os, collections
e=os.environ
gen=deliv=pdr=hops=""
try:
    j=json.load(open("mesh_dv_summary.json")); p=j["pdr"]
    gen=p["total_data_generated"]; deliv=p["delivered"]; pdr=p["pdr"]
    hops=j.get("forwarding",{}).get("avg_hops_delivered","")
except Exception: pass
sfd=collections.Counter(); ad=0.0; ssum=cnt=0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        if r.get("dst")=="65535": continue
        try: sf=int(r["sf"]); t=float(r["toaUs"])
        except Exception: continue
        sfd[sf]+=1; ad+=t; ssum+=sf; cnt+=1
except Exception: pass
drops=0
try:
    with open("run.log", errors="replace") as f:
        for l in f:
            if "PHY_INTERFERENCE_DROP" in l: drops+=1
except Exception: pass
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["MG"],e["R"],e["SP"],e["N"],e["SEED"],e["RC"],gen,deliv,pdr,hops,
    *[sfd.get(k,0) for k in range(7,13)],
    f4(ssum/cnt) if cnt else "", f4(ad/1e6), drops])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# PRECONDICION: el margen tiene que MORDER. La puerta --allowSfMarginOverride
# es nueva; si no funcionara, las cinco columnas saldrian identicas y la campaña
# concluiria que el margen da igual, que es lo contrario de la verdad.
echo "== ¿muerde el margen? =="
pk=/tmp/e23chk_$$; rm -rf $pk; prev=""
for mg in 0 6; do
  d=$pk/$mg; mkdir -p "$d"; cd "$d" || exit 2
  "$BIN" --profile=pueyo2024_paper_like --nEd=9 --stopSec=30000 --rngRun=1 \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=177 \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 \
      --allowSfMarginOverride=true --sfLinkMarginDb=$mg \
      --shadowingModel=none --enablePcap=false --verboseLogs=false --trafficLoad=low \
      > run.log 2>&1 || { echo "  ABORTA: margen=$mg no corre (rc=$?). ¿Falta la puerta?"; exit 5; }
  v=$(python3 -c "
import csv, collections
c=collections.Counter()
for r in csv.DictReader(open('mesh_dv_metrics_tx.csv')):
    if r.get('dst')!='65535':
        try: c[int(r['sf'])]+=1
        except Exception: pass
print(dict(sorted(c.items())))" 2>/dev/null)
  echo "  margen=$mg dB -> SF de datos: $v"
  [ -n "$prev" ] && [ "$prev" = "$v" ] && {
      echo "  ABORTA: mismo reparto de SF con 0 y 6 dB. El margen NO muerde."; rm -rf $pk; exit 4; }
  prev=$v
done
cd "$HOME" || exit 2; rm -rf $pk
echo "  distintos: el margen muerde, se puede medir."

for mg in 0 1 2 3 6; do
  for rango in sf78 sf712; do
    for sp in 177 200 240 248 260; do
      for seed in $(seq 1 "$SEEDS"); do echo "$mg $rango $sp 25 $seed"; done
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E23_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
