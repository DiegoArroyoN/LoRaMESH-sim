#!/bin/bash
# run_e21c_semillas_margen1.sh — sube a 20 semillas el brazo de margen 1 dB de E21b.
#
# POR QUE. Las ocho subfiguras de Pueyo-Centelles (fig. 11a-d en rejilla y
# 12a-d en aleatoria) van a reproducirse con margen 1 dB y los DOS modelos de
# interferencia, y sus propias figuras son boxplots. Con 10 semillas la caja es
# pobre; Diego pide 20.
#
# E21b ya midio las 32 combinaciones (margen x modelo x topo x separacion x
# carga) con semillas 1-10. Este runner añade SOLO las semillas 11-20 y SOLO
# para margen 1, que es el unico brazo que entra en esas figuras. Escribe en el
# MISMO directorio de celdas que E21b y con el mismo esquema de nombre, asi que
# las 10 viejas se reutilizan y quedan las 20 juntas.
#
#   2 modelos x 2 topologias x 2 separaciones x 2 cargas x 6 N x 10 semillas
#   = 960 celdas nuevas
#
# COMPARACION CON SUS FIGURAS: SOLO VISUAL, de tendencia y comportamiento. Sus
# figuras son boxplots y no publican valores; cualquier cifra "suya" que
# apareciera en nuestras figuras seria una lectura nuestra a ojo, no un dato. Por
# eso las figuras de replica NO llevan una linea de referencia con sus valores.
#
#   bash run_e21c_semillas_margen1.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
S_INI=${S_INI:-11}
S_FIN=${S_FIN:-20}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e21b_replica          # el MISMO de E21b, a proposito
mkdir -p "$OUT/cells"
CSV="$OUT/e21b_replica_pueyo.csv"
HDR="margen_db,modelo,topo,spacing,carga,nEd,seed,rc,stop_s,generados,entregados,pdr,hops,air_baliza,air_dato,frac_baliza,phy_drops,sf7_d,sf8_d"

horizonte() {  # horizonte <nEd> <carga>
  local n=$1 c=$2 iv
  [ "$c" = "low" ] && iv=100 || iv=1
  python3 -c "print(int(100*($n-1)*$iv*1.15))"
}
export -f horizonte

celda() {
  local mg=$1 mod=$2 topo=$3 sp=$4 carga=$5 n=$6 seed=$7
  local id="m${mg}_${mod}_${topo}_${sp}_${carga}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local imf=""
  [ "$mod" = "ortho" ] && imf="--allowInterferenceModelOverride=true --interferenceModel=pueyo_fixed_capture"
  local pm
  case "$topo" in
    grid)   pm="pueyo_grid" ;;
    random) pm="pueyo_random_equiv" ;;
    *) return 1 ;;
  esac
  local stop; stop=$(horizonte "$n" "$carga")

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 7200 "$BIN" --profile=pueyo2024_paper_like --nEd="$n" --stopSec="$stop" --rngRun="$seed" \
      --nodePlacementMode=$pm --trafficMode=pueyo_all_to_all --pueyoGridSpacingM="$sp" \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 \
      --shadowingModel=none --enablePcap=false --verboseLogs=false \
      --trafficLoad="$carga" --allowSfMarginOverride=true --sfLinkMarginDb="$mg" \
      $imf > run.log 2>&1
  local rc=$?

  MG=$mg M=$mod T=$topo SP=$sp C=$carga N=$n SEED=$seed RC=$rc STOP=$stop ROW="$row" python3 - <<'PY'
import csv, json, os, collections
e=os.environ
gen=deliv=pdr=hops=""
try:
    j=json.load(open("mesh_dv_summary.json")); p=j["pdr"]
    gen=p["total_data_generated"]; deliv=p["delivered"]; pdr=p["pdr"]
    hops=j.get("forwarding",{}).get("avg_hops_delivered","")
except Exception: pass
ab=ad=0.0; sfd=collections.Counter()
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: t=float(r["toaUs"])
        except Exception: continue
        if r.get("dst")=="65535": ab+=t
        else:
            ad+=t
            try: sfd[int(r["sf"])]+=1
            except Exception: pass
except Exception: pass
tot=ab+ad
drops=0
try:
    with open("run.log", errors="replace") as f:
        for l in f:
            if "PHY_INTERFERENCE_DROP" in l: drops+=1
except Exception: pass
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["MG"],e["M"],e["T"],e["SP"],e["C"],e["N"],e["SEED"],e["RC"],e["STOP"],
    gen,deliv,pdr,hops, f4(ab/1e6), f4(ad/1e6), f4(ab/tot) if tot else "",
    drops, sfd.get(7,0), sfd.get(8,0)])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

# PRECONDICION: las semillas nuevas tienen que ser REALMENTE nuevas. Si rngRun
# no se propagara, las 10 añadidas serian copias de las viejas y la caja saldria
# artificialmente estrecha -- que es justo lo contrario de lo que se busca.
echo "== ¿cambia el resultado con la semilla? =="
pk=/tmp/e21cchk_$$; rm -rf $pk; prev=""
for s in 11 12; do
  d=$pk/$s; mkdir -p "$d"; cd "$d" || exit 2
  "$BIN" --profile=pueyo2024_paper_like --nEd=16 --stopSec=40000 --rngRun=$s \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=177 \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 --shadowingModel=none \
      --enablePcap=false --verboseLogs=false --trafficLoad=low \
      --allowSfMarginOverride=true --sfLinkMarginDb=1 > run.log 2>&1 || {
      echo "  ABORTA: semilla $s no corre (rc=$?)"; rm -rf $pk; exit 5; }
  v=$(python3 -c "import json;print('%.6f'%json.load(open('mesh_dv_summary.json'))['pdr']['pdr'])" 2>/dev/null)
  echo "  semilla $s -> PDR $v"
  [ -n "$prev" ] && [ "$prev" = "$v" ] && {
      echo "  ABORTA: semillas 11 y 12 dan el MISMO PDR. rngRun no se propaga."; rm -rf $pk; exit 4; }
  prev=$v
done
cd "$HOME" || exit 2; rm -rf $pk
echo "  las semillas mueven el resultado: se puede medir."

for mod in base ortho; do
  for topo in grid random; do
    for sp in 177 248; do
      for carga in low high; do
        for n in 9 16 25 36 49 64; do
          for seed in $(seq "$S_INI" "$S_FIN"); do echo "1 $mod $topo $sp $carga $n $seed"; done
        done
      done
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4 $5 $6'

cat "$OUT"/cells/*.row 2>/dev/null | sort > /tmp/e21b_all_$$
{ echo "$HDR"; cat /tmp/e21b_all_$$; } > "$CSV"; rm -f /tmp/e21b_all_$$
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E21C_DONE: $(( $(wc -l < "$CSV") - 1 )) filas totales =="
