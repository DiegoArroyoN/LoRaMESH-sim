#!/bin/bash
# run_e28_baliza_con_margen.sh — E22 rehecha fuera del regimen fragil.
#
# QUE DEUDA PAGA. E22 midio el precio de la baliza y encontro un OPTIMO INTERIOR
# en 18 rutas por emision: recortarla hasta la facturacion de FLoRaMesh (2 rutas)
# da el PEOR PDR, no el mejor.
#
#   bytes rutas    N=9     N=25     N=49     N=64
#       7     2  0.4740   0.2379   0.1284   0.1009   <- su facturacion: LA PEOR
#      28     9  0.4790   0.2862   0.1738   0.1370
#      56    18  0.4790   0.2827   0.1790   0.1453   <- optimo
#     112    37  0.4790   0.2737   0.1769   0.1447
#     251    83  0.4790   0.2737   0.1676   0.1366
#
# Pero E22 corrio a sfLinkMarginDb=0, y despues E23 descubrio que ese valor
# deprime el PDR un factor 2.3x en esa misma configuracion: a 177 m la DIAGONAL
# de la rejilla cae a 250.3 m, o sea a 0.02 dB del umbral de SF7. O sea que el
# optimo se midio con los enlaces diagonales rotos.
#
# El MECANISMO del ToA no depende del margen -- es contabilidad de aire pura, y
# la baliza de 83 rutas seguira costando 99.9 -> 399.5 ms entre N=9 y N=64 con
# cualquier margen. Lo que si es un contraste pareado, y por tanto sospechoso, es
# el OPTIMO INTERIOR: si la ventaja de las 18 rutas venia de que con los
# diagonales rotos hacia falta menos informacion de ruteo, con margen puede
# moverse o desaparecer.
#
# QUE HACE ESTA CAMPAÑA. Repite el barrido con los DOS margenes en la misma
# corrida y con el mismo binario, en vez de comparar contra las celdas de E22.
# Cuesta el doble de celdas y elimina de golpe dos dudas: el margen y la mezcla
# de binarios (E22 corrio antes de la facturacion plana y de las seis columnas de
# trazabilidad; hay regresion verificada, pero medirlo de nuevo es mas barato que
# defenderlo).
#
#   2 margenes x 5 capacidades x 6 tamaños x 10 semillas = 600 celdas
#
# OJO CON LA TRADUCCION BYTES->RUTAS. kPueyoBeaconEntryBytes = 3 y la cabecera son
# 6 B, con tope de MTU en (255-6)/3 = 83. La cabecera de E22 decia "7 B por
# entrada" y su columna rutas_max dividia entre 7: las corridas siempre fueron
# correctas y lo que estaba mal era la etiqueta. Aqui va corregida.
#
# PREDICCION registrada antes de mirar: el optimo interior SOBREVIVE pero se
# desplaza hacia MAS rutas. Con los diagonales sanos hay mas caminos que anunciar,
# asi que la informacion de ruteo vale mas y compensa pagar mas aire por baliza.
# Si en cambio el optimo desaparece y el PDR crece monotono con la capacidad,
# entonces "el tamaño de baliza es un parametro de diseño con frontera" se cae
# como resultado y hay que reportarlo asi.
#
#   bash run_e28_baliza_con_margen.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-6}
SEEDS=${SEEDS:-10}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e28_baliza_margen
mkdir -p "$OUT/cells"
CSV="$OUT/e28_baliza_con_margen.csv"
HDR="margen_db,payload_b,rutas_max,nEd,seed,rc,stop_s,generados,entregados,pdr,hops,n_baliza,toa_baliza_ms,air_baliza,air_dato,frac_baliza,phy_drops"

horizonte() { python3 -c "print(int(100*($1-1)*100*1.15))"; }
export -f horizonte

celda() {
  local mg=$1 pb=$2 n=$3 seed=$4
  local id="m${mg}_pb${pb}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0
  local stop; stop=$(horizonte "$n")

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # Brazo `ortho` como base, igual que E22: SF ortogonales, para que lo unico que
  # cambie entre celdas sea la capacidad de la baliza y el margen.
  # shellcheck disable=SC2086
  timeout 7200 "$BIN" --profile=pueyo2024_paper_like --nEd="$n" --stopSec="$stop" --rngRun="$seed" \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=177 \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 \
      --shadowingModel=none --enablePcap=false --verboseLogs=false --trafficLoad=low \
      --allowInterferenceModelOverride=true --interferenceModel=pueyo_fixed_capture \
      --allowSfMarginOverride=true --sfLinkMarginDb="$mg" \
      --allowDvPayloadOverride=true --dvPayloadMaxBytes="$pb" > run.log 2>&1
  local rc=$?

  MG=$mg PB=$pb N=$n SEED=$seed RC=$rc STOP=$stop ROW="$row" python3 - <<'PY'
import csv, json, os
e=os.environ
gen=deliv=pdr=hops=""
try:
    j=json.load(open("mesh_dv_summary.json")); p=j["pdr"]
    gen=p["total_data_generated"]; deliv=p["delivered"]; pdr=p["pdr"]
    hops=j.get("forwarding",{}).get("avg_hops_delivered","")
except Exception: pass
ab=ad=0.0; nb=0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: t=float(r["toaUs"])
        except Exception: continue
        if r.get("dst")=="65535": ab+=t; nb+=1
        else: ad+=t
except Exception: pass
tot=ab+ad
drops=0
try:
    with open("run.log", errors="replace") as f:
        for l in f:
            if "PHY_INTERFERENCE_DROP" in l: drops+=1
except Exception: pass
rutas=min(max(1, int(e["PB"])//3), (255-6)//3)
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["MG"], e["PB"], rutas, e["N"], e["SEED"], e["RC"], e["STOP"],
    gen, deliv, pdr, hops, nb,
    f4(ab/nb/1000) if nb else "", f4(ab/1e6), f4(ad/1e6),
    f4(ab/tot) if tot else "", drops])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# PRECONDICION: los DOS factores tienen que morder por separado. Si solo se
# comprobara uno, el otro podria estar inerte y la campaña concluiria que da
# igual -- que es lo contrario de la verdad. Ya paso con --dvBeaconMaxRoutes, que
# era codigo muerto y daba corridas bit-identicas con K=0,1,4,16.
echo "== ¿muerden la capacidad de baliza y el margen, por separado? =="
pk=/tmp/e28chk_$$; rm -rf $pk
probe() {  # probe <margen> <payload> -> "ToA_baliza pdr"
  local d=$pk/$1_$2; rm -rf "$d"; mkdir -p "$d"; cd "$d" || exit 2
  "$BIN" --profile=pueyo2024_paper_like --nEd=25 --stopSec=30000 --rngRun=1 \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=177 \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 --shadowingModel=none \
      --enablePcap=false --verboseLogs=false --trafficLoad=low \
      --allowInterferenceModelOverride=true --interferenceModel=pueyo_fixed_capture \
      --allowSfMarginOverride=true --sfLinkMarginDb="$1" \
      --allowDvPayloadOverride=true --dvPayloadMaxBytes="$2" > run.log 2>&1 || {
      echo "ERROR"; return; }
  python3 -c "
import csv, json
ab=0.0; nb=0
for r in csv.DictReader(open('mesh_dv_metrics_tx.csv')):
    if r.get('dst')=='65535':
        try: ab+=float(r['toaUs']); nb+=1
        except Exception: pass
print('toa=%.1f pdr=%.4f' % (ab/nb/1000 if nb else 0, json.load(open('mesh_dv_summary.json'))['pdr']['pdr']))"
}
a=$(probe 1 7); b=$(probe 1 251); c=$(probe 0 251)
echo "  margen 1, 2 rutas   -> $a"
echo "  margen 1, 83 rutas  -> $b"
echo "  margen 0, 83 rutas  -> $c"
[ "$a" = "$b" ] && { echo "  ABORTA: la capacidad de baliza NO muerde."; rm -rf $pk; exit 4; }
[ "$b" = "$c" ] && { echo "  ABORTA: el margen NO muerde."; rm -rf $pk; exit 4; }
cd "$HOME" || exit 2; rm -rf $pk
echo "  los dos muerden por separado: se puede medir."

for mg in 0 1; do
  for pb in 7 28 56 112 251; do
    for n in 9 16 25 36 49 64; do
      for seed in $(seq 1 "$SEEDS"); do echo "$mg $pb $n $seed"; done
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E28_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
