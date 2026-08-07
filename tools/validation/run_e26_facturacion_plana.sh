#!/bin/bash
# run_e26_facturacion_plana.sh — la prueba decisiva de la replica: ¿explican los
# 12 B lo que queda de brecha con Pueyo-Centelles?
#
# DE DONDE VIENE. E21b dejo la replica en este estado (rejilla, trafico bajo,
# toa_only, SF7-8, ALOHA, sin duty, canal determinista, margen 1 + SF ortogonales):
#
#     177 m: reproducido. Seis puntos entre +0.2% y +3.9% de sus valores.
#     248 m: NO reproducido, y la desviacion CRECE con N:
#            -5.0% (N=9) -> -17.5% (N=25) -> -38.0% (N=49) -> -45.9% (N=64)
#
# Que la desviacion crezca con N es la firma de su facturacion de control:
#
#     routingPacket->setByteLength(routingPacketMaxSize);   // 12 B, constante
#
# En OMNeT++/INET setByteLength es lo que fija la duracion de la transmision, y
# las entradas de ruta viven en una estructura C++ que NUNCA se serializa: viaja
# por referencia. Asi que su modelo entrega informacion de ruteo COMPLETA al
# precio de un paquete MINIMO. Su plano de control no escala con la red por
# construccion. Medido en nuestro simulador (E22, ToA media de baliza):
#
#     baliza real (83 rutas)   99.9 -> 140.3 -> 184.5 -> 248.1 -> 315.5 -> 399.5 ms
#     facturada a 12 B          65.9 ->  65.9 ->  65.9 ->  65.9 ->  65.9 ->  65.9 ms
#                                N=9     N=16     N=25     N=36     N=49     N=64
#
# QUE HACE ESTA CAMPAÑA, Y EN QUE SE DIFERENCIA DE E22. E22 recorto la baliza:
# pagaba 12 B y difundia 2 rutas. Eso NO es su modelo, y por eso salio peor que
# la baliza completa (PDR 0.1009 frente a 0.1366 en N=64). Aqui reproducimos su
# modelo EXACTO -- 12 B de aire con las rutas completas -- mediante
# --pueyoFlatBeaconBytes=12, que deja las entradas en un tag de ns-3. Los tags no
# cuentan para Packet::GetSize() y GetSize() es de donde LoraPhy::GetOnAirTime
# saca la duracion, asi que la semantica es identica a la suya sin tocar el PHY.
#
# ESTO NO ES UN MODO DE OPERACION. Es fisicamente irrealizable: no existe radio
# que transmita 83 entradas en el aire de 12 bytes. El parametro esta apagado por
# defecto, exige la puerta --allowFlatBeaconBillingOverride, y el binario avisa
# por stderr cada vez que se activa. Ninguna cifra de esta campaña describe
# nuestro protocolo: describen el suyo.
#
# PREDICCION registrada antes de mirar:
#   1) a 248 m la brecha se cerrara en su mayor parte y, lo mas importante, dejara
#      de crecer con N. Si sigue creciendo, los 12 B NO son la explicacion y hay
#      un quinto factor que no hemos identificado.
#   2) a 177 m, donde ya reproducimos su curva sin esto, la facturacion plana
#      SOBREPASARA sus valores. Eso seria la prueba mas fuerte del argumento: su
#      propio modelo es optimista y sus numeros publicados quedan por debajo de
#      lo que su simulador daria si el resto fuera igual al nuestro.
#   3) el numero de rutas que llegan sera el mismo en los dos brazos. Si baja, el
#      tag no esta transportando bien y la medida no vale.
#
#   bash run_e26_facturacion_plana.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-14}
SEEDS=${SEEDS:-10}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e26_plana
mkdir -p "$OUT/cells"
CSV="$OUT/e26_facturacion_plana.csv"
HDR="factura,spacing,nEd,seed,rc,stop_s,generados,entregados,pdr,hops,n_baliza,toa_baliza_ms,air_baliza,air_dato,frac_baliza,phy_drops"

horizonte() { python3 -c "print(int(100*($1-1)*100*1.15))"; }
export -f horizonte

celda() {
  local fac=$1 sp=$2 n=$3 seed=$4
  local id="${fac}_${sp}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0
  local stop; stop=$(horizonte "$n")

  # brazo `real`: nuestra baliza paga su aire. brazo `plano`: la suya.
  local ff=""
  [ "$fac" = "plano" ] && ff="--allowFlatBeaconBillingOverride=true --pueyoFlatBeaconBytes=12"

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # Configuracion identica al mejor brazo de E21b (margen 1 + SF ortogonales):
  # lo unico que cambia entre celdas es como se factura la baliza.
  # shellcheck disable=SC2086
  timeout 7200 "$BIN" --profile=pueyo2024_paper_like --nEd="$n" --stopSec="$stop" --rngRun="$seed" \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM="$sp" \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 \
      --shadowingModel=none --enablePcap=false --verboseLogs=false --trafficLoad=low \
      --allowInterferenceModelOverride=true --interferenceModel=pueyo_fixed_capture \
      --allowSfMarginOverride=true --sfLinkMarginDb=1 \
      $ff > run.log 2>&1
  local rc=$?

  F=$fac SP=$sp N=$n SEED=$seed RC=$rc STOP=$stop ROW="$row" python3 - <<'PY'
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
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["F"], e["SP"], e["N"], e["SEED"], e["RC"], e["STOP"], gen, deliv, pdr, hops, nb,
    f4(ab/nb/1000) if nb else "", f4(ab/1e6), f4(ad/1e6),
    f4(ab/tot) if tot else "", drops])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# PRECONDICION. Dos cosas tienen que cumplirse A LA VEZ, y la segunda es la que
# de verdad hace peligrosa a esta campaña:
#   (a) la facturacion plana MUERDE: la ToA de baliza tiene que caer.
#   (b) los dos brazos ENCAMINAN IGUAL: si el tag no transporta las rutas, el
#       brazo `plano` mide una red sin ruteo y todo lo que se concluya es basura.
#
# EL INVARIANTE ES `hops`, NO EL PDR. La primera version de esta guarda pedia
# solo PDR > 0.05 y dejo pasar la campaña entera con el tag roto: el brazo plano
# dio hops=0.0000 en las 120 celdas -- cero reenvios, tabla de rutas vacia -- con
# un PDR de 0.21 que superaba el umbral de sobra porque los vecinos directos
# seguian entregando. La causa era ProcessTxQueue: hace RemoveAllPacketTags() y
# repone solo el metric tag, asi que el tag de rutas moria en la cola de salida
# mientras el TX parecia perfecto. Comparar los saltos entre brazos lo habria
# cazado en la primera corrida; comparar el PDR no, y ademas el PDR CAMBIA a
# proposito entre brazos (es lo que la campaña mide), asi que no puede servir de
# invariante. Se exige que los saltos medios no difieran mas de un 10%.
echo "== ¿muerde la facturacion plana, y encaminan igual los dos brazos? =="
pk=/tmp/e26chk_$$; rm -rf $pk; prevtoa=""; prevhops=""
for fac in real plano; do
  d=$pk/$fac; mkdir -p "$d"; cd "$d" || exit 2
  ff=""
  [ "$fac" = "plano" ] && ff="--allowFlatBeaconBillingOverride=true --pueyoFlatBeaconBytes=12"
  # shellcheck disable=SC2086
  "$BIN" --profile=pueyo2024_paper_like --nEd=25 --stopSec=60000 --rngRun=1 \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=177 \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 \
      --shadowingModel=none --enablePcap=false --verboseLogs=false --trafficLoad=low \
      --allowInterferenceModelOverride=true --interferenceModel=pueyo_fixed_capture \
      --allowSfMarginOverride=true --sfLinkMarginDb=1 $ff > run.log 2>&1 || {
      echo "  ABORTA: brazo '$fac' no corre (rc=$?). ¿Falta la puerta?"; rm -rf $pk; exit 5; }
  read -r toa pdr hops <<<"$(python3 -c "
import csv, json
ab=0.0; nb=0
for r in csv.DictReader(open('mesh_dv_metrics_tx.csv')):
    if r.get('dst')=='65535':
        try: ab+=float(r['toaUs']); nb+=1
        except Exception: pass
j=json.load(open('mesh_dv_summary.json'))
h=j.get('forwarding',{}).get('avg_hops_delivered',0) or 0
print('%.1f %.4f %.4f' % (ab/nb/1000 if nb else 0, j['pdr']['pdr'], float(h)))" 2>/dev/null)"
  echo "  $fac -> ToA baliza $toa ms, PDR $pdr, saltos $hops"
  if [ "$fac" = "plano" ]; then
    [ "$prevtoa" = "$toa" ] && {
        echo "  ABORTA: misma ToA de baliza con y sin facturacion plana. NO muerde."; rm -rf $pk; exit 4; }
    python3 -c "
import sys
r, p = $prevhops, $hops
if r <= 0 or p <= 0: sys.exit(1)
sys.exit(0 if abs(p-r)/r <= 0.10 else 2)" || {
        echo "  ABORTA: saltos $prevhops (real) frente a $hops (plano). Los brazos NO encaminan"
        echo "          igual: el tag no esta transportando las rutas y la campaña mediria una"
        echo "          red sin ruteo, no la facturacion de la baliza."; rm -rf $pk; exit 6; }
  fi
  prevtoa=$toa; prevhops=$hops
done
cd "$HOME" || exit 2; rm -rf $pk
echo "  muerde la facturacion y los dos brazos encaminan igual: se puede medir."

for fac in real plano; do
  for sp in 177 248; do
    for n in 9 16 25 36 49 64; do
      for seed in $(seq 1 "$SEEDS"); do echo "$fac $sp $n $seed"; done
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E26_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
