#!/bin/bash
# run_e22_coste_baliza.sh — el tercer brazo de la replicacion: ¿cuanto de la
# brecha con Pueyo la explica el precio de la baliza?
#
# E21 midio los dos primeros brazos sobre su fig. 11a (rejilla 177 m, trafico
# bajo, ToA SF7-8, ALOHA, sin duty, canal determinista):
#
#     N     Pueyo    base(Goursaud)  ortho(SF ortogonales)  %aire en balizas
#     9     0.955        0.354            0.479                 65.8%
#    16     0.910        0.252            0.360                 70.1%
#    25     0.870        0.187            0.274                 73.9%
#    36     0.830        0.140            0.214                 78.3%
#    49     0.790        0.107            0.168                 79.6%
#    64     0.730        0.085            0.137                 82.9%
#
# La ortogonalidad perfecta aporta un +35% relativo constante y NO cierra nada:
# la brecha CRECE con N, de 2x a 5.3x. Y la ultima columna dice por que -- el
# aire consumido por balizas sube del 65.8% al 82.9% conforme crece la red.
#
# En FLoRaMesh esa columna no crece. `routingPacket->setByteLength(12B)` factura
# todo paquete de ruteo a 12 bytes fijos por mas rutas que lleve: a 64 nodos un
# nodo difunde hasta 63 entradas cobradas como si fuera una. Su coste de control
# no escala con la red, por construccion.
#
# QUE MIDE ESTA CAMPAÑA. dvPayloadMaxBytes fija cuantas entradas caben por
# baliza, a kPueyoBeaconEntryBytes = 3 B por entrada, sobre una cabecera de
# kPueyoBeaconHeaderBytes = 6 B. Barriendolo se dibuja la frontera
# INFORMACION-contra-AIRTIME del plano de control:
#
#     dvPayloadMaxBytes=7   ->  2 entradas, baliza de 12 B  <- la facturacion suya
#     dvPayloadMaxBytes=28  ->  9 entradas, baliza de 33 B
#     dvPayloadMaxBytes=56  -> 18 entradas
#     dvPayloadMaxBytes=112 -> 37 entradas
#     dvPayloadMaxBytes=251 -> 83 entradas (nuestra base actual; el tope real lo
#                              pone la MTU de 255 B: (255-6)/3 = 83)
#
# CORRECCION 2026-08-06. Estas cuatro lineas decian "7 B por entrada" y daban
# 1/4/16/35 entradas. Es falso: la constante es 3 y lo ha sido desde el port
# (dd33b0884). Las CORRIDAS SIEMPRE FUERON CORRECTAS -- el barrido pasa
# --dvPayloadMaxBytes y el binario divide entre kPueyoBeaconEntryBytes -- lo que
# estaba mal era solo la traduccion bytes->rutas de este comentario y de la
# columna `rutas_max` del CSV, que dividia entre 7. Corregidas ambas.
#
# DIFERENCIA SEMANTICA QUE HAY QUE DECLARAR. Su modelo es "transmite todas las
# rutas y paga 12 B". Aqui conseguimos "paga 12 B y transmite 1 ruta". NO es lo
# mismo, y esa es justamente la cuestion: su combinacion no es fisicamente
# realizable. Si con baliza de 12 B el PDR TAMPOCO sube hacia 0.73, queda
# demostrado que su resultado exige informacion completa al precio de un paquete
# minimo -- que es imposible.
#
# DOS DEFECTOS ARREGLADOS PARA PODER CORRER ESTO (2026-08-03):
#  1) GetBeaconRouteCapacity() tenia un return temprano: si dvPayloadMaxBytes>0
#     salia antes de mirar dvBeaconMaxRoutes. Como el perfil comparable fija
#     dvPayloadMaxBytes=251, esa rama se tomaba SIEMPRE y --dvBeaconMaxRoutes
#     era codigo muerto (verificado: K=0,1,4,16 daban corridas bit-identicas).
#     Ahora los dos topes componen.
#  2) dvPayloadMaxBytes esta en el snapshot del perfil, asi que pasarlo distinto
#     de 251 abortaba. Se le añadio puerta --allowDvPayloadOverride, siguiendo
#     el patron de las otras nueve.
#
# PREDICCION registrada antes de mirar: el PDR subira al abaratar la baliza,
# pero saturara muy por debajo de 0.73, porque con pocas rutas por emision la
# propagacion se vuelve el cuello de botella. El optimo estara en un valor
# intermedio, y ESE optimo es un resultado nuestro: el tamaño de baliza es un
# parametro de diseño con frontera, no un detalle de implementacion.
#
#
# RESULTADO (300 celdas, rc=0 en todas). La prediccion acerto a medias.
#
#   bytes rutas    N=9     N=25     N=49     N=64
#       7     2  0.4740   0.2379   0.1284   0.1009   <- su facturacion: LA PEOR
#      28     9  0.4790   0.2862   0.1738   0.1370
#      56    18  0.4790   0.2827   0.1790   0.1453   <- optimo
#     112    37  0.4790   0.2737   0.1769   0.1447
#     251    83  0.4790   0.2737   0.1676   0.1366
#
# ACERTO el optimo interior (18 rutas). FALLO la direccion en el extremo barato:
# el PDR no sube al abaratar la baliza, BAJA. Con 2 rutas por emision se ahorra
# aire pero se difunde tan poca informacion que la convergencia se degrada mas de
# lo que compensa el ahorro. Ni el optimo se acerca a 0.73.
#
# CONCLUSION: su resultado exige informacion de ruteo COMPLETA al precio de un
# paquete MINIMO, y esa combinacion no existe en el espacio de diseño. No es que
# entreguemos menos que ellos: es que ese punto no es realizable.
#
# EL MECANISMO, medido (ToA media de baliza por N):
#     251 B (83 rutas)  99.9 -> 140.3 -> 184.5 -> 248.1 -> 315.5 -> 399.5 ms  (x4)
#       7 B (2 rutas)   65.9 ->  65.9 ->  65.9 ->  65.9 ->  65.9 ->  65.9 ms  (plana)
# La fila plana es su modelo; la de arriba es la fisica. Por eso la discrepancia
# con su curva CRECE con N: su error crece con N.
#
# PENDIENTE. Esta campaña corrio a sfLinkMarginDb=0, antes del arreglo del
# selector. El mecanismo del ToA es contabilidad de aire pura y no depende del
# margen, asi que esa parte esta firme. Pero el OPTIMO INTERIOR es un contraste
# pareado y hay que reverificarlo con margen 1 antes de reportarlo -- mismo
# argumento que E24, donde el contraste B se atenuo de +5.69% a +3.62%.
#
#   bash run_e22_coste_baliza.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-14}
SEEDS=${SEEDS:-10}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e22_baliza
mkdir -p "$OUT/cells"
CSV="$OUT/e22_coste_baliza.csv"
HDR="payload_b,rutas_max,nEd,seed,rc,stop_s,generados,entregados,pdr,hops,n_baliza,toa_baliza_ms,air_baliza,air_dato,frac_baliza,phy_drops"

horizonte() { python3 -c "print(int(100*($1-1)*100*1.15))"; }
export -f horizonte

celda() {
  local pb=$1 n=$2 seed=$3
  local id="pb${pb}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0
  local stop; stop=$(horizonte "$n")

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # Brazo `ortho` de E21 como base (SF ortogonales, como su simulador), para
  # que lo unico que cambie entre celdas sea el precio de la baliza.
  # shellcheck disable=SC2086
  timeout 7200 "$BIN" --profile=pueyo2024_paper_like --nEd="$n" --stopSec="$stop" --rngRun="$seed" \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=177 \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 \
      --shadowingModel=none --enablePcap=false --verboseLogs=false --trafficLoad=low \
      --allowInterferenceModelOverride=true --interferenceModel=pueyo_fixed_capture \
      --allowDvPayloadOverride=true --dvPayloadMaxBytes="$pb" > run.log 2>&1
  local rc=$?

  PB=$pb N=$n SEED=$seed RC=$rc STOP=$stop ROW="$row" python3 - <<'PY'
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
    e["PB"], min(max(1, int(e["PB"])//3), (255-6)//3), e["N"], e["SEED"], e["RC"], e["STOP"],
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

# PRECONDICION: el tamaño de baliza tiene que MORDER. Es el arreglo que acaba de
# entrar, asi que se comprueba antes de gastar una sola celda -- si el early
# return siguiera vivo, las cuatro columnas saldrian identicas y la campaña
# concluiria justo lo contrario de la verdad.
echo "== ¿muerde ahora el tamaño de baliza? =="
pk=/tmp/e22chk_$$; rm -rf $pk
prev=""
for pb in 7 251; do
  d=$pk/$pb; mkdir -p "$d"; cd "$d" || exit 2
  "$BIN" --profile=pueyo2024_paper_like --nEd=25 --stopSec=30000 --rngRun=1 \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=177 \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 --shadowingModel=none \
      --enablePcap=false --verboseLogs=false --trafficLoad=low \
      --allowInterferenceModelOverride=true --interferenceModel=pueyo_fixed_capture \
      --allowDvPayloadOverride=true --dvPayloadMaxBytes=$pb > run.log 2>&1 || {
      echo "  ABORTA: dvPayloadMaxBytes=$pb no corre (rc=$?). ¿Falta la puerta?"; exit 5; }
  v=$(python3 -c "
import csv
ab=0.0; nb=0
for r in csv.DictReader(open('mesh_dv_metrics_tx.csv')):
    if r.get('dst')=='65535':
        try: ab+=float(r['toaUs']); nb+=1
        except Exception: pass
print('%.1f' % (ab/nb/1000 if nb else 0))" 2>/dev/null)
  echo "  dvPayloadMaxBytes=$pb -> ToA media de baliza $v ms"
  [ -n "$prev" ] && [ "$prev" = "$v" ] && {
      echo "  ABORTA: mismo ToA con 7 y 251 bytes. El tamaño de baliza NO muerde."; rm -rf $pk; exit 4; }
  prev=$v
done
cd "$HOME" || exit 2; rm -rf $pk
echo "  distintos: el tamaño de baliza muerde, se puede medir."

for pb in 7 28 56 112 251; do
  for n in 9 16 25 36 49 64; do
    for seed in $(seq 1 "$SEEDS"); do echo "$pb $n $seed"; done
  done
done | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E22_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
