#!/bin/bash
# run_e21_replica_pueyo.sh — replicacion de Pueyo-Centelles 2024 como ABLACION
# DE SUPUESTOS DE MODELADO.
#
# Nivel 3 de V&V: reproducir el resultado publicado de otro grupo. Es el unico
# nivel que cierra la duda sobre el simulador, y el que preguntara el revisor.
#
# PRIMER INTENTO (2026-08-03, piloto N=9, rejilla 177 m, trafico bajo):
#     Pueyo fig. 11a: PDR 0.95-0.96      nosotros: PDR 0.3540
# con la configuracion verificada identica a la suya: toa_only, ALOHA, SF7-8,
# sin duty, canal determinista, 100 paq/destino, intervalo 100 s, 7200 paquetes
# generados = 9 x 8 x 100 exactamente como especifican.
#
# La diferencia NO es un bug nuestro. Un agente audito su codigo publicado
# (https://gitlab.com/rogerpueyo/floramesh, ref. [37] del paper; espejo en
# github.com/DSG-UPC/FLoRaMesh) y encontro cinco decisiones de modelado que se
# multiplican a su favor:
#
#  1) ORTOGONALIDAD PERFECTA entre SF. LoRaReceiver.cc: si los SF diferentes,
#     `spreadingFactorColision = false` y no se entra al bloque de colision.
#     FLoRa moderno SI tiene la matriz nonOrthDelta[6][6] (commit a401d30,
#     2019-04-28, "Change colision model into nonorthogonal") pero FLoRaMesh se
#     bifurco del FLoRa de 2017 y nunca la incorporo. El paper afirma en su
#     pag. 2 que los SF son "cuasi-ortogonales" y cita el trabajo sobre
#     ortogonalidad imperfecta -- pero su simulador modela la perfecta.
#  2) SIN COMPROBACION DE SINR: computeIsReceptionSuccessful() devuelve true
#     incondicionalmente.
#  3) CAPTURA PAREADA a 6 dB: se compara la señal deseada contra CADA
#     interferente por separado. Tres interferentes a 7 dB por debajo cada uno
#     dan cero colisiones. No se suma potencia interferente en ningun punto.
#  4) PAQUETES DE RUTEO FACTURADOS A 12 B FIJOS
#     (`routingPacket->setByteLength(routingPacketMaxSize)`) aunque lleven hasta
#     N-1 entradas de ruta. Su airtime de control esta drasticamente
#     subestimado al escalar. El truncado que describen en su pag. 7 no esta
#     implementado en esa rama.
#  5) INTERVALO DE ANUNCIO uniform(0,120) s: los "60 s" del paper son una MEDIA,
#     no un periodo.
#
# QUE HACE ESTA CAMPAÑA. En vez de reportar "no reproducimos su numero",
# convierte la discrepancia en medida: acerca nuestra configuracion a la suya un
# supuesto cada vez y cuantifica cuanto PDR aporta cada decision de modelado.
#
#   base      Goursaud (interferencia inter-SF con umbrales finitos, SINR agregado)
#   ortho     pueyo_fixed_capture (SF perfectamente ortogonales, como el suyo)
#
# Lo que YA coincide sin tocar nada: canal determinista (sigma=0), duty off,
# SF aleatorio en balizas (useProbabilisticSfForBeacons ya venia activo, por eso
# medimos balizas repartidas entre SF7 y SF8), 20 dBm, CR 4/5, preambulo 16,
# 100 paquetes a cada destino, intervalos 100 s / 1 s.
#
# Lo que NO podemos emular y se declara como limitacion:
#   - el jitter uniform(0,120) del intervalo de anuncio: nuestro intervalo es
#     fijo. Mismo valor medio, distinta varianza.
#   - la facturacion de 12 B fijos para paquetes de ruteo: los nuestros pagan su
#     tamaño real (~100 ms de ToA). Este es probablemente el segundo factor en
#     importancia y NO esta en la ablacion.
#
# HORIZONTE POR N. Su criterio de fin es "hasta transmitir todos los paquetes
# generados": 100 x (N-1) paquetes por nodo al intervalo correspondiente. Para
# N=64 con trafico bajo son 6300 x 100 s = 630 ks. Se calcula por celda.
#
# PREDICCION registrada: si la ortogonalidad es el factor dominante, el brazo
# `ortho` debe subir sustancialmente hacia 0.95 en trafico bajo. Si sube poco,
# entonces el peso esta en la facturacion de 12 B (que no emulamos) y hay que
# decirlo asi.
#
#
# REVISION 2026-08-03 -- EL MARGEN, Y POR QUE NO ES "REPLICAR MEJOR".
#
# La primera pasada de E21 corrio con sfLinkMarginDb=0. Despues, E23 descubrio
# que ese valor deprime nuestro PDR un factor 2.3x en la propia configuracion de
# Pueyo (177 m, SF7-8, determinista, trafico bajo: 0.1867 con margen 0 frente a
# 0.4249 con 1 dB). O sea que parte de la brecha que atribuimos a SUS decisiones
# de modelado era un defecto NUESTRO.
#
# Diego pregunto lo correcto: "ese margen no lo tienen en su simulador". Es
# cierto, y la respuesta importa para como se reporta esto:
#
#   ELLOS   selector: sensibilidad pura, sin margen
#           recepcion: sensibilidad pura + colision SOLO con mismo SF a 6 dB.
#                      computeIsReceptionSuccessful() devuelve true sin mirar SINR.
#
#   NOSOTROS selector: sensibilidad pura, sin margen (identico al suyo)
#            recepcion: sensibilidad + matriz de aislamiento 6x6 entre TODOS los
#                       SF (collisionSnirGoursaud: diagonal 6 dB, fuera de la
#                       diagonal de -16 a -36 dB)
#
# En su simulador selector y receptor usan el MISMO criterio, asi que no hace
# falta margen. En el nuestro hay una INCOHERENCIA INTERNA: el selector declara
# alcanzable un enlace a 0.1 dB de la sensibilidad y luego el receptor exige
# ademas sobrevivir a la matriz. Los dos componentes no se hablan.
#
# Conclusion sobre el encuadre: el margen es un arreglo legitimo de NUESTRO
# simulador -- resuelve una incoherencia entre nuestro selector y nuestro PHY --
# pero NO es "lo que hace Pueyo". Correr con margen no es una replicacion mas
# fiel: es nuestro simulador funcionando de forma coherente, comparado contra sus
# cifras publicadas. La diferencia que quede NO se puede atribuir al margen sino
# al modelo de recepcion, que es la diferencia de fondo y no se puede eliminar
# sin reimplementar su PHY.
#
# Por eso el margen entra como TERCER FACTOR y no sustituye al brazo de margen 0:
# hay que poder reportar ambos y decir cual es cual.
#
#   bash run_e21_replica_pueyo.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-14}
SEEDS=${SEEDS:-10}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e21b_replica
mkdir -p "$OUT/cells"
CSV="$OUT/e21b_replica_pueyo.csv"
HDR="margen_db,modelo,topo,spacing,carga,nEd,seed,rc,stop_s,generados,entregados,pdr,hops,air_baliza,air_dato,frac_baliza,phy_drops,sf7_d,sf8_d"

# Horizonte de Pueyo: 100 paquetes a cada uno de los (N-1) destinos, al
# intervalo de la carga, mas 15% de drenaje.
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
      --trafficLoad="$carga" --allowSfMarginOverride=true --sfLinkMarginDb="$mg" \n      $imf > run.log 2>&1
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
        try: t=float(r["toaUs"]); sf=int(r["sf"])
        except Exception: continue
        if r.get("dst")=="65535": ab+=t
        else: ad+=t; sfd[sf]+=1
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
    gen,deliv,pdr,hops,
    f4(ab/1e6),f4(ad/1e6),f4(ab/tot) if tot else "",drops,
    sfd.get(7,0),sfd.get(8,0)])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# PRECONDICION: el modelo de interferencia tiene que MORDER. Si las dos ramas
# dieran identico, la ablacion mediria cero y concluiriamos lo contrario de la
# verdad. Es la misma disciplina que salvo a E19 de correr sin Psi.
echo "== ¿muerde el modelo de interferencia? =="
pk=/tmp/e21chk_$$; rm -rf $pk
for m in goursaud pueyo_fixed_capture; do
  d=$pk/$m; mkdir -p "$d"; cd "$d" || exit 2
  ex=""; [ "$m" != "goursaud" ] && ex="--allowInterferenceModelOverride=true --interferenceModel=$m"
  # shellcheck disable=SC2086
  "$BIN" --profile=pueyo2024_paper_like --nEd=9 --stopSec=20000 --rngRun=1 \
      --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=177 \
      --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 \
      --shadowingModel=none --enablePcap=false --verboseLogs=false \
      --trafficLoad=low $ex > run.log 2>&1
  v=$(python3 -c "import json;print('%.4f'%json.load(open('mesh_dv_summary.json'))['pdr']['pdr'])" 2>/dev/null)
  echo "  $m -> PDR $v"
  echo "$v" > "$pk/$m.pdr"
done
cd "$HOME" || exit 2
a=$(cat $pk/goursaud.pdr 2>/dev/null); b=$(cat $pk/pueyo_fixed_capture.pdr 2>/dev/null)
rm -rf $pk
if [ "$a" = "$b" ]; then
    echo "ABORTA: los dos modelos de interferencia dan el MISMO PDR ($a). El flag no muerde."
    exit 4
fi
echo "  distintos ($a vs $b): el factor muerde, se puede medir."

{
 for mg in 0 1 3; do
  for mod in base ortho; do
    for topo in grid random; do
      for sp in 177 248; do
        for carga in low high; do
          for n in 9 16 25 36 49 64; do
            for seed in $(seq 1 "$SEEDS"); do echo "$mg $mod $topo $sp $carga $n $seed"; done
          done
        done
      done
    done
  done
 done
} | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4 $5 $6'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E21B_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
