#!/bin/bash
# run_rerun_all.sh — re-lanza E1/E2, E3 y E6 con el arbol ya corregido.
#
# Por que hay que repetirlas enteras y no solo las celdas de toa_only:
#
#   El arreglo de histeresis (65f40f408) toca SOLO la rama TOA_ONLY -- para el
#   resto de modos la expresion vieja y la nueva valen ambas true, asi que sus
#   celdas saldrian identicas. Pero estas tres campañas son ANTERIORES al
#   arreglo de seleccion de SF por enlace, y aquel afectaba a todo: con
#   GetDataSfForNeighbor devolviendo 12 constante, ToaHopCostUnits(12) daba 32
#   en todos los enlaces y degeneraba toa_only en conteo de saltos, el termino
#   T_hat de la compuesta estaba saturado, y las transmisiones de datos iban
#   todas en SF12. E6 tampoco se salva: su trafico tambien viajaba en SF12.
#
# Orden: E1/E2 primero (corridas de 40 ks, dan resultados usables antes), luego
# E3 (300 ks, la cara) y por ultimo E6. Secuencial y no en paralelo para que no
# se peleen por los nucleos; cada runner ya paraleliza dentro.
#
#   bash run_rerun_all.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"

echo "== binario =="
ls -la "$BIN"

# Las tres campañas son reanudables por fichero .row, asi que si quedan celdas
# viejas se saltarian y el re-lanzamiento no haria nada. Se archivan, no se
# borran: sirven para medir cuanto movieron los arreglos.
STAMP=pre20260727
echo "== archivando resultados anteriores =="
for d in e1e2 e3 e6; do
    if [ -d "$HOME/ns3-runs/$d" ] && [ ! -d "$HOME/ns3-runs/${d}_$STAMP" ]; then
        mv "$HOME/ns3-runs/$d" "$HOME/ns3-runs/${d}_$STAMP"
        echo "  $d -> ${d}_$STAMP"
    else
        echo "  $d: nada que archivar (o ya archivado)"
    fi
done

# Comprobar que el binario trae los dos arreglos antes de encolar ~3440
# corridas. La leccion del 25-jul: 480 celdas perdidas por lanzar contra un
# binario sin la funcion.
echo "== comprobaciones previas =="
if ! "$BIN" --PrintHelp 2>&1 | grep -q "routeSwitchHysteresis"; then
    echo "ABORTA: binario sin --routeSwitchHysteresis. Falta sincronizar y recompilar."
    exit 3
fi
echo "  flag de histeresis: ok"

# El arreglo de SF se comprueba mirando que el SF de datos VARIA con el enlace:
# si volviese a salir constante, las tres campañas medirian lo mismo que las
# viejas y no habria por que repetirlas.
probe=/tmp/rerun_sfprobe; rm -rf $probe; mkdir -p $probe; cd $probe || exit 2
"$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=15000 --rngRun=3 \
    --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
    --enablePcap=false --verboseLogs=false > probe.log 2>&1
nsf=$(python3 -c "
import csv,collections
c=collections.Counter()
for r in csv.DictReader(open('mesh_dv_metrics_tx.csv')):
    if r.get('dst')!='65535':
        try: c[int(r['sf'])]+=1
        except: pass
print(len(c), dict(sorted(c.items())))
" 2>/dev/null)
echo "  SF de datos distintos: $nsf"
if [ "${nsf%% *}" = "1" ] || [ -z "$nsf" ]; then
    echo "ABORTA: el SF de datos no varia con el enlace. El arreglo no esta en este binario."
    exit 4
fi
# Salir del directorio ANTES de borrarlo: si no, el shell se queda con un cwd
# inexistente y todo lo que venga despues -- incluidos los subshells de xargs --
# arranca escupiendo errores de getcwd.
cd "$HOME" || exit 2
rm -rf $probe

run_block() {
    local name=$1 script=$2
    echo ""
    echo "===================================================================="
    echo "== $name  ($(date '+%F %T'))"
    echo "===================================================================="
    bash "$HOME/$script" "$NS3" "$JOBS"
    echo "== $name terminado ($(date '+%F %T')) =="
}

run_block "E1/E2 -- ranking de metricas e interaccion MAC x ruteo (2880)" run_e1e2.sh
run_block "E3 -- vida util (240)" run_e3.sh
run_block "E6 -- flooding como linea de referencia (320)" run_e6.sh

echo ""
echo "== RERUN_ALL_DONE ($(date '+%F %T')) =="
for f in e1e2/e1e2_results.csv e3/e3_results.csv e6/e6_results.csv; do
    p=$HOME/ns3-runs/$f
    [ -f "$p" ] && echo "  $f: $(( $(wc -l < "$p") - 1 )) filas"
done
