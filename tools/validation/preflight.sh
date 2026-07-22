#!/bin/bash
# preflight.sh -- lo que hay que pasar ANTES de lanzar una campana.
#
# Las campanas corren en un arbol optimizado, donde NS_ASSERT no existe. Eso
# dejo pasar durante meses un aserto de BasicEnergySource que aborta en cuanto
# un nodo se queda sin bateria: en optimizado la carga se hundia por debajo de
# cero en silencio (VALIDATION.md, 2026-07-22). Esta rutina corre con asertos
# activos y toca los caminos que una campana larga acaba tocando, pero en
# segundos.
#
#   ./tools/validation/preflight.sh [ruta-al-arbol-ns3]
#
# Devuelve 0 solo si todo pasa.

set -u
NS3=${1:-$HOME/ns3/ns-3-dev}
cd "$NS3" || { echo "no existe el arbol ns-3: $NS3"; exit 2; }

BIN=$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default
export LD_LIBRARY_PATH=$NS3/build/lib
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
fails=0

echo "== arbol: $NS3"
grep -E 'NS3_ASSERT|CMAKE_BUILD_TYPE' cmake-cache/CMakeCache.txt 2>/dev/null | sed 's/^/   /'
if ! grep -q 'NS3_ASSERT:BOOL=ON' cmake-cache/CMakeCache.txt 2>/dev/null; then
    echo "   AVISO: los asertos estan desactivados; esta rutina no sirve de nada asi."
    fails=$((fails + 1))
fi

echo
echo "== 1. build"
if ./ns3 build dv-cl dv-cl-campaign-example > "$WORK/build.log" 2>&1; then
    echo "   OK"
else
    echo "   FALLO -- ultimas lineas:"
    grep -E 'error:' "$WORK/build.log" | head -5 | sed 's/^/      /'
    exit 1
fi

echo
echo "== 2. suites"
for s in dv-cl dv-cl-wire dv-cl-routing dv-cl-mac dv-cl-energy dv-cl-region \
         dv-cl-conservation dv-cl-convergence dv-cl-aloha dv-cl-lorawan-equiv; do
    r=$(./test.py -s "$s" 2>&1 | tr -d '\033' | grep -oE 'PASS|FAIL|CRASH' | head -1)
    printf '   %-24s %s\n' "$s" "${r:-NORUN}"
    [ "$r" = PASS ] || fails=$((fails + 1))
done

echo
echo "== 3. cada perfil arranca (corrida corta, asertos activos)"
for p in pueyo2024_paper_like pueyo2024 pueyo2024_paper_like_csmacad \
         csmacad_free_backoff proposal_pueyo_like proposal_pueyo_like_aloha \
         proposal_pueyo_like_csmacad proposal_pueyo_like_observed extended; do
    d=$WORK/$p; mkdir -p "$d"
    (cd "$d" && "$BIN" --profile="$p" --nEd=9 --stopSec=300 \
        --enablePcap=false --verboseLogs=false > run.log 2>&1) 2>/dev/null
    rc=$?
    printf '   %-32s rc=%d %s\n' "$p" "$rc" "$([ $rc -eq 0 ] || echo '<-- FALLO')"
    [ $rc -eq 0 ] || { fails=$((fails + 1)); grep -iE 'assert|msg=' "$d/run.log" | head -2 | sed 's/^/      /'; }
done

echo
echo "== 4. agotamiento de bateria (el camino que abortaba)"
# Bateria casi vacia al arrancar: los nodos mueren en segundos en vez de en
# 300 ks, y se recorre el codigo de depletion sin pagar una corrida larga.
d=$WORK/depletion; mkdir -p "$d"
(cd "$d" && "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=9 --stopSec=20000 \
    --socInitMin=0.02 --socInitMax=0.03 \
    --enablePcap=false --verboseLogs=false > run.log 2>&1)
rc=$?
fnd=$(grep -E '^fnd_s' "$d/mesh_dv_metrics_lifetime.csv" 2>/dev/null | cut -d, -f2)
printf '   rc=%d  fnd_s=%s\n' "$rc" "${fnd:-N/A}"
[ $rc -eq 0 ] || { fails=$((fails + 1)); grep -iE 'assert|msg=' "$d/run.log" | head -3 | sed 's/^/      /'; }
# Si nadie muere, el caso no se ejercita y la rutina da falsa tranquilidad.
if [ -z "${fnd:-}" ] || [ "${fnd:-−1}" = "-1" ]; then
    echo "      FALLO: ningun nodo murio; el camino de agotamiento no se recorrio"
    fails=$((fails + 1))
fi

echo
echo "== 5. el perfil no descarta flags en silencio"
# Un flag que el perfil pisa debe abortar nombrandolo, no colarse.
d=$WORK/flag; mkdir -p "$d"
(cd "$d" && "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=9 --stopSec=200 \
    --txPowerDbm=8 --enablePcap=false --verboseLogs=false > run.log 2>&1) 2>/dev/null
if grep -q 'descarta' "$d/run.log"; then
    echo "   OK (aborta y nombra el flag)"
else
    echo "   FALLO: --txPowerDbm=8 se acepto en un perfil que lo fija en 20"
    fails=$((fails + 1))
fi

echo
if [ $fails -eq 0 ]; then
    echo "PREFLIGHT OK"
else
    echo "PREFLIGHT: $fails fallo(s)"
fi
exit $((fails > 0))
