#!/bin/bash
# chain_e9_after_e8.sh — espera a E8, RECOMPILA, y lanza el barrido de carga.
#
# El paso de recompilar no es adorno: E9 necesita --dataPeriodSec, que no
# existe en el binario con el que corre E8. Recompilar mientras E8 esta vivo
# cambiaria el binario a mitad de campaña -- las celdas ya lanzadas conservan su
# inode, pero las siguientes cogerian el nuevo -- y E8 quedaria siendo una
# mezcla de dos arboles. Por eso el orden es: esperar, sincronizar, compilar,
# comprobar, correr.
#
# Espera por ausencia del proceso y no por el marcador del log: si E8 aborta a
# medias nunca escribe E8_DONE y la espera seria eterna. El marcador se
# comprueba despues para decidir si seguir.
#
#   bash chain_e9_after_e8.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
REPO=$HOME/sim/LoRaMESH-sim-override-20260530
BUNDLE=$HOME/sim/bundles/dvcl-load.bundle
me=$$

echo "== esperando a que termine E8 ($(date '+%F %T')) =="
while pgrep -f "run_e8_delta_regime.sh" | grep -qv "^$me$"; do
    sleep 120
done
echo "== E8 ya no corre ($(date '+%F %T')) =="

if ! grep -q "E8_DONE" "$HOME/e8.log" 2>/dev/null; then
    echo "NO LANZO E9: E8 termino sin escribir E8_DONE."
    tail -15 "$HOME/e8.log" 2>/dev/null
    exit 1
fi
echo "  E8: $(ls $HOME/ns3-runs/e8/cells 2>/dev/null | wc -l) celdas"

echo ""
echo "== sincronizando y recompilando ($(date '+%F %T')) =="
if [ ! -f "$BUNDLE" ]; then
    echo "ABORTA: falta $BUNDLE. Hay que subirlo antes."
    exit 2
fi
cd "$REPO" || exit 2
git fetch "$BUNDLE" dvcl-module-scaffold >/dev/null 2>&1 || { echo "ABORTA: fetch fallo"; exit 2; }
git reset --hard FETCH_HEAD >/dev/null 2>&1 || { echo "ABORTA: reset fallo"; exit 2; }
echo "  HEAD: $(git log --oneline -1)"
rsync -rc --delete contrib/dv-cl/ "$NS3/contrib/dv-cl/"
cd "$NS3" || exit 2
find contrib/dv-cl \( -name '*.cc' -o -name '*.h' \) -exec touch {} +
./ns3 build dv-cl dv-cl-campaign-example 2>&1 | grep -E "error:|Linking CXX exec" | tail -3

echo ""
echo "===================================================================="
echo "== E9 -- barrido de carga ($(date '+%F %T'))"
echo "===================================================================="
bash "$HOME/run_e9_load.sh" "$NS3" "$JOBS"
echo "== CHAIN_E9_DONE ($(date '+%F %T')) =="
