#!/bin/bash
# chain_e7_after_rerun.sh — espera a que terminen E1/E2, E3 y E6, y entonces
# lanza el barrido de espaciados.
#
# Encadenar en el servidor y no desde fuera: asi el barrido arranca aunque la
# sesion que lo programo ya no exista, y no se solapa con las campañas en curso
# peleandose por los nucleos.
#
# Espera por AUSENCIA DEL PROCESO y no por el marcador del log, porque si el
# re-lanzamiento aborta a medias nunca escribe RERUN_ALL_DONE y esto se quedaria
# esperando para siempre. Al terminar comprueba el marcador para decidir si
# sigue adelante o se planta.
#
#   bash chain_e7_after_rerun.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
LOG=$HOME/rerun_all.log
me=$$

echo "== esperando a que termine el re-lanzamiento ($(date '+%F %T')) =="
while pgrep -f "run_rerun_all.sh" | grep -qv "^$me$"; do
    sleep 120
done
echo "== el re-lanzamiento ya no corre ($(date '+%F %T')) =="

if ! grep -q "RERUN_ALL_DONE" "$LOG" 2>/dev/null; then
    echo "NO LANZO E7: el re-lanzamiento termino sin escribir RERUN_ALL_DONE."
    echo "Ultimas lineas de $LOG:"
    tail -15 "$LOG" 2>/dev/null
    exit 1
fi

for f in e1e2/e1e2_results.csv e3/e3_results.csv e6/e6_results.csv; do
    p=$HOME/ns3-runs/$f
    [ -f "$p" ] && echo "  $f: $(( $(wc -l < "$p") - 1 )) filas"
done

echo ""
echo "===================================================================="
echo "== E7 -- los cuatro espaciados de Pueyo-Centelles ($(date '+%F %T'))"
echo "===================================================================="
bash "$HOME/run_e7_spacing.sh" "$NS3" "$JOBS"
echo "== CHAIN_E7_DONE ($(date '+%F %T')) =="
