#!/bin/bash
# cola_e14_e16.sh — espera a que cierren E27 y E28, y entonces relanza E14 y E16
# con el margen de SF a 1 dB.
#
# POR QUE HAY COLA Y NO SE LANZA A LA VEZ. El servidor tiene 16 nucleos y ya
# vimos en E26 que con 25 procesos y celdas de N grande el OOM killer se lleva
# celdas por delante (tres murieron con rc=137). Añadir dos campañas encima de
# las dos que corren repetiria el problema.
#
# POR QUE SE RELANZAN. E14 y E16 corrieron a sfLinkMarginDb=0, que era un defecto
# nuestro: el selector declaraba viable un enlace a 0.02 dB del umbral de SF7 (la
# diagonal de la rejilla a 177 m cae a 250.3 m, con alcance teorico de 251.0 m) y
# el receptor lo mataba despues. Decision de Diego el 2026-08-07: los resultados
# de margen 0 no se analizan.
#
#   E14 -- presupuesto energetico por actividad. Es el Acto 1 del paper: de aqui
#          salen "reposo 45.7%, TX 33.2%, RX 19.2%" y sobre todo el "el ruteo
#          gobierna el 1.61%" que da titulo al manuscrito. 320 celdas.
#   E16 -- frontera alfa/beta con delta=0. Es lo que justifica los pesos que se
#          reportan. La tanda de margen 0 dio una frontera PLANA (spread <=2.2%)
#          y de ahi salio la recomendacion 85/15, ahora retractada. Con las
#          diagonales sanas hay mas caminos que anunciar, asi que el termino de
#          saltos tiene mas de donde elegir y la frontera podria dejar de ser
#          plana. 480 celdas.
#
# SUF=_m1 manda los resultados a directorios NUEVOS (e14_static_m1,
# e16_static_m1). Sin eso, el salto de celdas existentes haria que las campañas
# "terminaran" en segundos sin simular nada, reutilizando las filas de margen 0
# -- exactamente el fallo que ya nos paso con E7/E5 el 29-jul, cuando reporte
# como completas unas celdas cacheadas.
#
# Los dos runners exigen ahora sfmargin=1 en assert_config, asi que si el binario
# no trajera el defecto nuevo la campaña aborta antes de gastar una celda en vez
# de producir 800 filas silenciosamente invalidas.
#
#   bash cola_e14_e16.sh
set -u
LOG=$HOME/cola_e14_e16.log
: > "$LOG"
echo "[$(date +%H:%M)] cola arrancada; esperando a E27 y E28" >> "$LOG"

for i in $(seq 1 2000); do
  e27=$(grep -c E27_DONE "$HOME/e27.log" 2>/dev/null || echo 0)
  e28=$(grep -c E28_DONE "$HOME/e28.log" 2>/dev/null || echo 0)
  if [ "$e27" -ge 1 ] && [ "$e28" -ge 1 ]; then
    echo "[$(date +%H:%M)] E27 y E28 cerradas; arranco E14 y E16" >> "$LOG"
    break
  fi
  # guarda: si no queda ni un proceso de simulacion durante dos vueltas
  # seguidas, las campañas murieron sin escribir su DONE y no tiene sentido
  # esperar tres dias.
  vivos=$(ps -eo cmd | grep -c "[d]v-cl-campaign-example")
  if [ "$vivos" -eq 0 ]; then
    sleep 180
    vivos=$(ps -eo cmd | grep -c "[d]v-cl-campaign-example")
    if [ "$vivos" -eq 0 ]; then
      echo "[$(date +%H:%M)] AVISO: sin procesos vivos y sin DONE (E27=$e27 E28=$e28)." >> "$LOG"
      echo "  Arranco igual: las celdas ya hechas se saltan solas." >> "$LOG"
      break
    fi
  fi
  sleep 120
done

cd "$HOME" || exit 1
export SUF=_m1

echo "[$(date +%H:%M)] === E14 (presupuesto energetico, 320 celdas) ===" >> "$LOG"
SUF=_m1 bash "$HOME/run_e14_energia.sh" "" 6 > "$HOME/e14_m1.log" 2>&1
echo "[$(date +%H:%M)] E14 rc=$?  $(tail -1 "$HOME/e14_m1.log")" >> "$LOG"

echo "[$(date +%H:%M)] === E16 (frontera alfa/beta, 480 celdas) ===" >> "$LOG"
SUF=_m1 bash "$HOME/run_e16_frontera_malla.sh" "" 6 > "$HOME/e16_m1.log" 2>&1
echo "[$(date +%H:%M)] E16 rc=$?  $(tail -1 "$HOME/e16_m1.log")" >> "$LOG"

echo "[$(date +%H:%M)] COLA_DONE" >> "$LOG"
