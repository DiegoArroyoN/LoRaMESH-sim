#!/bin/bash
# audit_all_params.sh — ¿de verdad se aplica cada parametro que exponemos?
#
# El binario acepta 147 parametros por linea de comandos. La proteccion que
# tenemos (el bloque que aborta cuando el perfil descarta un flag pedido) SOLO
# mira los 36 que estan en snapshotProfileForced(): para el resto, el bucle
# hace `continue` y nunca los compara. O sea que 111 parametros pueden aceptarse
# por linea de comandos y no llegar a ninguna parte sin que nada lo delate.
#
# Es la misma familia de fallo que sfMax=8: un valor que la campaña cree estar
# fijando y la simulacion no usa. Aquel costo tres meses y entre 19% y 24% de
# PDR. Este script lo busca por fuerza bruta: perturba cada parametro, uno a
# uno, y mira si la simulacion cambia.
#
# Tres señales, porque "no cambio nada" tiene varias causas distintas y no
# conviene confundirlas:
#
#   fisica   md5 de tx/rx/energia/retardo/rutas/resumen
#   config   md5 de mesh_dv_effective_config.csv
#   ficheros nombres y tamaños de la salida
#
# y de ahi la clasificacion:
#
#   MUERDE       la fisica cambia -> el parametro llega al motor
#   SOLO-CONFIG  la config cambia y la fisica no -> se aplica pero no muerde en
#                este regimen (p.ej. delta fuera de la rampa de Psi)
#   SOLO-SALIDA  solo cambian los ficheros -> controla la instrumentacion
#   ABORTA-OK    el perfil lo descarta y el binario aborta -> proteccion buena
#   RECHAZA      valor invalido, no concluyente (hay que refinar la perturbacion)
#   INERTE       no cambia NADA -> sospechoso, es la clase peligrosa
#
#   bash audit_all_params.sh [ns3_dir] [jobs] [stopSec]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-6}
STOP=${3:-20000}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/audit_params
mkdir -p "$OUT/cells"
TAB="$HOME/params_probe.tsv"
SNAP="$HOME/params_snapshot.txt"
export BIN LD_LIBRARY_PATH OUT STOP SNAP

[ -s "$TAB" ]  || { echo "falta $TAB";  exit 2; }
[ -s "$SNAP" ] || { echo "falta $SNAP"; exit 2; }

# La linea base es la de las campañas de verdad (E14/E15), no una inventada:
# la pregunta util no es "¿este parametro muerde en algun sitio?" sino "¿muerde
# en la configuracion en la que corremos el paper?".
BASEFLAGS="--profile=proposal_pueyo_like_csmacad --nEd=16 --rngRun=1
  --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all
  --pueyoGridSpacingM=178 --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400
  --shadowingModel=static --enablePcap=false --verboseLogs=false
  --routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15
  --compositeWEnergy=0"
export BASEFLAGS

huella() {  # huella <directorio> -> "fisica config ficheros"
  local d=$1
  local f c s
  f=$(cat "$d"/mesh_dv_metrics_tx.csv "$d"/mesh_dv_metrics_rx.csv \
          "$d"/mesh_dv_metrics_energy.csv "$d"/mesh_dv_metrics_energy_breakdown.csv \
          "$d"/mesh_dv_metrics_delay.csv "$d"/mesh_dv_metrics_routes.csv \
          "$d"/mesh_dv_metrics_routes_used.csv "$d"/mesh_dv_metrics_duty.csv \
          "$d"/mesh_dv_metrics_lifetime.csv "$d"/mesh_dv_metrics_overhead.csv \
          "$d"/mesh_dv_summary.json 2>/dev/null | md5sum | cut -c1-12)
  c=$(md5sum < "$d"/mesh_dv_effective_config.csv 2>/dev/null | cut -c1-12)
  s=$(cd "$d" && ls -la *.csv *.json 2>/dev/null | awk '{print $5,$9}' | md5sum | cut -c1-12)
  echo "$f $c $s"
}
export -f huella

# --- linea base -------------------------------------------------------------
BASED=$OUT/_base
rm -rf "$BASED"; mkdir -p "$BASED"; cd "$BASED" || exit 2
# shellcheck disable=SC2086
$BIN $BASEFLAGS --stopSec=$STOP > run.log 2>&1
rc=$?
[ $rc -eq 0 ] || { echo "la linea base falla (rc=$rc):"; tail -5 run.log; exit 3; }
read -r BF BC BS <<<"$(huella "$BASED")"
export BF BC BS
echo "linea base: fisica=$BF config=$BC ficheros=$BS  (stopSec=$STOP)"
echo ""

celda() {
  # IFS por defecto: quien nos llama lo cambia para trocear la fila, y si se
  # hereda aqui entonces $BASEFLAGS deja de partirse en palabras y el binario
  # recibe un unico argumento gigante (aborta con rc=134 en los 147 casos).
  local IFS=$' \t\n'
  local name=$1 kind=$2 dflt=$3 alt=$4
  local row="$OUT/cells/$name.row"
  [ -s "$row" ] && return 0

  # Puerta de override cuando el parametro la exige, para separar "el perfil lo
  # protege" (que es correcto) de "no llega al motor" (que no lo es).
  local gate=""
  case "$name" in
    beaconIntervalStableSec|beaconIntervalWarmSec) gate="--allowBeaconOverride=true" ;;
    dataPayloadSizeBytes)      gate="--allowPayloadOverride=true" ;;
    enableNs3EnergyFramework)  gate="--allowEnergyFwOverride=true" ;;
    interferenceModel)         gate="--allowInterferenceModelOverride=true" ;;
    pueyoPacketsPerPair)       gate="--allowPacketsPerPairOverride=true" ;;
    routeMetricMode)           gate="--allowMetricModeOverride=true" ;;
    sfMax|sfMin)               gate="--allowPaperLikeSfRangeVariant=true" ;;
    shadowingSigmaDb)          gate="--allowShadowOverride=true" ;;
  esac

  local d="$OUT/work/$name"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # Guarda de tiempo. Sin ella, un parametro que cuelgue el motor bloquea la
  # auditoria entera y se pierde en silencio: --studySuperframeEnable=true
  # estuvo 4h12m en una corrida que tarda 6s, y el resultado fue que la tabla
  # salio con 146 de 147 sin que nada dijera cual faltaba ni por que.
  # Un cuelgue es un hallazgo, no una espera.
  # shellcheck disable=SC2086
  timeout "${TMO:-120}" $BIN $BASEFLAGS --stopSec=$STOP $gate --"$name"="$alt" > run.log 2>&1
  local rc=$?

  local veredicto detalle=""
  if [ $rc -eq 124 ]; then
      veredicto=COLGADO
      detalle="no termino en ${TMO:-120}s (la corrida de referencia tarda ~6s)"
  elif [ $rc -ne 0 ]; then
      if grep -qi "descarta .* flag" run.log; then
          veredicto=ABORTA-OK
          detalle=$(grep -oi -- "--$name (pedido [^)]*)" run.log | head -1)
      else
          veredicto=RECHAZA
          detalle=$(grep -oiE "(msg=|assert failed|Error:)[^\"]{0,90}" run.log | head -1 | tr ',' ' ')
      fi
  else
      read -r f c s <<<"$(huella "$d")"
      if   [ "$f" != "$BF" ]; then veredicto=MUERDE
      elif [ "$c" != "$BC" ]; then veredicto=SOLO-CONFIG
      elif [ "$s" != "$BS" ]; then veredicto=SOLO-SALIDA
      else                         veredicto=INERTE
      fi
  fi

  local ensnap=no
  grep -qx "$name" "$SNAP" && ensnap=si
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$name" "$kind" "$dflt" "$alt" "$ensnap" "$rc" "$veredicto" "$detalle" > "$row"
  cd "$OUT" && rm -rf "$d"
}
export -f celda

awk -F'\t' 'NF>=4 {print $1"\t"$2"\t"$3"\t"$4}' "$TAB" |
  while IFS=$'\t' read -r n k d a; do
    [ -z "${a:-}" ] && continue          # las cadenas sin alternativa van aparte
    printf '%s\t%s\t%s\t%s\n' "$n" "$k" "$d" "$a"
  done | tr '\t' '\037' |
  xargs -P "$JOBS" -I{} bash -c 'IFS=$(printf "\037"); set -- $1; celda "$1" "$2" "$3" "$4"' _ {}

RES="$OUT/audit_params.tsv"
{ printf 'parametro\ttipo\tdefecto\tprobado\ten_snapshot\trc\tveredicto\tdetalle\n'
  cat "$OUT"/cells/*.row 2>/dev/null | sort; } > "$RES"
echo "== AUDIT_DONE: $(( $(wc -l < "$RES") - 1 )) parametros =="
awk -F'\t' 'NR>1{c[$7]++} END{for(k in c) printf "  %-12s %d\n", k, c[k]}' "$RES" | sort -k2 -rn
