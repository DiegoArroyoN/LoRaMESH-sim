#!/bin/bash
# run_all_two_arms.sh — las campañas completas en los dos brazos de canal.
#
#   CHAN=none    canal determinista, la condicion de Pueyo-Centelles (su paper
#                no modela sombreado ni desvanecimiento rapido). Es el brazo
#                COMPARABLE: ahi el protocolo de referencia opera en las
#                condiciones para las que fue diseñado.
#   CHAN=static  sombreado log-normal sigma=3.57 dB sorteado por enlace y
#                mantenido. Es el brazo REALISTA.
#
# 20 semillas en LOS DOS brazos. La idea inicial era bajar a 10 en el
# determinista suponiendo que sin sombreado la dispersion entre semillas caeria,
# pero la sonda del 2026-07-28 lo desmiente: cae en el PDR de vida util (0.20x)
# y NO cae en rendimiento a N=49 (1.14x, o sea peor) ni en FND (0.90x). Lo que
# domina la dispersion a N alto son las colisiones, que siguen ahi. Recortar
# habria degradado justo las celdas donde el determinismo no ayuda.
#
# Orden: primero las de 40 ks, que dan resultados usables antes, y luego las de
# 300 ks. Dentro de cada bloque, el brazo determinista primero: es el que se
# compara contra la referencia, asi que es el que mas urge tener.
#
#   bash run_all_two_arms.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-12}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
SEEDS=20

echo "== binario: $(ls -la "$BIN" | awk '{print $6,$7,$8}') =="

# Las tres cosas que han costado campañas enteras este mes se comprueban por
# COMPORTAMIENTO y no por presencia de flag, porque un flag aceptado y luego
# pisado por el perfil es exactamente el fallo que se quiere evitar.
echo "== comprobaciones previas =="
for f in shadowingModel dataPeriodSec routeSwitchHysteresis; do
    "$BIN" --PrintHelp 2>&1 | grep -q -- "$f" || { echo "ABORTA: falta --$f"; exit 3; }
done
echo "  flags presentes"

probe=/tmp/twoarm_probe; rm -rf $probe; mkdir -p $probe
for m in none static; do
  d=$probe/$m; mkdir -p "$d"; cd "$d" || exit 2
  "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=12000 --rngRun=3 \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --shadowingModel=$m --enablePcap=false --verboseLogs=false > run.log 2>&1
done
chk=$(cd $probe && python3 -c "
import csv, collections, json
def sfh(p):
    c=collections.Counter()
    try:
        for r in csv.DictReader(open(p+'/mesh_dv_metrics_tx.csv')):
            if r.get('dst')!='65535':
                try: c[int(r['sf'])]+=1
                except Exception: pass
    except Exception: return {}
    return dict(sorted(c.items()))
def pdr(p):
    try: return float(json.load(open(p+'/mesh_dv_summary.json'))['pdr']['pdr'])
    except Exception: return -1.0
a,b = sfh('none'), sfh('static')
pa,pb = pdr('none'), pdr('static')
print('  none  : SF %s  PDR=%.4f' % (a, pa))
print('  static: SF %s  PDR=%.4f' % (b, pb))
# El rango tiene que llegar por encima de SF8 -- si no, seguimos con la red
# truncada -- y los dos brazos tienen que DIFERIR, o el flag no muerde.
ok = max(a or {0:0}) > 8 and max(b or {0:0}) > 8 and abs(pa-pb) > 1e-9
print('OK' if ok else 'MAL')
" 2>/dev/null)
echo "$chk"
cd "$HOME" || exit 2
rm -rf $probe
if ! echo "$chk" | grep -q "^OK"; then
    echo "ABORTA: o el rango de SF sigue truncado en 8, o los dos brazos de canal dan lo mismo."
    exit 4
fi

bloque() {
    local nombre=$1 script=$2 chan=$3
    echo ""
    echo "===================================================================="
    echo "== $nombre  [canal=$chan]  ($(date '+%F %T'))"
    echo "===================================================================="
    CHAN=$chan SEEDS=$SEEDS bash "$HOME/$script" "$NS3" "$JOBS"
    echo "== $nombre [$chan] terminado ($(date '+%F %T')) =="
}

for chan in none static; do
    bloque "E1/E2 ranking de metricas e interaccion MAC x ruteo" run_e1e2.sh "$chan"
done
for chan in none static; do
    bloque "E6 flooding como linea de referencia" run_e6.sh "$chan"
done
for chan in none static; do
    bloque "E9 barrido de carga" run_e9_load.sh "$chan"
done
for chan in none static; do
    bloque "E3 vida util" run_e3.sh "$chan"
done
for chan in none static; do
    bloque "E7 los cuatro espaciados" run_e7_spacing.sh "$chan"
done

echo ""
echo "== TWO_ARMS_DONE ($(date '+%F %T')) =="
for chan in none static; do
  for f in e1e2_$chan/e1e2_results.csv e6_$chan/e6_results.csv e9_$chan/e9_load.csv \
           e3_$chan/e3_results.csv e7_$chan/e7_spacing.csv; do
    p=$HOME/ns3-runs/$f
    [ -f "$p" ] && echo "  $f: $(( $(wc -l < "$p") - 1 )) filas"
  done
done
