#!/bin/bash
# run_e29_normalizacion.sh — cuanto aire vale un salto, y por que hasta hoy no lo
# sabiamos.
#
# ===========================================================================
# EL PROBLEMA, EN UNA LINEA
# ===========================================================================
# Nuestra metrica compuesta dice que un salto extra cuesta 1.49 SEGUNDOS de
# aire, o sea 23 transmisiones SF7 de 20 B. Nadie decidio eso: sale de una
# constante de normalizacion que nunca examinamos.
#
# ===========================================================================
# DE DONDE SALE ESE 1.49 s
# ===========================================================================
# El coste de enlace es   alfa * (ToA / C) + beta   con C = 8 462 336 us, que es
# el ToA de SF12 con payload maximo. Multiplicando por C/alfa la formula se
# vuelve legible:
#
#     coste' = ToA + (beta/alfa) * C        -> el salto vale (beta/alfa)*C
#
# Con alfa=0.85 y beta=0.15 eso son 1.493 s. Pero nuestros datos son de 20 B en
# SF7, que ocupan 64.8 ms: 23 veces menos. Por eso el termino de ToA aporta solo
# el 4.2% del coste de enlace en SF7 y el 7.4% en SF8, y beta se lleva el resto.
#
# CONSECUENCIA MEDIBLE, no teorica. La metrica prefiere UN salto SF12 antes que
# DOS saltos SF7, aunque el SF12 cueste 1581 ms de aire y los dos saltos SF7 solo
# 130 ms -- doce veces mas aire. Y ya se ve en los datos: E7 midio que la
# compuesta usa SF medio 8.55 frente al 7.00 de toa_only, con CINCO VECES menos
# saltos. Estabamos atribuyendo al PHY una tension que en parte creamos nosotros.
#
# ===========================================================================
# Y HAY UN SEGUNDO DEFECTO, PEOR, QUE SALIO AL MIRAR ESTO
# ===========================================================================
# El coste se cuantiza en pasos de 0.025 para caber en el byte del score. Con los
# pesos actuales:
#
#     enlace SF7 -> coste 0.15651 -> 6.260 pasos -> se anuncia como 6
#     enlace SF8 -> coste 0.16198 -> 6.479 pasos -> se anuncia como 6
#
# SF7 y SF8 SE ANUNCIAN CON EL MISMO VALOR. El termino de ToA no es que pese
# poco: es que no cabe en la rejilla y se redondea a nada al re-anunciar la ruta.
# La diferencia SF7-SF8 es 0.0055, un 22% de un solo paso.
#
# ===========================================================================
# LA TENSION QUE ESTO DESTAPA, Y QUE ES PUBLICABLE
# ===========================================================================
# El paso tiene que ser PEQUEÑO para distinguir SF, y GRANDE para que un camino
# largo quepa en las 255 unidades del byte. Las dos condiciones se pelean:
#
#     (A) paso <= 0.0055           para que SF7 y SF8 no colisionen
#     (B) 255*paso >= coste del camino mas largo
#
# Con beta=0.15 no hay ningun paso que cumpla las dos con caminos de 14 saltos.
# Bajando beta el coste por salto baja y las dos caben. O sea que **la resolucion
# de la metrica y su alcance estan acoplados por el tamaño del campo**, y eso
# nadie lo ha reportado.
#
# ===========================================================================
# DISEÑO
# ===========================================================================
# Dos factores, cruzados, con el resto del escenario congelado:
#
#   beta (con alfa = 1-beta) -> fija H = (beta/alfa)*C, el aire que vale un salto
#       0.1500  -> H = 1493 ms   <- el nuestro
#       0.0500  -> H =  445 ms
#       0.0150  -> H =  129 ms
#       0.0050  -> H =   43 ms   (comparable a un paquete SF7: 64.8 ms)
#       0.0015  -> H =   13 ms
#
#   paso de cuantizacion
#       0.025   <- el nuestro, no resuelve SF7 vs SF8
#       0.005   <- si lo resuelve
#
# 5 x 2 x 2 tamaños x 20 semillas = 400 celdas, mas 40 de toa_only como
# referencia externa = 440.
#
# RANGO DE SF COMPLETO (7-12) a proposito. Con sfMax=8 la metrica no puede elegir
# SF alto y el efecto que se quiere medir no existe. A 178 m de separacion y con
# SF12 alcanzando 1058 m, un nodo tiene vecinos directos a seis pasos de rejilla:
# ahi si hay una eleccion real entre "pocos saltos caros" y "muchos saltos
# baratos", que es exactamente lo que H decide.
#
# ===========================================================================
# PREDICCIONES registradas antes de mirar
# ===========================================================================
#  1) Con paso 0.025 el SF medio apenas se movera al bajar beta, porque la
#     diferencia entre SF se redondea igual. Con paso 0.005 SI bajara: al abaratar
#     el salto, la metrica deja de forzar SF alto para ahorrar saltos.
#  2) Habra un OPTIMO INTERIOR en PDR. Con H muy grande se malgasta aire en SF
#     alto; con H muy pequeño se encadenan saltos y sube el relevo y las
#     colisiones. Si no aparece optimo y el PDR es monotono, entonces H no es un
#     parametro de diseño sino una direccion, y hay que decirlo asi.
#  3) El brazo (beta=0.15, paso=0.005) es el CONTROL que separa las dos causas:
#     mismo H que el nuestro pero con el ToA ya resoluble. Si ese brazo se parece
#     al actual, el problema es la escala de H; si se parece a los de beta bajo,
#     el problema era la cuantizacion.
#
#   bash run_e29_normalizacion.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-6}
SEEDS=${SEEDS:-20}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e29_normalizacion
mkdir -p "$OUT/cells"
CSV="$OUT/e29_normalizacion.csv"
HDR="beta,paso,h_ms,nEd,seed,rc,pdr,fnd_s,t50_s,sf_medio,hops,sf7,sf8,sf9,sf10,sf11,sf12,air_dato,air_baliza,e_mean"

ESC="--nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178
     --allowBeaconOverride=true --beaconIntervalWarmSec=900 --beaconIntervalStableSec=900"
export ESC

celda() {
  local beta=$1 paso=$2 n=$3 seed=$4
  local id="b${beta}_p${paso}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$beta" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      local alfa; alfa=$(python3 -c "print(f'{1-$beta:.4f}')")
      mf="--routeMetricMode=composite_score --compositeWToa=$alfa --compositeWHop=$beta
          --compositeWEnergy=0 --compositeCostStep=$paso"
  fi
  local hms; hms=$(python3 -c "
b=0.0 if '$beta'=='toaref' else float('$beta')
print(f'{(b/(1-b))*8462336/1000:.0f}' if b>0 else '0')")

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  timeout 5400 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true $ESC --socInitMin=0.75 --socInitMax=0.75 \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel=static --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1
  local rc=$?

  B=$beta P=$paso HMS=$hms N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os, statistics as st, collections
e=os.environ
pdr=""
try: pdr=json.load(open("mesh_dv_summary.json"))["pdr"]["pdr"]
except Exception: pass
fnd=t50=""
try:
    for r in csv.reader(open("mesh_dv_metrics_lifetime.csv")):
        if r and r[0]=="fnd_s": fnd=r[1]
        if r and r[0]=="t50_s": t50=r[1]
except Exception: pass
sfd=collections.Counter(); ab=ad=0.0; ssum=hops=dn=0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: t=float(r["toaUs"])
        except Exception: continue
        if r.get("dst")=="65535": ab+=t; continue
        ad+=t; dn+=1
        try:
            sf=int(r["sf"]); sfd[sf]+=1; ssum+=sf
            hops+=int(r["hops"])
        except Exception: pass
except Exception: pass
tot=[]
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tot.append(float(r["txMah"])+float(r["rxMah"])+float(r["cadMah"])+float(r["idleMah"]))
except Exception: pass
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["B"],e["P"],e["HMS"],e["N"],e["SEED"],e["RC"],pdr,fnd,t50,
    f4(ssum/dn) if dn else "", f4(hops/dn) if dn else "",
    *[sfd.get(k,0) for k in range(7,13)],
    f4(ad/1e6), f4(ab/1e6), f4(st.mean(tot)) if tot else ""])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# PRECONDICION. Los DOS knobs tienen que morder, y por separado. Si solo se
# comprobara uno, el otro podria estar inerte y la campaña concluiria que la
# normalizacion da igual -- justo lo contrario de lo que sabemos. Y el invariante
# no puede ser el PDR, que es lo que la campaña mide a proposito: se usa el
# REPARTO DE SF, que es donde actua el mecanismo.
echo "== ¿muerden beta y el paso, por separado? =="
pk=/tmp/e29chk_$$; rm -rf $pk
sonda() {  # sonda <beta> <paso>
  local d=$pk/$1_$2; rm -rf "$d"; mkdir -p "$d"; cd "$d" || exit 2
  local alfa; alfa=$(python3 -c "print(f'{1-$1:.4f}')")
  # shellcheck disable=SC2086
  timeout 3600 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=60000 --rngRun=1 \
      --allowDutyOverride=true $ESC --socInitMin=0.75 --socInitMax=0.75 \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel=static --enablePcap=false --verboseLogs=false \
      --routeMetricMode=composite_score --compositeWToa="$alfa" --compositeWHop="$1" \
      --compositeWEnergy=0 --compositeCostStep="$2" > run.log 2>&1 || { echo "ERROR"; return; }
  python3 -c "
import csv, collections, json
c=collections.Counter(); s=n=0
for r in csv.DictReader(open('mesh_dv_metrics_tx.csv')):
    if r.get('dst')!='65535':
        try: sf=int(r['sf']); c[sf]+=1; s+=sf; n+=1
        except Exception: pass
print('sf_medio=%.3f reparto=%s' % (s/n if n else 0, dict(sorted(c.items()))))"
}
a=$(sonda 0.15 0.025); b=$(sonda 0.0015 0.025); c=$(sonda 0.15 0.005)
echo "  beta=0.15   paso=0.025 (actual) -> $a"
echo "  beta=0.0015 paso=0.025          -> $b"
echo "  beta=0.15   paso=0.005          -> $c"
[ "$a" = "$b" ] && [ "$a" = "$c" ] && {
    echo "  ABORTA: los tres identicos. Ni beta ni el paso mueven el reparto de SF."
    rm -rf $pk; exit 4; }
cd "$HOME" || exit 2; rm -rf $pk
echo "  al menos uno muerde: se puede medir."

for beta in 0.1500 0.0500 0.0150 0.0050 0.0015; do
  for paso in 0.025 0.005; do
    for n in 25 49; do
      for seed in $(seq 1 "$SEEDS"); do echo "$beta $paso $n $seed"; done
    done
  done
done > /tmp/e29_lista_$$
# referencia externa: toa_only, que no tiene ni pesos ni paso
for n in 25 49; do
  for seed in $(seq 1 "$SEEDS"); do echo "toaref 0.025 $n $seed"; done
done >> /tmp/e29_lista_$$
xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3' < /tmp/e29_lista_$$
rm -f /tmp/e29_lista_$$

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E29_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
