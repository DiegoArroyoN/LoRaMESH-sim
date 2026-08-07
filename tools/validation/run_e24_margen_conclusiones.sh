#!/bin/bash
# run_e24_margen_conclusiones.sh — ¿sobreviven nuestras conclusiones fuera del
# regimen fragil?
#
# E23 (2026-08-03) encontro algo que yo NO habia predicho y que obliga a
# reexaminar todo lo medido. Yo sostuve que "todo lo medido a 177-178 m esta
# limpio porque el enlace tiene +3.16 dB de reserva". Es falso:
#
#     separacion 177 m, rango SF7-8:   margen 0 dB -> PDR 0.1867
#                                      margen 1 dB -> PDR 0.4249  (x2.3)
#                                      margen 6 dB -> PDR 0.5033
#     separacion 177 m, rango SF7-12:  margen 0 dB -> PDR 0.1611
#                                      margen 1 dB -> PDR 0.4014  (x2.5)
#
# O sea que sfLinkMarginDb=0 no solo crea una banda muerta en 240-251 m: penaliza
# el PDR MAS DEL DOBLE en nuestro propio punto de operacion. Y todas las
# campañas -- E14, E16, E17, E18, E19, E20, E21, E22 -- corren con margen 0.
#
# LO QUE ESTO PONE EN DUDA, Y LO QUE NO. Es un sesgo de NIVEL. Nuestras
# conclusiones son comparaciones PAREADAS (compuesta contra toa_only, delta
# contra delta=0, cadencia contra cadencia) y los dos brazos comparten margen,
# asi que el sesgo se cancela en primer orden. Lo que NO se puede dar por
# supuesto es que los efectos RELATIVOS sean iguales cuando el enlace deja de
# operar al borde del precipicio: un enlace con 0.1 dB de reserva se rompe con
# cualquier colision, y eso puede amplificar o enmascarar el efecto de la
# cadencia y el de la metrica.
#
# QUE MIDE ESTA CAMPAÑA. Repite los DOS contrastes que sostienen el paper, con
# margen 0 y margen 1 dB, todo lo demas identico a E20:
#
#   contraste A: cadencia de balizas 60 -> 900 s   (nuestro resultado principal:
#                +229% de PDR y +8.1% de FND a margen 0)
#   contraste B: compuesta frente a toa_only        (+4.8% PDR / -13.2% FND a
#                margen 0, tras sanear la cadencia)
#
# La pregunta no es si los niveles cambian -- van a cambiar -- sino si cambian
# las CONCLUSIONES.
#
# PREDICCION registrada antes de mirar: el contraste A (cadencia) sobrevivira y
# probablemente se atenue, porque parte de su enorme efecto viene de que con
# margen 0 las colisiones destruyen enlaces que ya estaban al borde; al dar
# reserva, cada colision perdida duele menos. El contraste B (metrica) es el que
# mas riesgo corre: si la ventaja de la compuesta venia de elegir SF mas bajos en
# enlaces marginales, con margen el selector sube igual y la ventaja se diluye.
# Si B se invierte o desaparece, hay que rehacer el punto de operacion.
#
#   bash run_e24_margen_conclusiones.sh [ns3_dir] [jobs]
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
JOBS=${2:-14}
SEEDS=${SEEDS:-10}
export SEEDS
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/e24_margen
mkdir -p "$OUT/cells"
CSV="$OUT/e24_margen_conclusiones.csv"
HDR="margen_db,baliza_s,cfg,nEd,seed,rc,pdr,fnd_s,t50_s,soc_min,e_mean,air_baliza,air_dato,frac_baliza,hops,sf_medio"

celda() {
  local mg=$1 bcn=$2 cfg=$3 n=$4 seed=$5
  local id="m${mg}_b${bcn}_${cfg}_n${n}_s${seed}"
  local row="$OUT/cells/$id.row"
  [ -s "$row" ] && return 0

  local mf
  if [ "$cfg" = "toaref" ]; then
      mf="--allowMetricModeOverride=true --routeMetricMode=toa_only"
  else
      mf="--routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 --compositeWEnergy=0"
  fi
  local bf=""
  [ "$bcn" != "60" ] && bf="--allowBeaconOverride=true --beaconIntervalWarmSec=$bcn --beaconIntervalStableSec=$bcn"

  local d="$OUT/w/$id"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # Identico a E20 salvo el margen, que es el factor nuevo.
  # shellcheck disable=SC2086
  timeout 5400 "$BIN" --profile=proposal_pueyo_like_csmacad --nEd="$n" --stopSec=300000 --rngRun="$seed" \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --pueyoGridSpacingM=178 $bf \
      --allowSfMarginOverride=true --sfLinkMarginDb="$mg" \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel=static --enablePcap=false --verboseLogs=false \
      $mf > run.log 2>&1
  local rc=$?

  MG=$mg B=$bcn C=$cfg N=$n SEED=$seed RC=$rc ROW="$row" python3 - <<'PY'
import csv, json, os, statistics as st
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
tot=[]
try:
    for r in csv.DictReader(open("mesh_dv_metrics_energy_breakdown.csv")):
        tot.append(float(r["txMah"])+float(r["rxMah"])+float(r["cadMah"])+float(r["idleMah"]))
except Exception: pass
smin=""
try:
    fr=sorted(float(r["energyFrac"]) for r in csv.DictReader(open("mesh_dv_metrics_energy.csv")))
    if fr: smin="%.6f"%fr[0]
except Exception: pass
ab=ad=0.0; ssum=hops=dn=0
try:
    for r in csv.DictReader(open("mesh_dv_metrics_tx.csv")):
        try: t=float(r["toaUs"]); sf=int(r["sf"])
        except Exception: continue
        if r.get("dst")=="65535": ab+=t
        else:
            ad+=t; dn+=1; ssum+=sf
            try: hops+=int(r["hops"])
            except Exception: pass
except Exception: pass
tt=ab+ad
f4=lambda x:"%.4f"%x
open(e["ROW"],"w").write(",".join(str(x) for x in [
    e["MG"],e["B"],e["C"],e["N"],e["SEED"],e["RC"],pdr,fnd,t50,smin,
    f4(st.mean(tot)) if tot else "", f4(ab/1e6), f4(ad/1e6),
    f4(ab/tt) if tt else "", f4(hops/dn) if dn else "", f4(ssum/dn) if dn else ""])+"\n")
PY
  [ -s mesh_dv_effective_config.csv ] && echo "$id,$(tail -1 mesh_dv_effective_config.csv)" > "$OUT/cells/$id.cfg"
  cd "$OUT" && rm -rf "$d"
}
export -f celda
export BIN LD_LIBRARY_PATH OUT

[ -f "$CSV" ] || echo "$HDR" > "$CSV"

# PRECONDICION: la puerta del margen tiene que funcionar CON ESTE PERFIL. E23 la
# probo con pueyo2024_paper_like; aqui el perfil es proposal_pueyo_like_csmacad y
# no se puede dar por supuesto que la restauracion ocurra igual.
echo "== ¿muerde el margen con el perfil de la propuesta? =="
pk=/tmp/e24chk_$$; rm -rf $pk; prev=""
for mg in 0 1; do
  d=$pk/$mg; mkdir -p "$d"; cd "$d" || exit 2
  "$BIN" --profile=proposal_pueyo_like_csmacad --nEd=25 --stopSec=30000 --rngRun=1 \
      --allowDutyOverride=true --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all \
      --pueyoGridSpacingM=178 --allowSfMarginOverride=true --sfLinkMarginDb=$mg \
      --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 \
      --shadowingModel=static --enablePcap=false --verboseLogs=false \
      --routeMetricMode=composite_score --compositeWToa=0.85 --compositeWHop=0.15 \
      --compositeWEnergy=0 > run.log 2>&1 || {
      echo "  ABORTA: margen=$mg no corre con este perfil (rc=$?)."; rm -rf $pk; exit 5; }
  v=$(python3 -c "
import csv,json
c={}
for r in csv.DictReader(open('mesh_dv_metrics_tx.csv')):
    if r.get('dst')!='65535':
        try: c[int(r['sf'])]=c.get(int(r['sf']),0)+1
        except Exception: pass
print('%s pdr=%.4f' % (dict(sorted(c.items())), json.load(open('mesh_dv_summary.json'))['pdr']['pdr']))" 2>/dev/null)
  echo "  margen=$mg dB -> $v"
  [ -n "$prev" ] && [ "$prev" = "$v" ] && {
      echo "  ABORTA: identico con 0 y 1 dB. La puerta no muerde con este perfil."; rm -rf $pk; exit 4; }
  prev=$v
done
cd "$HOME" || exit 2; rm -rf $pk
echo "  distintos: se puede medir."

for mg in 0 1; do
  for bcn in 60 300 900 1800; do
    for cfg in comp toaref; do
      for n in 25 49; do
        for seed in $(seq 1 "$SEEDS"); do echo "$mg $bcn $cfg $n $seed"; done
      done
    done
  done
done | xargs -P "$JOBS" -L1 bash -c 'celda $0 $1 $2 $3 $4'

cat "$OUT"/cells/*.row 2>/dev/null | sort >> "$CSV"
cat "$OUT"/cells/*.cfg 2>/dev/null | sort > "${CSV%.csv}_config.csv"
echo "== E24_DONE: $(( $(wc -l < "$CSV") - 1 )) filas =="
