#!/bin/bash
# check_axis_levers.sh — ¿que flag mueve de verdad cada eje de la matriz?
#
# La auditoria de los 147 parametros devolvio INERTE para --enableDuty y para
# --enableCsma, que son DOS DE LOS EJES de la matriz factorial. Los perfiles
# escriben cfg.enableCsma y cfg.enableDutyCycle DESPUES de leer la linea de
# comandos, y ninguno de los dos esta en snapshotProfileForced(), asi que el
# bloque que aborta cuando el perfil descarta un flag ni los mira.
#
# Consecuencia: una campaña puede pedir --enableDuty=false, correr con duty
# puesto, y no dejar rastro de la discrepancia en ningun sitio. Es exactamente
# la familia de sfMax=8.
#
# Aqui se comprueba, para cada eje, cual es la palanca que funciona y cual es
# la que engaña. Sale a un TSV para poder citarlo.
set -u
NS3=${1:-$HOME/ns346/ns-3-dev}
B="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
OUT=$HOME/ns3-runs/axis_levers
rm -rf "$OUT"; mkdir -p "$OUT"
RES="$OUT/axis_levers.tsv"

BASE="--nEd=16 --rngRun=1 --stopSec=20000 --nodePlacementMode=pueyo_grid
  --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=178
  --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400
  --shadowingModel=static --enablePcap=false --verboseLogs=false"

printf 'eje\tcaso\trc\tduty_on\tduty_limite\tmac\tsf_max\tpdr\thuella\n' > "$RES"

probar() {  # probar <eje> <etiqueta> <flags...>
  local eje=$1 tag=$2; shift 2
  local d="$OUT/w"; rm -rf "$d"; mkdir -p "$d"; cd "$d" || return 1
  # shellcheck disable=SC2086
  $B $BASE "$@" > run.log 2>&1
  local rc=$?
  local vals pdr h
  vals=$(python3 - <<'PY' 2>/dev/null
import csv
try:
    r = list(csv.DictReader(open("mesh_dv_effective_config.csv")))[0]
except Exception:
    print("?\t?\t?\t?"); raise SystemExit
def g(*ks):
    for k in ks:
        if k in r: return r[k]
    return "?"
print("\t".join([g("duty_enabled","dutyEnabled","duty"),
                 g("duty_limit","dutyLimit"),
                 g("mac"),
                 g("sf_max","sfMax")]))
PY
)
  [ -z "$vals" ] && vals=$'?\t?\t?\t?'
  pdr=$(python3 -c 'import json;print(json.load(open("mesh_dv_summary.json"))["pdr"]["pdr"])' 2>/dev/null)
  # huella de fisica: si dos casos la comparten, el flag no cambio nada
  h=$(cat mesh_dv_metrics_tx.csv mesh_dv_metrics_rx.csv mesh_dv_metrics_energy.csv \
          mesh_dv_summary.json 2>/dev/null | md5sum | cut -c1-12)
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$eje" "$tag" "$rc" "$vals" "${pdr:-}" "$h" >> "$RES"
  cd "$OUT" && rm -rf "$d"
}

probar duty "sin allowDutyOverride"            --profile=proposal_pueyo_like_csmacad
probar duty "con allowDutyOverride"            --profile=proposal_pueyo_like_csmacad --allowDutyOverride=true
probar duty "allowDuty + enableDuty=false"     --profile=proposal_pueyo_like_csmacad --allowDutyOverride=true --enableDuty=false
probar duty "allowDuty + dutyLimit=0.05"       --profile=proposal_pueyo_like_csmacad --allowDutyOverride=true --dutyLimit=0.05
probar duty "allowDuty + dutyOverridePct=5"    --profile=proposal_pueyo_like_csmacad --allowDutyOverride=true --dutyOverridePct=5

probar mac  "perfil csmacad"                   --profile=proposal_pueyo_like_csmacad --allowDutyOverride=true
probar mac  "perfil aloha"                     --profile=proposal_pueyo_like_aloha   --allowDutyOverride=true
probar mac  "csmacad + enableCsma=false"       --profile=proposal_pueyo_like_csmacad --allowDutyOverride=true --enableCsma=false

probar sf   "rango por defecto"                --profile=proposal_pueyo_like_csmacad --allowDutyOverride=true
probar sf   "sfMax=8 sin la puerta"            --profile=proposal_pueyo_like_csmacad --allowDutyOverride=true --sfMax=8
probar sf   "sfMax=8 con la puerta"            --profile=proposal_pueyo_like_csmacad --allowDutyOverride=true --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8

echo "== LEVERS_DONE =="
column -t -s$'\t' "$RES"
echo ""
echo "== huellas repetidas dentro de cada eje (flag que NO cambio nada) =="
awk -F'\t' 'NR>1{k=$1"|"$9; if(k in seen) printf "  %-5s \"%s\" es identico a \"%s\"\n",$1,$2,seen[k]; else seen[k]=$2}' "$RES"
