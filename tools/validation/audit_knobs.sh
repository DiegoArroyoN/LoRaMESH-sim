#!/bin/bash
# audit_knobs.sh — cada mando del DoE tiene que MORDER, y en la direccion
# esperada.
#
# Por que existe: entre el 26 y el 29 de julio de 2026 aparecieron nueve
# defectos, y los ocho primeros se descubrieron persiguiendo un resultado que no
# cuadraba, nunca por una comprobacion previa. El preflight verificaba que los
# flags EXISTIERAN, no que hicieran algo:
#
#   - el SF de datos estaba clavado en 12 (2716 de 2716 llamadas al fallback)
#   - T_hat saturado a 1.0, o sea la metrica compuesta era conteo de saltos
#   - la histeresis solo se aplicaba a los modos distintos de toa_only, que es
#     la referencia de toda comparacion
#   - los cinco perfiles truncaban el SF a 7-8 pisando el 7-12 de la base
#   - el sombreado se sorteaba por paquete, o sea era fading y no sombreado
#   - CHAN se asignaba sin exportar y --shadowingModel viajaba vacio
#
# Todos comparten la misma forma: un parametro que decia hacer X y hacia Y, sin
# nada que lo comprobara. Esta auditoria cierra esa clase entera. Corre en
# minutos y no consume campaña.
#
# Convencion de salida: PASA / FALLA / OMITE. 'OMITE' es para los mandos que
# solo se pueden auditar si se cumple una precondicion de regimen -- delta es el
# caso: si Psi esta fuera de su rampa, el mando no puede morder y decir 'PASA'
# seria mentir, pero decir 'FALLA' tambien. La distincion importa porque el
# 2026-07-29 se concluyo que delta era inerte a partir de una tabla donde
# soc_min valia 0.0000, o sea con Psi saturada: la medida no decia nada sobre
# delta y se leyo como si dijera.
#
#   bash audit_knobs.sh [ns3_dir]
set -u
NS3=${1:-$HOME/ns3/ns-3-dev}
BIN="$NS3/build/contrib/dv-cl/examples/ns3-dev-dv-cl-campaign-example-default"
export LD_LIBRARY_PATH="$NS3/build/lib"
BASE=$HOME/knob-audit
rm -rf "$BASE"; mkdir -p "$BASE"
PASA=0; FALLA=0; OMITE=0
FALLOS=""

# Nota: NO se usa --enableMetricsEssentialOnly, porque suprime las muestras de
# cuantizacion y son justamente lo que permite auditar los pesos sobre el coste
# calculado en vez de inferirlo del reparto de SF.
COMUN="--profile=proposal_pueyo_like_csmacad --nEd=16 --stopSec=9000 --rngRun=3 \
       --allowDutyOverride=true --enablePcap=false --verboseLogs=false"

run() {  # run <etiqueta> <flags...>
    local tag=$1; shift
    local d="$BASE/$tag"; mkdir -p "$d"; cd "$d" || return 1
    # shellcheck disable=SC2086
    "$BIN" $COMUN "$@" > run.log 2>&1
    echo $?
}

# Extractores. Cada uno devuelve un escalar o cadena vacia si no se pudo leer,
# y NUNCA un valor por defecto plausible: un 0 silencioso aqui haria pasar una
# asercion que en realidad no se pudo evaluar.
val() {  # val <etiqueta> <expresion python sobre j (summary) y filas>
    local tag=$1 expr=$2
    ( cd "$BASE/$tag" 2>/dev/null || exit 1
      TAG="$tag" python3 -c "
import json, csv, sys, collections
try:
    j = json.load(open('mesh_dv_summary.json'))
except Exception:
    sys.exit(1)
def sfhist():
    c = collections.Counter()
    try:
        for r in csv.DictReader(open('mesh_dv_metrics_tx.csv')):
            if r.get('dst') != '65535':
                try: c[int(r['sf'])] += 1
                except Exception: pass
    except Exception: return {}
    return dict(sorted(c.items()))
def sfmean():
    c = sfhist(); n = sum(c.values())
    return sum(k*v for k,v in c.items())/n if n else float('nan')
def sfmax():
    c = sfhist()
    return max(c) if c else float('nan')
def dutymean():
    v = []
    try:
        for r in csv.DictReader(open('mesh_dv_metrics_duty.csv')):
            v.append(float(r['dutyUsed']))
    except Exception: return float('nan')
    return sum(v)/len(v) if v else float('nan')
def socspread():
    return float(j['energy']['max_remaining_frac']) - float(j['energy']['min_remaining_frac'])
print($expr)
" 2>/dev/null )
}

# cmp_dir <nombre> <a> <b> <expresion> <relacion>
#   relacion: '>' exige val(b) > val(a); '!=' exige que difieran
cmp_dir() {
    local nombre=$1 a=$2 b=$3 expr=$4 rel=$5
    local va vb
    va=$(val "$a" "$expr"); vb=$(val "$b" "$expr")
    if [ -z "$va" ] || [ -z "$vb" ]; then
        printf "  %-34s %-6s  no se pudo leer la metrica (a='%s' b='%s')\n" "$nombre" "FALLA" "$va" "$vb"
        FALLA=$((FALLA+1)); FALLOS="$FALLOS\n    - $nombre: metrica ilegible"
        return
    fi
    local ok
    ok=$(python3 -c "
a, b = float('$va'), float('$vb')
rel = '$rel'
if rel == '>':  print('1' if b > a else '0')
elif rel == '<': print('1' if b < a else '0')
else:            print('1' if abs(b-a) > 1e-12 else '0')
" 2>/dev/null)
    if [ "$ok" = "1" ]; then
        printf "  %-34s %-6s  %s %s %s\n" "$nombre" "PASA" "$va" "$rel" "$vb"
        PASA=$((PASA+1))
    else
        printf "  %-34s %-6s  %s %s %s  <-- el mando no muerde\n" "$nombre" "FALLA" "$va" "$rel" "$vb"
        FALLA=$((FALLA+1)); FALLOS="$FALLOS\n    - $nombre: $va vs $vb (se esperaba $rel)"
    fi
}

echo "======================================================================"
echo "AUDITORIA DE MANDOS  ($(date '+%F %T'))"
echo "binario: $BIN"
echo "======================================================================"
echo ""
echo "--- 1. modo de metrica ---"
run met_toa  --allowMetricModeOverride=true --routeMetricMode=toa_only >/dev/null
run met_comp --routeMetricMode=composite_score >/dev/null
run met_rssi --allowMetricModeOverride=true --routeMetricMode=rssi >/dev/null
run met_hops --compositeWToa=0 --compositeWHop=1 --compositeWEnergy=0 >/dev/null
cmp_dir "toa_only vs compuesta (SF medio)"   met_toa met_comp "sfmean()" "!="
cmp_dir "compuesta vs rssi (SF medio)"       met_comp met_rssi "sfmean()" "!="
cmp_dir "compuesta vs hops (SF medio)"       met_comp met_hops "sfmean()" "!="

echo ""
echo "--- 2. rango de SF (el defecto de sfMax=8) ---"
run sf78  --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=8 >/dev/null
run sf712 --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=12 >/dev/null
cmp_dir "ampliar el rango sube el SF max"    sf78 sf712 "sfmax()" ">"

echo ""
echo "--- 3. modelo de canal ---"
run ch_none --shadowingModel=none >/dev/null
run ch_stat --shadowingModel=static >/dev/null
run ch_pkt  --shadowingModel=per_packet >/dev/null
cmp_dir "none vs static (PDR)"               ch_none ch_stat "j['pdr']['pdr']" "!="
cmp_dir "static vs per_packet (PDR)"         ch_stat ch_pkt  "j['pdr']['pdr']" "!="

echo ""
echo "--- 4. caudal de datos ---"
run ld_200 --dataPeriodSec=200 --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 >/dev/null
run ld_20  --dataPeriodSec=20  --allowPacketsPerPairOverride=true --pueyoPacketsPerPair=1400 >/dev/null
cmp_dir "bajar el periodo sube lo generado"  ld_200 ld_20 "float(j['pdr']['total_data_generated'])" ">"

echo ""
echo "--- 5. espaciado de la rejilla ---"
run sp_177 --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=177 >/dev/null
run sp_320 --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all --pueyoGridSpacingM=320 >/dev/null
cmp_dir "separar los nodos sube el SF medio" sp_177 sp_320 "sfmean()" ">"

echo ""
echo "--- 6. duty cycle ---"
run dc_05 --dutyOverridePct=0.5 >/dev/null
run dc_20 --dutyOverridePct=2.0 >/dev/null
cmp_dir "mas duty permite mas airtime"       dc_05 dc_20 "dutymean()" ">"
# duty_blocked_data es un CONTEO, no una tasa: con mas presupuesto el nodo
# admite mucho mas trafico e intenta mucho mas, asi que el conteo absoluto SUBE
# aunque cada intento tenga mas holgura. La primera version de esta auditoria
# comparaba el conteo y 'fallaba' por eso -- el mando mordia, la asercion estaba
# mal. Normalizado por intentos de datos, la direccion es la esperada.
cmp_dir "mas duty bloquea menor FRACCION"   dc_05 dc_20         "-float(j['control_plane']['duty_blocked_data'])/max(1.0,float(j['tx_attempts']['total_data_tx_attempts']))" ">"

echo ""
echo "--- 7. pesos de la metrica compuesta (sobre el coste calculado) ---"
run w_a02 --routeMetricMode=composite_score --compositeWToa=0.2 --compositeWHop=0.15 --compositeWEnergy=0 >/dev/null
run w_a40 --routeMetricMode=composite_score --compositeWToa=4.0 --compositeWHop=0.15 --compositeWEnergy=0 >/dev/null
run w_b00 --routeMetricMode=composite_score --compositeWToa=0.6 --compositeWHop=0.00 --compositeWEnergy=0 >/dev/null
run w_b05 --routeMetricMode=composite_score --compositeWToa=0.6 --compositeWHop=0.50 --compositeWEnergy=0 >/dev/null
cmp_dir "alfa sube el coste bruto medio"     w_a02 w_a40 "float(j['quantization']['metric_raw_mean'])" ">"
cmp_dir "beta sube el coste bruto medio"     w_b00 w_b05 "float(j['quantization']['metric_raw_mean'])" ">"

echo ""
echo "--- 8. delta, solo si Psi esta viva ---"
# La rampa de Psi va de b_lo=0.20 a b_hi=0.50. Fuera de ella Psi es una
# CONSTANTE y delta no puede cambiar ningun orden: la medida no dice nada.
# Se SIEMBRA el regimen en vez de esperarlo: con SoC bimodal en 0.25 y 0.45,
# ambos DENTRO de la rampa (0.20, 0.50), Psi difiere entre vecinos y delta puede
# discriminar en una corrida corta. Eso separa dos preguntas que llevaban dias
# mezcladas: "delta muerde?" es auditable aqui en 9 ks, mientras "delta mejora
# la vida util?" necesita la campaña de 150 ks. Confundirlas es lo que produjo
# la conclusion prematura de que delta era inerte.
PSI="--socInitBimodal=true --socInitMin=0.25 --socInitMax=0.45"
run d_00 --routeMetricMode=composite_score --compositeWToa=0.6 --compositeWHop=0.15 --compositeWEnergy=0.0 $PSI >/dev/null
run d_10 --routeMetricMode=composite_score --compositeWToa=0.6 --compositeWHop=0.15 --compositeWEnergy=1.0 $PSI >/dev/null
smin=$(val d_10 "float(j['energy']['min_remaining_frac'])")
smax=$(val d_10 "float(j['energy']['max_remaining_frac'])")
viva=$(python3 -c "
try:
    lo, hi = float('$smin'), float('$smax')
except Exception:
    print('0'); raise SystemExit
# Psi discrimina si algun nodo cae DENTRO de (0.20, 0.50)
print('1' if lo < 0.50 and hi > 0.20 else '0')
" 2>/dev/null)
if [ "$viva" = "1" ]; then
    cmp_dir "delta sube el coste bruto medio" d_00 d_10 "float(j['quantization']['metric_raw_mean'])" ">"
else
    printf "  %-34s %-6s  SoC en [%s, %s], fuera de la rampa (0.20, 0.50)\n" \
           "delta sobre el coste bruto" "OMITE" "$smin" "$smax"
    echo "       Psi es constante aqui: delta NO PUEDE morder y la medida no"
    echo "       dice nada sobre el. Hace falta un regimen intermedio (~150 ks"
    echo "       con trafico sostenido, o socInitBimodal) para auditarlo."
    OMITE=$((OMITE+1))
fi

echo ""
echo "--- 9. MAC ---"
run mac_csma --profile=proposal_pueyo_like_csmacad >/dev/null
run mac_aloha --profile=proposal_pueyo_like_aloha >/dev/null
cmp_dir "CSMA/CAD frente a ALOHA (PDR)"      mac_aloha mac_csma "j['pdr']['pdr']" "!="

echo ""
echo "--- 10. flooding ---"
run fl_off >/dev/null
run fl_on --floodingMode=true >/dev/null
cmp_dir "flooding cambia los relevos"        fl_off fl_on "float(j['forwarding']['forward_tx_sent_total'])" "!="

echo ""
echo "--- 11. carga inicial de bateria ---"
run soc_fija --socInitMin=1.00 --socInitMax=1.00 >/dev/null
run soc_unif --socInitMin=0.60 --socInitMax=1.00 >/dev/null
cmp_dir "U[60,100] dispersa mas que fija"    soc_fija soc_unif "socspread()" ">"

echo ""
echo "--- 12. histeresis de conmutacion ---"
run hy_off --routeSwitchHysteresis=false >/dev/null
run hy_on  --routeSwitchHysteresis=true >/dev/null
cmp_dir "la histeresis cambia los cambios"   hy_off hy_on "float(j['routes']['update_events'])" "!="

echo ""
echo "--- 13. topologia y trafico ---"
run tp_grid --nodePlacementMode=pueyo_grid --trafficMode=pueyo_all_to_all >/dev/null
run tp_rnd  --nodePlacementMode=random --forcedDataDestinationId=0 >/dev/null
cmp_dir "rejilla frente a aleatoria+sumidero" tp_grid tp_rnd "float(j['pdr']['total_data_generated'])" "!="

echo ""
echo "======================================================================"
printf "RESULTADO: %d pasan, %d fallan, %d omitidas\n" "$PASA" "$FALLA" "$OMITE"
if [ "$FALLA" -gt 0 ]; then
    printf "MANDOS QUE NO MUERDEN:%b\n" "$FALLOS"
    echo "======================================================================"
    exit 1
fi
if [ "$OMITE" -gt 0 ]; then
    echo "Ningun mando falla, pero hay auditorias OMITIDAS por falta de regimen."
    echo "No se pueden dar por buenas: una omitida es un mando SIN auditar."
fi
echo "======================================================================"
