#!/bin/bash
# assert_config.sh — un runner declara lo que cree estar midiendo, y el binario
# lo confirma antes de gastar la campaña.
#
# El defecto mas caro de julio de 2026 fue `sfMax=8` en los cinco perfiles:
# truncaba el alcance a 350 m, costaba entre un 19% y un 24% de PDR, aplastaba
# la diferencia entre metricas por un factor diez, e invalido cuatro campañas.
# Sobrevivio tres meses. No porque el dato faltara -- `sf_max` estaba en el JSON
# de cada corrida con el valor 8 -- sino porque nadie lo leyo, y el runner
# reducia cada corrida a una fila de resultados y borraba el resto.
#
# La leccion no es "guardar mas": es que **nadie estaba comparando la
# configuracion efectiva contra la intencion declarada**. Eso es lo que hace
# este script. Se invoca desde el preflight de un runner con las parejas
# clave=valor que la campaña afirma medir, corre una sonda corta con los mismos
# flags, y aborta si algo no cuadra. Un sfMax equivocado pasa de invisible
# durante tres meses a un aborto en la primera celda.
#
# Uso:
#   assert_config.sh <bin> <workdir> "<flags de la corrida>" clave=valor ...
#
# Ejemplo:
#   assert_config.sh "$BIN" /tmp/chk "--profile=X --nEd=25 ..." \
#       sfmax=12 shadow=static metric=composite_score
#
# Claves disponibles: las que emite mesh_dv_effective_config.csv (git, profile,
# metric, alpha, beta, delta, wsum, coststep, costenc, sfmin, sfmax, sflinkmode,
# shadow, sigma, pathloss, periodsec, trafficload, trafficmode, placement,
# spacing, pktsperpair, duty, hyst, hystdelta, socmin, socmax, socbimodal,
# wire, mac, flooding).
#
# Los numericos se comparan con tolerancia, para que 12 case con "12" y 0.15 con
# "0.150000": exigir igualdad textual daria falsos abortos y el guardian acabaria
# desactivado, que es peor que no tenerlo.
set -u
BIN=$1; WORK=$2; FLAGS=$3; shift 3

rm -rf "$WORK"; mkdir -p "$WORK"
cd "$WORK" || { echo "assert_config: no puedo entrar en $WORK"; exit 2; }

# shellcheck disable=SC2086
"$BIN" $FLAGS > run.log 2>&1
rc=$?
if [ ! -s mesh_dv_effective_config.csv ]; then
    echo "assert_config: ABORTA -- el binario no emitio mesh_dv_effective_config.csv (rc=$rc)."
    echo "  Binario sin la emision de configuracion efectiva, o la corrida murio:"
    grep -m1 -E "aborted|msg=" run.log 2>/dev/null | cut -c1-240
    exit 3
fi

ESPERADO="$*"
ESPERADO="$ESPERADO" python3 - <<'PY'
import csv, os, sys

with open("mesh_dv_effective_config.csv") as fh:
    filas = list(csv.reader(fh))
if len(filas) < 2:
    print("assert_config: ABORTA -- fichero de configuracion malformado.")
    sys.exit(3)
efectiva = dict(zip(filas[0], filas[1]))

esperado = [p for p in os.environ.get("ESPERADO", "").split() if "=" in p]
if not esperado:
    print("assert_config: ABORTA -- no se declaro ninguna expectativa.")
    print("  Un runner sin expectativas declaradas es un runner sin auditar.")
    sys.exit(3)

fallos, ok = [], []
for par in esperado:
    clave, quiero = par.split("=", 1)
    if clave not in efectiva:
        fallos.append("  %-14s clave inexistente (revisar el nombre)" % clave)
        continue
    hay = efectiva[clave]
    try:
        # Tolerancia numerica: 12 == "12", 0.15 == "0.150000".
        igual = abs(float(hay) - float(quiero)) < 1e-6
    except ValueError:
        igual = (hay.strip() == quiero.strip())
    if igual:
        ok.append("  %-14s %s" % (clave, hay))
    else:
        fallos.append("  %-14s declarado '%s' pero efectivo '%s'" % (clave, quiero, hay))

if fallos:
    print("assert_config: ABORTA -- la configuracion efectiva no coincide con lo declarado:")
    print("\n".join(fallos))
    print("")
    print("  La campaña dice medir una cosa y mediria otra. Corregir el runner o")
    print("  el perfil ANTES de lanzar, no despues de analizar los resultados.")
    sys.exit(1)

print("assert_config: %d expectativas confirmadas" % len(ok))
print("\n".join(ok))
PY
