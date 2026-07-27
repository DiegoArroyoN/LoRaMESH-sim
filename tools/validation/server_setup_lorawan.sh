#!/bin/bash
# server_setup_lorawan.sh — deja el arbol ns-3 del servidor listo para dv-cl.
#
# Contexto (2026-07-24): el arbol de build del servidor (ns346/ns-3-dev, ns-3.46)
# no traia el modulo lorawan del que dv-cl depende. La copia de lorawan de la
# version 3.46 tiene un unico defecto de higiene incompatible con la guarda
# NS3_MODULE_COMPILATION que ese ns-3 aplica: src/lorawan/model/network-scheduler.h
# incluye el agregador ns3/core-module.h. dv-cl no usa el network-server de
# LoRaWAN; sustituimos ese include por los headers especificos. (En WSL el
# defecto queda latente porque el objeto de lorawan esta cacheado de antes de que
# la guarda existiera; un build limpio en WSL fallaria igual.)
#
# Uso:  bash server_setup_lorawan.sh /ruta/al/lorawan.tgz  [/ruta/al/ns3]
#   lorawan.tgz: tar del modulo lorawan 3.46 (tar czf lorawan.tgz -C ns-3-dev/src lorawan)
#   ns3: arbol destino (default ~/ns346/ns-3-dev)
set -e
TGZ=${1:?falta el tar de lorawan}
NS3=${2:-$HOME/ns346/ns-3-dev}

echo "== instalar lorawan 3.46 en $NS3/src =="
rm -rf "$NS3/src/lorawan"
tar xzf "$TGZ" -C "$NS3/src/"

f="$NS3/src/lorawan/model/network-scheduler.h"
if grep -q 'ns3/core-module.h' "$f"; then
    sed -i 's#\#include "ns3/core-module.h"#\#include "ns3/nstime.h"\n\#include "ns3/simulator.h"#' "$f"
    echo "== parche aplicado a network-scheduler.h =="
fi

# Parche 2: el receptor sabe en que SF demodulo (event->GetSpreadingFactor())
# pero no lo deja en el LoraTag. SimpleEndDeviceLoraPhy::Send si etiqueta el SF
# al transmitir; SimpleGatewayLoraPhy::Send no, y nuestros nodos usan el de
# gateway. Resultado: el SF no llegaba a las capas superiores.
g="$NS3/src/lorawan/model/simple-gateway-lora-phy.cc"
if ! grep -q 'tag.SetSpreadingFactor(event->GetSpreadingFactor())' "$g"; then
    sed -i 's#^\( *\)tag.SetFrequency(event->GetFrequency());#\1tag.SetFrequency(event->GetFrequency());\n\1tag.SetSpreadingFactor(event->GetSpreadingFactor());#' "$g"
    echo "== parche del SF en el tag aplicado =="
fi

# lorawan es una dependencia, no objeto de estudio: sus ejemplos y tests traen
# mas includes de agregador que la guarda de ns-3.46 rechaza y romperian el
# build completo que test.py fuerza. Los quitamos -> lorawan library-only.
L="$NS3/src/lorawan"
sed -i '/^  TEST_SOURCES$/d; /^    test\/.*\.cc$/d' "$L/CMakeLists.txt"
: > "$L/examples/CMakeLists.txt"
echo "== lorawan dejado library-only (sin examples ni tests) =="

cd "$NS3"
./ns3 configure --enable-tests --enable-examples -d default 2>&1 | tail -2
./ns3 build dv-cl dv-cl-campaign-example 2>&1 | grep -E 'error:|Linking CXX (shared|exec)|no work' | tail -6
echo "== setup lorawan OK =="
