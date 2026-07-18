#!/bin/bash
# smoke_verify.sh — Pre-campaign sanity check
# Runs N=9 grid 177m stopSec=2100 rngRun=1, verifies KPIs + md5 against reference.
# Usage: bash smoke_verify.sh [--skip-md5] [--verbose]
#
# REFERENCE (2026-04-20, fixes B1-B5 + C4 applied):
#   md5 rx.csv  : 84c7b7a72a910c415d7781792c9fa3c4
#   md5 tx.csv  : 2b8f7c8171ce490a9a9f7c5b49ee7e6c
#   PDR         : 0.5345  (938/1755)
#   throughput  : 105.711 bps  (any-hop RX: 1328)
#   goodput     : 74.667 bps
#   delay.avg_s : 0.154526 s  (n=938)

set -euo pipefail

REPO="/home/diego/sim/LoRaMESH-sim-frozen-20260327"
BIN="$REPO/build/scratch/LoRaMESH-sim/ns3-dev-mesh_dv_baseline-default"
WORK="/tmp/smoke_verify_$$"
SKIP_MD5=false
VERBOSE=false

for arg in "$@"; do
  case "$arg" in
    --skip-md5) SKIP_MD5=true ;;
    --verbose)  VERBOSE=true  ;;
  esac
done

REF_MD5_RX="84c7b7a72a910c415d7781792c9fa3c4"
REF_MD5_TX="2b8f7c8171ce490a9a9f7c5b49ee7e6c"
REF_PDR="0.5345"
REF_DELIVERED="938"
REF_GENERATED="1755"
REF_THROUGHPUT="105.711"
REF_ANY_HOP_RX="1328"
REF_GOODPUT="74.667"
REF_DELAY="0.154526"
REF_DELAY_COUNT="938"

PASS=0; FAIL=0

check() {
  local label="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    echo "  [OK]  $label = $got"
    PASS=$((PASS+1))
  else
    echo "  [FAIL] $label: got=$got  want=$want"
    FAIL=$((FAIL+1))
  fi
}

echo "======================================"
echo " smoke_verify.sh  $(date)"
echo "======================================"

[ -x "$BIN" ] || { echo "[FATAL] binary not found: $BIN"; exit 1; }
echo "[OK] binary present"

mkdir -p "$WORK" && cd "$WORK"
echo "Running N=9 grid 177m stopSec=2100 rngRun=1 ..."
"$BIN" \
  --nEd=9 --stopSec=2100 \
  --nodePlacementMode=pueyo_grid --pueyoGridSide=3 --pueyoGridSpacingM=177 \
  --wireFormat=pueyo7b --trafficMode=pueyo_all_to_all \
  --rngRun=1 \
  2>&1 | (if $VERBOSE; then cat; else tail -4; fi)

[ -f mesh_dv_summary.json ] || { echo "[FATAL] no summary JSON produced"; exit 1; }
echo

if ! $SKIP_MD5; then
  echo "--- MD5 ---"
  check "md5_rx.csv" "$(md5sum mesh_dv_metrics_rx.csv | awk '{print $1}')" "$REF_MD5_RX"
  check "md5_tx.csv" "$(md5sum mesh_dv_metrics_tx.csv | awk '{print $1}')" "$REF_MD5_TX"
fi

echo "--- KPIs ---"
python3 - "$REF_PDR" "$REF_DELIVERED" "$REF_GENERATED" \
           "$REF_THROUGHPUT" "$REF_ANY_HOP_RX" "$REF_GOODPUT" \
           "$REF_DELAY" "$REF_DELAY_COUNT" << 'EOFPY'
import json, sys

j   = json.load(open('mesh_dv_summary.json'))
pdr = j['pdr']
t   = j['throughput']
d   = j['delay']

args = sys.argv[1:]
ref = {
    'pdr':           args[0],
    'delivered':     args[1],
    'generated':     args[2],
    'throughput_bps':args[3],
    'data_rx_any_hop':args[4],
    'goodput_bps':   args[5],
    'delay.avg_s':   args[6],
    'delay.count':   args[7],
}
got = {
    'pdr':            str(pdr['pdr']),
    'delivered':      str(pdr['delivered']),
    'generated':      str(pdr['total_data_generated']),
    'throughput_bps': f"{t['throughput_bps']:.3f}",
    'data_rx_any_hop':str(t['data_rx_any_hop_count']),
    'goodput_bps':    f"{t['goodput_bps']:.3f}",
    'delay.avg_s':    f"{d['avg_s']:.6f}",
    'delay.count':    str(d['delivered_count']),
}

ok = fail = 0
for k in ref:
    if got[k] == ref[k]:
        print(f'  [OK]  {k} = {got[k]}')
        ok += 1
    else:
        print(f'  [FAIL] {k}: got={got[k]}  want={ref[k]}')
        fail += 1

print(f'\nKPI: {ok} passed, {fail} failed')
sys.exit(0 if fail == 0 else 1)
EOFPY
KPI_RC=$?

echo
echo "======================================"
if [ "$KPI_RC" -eq 0 ] && [ "$FAIL" -eq 0 ]; then
  echo " RESULT: ALL CHECKS PASSED ✓"
  echo " Safe to launch campaigns."
else
  echo " RESULT: CHECKS FAILED ✗  ($FAIL md5 + KPI failures)"
  echo " DO NOT launch campaigns until resolved."
fi
echo "======================================"

rm -rf "$WORK"
exit $((KPI_RC + FAIL))
