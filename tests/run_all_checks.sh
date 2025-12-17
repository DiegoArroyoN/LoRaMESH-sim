#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PYTHON=${PYTHON:-python3}
RESULTS_DIR="${ROOT_DIR}/tests/results"
mkdir -p "${RESULTS_DIR}"

function clean_metrics() {
  rm -f mesh_dv_metrics_*.csv mac_duty.csv adr_off_phy.csv adr_on_phy.csv
}

function run_and_copy() {
  local label="$1"
  shift
  clean_metrics
  ./ns3 run "$@"
  # Persist current metrics with a label
  for f in mesh_dv_metrics_tx.csv mesh_dv_metrics_rx.csv mesh_dv_metrics_routes.csv mesh_dv_metrics_duty.csv adr_off_phy.csv adr_on_phy.csv; do
    if [[ -f "${f}" ]]; then
      cp "${f}" "${RESULTS_DIR}/${label}_${f}"
    fi
  done
}

function run_check() {
  local script="$1"; shift
  if ! ${PYTHON} "${script}" "$@"; then
    code=$?
    if [[ $code -eq 2 ]]; then
      echo "[SKIP] ${script} (missing data)"
      return 0
    fi
    echo "[FAIL] ${script}"
    exit 1
  fi
}

echo "[RUN] Baseline delivery (nEd=5, ADR off, Duty off)"
run_and_copy "baseline" "scratch/LoRaMESH-sim/mesh_dv_baseline --nEd=5 --stopSec=60 --enableAdr=false --enableDuty=false --enablePcap=false"
run_check tests/check_delivery_basic.py --rx "${RESULTS_DIR}/baseline_mesh_dv_metrics_rx.csv" --tx "${RESULTS_DIR}/baseline_mesh_dv_metrics_tx.csv"

echo "[RUN] Multihop scenario (nEd=10, spacing=300, ADR off, Duty off)"
run_and_copy "multihop" "scratch/LoRaMESH-sim/mesh_dv_baseline --nEd=10 --stopSec=120 --spacing=300 --enableAdr=false --enableDuty=false --enablePcap=false"
run_check tests/check_multihop.py --rx "${RESULTS_DIR}/multihop_mesh_dv_metrics_rx.csv"

echo "[RUN] Routing consistency check (nEd=10, ADR off, Duty off)"
run_check tests/check_routing_consistency.py --tx "${RESULTS_DIR}/multihop_mesh_dv_metrics_tx.csv" --routes "${RESULTS_DIR}/multihop_mesh_dv_metrics_routes.csv" --start-time 0

echo "[RUN] ADR comparison (runs saved separately)"
clean_metrics
./ns3 run "scratch/LoRaMESH-sim/mesh_dv_baseline --nEd=5 --stopSec=120 --enableAdr=false --enableDuty=false --enablePcap=false"
[[ -f mesh_dv_metrics_rx.csv ]] && mv mesh_dv_metrics_rx.csv adr_off_phy.csv
clean_metrics
./ns3 run "scratch/LoRaMESH-sim/mesh_dv_baseline --nEd=5 --stopSec=120 --enableAdr=true --enableDuty=false --enablePcap=false"
[[ -f mesh_dv_metrics_rx.csv ]] && mv mesh_dv_metrics_rx.csv adr_on_phy.csv
run_check tests/check_adr_effect.py --no-adr adr_off_phy.csv --adr adr_on_phy.csv

echo "[RUN] Duty cycle scenario (nEd=5, ADR on, Duty on, limit=1%)"
clean_metrics
./ns3 run "scratch/LoRaMESH-sim/mesh_dv_baseline --nEd=5 --stopSec=120 --enableAdr=true --enableDuty=true --dutyLimit=0.01 --enablePcap=false"
run_check tests/check_duty_cycle.py --file mesh_dv_metrics_duty.csv --limit 0.01

echo "[OK] All checks passed (or skipped where data missing)."
