#!/bin/bash
# validate_margin3.sh — cross-validation of cadSenseMarginDb=3 dB
# across all (N, load) scenarios (2026-04-24).
#
# Hypothesis: Fase 2C showed margin=3 dB gives +37.7% PDR vs default 6 dB at
# N=25 high. Need to verify this generalises before changing the default.
#
# Decision criteria:
#   C-A: PDR(margin=3) >= PDR(margin=6) in N=9 high AND N=49 high
#   C-B: |delta_PDR| < 15% across all low-load scenarios
#   C-C: (drop_max_csma_retries + drop_queue_overflow) / generated < 30% in all high
# Baseline for comparison: /home/diego/ns3-runs/validate_csmacad/fase1/pueyo2024_paper_like_csmacad/

set -euo pipefail

REPO="/home/diego/sim/LoRaMESH-sim-frozen-20260327"
BIN="$REPO/build/scratch/LoRaMESH-sim/ns3-dev-mesh_dv_baseline-default"
OUTBASE="/home/diego/ns3-runs/validate_margin3"
JOBS=12
STOP_SEC=2100

[ -x "$BIN" ] || { echo "[FATAL] binary not found: $BIN"; exit 1; }
mkdir -p "$OUTBASE"
LOGFILE="$OUTBASE/validate_$(date +%Y%m%d-%H%M%S).log"
echo "[$(date)] validate_margin3 starting (18 runs total)" | tee -a "$LOGFILE"

# Common base args (matches validate_csmacad.sh exactly)
BASE_ARGS=(
  --stopSec="$STOP_SEC"
  --nodePlacementMode=pueyo_grid
  --pueyoGridSpacingM=177
  --wireFormat=pueyo7b
  --trafficMode=pueyo_all_to_all
  --sfMin=7 --sfMax=8 --sfControl=8
  --autoDataStartSec=true
  --enablePcap=false
  --enableMetricsEssentialOnly=true
  --verboseLogs=false
)

side_of() {
  case "$1" in
    9)  echo 3 ;;  16) echo 4 ;;  25) echo 5 ;;
    36) echo 6 ;;  49) echo 7 ;;  64) echo 8 ;;
    *)  echo "[FATAL] unknown N=$1" >&2; exit 1 ;;
  esac
}

# FD-based semaphore for parallel jobs
fifo=$(mktemp -u)
mkfifo "$fifo"; exec 3<>"$fifo"; rm "$fifo"
for ((i=0; i<JOBS; i++)); do printf 'x' >&3; done

acquire() { read -r -n1 -u3; }
release() { printf 'x' >&3; }

run_one() {
  local outdir="$1"; shift
  mkdir -p "$outdir"
  if [ -s "$outdir/mesh_dv_summary.json" ]; then
    echo "[SKIP] $outdir" >> "$LOGFILE"
    return 0
  fi
  local start_ts; start_ts=$(date +%s)
  (
    cd "$outdir"
    "$BIN" "${BASE_ARGS[@]}" "$@" > run.log 2>&1
  )
  local rc=$?
  local elapsed=$(( $(date +%s) - start_ts ))
  if [ "$rc" -ne 0 ] || [ ! -s "$outdir/mesh_dv_summary.json" ]; then
    touch "$outdir/FAILED"
    echo "[FAIL rc=$rc t=${elapsed}s] $outdir" >> "$LOGFILE"
    return 1
  fi
  echo "[OK t=${elapsed}s] $outdir" >> "$LOGFILE"
  return 0
}

# Launch in cost-descending order: N=49 high first, smallest last.
total=0
for N in 49 25 9; do
  side=$(side_of "$N")
  for load in high low; do
    for seed in 1 2 3; do
      outdir="$OUTBASE/N$(printf '%02d' $N)/${load}/seed$(printf '%02d' $seed)"
      total=$((total+1))
      acquire
      (
        run_one "$outdir" \
          --profile=pueyo2024_paper_like_csmacad \
          --nEd="$N" \
          --pueyoGridSide="$side" \
          --trafficLoad="$load" \
          --cadSenseMarginDb=3 \
          --rngRun="$seed" || true
        release
      ) &
    done
  done
done
wait

echo "[$(date)] validate_margin3 complete, launched=$total" | tee -a "$LOGFILE"

# Quick verdict summary
done_n=$(find "$OUTBASE" -name mesh_dv_summary.json | wc -l)
fail_n=$(find "$OUTBASE" -name FAILED | wc -l)
echo "[summary] done=$done_n failed=$fail_n total_planned=18" | tee -a "$LOGFILE"
