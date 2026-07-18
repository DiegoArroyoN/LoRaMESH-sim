#!/bin/bash
# validate_csmacad.sh — 4-fase validation of CSMA/CAD MAC (2026-04-24).
# Usage: bash validate_csmacad.sh [fase1|fase2a|fase2b|fase2c|fase3d|all]
#
# Fase 1: ALOHA vs CSMA comparative (36 runs)
# Fase 2A: CsmaMaxRetries sweep (12 runs)
# Fase 2B: CsmaTxQueueMax sweep (12 runs)
# Fase 2C: CadSenseMarginDb sweep (12 runs)
# Fase 3D: Backoff factors sweep (9 runs)

set -euo pipefail

REPO="/home/diego/sim/LoRaMESH-sim-frozen-20260327"
BIN="$REPO/build/scratch/LoRaMESH-sim/ns3-dev-mesh_dv_baseline-default"
OUTBASE="/home/diego/ns3-runs/validate_csmacad"
JOBS=12
STOP_SEC=2100
STAGE="${1:-all}"

[ -x "$BIN" ] || { echo "[FATAL] binary not found: $BIN"; exit 1; }
mkdir -p "$OUTBASE"
LOGFILE="$OUTBASE/validate_$(date +%Y%m%d-%H%M%S).log"
echo "[$(date)] validate_csmacad stage=$STAGE" | tee -a "$LOGFILE"

# Common base args (matches campaign_launch.sh)
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

# Map N -> grid side
side_of() {
  case "$1" in
    9)  echo 3 ;;  16) echo 4 ;;  25) echo 5 ;;
    36) echo 6 ;;  49) echo 7 ;;  64) echo 8 ;;
    *)  echo "[FATAL] unknown N=$1" >&2; exit 1 ;;
  esac
}

# Create FD-based semaphore for parallelism
fifo=$(mktemp -u)
mkfifo "$fifo"; exec 3<>"$fifo"; rm "$fifo"
for ((i=0; i<JOBS; i++)); do printf 'x' >&3; done

acquire() { read -r -n1 -u3; }
release() { printf 'x' >&3; }

run_one() {
  # $1=outdir, shift, rest=extra args
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

# ---------------------------------------------------------------------------
# FASE 1: ALOHA vs CSMA comparative
# 2 profiles × 3 N × 2 loads × 3 seeds = 36 runs
# ---------------------------------------------------------------------------
fase1() {
  echo "[fase1] starting 36 runs" | tee -a "$LOGFILE"
  local total=0
  # Sort by descending cost (N=49 high first) to fill slots optimally.
  for N in 49 25 9; do
    local side; side=$(side_of "$N")
    for load in high low; do
      for seed in 1 2 3; do
        for profile in pueyo2024_paper_like pueyo2024_paper_like_csmacad; do
          local outdir="$OUTBASE/fase1/${profile}/N$(printf '%02d' $N)/${load}/seed$(printf '%02d' $seed)"
          total=$((total+1))
          acquire
          (
            run_one "$outdir" \
              --profile="$profile" \
              --nEd="$N" \
              --pueyoGridSide="$side" \
              --trafficLoad="$load" \
              --rngRun="$seed" || true
            release
          ) &
        done
      done
    done
  done
  wait
  echo "[fase1] launched=$total done" | tee -a "$LOGFILE"
}

# ---------------------------------------------------------------------------
# FASE 2A: CsmaMaxRetries sweep @ N=25 high
# ---------------------------------------------------------------------------
fase2a() {
  echo "[fase2a] starting 12 runs (CsmaMaxRetries sweep)" | tee -a "$LOGFILE"
  for retries in 4 8 16 32; do
    for seed in 1 2 3; do
      local outdir="$OUTBASE/fase2a/retries${retries}/seed$(printf '%02d' $seed)"
      acquire
      (
        run_one "$outdir" \
          --profile=pueyo2024_paper_like_csmacad \
          --nEd=25 --pueyoGridSide=5 --trafficLoad=high \
          --csmaMaxRetries="$retries" \
          --rngRun="$seed" || true
        release
      ) &
    done
  done
  wait
  echo "[fase2a] done" | tee -a "$LOGFILE"
}

# ---------------------------------------------------------------------------
# FASE 2B: CsmaTxQueueMax sweep @ N=25 high
# ---------------------------------------------------------------------------
fase2b() {
  echo "[fase2b] starting 12 runs (CsmaTxQueueMax sweep)" | tee -a "$LOGFILE"
  for qmax in 16 32 64 128; do
    for seed in 1 2 3; do
      local outdir="$OUTBASE/fase2b/qmax${qmax}/seed$(printf '%02d' $seed)"
      acquire
      (
        run_one "$outdir" \
          --profile=pueyo2024_paper_like_csmacad \
          --nEd=25 --pueyoGridSide=5 --trafficLoad=high \
          --csmaTxQueueMax="$qmax" \
          --rngRun="$seed" || true
        release
      ) &
    done
  done
  wait
  echo "[fase2b] done" | tee -a "$LOGFILE"
}

# ---------------------------------------------------------------------------
# FASE 2C: CadSenseMarginDb sweep @ N=25 high
# ---------------------------------------------------------------------------
fase2c() {
  echo "[fase2c] starting 12 runs (CadSenseMarginDb sweep)" | tee -a "$LOGFILE"
  for margin in 3 6 9 12; do
    for seed in 1 2 3; do
      local outdir="$OUTBASE/fase2c/margin${margin}/seed$(printf '%02d' $seed)"
      acquire
      (
        run_one "$outdir" \
          --profile=pueyo2024_paper_like_csmacad \
          --nEd=25 --pueyoGridSide=5 --trafficLoad=high \
          --cadSenseMarginDb="$margin" \
          --rngRun="$seed" || true
        release
      ) &
    done
  done
  wait
  echo "[fase2c] done" | tee -a "$LOGFILE"
}

# ---------------------------------------------------------------------------
# FASE 3D: Backoff factors sweep @ N=25 high
# ---------------------------------------------------------------------------
fase3d() {
  echo "[fase3d] starting 9 runs (backoff factors)" | tee -a "$LOGFILE"
  for combo in "0.5_1.0" "1.0_1.0" "0.25_2.0"; do
    local cbf="${combo%_*}"
    local dbf="${combo#*_}"
    for seed in 1 2 3; do
      local outdir="$OUTBASE/fase3d/cbf${cbf}_dbf${dbf}/seed$(printf '%02d' $seed)"
      acquire
      (
        run_one "$outdir" \
          --profile=pueyo2024_paper_like_csmacad \
          --nEd=25 --pueyoGridSide=5 --trafficLoad=high \
          --controlBackoffFactor="$cbf" \
          --dataBackoffFactor="$dbf" \
          --rngRun="$seed" || true
        release
      ) &
    done
  done
  wait
  echo "[fase3d] done" | tee -a "$LOGFILE"
}

case "$STAGE" in
  fase1)  fase1 ;;
  fase2a) fase2a ;;
  fase2b) fase2b ;;
  fase2c) fase2c ;;
  fase3d) fase3d ;;
  all)    fase1; fase2a; fase2b; fase2c; fase3d ;;
  *)      echo "Usage: $0 [fase1|fase2a|fase2b|fase2c|fase3d|all]"; exit 1 ;;
esac

echo "[$(date)] validate_csmacad stage=$STAGE finished" | tee -a "$LOGFILE"
done_count=$(find "$OUTBASE" -name 'mesh_dv_summary.json' 2>/dev/null | wc -l)
fail_count=$(find "$OUTBASE" -name 'FAILED' 2>/dev/null | wc -l)
echo "Total summaries: $done_count  Failed: $fail_count"
