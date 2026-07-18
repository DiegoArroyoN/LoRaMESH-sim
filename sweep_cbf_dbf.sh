#!/bin/bash
# sweep_cbf_dbf.sh — explore (cbf, dbf) on the new csmacad_free_backoff profile
# at N=25 high (the scenario where Fase 1/2 showed the largest CSMA effect).
# 5 combos x 3 seeds = 15 runs.
set -euo pipefail

REPO="/home/diego/sim/LoRaMESH-sim-frozen-20260327"
BIN="$REPO/build/scratch/LoRaMESH-sim/ns3-dev-mesh_dv_baseline-default"
OUTBASE="/home/diego/ns3-runs/sweep_cbf_dbf"
JOBS=12
STOP_SEC=2100

mkdir -p "$OUTBASE"
LOGFILE="$OUTBASE/sweep_$(date +%Y%m%d-%H%M%S).log"
echo "[$(date)] sweep_cbf_dbf starting (15 runs)" | tee -a "$LOGFILE"

BASE_ARGS=(
  --stopSec="$STOP_SEC"
  --nodePlacementMode=pueyo_grid
  --pueyoGridSpacingM=177
  --pueyoGridSide=5
  --wireFormat=pueyo7b
  --trafficMode=pueyo_all_to_all
  --sfMin=7 --sfMax=8 --sfControl=8
  --autoDataStartSec=true
  --enablePcap=false
  --enableMetricsEssentialOnly=true
  --verboseLogs=false
  --profile=csmacad_free_backoff
  --nEd=25
  --trafficLoad=high
)

# (cbf, dbf) combos
COMBOS=(
  "1.0 10.0"   # Pueyo control point (should match pueyo2024_paper_like_csmacad)
  "1.0 5.0"    # less aggressive data backoff
  "1.0 1.0"    # symmetric, no asymmetry
  "1.0 20.0"   # more aggressive data backoff
  "3.0 10.0"   # more aggressive control backoff (data prio = control prio)
)

fifo=$(mktemp -u); mkfifo "$fifo"; exec 3<>"$fifo"; rm "$fifo"
for ((i=0; i<JOBS; i++)); do printf 'x' >&3; done

run_one() {
  local outdir="$1"; shift
  mkdir -p "$outdir"
  if [ -s "$outdir/mesh_dv_summary.json" ]; then
    echo "[SKIP] $outdir" >> "$LOGFILE"; return 0
  fi
  local s; s=$(date +%s)
  ( cd "$outdir" && "$BIN" "${BASE_ARGS[@]}" "$@" > run.log 2>&1 )
  local rc=$? e=$(( $(date +%s) - s ))
  if [ "$rc" -ne 0 ] || [ ! -s "$outdir/mesh_dv_summary.json" ]; then
    touch "$outdir/FAILED"
    echo "[FAIL rc=$rc t=${e}s] $outdir" >> "$LOGFILE"
    return 1
  fi
  echo "[OK t=${e}s] $outdir" >> "$LOGFILE"
}

for combo in "${COMBOS[@]}"; do
  read -r cbf dbf <<< "$combo"
  tag="cbf${cbf//./_}_dbf${dbf//./_}"
  for seed in 1 2 3; do
    outdir="$OUTBASE/${tag}/seed$(printf '%02d' $seed)"
    read -r -n1 -u3
    (
      run_one "$outdir" \
        --controlBackoffFactor="$cbf" \
        --dataBackoffFactor="$dbf" \
        --rngRun="$seed" || true
      printf 'x' >&3
    ) &
  done
done
wait

echo "[$(date)] sweep complete" | tee -a "$LOGFILE"
done_n=$(find "$OUTBASE" -name mesh_dv_summary.json | wc -l)
fail_n=$(find "$OUTBASE" -name FAILED 2>/dev/null | wc -l)
echo "[summary] done=$done_n failed=$fail_n / 15" | tee -a "$LOGFILE"
