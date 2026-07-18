#!/bin/bash
# campaign_launch.sh — Launch 480 campaign runs for Pueyo 2024 replication
# 4 loads × 2 spacings × 6 N-values × 10 seeds = 480 runs
#
# Usage:
#   bash campaign_launch.sh              # launch with 12 parallel jobs
#   bash campaign_launch.sh --dry-run    # print stopSec table and first 10 jobs, no execution
#   bash campaign_launch.sh --jobs=8     # override parallelism
#
# Resumable: re-run same command to skip already-done runs (checks for mesh_dv_summary.json)
# No GNU parallel required — uses a bash background-job pool.
#
# Calibration-verified timing (April 2026):
#   N=9  low=21s   high=2s
#   N=64 low=30min high=6.7min
#   → Total ~41 CPU-h → ~3.5h wall-clock with 12 jobs
#
# 2026-04-20 — Pueyo-Centelles 2024, IEEE Access

set -uo pipefail

BIN="/home/diego/sim/LoRaMESH-sim-frozen-20260327/build/scratch/LoRaMESH-sim/ns3-dev-mesh_dv_baseline-default"
OUTBASE="/home/diego/ns3-runs/campaigns_aloha_exact"
JOBS=12
DRY_RUN=false

for arg in "$@"; do
  case "$arg" in
    --dry-run)   DRY_RUN=true ;;
    --jobs=*)    JOBS="${arg#--jobs=}" ;;
  esac
done

# ----- sanity checks -----
[ -x "$BIN" ] || { echo "FATAL: binary not found: $BIN"; exit 1; }
which python3 >/dev/null 2>&1 || { echo "FATAL: python3 not found"; exit 1; }
mkdir -p "$OUTBASE"

# ----- campaign axes -----
SPACINGS=(177 248)
LOADS=(low high)
NS=(    9   16   25   36   49   64)
SIDES=( 3    4    5    6    7    8)
SEEDS=(1 2 3 4 5 6 7 8 9 10)
PK_PER_PAIR=100

# pkInterval (seconds) per load level  [paper: Low=100s, Medium=10s, High=1s, Saturation=0.1s]
declare -A PK_INTERVAL
PK_INTERVAL[low]=100
PK_INTERVAL[medium]=10
PK_INTERVAL[high]=1
PK_INTERVAL[saturation]=0.1

# ----- stopSec formula -----
# Sequential per-pair traffic: each source visits (N-1) destinations, PK_PER_PAIR pks each.
# dataStart = Option-B: ceil((60 + max(0, 2*(side-1)-1)*60) * 1.3)
# stopSec   = ceil((dataStart + (N-1)*PK_PER_PAIR*pkInterval) * 1.05)
#
# Calibration cross-check vs known-good cmds:
#   N=64 low  : ceil((1092+63*100*100)*1.05) = 662647  (cmd used 662668, <0.01% off) ✓
#   N=64 high : ceil((1092+63*100*1  )*1.05) = 7707    (cmd used 7887,   ~2.3% off)  ~OK

compute_stop_sec() {
  local N=$1 side=$2 pk_interval=$3
  python3 - <<EOF
import math
N, side, pk_interval = $N, $side, $pk_interval
pk_per_pair, margin = $PK_PER_PAIR, 1.05
beaconWarm = beaconStable = warmup = 60.0
diameter   = 2 * (side - 1)
warmup_hops = int(warmup / beaconWarm)          # = 1
rem_hops    = max(0, diameter - warmup_hops)
conv_sec    = warmup + rem_hops * beaconStable
data_start  = math.ceil(conv_sec * 1.3)
traffic     = (N - 1) * pk_per_pair * pk_interval
stop        = math.ceil((data_start + traffic) * margin)
print(int(stop))
EOF
}

# ----- pre-compute stopSec table -----
declare -A STOPSEC
for i in "${!NS[@]}"; do
  N="${NS[$i]}"; side="${SIDES[$i]}"
  for load in "${LOADS[@]}"; do
    STOPSEC["${N}_${load}"]=$(compute_stop_sec "$N" "$side" "${PK_INTERVAL[$load]}")
  done
done

# ----- header -----
echo "========================================"
echo " Campaign: Pueyo 2024 replication"
echo " $(date)"
echo "========================================"
printf " %-15s: %s\n" "Output base"   "$OUTBASE"
printf " %-15s: %s m\n" "Spacings"    "${SPACINGS[*]}"
printf " %-15s: %s\n" "Loads"         "${LOADS[*]}"
printf " %-15s: %s\n" "N values"      "${NS[*]}"
printf " %-15s: 1..10\n" "Seeds"
printf " %-15s: %s\n" "Parallel jobs" "$JOBS"
echo ""
echo " stopSec per (N, load):"
printf "  %-8s" "N\load"
for load in "${LOADS[@]}"; do printf "  %-10s" "$load"; done
echo ""
for i in "${!NS[@]}"; do
  N="${NS[$i]}"
  printf "  %-8s" "N=$N"
  for load in "${LOADS[@]}"; do
    printf "  %-10s" "${STOPSEC[${N}_${load}]}"
  done
  echo ""
done
echo ""
echo " ⚠  Est. wall-clock: ~3.5h  (N64-low ≈ 30min/run, 12 parallel jobs)"
echo ""

# ----- runner function -----
run_one_job() {
  local spacing="$1" load="$2" N="$3" side="$4" seed="$5" stop_sec="$6" outdir="$7"
  local tag="s${spacing}m_${load}_N${N}_seed${seed}"

  mkdir -p "$outdir"

  # Resume: skip if output already exists and is non-empty
  if [ -s "$outdir/mesh_dv_summary.json" ]; then
    echo "[SKIP] $tag"
    return 0
  fi

  local start_ts; start_ts=$(date +%s)

  # stdout + stderr → /dev/null
  # Both go to /dev/null: stdout has export messages (~MB), stderr has DV_UPDATE
  # routing logs that grow to 5-9 GB per run even with --verboseLogs=false.
  # Failure detection: check JSON existence + exit code.
  (
    cd "$outdir"
    "$BIN"                                    \
      --nEd="$N"                              \
      --stopSec="$stop_sec"                   \
      --nodePlacementMode=pueyo_grid          \
      --pueyoGridSide="$side"                 \
      --pueyoGridSpacingM="$spacing"          \
      --wireFormat=pueyo7b                    \
      --trafficMode=pueyo_all_to_all          \
      --trafficLoad="$load"                   \
      --sfMin=7 --sfMax=8 --sfControl=8       \
      --autoDataStartSec=true                 \
      --enablePcap=false                      \
      --enableMetricsEssentialOnly=true       \
      --verboseLogs=false                     \
      --profile=pueyo2024_paper_like     \
      --rngRun="$seed"
  ) > /dev/null 2>/dev/null

  local rc=$?
  local elapsed=$(( $(date +%s) - start_ts ))
  local hh=$((elapsed/3600))
  local mm=$(( (elapsed%3600)/60 ))
  local ss=$((elapsed%60))

  if [ $rc -eq 0 ] && [ -s "$outdir/mesh_dv_summary.json" ]; then
    echo "[DONE] $tag  (${hh}h${mm}m${ss}s)"
    # Remove TX/delay CSVs — only JSON needed for analysis.
    # Without this, TX CSV alone can reach 1-4 GB per long-sim run.
    rm -f "$outdir"/mesh_dv_metrics_*.csv "$outdir"/mesh_dv_node_*.pcap 2>/dev/null || true
  else
    echo "[FAIL] $tag  rc=$rc  (${hh}h${mm}m${ss}s)  no JSON produced"
    echo "FAILED rc=$rc $(date)" > "$outdir/FAILED" 2>/dev/null || true
    return 1
  fi
}

# ----- build job list -----
declare -a JOBS_LIST
for spacing in "${SPACINGS[@]}"; do
  for load in "${LOADS[@]}"; do
    for i in "${!NS[@]}"; do
      N="${NS[$i]}"; side="${SIDES[$i]}"
      stop_sec="${STOPSEC[${N}_${load}]}"
      for seed in "${SEEDS[@]}"; do
        outdir="$OUTBASE/s${spacing}m/${load}/N$(printf '%02d' "$N")/seed$(printf '%02d' "$seed")"
        JOBS_LIST+=("$spacing|$load|$N|$side|$seed|$stop_sec|$outdir")
      done
    done
  done
done
TOTAL=${#JOBS_LIST[@]}
echo " Total jobs: $TOTAL"

if $DRY_RUN; then
  echo ""
  echo "=== DRY RUN — first 10 jobs ==="
  for i in "${!JOBS_LIST[@]}"; do
    [ $i -ge 10 ] && break
    echo "  ${JOBS_LIST[$i]}" | tr '|' '\t' | column -t -s $'\t'
  done
  echo "..."
  echo "(not launching)"
  exit 0
fi

# ----- bash job-pool (no GNU parallel needed) -----
# Keeps at most $JOBS background processes at a time.
# Uses a FIFO fd to track available slots.
FIFO=$(mktemp -u /tmp/jobpool_XXXXXX)
mkfifo "$FIFO"
exec 3<>"$FIFO"
rm -f "$FIFO"

# Seed the FIFO with N tokens (one per available slot)
for ((t=0; t<JOBS; t++)); do printf 'x' >&3; done

DONE_COUNT=0; FAIL_COUNT=0; SKIP_COUNT=0
LOGFILE="$OUTBASE/campaign_joblog.txt"
echo "# campaign_launch.sh joblog  $(date)" > "$LOGFILE"

echo ""
echo "Launching at $(date)"
echo "Log: $LOGFILE"
echo ""

for job in "${JOBS_LIST[@]}"; do
  # Acquire a slot (blocks until one is free)
  read -r -n1 -u3 _token

  IFS='|' read -r spacing load N side seed stop_sec outdir <<< "$job"

  (
    run_one_job "$spacing" "$load" "$N" "$side" "$seed" "$stop_sec" "$outdir"
    result_line="[$(date '+%H:%M:%S')] s${spacing}m ${load} N${N} seed${seed} → $outdir"
    echo "$result_line" >> "$LOGFILE"
    # Release the slot
    printf 'x' >&3
  ) &

done

# Wait for all running jobs to finish
wait

exec 3>&-

# ----- final report -----
DONE_COUNT=$(find "$OUTBASE" -name 'mesh_dv_summary.json' 2>/dev/null | wc -l)
FAIL_COUNT=$(find "$OUTBASE" -name 'FAILED'             2>/dev/null | wc -l)
SKIP_COUNT=$(( TOTAL - DONE_COUNT - FAIL_COUNT )) 2>/dev/null || true

echo ""
echo "========================================"
echo " Campaign finished  $(date)"
echo "========================================"
echo " Completed  : $DONE_COUNT / $TOTAL"
echo " Failed     : $FAIL_COUNT"
echo " Disk used  : $(du -sh "$OUTBASE" 2>/dev/null | cut -f1)"
echo ""
if [ "$FAIL_COUNT" -gt 0 ]; then
  echo " Failed runs:"
  find "$OUTBASE" -name 'FAILED' | sed 's|/FAILED||' | sort | sed 's|^|   |'
  echo ""
  echo " Re-run script to retry (already-done runs are skipped automatically)."
fi
