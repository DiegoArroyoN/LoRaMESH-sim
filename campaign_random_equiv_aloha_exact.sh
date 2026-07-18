#!/bin/bash
# campaign_random_equiv.sh — Random topology, equivalent areas to 177m/248m grid
# Fig 12 data: 2 spacings × 2 loads × 6 N × 10 seeds = 240 runs
#
# Usage:
#   bash campaign_random_equiv.sh              # launch with 12 parallel jobs
#   bash campaign_random_equiv.sh --dry-run
#   bash campaign_random_equiv.sh --jobs=8

set -uo pipefail

BIN="/home/diego/sim/LoRaMESH-sim-frozen-20260327/build/scratch/LoRaMESH-sim/ns3-dev-mesh_dv_baseline-default"
OUTBASE="/home/diego/ns3-runs/campaigns_random_aloha_exact"
JOBS=12
DRY_RUN=false

for arg in "$@"; do
  case "$arg" in
    --dry-run)   DRY_RUN=true ;;
    --jobs=*)    JOBS="${arg#--jobs=}" ;;
  esac
done

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

declare -A PK_INTERVAL
PK_INTERVAL[low]=100
PK_INTERVAL[high]=1

compute_stop_sec() {
  local N=$1 side=$2 pk_interval=$3
  python3 - <<EOF
import math
N, side, pk_interval = $N, $side, $pk_interval
pk_per_pair, margin = $PK_PER_PAIR, 1.05
beaconWarm = beaconStable = warmup = 60.0
diameter   = 2 * (side - 1)
warmup_hops = int(warmup / beaconWarm)
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
echo " Campaign: Random-equiv topology (Fig 12)"
echo " $(date)"
echo "========================================"
printf " %-15s: %s\n" "Output base"   "$OUTBASE"
printf " %-15s: %s m\n" "Spacings"    "${SPACINGS[*]}"
printf " %-15s: %s\n" "Loads"         "${LOADS[*]}"
printf " %-15s: %s\n" "N values"      "${NS[*]}"
printf " %-15s: 1..10\n" "Seeds"
printf " %-15s: pueyo_random_equiv\n" "Topology"
printf " %-15s: %s\n" "Parallel jobs" "$JOBS"
echo ""
echo " stopSec per (N, load):"
printf "  %-8s" "N\load"
for load in "${LOADS[@]}"; do printf "  %-12s" "$load"; done
echo ""
for i in "${!NS[@]}"; do
  N="${NS[$i]}"
  printf "  %-8s" "N=$N"
  for load in "${LOADS[@]}"; do
    printf "  %-12s" "${STOPSEC[${N}_${load}]}"
  done
  echo ""
done
echo ""

# ----- runner function -----
run_one_job() {
  local spacing="$1" load="$2" N="$3" side="$4" seed="$5" stop_sec="$6" outdir="$7"
  local tag="rand_s${spacing}m_${load}_N${N}_seed${seed}"

  mkdir -p "$outdir"

  if [ -s "$outdir/mesh_dv_summary.json" ]; then
    echo "[SKIP] $tag"
    return 0
  fi

  local start_ts; start_ts=$(date +%s)

  (
    cd "$outdir"
    "$BIN"                                    \
      --nEd="$N"                              \
      --stopSec="$stop_sec"                   \
      --nodePlacementMode=pueyo_random_equiv  \
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
  local hh=$((elapsed/3600)) mm=$(( (elapsed%3600)/60 )) ss=$((elapsed%60))

  if [ $rc -eq 0 ] && [ -s "$outdir/mesh_dv_summary.json" ]; then
    echo "[DONE] $tag  (${hh}h${mm}m${ss}s)"
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
        outdir="$OUTBASE/s${spacing}m_equiv/${load}/N$(printf '%02d' "$N")/seed$(printf '%02d' "$seed")"
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
    echo "  ${JOBS_LIST[$i]}" | tr '|' '\t'
  done
  echo "..."
  echo "(not launching)"
  exit 0
fi

# ----- bash job-pool -----
FIFO=$(mktemp -u /tmp/jobpool_XXXXXX)
mkfifo "$FIFO"
exec 3<>"$FIFO"
rm -f "$FIFO"

for ((t=0; t<JOBS; t++)); do printf 'x' >&3; done

LOGFILE="$OUTBASE/campaign_random_joblog.txt"
echo "# campaign_random_equiv.sh joblog  $(date)" > "$LOGFILE"
echo "Launching at $(date)"
echo "Log: $LOGFILE"
echo ""

for job in "${JOBS_LIST[@]}"; do
  read -r -n1 -u3 _token
  IFS='|' read -r spacing load N side seed stop_sec outdir <<< "$job"
  (
    run_one_job "$spacing" "$load" "$N" "$side" "$seed" "$stop_sec" "$outdir"
    echo "[$(date '+%H:%M:%S')] rand_s${spacing}m ${load} N${N} seed${seed}" >> "$LOGFILE"
    printf 'x' >&3
  ) &
done

wait
exec 3>&-

# ----- final report -----
DONE_COUNT=$(find "$OUTBASE" -name 'mesh_dv_summary.json' 2>/dev/null | wc -l)
FAIL_COUNT=$(find "$OUTBASE" -name 'FAILED'             2>/dev/null | wc -l)

echo ""
echo "========================================"
echo " Random-equiv campaign finished  $(date)"
echo "========================================"
echo " Completed  : $DONE_COUNT / $TOTAL"
echo " Failed     : $FAIL_COUNT"
echo " Disk used  : $(du -sh "$OUTBASE" 2>/dev/null | cut -f1)"
echo ""
if [ "$FAIL_COUNT" -gt 0 ]; then
  echo " Failed runs:"
  find "$OUTBASE" -name 'FAILED' | sed 's|/FAILED||' | sort | sed 's|^|   |'
fi
