#!/bin/bash
# run_all_csmacad_campaigns.sh — orchestrate the 3 CSMA/CAD campaign scripts
# in sequence. Each runs with JOBS=12 internally. Total ~720 runs.
#
# Output dirs (separate from ALOHA campaigns):
#   /home/diego/ns3-runs/campaigns_csmacad/         (240 runs, grid 177/248m)
#   /home/diego/ns3-runs/campaigns_random_csmacad/  (240 runs, random 177/248m equiv)
#   /home/diego/ns3-runs/campaigns_density_csmacad/ (240 runs, fixed area, grid+random)
#
# Profile: pueyo2024_paper_like_csmacad with default cadSenseMarginDb=3 dB
# (post-§8.7 item 1 tuning).

set -uo pipefail

REPO="/home/diego/sim/LoRaMESH-sim-frozen-20260327"
LOGDIR="/home/diego/ns3-runs"
MASTER_LOG="${LOGDIR}/run_all_csmacad_$(date +%Y%m%d-%H%M%S).log"

cd "$REPO"

{
  echo "============================================================"
  echo " CSMA/CAD campaigns master orchestrator"
  echo " Started: $(date)"
  echo " Host: $(hostname)"
  echo " Repo:  $REPO"
  echo " Profile: pueyo2024_paper_like_csmacad"
  echo " Margin (default after §8.7 item 1): 3.0 dB"
  echo "============================================================"
} | tee -a "$MASTER_LOG"

run_campaign() {
  local script="$1" tag="$2"
  local logfile="${LOGDIR}/${tag}_$(date +%Y%m%d-%H%M%S).log"
  {
    echo ""
    echo "--- $(date) — Starting $tag ($script) ---"
  } | tee -a "$MASTER_LOG"
  bash "$REPO/$script" > "$logfile" 2>&1
  local rc=$?
  {
    echo "--- $(date) — Finished $tag (rc=$rc) ---"
    echo "    log: $logfile"
  } | tee -a "$MASTER_LOG"
  return 0   # never abort the orchestrator on a single failure
}

run_campaign "campaign_launch_csmacad.sh"        "campaign_launch_csmacad"
run_campaign "campaign_random_equiv_csmacad.sh"  "campaign_random_equiv_csmacad"
run_campaign "campaign_density_csmacad.sh"       "campaign_density_csmacad"

{
  echo ""
  echo "============================================================"
  echo " ALL DONE: $(date)"
  echo "============================================================"
  for d in campaigns_csmacad campaigns_random_csmacad campaigns_density_csmacad; do
    done_n=$(find "$LOGDIR/$d" -name mesh_dv_summary.json 2>/dev/null | wc -l)
    fail_n=$(find "$LOGDIR/$d" -name FAILED              2>/dev/null | wc -l)
    echo "  $d: done=$done_n failed=$fail_n"
  done
} | tee -a "$MASTER_LOG"
