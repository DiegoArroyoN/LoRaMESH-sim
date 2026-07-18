#!/bin/bash
# run_all_aloha_exact_campaigns.sh — orchestrate the 3 ALOHA exact-counters campaign scripts
# in sequence. Each runs with JOBS=12 internally. Total ~720 runs.
#
# Output dirs (separate from ALOHA campaigns):
#   /home/diego/ns3-runs/campaigns_aloha_exact/         (240 runs, grid 177/248m)
#   /home/diego/ns3-runs/campaigns_random_aloha_exact/  (240 runs, random 177/248m equiv)
#   /home/diego/ns3-runs/campaigns_density_aloha_exact/ (240 runs, fixed area, grid+random)
#
# Profile: pueyo2024_paper_like with default cadSenseMarginDb=3 dB
# (post-§8.7 item 1 tuning).

set -uo pipefail

REPO="/home/diego/sim/LoRaMESH-sim-frozen-20260327"
LOGDIR="/home/diego/ns3-runs"
MASTER_LOG="${LOGDIR}/run_all_aloha_exact_$(date +%Y%m%d-%H%M%S).log"

cd "$REPO"

{
  echo "============================================================"
  echo " ALOHA exact-counters campaigns master orchestrator"
  echo " Started: $(date)"
  echo " Host: $(hostname)"
  echo " Repo:  $REPO"
  echo " Profile: pueyo2024_paper_like"
  echo " Margin: N/A (ALOHA, no CSMA)"
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

run_campaign "campaign_launch_aloha_exact.sh"        "campaign_launch_aloha_exact"
run_campaign "campaign_random_equiv_aloha_exact.sh"  "campaign_random_equiv_aloha_exact"
run_campaign "campaign_density_aloha_exact.sh"       "campaign_density_aloha_exact"

{
  echo ""
  echo "============================================================"
  echo " ALL DONE: $(date)"
  echo "============================================================"
  for d in campaigns_aloha_exact campaigns_random_aloha_exact campaigns_density_aloha_exact; do
    done_n=$(find "$LOGDIR/$d" -name mesh_dv_summary.json 2>/dev/null | wc -l)
    fail_n=$(find "$LOGDIR/$d" -name FAILED              2>/dev/null | wc -l)
    echo "  $d: done=$done_n failed=$fail_n"
  done
} | tee -a "$MASTER_LOG"
