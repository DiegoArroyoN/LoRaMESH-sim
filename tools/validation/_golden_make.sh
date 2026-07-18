#!/bin/bash
OV=/home/diego/sim/LoRaMESH-sim-override-20260530
BIN=$OV/build/scratch/LoRaMESH-sim/ns3-dev-mesh_dv_baseline-default
G=/home/diego/sim/golden_traces; mkdir -p $G
common="--pueyoGridSpacingM=500.0 --trafficLoad=low --dataStartSec=300 --stopSec=2592000 --pdrEndWindowSec=600 --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true --stopOnFullDepletion=true --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=12"
declare -A SC
SC[g1_a2a_N9_cmpcsma_dcoff]="--profile=proposal_pueyo_like_csmacad --nEd=9 --pueyoGridSide=3 --nodePlacementMode=pueyo_grid --rngRun=1 --trafficMode=pueyo_all_to_all --pueyoPacketsPerPair=20"
SC[g2_conv_N25_toaaloha_dc1]="--profile=pueyo2024_paper_like --nEd=25 --pueyoGridSide=5 --nodePlacementMode=pueyo_grid --rngRun=1 --trafficMode=periodic_any_to_any --forcedDataDestinationId=24 --allowDutyOverride=true --dutyOverridePct=1.0"
SC[g3_a2a_N9_cmpcsma_dc1]="--profile=proposal_pueyo_like_csmacad --nEd=9 --pueyoGridSide=3 --nodePlacementMode=pueyo_grid --rngRun=2 --trafficMode=pueyo_all_to_all --pueyoPacketsPerPair=20 --allowDutyOverride=true --dutyOverridePct=1.0"
for k in "${!SC[@]}"; do
  D=$G/$k; rm -rf $D
  /home/diego/sim/run_with_metadata.sh $D $BIN ${SC[$k]} $common >/dev/null 2>&1
  echo "$k rc=$? summary=$([ -f $D/mesh_dv_summary.json ] && echo OK || echo MISSING)"
done
sha256sum $G/*/mesh_dv_summary.json > $G/GOLDEN_SHA256
cat $G/GOLDEN_SHA256
echo "GOLDEN DONE"
