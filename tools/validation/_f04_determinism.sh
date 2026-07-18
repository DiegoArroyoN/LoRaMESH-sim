#!/bin/bash
OV=/home/diego/sim/LoRaMESH-sim-override-20260530
BIN=$OV/build/scratch/LoRaMESH-sim/ns3-dev-mesh_dv_baseline-default
run(){ D=/home/diego/sim/_det_$1; rm -rf $D; mkdir -p $D; cd $D
  LD_LIBRARY_PATH=$OV/build/lib timeout 2500 $BIN --profile=proposal_pueyo_like_csmacad --nEd=25 --pueyoGridSide=5 --nodePlacementMode=pueyo_grid --pueyoGridSpacingM=500.0 --trafficLoad=low --dataStartSec=300 --stopSec=2592000 --pdrEndWindowSec=600 --rngRun=7 --enablePcap=false --verboseLogs=false --enableMetricsEssentialOnly=true --stopOnFullDepletion=true --allowPaperLikeSfRangeVariant=true --sfMin=7 --sfMax=12 --trafficMode=pueyo_all_to_all --pueyoPacketsPerPair=20 >/dev/null 2>&1; }
echo "F0.4 run A..."; run A
echo "F0.4 run B..."; run B
echo "=== DIFF summary (bit-identico si vacio) ==="
if diff -q /home/diego/sim/_det_A/mesh_dv_summary.json /home/diego/sim/_det_B/mesh_dv_summary.json; then
  echo "DETERMINISMO: PASS (summary bit-identico)"
else
  echo "DETERMINISMO: DIFIEREN - primeras lineas:"
  diff /home/diego/sim/_det_A/mesh_dv_summary.json /home/diego/sim/_det_B/mesh_dv_summary.json | head -12
fi
echo "=== DIFF metricas csv ==="
for f in /home/diego/sim/_det_A/mesh_dv_metrics_*.csv; do
  b=/home/diego/sim/_det_B/$(basename $f)
  diff -q "$f" "$b" >/dev/null 2>&1 && echo "OK $(basename $f)" || echo "DIFIERE $(basename $f)"
done
echo "F04 DONE"
