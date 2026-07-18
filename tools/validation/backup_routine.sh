#!/bin/bash
# F0.5 - respaldo semanal: bundle git fechado del simulador (override tree).
# Conserva los ultimos 4. Bajar periodicamente una copia a otra maquina.
set -e
B=/home/diego/sim/backups
cd /home/diego/sim/LoRaMESH-sim-override-20260530
git add -A >/dev/null 2>&1 || true
if ! git diff --cached --quiet 2>/dev/null; then
  git -c user.name="backup-bot" -c user.email="backup@local" commit -m "auto-snapshot $(date -u +%F)" >/dev/null
fi
git bundle create "$B/override-$(date -u +%Y%m%d).bundle" --all
ls -t "$B"/override-*.bundle | tail -n +5 | xargs -r rm -f
echo "$(date -u) OK $(ls -t $B/override-*.bundle | head -1)" >> "$B/backup.log"
