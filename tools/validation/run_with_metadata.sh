#!/bin/bash
# F0.3 — wrapper de trazabilidad: corre el binario del simulador y emite
# run_metadata.json junto a la salida. Uso:
#   run_with_metadata.sh <outdir> <binario> [args...]
set -u
D="$1"; BIN="$2"; shift 2
mkdir -p "$D"; cd "$D"
OV_DIR=$(cd "$(dirname "$BIN")/../../.." && pwd)
COMMIT=$(git -C "$OV_DIR" rev-parse HEAD 2>/dev/null || echo unknown)
DIRTY=$(git -C "$OV_DIR" status --porcelain 2>/dev/null | wc -l)
BINSHA=$(sha256sum "$BIN" | cut -d" " -f1)
T0=$(date -u +%s)
LD_LIBRARY_PATH="$OV_DIR/build/lib" "$BIN" "$@"
RC=$?
T1=$(date -u +%s)
python3 - "$D" <<PYEOF
import json, sys, os
meta = {
  "commit": "$COMMIT", "tree_dirty_files": int("$DIRTY"),
  "binary_sha256": "$BINSHA", "binary_path": "$BIN",
  "argv": """$*""".split(), "rc": int("$RC"),
  "start_utc": int("$T0"), "end_utc": int("$T1"),
  "duration_s": int("$T1") - int("$T0"),
  "hostname": os.uname().nodename,
}
json.dump(meta, open(os.path.join(sys.argv[1], "run_metadata.json"), "w"), indent=1)
PYEOF
exit $RC
