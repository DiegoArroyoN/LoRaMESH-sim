# LoRaMESH Simulator Frozen Repo

Frozen on: 2026-03-27
Source host: /home/diego/ns3/ns-3-dev
Purpose: reproducible clean snapshot of the current LoRaMESH simulator used for the paper-like campaigns.

## Included custom components
- `src/loramesh/`
- `src/lorawan/`
- `scratch/LoRaMESH-sim/`

## Excluded on purpose
- `build/`, `build-dir/`, `cmake-cache/`
- `scratch/LoRaMESH-sim/validation_results/`
- temporary CSV/JSON/log outputs
- old forensic / legacy runner scripts that are not part of the current paper-like workflow

## Current operational default
- profile: `pueyo2024_paper_like`
- wire format: `pueyo7b`
- receive-start: FLoRa-like immediate lock
- SF range default in the profile: `7-8`
- ns-3 energy framework disabled for comparable Pueyo runs

## Current runner set kept in this frozen repo
See `scratch/LoRaMESH-sim/CURRENT_SCRIPT_SET.txt`.

## Clean build steps
```bash
git clone <this-repo-or-bundle> LoRaMESH-sim-frozen
cd LoRaMESH-sim-frozen
./ns3 configure
cmake --build cmake-cache -j$(nproc) --target scratch_LoRaMESH-sim_mesh_dv_baseline
```

## Minimal smoke
```bash
./ns3 run "mesh_dv_baseline --profile=pueyo2024_paper_like --nNodes=9 --time=60"
```

## Notes
- This snapshot is meant to be cloned on a clean Linux machine.
- Do not mix it with an older `ns-3-dev` tree in place.
- Prefer a fresh clone into a new directory.
