# Codex Handoff: LoRaMESH / Pueyo Context

Date: 2026-03-27
Authoritative source for current behavior: code in `mesh_dv_baseline.cc`, `mesh_dv_app.cc`, `simple-gateway-lora-phy.cc`, `src/loramesh/`, `src/lorawan/`.
Use this handoff if older README/FSD text conflicts with runtime behavior.

## 1. Current objective
The current objective is not generic LoRa mesh experimentation. It is a controlled qualitative/quantitative replication of the Pueyo-Centelles baseline and comparison against proposal profiles.

There are two distinct use cases:
- Paper-like reference baseline: `pueyo2024_paper_like`
- Proposal profiles for thesis comparisons: `proposal_pueyo_like`, `proposal_pueyo_like_observed`

`extended` and generic `v2` paths are legacy. They still exist in code but are not the main operational path for the paper comparison.

## 2. Canonical profile choices
### 2.1 Main reference profile
Use `pueyo2024_paper_like` as the main reference to approximate the paper.

It is the current best approximation because it combines:
- comparable Pueyo beacon/data path
- `toa_only`
- `SF7-8`
- `pueyoFloraLikeRx=true`
- `EnableSfScanRx=false`
- single-channel
- one active reception at a time
- one reception path
- `enableNs3EnergyFramework=false`

### 2.2 Historical strict comparable profile
`pueyo2024` is kept as a strict historical comparable baseline, but it is no longer the preferred paper-like reference.

Main differences vs `pueyo2024_paper_like`:
- `pueyo2024`: `SF7-12`, scan/lock receive-start
- `pueyo2024_paper_like`: `SF7-8`, FLoRa-like immediate receive-start

### 2.3 Proposal profiles
Use only for the thesis proposal side, not as paper reference:
- `proposal_pueyo_like`
- `proposal_pueyo_like_observed`

These inherit the corrected Pueyo beacon path but add proposal-side MAC/routing semantics.

## 3. Key technical decisions already validated
### 3.1 Beacon path comparable is already fixed
This was a major source of drift and has already been corrected.

Current status:
- comparable beacons do not use the generic V2 serializer path
- `pueyo2024` and `pueyo2024_paper_like` use the dedicated Pueyo beacon path
- the old `clamp100` issue from the generic V2 path is not part of the comparable Pueyo beacon anymore

Implication:
- do not re-open beacon-wire debugging unless new evidence appears
- do not mix conclusions from `v2` generic wire with `pueyo7b` comparable wire

### 3.2 SF-range was the dominant methodological drift
This is already established.

Strong result already validated:
- `SF7-8` was the best range in the comparable campaigns for both `low` and `high`
- `SF7-12` strongly penalized PDR and was not the best paper-like approximation in this implementation

Implication:
- for Fig. 11/12-like comparisons, use `SF7-8` as the primary comparable configuration unless a new explicit sensitivity study is being run

### 3.3 Receive-start was the second important drift
The old scan/lock receive-start was not the best match for the paper.

Current accepted approximation:
- `pueyoFloraLikeRx=true`
- `EnableSfScanRx=false`

Important clarification:
- in `paper_like`, there is no scan-before-lock stage
- there is still immediate lock on the single active reception path
- `rx_post_lock_interference_fail` still makes sense in this mode because it refers to interference after reception has started, not to scan-lock failure

### 3.4 Collision/capture sensitivity was audited but not adopted as default change
A sensitivity campaign was already run.

Current conclusion:
- keep the current comparable collision/capture model for the main paper-like profile
- do not integrate a new capture model into `pueyo2024_paper_like` unless there is new strong evidence
- capture/collision is a secondary threat to validity, not the first lever to pull now

### 3.5 Temporal workload parity matters, especially for `low`
A temporal audit already showed:
- the old `low` window did not complete the full paper workload
- this was a methodological drift

Current rule:
- for paper-like `low` runs that aim to match the workload, use the full horizon needed to generate all `100 packets per pair`

For `N=9` and `low`, the validated full-workload setup was approximately:
- `dataStartSec=300`
- `dataStopSec=80300`
- `stopSec=80900`

## 4. Current validated behavior and conclusions
### 4.1 Remote server validation already succeeded
A remote-server smoke test already matched the expected profile semantics:
- `profile = pueyo2024_paper_like`
- `wire_format = pueyo7b`
- `sf_min = 7`
- `sf_max = 8`
- `pueyo_flora_like_rx = true`
- `enable_sf_scan_rx = false`
- `enable_ns3_energy_framework = false`
- `channel_count = 1`
- `reception_paths = 1`
- `delivery_ratio ≈ 0.528` for a short `N=9`, `grid`, `low` smoke

This means the remote installation is consistent with the frozen snapshot.

### 4.2 Grid trend already closed up to `N=49`
For `pueyo2024_paper_like`, `grid`, `dx=177 m`, `low`, `SF7-8`, the currently consolidated mean PDR values are:

| N | Mean PDR |
|---|---:|
| 9  | 0.5252 |
| 16 | 0.3790 |
| 25 | 0.2874 |
| 36 | 0.2233 |
| 49 | 0.1795 |

Interpretation:
- the curve decays progressively with increasing `N`
- no absurd collapse was observed up to `N=49`
- this supports qualitative replication of the trend, not exact numeric equality with the paper

### 4.3 Main causes damaging PDR in the current paper-like profile
For the closed grid sweep, the dominant causes were not routing failures.
The dominant causes were PHY/RX-side:
- `rx_no_more_demodulators`
- `rx_post_lock_interference_fail`

Interpretation:
- `rx_no_more_demodulators`: a receivable signal arrives but the single active reception path is already busy
- `rx_post_lock_interference_fail`: reception started, but later failed due to overlap/interference

`drop_no_route` was present but negligible relative to those two.

### 4.4 What this means
The current paper-like bottleneck is mainly:
- receiver saturation with only one active reception path
- interference/collision during ongoing receptions

It is not primarily:
- lack of routes
- incomplete beacon comparable wire
- old V2 serializer issues
- temporal immaturity of the network at data start, for the already-audited cases

## 5. Non-negotiable rules for future Codex sessions
### 5.1 Do not switch back to `extended` or generic `v2` for paper comparison
If the task is about Pueyo comparison, the default reasoning path must start from:
- `pueyo2024_paper_like`

### 5.2 Do not re-enable ns-3 energy framework for Pueyo comparable runs
Reason:
- Pueyo does not analyze that dimension in the reference setup
- energy framework caused instability and irrelevant distortion in long runs

For comparable runs, keep:
- `enableNs3EnergyFramework=false`

### 5.3 Do not add gateway-like semantics
Do not change the mesh nodes into something closer to LoRaWAN gateways.
Specifically avoid:
- multiple reception paths as default paper-like semantics
- concurrent multi-demod gateway behavior
- SX1301/SX1302-like receive semantics for the mesh nodes

The intended paper-like assumption remains:
- single-channel
- one active reception at a time
- one reception path

### 5.4 Do not re-open already-closed drifts without evidence
The following were already audited and should be treated as settled unless a new inconsistency appears:
- beacon path correction
- `SF7-8` as best current comparable range
- `pueyoFloraLikeRx=true` / `EnableSfScanRx=false` as better receive-start approximation
- collision/capture sensitivity not being the first default change to adopt

### 5.5 Do not reuse old WSL heavy-run assumptions
WSL became unstable due to:
- memory pressure
- gigantic logs
- massive metric files

On the stronger server, use the safe infrastructure when needed.

## 6. Safe long-run policy for heavy cases like `N=64`
The heavy `N=64` case should not be launched with full metric dumping and raw WSL-style logging.

Use the dedicated safe runner:
- `scratch/LoRaMESH-sim/run_pueyo_paper_like_grid_n64_safe.py`

It already enforces:
- `pueyo2024_paper_like`
- `grid`
- `N=64`
- `low`
- `SF7-8`
- `enableNs3EnergyFramework=false`
- `enableMetricsPeriodicFlush=true`
- `enableMetricsEssentialOnly=true`

Operational rule:
- run `seed=1` first
- only run `seed=2,3` if `seed=1` closes cleanly

## 7. Repository and reproducibility context
A frozen clean repo snapshot was created locally with:
- a clean repo tree
- current custom `src/loramesh`
- current custom `src/lorawan`
- current curated `scratch/LoRaMESH-sim`
- a frozen tag for external replication

This matters because the original WSL tree had:
- ignored directories
- nested git usage in `scratch/LoRaMESH-sim`
- large non-versioned validation outputs

Future work should prefer the frozen repo on the stronger machine, not the old WSL tree.

## 8. Recommended next tasks on the remote machine
Priority order:
1. close `N=64` safely using the dedicated safe runner
2. integrate `N=64` into the grid trend replication and regenerate the boxplot/table
3. if needed, run the analogous trend campaign for `pueyo_random_equiv`
4. only after that, reopen additional sensitivities if a real gap remains

## 9. Short prompt to give future Codex sessions
Use this if you want to bootstrap context quickly:

> We are working on the LoRaMESH ns-3 simulator in its frozen paper-like state. The main reference profile is `pueyo2024_paper_like`, not `extended` and not generic `v2`. The beacon comparable path is already fixed and should not be revisited without evidence. The main validated comparable settings are: `wireFormat=pueyo7b`, `toa_only`, `SF7-8`, `pueyoFloraLikeRx=true`, `EnableSfScanRx=false`, single-channel, one active reception, one reception path, `enableNs3EnergyFramework=false`. The main closed result so far is the grid `low` PDR-vs-N sweep up to `N=49`, where PDR decays progressively and the main damage causes are `rx_no_more_demodulators` and `rx_post_lock_interference_fail`. For heavy `N=64` runs, use the safe runner with essential metrics and periodic flush; do not repeat the old WSL logging/metrics pattern.

