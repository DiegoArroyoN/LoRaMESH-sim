# dv-cl — duty-cycle-aware cross-layer LoRa mesh routing for ns-3

Contributed module implementing the DV-CL protocol stack: distance-vector
routing with a composite cross-layer metric (time-on-air + hop cost +
state-of-charge penalty) and a CSMA/CAD MAC under regional duty-cycle
enforcement.

**Status: skeleton with verified building blocks (work in progress).**
This module currently ships the two pure units of the stack, each backed
by tests runnable with the standard ns-3 workflow:

- `model/dv-cl-toa.{h,cc}` — LoRa time-on-air (Semtech AN1200.13).
  Verified against an independent implementation over the full
  SF/BW/CR/payload/DE/CRC/IH grid (7296 combos, 0 mismatches) and public
  anchor values. See `test/reference/toa_reference.py`.
- `model/dv-cl-metric.{h,cc}` — the composite metric of the paper
  (`DvClCompositeMetric`), implemented behind the pluggable
  `DvClRoutingMetric` interface so third parties can provide their own
  metric without touching the protocol core. All parameters are ns-3
  Attributes.

The full protocol port (DV routing engine, beacon wire format, CSMA/CAD
MAC, energy model glue, helper) is staged from the campaign tree; see
the roadmap below.

## Build and test

Place this directory at `contrib/dv-cl` of an ns-3 (>= 3.41) tree:

```bash
./ns3 configure --enable-tests --enable-examples
./ns3 build dv-cl
./test.py -s dv-cl
./ns3 run "dv-cl-toa-example --payload=51"
```

## Roadmap (Plan Maestro F5/F6)

1. Port `RoutingDv` + beacon wire headers + CSMA/CAD MAC from the
   campaign tree into `model/`, replacing the legacy duplicate metric
   (see audit note in `dv-cl-metric.h`).
2. `helper/DvClHelper` to install the stack on nodes.
3. Regional profiles (EU868 sub-bands, US915 dwell time) as a pluggable
   `RegionalProfile`.
4. TestSuites for Fases 1-2 of the validation plan (Bellman-Ford
   convergence, duty-cycle sliding-window invariant, energy accounting,
   packet conservation).

## License

GPL-2.0-only (SPDX headers in every source file), as required for ns-3
modules.
