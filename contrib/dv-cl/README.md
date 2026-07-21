# dv-cl — duty-cycle-aware cross-layer LoRa mesh routing for ns-3

Contributed module implementing the DV-CL protocol stack: distance-vector
routing with a composite cross-layer metric (time-on-air + hop cost +
state-of-charge penalty) and a CSMA/CAD MAC under regional duty-cycle
enforcement.

**Status: complete stack, ported and checked against the campaign binary
it came from.** Running the module and that binary on the same ns-3 base
with the same seed produces byte-identical transmit, receive and route
traces. Five test suites cover the units; `VALIDATION.md` at the
repository root carries the full validation log.

## What is here

- `model/dv-cl-app.{h,cc}` — the protocol: beacons, DV advertisement and
  route selection, data generation and forwarding, transmit queue.
- `model/dv-cl-routing.{h,cc}` — routing table: install, update, expiry,
  poison, hold-down, backup routes, switch hysteresis.
- `model/dv-cl-mac-csma-cad.{h,cc}` — CAD carrier sensing, backoff and
  duty-cycle enforcement (`DutyEnforcement`: ETSI time-off-air by
  default, legacy sliding window available).
- `model/dv-cl-wire.{h,cc}` — the wire contract: one 6-byte beacon
  header, one 7-byte data header, byte layouts locked by golden tests.
- `model/dv-cl-metric.{h,cc}` — the composite metric behind the
  pluggable `DvClRoutingMetric` interface.
- `model/dv-cl-stats-sink.h` — the measurement seam.
- `model/dv-cl-regional-profile.{h,cc}` — the region: carrier, bandwidth,
  coding rate, SF range, and the access rule it imposes (EU868 duty
  cycle, US915 dwell time). `DvClEu868Profile` is the default.
- `model/dv-cl-lora-net-device.{h,cc}`, `model/dv-cl-lora-energy-model.{h,cc}`,
  `model/dv-cl-energy-registry.{h,cc}` — radio, per-state energy ledger
  and reported charge.
- `helper/dv-cl-helper.{h,cc}` — installs the stack on a NodeContainer.

Three things are meant to be replaced from outside: the metric
(`DvClRouting::SetMetric`), the measurement sink
(`DvClApp::SetStatsSink`) and the region
(`DvClCsmaCadMac::SetRegionalProfile`). None requires touching the
protocol.

## Build and test

Place this directory at `contrib/dv-cl` of an ns-3 (>= 3.41) tree:

```bash
./ns3 configure --enable-tests --enable-examples
./ns3 build dv-cl
./test.py -s dv-cl -s dv-cl-wire -s dv-cl-routing -s dv-cl-mac -s dv-cl-energy
./ns3 run "dv-cl-mesh-example --nEd=9 --simTime=600"
```

Builds clean on ns-3.46 with gcc 13 and on an older ns-3-dev with
gcc 15.

## Examples

- `dv-cl-toa-example` — time-on-air across the SF grid.
- `dv-cl-mesh-example` — 3x3 grid, all-to-all traffic under the 1% duty
  cycle, minimal sink printing delivery.
- `dv-cl-campaign-example` — the full experimental campaign with the
  collector that writes the analysis pipeline's CSV and JSON.

## Known limitations

- Two regions ship (EU868, US915) through `DvClRegionalProfile`, on a
  single channel each; frequency hopping and sub-band selection are not
  modelled.
- The interference model is Goursaud, which prices cross-SF interference;
  `pueyo_fixed_capture`, which treats spreading factors as perfectly
  orthogonal, is selectable for study.
- The energy model prices radio states only; MCU and sensing are out of
  scope.
- One backup route per destination, no multipath forwarding.

## Roadmap

1. Sub-band selection and frequency hopping within a region.
2. Remaining validation suites of the plan's phases 1-2 (Bellman-Ford
   convergence, packet conservation) promoted from scripts to
   TestSuites.

## License

GPL-2.0-only (SPDX headers in every source file), as required for ns-3
modules.
