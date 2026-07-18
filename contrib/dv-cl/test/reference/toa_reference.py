#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""F1.1 ToA verification (Plan Maestro DV-CL, Gate 1).

Two independent implementations of the LoRa time-on-air:
  (a) semtech_toa_us : written from Semtech AN1200.13 / SX1276 datasheet
      (section 4.1.1.6), the analytical ground truth.
  (b) sim_toa_us     : faithful transpilation of
      MeshDvApp::ComputeLoRaToAUs (mesh_dv_app.cc, snapshot 2026-03-05).

The script compares them over the full grid SF7-12 x BW{125,250} x
CR{1..4} x payload{1..255} x DE{on,off} x CRC{on,off} x IH{on,off}
(7296 sampled combos), asserts exact agreement, checks two published anchor
values, and emits a golden table (toa_golden.csv) consumed by the ns-3
TestSuite (test/dv-cl-test-suite.cc).

Exit code 0 = PASS. Any mismatch prints the offending combo and fails.
"""
import csv
import math
import sys
from pathlib import Path


def semtech_toa_us(sf, bw_hz, cr, pl, n_preamble=8, ih=False, de=False, crc=True):
    """Semtech AN1200.13: T_packet in microseconds.

    payloadSymbNb = 8 + max(ceil((8PL - 4SF + 28 + 16CRC - 20IH)
                                 / (4(SF - 2DE))) * (CR+4), 0)
    """
    t_sym = (2.0 ** sf) / bw_hz
    num = 8.0 * pl - 4.0 * sf + 28.0 + (16.0 if crc else 0.0) - (20.0 if ih else 0.0)
    den = 4.0 * (sf - (2.0 if de else 0.0))
    payload_symb = 8.0 + max(math.ceil(num / den) * (cr + 4.0), 0.0)
    t_preamble = (n_preamble + 4.25) * t_sym
    t_packet = t_preamble + payload_symb * t_sym
    return int(t_packet * 1e6 + 0.5)


def sim_toa_us(sf, bw_hz, cr, pl, ih=False, de=False, crc=True):
    """Faithful transpilation of MeshDvApp::ComputeLoRaToAUs."""
    t_sym = (2.0 ** sf) / float(bw_hz)
    num = 8.0 * pl - 4.0 * sf + 28.0 + (16.0 if crc else 0.0) - (20.0 if ih else 0.0)
    den = 4.0 * (sf - (2.0 if de else 0.0))
    ce = math.ceil(max(num / den, 0.0))
    pay_sym = 8.0 + ce * (cr + 4.0)
    t_preamble = (8.0 + 4.25) * t_sym
    t_tot = t_preamble + pay_sym * t_sym
    return int(t_tot * 1e6 + 0.5)


def main():
    mismatches = 0
    checked = 0
    golden = []
    for sf in range(7, 13):
        for bw in (125000, 250000):
            for cr in (1, 2, 3, 4):
                for de in (False, True):
                    for crc in (True, False):
                        for ih in (False, True):
                            for pl in list(range(1, 256, 16)) + [51, 222, 255]:
                                a = semtech_toa_us(sf, bw, cr, pl, ih=ih, de=de, crc=crc)
                                b = sim_toa_us(sf, bw, cr, pl, ih=ih, de=de, crc=crc)
                                checked += 1
                                if a != b:
                                    mismatches += 1
                                    if mismatches <= 10:
                                        print(f"MISMATCH sf={sf} bw={bw} cr={cr} pl={pl} "
                                              f"de={de} crc={crc} ih={ih}: semtech={a} sim={b}")
    # Anchor 1: SF7/BW125/CR4-5, 20 B, preamble 8, explicit hdr, CRC on, DE off
    # -> 56.576 ms (any public LoRa airtime calculator).
    a1 = semtech_toa_us(7, 125000, 1, 20)
    ok1 = abs(a1 - 56576) <= 1
    # Anchor 2: SF12/BW125/CR4-5, 51 B, DE on (mandatory at SF12/125k),
    # preamble 8, explicit header, CRC on -> 2465.792 ms (TTN airtime calc).
    a2 = semtech_toa_us(12, 125000, 1, 51, de=True)
    ok2 = abs(a2 - 2465792) <= 1
    print(f"anchor SF7/20B  = {a1/1000.0:.3f} ms (expected 56.576)  -> {'OK' if ok1 else 'FAIL'}")
    print(f"anchor SF12/51B = {a2/1000.0:.3f} ms (expected 2465.792) -> {'OK' if ok2 else 'FAIL'}")

    # Golden rows for the C++ TestSuite (defaults of the simulator profile:
    # explicit header, CRC on; DE toggled per row).
    out = Path(__file__).parent / "toa_golden.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sf", "bw_hz", "cr", "payload", "de", "toa_us"])
        for sf, bw, cr, pl, de in [
            (7, 125000, 1, 20, 0), (7, 125000, 1, 51, 0), (7, 250000, 1, 51, 0),
            (8, 125000, 1, 51, 0), (9, 125000, 1, 51, 0), (10, 125000, 1, 51, 0),
            (11, 125000, 1, 51, 1), (12, 125000, 1, 51, 1), (12, 125000, 4, 51, 1),
            (7, 125000, 1, 1, 0), (12, 125000, 1, 222, 1), (10, 125000, 2, 128, 0),
        ]:
            w.writerow([sf, bw, cr, pl, de,
                        semtech_toa_us(sf, bw, cr, pl, de=bool(de))])
    print(f"golden table -> {out.name} (12 rows)")

    print(f"checked={checked} mismatches={mismatches}")
    if mismatches or not (ok1 and ok2):
        print("RESULT: FAIL")
        return 1
    print("RESULT: PASS — sim ToA == Semtech AN1200.13 over full grid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
