/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_TOA_H
#define DV_CL_TOA_H

#include <cstdint>

/**
 * \defgroup dv-cl DV-CL: duty-cycle-aware cross-layer LoRa mesh routing
 *
 * Contributed module implementing the DV-CL protocol stack
 * (distance-vector routing with a composite cross-layer metric and a
 * CSMA/CAD MAC under regional duty-cycle enforcement).
 *
 * Current status: verified building blocks (time-on-air, composite
 * metric). The full protocol port from the campaign tree is staged
 * (see README roadmap).
 */

namespace ns3
{
namespace dvcl
{

/**
 * \ingroup dv-cl
 * \brief LoRa packet time-on-air, Semtech AN1200.13 / SX1276 datasheet
 *        (section 4.1.1.6).
 *
 * T_sym      = 2^SF / BW
 * payloadSym = 8 + max(ceil((8*PL - 4*SF + 28 + 16*CRC - 20*IH)
 *                           / (4*(SF - 2*DE))) * (CR + 4), 0)
 * T_packet   = (preamble + 4.25) * T_sym + payloadSym * T_sym
 *
 * Verified 2026-07-17 against an independent Python implementation over
 * the full grid SF7-12 x BW{125,250} x CR{4/5..4/8} x payload{1..255}
 * x DE x CRC x IH (7296 combos, 0 mismatches) and against public
 * anchor values (SF7/20B = 56.576 ms; SF12/51B/DE = 2465.792 ms).
 * See contrib/dv-cl/test/reference/toa_reference.py.
 *
 * \param sf spreading factor, clamped semantics follow the caller (7..12)
 * \param bwHz bandwidth in Hz (e.g. 125000)
 * \param cr coding rate index 1..4 (4/5 .. 4/8)
 * \param payloadBytes PHY payload size in bytes
 * \param implicitHeader true for implicit PHY header (IH=1)
 * \param lowDataRateOpt true when low-data-rate optimization is on (DE=1)
 * \param crcOn true when the PHY CRC is enabled
 * \param preambleSymbols number of programmed preamble symbols (default 8)
 * \return time-on-air in integer microseconds (rounded to nearest)
 */
uint32_t ComputeToaUs(uint8_t sf,
                      uint32_t bwHz,
                      uint8_t cr,
                      uint32_t payloadBytes,
                      bool implicitHeader = false,
                      bool lowDataRateOpt = false,
                      bool crcOn = true,
                      uint16_t preambleSymbols = 8);

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_TOA_H */
