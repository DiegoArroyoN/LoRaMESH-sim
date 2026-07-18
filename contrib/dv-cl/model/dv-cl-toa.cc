/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-toa.h"

#include <algorithm>
#include <cmath>

namespace ns3
{
namespace dvcl
{

uint32_t
ComputeToaUs(uint8_t sf,
             uint32_t bwHz,
             uint8_t cr,
             uint32_t payloadBytes,
             bool implicitHeader,
             bool lowDataRateOpt,
             bool crcOn,
             uint16_t preambleSymbols)
{
    const double bw = static_cast<double>(bwHz);
    const double tSym = std::pow(2.0, sf) / bw;

    const double num = 8.0 * static_cast<double>(payloadBytes) - 4.0 * sf + 28.0 +
                       (crcOn ? 16.0 : 0.0) - (implicitHeader ? 20.0 : 0.0);
    const double den = 4.0 * (sf - (lowDataRateOpt ? 2.0 : 0.0));
    const double ce = std::ceil(std::max(num / den, 0.0));
    const double paySym = 8.0 + ce * (cr + 4.0);

    const double tPreamble = (preambleSymbols + 4.25) * tSym;
    const double tTotal = tPreamble + paySym * tSym;
    return static_cast<uint32_t>(tTotal * 1e6 + 0.5);
}

} // namespace dvcl
} // namespace ns3
