/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-metric.h"

#include "ns3/boolean.h"
#include "ns3/double.h"
#include "ns3/log.h"

#include <algorithm>
#include <cmath>

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("DvClMetric");

namespace dvcl
{

NS_OBJECT_ENSURE_REGISTERED(DvClRoutingMetric);
NS_OBJECT_ENSURE_REGISTERED(DvClCompositeMetric);
NS_OBJECT_ENSURE_REGISTERED(DvClRssiMetric);

constexpr double DvClCompositeMetric::kMaxToaUs[6];

TypeId
DvClRoutingMetric::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClRoutingMetric").SetParent<Object>().SetGroupName("DvCl");
    return tid;
}

TypeId
DvClCompositeMetric::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClCompositeMetric")
            .SetParent<DvClRoutingMetric>()
            .SetGroupName("DvCl")
            .AddConstructor<DvClCompositeMetric>()
            .AddAttribute("WToa",
                          "alpha: weight of the normalized time-on-air term.",
                          DoubleValue(0.60),
                          MakeDoubleAccessor(&DvClCompositeMetric::m_wToa),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("WHop",
                          "beta: constant cost added per hop.",
                          DoubleValue(0.15),
                          MakeDoubleAccessor(&DvClCompositeMetric::m_wHop),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("WEnergy",
                          "delta: weight of the piecewise energy penalty.",
                          DoubleValue(0.25),
                          MakeDoubleAccessor(&DvClCompositeMetric::m_wEnergy),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("EnergyLo",
                          "bLo: state of charge at/below which Psi saturates.",
                          DoubleValue(0.20),
                          MakeDoubleAccessor(&DvClCompositeMetric::m_energyLo),
                          MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute("EnergyHi",
                          "bHi: state of charge at/above which Psi is zero.",
                          DoubleValue(0.50),
                          MakeDoubleAccessor(&DvClCompositeMetric::m_energyHi),
                          MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute("EnergyPow",
                          "p: exponent of the piecewise penalty ramp.",
                          DoubleValue(2.0),
                          MakeDoubleAccessor(&DvClCompositeMetric::m_energyPow),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("EnergyMaxPenalty",
                          "PsiMax: penalty value at/below bLo (before delta).",
                          DoubleValue(1.0),
                          MakeDoubleAccessor(&DvClCompositeMetric::m_energyMaxPenalty),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("ToaNormGlobal",
                          "true: normalise time-on-air against ONE global ceiling, so the term "
                          "grows with the spreading factor and actually prices airtime. false: "
                          "normalise per spreading factor, as the thesis text states -- kept for "
                          "reproduction, but numerator and denominator scale together so the term "
                          "barely varies across SF (VALIDATION.md 2026-07-25).",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClCompositeMetric::m_toaNormGlobal),
                          MakeBooleanChecker())
            .AddAttribute("ToaCeilingUs",
                          "Global normalisation ceiling in microseconds (ToaNormGlobal=true). "
                          "Default: airtime of a 222 B payload at SF12, 16-symbol preamble.",
                          DoubleValue(8462336.0),
                          MakeDoubleAccessor(&DvClCompositeMetric::m_toaCeilingUs),
                          MakeDoubleChecker<double>(1.0));
    return tid;
}

double
DvClCompositeMetric::NormalizeToa(double toaUs, uint8_t sf) const
{
    if (m_toaNormGlobal)
    {
        // One ceiling for every link: the term then grows with the spreading
        // factor, which is what makes it price airtime across links.
        return std::min(toaUs / std::max(m_toaCeilingUs, 1e-9), 1.0);
    }
    const uint8_t sfClamped = std::clamp<uint8_t>(sf, 7, 12);
    const std::size_t idx = static_cast<std::size_t>(sfClamped - 7);
    return std::min(toaUs / kMaxToaUs[idx], 1.0);
}

double
DvClCompositeMetric::EnergyPenalty(double energyFraction, double batteryMv) const
{
    if (!std::isfinite(energyFraction))
    {
        return 0.0; // unusable reading prices as full battery
    }
    if (energyFraction < 0.0)
    {
        // Charge unknown: fall back to the terminal voltage, as the campaign
        // metric does (Li-Ion 18650: 3000 mV = empty, 4200 mV = full). Without
        // this the link would price as a full battery and the energy term would
        // silently vanish for every neighbour that never reported a fraction.
        if (!std::isfinite(batteryMv) || batteryMv < 0.0)
        {
            return 0.0;
        }
        constexpr double vMin = 3000.0;
        constexpr double vMax = 4200.0;
        energyFraction = std::clamp((batteryMv - vMin) / (vMax - vMin), 0.0, 1.0);
    }
    const double lo = std::clamp(std::min(m_energyLo, m_energyHi), 0.0, 1.0);
    const double hi = std::clamp(std::max(m_energyLo, m_energyHi), 0.0, 1.0);
    const double e = std::clamp(energyFraction, 0.0, 1.0);
    if (e >= hi)
    {
        return 0.0;
    }
    if (e <= lo)
    {
        return m_wEnergy * m_energyMaxPenalty;
    }
    const double denom = std::max(hi - lo, 1e-9);
    const double x = std::clamp((hi - e) / denom, 0.0, 1.0);
    return m_wEnergy * m_energyMaxPenalty * std::pow(x, m_energyPow);
}

double
DvClCompositeMetric::ComputeLinkCost(const LinkInputs& in) const
{
    const double toaCost = m_wToa * NormalizeToa(in.toaUs, in.sf);
    const double hopCost = m_wHop;
    const double energyCost = EnergyPenalty(in.energyFraction, in.batteryMv);
    NS_LOG_DEBUG("link cost: toa=" << toaCost << " hop=" << hopCost << " energy=" << energyCost);
    return toaCost + hopCost + energyCost;
}

TypeId
DvClRssiMetric::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClRssiMetric")
            .SetParent<DvClRoutingMetric>()
            .SetGroupName("DvCl")
            .AddConstructor<DvClRssiMetric>()
            .AddAttribute("WRssi",
                          "Weight on the normalised link weakness term.",
                          DoubleValue(1.0),
                          MakeDoubleAccessor(&DvClRssiMetric::m_wRssi),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("Hop",
                          "Strictly-positive per-hop term (keeps path cost monotone).",
                          DoubleValue(0.05),
                          MakeDoubleAccessor(&DvClRssiMetric::m_hop),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("RefDbm",
                          "RSSI of a perfect link; weakness is 0 here.",
                          DoubleValue(-30.0),
                          MakeDoubleAccessor(&DvClRssiMetric::m_refDbm),
                          MakeDoubleChecker<double>())
            .AddAttribute("FloorDbm",
                          "Receiver sensitivity; weakness is 1 here.",
                          DoubleValue(-137.0),
                          MakeDoubleAccessor(&DvClRssiMetric::m_floorDbm),
                          MakeDoubleChecker<double>())
            .AddAttribute("UnmeasuredWeakness",
                          "Weakness charged when RSSI is unmeasured (rssiDbm == 0).",
                          DoubleValue(0.5),
                          MakeDoubleAccessor(&DvClRssiMetric::m_unmeasuredWeakness),
                          MakeDoubleChecker<double>(0.0, 1.0));
    return tid;
}

double
DvClRssiMetric::ComputeLinkCost(const LinkInputs& in) const
{
    // rssiDbm == 0 means the receiver never heard this neighbour directly, so
    // there is no local measurement to price -- charge a fixed middling
    // weakness rather than pretend the link is perfect.
    double weakness;
    if (in.rssiDbm == 0.0)
    {
        weakness = m_unmeasuredWeakness;
    }
    else
    {
        const double span = m_refDbm - m_floorDbm; // e.g. -30 - (-137) = 107
        weakness = (span > 0.0) ? std::clamp((m_refDbm - in.rssiDbm) / span, 0.0, 1.0) : 0.0;
    }
    return m_wRssi * weakness + m_hop;
}

} // namespace dvcl
} // namespace ns3
