/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-metric.h"

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

constexpr double DvClCompositeMetric::kMaxToaUs[6];

TypeId
DvClRoutingMetric::GetTypeId()
{
    static TypeId tid = TypeId("ns3::dvcl::DvClRoutingMetric")
                            .SetParent<Object>()
                            .SetGroupName("DvCl");
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
                          MakeDoubleChecker<double>(0.0));
    return tid;
}

double
DvClCompositeMetric::NormalizeToa(double toaUs, uint8_t sf) const
{
    const uint8_t sfClamped = std::clamp<uint8_t>(sf, 7, 12);
    const std::size_t idx = static_cast<std::size_t>(sfClamped - 7);
    return std::min(toaUs / kMaxToaUs[idx], 1.0);
}

double
DvClCompositeMetric::EnergyPenalty(double energyFraction) const
{
    if (!std::isfinite(energyFraction) || energyFraction < 0.0)
    {
        return 0.0; // unknown charge prices as full battery
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
    const double energyCost = EnergyPenalty(in.energyFraction);
    NS_LOG_DEBUG("link cost: toa=" << toaCost << " hop=" << hopCost
                                   << " energy=" << energyCost);
    return toaCost + hopCost + energyCost;
}

} // namespace dvcl
} // namespace ns3
