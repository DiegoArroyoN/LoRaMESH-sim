#include "loramesh-metric-composite.h"

#include <algorithm>
#include <cmath>

namespace ns3
{
namespace loramesh
{

double
CompositeMetric::ComputeLinkCost (NodeId src, NodeId dst, const LinkStats& stats) const
{
  (void) src;
  (void) dst;

  const double wToa = 0.3;
  const double wHops = 0.2;
  const double wRssi = 0.4;
  const double wBattery = 0.1;

  const double toaNorm = NormalizeToa (stats.toaUs);
  const double hopNorm = std::min (static_cast<double> (stats.hops) / 10.0, 1.0);
  const double rssiPenalty = RssiPenalty (stats.rssiDbm);
  double energyFraction = stats.energyFraction;
  if (energyFraction < 0.0 && m_energyModel)
    {
      energyFraction = m_energyModel->GetEnergyFraction (src);
    }

  const double batteryPenalty = BatteryPenalty (energyFraction, stats.batteryMv);

  return wToa * toaNorm + wHops * hopNorm + wRssi * rssiPenalty + wBattery * batteryPenalty;
}

double
CompositeMetric::NormalizeToa (double toaUs) const
{
  const double toaMs = toaUs / 1000.0;
  return std::min (toaMs / 2000.0, 1.0);
}

double
CompositeMetric::RssiPenalty (double rssiDbm) const
{
  return std::clamp ((-rssiDbm - 90.0) / 40.0, 0.0, 1.0);
}

double
CompositeMetric::BatteryPenalty (double energyFraction, double batteryMv) const
{
  if (energyFraction >= 0.0)
    {
      return 1.0 - std::clamp (energyFraction, 0.0, 1.0);
    }
  const double battNorm = std::clamp ((batteryMv - 3000.0) / 1200.0, 0.0, 1.0);
  return 1.0 - battNorm;
}

} // namespace loramesh
} // namespace ns3
