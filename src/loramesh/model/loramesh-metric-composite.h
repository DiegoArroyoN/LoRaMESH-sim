#ifndef LORAMESH_METRIC_COMPOSITE_H
#define LORAMESH_METRIC_COMPOSITE_H

#include <cstdint>
#include "ns3/ptr.h"
#include "ns3/loramesh-energy-model.h"

namespace ns3
{

using NodeId = uint32_t;

namespace loramesh
{

struct LinkStats
{
  double toaUs {0.0};
  uint8_t hops {0};
  double rssiDbm {-120.0};
  double batteryMv {0.0};
  double energyFraction {-1.0};
};

class CompositeMetric
{
public:
  CompositeMetric () = default;

  void SetEnergyModel (Ptr<EnergyModel> energy) { m_energyModel = energy; }
  double ComputeLinkCost (NodeId src, NodeId dst, const LinkStats& stats) const;
  double NormalizeToa (double toaUs) const;
  double RssiPenalty (double rssiDbm) const;
  double BatteryPenalty (double energyFraction, double batteryMv) const;

private:
  Ptr<EnergyModel> m_energyModel;
};

} // namespace loramesh
} // namespace ns3

#endif /* LORAMESH_METRIC_COMPOSITE_H */
