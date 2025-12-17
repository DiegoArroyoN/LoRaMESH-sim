#ifndef LORAMESH_ENERGY_MODEL_H
#define LORAMESH_ENERGY_MODEL_H

#include "ns3/object.h"
#include "ns3/nstime.h"

#include <map>

namespace ns3
{

using NodeId = uint32_t;

namespace loramesh
{

/**
 * \brief Simple per-node energy tracker for LoRaMesh nodes.
 *
 * Keeps track of remaining capacity (mAh) and converts it into energy or voltage levels.
 * TX and RX updates are invoked explicitly, while idle consumption is applied whenever
 * state is queried or updated.
 */
class EnergyModel : public Object
{
public:
  static TypeId GetTypeId ();

  EnergyModel ();
  ~EnergyModel () override = default;

  void RegisterNode (NodeId id);

  void UpdateEnergy (NodeId id, double txCurrentMa, double durationSeconds);
  void UpdateRxEnergy (NodeId id, double durationSeconds);

  double GetRemainingEnergy (NodeId id);
  double GetEnergyFraction (NodeId id);
  double GetVoltageMv (NodeId id);

  void SetCapacityMah (double capacity);
  void SetTxCurrentMa (double current);
  void SetRxCurrentMa (double current);
  void SetIdleCurrentMa (double current);
  void SetVoltageWindow (double minMv, double maxMv);

  static constexpr double kDefaultCapacityMah = 2000.0;
  static constexpr double kDefaultTxCurrentMa = 120.0;
  static constexpr double kDefaultRxCurrentMa = 15.0;
  static constexpr double kDefaultIdleCurrentMa = 0.1;
  static constexpr double kDefaultVoltageMinMv = 3000.0;
  static constexpr double kDefaultVoltageMaxMv = 4200.0;

private:
  struct NodeEnergyState
  {
    double remainingMah {kDefaultCapacityMah};
    Time lastUpdate { Seconds (0) };
  };

  NodeEnergyState& EnsureNode (NodeId id);
  void ApplyIdleConsumption (NodeEnergyState& state);

  std::map<NodeId, NodeEnergyState> m_states;
  double m_capacityMah;
  double m_txCurrentMa;
  double m_rxCurrentMa;
  double m_idleCurrentMa;
  double m_voltageMinMv;
  double m_voltageMaxMv;
};

} // namespace loramesh
} // namespace ns3

#endif /* LORAMESH_ENERGY_MODEL_H */
