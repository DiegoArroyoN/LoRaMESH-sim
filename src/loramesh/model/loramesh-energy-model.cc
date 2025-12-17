#include "loramesh-energy-model.h"

#include "ns3/log.h"
#include "ns3/simulator.h"

#include <algorithm>

namespace ns3
{
namespace loramesh
{

NS_LOG_COMPONENT_DEFINE ("LoraMeshEnergyModel");
NS_OBJECT_ENSURE_REGISTERED (EnergyModel);

TypeId
EnergyModel::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::loramesh::EnergyModel")
    .SetParent<Object> ()
    .AddConstructor<EnergyModel> ();
  return tid;
}

EnergyModel::EnergyModel ()
  : m_capacityMah (kDefaultCapacityMah),
    m_txCurrentMa (kDefaultTxCurrentMa),
    m_rxCurrentMa (kDefaultRxCurrentMa),
    m_idleCurrentMa (kDefaultIdleCurrentMa),
    m_voltageMinMv (kDefaultVoltageMinMv),
    m_voltageMaxMv (kDefaultVoltageMaxMv)
{
}

void
EnergyModel::RegisterNode (NodeId id)
{
  NodeEnergyState& state = EnsureNode (id);
  state.remainingMah = m_capacityMah;
  state.lastUpdate = Simulator::Now ();
  NS_LOG_INFO ("EnergyModel: Node " << id << " registered capacity=" << m_capacityMah << "mAh");
}

void
EnergyModel::UpdateEnergy (NodeId id, double txCurrentMa, double durationSeconds)
{
  NodeEnergyState& state = EnsureNode (id);
  ApplyIdleConsumption (state);
  double current = (txCurrentMa > 0.0) ? txCurrentMa : m_txCurrentMa;
  double consumedMah = current * durationSeconds / 3600.0;
  state.remainingMah = std::max (0.0, state.remainingMah - consumedMah);
  state.lastUpdate = Simulator::Now ();
  NS_LOG_DEBUG ("EnergyModel: Node " << id << " TX consumed=" << consumedMah
                << "mAh remaining=" << state.remainingMah);
}

void
EnergyModel::UpdateRxEnergy (NodeId id, double durationSeconds)
{
  NodeEnergyState& state = EnsureNode (id);
  ApplyIdleConsumption (state);
  double consumedMah = m_rxCurrentMa * durationSeconds / 3600.0;
  state.remainingMah = std::max (0.0, state.remainingMah - consumedMah);
  state.lastUpdate = Simulator::Now ();
  NS_LOG_DEBUG ("EnergyModel: Node " << id << " RX consumed=" << consumedMah
                << "mAh remaining=" << state.remainingMah);
}

double
EnergyModel::GetRemainingEnergy (NodeId id)
{
  NodeEnergyState& state = EnsureNode (id);
  ApplyIdleConsumption (state);
  double remainingAh = state.remainingMah / 1000.0;
  double avgVoltage = (m_voltageMinMv + m_voltageMaxMv) / 2000.0; // convert to volts
  double energyJoules = remainingAh * avgVoltage * 3600.0;
  return std::max (0.0, energyJoules);
}

double
EnergyModel::GetEnergyFraction (NodeId id)
{
  NodeEnergyState& state = EnsureNode (id);
  ApplyIdleConsumption (state);
  return std::clamp (state.remainingMah / m_capacityMah, 0.0, 1.0);
}

double
EnergyModel::GetVoltageMv (NodeId id)
{
  double fraction = GetEnergyFraction (id);
  double span = m_voltageMaxMv - m_voltageMinMv;
  return m_voltageMinMv + span * fraction;
}

void
EnergyModel::SetCapacityMah (double capacity)
{
  m_capacityMah = std::max (capacity, 1.0);
}

void
EnergyModel::SetTxCurrentMa (double current)
{
  m_txCurrentMa = std::max (current, 0.0);
}

void
EnergyModel::SetRxCurrentMa (double current)
{
  m_rxCurrentMa = std::max (current, 0.0);
}

void
EnergyModel::SetIdleCurrentMa (double current)
{
  m_idleCurrentMa = std::max (current, 0.0);
}

void
EnergyModel::SetVoltageWindow (double minMv, double maxMv)
{
  m_voltageMinMv = std::min (minMv, maxMv);
  m_voltageMaxMv = std::max (minMv, maxMv);
}

EnergyModel::NodeEnergyState&
EnergyModel::EnsureNode (NodeId id)
{
  auto it = m_states.find (id);
  if (it == m_states.end ())
    {
      NodeEnergyState state;
      state.remainingMah = m_capacityMah;
      state.lastUpdate = Simulator::Now ();
      it = m_states.emplace (id, state).first;
    }
  return it->second;
}

void
EnergyModel::ApplyIdleConsumption (NodeEnergyState& state)
{
  Time now = Simulator::Now ();
  if (state.lastUpdate >= now)
    {
      return;
    }
  double dt = (now - state.lastUpdate).GetSeconds ();
  double consumedMah = m_idleCurrentMa * dt / 3600.0;
  state.remainingMah = std::max (0.0, state.remainingMah - consumedMah);
  state.lastUpdate = now;
}

} // namespace loramesh
} // namespace ns3
