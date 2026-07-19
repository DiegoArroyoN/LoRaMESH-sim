/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-energy-registry.h"

#include "ns3/log.h"
#include "ns3/simulator.h"

#include <algorithm>

namespace ns3
{
namespace dvcl
{

NS_LOG_COMPONENT_DEFINE ("LoraMeshEnergyModel");
NS_OBJECT_ENSURE_REGISTERED (DvClEnergyRegistry);

TypeId
DvClEnergyRegistry::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::dvcl::DvClEnergyRegistry")
    .SetParent<Object> ()
    .AddConstructor<DvClEnergyRegistry> ();
  return tid;
}

DvClEnergyRegistry::DvClEnergyRegistry ()
  : m_capacityMah (kDefaultCapacityMah),
    m_txCurrentMa (kDefaultTxCurrentMa),
    m_rxCurrentMa (kDefaultRxCurrentMa),
    m_cadCurrentMa (kDefaultCadCurrentMa),
    m_idleCurrentMa (kDefaultIdleCurrentMa),
    m_voltageMinMv (kDefaultVoltageMinMv),
    m_voltageMaxMv (kDefaultVoltageMaxMv)
{
}

void
DvClEnergyRegistry::RegisterNode (NodeId id)
{
  auto it = m_states.find (id);
  if (it == m_states.end ())
    {
      NodeEnergyState state;
      state.remainingMah = m_capacityMah;
      state.lastUpdate = Simulator::Now ();
      m_states.emplace (id, state);
      NS_LOG_INFO ("DvClEnergyRegistry: Node " << id << " registered capacity=" << m_capacityMah << "mAh");
      return;
    }

  it->second.lastUpdate = Simulator::Now ();
  NS_LOG_INFO ("DvClEnergyRegistry: Node " << id << " re-registered keeping remaining="
               << it->second.remainingMah << "mAh");
}

void
DvClEnergyRegistry::SetRemainingFraction (NodeId id, double fraction)
{
  NodeEnergyState& state = EnsureNode (id);
  const double clamped = std::clamp (fraction, 0.0, 1.0);
  state.remainingMah = clamped * m_capacityMah;
  state.lastUpdate = Simulator::Now ();
}

void
DvClEnergyRegistry::UpdateEnergy (NodeId id, double txCurrentMa, double durationSeconds)
{
  NodeEnergyState& state = EnsureNode (id);
  ApplyIdleConsumption (state);
  double current = (txCurrentMa > 0.0) ? txCurrentMa : m_txCurrentMa;
  double consumedMah = current * durationSeconds / 3600.0;
  state.remainingMah = std::max (0.0, state.remainingMah - consumedMah);
  state.lastUpdate = Simulator::Now ();
  NS_LOG_DEBUG ("DvClEnergyRegistry: Node " << id << " TX consumed=" << consumedMah
                << "mAh remaining=" << state.remainingMah);
}

void
DvClEnergyRegistry::UpdateRxEnergy (NodeId id, double durationSeconds)
{
  NodeEnergyState& state = EnsureNode (id);
  ApplyIdleConsumption (state);
  double consumedMah = m_rxCurrentMa * durationSeconds / 3600.0;
  state.remainingMah = std::max (0.0, state.remainingMah - consumedMah);
  state.lastUpdate = Simulator::Now ();
  NS_LOG_DEBUG ("DvClEnergyRegistry: Node " << id << " RX consumed=" << consumedMah
                << "mAh remaining=" << state.remainingMah);
}

void
DvClEnergyRegistry::UpdateCadEnergy (NodeId id, double durationSeconds)
{
  NodeEnergyState& state = EnsureNode (id);
  ApplyIdleConsumption (state);
  double consumedMah = m_cadCurrentMa * durationSeconds / 3600.0;
  state.remainingMah = std::max (0.0, state.remainingMah - consumedMah);
  state.lastUpdate = Simulator::Now ();
  NS_LOG_DEBUG ("DvClEnergyRegistry: Node " << id << " CAD consumed=" << consumedMah
                << "mAh remaining=" << state.remainingMah);
}

double
DvClEnergyRegistry::GetRemainingEnergy (NodeId id)
{
  NodeEnergyState& state = EnsureNode (id);
  ApplyIdleConsumption (state);
  double remainingAh = state.remainingMah / 1000.0;
  double avgVoltage = (m_voltageMinMv + m_voltageMaxMv) / 2000.0; // convert to volts
  double energyJoules = remainingAh * avgVoltage * 3600.0;
  return std::max (0.0, energyJoules);
}

double
DvClEnergyRegistry::GetEnergyFraction (NodeId id)
{
  NodeEnergyState& state = EnsureNode (id);
  ApplyIdleConsumption (state);
  return std::clamp (state.remainingMah / m_capacityMah, 0.0, 1.0);
}

double
DvClEnergyRegistry::GetVoltageMv (NodeId id)
{
  double fraction = GetEnergyFraction (id);
  double span = m_voltageMaxMv - m_voltageMinMv;
  return m_voltageMinMv + span * fraction;
}

void
DvClEnergyRegistry::SetCapacityMah (double capacity)
{
  m_capacityMah = std::max (capacity, 1.0);
}

void
DvClEnergyRegistry::SetTxCurrentMa (double current)
{
  m_txCurrentMa = std::max (current, 0.0);
}

void
DvClEnergyRegistry::SetRxCurrentMa (double current)
{
  m_rxCurrentMa = std::max (current, 0.0);
}

void
DvClEnergyRegistry::SetCadCurrentMa (double current)
{
  m_cadCurrentMa = std::max (current, 0.0);
}

void
DvClEnergyRegistry::SetIdleCurrentMa (double current)
{
  m_idleCurrentMa = std::max (current, 0.0);
}

void
DvClEnergyRegistry::SetVoltageWindow (double minMv, double maxMv)
{
  m_voltageMinMv = std::min (minMv, maxMv);
  m_voltageMaxMv = std::max (minMv, maxMv);
}

DvClEnergyRegistry::NodeEnergyState&
DvClEnergyRegistry::EnsureNode (NodeId id)
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
DvClEnergyRegistry::ApplyIdleConsumption (NodeEnergyState& state)
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

} // namespace dvcl
} // namespace ns3
