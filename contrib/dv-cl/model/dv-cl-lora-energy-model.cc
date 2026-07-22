/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-lora-energy-model.h"

#include "ns3/double.h"
#include "ns3/log.h"
#include "ns3/simulator.h"

#include <algorithm>

namespace ns3
{
namespace dvcl
{

NS_LOG_COMPONENT_DEFINE("DvClLoraEnergyModel");
NS_OBJECT_ENSURE_REGISTERED(DvClLoraEnergyModel);

TypeId
DvClLoraEnergyModel::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClLoraEnergyModel")
            .SetParent<energy::DeviceEnergyModel>()
            .SetGroupName("DvCl")
            .AddConstructor<DvClLoraEnergyModel>()
            .AddTraceSource("EnergyDepleted",
                            "Fired once when the ledger empties: (nodeId, remainingFraction).",
                            MakeTraceSourceAccessor(&DvClLoraEnergyModel::m_energyDepletedTrace),
                            "ns3::dvcl::DvClLoraEnergyModel::EnergyDepletedCallback");
    return tid;
}

DvClLoraEnergyModel::DvClLoraEnergyModel()
    : m_source(nullptr),
      m_capacityMah(kDefaultCapacityMah),
      m_remainingMah(kDefaultCapacityMah),
      m_lastUpdate(Seconds(0)),
      m_txMah(0.0),
      m_rxMah(0.0),
      m_cadMah(0.0),
      m_idleMah(0.0),
      m_txSec(0.0),
      m_rxSec(0.0),
      m_cadSec(0.0),
      m_idleSec(0.0),
      m_depleted(false),
      m_txCurrentMa(kDefaultTxCurrentMa),
      m_rxCurrentMa(kDefaultRxCurrentMa),
      m_cadCurrentMa(kDefaultCadCurrentMa),
      m_idleCurrentMa(kDefaultIdleCurrentMa),
      m_voltageMinMv(kDefaultVoltageMinMv),
      m_voltageMaxMv(kDefaultVoltageMaxMv),
      m_instantState(DvClRadioState::IDLE)
{
    NS_LOG_FUNCTION(this);
}

// --- ns-3 DeviceEnergyModel interface -------------------------------------

void
DvClLoraEnergyModel::SetEnergySource(Ptr<energy::EnergySource> source)
{
    NS_LOG_FUNCTION(this << source);
    m_source = source;
}

double
DvClLoraEnergyModel::GetTotalEnergyConsumption() const
{
    // Joules spent = charge drawn (mAh) x terminal voltage x 3.6.
    const double spentMah = m_txMah + m_rxMah + m_cadMah + m_idleMah;
    return spentMah * GetSupplyVoltageV() * 3.6;
}

void
DvClLoraEnergyModel::ChangeState(int newState)
{
    // The instantaneous state is what a source would integrate through
    // DoGetCurrentA. Charge itself is event-driven (ChargeTx/Rx/Cad), so this
    // does not debit -- it only records what the radio is doing right now.
    if (newState >= 0 && newState <= static_cast<int>(DvClRadioState::SLEEP))
    {
        m_instantState = static_cast<DvClRadioState>(newState);
    }
}

void
DvClLoraEnergyModel::HandleEnergyDepletion()
{
    NS_LOG_FUNCTION(this);
    m_depleted = true;
    m_instantState = DvClRadioState::SLEEP;
}

void
DvClLoraEnergyModel::HandleEnergyRecharged()
{
    NS_LOG_FUNCTION(this);
    m_depleted = false;
}

void
DvClLoraEnergyModel::HandleEnergyChanged()
{
    NS_LOG_FUNCTION(this);
}

double
DvClLoraEnergyModel::DoGetCurrentA() const
{
    if (m_depleted)
    {
        return 0.0;
    }
    switch (m_instantState)
    {
    case DvClRadioState::TX:
        return m_txCurrentMa / 1000.0;
    case DvClRadioState::RX:
        return m_rxCurrentMa / 1000.0;
    case DvClRadioState::CAD:
        return m_cadCurrentMa / 1000.0;
    case DvClRadioState::SLEEP:
        return 0.0;
    case DvClRadioState::IDLE:
    default:
        return m_idleCurrentMa / 1000.0;
    }
}

// --- charge accounting ----------------------------------------------------

void
DvClLoraEnergyModel::Charge(double durationSeconds,
                            double currentMa,
                            double& categoryMah,
                            double& categorySec)
{
    if (durationSeconds <= 0.0)
    {
        return;
    }
    ApplyIdleConsumption();
    const double wantMah = currentMa * durationSeconds / 3600.0;
    // A flat battery draws nothing: charge only what was left. Booking the
    // nominal draw past zero grew the lifetime counters after a node died and
    // stopped the ledger from closing (VALIDATION.md, 2026-07-22).
    const double drawnMah = std::min(wantMah, m_remainingMah);
    m_remainingMah -= drawnMah;
    categoryMah += drawnMah;
    categorySec += durationSeconds;
    m_lastUpdate = Simulator::Now();
    MaybeNotifyDepleted();
}

void
DvClLoraEnergyModel::ChargeTx(double durationSeconds)
{
    Charge(durationSeconds, m_txCurrentMa, m_txMah, m_txSec);
}

void
DvClLoraEnergyModel::ChargeRx(double durationSeconds)
{
    Charge(durationSeconds, m_rxCurrentMa, m_rxMah, m_rxSec);
}

void
DvClLoraEnergyModel::ChargeCad(double durationSeconds)
{
    Charge(durationSeconds, m_cadCurrentMa, m_cadMah, m_cadSec);
}

void
DvClLoraEnergyModel::ApplyIdleConsumption()
{
    const Time now = Simulator::Now();
    if (m_lastUpdate >= now)
    {
        return;
    }
    const double dt = (now - m_lastUpdate).GetSeconds();
    const double wantMah = m_idleCurrentMa * dt / 3600.0;
    const double drawnMah = std::min(wantMah, m_remainingMah);
    m_remainingMah -= drawnMah;
    m_idleMah += drawnMah;
    m_idleSec += dt;
    m_lastUpdate = now;
    MaybeNotifyDepleted();
}

void
DvClLoraEnergyModel::MaybeNotifyDepleted()
{
    if (!m_depleted && m_remainingMah <= 0.0)
    {
        m_depleted = true;
        m_instantState = DvClRadioState::SLEEP;
        const uint32_t nodeId =
            (m_source && m_source->GetNode()) ? m_source->GetNode()->GetId() : 0;
        m_energyDepletedTrace(nodeId, 0.0);
    }
}

// --- seeding --------------------------------------------------------------

void
DvClLoraEnergyModel::SetInitialSocFraction(double fraction)
{
    const double clamped = std::clamp(fraction, 0.0, 1.0);
    m_remainingMah = clamped * m_capacityMah;
    m_lastUpdate = Simulator::Now();
    m_depleted = (m_remainingMah <= 0.0);
}

// --- authoritative queries ------------------------------------------------

double
DvClLoraEnergyModel::GetEnergyFraction() const
{
    // Const query, but idle draw must be current: fold in the gap since the
    // last event without mutating observable category totals beyond idle.
    const Time now = Simulator::Now();
    double remaining = m_remainingMah;
    if (now > m_lastUpdate)
    {
        const double dt = (now - m_lastUpdate).GetSeconds();
        const double wantMah = m_idleCurrentMa * dt / 3600.0;
        remaining -= std::min(wantMah, remaining);
    }
    return std::clamp(remaining / m_capacityMah, 0.0, 1.0);
}

double
DvClLoraEnergyModel::GetRemainingEnergyJ() const
{
    return std::max(0.0, GetEnergyFraction() * m_capacityMah / 1000.0 * GetSupplyVoltageV() * 3600.0);
}

double
DvClLoraEnergyModel::GetVoltageMv() const
{
    const double span = m_voltageMaxMv - m_voltageMinMv;
    return m_voltageMinMv + span * GetEnergyFraction();
}

double
DvClLoraEnergyModel::GetSupplyVoltageV() const
{
    if (m_source)
    {
        return m_source->GetSupplyVoltage();
    }
    return (m_voltageMinMv + m_voltageMaxMv) / 2000.0; // mV midpoint -> V
}

double
DvClLoraEnergyModel::GetTxMah() const
{
    const_cast<DvClLoraEnergyModel*>(this)->ApplyIdleConsumption();
    return m_txMah;
}

double
DvClLoraEnergyModel::GetRxMah() const
{
    const_cast<DvClLoraEnergyModel*>(this)->ApplyIdleConsumption();
    return m_rxMah;
}

double
DvClLoraEnergyModel::GetCadMah() const
{
    const_cast<DvClLoraEnergyModel*>(this)->ApplyIdleConsumption();
    return m_cadMah;
}

double
DvClLoraEnergyModel::GetIdleMah() const
{
    const_cast<DvClLoraEnergyModel*>(this)->ApplyIdleConsumption();
    return m_idleMah;
}

Time
DvClLoraEnergyModel::GetTimeInState(DvClRadioState s) const
{
    switch (s)
    {
    case DvClRadioState::TX:
        return Seconds(m_txSec);
    case DvClRadioState::RX:
        return Seconds(m_rxSec);
    case DvClRadioState::CAD:
        return Seconds(m_cadSec);
    case DvClRadioState::IDLE:
        return Seconds(m_idleSec);
    case DvClRadioState::SLEEP:
    default:
        return Seconds(0.0);
    }
}

// --- configuration --------------------------------------------------------

void
DvClLoraEnergyModel::SetCapacityMah(double capacity)
{
    m_capacityMah = std::max(capacity, 1.0);
    m_remainingMah = std::min(m_remainingMah, m_capacityMah);
}

void
DvClLoraEnergyModel::SetTxCurrentMa(double current)
{
    m_txCurrentMa = std::max(current, 0.0);
}

void
DvClLoraEnergyModel::SetRxCurrentMa(double current)
{
    m_rxCurrentMa = std::max(current, 0.0);
}

void
DvClLoraEnergyModel::SetCadCurrentMa(double current)
{
    m_cadCurrentMa = std::max(current, 0.0);
}

void
DvClLoraEnergyModel::SetIdleCurrentMa(double current)
{
    m_idleCurrentMa = std::max(current, 0.0);
}

void
DvClLoraEnergyModel::SetVoltageWindow(double minMv, double maxMv)
{
    m_voltageMinMv = std::min(minMv, maxMv);
    m_voltageMaxMv = std::max(minMv, maxMv);
}

} // namespace dvcl
} // namespace ns3
