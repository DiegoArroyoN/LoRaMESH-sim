/* SPDX-License-Identifier: GPL-2.0-only */

/*
 * LoRa Device Energy Model Implementation
 */

#include "dv-cl-lora-energy-model.h"

#include "ns3/double.h"
#include "ns3/log.h"
#include "ns3/simulator.h"
#include "ns3/trace-source-accessor.h"

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
            .AddAttribute(
                "TxCurrentA",
                "TX current [A]. SX1276/77/78/79 DS: 120 mA at +20 dBm PA_BOOST.",
                DoubleValue(0.120), // 120 mA @ +20 dBm PA_BOOST [SX1276/77/78/79 DS, IDD_TXLORA]
                MakeDoubleAccessor(&DvClLoraEnergyModel::SetTxCurrentA,
                                   &DvClLoraEnergyModel::GetTxCurrentA),
                MakeDoubleChecker<double>())
            .AddAttribute(
                "AutoTxCurrentFromPower",
                "If true, TX current is auto-adjusted from txPowerDbm using anchor values.",
                BooleanValue(false), // 20 dBm only; no interpolation needed
                MakeBooleanAccessor(&DvClLoraEnergyModel::SetAutoTxCurrentFromPower,
                                    &DvClLoraEnergyModel::GetAutoTxCurrentFromPower),
                MakeBooleanChecker())
            .AddAttribute("TxCurrentAt14dBmA",
                          "Anchor TX current [A] at 14 dBm (unused: sim operates at 20 dBm only).",
                          DoubleValue(0.120), // set equal to 20 dBm anchor; unused in practice
                          MakeDoubleAccessor(&DvClLoraEnergyModel::SetTxCurrentAt14dBmA,
                                             &DvClLoraEnergyModel::GetTxCurrentAt14dBmA),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("TxCurrentAt20dBmA",
                          "Anchor TX current [A] at 20 dBm.",
                          DoubleValue(0.120),
                          MakeDoubleAccessor(&DvClLoraEnergyModel::SetTxCurrentAt20dBmA,
                                             &DvClLoraEnergyModel::GetTxCurrentAt20dBmA),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute(
                "RxCurrentA",
                "The current draw in Amperes during RX mode",
                DoubleValue(0.0103), // 10.3 mA, LoRa BW=125 kHz [SX1276/77/78/79 DS, IDD_RXLORA]
                MakeDoubleAccessor(&DvClLoraEnergyModel::SetRxCurrentA,
                                   &DvClLoraEnergyModel::GetRxCurrentA),
                MakeDoubleChecker<double>())
            .AddAttribute("CadCurrentA",
                          "The current draw in Amperes during CAD mode",
                          DoubleValue(0.0103), // 10.3 mA, same RX circuitry [SX1276/77/78/79 DS]
                          MakeDoubleAccessor(&DvClLoraEnergyModel::SetCadCurrentA,
                                             &DvClLoraEnergyModel::GetCadCurrentA),
                          MakeDoubleChecker<double>())
            .AddAttribute("IdleCurrentA",
                          "The current draw in Amperes during IDLE mode",
                          DoubleValue(0.0016), // 1.6 mA standby [SX1276/77/78/79 DS, IDD_STDB]
                          MakeDoubleAccessor(&DvClLoraEnergyModel::SetIdleCurrentA,
                                             &DvClLoraEnergyModel::GetIdleCurrentA),
                          MakeDoubleChecker<double>())
            .AddAttribute("SleepCurrentA",
                          "The current draw in Amperes during SLEEP mode",
                          DoubleValue(0.0000002), // 0.2 uA [SX1276/77/78/79 DS, IDD_SLEEP]
                          MakeDoubleAccessor(&DvClLoraEnergyModel::SetSleepCurrentA,
                                             &DvClLoraEnergyModel::GetSleepCurrentA),
                          MakeDoubleChecker<double>())
            .AddTraceSource(
                "TotalEnergyConsumption",
                "Total energy consumption in Joules",
                MakeTraceSourceAccessor(&DvClLoraEnergyModel::m_totalEnergyConsumptionTrace),
                "ns3::TracedValueCallback::Double");
    ;
    static bool traceRegistered =
        (tid.AddTraceSource("EnergyDepleted",
                            "Node ran out of energy: (nodeId, remaining fraction).",
                            MakeTraceSourceAccessor(&DvClLoraEnergyModel::m_energyDepletedTrace),
                            "ns3::dvcl::DvClLoraEnergyModel::EnergyDepletedCallback"),
         true);
    (void)traceRegistered;
    return tid;
}

DvClLoraEnergyModel::DvClLoraEnergyModel()
    : m_source(nullptr),
      m_node(nullptr),
      m_txCurrentA(0.120),             // 120 mA @ +20 dBm PA_BOOST [SX1276/77/78/79 DS]
      m_rxCurrentA(0.0103),            //  10.3 mA LoRa BW=125 kHz   [SX1276/77/78/79 DS]
      m_cadCurrentA(0.0103),           //  10.3 mA (same RX circuits) [SX1276/77/78/79 DS]
      m_idleCurrentA(0.0016),          //   1.6 mA standby            [SX1276/77/78/79 DS]
      m_sleepCurrentA(0.0000002),      //   0.2 uA sleep              [SX1276/77/78/79 DS]
      m_autoTxCurrentFromPower(false), // 20 dBm only, no interpolation
      m_txCurrentAt14dBmA(0.120),      // unused (sim uses 20 dBm only)
      m_txCurrentAt20dBmA(0.120),      // 120 mA @ +20 dBm PA_BOOST [SX1276/77/78/79 DS]
      m_lastTxPowerDbm(14.0),
      m_currentState(DvClRadioState::IDLE),
      m_depleted(false),
      m_lastUpdateTime(Seconds(0)),
      m_totalEnergyConsumption(0.0)
{
    NS_LOG_FUNCTION(this);
}

DvClLoraEnergyModel::~DvClLoraEnergyModel()
{
    NS_LOG_FUNCTION(this);
}

void
DvClLoraEnergyModel::SetEnergySource(Ptr<energy::EnergySource> source)
{
    NS_LOG_FUNCTION(this << source);
    m_source = source;
    m_lastUpdateTime = Simulator::Now();
}

double
DvClLoraEnergyModel::GetTotalEnergyConsumption() const
{
    NS_LOG_FUNCTION(this);
    return m_totalEnergyConsumption;
}

void
DvClLoraEnergyModel::ChangeState(int newState)
{
    NS_LOG_FUNCTION(this << newState);

    // Update energy consumption for time spent in previous state
    UpdateEnergyConsumption();

    // Change to new state
    m_currentState = static_cast<DvClRadioState>(newState);

    NS_LOG_DEBUG("DvClLoraEnergyModel: State changed to "
                 << newState << " current=" << DoGetCurrentA() * 1000.0 << " mA");
}

void
DvClLoraEnergyModel::HandleEnergyDepletion()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_WARN("DvClLoraEnergyModel: Energy depleted on node " << (m_node ? m_node->GetId() : 0));

    // Module-clean: report through a TraceSource instead of a global collector.
    if (m_node && m_source)
    {
        const double frac = m_source->GetRemainingEnergy() / m_source->GetInitialEnergy();
        m_energyDepletedTrace(m_node->GetId(), frac);
    }

    // Disable the radio, and stop drawing: the state change must come first so
    // the time up to this instant is still charged at the live current.
    ChangeState(static_cast<int>(DvClRadioState::SLEEP));
    m_depleted = true;
}

void
DvClLoraEnergyModel::HandleEnergyRecharged()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("DvClLoraEnergyModel: Energy recharged on node " << (m_node ? m_node->GetId() : 0));
    m_depleted = false;
}

void
DvClLoraEnergyModel::HandleEnergyChanged()
{
    NS_LOG_FUNCTION(this);
    // No action needed for now
}

Time
DvClLoraEnergyModel::GetTimeInState(DvClRadioState s) const
{
    const int idx = static_cast<int>(s);
    if (idx < 0 || idx >= 8)
    {
        return Time(0);
    }
    return m_stateTimes[idx];
}

double
DvClLoraEnergyModel::DoGetCurrentA() const
{
    if (m_depleted)
    {
        return 0.0;
    }
    switch (m_currentState)
    {
    case DvClRadioState::TX:
        return m_txCurrentA;
    case DvClRadioState::RX:
        return m_rxCurrentA;
    case DvClRadioState::CAD:
        return m_cadCurrentA;
    case DvClRadioState::IDLE:
        return m_idleCurrentA;
    case DvClRadioState::SLEEP:
        return m_sleepCurrentA;
    default:
        return m_idleCurrentA;
    }
}

void
DvClLoraEnergyModel::UpdateEnergyConsumption()
{
    NS_LOG_FUNCTION(this);

    Time now = Simulator::Now();
    Time duration = now - m_lastUpdateTime;
    {
        const int idx = static_cast<int>(m_currentState);
        if (idx >= 0 && idx < 8)
        {
            m_stateTimes[idx] += duration; // F1.3b per-state ledger
        }
    }

    if (duration.IsPositive() && m_source)
    {
        double current = DoGetCurrentA();
        double voltage = m_source->GetSupplyVoltage();
        double energyJoules = current * voltage * duration.GetSeconds();

        m_totalEnergyConsumption += energyJoules;
        m_totalEnergyConsumptionTrace = m_totalEnergyConsumption;

        // Decrease energy from source
        m_source->UpdateEnergySource();

        NS_LOG_DEBUG("DvClLoraEnergyModel: duration="
                     << duration.GetSeconds() << "s current=" << current * 1000.0 << "mA"
                     << " energy=" << energyJoules * 1000.0 << "mJ"
                     << " total=" << m_totalEnergyConsumption << "J");
    }

    m_lastUpdateTime = now;
}

// Setters
void
DvClLoraEnergyModel::SetTxCurrentA(double currentA)
{
    m_txCurrentA = currentA;
}

void
DvClLoraEnergyModel::SetRxCurrentA(double currentA)
{
    m_rxCurrentA = currentA;
}

void
DvClLoraEnergyModel::SetCadCurrentA(double currentA)
{
    m_cadCurrentA = currentA;
}

void
DvClLoraEnergyModel::SetIdleCurrentA(double currentA)
{
    m_idleCurrentA = currentA;
}

void
DvClLoraEnergyModel::SetSleepCurrentA(double currentA)
{
    m_sleepCurrentA = currentA;
}

void
DvClLoraEnergyModel::SetAutoTxCurrentFromPower(bool enable)
{
    m_autoTxCurrentFromPower = enable;
    if (m_autoTxCurrentFromPower)
    {
        m_txCurrentA =
            EstimateTxCurrentA(m_lastTxPowerDbm, m_txCurrentAt14dBmA, m_txCurrentAt20dBmA);
    }
}

bool
DvClLoraEnergyModel::GetAutoTxCurrentFromPower() const
{
    return m_autoTxCurrentFromPower;
}

void
DvClLoraEnergyModel::SetTxCurrentAt14dBmA(double currentA)
{
    m_txCurrentAt14dBmA = currentA;
    if (m_autoTxCurrentFromPower)
    {
        m_txCurrentA =
            EstimateTxCurrentA(m_lastTxPowerDbm, m_txCurrentAt14dBmA, m_txCurrentAt20dBmA);
    }
}

double
DvClLoraEnergyModel::GetTxCurrentAt14dBmA() const
{
    return m_txCurrentAt14dBmA;
}

void
DvClLoraEnergyModel::SetTxCurrentAt20dBmA(double currentA)
{
    m_txCurrentAt20dBmA = currentA;
    if (m_autoTxCurrentFromPower)
    {
        m_txCurrentA =
            EstimateTxCurrentA(m_lastTxPowerDbm, m_txCurrentAt14dBmA, m_txCurrentAt20dBmA);
    }
}

double
DvClLoraEnergyModel::GetTxCurrentAt20dBmA() const
{
    return m_txCurrentAt20dBmA;
}

void
DvClLoraEnergyModel::SetTxPowerDbm(double txPowerDbm)
{
    m_lastTxPowerDbm = txPowerDbm;
    if (m_autoTxCurrentFromPower)
    {
        m_txCurrentA = EstimateTxCurrentA(txPowerDbm, m_txCurrentAt14dBmA, m_txCurrentAt20dBmA);
    }
}

double
DvClLoraEnergyModel::GetLastTxPowerDbm() const
{
    return m_lastTxPowerDbm;
}

double
DvClLoraEnergyModel::EstimateTxCurrentA(double txPowerDbm,
                                        double txCurrentAt14dBmA,
                                        double txCurrentAt20dBmA)
{
    // Piecewise-linear estimate anchored at SX1276 typical values:
    // 14 dBm -> txCurrentAt14dBmA, 20 dBm -> txCurrentAt20dBmA.
    // Extrapolates down to 2 dBm and clamps the final result to positive range.
    const double clampedDbm = std::max(2.0, std::min(20.0, txPowerDbm));
    const double slope = (txCurrentAt20dBmA - txCurrentAt14dBmA) / 6.0;
    const double estimated = txCurrentAt14dBmA + slope * (clampedDbm - 14.0);
    return std::max(0.0, estimated);
}

// Getters
double
DvClLoraEnergyModel::GetTxCurrentA() const
{
    return m_txCurrentA;
}

double
DvClLoraEnergyModel::GetRxCurrentA() const
{
    return m_rxCurrentA;
}

double
DvClLoraEnergyModel::GetCadCurrentA() const
{
    return m_cadCurrentA;
}

double
DvClLoraEnergyModel::GetIdleCurrentA() const
{
    return m_idleCurrentA;
}

double
DvClLoraEnergyModel::GetSleepCurrentA() const
{
    return m_sleepCurrentA;
}

DvClRadioState
DvClLoraEnergyModel::GetCurrentState() const
{
    return m_currentState;
}

void
DvClLoraEnergyModel::SetNode(Ptr<Node> node)
{
    m_node = node;
}

Ptr<Node>
DvClLoraEnergyModel::GetNode() const
{
    return m_node;
}

double
DvClLoraEnergyModel::GetRemainingEnergyJ() const
{
    if (!m_source)
    {
        return -1.0;
    }
    return m_source->GetRemainingEnergy();
}

double
DvClLoraEnergyModel::GetEnergyFraction() const
{
    if (!m_source)
    {
        return -1.0;
    }
    const double initial = m_source->GetInitialEnergy();
    if (initial <= 0.0)
    {
        return 0.0;
    }
    return m_source->GetRemainingEnergy() / initial;
}

double
DvClLoraEnergyModel::GetSupplyVoltageV() const
{
    if (!m_source)
    {
        return -1.0;
    }
    return m_source->GetSupplyVoltage();
}

} // namespace dvcl
} // namespace ns3
