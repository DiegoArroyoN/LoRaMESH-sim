/*
 * LoRa Device Energy Model Implementation
 */

#include "lora-device-energy-model.h"

#include "metrics_collector.h"

#include "ns3/double.h"
#include "ns3/log.h"
#include "ns3/simulator.h"
#include "ns3/trace-source-accessor.h"

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("LoRaDeviceEnergyModel");
NS_OBJECT_ENSURE_REGISTERED(LoRaDeviceEnergyModel);

TypeId
LoRaDeviceEnergyModel::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::LoRaDeviceEnergyModel")
            .SetParent<energy::DeviceEnergyModel>()
            .SetGroupName("Energy")
            .AddConstructor<LoRaDeviceEnergyModel>()
            .AddAttribute("TxCurrentA",
                          "The current draw in Amperes during TX mode",
                          DoubleValue(0.100), // 100 mA @ 14dBm (SX1276 Table 6, PA_BOOST)
                          MakeDoubleAccessor(&LoRaDeviceEnergyModel::SetTxCurrentA,
                                             &LoRaDeviceEnergyModel::GetTxCurrentA),
                          MakeDoubleChecker<double>())
            .AddAttribute("RxCurrentA",
                          "The current draw in Amperes during RX mode",
                          DoubleValue(0.011), // 11 mA
                          MakeDoubleAccessor(&LoRaDeviceEnergyModel::SetRxCurrentA,
                                             &LoRaDeviceEnergyModel::GetRxCurrentA),
                          MakeDoubleChecker<double>())
            .AddAttribute("CadCurrentA",
                          "The current draw in Amperes during CAD mode",
                          DoubleValue(0.011), // 11 mA (similar to RX)
                          MakeDoubleAccessor(&LoRaDeviceEnergyModel::SetCadCurrentA,
                                             &LoRaDeviceEnergyModel::GetCadCurrentA),
                          MakeDoubleChecker<double>())
            .AddAttribute("IdleCurrentA",
                          "The current draw in Amperes during IDLE mode",
                          DoubleValue(0.001), // 1 mA
                          MakeDoubleAccessor(&LoRaDeviceEnergyModel::SetIdleCurrentA,
                                             &LoRaDeviceEnergyModel::GetIdleCurrentA),
                          MakeDoubleChecker<double>())
            .AddAttribute("SleepCurrentA",
                          "The current draw in Amperes during SLEEP mode",
                          DoubleValue(0.0000002), // 0.2 µA
                          MakeDoubleAccessor(&LoRaDeviceEnergyModel::SetSleepCurrentA,
                                             &LoRaDeviceEnergyModel::GetSleepCurrentA),
                          MakeDoubleChecker<double>())
            .AddTraceSource(
                "TotalEnergyConsumption",
                "Total energy consumption in Joules",
                MakeTraceSourceAccessor(&LoRaDeviceEnergyModel::m_totalEnergyConsumptionTrace),
                "ns3::TracedValueCallback::Double");
    return tid;
}

LoRaDeviceEnergyModel::LoRaDeviceEnergyModel()
    : m_source(nullptr),
      m_node(nullptr),
      m_txCurrentA(0.100),
      m_rxCurrentA(0.011),
      m_cadCurrentA(0.011),
      m_idleCurrentA(0.001),
      m_sleepCurrentA(0.0000002),
      m_currentState(LoRaRadioState::IDLE),
      m_lastUpdateTime(Seconds(0)),
      m_totalEnergyConsumption(0.0)
{
    NS_LOG_FUNCTION(this);
}

LoRaDeviceEnergyModel::~LoRaDeviceEnergyModel()
{
    NS_LOG_FUNCTION(this);
}

void
LoRaDeviceEnergyModel::SetEnergySource(Ptr<energy::EnergySource> source)
{
    NS_LOG_FUNCTION(this << source);
    m_source = source;
    m_lastUpdateTime = Simulator::Now();
}

double
LoRaDeviceEnergyModel::GetTotalEnergyConsumption() const
{
    NS_LOG_FUNCTION(this);
    return m_totalEnergyConsumption;
}

void
LoRaDeviceEnergyModel::ChangeState(int newState)
{
    NS_LOG_FUNCTION(this << newState);

    // Update energy consumption for time spent in previous state
    UpdateEnergyConsumption();

    // Change to new state
    m_currentState = static_cast<LoRaRadioState>(newState);

    NS_LOG_DEBUG("LoRaDeviceEnergyModel: State changed to "
                 << newState << " current=" << DoGetCurrentA() * 1000.0 << " mA");
}

void
LoRaDeviceEnergyModel::HandleEnergyDepletion()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_WARN("LoRaDeviceEnergyModel: Energy depleted on node "
                << (m_node ? m_node->GetId() : 0));

    // Record node death for FND metric
    if (g_metricsCollector && m_node && m_source)
    {
        double frac = m_source->GetRemainingEnergy() / m_source->GetInitialEnergy();
        g_metricsCollector->RecordNodeDeath(m_node->GetId(), frac, "energy_depleted");
    }

    // Disable the radio
    ChangeState(static_cast<int>(LoRaRadioState::SLEEP));
}

void
LoRaDeviceEnergyModel::HandleEnergyRecharged()
{
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("LoRaDeviceEnergyModel: Energy recharged on node "
                << (m_node ? m_node->GetId() : 0));
}

void
LoRaDeviceEnergyModel::HandleEnergyChanged()
{
    NS_LOG_FUNCTION(this);
    // No action needed for now
}

double
LoRaDeviceEnergyModel::DoGetCurrentA() const
{
    switch (m_currentState)
    {
    case LoRaRadioState::TX:
        return m_txCurrentA;
    case LoRaRadioState::RX:
        return m_rxCurrentA;
    case LoRaRadioState::CAD:
        return m_cadCurrentA;
    case LoRaRadioState::IDLE:
        return m_idleCurrentA;
    case LoRaRadioState::SLEEP:
        return m_sleepCurrentA;
    default:
        return m_idleCurrentA;
    }
}

void
LoRaDeviceEnergyModel::UpdateEnergyConsumption()
{
    NS_LOG_FUNCTION(this);

    Time now = Simulator::Now();
    Time duration = now - m_lastUpdateTime;

    if (duration.IsPositive() && m_source)
    {
        double current = DoGetCurrentA();
        double voltage = m_source->GetSupplyVoltage();
        double energyJoules = current * voltage * duration.GetSeconds();

        m_totalEnergyConsumption += energyJoules;
        m_totalEnergyConsumptionTrace = m_totalEnergyConsumption;

        // Decrease energy from source
        m_source->UpdateEnergySource();

        NS_LOG_DEBUG("LoRaDeviceEnergyModel: duration="
                     << duration.GetSeconds() << "s current=" << current * 1000.0 << "mA"
                     << " energy=" << energyJoules * 1000.0 << "mJ"
                     << " total=" << m_totalEnergyConsumption << "J");
    }

    m_lastUpdateTime = now;
}

// Setters
void
LoRaDeviceEnergyModel::SetTxCurrentA(double currentA)
{
    m_txCurrentA = currentA;
}

void
LoRaDeviceEnergyModel::SetRxCurrentA(double currentA)
{
    m_rxCurrentA = currentA;
}

void
LoRaDeviceEnergyModel::SetCadCurrentA(double currentA)
{
    m_cadCurrentA = currentA;
}

void
LoRaDeviceEnergyModel::SetIdleCurrentA(double currentA)
{
    m_idleCurrentA = currentA;
}

void
LoRaDeviceEnergyModel::SetSleepCurrentA(double currentA)
{
    m_sleepCurrentA = currentA;
}

// Getters
double
LoRaDeviceEnergyModel::GetTxCurrentA() const
{
    return m_txCurrentA;
}

double
LoRaDeviceEnergyModel::GetRxCurrentA() const
{
    return m_rxCurrentA;
}

double
LoRaDeviceEnergyModel::GetCadCurrentA() const
{
    return m_cadCurrentA;
}

double
LoRaDeviceEnergyModel::GetIdleCurrentA() const
{
    return m_idleCurrentA;
}

double
LoRaDeviceEnergyModel::GetSleepCurrentA() const
{
    return m_sleepCurrentA;
}

LoRaRadioState
LoRaDeviceEnergyModel::GetCurrentState() const
{
    return m_currentState;
}

void
LoRaDeviceEnergyModel::SetNode(Ptr<Node> node)
{
    m_node = node;
}

Ptr<Node>
LoRaDeviceEnergyModel::GetNode() const
{
    return m_node;
}

double
LoRaDeviceEnergyModel::GetRemainingEnergyJ() const
{
    if (!m_source)
    {
        return -1.0;
    }
    return m_source->GetRemainingEnergy();
}

double
LoRaDeviceEnergyModel::GetEnergyFraction() const
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
LoRaDeviceEnergyModel::GetSupplyVoltageV() const
{
    if (!m_source)
    {
        return -1.0;
    }
    return m_source->GetSupplyVoltage();
}

} // namespace ns3
