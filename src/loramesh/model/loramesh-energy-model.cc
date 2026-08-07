#include "loramesh-energy-model.h"
#include "ns3/log.h"
#include "ns3/simulator.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE ("LorameshEnergyModel");
NS_OBJECT_ENSURE_REGISTERED (LorameshEnergyModel);

TypeId
LorameshEnergyModel::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::LorameshEnergyModel")
    .SetParent<Object> ()
    .SetGroupName ("Loramesh")
    .AddConstructor<LorameshEnergyModel> ()
    .AddAttribute ("InitialEnergy",
                   "Initial energy of the battery in Joules",
                   DoubleValue (10000.0),
                   MakeDoubleAccessor (&LorameshEnergyModel::SetInitialEnergy),
                   MakeDoubleChecker<double> ())
    .AddAttribute ("SupplyVoltage",
                   "Supply voltage in Volts",
                   DoubleValue (3.3),
                   MakeDoubleAccessor (&LorameshEnergyModel::SetSupplyVoltage),
                   MakeDoubleChecker<double> ())
    .AddAttribute ("TxCurrent",
                   "Current consumption in TX state (A)",
                   DoubleValue (0.120), // 120mA
                   MakeDoubleAccessor (&LorameshEnergyModel::SetTxCurrent),
                   MakeDoubleChecker<double> ())
    .AddAttribute ("RxCurrent",
                   "Current consumption in RX state (A)",
                   DoubleValue (0.015), // 15mA
                   MakeDoubleAccessor (&LorameshEnergyModel::SetRxCurrent),
                   MakeDoubleChecker<double> ())
    .AddAttribute ("IdleCurrent",
                   "Current consumption in IDLE state (A)",
                   DoubleValue (0.001), // 1mA
                   MakeDoubleAccessor (&LorameshEnergyModel::SetIdleCurrent),
                   MakeDoubleChecker<double> ())
    .AddAttribute ("SleepCurrent",
                   "Current consumption in SLEEP state (A)",
                   DoubleValue (0.000001), // 1uA
                   MakeDoubleAccessor (&LorameshEnergyModel::SetSleepCurrent),
                   MakeDoubleChecker<double> ());
  return tid;
}

LorameshEnergyModel::LorameshEnergyModel ()
  : m_initialEnergyJ (10000.0),
    m_supplyVoltageV (3.3),
    m_txCurrentA (0.120),
    m_rxCurrentA (0.015),
    m_idleCurrentA (0.001),
    m_sleepCurrentA (0.000001),
    m_remainingEnergyJ (10000.0),
    m_lastUpdateTime (Seconds (0)),
    m_currentState (IDLE)
{
}

LorameshEnergyModel::~LorameshEnergyModel ()
{
}

void
LorameshEnergyModel::SetInitialEnergy (double joules)
{
  m_initialEnergyJ = joules;
  m_remainingEnergyJ = joules;
}

void
LorameshEnergyModel::SetSupplyVoltage (double volts)
{
  m_supplyVoltageV = volts;
}

void
LorameshEnergyModel::SetTxCurrent (double amperes)
{
  m_txCurrentA = amperes;
}

void
LorameshEnergyModel::SetRxCurrent (double amperes)
{
  m_rxCurrentA = amperes;
}

void
LorameshEnergyModel::SetIdleCurrent (double amperes)
{
  m_idleCurrentA = amperes;
}

void
LorameshEnergyModel::SetSleepCurrent (double amperes)
{
  m_sleepCurrentA = amperes;
}

double
LorameshEnergyModel::GetTotalEnergyConsumption () const
{
  return m_initialEnergyJ - m_remainingEnergyJ;
}

double
LorameshEnergyModel::GetRemainingEnergy () const
{
  return m_remainingEnergyJ;
}

double
LorameshEnergyModel::GetSupplyVoltage () const
{
  return m_supplyVoltageV;
}

double
LorameshEnergyModel::GetBatteryPercentage () const
{
  if (m_initialEnergyJ <= 0) return 0.0;
  return (m_remainingEnergyJ / m_initialEnergyJ) * 100.0;
}

void
LorameshEnergyModel::UpdateEnergy ()
{
  Time now = Simulator::Now ();
  Time duration = now - m_lastUpdateTime;
  
  if (duration.IsZero ()) return;

  double currentA = 0.0;
  switch (m_currentState)
    {
    case TX: currentA = m_txCurrentA; break;
    case RX: currentA = m_rxCurrentA; break;
    case IDLE: currentA = m_idleCurrentA; break;
    case SLEEP: currentA = m_sleepCurrentA; break;
    }

  double energyConsumed = currentA * m_supplyVoltageV * duration.GetSeconds ();
  m_remainingEnergyJ = std::max (0.0, m_remainingEnergyJ - energyConsumed);
  
  m_lastUpdateTime = now;
}

void
LorameshEnergyModel::ChangeState (State newState)
{
  if (m_currentState == newState) return;
  
  UpdateEnergy ();
  m_currentState = newState;
  
  NS_LOG_DEBUG ("State changed to " << newState << " at " << Simulator::Now ().GetSeconds () 
                << "s. Remaining energy: " << m_remainingEnergyJ << "J");
}

LorameshEnergyModel::State
LorameshEnergyModel::GetCurrentState () const
{
  return m_currentState;
}

} // namespace ns3
