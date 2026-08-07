#ifndef LORAMESH_ENERGY_MODEL_H
#define LORAMESH_ENERGY_MODEL_H

#include "ns3/object.h"
#include "ns3/nstime.h"
#include "ns3/traced-value.h"
#include "ns3/event-id.h"

namespace ns3 {

class LorameshEnergyModel : public Object
{
public:
  static TypeId GetTypeId ();
  LorameshEnergyModel ();
  ~LorameshEnergyModel () override;

  enum State
  {
    IDLE,
    TX,
    RX,
    SLEEP
  };

  void SetInitialEnergy (double joules);
  void SetSupplyVoltage (double volts);
  
  // Current consumption in Amperes
  void SetTxCurrent (double amperes);
  void SetRxCurrent (double amperes);
  void SetIdleCurrent (double amperes);
  void SetSleepCurrent (double amperes);

  double GetTotalEnergyConsumption () const;
  double GetRemainingEnergy () const;
  double GetSupplyVoltage () const;
  double GetBatteryPercentage () const;

  void ChangeState (State newState);
  State GetCurrentState () const;

private:
  void UpdateEnergy ();

  double m_initialEnergyJ;
  double m_supplyVoltageV;
  
  double m_txCurrentA;
  double m_rxCurrentA;
  double m_idleCurrentA;
  double m_sleepCurrentA;

  TracedValue<double> m_remainingEnergyJ;
  Time m_lastUpdateTime;
  State m_currentState;
};

} // namespace ns3

#endif // LORAMESH_ENERGY_MODEL_H
