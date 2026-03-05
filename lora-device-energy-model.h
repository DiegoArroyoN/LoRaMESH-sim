/*
 * LoRa Device Energy Model for ns-3 Energy Framework
 *
 * Models power consumption of LoRa radio in different states:
 * TX, RX, CAD, IDLE, SLEEP
 *
 * Based on SX1276 datasheet values.
 */

#ifndef LORA_DEVICE_ENERGY_MODEL_H
#define LORA_DEVICE_ENERGY_MODEL_H

#include "ns3/energy-module.h"
#include "ns3/node.h"
#include "ns3/nstime.h"
#include "ns3/object.h"
#include "ns3/ptr.h"
#include "ns3/traced-value.h"

namespace ns3
{

/**
 * \brief LoRa radio states for energy consumption modeling
 */
enum class LoRaRadioState : int
{
    TX = 0,   ///< Transmitting
    RX = 1,   ///< Receiving
    CAD = 2,  ///< Channel Activity Detection
    IDLE = 3, ///< Idle (radio on, not TX/RX)
    SLEEP = 4 ///< Deep sleep mode
};

/**
 * \brief Device energy model for LoRa radio transceivers
 *
 * This model tracks energy consumption based on the current state
 * of the LoRa radio. Current values are based on Semtech SX1276 datasheet.
 *
 * Typical current consumption (SX1276):
 *   TX @20dBm:  120 mA
 *   TX @14dBm:  100 mA
 *   RX:         10-12 mA
 *   CAD:        ~10 mA (similar to RX)
 *   IDLE:       ~1 mA
 *   SLEEP:      0.2 µA
 */
class LoRaDeviceEnergyModel : public energy::DeviceEnergyModel
{
  public:
    static TypeId GetTypeId();

    LoRaDeviceEnergyModel();
    ~LoRaDeviceEnergyModel() override;

    // Required by DeviceEnergyModel
    void SetEnergySource(Ptr<energy::EnergySource> source) override;
    double GetTotalEnergyConsumption() const override;
    void ChangeState(int newState) override;
    void HandleEnergyDepletion() override;
    void HandleEnergyRecharged() override;
    void HandleEnergyChanged() override;

    // LoRa-specific methods
    void SetTxCurrentA(double currentA);
    void SetRxCurrentA(double currentA);
    void SetCadCurrentA(double currentA);
    void SetIdleCurrentA(double currentA);
    void SetSleepCurrentA(double currentA);

    double GetTxCurrentA() const;
    double GetRxCurrentA() const;
    double GetCadCurrentA() const;
    double GetIdleCurrentA() const;
    double GetSleepCurrentA() const;

    LoRaRadioState GetCurrentState() const;
    void SetNode(Ptr<Node> node);
    Ptr<Node> GetNode() const;
    double GetRemainingEnergyJ() const;
    double GetEnergyFraction() const;
    double GetSupplyVoltageV() const;

  private:
    double DoGetCurrentA() const override;
    void UpdateEnergyConsumption();

    Ptr<energy::EnergySource> m_source;
    Ptr<Node> m_node;

    // Current draw in Amperes for each state (from SX1276 datasheet)
    double m_txCurrentA;    ///< TX current (default: 0.120 A = 120 mA @ 20dBm)
    double m_rxCurrentA;    ///< RX current (default: 0.011 A = 11 mA)
    double m_cadCurrentA;   ///< CAD current (default: 0.011 A = 11 mA)
    double m_idleCurrentA;  ///< Idle current (default: 0.001 A = 1 mA)
    double m_sleepCurrentA; ///< Sleep current (default: 0.0000002 A = 0.2 µA)

    LoRaRadioState m_currentState;
    Time m_lastUpdateTime;
    double m_totalEnergyConsumption; ///< Total energy consumed in Joules

    TracedValue<double> m_totalEnergyConsumptionTrace;
};

} // namespace ns3

#endif /* LORA_DEVICE_ENERGY_MODEL_H */
