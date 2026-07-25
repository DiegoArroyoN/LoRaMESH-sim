/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_LORA_ENERGY_MODEL_H
#define DV_CL_LORA_ENERGY_MODEL_H

// Specific energy headers, never the ns3/energy-module.h aggregator: a module
// header that includes an aggregator trips ns-3's own guard against it
// (NS3_MODULE_COMPILATION), which some 3.46 point releases enforce and others
// tolerate -- the old model included the aggregator and only built by luck.
#include "ns3/device-energy-model.h"
#include "ns3/energy-source.h"
#include "ns3/nstime.h"
#include "ns3/traced-callback.h"

#include <algorithm>

namespace ns3
{
namespace dvcl
{

/**
 * \ingroup dv-cl
 * \brief LoRa radio states, indexing the per-state time ledger.
 */
enum class DvClRadioState : int
{
    TX = 0,   //!< Transmitting
    RX = 1,   //!< Receiving
    CAD = 2,  //!< Channel Activity Detection
    IDLE = 3, //!< Radio on, neither TX nor RX
    SLEEP = 4 //!< Deep sleep (a flat battery draws nothing)
};

/**
 * \ingroup dv-cl
 * \brief The one energy model of the module: a per-device ns-3
 *        DeviceEnergyModel backed by an event-driven charge ledger.
 *
 * There used to be two books for one battery: a bespoke registry that drove
 * the SoC byte, the metric and node death, and a second DeviceEnergyModel
 * whose depletion notification was wired to nothing. This is the merge of the
 * two into a single class (VALIDATION.md, 2026-07-22).
 *
 * Charge is accounted by explicit, event-driven debits: the device charges a
 * transmission, a reception or a CAD probe for its exact on-air duration, and
 * idle draw is applied lazily for the gap since the last event. That is the
 * authoritative ledger -- it answers GetEnergyFraction(), which feeds the
 * beacon SoC, the receiver-applied energy term of the metric, and the FND.
 *
 * It is a real ns-3 DeviceEnergyModel: it may be attached to an EnergySource
 * (for the supply voltage and framework citizenship), it reports the current
 * of its instantaneous state through DoGetCurrentA(), and it fires
 * HandleEnergyDepletion the moment the ledger empties. The ledger is the
 * single authority whether or not a source is attached; a source, when
 * present, contributes its supply voltage but is not a second store of charge.
 * This event-driven accounting is deliberate: a state machine fed from a
 * "reception complete" hook cannot represent the reception interval, which is
 * exactly the defect that made the old model report 88% of the energy as RX.
 */
class DvClLoraEnergyModel : public energy::DeviceEnergyModel
{
  public:
    static TypeId GetTypeId();

    DvClLoraEnergyModel();
    ~DvClLoraEnergyModel() override = default;

    // --- ns-3 DeviceEnergyModel interface ---
    void SetEnergySource(Ptr<energy::EnergySource> source) override;
    double GetTotalEnergyConsumption() const override;
    void ChangeState(int newState) override;
    void HandleEnergyDepletion() override;
    void HandleEnergyRecharged() override;
    void HandleEnergyChanged() override;

    // --- charge accounting (called by the device and the MAC) ---
    /// Charge a transmission of the given on-air duration at the TX current.
    void ChargeTx(double durationSeconds);
    /// Charge a reception of the given duration at the RX current.
    void ChargeRx(double durationSeconds);
    /// Charge a CAD probe of the given duration at the CAD current.
    void ChargeCad(double durationSeconds);

    // --- seeding ---
    /// Seed the battery to a fraction [0,1] of capacity. Resets the ledger.
    void SetInitialSocFraction(double fraction);

    // --- authoritative queries ---
    double GetEnergyFraction() const;  //!< remaining charge / capacity, in [0,1]
    double GetRemainingEnergyJ() const; //!< remaining charge in Joules
    double GetVoltageMv() const;        //!< terminal voltage from the SoC window

    /// What the node spent, split by activity, in mAh. Only the relayed-data
    /// share of the TX total is redistributable by a routing decision; the
    /// rest is paid by every node alike, which bounds what any metric can do.
    double GetTxMah() const;
    double GetRxMah() const;
    double GetCadMah() const;
    double GetIdleMah() const;

    /// Accumulated time spent in a radio state (F1.3b per-state ledger).
    Time GetTimeInState(DvClRadioState s) const;

    // --- configuration ---
    void SetCapacityMah(double capacity);
    void SetTxCurrentMa(double current);
    void SetRxCurrentMa(double current);
    void SetCadCurrentMa(double current);
    void SetIdleCurrentMa(double current);
    void SetVoltageWindow(double minMv, double maxMv);

    double GetTxCurrentA() const { return m_txCurrentMa / 1000.0; }
    double GetRxCurrentA() const { return m_rxCurrentMa / 1000.0; }
    double GetCadCurrentA() const { return m_cadCurrentMa / 1000.0; }
    double GetIdleCurrentA() const { return m_idleCurrentMa / 1000.0; }
    double GetSupplyVoltageV() const;

    /// Fired on depletion: (nodeId, remainingFraction). nodeId is 0 when no
    /// source/node context is available.
    typedef void (*EnergyDepletedCallback)(uint32_t nodeId, double remainingFraction);

    // Current values: Semtech SX1276/77/78/79 datasheet, DC characteristics.
    static constexpr double kDefaultCapacityMah = 300.0; //!< thesis node energy budget
    static constexpr double kDefaultTxCurrentMa = 120.0; //!< +20 dBm PA_BOOST [IDD_TXLORA]
    static constexpr double kDefaultRxCurrentMa = 10.3;  //!< BW=125 kHz [IDD_RXLORA]
    static constexpr double kDefaultCadCurrentMa = 10.3; //!< same RX circuits
    static constexpr double kDefaultIdleCurrentMa = 1.6; //!< standby [IDD_STDB]
    static constexpr double kDefaultVoltageMinMv = 3000.0;
    static constexpr double kDefaultVoltageMaxMv = 4200.0;

  private:
    double DoGetCurrentA() const override;

    /// Apply idle draw for the time since the last event, then advance the clock.
    void ApplyIdleConsumption();
    /// Charge one activity: debit min(want, remaining), book it to the category.
    void Charge(double durationSeconds, double currentMa, double& categoryMah, double& categorySec);
    /// Fire the depletion trace once, when the ledger first empties.
    void MaybeNotifyDepleted();

    Ptr<energy::EnergySource> m_source; //!< optional; supplies voltage, not charge

    // The ledger (authoritative).
    double m_capacityMah;
    double m_remainingMah;
    Time m_lastUpdate;
    double m_txMah;
    double m_rxMah;
    double m_cadMah;
    double m_idleMah;
    double m_txSec;
    double m_rxSec;
    double m_cadSec;
    double m_idleSec;
    bool m_depleted;

    // Per-state currents (mA) and the SoC->voltage window.
    double m_txCurrentMa;
    double m_rxCurrentMa;
    double m_cadCurrentMa;
    double m_idleCurrentMa;
    double m_voltageMinMv;
    double m_voltageMaxMv;

    // Instantaneous state, reported to a source through DoGetCurrentA.
    DvClRadioState m_instantState;

    TracedCallback<uint32_t, double> m_energyDepletedTrace;
};

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_LORA_ENERGY_MODEL_H */
