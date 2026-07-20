/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_ENERGY_REGISTRY_H
#define DV_CL_ENERGY_REGISTRY_H

#include "ns3/nstime.h"
#include "ns3/object.h"

#include <map>

namespace ns3
{

using NodeId = uint32_t;

namespace dvcl
{

/**
 * \brief Simple per-node energy tracker for LoRaMesh nodes.
 *
 * Keeps track of remaining capacity (mAh) and converts it into energy or voltage levels.
 * TX and RX updates are invoked explicitly, while idle consumption is applied whenever
 * state is queried or updated.
 *
 * FIX E3: Nota sobre consumo de energía según SF
 * El consumo de energía en TX depende directamente del Time-on-Air (ToA), que varía
 * significativamente con el SF. Para el mismo payload:
 *   - SF7:  ~72ms ToA  → ~2.4 mAh @ 120mA
 *   - SF12: ~1.5s ToA  → ~50 mAh @ 120mA  (aprox 20x más que SF7)
 *
 * El caller (MeshDvApp) debe pasar la duración correcta calculada según el SF usado.
 * Para mayor precisión, usar: ToA = GetOnAirTime(packet, txParams) del PHY.
 */
class DvClEnergyRegistry : public Object
{
  public:
    static TypeId GetTypeId();

    DvClEnergyRegistry();
    ~DvClEnergyRegistry() override = default;

    void RegisterNode(NodeId id);
    void SetRemainingFraction(NodeId id, double fraction);

    void UpdateEnergy(NodeId id, double txCurrentMa, double durationSeconds);
    void UpdateRxEnergy(NodeId id, double durationSeconds);
    void UpdateCadEnergy(NodeId id, double durationSeconds);

    double GetRemainingEnergy(NodeId id);
    double GetEnergyFraction(NodeId id);
    double GetVoltageMv(NodeId id);

    void SetCapacityMah(double capacity);
    void SetTxCurrentMa(double current);
    void SetRxCurrentMa(double current);
    void SetCadCurrentMa(double current);
    void SetIdleCurrentMa(double current);
    void SetVoltageWindow(double minMv, double maxMv);

    // Current values: Semtech SX1276/77/78/79 Datasheet, Table DC Characteristics.
    // Must match lora-device-energy-model.cc for consistent SoC/FND/T50 reporting.
    static constexpr double kDefaultCapacityMah = 300.0; // 300 mAh thesis node energy budget
    static constexpr double kDefaultTxCurrentMa =
        120.0; // 120 mA @ +20 dBm PA_BOOST [SX1276 DS, IDD_TXLORA]
    static constexpr double kDefaultRxCurrentMa =
        10.3; // 10.3 mA LoRa BW=125 kHz   [SX1276 DS, IDD_RXLORA]
    static constexpr double kDefaultCadCurrentMa = 10.3; // 10.3 mA (same RX circuits) [SX1276 DS]
    static constexpr double kDefaultIdleCurrentMa =
        1.6; //  1.6 mA standby            [SX1276 DS, IDD_STDB]
    static constexpr double kDefaultVoltageMinMv = 3000.0;
    static constexpr double kDefaultVoltageMaxMv = 4200.0;

  private:
    struct NodeEnergyState
    {
        double remainingMah{kDefaultCapacityMah};
        Time lastUpdate{Seconds(0)};
    };

    NodeEnergyState& EnsureNode(NodeId id);
    void ApplyIdleConsumption(NodeEnergyState& state);

    std::map<NodeId, NodeEnergyState> m_states;
    double m_capacityMah;
    double m_txCurrentMa;
    double m_rxCurrentMa;
    double m_cadCurrentMa;
    double m_idleCurrentMa;
    double m_voltageMinMv;
    double m_voltageMaxMv;
};

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_ENERGY_REGISTRY_H */
