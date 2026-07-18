#ifndef LORAMESH_METRIC_COMPOSITE_H
#define LORAMESH_METRIC_COMPOSITE_H

#include "ns3/loramesh-energy-model.h"
#include "ns3/ptr.h"

#include <cstdint>

namespace ns3
{

using NodeId = uint32_t;

namespace loramesh
{

/**
 * \brief Link statistics used for composite metric calculation.
 *
 * Contains all metrics needed to compute the link cost:
 * - ToA: Time on Air in microseconds
 * - Hops: Current hop count
 * - SNR: Signal-to-Noise Ratio in dB (preferred over RSSI for link quality)
 * - Battery: Battery voltage in mV or energy fraction
 */
struct LinkStats
{
    double toaUs{0.0};           ///< Time on Air in microseconds
    uint8_t hops{0};             ///< Hop count
    double snrDb{0.0};           ///< SNR in dB (more accurate than RSSI for LoRa)
    double rssiDbm{-120.0};      ///< RSSI in dBm (kept for compatibility, deprecated)
    double batteryMv{0.0};       ///< Battery voltage in mV
    double energyFraction{-1.0}; ///< Energy remaining fraction [0,1], -1 if unknown
    uint8_t sf{7};               ///< Spreading Factor used (for ToA normalization)
};

/**
 * \brief Composite metric for LoRaMesh routing decisions.
 *
 * Implements the formula from Diego Arroyo's thesis (§4.2):
 *   ΔC_ij = α·T̂_ij + β + δ·Ψ(b_j)
 *
 * Where:
 * - T̂_ij is the ToA normalized against the max ToA for the given SF.
 * - β is a FIXED cost per hop (accumulated hop-by-hop in DV); NOT hop/H_max.
 * - Ψ(b_j) is a piecewise energy penalty: 0 when b≥b_w, 1 when b≤b_c,
 *   power-law in between (Eq.3 of thesis).
 *
 * Weights (thesis §4.2, Table): α=0.60, β=0.15, δ=0.25 (α+β+δ=1).
 * Energy penalty thresholds: b_w=0.50, b_c=0.20, p=2.
 */
class CompositeMetric
{
  public:
    CompositeMetric() = default;

    void SetEnergyModel(Ptr<EnergyModel> energy)
    {
        m_energyModel = energy;
    }

    double ComputeLinkCost(NodeId src, NodeId dst, const LinkStats& stats) const;

    /// Normalize ToA against max ToA for the given SF (based on Semtech/LoRa Alliance specs)
    double NormalizeToa(double toaUs, uint8_t sf) const;

    /// SNR-based link quality penalty using Semtech thresholds per SF
    double SnrPenalty(double snrDb, uint8_t sf) const;

    /// Piecewise energy penalty Ψ(b_j) per thesis Eq.3:
    ///   Ψ=0 if b≥b_w; Ψ=1 if b≤b_c; power-law in between.
    double BatteryPenalty(double energyFraction, double batteryMv) const;

    // Thesis §4.2 energy penalty thresholds
    static constexpr double kEnergyWarningThreshold  = 0.50; // b_w
    static constexpr double kEnergyCriticalThreshold = 0.20; // b_c
    static constexpr double kEnergyPenaltyExponent   = 2.0;  // p

    // Max ToA values per SF in microseconds (Semtech SX1276 datasheet, BW=125kHz, CR=4/5, max
    // payload 222B) These are reference values for normalization, based on EU868 regulations
    static constexpr double kMaxToaUs[6] = {
        143360.0,  // SF7:  ~143ms
        256512.0,  // SF8:  ~257ms
        462848.0,  // SF9:  ~463ms
        829440.0,  // SF10: ~829ms
        1810432.0, // SF11: ~1810ms (1.81s)
        3293184.0  // SF12: ~3293ms (3.29s)
    };

    // SNR demodulation thresholds per SF (Semtech SX1276 datasheet)
    // These are the minimum SNR required for successful packet reception
    static constexpr double kSnrThreshold[6] = {
        -7.5,  // SF7
        -10.0, // SF8
        -12.5, // SF9
        -15.0, // SF10
        -17.5, // SF11
        -20.0  // SF12
    };

  private:
    Ptr<EnergyModel> m_energyModel;
};

} // namespace loramesh
} // namespace ns3

#endif /* LORAMESH_METRIC_COMPOSITE_H */
