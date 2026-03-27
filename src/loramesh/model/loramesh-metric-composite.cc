#include "loramesh-metric-composite.h"

#include <algorithm>
#include <cmath>

namespace ns3
{
namespace loramesh
{

// Initialize the static constexpr arrays
constexpr double CompositeMetric::kMaxToaUs[6];
constexpr double CompositeMetric::kSnrThreshold[6];

double
CompositeMetric::ComputeLinkCost(NodeId src, NodeId dst, const LinkStats& stats) const
{
    (void)src;
    (void)dst;

    // ========================================================================
    // THESIS FORMULA (Diego Arroyo):
    // C_ij = α·ToA_norm + β·Hop_norm + δ·Ψ(battery)
    //
    // IMPORTANT: SNR/RSSI is NOT used directly as per thesis specification.
    // Link quality is already "embedded" in the SF selection mechanism:
    // - Bad link -> higher SF required -> higher ToA -> higher cost automatically
    //
    // Weights: α + β + δ = 1 (configurable, thesis doesn't specify exact values)
    // ========================================================================
    const double alpha = 0.40; // α: ToA weight (duty cycle efficiency)
    const double beta = 0.30;  // β: Hop count weight (path length)
    const double delta = 0.30; // δ: Battery weight (network lifetime)

    // 1. ToA normalized: ToA_ij / ToA_max (per SF from Semtech datasheet)
    const double toaNorm = NormalizeToa(stats.toaUs, stats.sf);

    // 2. Hop count normalized: 1/H_max per hop (H_max = 10 as per thesis)
    const double hopNorm = std::min(static_cast<double>(stats.hops) / 10.0, 1.0);

    // 3. Battery penalty: Ψ(b) = 1 - b^p where p ≥ 2 (thesis: non-linear asymptotic)
    double energyFraction = stats.energyFraction;
    if (energyFraction < 0.0 && m_energyModel)
    {
        energyFraction = m_energyModel->GetEnergyFraction(src);
    }
    const double batteryPenalty = BatteryPenalty(energyFraction, stats.batteryMv);

    return alpha * toaNorm + beta * hopNorm + delta * batteryPenalty;
}

double
CompositeMetric::NormalizeToa(double toaUs, uint8_t sf) const
{
    // Clamp SF to valid range [7, 12]
    const uint8_t sfClamped = std::clamp<uint8_t>(sf, 7, 12);
    const size_t index = static_cast<size_t>(sfClamped - 7);

    // Normalize against max ToA for this SF (Semtech SX1276 datasheet values)
    const double maxToaForSf = kMaxToaUs[index];
    return std::min(toaUs / maxToaForSf, 1.0);
}

double
CompositeMetric::SnrPenalty(double snrDb, uint8_t sf) const
{
    // SNR margin-based penalty using Semtech SX1276 thresholds
    // Each SF has a minimum SNR required for demodulation:
    //   SF7: -7.5dB, SF8: -10dB, SF9: -12.5dB, SF10: -15dB, SF11: -17.5dB, SF12: -20dB
    //
    // The margin is SNR - threshold. A positive margin means the link is better than minimum.
    // We use a 20dB margin for "excellent" quality.

    const uint8_t sfClamped = std::clamp<uint8_t>(sf, 7, 12);
    const size_t index = static_cast<size_t>(sfClamped - 7);
    const double threshold = kSnrThreshold[index];

    // Margin over minimum required SNR
    const double margin = snrDb - threshold;

    // margin >= 20dB -> excellent link, penalty = 0
    // margin <= 0dB  -> at threshold limit, penalty = 1
    constexpr double excellentMargin = 20.0; // dB above threshold for penalty=0

    return std::clamp(1.0 - (margin / excellentMargin), 0.0, 1.0);
}

double
CompositeMetric::BatteryPenalty(double energyFraction, double batteryMv) const
{
    // Thesis formula: Ψ(b) = 1 - b^p  where p >= 2
    // b = E_rem / E_max (State of Charge normalized 0-1)
    // This is a non-linear penalty that protects low-battery nodes

    double b = energyFraction;

    // Use battery voltage as fallback if energy fraction not available
    if (b < 0.0)
    {
        // Li-Ion 18650: 3000mV = 0%, 4200mV = 100%
        constexpr double vMin = 3000.0;
        constexpr double vMax = 4200.0;
        b = std::clamp((batteryMv - vMin) / (vMax - vMin), 0.0, 1.0);
    }

    // Thesis formula: Ψ(b) = 1 - b^p with p = 2
    // When b = 1.0 (100% battery): Ψ = 1 - 1 = 0 (no penalty)
    // When b = 0.5 (50% battery): Ψ = 1 - 0.25 = 0.75 (high penalty)
    // When b = 0.0 (0% battery): Ψ = 1 - 0 = 1 (max penalty)
    constexpr double p = 2.0; // Thesis: p >= 2

    const double penalty = 1.0 - std::pow(b, p);
    return std::clamp(penalty, 0.0, 1.0);
}

} // namespace loramesh
} // namespace ns3
