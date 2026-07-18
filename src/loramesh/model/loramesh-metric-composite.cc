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
    // THESIS FORMULA §4.2 (Diego Arroyo):
    //   ΔC_ij = α·T̂_ij + β + δ·Ψ(b_j)
    //
    // β is a FIXED per-hop cost (NOT β·hops/H_max).
    // At the DV beacon receive side, hops=1 always (direct link score).
    // The β term accumulates naturally hop-by-hop as DV propagates scores.
    //
    // Weights (thesis §4.2, Table): α+β+δ=1.
    // ========================================================================
    const double alpha = 0.60; // α: ToA weight (duty-cycle efficiency)
    const double beta  = 0.15; // β: fixed cost per hop (path-depth regularizer)
    const double delta = 0.25; // δ: energy penalty weight (network lifetime)

    // 1. Normalize ToA: T̂_ij = ToA_ij / ToA_max(SF)
    const double toaNorm = NormalizeToa(stats.toaUs, stats.sf);

    // 2. Fixed hop cost β (one constant per link, accumulated hop-by-hop in DV).
    //    hops field is informational; the per-link cost is always β.
    const double hopCost = beta;

    // 3. Battery penalty Ψ(b_j): piecewise formula per thesis Eq.3.
    double energyFraction = stats.energyFraction;
    if (energyFraction < 0.0 && m_energyModel)
    {
        energyFraction = m_energyModel->GetEnergyFraction(src);
    }
    const double batteryPenalty = BatteryPenalty(energyFraction, stats.batteryMv);

    return alpha * toaNorm + hopCost + delta * batteryPenalty;
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
    // Thesis Eq.3 — piecewise energy penalty Ψ(b_j):
    //
    //   Ψ(b) = 0                               if b ≥ b_w
    //   Ψ(b) = ((b_w - b) / (b_w - b_c))^p   if b_c < b < b_w
    //   Ψ(b) = 1                               if b ≤ b_c
    //
    // b_w = 0.50 (warning threshold)
    // b_c = 0.20 (critical threshold)
    // p   = 2    (exponent)
    //
    // This means:
    //   - Healthy nodes (b ≥ 50%) receive zero penalty.
    //   - Critically low nodes (b ≤ 20%) receive maximum penalty (1).
    //   - Intermediate nodes receive a smooth power-law penalty.

    double b = energyFraction;

    // Use battery voltage as fallback if energy fraction not available
    if (b < 0.0)
    {
        // Li-Ion 18650: 3000mV = 0%, 4200mV = 100%
        constexpr double vMin = 3000.0;
        constexpr double vMax = 4200.0;
        b = std::clamp((batteryMv - vMin) / (vMax - vMin), 0.0, 1.0);
    }

    // Clamp to [0,1] for safety
    b = std::clamp(b, 0.0, 1.0);

    if (b >= kEnergyWarningThreshold)
    {
        return 0.0;
    }
    if (b <= kEnergyCriticalThreshold)
    {
        return 1.0;
    }

    const double ratio = (kEnergyWarningThreshold - b) /
                         (kEnergyWarningThreshold - kEnergyCriticalThreshold);
    return std::clamp(std::pow(ratio, kEnergyPenaltyExponent), 0.0, 1.0);
}

} // namespace loramesh
} // namespace ns3
