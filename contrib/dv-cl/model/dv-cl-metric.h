/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_METRIC_H
#define DV_CL_METRIC_H

#include "ns3/object.h"

#include <cstdint>

namespace ns3
{
namespace dvcl
{

/**
 * \ingroup dv-cl
 * \brief Inputs a routing metric may use to price one link.
 *
 * Kept deliberately small: link quality is embedded in the SF selection
 * (a bad link needs a higher SF, hence a larger ToA), so neither RSSI
 * nor SNR enter the metric directly.
 */
struct LinkInputs
{
    double toaUs{0.0};         //!< accumulated/announced time-on-air, microseconds
    uint8_t sf{7};             //!< spreading factor the link operates at (7..12)
    double energyFraction{1.0}; //!< next-hop state of charge b in [0,1]; <0 = unknown
    double batteryMv{-1.0};    //!< next-hop terminal voltage, mV; used only when
                               //!< energyFraction is unknown (<0)
};

/**
 * \ingroup dv-cl
 * \brief Abstract routing metric: prices a candidate link/next hop.
 *
 * Pluggable-metric seam of the module (Plan Maestro F6.1): third parties
 * implement their own metric (ETX, RSSI, learned metrics, ...) by
 * subclassing this interface, without touching the protocol core.
 */
class DvClRoutingMetric : public Object
{
  public:
    static TypeId GetTypeId();
    ~DvClRoutingMetric() override = default;

    /**
     * \param in link inputs (ToA, SF, next-hop state of charge)
     * \return non-negative incremental link cost; lower is better
     */
    virtual double ComputeLinkCost(const LinkInputs& in) const = 0;
};

/**
 * \ingroup dv-cl
 * \brief DV-CL composite cross-layer metric (thesis formula).
 *
 * Incremental link cost of taking next hop j:
 *
 *   Delta C_ij = alpha * ToA_hat_ij + beta + delta * Psi(b_j)
 *
 * with ToA_hat in [0,1] (ToA normalized per SF), beta a constant per-hop
 * cost, and Psi the piecewise energy penalty of the paper (eq. psi):
 *
 *   Psi(b) = 0                                   for b >= bHi
 *          = PsiMax * ((bHi - b)/(bHi - bLo))^p  for bLo < b < bHi
 *          = PsiMax                              for b <= bLo
 *
 * Defaults (alpha,beta,delta) = (0.60, 0.15, 0.25), bLo = 0.20,
 * bHi = 0.50, p = 2, PsiMax = 1 — extracted from the campaign tree
 * (override 2026-05-30, RoutingDv::ComputeThesisLinkCost +
 * ComputeCompositeEnergyPenalty) and matching the paper.
 *
 * NOTE (audit 2026-07-17): the legacy scratch tree carries a second,
 * older metric implementation (CompositeMetric, weights 0.40/0.30/0.30,
 * asymptotic penalty). This class is the single implementation of
 * record for the module; the duplicate is retired at porting time.
 */
class DvClCompositeMetric : public DvClRoutingMetric
{
  public:
    static TypeId GetTypeId();

    double ComputeLinkCost(const LinkInputs& in) const override;

    /**
     * \brief ToA normalization: toaUs / kMaxToaUs[sf], capped at 1.
     * \param toaUs time-on-air in microseconds
     * \param sf spreading factor (clamped to 7..12)
     * \return ToA_hat in [0,1]
     */
    double NormalizeToa(double toaUs, uint8_t sf) const;

    /**
     * \brief delta * Psi(b): weighted piecewise energy penalty.
     * \param energyFraction next-hop state of charge in [0,1]; <0 = unknown
     *        (unknown chargers price as full battery: no penalty)
     * \return weighted penalty in [0, WEnergy * PsiMax]
     */
    double EnergyPenalty(double energyFraction, double batteryMv = -1.0) const;

  private:
    // Per-SF ToA normalization ceilings, microseconds. Carried over
    // verbatim from the campaign tree (CompositeMetric::kMaxToaUs) so the
    // module reproduces campaign behavior bit-for-bit. Audit note
    // 2026-07-17: the original comment attributes these to "max payload
    // 222 B", which does not match the AN1200.13 value at 222 B; treat
    // them as internal normalization constants (their absolute origin
    // does not affect ranking, only the scale of ToA_hat).
    static constexpr double kMaxToaUs[6] = {143360.0,
                                            256512.0,
                                            462848.0,
                                            829440.0,
                                            1810432.0,
                                            3293184.0};

    double m_wToa{0.60};           //!< alpha
    double m_wHop{0.15};           //!< beta (constant per hop)
    double m_wEnergy{0.25};        //!< delta
    double m_energyLo{0.20};       //!< bLo
    double m_energyHi{0.50};       //!< bHi
    double m_energyPow{2.0};       //!< p
    double m_energyMaxPenalty{1.0}; //!< PsiMax
};

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_METRIC_H */
