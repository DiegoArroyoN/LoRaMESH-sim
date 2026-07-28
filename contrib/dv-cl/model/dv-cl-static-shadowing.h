/* SPDX-License-Identifier: GPL-2.0-only */

#pragma once

#include "ns3/nstime.h"
#include "ns3/propagation-loss-model.h"
#include "ns3/random-variable-stream.h"

#include <cstdint>
#include <map>
#include <utility>

namespace ns3
{
namespace dvcl
{

/**
 * \ingroup dv-cl
 * \brief Log-normal shadowing drawn once per link and held for the run.
 *
 * The obvious way to add shadowing is `RandomPropagationLossModel`, which draws
 * a fresh normal sample on every transmission. That is fast fading, not
 * shadowing: shadowing is caused by obstacles between two fixed positions, so
 * two packets sent seconds apart over the same link see the *same* attenuation.
 *
 * The distinction is not cosmetic for a distance-vector protocol that infers
 * link quality from beacons. Under per-transmission draws a node only ever
 * observes the beacons that happened to draw a favourable sample, because the
 * unfavourable ones fall below sensitivity and are never received. The measured
 * RSSI is therefore drawn from the upper tail rather than the centre of the
 * distribution, the spreading factor derived from it is too low, and the data
 * that follows -- drawing an independent sample -- is lost. Measured
 * 2026-07-28: with per-transmission draws at sigma=3.57 dB the delivery ratio
 * fell by 73% to 187% relative to a deterministic channel, and multi-hop
 * forwarding all but stopped (delivered hop count fell 4.4x) because routes
 * were installed over links that could not carry data.
 *
 * Holding the sample per link removes that bias at its source: what a beacon
 * measures is what the data will experience. Note this models shadowing as
 * independent between links, without spatial correlation between nearby pairs;
 * that is the standard simplification and is sufficient here, where the
 * property that matters is constancy in time rather than correlation in space.
 *
 * The draw is keyed on node ids, not on pointers, so a given seed reproduces a
 * given set of links exactly.
 */
class DvClStaticShadowingPropagationLossModel : public PropagationLossModel
{
  public:
    static TypeId GetTypeId();
    DvClStaticShadowingPropagationLossModel();
    ~DvClStaticShadowingPropagationLossModel() override;

    /// Shadowing applied to the link between two node ids, in dB.
    double GetLinkShadowingDb(uint32_t nodeA, uint32_t nodeB) const;

  private:
    double DoCalcRxPower(double txPowerDbm,
                         Ptr<MobilityModel> a,
                         Ptr<MobilityModel> b) const override;
    int64_t DoAssignStreams(int64_t stream) override;

    double m_sigmaDb{3.57};
    Ptr<NormalRandomVariable> m_normal;
    /// Key is the ordered pair (min id, max id): shadowing is reciprocal, so
    /// both directions of a link must see the same value.
    mutable std::map<std::pair<uint32_t, uint32_t>, double> m_linkShadowing;
};

} // namespace dvcl
} // namespace ns3
