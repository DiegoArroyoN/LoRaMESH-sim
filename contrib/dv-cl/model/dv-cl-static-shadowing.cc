/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-static-shadowing.h"

#include "ns3/double.h"
#include "ns3/log.h"
#include "ns3/mobility-model.h"
#include "ns3/node.h"
#include "ns3/pointer.h"

#include <algorithm>

namespace ns3
{
namespace dvcl
{

NS_LOG_COMPONENT_DEFINE("DvClStaticShadowing");
NS_OBJECT_ENSURE_REGISTERED(DvClStaticShadowingPropagationLossModel);

TypeId
DvClStaticShadowingPropagationLossModel::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClStaticShadowingPropagationLossModel")
            .SetParent<PropagationLossModel>()
            .SetGroupName("DvCl")
            .AddConstructor<DvClStaticShadowingPropagationLossModel>()
            .AddAttribute("SigmaDb",
                          "Standard deviation of the log-normal shadowing [dB]. The sample is "
                          "drawn once per link and held for the whole run.",
                          DoubleValue(3.57),
                          MakeDoubleAccessor(
                              &DvClStaticShadowingPropagationLossModel::m_sigmaDb),
                          MakeDoubleChecker<double>(0.0));
    return tid;
}

DvClStaticShadowingPropagationLossModel::DvClStaticShadowingPropagationLossModel()
    : m_normal(CreateObject<NormalRandomVariable>())
{
    m_normal->SetAttribute("Mean", DoubleValue(0.0));
}

DvClStaticShadowingPropagationLossModel::~DvClStaticShadowingPropagationLossModel()
{
}

double
DvClStaticShadowingPropagationLossModel::GetLinkShadowingDb(uint32_t nodeA, uint32_t nodeB) const
{
    if (m_sigmaDb <= 0.0)
    {
        return 0.0;
    }
    // Reciprocal by construction: A->B and B->A must not draw twice, or the
    // link would look asymmetric and a route could be installed in one
    // direction while the reverse path silently failed.
    const auto key = std::minmax(nodeA, nodeB);
    const auto it = m_linkShadowing.find({key.first, key.second});
    if (it != m_linkShadowing.end())
    {
        return it->second;
    }
    const double draw = m_normal->GetValue(0.0, m_sigmaDb * m_sigmaDb);
    m_linkShadowing.emplace(std::make_pair(key.first, key.second), draw);
    return draw;
}

double
DvClStaticShadowingPropagationLossModel::DoCalcRxPower(double txPowerDbm,
                                                      Ptr<MobilityModel> a,
                                                      Ptr<MobilityModel> b) const
{
    Ptr<Node> nodeA = a ? a->GetObject<Node>() : nullptr;
    Ptr<Node> nodeB = b ? b->GetObject<Node>() : nullptr;
    if (!nodeA || !nodeB)
    {
        // Without node identity there is nothing stable to key the draw on, and
        // a per-call draw here would quietly reintroduce the per-transmission
        // behaviour this model exists to avoid. Leaving the power untouched is
        // the honest failure.
        NS_LOG_WARN("Sin Node asociado al MobilityModel: no se aplica sombreado a este enlace");
        return txPowerDbm;
    }
    return txPowerDbm - GetLinkShadowingDb(nodeA->GetId(), nodeB->GetId());
}

int64_t
DvClStaticShadowingPropagationLossModel::DoAssignStreams(int64_t stream)
{
    m_normal->SetStream(stream);
    return 1;
}

} // namespace dvcl
} // namespace ns3
