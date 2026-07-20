/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-regional-profile.h"

#include "ns3/double.h"

namespace ns3
{
namespace dvcl
{

NS_OBJECT_ENSURE_REGISTERED(DvClRegionalProfile);
NS_OBJECT_ENSURE_REGISTERED(DvClEu868Profile);
NS_OBJECT_ENSURE_REGISTERED(DvClUs915Profile);

TypeId
DvClRegionalProfile::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClRegionalProfile").SetParent<Object>().SetGroupName("DvCl");
    return tid;
}

TypeId
DvClEu868Profile::GetTypeId()
{
    static TypeId tid = TypeId("ns3::dvcl::DvClEu868Profile")
                            .SetParent<DvClRegionalProfile>()
                            .SetGroupName("DvCl")
                            .AddConstructor<DvClEu868Profile>()
                            .AddAttribute("DutyCycleLimit",
                                          "Duty cycle as a fraction on the sub-band in use.",
                                          DoubleValue(0.01),
                                          MakeDoubleAccessor(&DvClEu868Profile::m_dutyCycleLimit),
                                          MakeDoubleChecker<double>(0.0, 1.0));
    return tid;
}

TypeId
DvClUs915Profile::GetTypeId()
{
    static TypeId tid = TypeId("ns3::dvcl::DvClUs915Profile")
                            .SetParent<DvClRegionalProfile>()
                            .SetGroupName("DvCl")
                            .AddConstructor<DvClUs915Profile>();
    return tid;
}

} // namespace dvcl
} // namespace ns3
