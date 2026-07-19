/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-stats-sink.h"

namespace ns3
{
namespace dvcl
{

NS_OBJECT_ENSURE_REGISTERED(DvClStatsSink);

TypeId
DvClStatsSink::GetTypeId()
{
    static TypeId tid = TypeId("ns3::dvcl::DvClStatsSink")
                            .SetParent<Object>()
                            .SetGroupName("DvCl");
    return tid;
}

} // namespace dvcl
} // namespace ns3
