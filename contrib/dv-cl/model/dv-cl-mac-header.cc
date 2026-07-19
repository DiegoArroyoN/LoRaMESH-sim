/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-mac-header.h"

namespace ns3 {
namespace dvcl {

TypeId
DvClMacHeader::GetTypeId ()
{
  static TypeId tid = TypeId("ns3::dvcl::DvClMacHeader")
    .SetParent<Header> ()
    .AddConstructor<DvClMacHeader> ();
  return tid;
}

TypeId
DvClMacHeader::GetInstanceTypeId () const
{
  return GetTypeId ();
}

uint32_t
DvClMacHeader::GetSerializedSize () const
{
  return 12; // two Mac48Address
}

void
DvClMacHeader::Serialize (Buffer::Iterator i) const
{
  uint8_t buf[6];
  m_dst.CopyTo (buf);
  i.Write (buf, 6);
  m_src.CopyTo (buf);
  i.Write (buf, 6);
}

uint32_t
DvClMacHeader::Deserialize (Buffer::Iterator i)
{
  uint8_t buf[6];
  i.Read (buf, 6);
  m_dst.CopyFrom (buf);
  i.Read (buf, 6);
  m_src.CopyFrom (buf);
  return GetSerializedSize ();
}

void
DvClMacHeader::Print (std::ostream& os) const
{
  os << "src=" << m_src << " dst=" << m_dst;
}

} // namespace dvcl
} // namespace ns3
