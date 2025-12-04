#include "mesh_mac_header.h"

namespace ns3 {

TypeId
MeshMacHeader::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::MeshMacHeader")
    .SetParent<Header> ()
    .AddConstructor<MeshMacHeader> ();
  return tid;
}

TypeId
MeshMacHeader::GetInstanceTypeId () const
{
  return GetTypeId ();
}

uint32_t
MeshMacHeader::GetSerializedSize () const
{
  return 12; // two Mac48Address
}

void
MeshMacHeader::Serialize (Buffer::Iterator i) const
{
  uint8_t buf[6];
  m_dst.CopyTo (buf);
  i.Write (buf, 6);
  m_src.CopyTo (buf);
  i.Write (buf, 6);
}

uint32_t
MeshMacHeader::Deserialize (Buffer::Iterator i)
{
  uint8_t buf[6];
  i.Read (buf, 6);
  m_dst.CopyFrom (buf);
  i.Read (buf, 6);
  m_src.CopyFrom (buf);
  return GetSerializedSize ();
}

void
MeshMacHeader::Print (std::ostream& os) const
{
  os << "src=" << m_src << " dst=" << m_dst;
}

} // namespace ns3
