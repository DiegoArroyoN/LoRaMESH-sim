#include "data_wire_header_pueyo7b.h"

namespace ns3
{

NS_OBJECT_ENSURE_REGISTERED(DataWireHeaderPueyo7b);

TypeId
DataWireHeaderPueyo7b::GetTypeId()
{
    static TypeId tid = TypeId("ns3::DataWireHeaderPueyo7b")
                            .SetParent<Header>()
                            .SetGroupName("Network")
                            .AddConstructor<DataWireHeaderPueyo7b>();
    return tid;
}

TypeId
DataWireHeaderPueyo7b::GetInstanceTypeId() const
{
    return GetTypeId();
}

uint32_t
DataWireHeaderPueyo7b::GetSerializedSize() const
{
    return kSerializedSize;
}

void
DataWireHeaderPueyo7b::Serialize(Buffer::Iterator i) const
{
    i.WriteU16(m_src);
    i.WriteU16(m_dst);
    i.WriteU16(m_via);
    i.WriteU8(m_flagsTtl);
}

uint32_t
DataWireHeaderPueyo7b::Deserialize(Buffer::Iterator i)
{
    m_src = i.ReadU16();
    m_dst = i.ReadU16();
    m_via = i.ReadU16();
    m_flagsTtl = i.ReadU8();
    return kSerializedSize;
}

void
DataWireHeaderPueyo7b::Print(std::ostream& os) const
{
    os << "src=" << m_src << " dst=" << m_dst << " via=" << m_via
       << " type=" << static_cast<uint32_t>(GetPacketTypeV2(m_flagsTtl))
       << " ttl=" << static_cast<uint32_t>(GetTtlFromFlagsV2(m_flagsTtl));
}

} // namespace ns3
