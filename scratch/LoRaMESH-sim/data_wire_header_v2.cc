#include "data_wire_header_v2.h"

namespace ns3
{

NS_OBJECT_ENSURE_REGISTERED(DataWireHeaderV2);

TypeId
DataWireHeaderV2::GetTypeId()
{
    static TypeId tid = TypeId("ns3::DataWireHeaderV2")
                            .SetParent<Header>()
                            .SetGroupName("Network")
                            .AddConstructor<DataWireHeaderV2>();
    return tid;
}

TypeId
DataWireHeaderV2::GetInstanceTypeId() const
{
    return GetTypeId();
}

uint32_t
DataWireHeaderV2::GetSerializedSize() const
{
    return kSerializedSize;
}

void
DataWireHeaderV2::Serialize(Buffer::Iterator i) const
{
    i.WriteU16(m_src);
    i.WriteU16(m_dst);
    i.WriteU16(m_via);
    i.WriteU8(m_flagsTtl);
    i.WriteU16(m_seq16);
}

uint32_t
DataWireHeaderV2::Deserialize(Buffer::Iterator i)
{
    m_src = i.ReadU16();
    m_dst = i.ReadU16();
    m_via = i.ReadU16();
    m_flagsTtl = i.ReadU8();
    m_seq16 = i.ReadU16();
    return kSerializedSize;
}

void
DataWireHeaderV2::Print(std::ostream& os) const
{
    os << "src=" << m_src << " dst=" << m_dst << " via=" << m_via
       << " type=" << static_cast<uint32_t>(GetPacketTypeV2(m_flagsTtl))
       << " ttl=" << static_cast<uint32_t>(GetTtlFromFlagsV2(m_flagsTtl))
       << " seq16=" << m_seq16;
}

} // namespace ns3

