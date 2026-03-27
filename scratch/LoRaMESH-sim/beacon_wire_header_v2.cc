#include "beacon_wire_header_v2.h"

#include <algorithm>

namespace ns3
{

NS_OBJECT_ENSURE_REGISTERED(BeaconWireHeaderV2);

TypeId
BeaconWireHeaderV2::GetTypeId()
{
    static TypeId tid = TypeId("ns3::BeaconWireHeaderV2")
                            .SetParent<Header>()
                            .SetGroupName("Network")
                            .AddConstructor<BeaconWireHeaderV2>();
    return tid;
}

TypeId
BeaconWireHeaderV2::GetInstanceTypeId() const
{
    return GetTypeId();
}

uint32_t
BeaconWireHeaderV2::GetSerializedSize() const
{
    return kSerializedSize;
}

void
BeaconWireHeaderV2::Serialize(Buffer::Iterator i) const
{
    i.WriteU16(m_src);
    i.WriteU16(m_dst);
    i.WriteU8(m_flagsTtl);
}

uint32_t
BeaconWireHeaderV2::Deserialize(Buffer::Iterator i)
{
    m_src = i.ReadU16();
    m_dst = i.ReadU16();
    m_flagsTtl = i.ReadU8();
    return kSerializedSize;
}

void
BeaconWireHeaderV2::Print(std::ostream& os) const
{
    os << "src=" << m_src << " dst=" << m_dst
       << " type=" << static_cast<uint32_t>(GetPacketTypeV2(m_flagsTtl))
       << " ttl=" << static_cast<uint32_t>(GetTtlFromFlagsV2(m_flagsTtl));
}

void
BeaconWireHeaderV2::SerializeDvEntries(const std::vector<DvEntryWireV2>& entries,
                                       uint8_t* out,
                                       size_t maxlen)
{
    if (!out || maxlen < kEntrySize)
    {
        return;
    }

    size_t offset = 0;
    for (const auto& e : entries)
    {
        if (offset + kEntrySize > maxlen)
        {
            break;
        }
        const uint8_t score = static_cast<uint8_t>(std::min<uint16_t>(e.score, 100));
        out[offset + 0] = static_cast<uint8_t>(e.destination & 0xFF);
        out[offset + 1] = static_cast<uint8_t>((e.destination >> 8) & 0xFF);
        out[offset + 2] = score;
        offset += kEntrySize;
    }
}

void
BeaconWireHeaderV2::DeserializeDvEntries(const uint8_t* in,
                                         size_t len,
                                         std::vector<DvEntryWireV2>& out)
{
    out.clear();
    if (!in || len < kEntrySize)
    {
        return;
    }

    const size_t count = len / kEntrySize;
    out.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        const size_t offset = i * kEntrySize;
        DvEntryWireV2 e;
        e.destination = static_cast<uint16_t>(in[offset + 0]) |
                        (static_cast<uint16_t>(in[offset + 1]) << 8);
        e.score = in[offset + 2];
        out.push_back(e);
    }
}

} // namespace ns3

