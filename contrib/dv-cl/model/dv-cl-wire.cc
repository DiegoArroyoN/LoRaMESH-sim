/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-wire.h"

#include "ns3/log.h"

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("DvClWire");

namespace dvcl
{

NS_OBJECT_ENSURE_REGISTERED(DvClBeaconHeader);
NS_OBJECT_ENSURE_REGISTERED(DvClDataHeader);

// ---------------------------------------------------------------- beacon

TypeId
DvClBeaconHeader::GetTypeId()
{
    static TypeId tid = TypeId("ns3::dvcl::DvClBeaconHeader")
                            .SetParent<Header>()
                            .SetGroupName("DvCl")
                            .AddConstructor<DvClBeaconHeader>();
    return tid;
}

TypeId
DvClBeaconHeader::GetInstanceTypeId() const
{
    return GetTypeId();
}

uint32_t
DvClBeaconHeader::GetSerializedSize() const
{
    return kSerializedSize;
}

void
DvClBeaconHeader::Serialize(Buffer::Iterator start) const
{
    start.WriteU16(m_src);
    start.WriteU16(m_dst);
    start.WriteU8(m_flagsTtl);
    start.WriteU8(m_soc);
}

uint32_t
DvClBeaconHeader::Deserialize(Buffer::Iterator start)
{
    m_src = start.ReadU16();
    m_dst = start.ReadU16();
    m_flagsTtl = start.ReadU8();
    m_soc = start.ReadU8();
    return kSerializedSize;
}

void
DvClBeaconHeader::Print(std::ostream& os) const
{
    os << "src=" << m_src << " dst=" << m_dst
       << " type=" << (UnpackType(m_flagsTtl) == DvClPacketType::BEACON ? "BEACON" : "DATA")
       << " ttl=" << unsigned(UnpackTtl(m_flagsTtl)) << " soc=" << unsigned(m_soc);
}

// ------------------------------------------------------------------ data

TypeId
DvClDataHeader::GetTypeId()
{
    static TypeId tid = TypeId("ns3::dvcl::DvClDataHeader")
                            .SetParent<Header>()
                            .SetGroupName("DvCl")
                            .AddConstructor<DvClDataHeader>();
    return tid;
}

TypeId
DvClDataHeader::GetInstanceTypeId() const
{
    return GetTypeId();
}

uint32_t
DvClDataHeader::GetSerializedSize() const
{
    return kSerializedSize;
}

void
DvClDataHeader::Serialize(Buffer::Iterator start) const
{
    start.WriteU16(m_src);
    start.WriteU16(m_dst);
    start.WriteU16(m_via);
    start.WriteU8(m_flagsTtl);
}

uint32_t
DvClDataHeader::Deserialize(Buffer::Iterator start)
{
    m_src = start.ReadU16();
    m_dst = start.ReadU16();
    m_via = start.ReadU16();
    m_flagsTtl = start.ReadU8();
    return kSerializedSize;
}

void
DvClDataHeader::Print(std::ostream& os) const
{
    os << "src=" << m_src << " dst=" << m_dst << " via=" << m_via
       << " ttl=" << unsigned(UnpackTtl(m_flagsTtl));
}

// --------------------------------------------------------------- entries

uint32_t
SerializeDvEntries(const std::vector<DvClDvEntry>& entries, uint8_t* out, uint32_t maxLen)
{
    if (!out || maxLen < DvClDvEntry::kEntrySize)
    {
        return 0;
    }
    uint32_t offset = 0;
    for (const auto& e : entries)
    {
        if (offset + DvClDvEntry::kEntrySize > maxLen)
        {
            NS_LOG_DEBUG("DV entries truncated at " << offset << " bytes");
            break;
        }
        out[offset + 0] = static_cast<uint8_t>(e.destination & 0xFF);
        out[offset + 1] = static_cast<uint8_t>((e.destination >> 8) & 0xFF);
        out[offset + 2] = e.score;
        offset += DvClDvEntry::kEntrySize;
    }
    return offset;
}

void
DeserializeDvEntries(const uint8_t* in, uint32_t len, std::vector<DvClDvEntry>& out)
{
    out.clear();
    if (!in || len < DvClDvEntry::kEntrySize)
    {
        return;
    }
    const uint32_t count = len / DvClDvEntry::kEntrySize;
    out.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
    {
        const uint32_t o = i * DvClDvEntry::kEntrySize;
        DvClDvEntry e;
        e.destination = static_cast<uint16_t>(in[o + 0]) | (static_cast<uint16_t>(in[o + 1]) << 8);
        e.score = in[o + 2];
        out.push_back(e);
    }
}

} // namespace dvcl
} // namespace ns3
