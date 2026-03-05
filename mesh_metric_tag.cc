#include "mesh_metric_tag.h"

#include "ns3/log.h"

#include <algorithm>
#include <cmath>

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("MeshMetricTag");
NS_OBJECT_ENSURE_REGISTERED(MeshMetricTag);

TypeId
MeshMetricTag::GetTypeId()
{
    static TypeId tid = TypeId("ns3::MeshMetricTag")
                            .SetParent<Tag>()
                            .SetGroupName("Network")
                            .AddConstructor<MeshMetricTag>();
    return tid;
}

TypeId
MeshMetricTag::GetInstanceTypeId() const
{
    return GetTypeId();
}

uint32_t
MeshMetricTag::GetSerializedSize() const
{
    return 23;
}

void
MeshMetricTag::Serialize(TagBuffer i) const
{
    i.WriteU16(m_src);
    i.WriteU16(m_dst);
    i.WriteU32(m_seq);
    i.WriteU8(m_ttl);
    i.WriteU8(m_hops);
    i.WriteU8(m_sf);
    i.WriteU32(m_toaUs);
    i.WriteU16(m_batt_mV);
    i.WriteU16(m_scoreX100);
    i.WriteU16(m_prevHop);
    i.WriteU16(m_expectedNextHop);
}

void
MeshMetricTag::Deserialize(TagBuffer i)
{
    m_src = i.ReadU16();
    m_dst = i.ReadU16();
    m_seq = i.ReadU32();
    m_ttl = i.ReadU8();
    m_hops = i.ReadU8();
    m_sf = i.ReadU8();
    m_toaUs = i.ReadU32();
    m_batt_mV = i.ReadU16();
    m_scoreX100 = i.ReadU16();
    m_prevHop = i.ReadU16();
    m_expectedNextHop = i.ReadU16();
}

void
MeshMetricTag::Print(std::ostream& os) const
{
    os << "src=" << m_src << " dst=" << m_dst << " seq=" << m_seq << " ttl=" << unsigned(m_ttl)
       << " hops=" << unsigned(m_hops) << " sf=" << unsigned(m_sf)
       << " toaUs=" << m_toaUs << " batt=" << m_batt_mV << " score=" << m_scoreX100
       << " prevHop=" << m_prevHop << " expNextHop=" << m_expectedNextHop;
}

void
MeshMetricTag::SerializeRoutePayload(const std::vector<RoutePayloadEntry>& entries,
                                     uint8_t* out,
                                     size_t maxlen)
{
    if (!out || maxlen < kRoutePayloadSize)
    {
        return;
    }

    size_t offset = 0;
    for (const auto& e : entries)
    {
        if (offset + kRoutePayloadSize > maxlen)
        {
            break;
        }

        const uint8_t hops = e.hops;
        const uint8_t sf = e.sf;
        const uint8_t score = static_cast<uint8_t>(std::min<uint16_t>(e.score, 100));

        // REMOVED: rssiQ - no se usa en métrica

        const uint16_t battClamped =
            static_cast<uint16_t>(std::min<uint16_t>(e.batt_mV, kBattMaxMv));
        const uint8_t battQ = static_cast<uint8_t>(
            std::lround(static_cast<double>(battClamped) * 255.0 / kBattMaxMv));

        out[offset + 0] = static_cast<uint8_t>(e.dst & 0xFF);
        out[offset + 1] = static_cast<uint8_t>((e.dst >> 8) & 0xFF);
        out[offset + 2] = hops;
        out[offset + 3] = sf;
        out[offset + 4] = score;
        out[offset + 5] = battQ;

        offset += kRoutePayloadSize;
    }
}

void
MeshMetricTag::DeserializeRoutePayload(const uint8_t* in,
                                       size_t len,
                                       std::vector<RoutePayloadEntry>& entries)
{
    entries.clear();
    if (!in || len < kRoutePayloadSize)
    {
        return;
    }

    const size_t count = len / kRoutePayloadSize;
    entries.reserve(count);

    for (size_t i = 0; i < count; ++i)
    {
        const size_t offset = i * kRoutePayloadSize;
        RoutePayloadEntry e;
        e.dst = static_cast<uint16_t>(in[offset + 0]) |
                (static_cast<uint16_t>(in[offset + 1]) << 8);
        e.hops = in[offset + 2];
        e.sf = in[offset + 3];
        e.score = in[offset + 4];

        // REMOVED: rssiQ - no se usa en métrica

        const uint8_t battQ = in[offset + 5];
        e.batt_mV =
            static_cast<uint16_t>(std::lround(static_cast<double>(battQ) * kBattMaxMv / 255.0));

        entries.push_back(e);
    }
}

} // namespace ns3
