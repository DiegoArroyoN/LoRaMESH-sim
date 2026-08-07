/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-metric-tag.h"

#include "ns3/log.h"

#include <algorithm>
#include <cmath>

namespace ns3
{
namespace dvcl
{

NS_LOG_COMPONENT_DEFINE("DvClMetricTag");
NS_OBJECT_ENSURE_REGISTERED(DvClMetricTag);
NS_OBJECT_ENSURE_REGISTERED(DvClFlatBeaconTag);

// ---------------------------------------------------------------------------
// DvClFlatBeaconTag: emulacion del setByteLength(12) de FLoRaMesh.
// Ver la nota extensa en dv-cl-metric-tag.h. Solo se instancia cuando el
// atributo PueyoFlatBeaconBytes es > 0, que no es el valor por defecto.
// ---------------------------------------------------------------------------

TypeId
DvClFlatBeaconTag::GetTypeId()
{
    static TypeId tid = TypeId("ns3::dvcl::DvClFlatBeaconTag")
                            .SetParent<Tag>()
                            .SetGroupName("DvCl")
                            .AddConstructor<DvClFlatBeaconTag>();
    return tid;
}

TypeId
DvClFlatBeaconTag::GetInstanceTypeId() const
{
    return GetTypeId();
}

void
DvClFlatBeaconTag::SetPayload(const uint8_t* data, uint32_t len)
{
    m_payload.clear();
    if (!data || len == 0)
    {
        return;
    }
    const uint32_t n = std::min<uint32_t>(len, kMaxPayloadBytes);
    m_payload.assign(data, data + n);
}

uint32_t
DvClFlatBeaconTag::GetSerializedSize() const
{
    return 2 + static_cast<uint32_t>(m_payload.size());
}

void
DvClFlatBeaconTag::Serialize(TagBuffer i) const
{
    i.WriteU16(static_cast<uint16_t>(m_payload.size()));
    for (uint8_t b : m_payload)
    {
        i.WriteU8(b);
    }
}

void
DvClFlatBeaconTag::Deserialize(TagBuffer i)
{
    const uint16_t n = i.ReadU16();
    m_payload.assign(std::min<uint16_t>(n, kMaxPayloadBytes), 0);
    for (uint16_t k = 0; k < n; ++k)
    {
        const uint8_t b = i.ReadU8();
        if (k < m_payload.size())
        {
            m_payload[k] = b;
        }
    }
}

void
DvClFlatBeaconTag::Print(std::ostream& os) const
{
    os << "flatBeaconPayloadBytes=" << m_payload.size();
}

TypeId
DvClMetricTag::GetTypeId()
{
    static TypeId tid = TypeId("ns3::dvcl::DvClMetricTag")
                            .SetParent<Tag>()
                            .SetGroupName("DvCl")
                            .AddConstructor<DvClMetricTag>();
    return tid;
}

TypeId
DvClMetricTag::GetInstanceTypeId() const
{
    return GetTypeId();
}

uint32_t
DvClMetricTag::GetSerializedSize() const
{
    return 24; // era 23; +1 byte §DC-wire (m_dcRemaining)
}

void
DvClMetricTag::Serialize(TagBuffer i) const
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
    i.WriteU8(m_dcRemaining); // §DC-wire
}

void
DvClMetricTag::Deserialize(TagBuffer i)
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
    m_dcRemaining = i.ReadU8(); // §DC-wire
}

void
DvClMetricTag::Print(std::ostream& os) const
{
    os << "src=" << m_src << " dst=" << m_dst << " seq=" << m_seq << " ttl=" << unsigned(m_ttl)
       << " hops=" << unsigned(m_hops) << " sf=" << unsigned(m_sf) << " toaUs=" << m_toaUs
       << " batt=" << m_batt_mV << " score=" << m_scoreX100 << " prevHop=" << m_prevHop
       << " expNextHop=" << m_expectedNextHop << " dcRem=" << unsigned(m_dcRemaining);
}

void
DvClMetricTag::SerializeRoutePayload(const std::vector<RoutePayloadEntry>& entries,
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
DvClMetricTag::DeserializeRoutePayload(const uint8_t* in,
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
        e.dst =
            static_cast<uint16_t>(in[offset + 0]) | (static_cast<uint16_t>(in[offset + 1]) << 8);
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

} // namespace dvcl
} // namespace ns3
