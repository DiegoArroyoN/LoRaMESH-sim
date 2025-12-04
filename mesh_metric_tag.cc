#include "mesh_metric_tag.h"
#include "ns3/log.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE ("MeshMetricTag");
NS_OBJECT_ENSURE_REGISTERED (MeshMetricTag);

TypeId
MeshMetricTag::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::MeshMetricTag")
    .SetParent<Tag> ()
    .SetGroupName ("Network")
    .AddConstructor<MeshMetricTag> ();
  return tid;
}

TypeId
MeshMetricTag::GetInstanceTypeId () const
{
  return GetTypeId ();
}

uint32_t
MeshMetricTag::GetSerializedSize () const
{
  return 21;
}

void
MeshMetricTag::Serialize (TagBuffer i) const
{
  i.WriteU16 (m_src);
  i.WriteU16 (m_dst);
  i.WriteU32 (m_seq);
  i.WriteU8  (m_ttl);
  i.WriteU8  (m_hops);
  i.WriteU8  (m_sf);
  i.WriteU32 (m_toaUs);
  i.WriteU16 (static_cast<uint16_t>(m_rssiDbm)); // cuidado con signo en print
  i.WriteU16 (m_batt_mV);
  i.WriteU16 (m_scoreX100);
}

void
MeshMetricTag::Deserialize (TagBuffer i)
{
  m_src       = i.ReadU16 ();
  m_dst       = i.ReadU16 ();
  m_seq       = i.ReadU32 ();
  m_ttl       = i.ReadU8  ();
  m_hops      = i.ReadU8  ();
  m_sf        = i.ReadU8  ();
  m_toaUs     = i.ReadU32 ();
  m_rssiDbm   = static_cast<int16_t>(i.ReadU16 ());
  m_batt_mV   = i.ReadU16 ();
  m_scoreX100 = i.ReadU16 ();
}

void
MeshMetricTag::Print (std::ostream &os) const
{
  os << "src=" << m_src
     << " dst=" << m_dst
     << " seq=" << m_seq
     << " ttl=" << unsigned(m_ttl)
     << " hops=" << unsigned(m_hops)
     << " sf=" << unsigned(m_sf)
     << " toaUs=" << m_toaUs
     << " rssi=" << m_rssiDbm << "dBm"
     << " batt=" << m_batt_mV << "mV"
     << " score=" << m_scoreX100;
}

} // namespace ns3
