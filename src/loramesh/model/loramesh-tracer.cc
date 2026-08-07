#include "loramesh-tracer.h"
#include "ns3/simulator.h"
#include "ns3/log.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE ("LorameshTracer");
NS_OBJECT_ENSURE_REGISTERED (LorameshTracer);

TypeId
LorameshTracer::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::LorameshTracer")
    .SetParent<Object> ()
    .SetGroupName ("Loramesh")
    .AddConstructor<LorameshTracer> ();
  return tid;
}

LorameshTracer::LorameshTracer ()
  : m_enabled (false), m_sep (",")
{
}

LorameshTracer::~LorameshTracer ()
{
  if (m_os.is_open ())
    {
      m_os.close ();
    }
}

void
LorameshTracer::Enable (std::string filename)
{
  m_os.open (filename.c_str (), std::ios_base::out | std::ios_base::trunc);
  if (!m_os.is_open ())
    {
      NS_LOG_ERROR ("Could not open trace file: " << filename);
      return;
    }
  
  m_enabled = true;
  // Header
  m_os << "Time" << m_sep << "Node" << m_sep << "Type" << m_sep 
       << "Src" << m_sep << "Dst" << m_sep << "Seq" << m_sep
       << "TTL" << m_sep << "Hops" << m_sep << "RSSI" << m_sep
       << "Batt" << m_sep << "Score" << m_sep << "Details" << std::endl;
}

void
LorameshTracer::TraceTx (uint32_t nodeId, uint32_t seq, uint32_t dst, uint8_t ttl, 
                         int16_t rssi, uint16_t battery, uint16_t score, bool success)
{
  if (!m_enabled) return;
  
  m_os << Simulator::Now ().GetSeconds () << m_sep
       << nodeId << m_sep
       << "TX" << m_sep
       << nodeId << m_sep // Src is self
       << dst << m_sep
       << seq << m_sep
       << (int)ttl << m_sep
       << 0 << m_sep // Hops
       << rssi << m_sep
       << battery << m_sep
       << score << m_sep
       << (success ? "OK" : "FAIL") << std::endl;
}

void
LorameshTracer::TraceRx (uint32_t nodeId, uint32_t src, uint32_t dst, uint32_t seq, 
                         uint8_t ttl, uint8_t hops, int16_t rssi, uint16_t battery, 
                         uint16_t score, bool forwarded)
{
  if (!m_enabled) return;

  m_os << Simulator::Now ().GetSeconds () << m_sep
       << nodeId << m_sep
       << "RX" << m_sep
       << src << m_sep
       << dst << m_sep
       << seq << m_sep
       << (int)ttl << m_sep
       << (int)hops << m_sep
       << rssi << m_sep
       << battery << m_sep
       << score << m_sep
       << (forwarded ? "FWD" : "DELIVERED") << std::endl;
}

void
LorameshTracer::TraceRouteUpdate (uint32_t nodeId, uint32_t dst, uint32_t nextHop,
                                  uint8_t hops, uint16_t score, uint32_t seq, std::string action)
{
  if (!m_enabled) return;

  m_os << Simulator::Now ().GetSeconds () << m_sep
       << nodeId << m_sep
       << "ROUTE" << m_sep
       << "-" << m_sep // Src
       << dst << m_sep
       << seq << m_sep
       << "-" << m_sep // TTL
       << (int)hops << m_sep
       << "-" << m_sep // RSSI
       << "-" << m_sep // Batt
       << score << m_sep
       << action << ":via=" << nextHop << std::endl;
}

} // namespace ns3
