#ifndef LORAMESH_TRACER_H
#define LORAMESH_TRACER_H

#include "ns3/object.h"
#include "ns3/nstime.h"
#include <fstream>
#include <string>

namespace ns3 {

class LorameshTracer : public Object
{
public:
  static TypeId GetTypeId ();
  LorameshTracer ();
  ~LorameshTracer () override;

  void Enable (std::string filename);

  void TraceTx (uint32_t nodeId, uint32_t seq, uint32_t dst, uint8_t ttl, 
                int16_t rssi, uint16_t battery, uint16_t score, bool success);
  
  void TraceRx (uint32_t nodeId, uint32_t src, uint32_t dst, uint32_t seq, 
                uint8_t ttl, uint8_t hops, int16_t rssi, uint16_t battery, 
                uint16_t score, bool forwarded);

  void TraceRouteUpdate (uint32_t nodeId, uint32_t dst, uint32_t nextHop,
                         uint8_t hops, uint16_t score, uint32_t seq, std::string action);

private:
  std::ofstream m_os;
  bool m_enabled;
  std::string m_sep;
};

} // namespace ns3

#endif // LORAMESH_TRACER_H
