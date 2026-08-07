#ifndef LORAMESH_HELPER_H
#define LORAMESH_HELPER_H

#include "ns3/object-factory.h"
#include "ns3/node-container.h"
#include "ns3/net-device-container.h"
#include "ns3/application-container.h"
#include "ns3/loramesh-routing-protocol.h"
#include "ns3/loramesh-energy-model.h"
#include "ns3/loramesh-tracer.h"

namespace ns3 {

class LorameshHelper
{
public:
  LorameshHelper ();
  
  void SetAttribute (std::string name, const AttributeValue &value);
  
  ApplicationContainer Install (NodeContainer c) const;

  void EnableTraces (std::string filename);

private:
  ObjectFactory m_factory;
  std::string m_traceFilename;
};

} // namespace ns3

#endif // LORAMESH_HELPER_H
