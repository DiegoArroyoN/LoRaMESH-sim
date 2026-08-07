#include "loramesh-helper.h"
#include "ns3/loramesh-routing-protocol.h"
#include "ns3/loramesh-energy-model.h"
#include "ns3/loramesh-tracer.h"
#include "ns3/names.h"

namespace ns3 {

LorameshHelper::LorameshHelper ()
{
  m_factory.SetTypeId ("ns3::LorameshRoutingProtocol");
}

void
LorameshHelper::SetAttribute (std::string name, const AttributeValue &value)
{
  m_factory.Set (name, value);
}

void
LorameshHelper::EnableTraces (std::string filename)
{
  m_traceFilename = filename;
}

ApplicationContainer
LorameshHelper::Install (NodeContainer c) const
{
  ApplicationContainer apps;
  
  // Create a single tracer instance if enabled
  Ptr<LorameshTracer> tracer;
  if (!m_traceFilename.empty ())
    {
      tracer = CreateObject<LorameshTracer> ();
      tracer->Enable (m_traceFilename);
    }

  for (NodeContainer::Iterator i = c.Begin (); i != c.End (); ++i)
    {
      Ptr<Node> node = *i;
      
      // Create Protocol App
      Ptr<LorameshRoutingProtocol> app = m_factory.Create<LorameshRoutingProtocol> ();
      
      // Create Energy Model
      Ptr<LorameshEnergyModel> energy = CreateObject<LorameshEnergyModel> ();
      app->SetEnergyModel (energy);
      
      // Set Tracer
      if (tracer)
        {
          app->SetTracer (tracer);
        }

      node->AddApplication (app);
      apps.Add (app);
    }
  
  return apps;
}

} // namespace ns3
