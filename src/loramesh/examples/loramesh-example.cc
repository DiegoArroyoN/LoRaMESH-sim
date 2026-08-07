#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/mobility-module.h"
#include "ns3/lorawan-module.h"
#include "ns3/loramesh-helper.h"
#include "ns3/mesh-lora-net-device.h"
#include "ns3/simple-gateway-lora-phy.h"

using namespace ns3;
using namespace lorawan;

NS_LOG_COMPONENT_DEFINE ("LorameshExample");

int main (int argc, char *argv[])
{
  uint32_t nNodes = 5;
  double spacing = 100.0; // meters
  std::string scenario = "Baseline"; // Baseline, Stress, Mobility
  double duration = 600.0; // seconds
  double dataPeriod = 60.0; // seconds
  bool verbose = false;

  CommandLine cmd;
  cmd.AddValue ("nNodes", "Number of nodes", nNodes);
  cmd.AddValue ("spacing", "Distance between nodes [m]", spacing);
  cmd.AddValue ("scenario", "Scenario: Baseline, Stress, Mobility", scenario);
  cmd.AddValue ("duration", "Simulation duration [s]", duration);
  cmd.AddValue ("dataPeriod", "Data generation period [s]", dataPeriod);
  cmd.AddValue ("verbose", "Enable logging", verbose);
  cmd.Parse (argc, argv);

  if (verbose)
    {
      LogComponentEnable ("LorameshRoutingProtocol", LOG_LEVEL_INFO);
      LogComponentEnable ("LorameshEnergyModel", LOG_LEVEL_INFO);
      LogComponentEnable ("LorameshExample", LOG_LEVEL_INFO);
    }

  NS_LOG_INFO ("=== LoRaMESH Example: " << scenario << " ===");
  NS_LOG_INFO ("Nodes: " << nNodes << ", Spacing: " << spacing << "m");
  NS_LOG_INFO ("Data Period: " << dataPeriod << "s");

  // 1. Create Nodes
  NodeContainer nodes;
  nodes.Create (nNodes);

  // 2. Mobility
  MobilityHelper mobility;
  Ptr<ListPositionAllocator> alloc = CreateObject<ListPositionAllocator> ();
  for (uint32_t i = 0; i < nNodes; ++i)
    {
      if (scenario == "Grid") // Simple Grid if needed, but let's stick to Linear for now as per prompt
        {
           // ...
        }
      // Linear topology
      alloc->Add (Vector (i * spacing, 0.0, 0.0));
    }
  mobility.SetPositionAllocator (alloc);
  
  if (scenario == "Mobility")
    {
      // Move nodes back and forth
      mobility.SetMobilityModel ("ns3::ConstantVelocityMobilityModel");
    }
  else
    {
      mobility.SetMobilityModel ("ns3::ConstantPositionMobilityModel");
    }
  mobility.Install (nodes);

  if (scenario == "Mobility")
    {
      // Set velocity for some nodes
      for (uint32_t i = 0; i < nNodes - 1; ++i) // Keep GW static (last node?)
        {
          Ptr<ConstantVelocityMobilityModel> mob = nodes.Get (i)->GetObject<ConstantVelocityMobilityModel> ();
          mob->SetVelocity (Vector (0.5, 0.0, 0.0)); // Slow movement
        }
    }

  // 3. LoRa Channel
  Ptr<LogDistancePropagationLossModel> loss = CreateObject<LogDistancePropagationLossModel> ();
  loss->SetPathLossExponent (3.0); // Higher loss to force multi-hop
  loss->SetReference (1, 7.7);
  
  Ptr<PropagationDelayModel> delay = CreateObject<ConstantSpeedPropagationDelayModel> ();
  Ptr<LoraChannel> channel = CreateObject<LoraChannel> (loss, delay);

  // 4. Devices (Manual setup for MeshLoraNetDevice)
  NetDeviceContainer devices;
  for (uint32_t i = 0; i < nodes.GetN (); ++i)
    {
      Ptr<Node> node = nodes.Get (i);
      
      // Use SimpleGatewayLoraPhy for multi-SF reception capability
      Ptr<SimpleGatewayLoraPhy> phy = CreateObject<SimpleGatewayLoraPhy> ();
      phy->SetMobility (node->GetObject<MobilityModel> ());
      phy->SetChannel (channel);
      phy->AddFrequency (915000000);
      for (int p = 0; p < 6; ++p) phy->AddReceptionPath (); // Listen on all SFs
      
      Ptr<MeshLoraNetDevice> dev = CreateObject<MeshLoraNetDevice> ();
      dev->SetNode (node);
      dev->SetAddress (Mac48Address::Allocate ());
      dev->SetMtu (255);
      dev->SetPhy (phy);
      
      channel->Add (phy);
      node->AddDevice (dev);
      devices.Add (dev);
      
      // Force Standby (RX)
      phy->SwitchToStandby ();
    }

  // 5. Install LoRaMESH Stack
  LorameshHelper helper;
  helper.SetAttribute ("DataGenerationPeriod", TimeValue (Seconds (dataPeriod)));
  helper.SetAttribute ("GatewayId", UintegerValue (nNodes - 1)); // Last node is GW
  
  if (scenario == "Stress")
    {
      helper.SetAttribute ("BeaconInterval", TimeValue (Seconds (30)));
    }
  
  helper.EnableTraces ("loramesh-trace-" + scenario + ".csv");
  helper.Install (nodes);

  // 6. Run
  Simulator::Stop (Seconds (duration));
  Simulator::Run ();
  Simulator::Destroy ();

  return 0;
}
