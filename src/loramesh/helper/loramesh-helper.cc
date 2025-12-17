#include "loramesh-helper.h"

#include "ns3/propagation-loss-model.h"
#include "ns3/propagation-delay-model.h"
#include "ns3/mobility-helper.h"
#include "ns3/position-allocator.h"
#include "ns3/simulator.h"
#include "ns3/pcap-file-wrapper.h"
#include "ns3/mac48-address.h"
#include "ns3/simple-gateway-lora-phy.h"
#include "ns3/lora-channel.h"
#include "ns3/mesh_dv_app.h"
#include "ns3/mesh_lora_net_device.h"
#include "ns3/simple-end-device-lora-phy.h"

namespace ns3
{

NS_LOG_COMPONENT_DEFINE ("LoraMeshHelper");

namespace loramesh
{

NS_OBJECT_ENSURE_REGISTERED (LoraMeshHelper);

TypeId
LoraMeshHelper::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::loramesh::LoraMeshHelper")
    .SetParent<Object> ()
    .AddConstructor<LoraMeshHelper> ();
  return tid;
}

LoraMeshHelper::LoraMeshHelper () = default;

void
LoraMeshHelper::SetConfig (const LoraMeshConfig& cfg)
{
  m_cfg = cfg;
}

void
LoraMeshHelper::EnablePcap (const std::string& prefix)
{
  m_enablePcap = true;
  if (!prefix.empty ())
    {
      m_pcapPrefix = prefix;
    }
}

void
LoraMeshHelper::Install (NodeContainer& nodes)
{
  ConfigureMobility (nodes);
  InstallDevices (nodes);
  InstallApplications (nodes);
  ForceStandbyMode (nodes);
}

void
LoraMeshHelper::ConfigureMobility (NodeContainer& nodes)
{
  MobilityHelper mobility;
  Ptr<ListPositionAllocator> alloc = CreateObject<ListPositionAllocator> ();

  const uint32_t totalNodes = nodes.GetN ();
  const uint32_t gwIndex = (totalNodes > 0) ? (totalNodes - 1) : 0;

  double currentX = 0.0;
  for (uint32_t i = 0; i < totalNodes; ++i)
    {
      double z = (i == gwIndex) ? m_cfg.gwHeight : 0.0;
      alloc->Add (Vector (currentX, 0.0, z));
      if (i == gwIndex)
        {
          NS_LOG_INFO ("Node " << i << " (GW/Dest) pos=(" << currentX << ", 0, " << m_cfg.gwHeight << ")");
        }
      else
        {
          NS_LOG_INFO ("Node " << i << " (ED) pos=(" << currentX << ", 0, 0)");
        }
      currentX += m_cfg.spacing;
    }

  mobility.SetPositionAllocator (alloc);
  mobility.SetMobilityModel ("ns3::ConstantPositionMobilityModel");
  mobility.Install (nodes);

  NS_LOG_INFO ("TOPOLOGY: Linear with spacing=" << m_cfg.spacing << "m, total length=" << currentX - m_cfg.spacing << "m");
}

void
LoraMeshHelper::InstallDevices (NodeContainer& nodes)
{
  Ptr<LogDistancePropagationLossModel> loss = CreateObject<LogDistancePropagationLossModel> ();
  loss->SetPathLossExponent (2.7);
  loss->SetReference (1, 7.7);

  Ptr<PropagationDelayModel> delay = CreateObject<ConstantSpeedPropagationDelayModel> ();
  Ptr<lorawan::LoraChannel> channel = CreateObject<lorawan::LoraChannel> (loss, delay);

  NS_LOG_UNCOND ("✓ Canal LoRa creado con LogDistancePropagationLossModel (ref=7.7dB@1m, exp=2.7)");

  for (uint32_t i = 0; i < nodes.GetN (); ++i)
    {
      Ptr<Node> node = nodes.Get (i);

      Ptr<lorawan::SimpleGatewayLoraPhy> phy = CreateObject<lorawan::SimpleGatewayLoraPhy> ();
      phy->SetMobility (node->GetObject<MobilityModel> ());
      phy->SetChannel (channel);
      phy->AddFrequency (915000000);
      for (int p = 0; p < 6; ++p)
        {
          phy->AddReceptionPath ();
        }

      Ptr<lorawan::MeshLoraNetDevice> dev = CreateObject<lorawan::MeshLoraNetDevice> ();
      dev->SetNode (node);
      dev->SetAddress (Mac48Address::Allocate ());
      dev->SetMtu (255);
      dev->SetPhy (phy);

      channel->Add (phy);

      node->AddDevice (dev);

      if (m_enablePcap)
        {
          std::ostringstream txName;
          std::ostringstream rxName;
          txName << m_pcapPrefix << i << "_tx.pcap";
          rxName << m_pcapPrefix << i << "_rx.pcap";
          Ptr<PcapFileWrapper> txPcap = CreateObject<PcapFileWrapper> ();
          Ptr<PcapFileWrapper> rxPcap = CreateObject<PcapFileWrapper> ();
          txPcap->Open (txName.str (), std::ios::out | std::ios::binary);
          txPcap->Init (0);
          rxPcap->Open (rxName.str (), std::ios::out | std::ios::binary);
          rxPcap->Init (0);
          dev->SetPcap (txPcap, rxPcap);
          NS_LOG_INFO ("Node " << i << " pcaps habilitados: " << txName.str () << " y " << rxName.str ());
        }

      NS_LOG_INFO ("Node " << i << " device installed (SimpleEndDeviceLoraPhy), freq=915MHz");
    }

  NS_LOG_INFO ("Total PHYs registered: " << channel->GetNDevices ());
}

void
LoraMeshHelper::InstallApplications (NodeContainer& nodes)
{
  const double stopTime = m_cfg.simTimeSec;
  for (uint32_t i = 0; i < nodes.GetN (); ++i)
    {
      Ptr<MeshDvApp> app = CreateObject<MeshDvApp> ();
      app->SetPeriod (Seconds (60.0));
      app->SetInitTtl (10);
      app->SetInitScoreX100 (100);
      app->SetCsmaEnabled (true);

      nodes.Get (i)->AddApplication (app);
      app->SetStartTime (Seconds (1.0 + i * 0.5));
      app->SetStopTime (Seconds (stopTime));

      NS_LOG_INFO (">>> MeshDvApp installed on node " << i);
    }

  NS_LOG_INFO ("MeshDvApp configured on ALL " << nodes.GetN () << " nodes");
}

void
LoraMeshHelper::ForceStandbyMode (NodeContainer& nodes)
{
  NS_LOG_INFO ("Forzando todos los nodos a modo STANDBY para RX...");

  for (uint32_t i = 0; i < nodes.GetN (); ++i)
    {
      Ptr<Node> node = nodes.Get (i);
      Ptr<NetDevice> netDev = node->GetDevice (0);
      Ptr<lorawan::MeshLoraNetDevice> meshDev =
        DynamicCast<lorawan::MeshLoraNetDevice> (netDev);

      if (meshDev)
        {
          Ptr<lorawan::LoraPhy> basePhy = meshDev->GetPhy ();
          Ptr<lorawan::SimpleEndDeviceLoraPhy> phy =
            DynamicCast<lorawan::SimpleEndDeviceLoraPhy> (basePhy);

          if (phy)
            {
              phy->SwitchToStandby ();
              NS_LOG_UNCOND ("✓ Node " << i << " PHY forzado a STANDBY (modo RX)");
            }
        }
    }

  NS_LOG_UNCOND ("=== TODOS LOS NODOS EN STANDBY, LISTOS PARA RX ===");
}

} // namespace loramesh
} // namespace ns3
