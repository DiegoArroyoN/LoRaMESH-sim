#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/mobility-module.h"
#include "ns3/lorawan-module.h"
#include "ns3/energy-module.h"
#include "ns3/simple-gateway-lora-phy.h"
#include "ns3/pcap-file-wrapper.h"
#include <sstream>

#include "mesh_dv_app.h"
#include "mesh_lora_net_device.h"
#include "metrics_collector.h"

using namespace ns3;
using namespace lorawan;

NS_LOG_COMPONENT_DEFINE ("MeshDvBaseline");

// Variable global para MetricsCollector
namespace ns3 {
MetricsCollector* g_metricsCollector = nullptr;
}

int main (int argc, char *argv[])
{
  uint32_t nEd = 3;
  double spacing = 30;
  double gwHeight = 12;
  double stopSec = 150;
  bool enablePcap = true;

  CommandLine cmd;
  cmd.AddValue ("nEd", "Número de end-devices", nEd);
  cmd.AddValue ("spacing", "Separación entre EDs [m]", spacing);
  cmd.AddValue ("gwHeight", "Altura del gateway [m]", gwHeight);
  cmd.AddValue ("stopSec", "Tiempo de simulación [s]", stopSec);
  cmd.AddValue ("enablePcap", "Generar archivos pcap por nodo (TX/RX)", enablePcap);
  cmd.Parse (argc, argv);

  LogComponentEnable ("MeshDvBaseline", LOG_LEVEL_INFO);
  LogComponentEnable ("MeshDvApp", LOG_LEVEL_INFO);

  NS_LOG_INFO ("=== Mesh DV Baseline - Peer-to-Peer Network ===");
  NS_LOG_INFO ("Total nodes: " << (nEd + 1) << " (all identical mesh nodes)");
  NS_LOG_INFO ("Duration: " << stopSec << " s");

  // Crear recolector ANTES de la simulación
  g_metricsCollector = new MetricsCollector ();

  // ========================================================================
  // CANAL LORA
  // ========================================================================
Ptr<LogDistancePropagationLossModel> loss = CreateObject<LogDistancePropagationLossModel> ();
// Atenuación moderada (exp=2.7, ref=7.7 dB @1m) para que haya multihop pero sin romper conectividad
loss->SetPathLossExponent (2.7);
loss->SetReference (1, 7.7);

Ptr<PropagationDelayModel> delay = CreateObject<ConstantSpeedPropagationDelayModel> ();
Ptr<LoraChannel> channel = CreateObject<LoraChannel> (loss, delay);

NS_LOG_UNCOND("✓ Canal LoRa creado con LogDistancePropagationLossModel (ref=7.7dB@1m, exp=2.7)");


  // ========================================================================
  // CREAR TODOS LOS NODOS DE FORMA IDÉNTICA (SIN DISTINCIÓN ED/GW)
  // ========================================================================
  NodeContainer allNodes;
  allNodes.Create (nEd + 1);
  NS_LOG_INFO ("Created " << allNodes.GetN () << " nodes (all identical mesh nodes)");

  // ========================================================================
  // MOVILIDAD - TOPOLOGÍA LINEAR (spacing variable)
  // ========================================================================
  MobilityHelper mobility;
  Ptr<ListPositionAllocator> alloc = CreateObject<ListPositionAllocator> ();

  double currentX = 0.0;
  for (uint32_t i = 0; i < nEd; ++i)
  {
    alloc->Add (Vector (currentX, 0.0, 0.0));
    NS_LOG_INFO ("Node " << i << " (ED) pos=(" << currentX << ", 0, 0)");
    currentX += spacing;
  }

  alloc->Add (Vector (currentX, 0.0, gwHeight));
  NS_LOG_INFO ("Node " << nEd << " (GW/Dest) pos=(" << currentX << ", 0, " << gwHeight << ")");

  mobility.SetPositionAllocator (alloc);
  mobility.SetMobilityModel ("ns3::ConstantPositionMobilityModel");
  mobility.Install (allNodes);

  NS_LOG_INFO ("TOPOLOGY: Linear with spacing=" << spacing << "m, total length=" << currentX << "m");

  // ========================================================================
  // NET DEVICES - FIX CRÍTICO: Registrar callback Receive
  // ========================================================================
  NetDeviceContainer meshDevices;

  for (uint32_t i = 0; i < allNodes.GetN (); ++i)
  {
    Ptr<Node> node = allNodes.Get (i);
    
    Ptr<SimpleGatewayLoraPhy> phy = CreateObject<SimpleGatewayLoraPhy> ();
    phy->SetMobility (node->GetObject<MobilityModel> ());
    phy->SetChannel (channel);
    // Multirecepción: misma frecuencia y múltiples paths para todos los SF
    phy->AddFrequency (915000000);
    for (int p = 0; p < 6; ++p) { phy->AddReceptionPath (); }
    
    Ptr<MeshLoraNetDevice> dev = CreateObject<MeshLoraNetDevice> ();
    dev->SetNode (node);
    dev->SetAddress (Mac48Address::Allocate ());
    dev->SetMtu (255);
    dev->SetPhy (phy);  // ← Esto llama SetReceiveOkCallback internamente
    
    // ★★★ CRÍTICO: Agregar PHY al canal DESPUÉS de configurar el device
    channel->Add (phy);
    
    node->AddDevice (dev);
    meshDevices.Add (dev);

    if (enablePcap)
    {
      std::ostringstream txName;
      std::ostringstream rxName;
      txName << "mesh_dv_node" << i << "_tx.pcap";
      rxName << "mesh_dv_node" << i << "_rx.pcap";
      Ptr<PcapFileWrapper> txPcap = CreateObject<PcapFileWrapper> ();
      Ptr<PcapFileWrapper> rxPcap = CreateObject<PcapFileWrapper> ();
      txPcap->Open (txName.str (), std::ios::out | std::ios::binary);
      txPcap->Init (0); // DLT_RAW
      rxPcap->Open (rxName.str (), std::ios::out | std::ios::binary);
      rxPcap->Init (0); // DLT_RAW
      dev->SetPcap (txPcap, rxPcap);
      NS_LOG_INFO ("Node " << i << " pcaps habilitados: " << txName.str () << " y " << rxName.str ());
    }
    
    NS_LOG_INFO ("Node " << i << " device installed (SimpleEndDeviceLoraPhy), freq=915MHz");
  }

  NS_LOG_INFO ("Total PHYs registered: " << channel->GetNDevices ());

  // ========================================================================
  // INSTALAR MESHDVAPP EN TODOS LOS NODOS
  // ========================================================================
  for (uint32_t i = 0; i < allNodes.GetN (); ++i)
  {
    Ptr<MeshDvApp> app = CreateObject<MeshDvApp> ();
    app->SetPeriod (Seconds (60.0));
    app->SetInitTtl (10);
    app->SetInitScoreX100 (100);
    app->SetCsmaEnabled (true); 
    
    allNodes.Get (i)->AddApplication (app);
    app->SetStartTime (Seconds (1.0 + i * 0.5));
    app->SetStopTime (Seconds (stopSec));
    
    NS_LOG_INFO (">>> MeshDvApp installed on node " << i);
  }

  NS_LOG_INFO ("MeshDvApp configured on ALL " << allNodes.GetN () << " nodes");

  // ========================================================================
  // EJECUTAR SIMULACIÓN
  // ========================================================================
  NS_LOG_INFO ("Starting simulation for " << stopSec << " seconds...");
  Simulator::Stop (Seconds (stopSec));

  // ========================================================================
  // CRÍTICO: Forzar TODOS los PHYs a modo STANDBY (escucha)
  // ========================================================================
  NS_LOG_INFO("Forzando todos los nodos a modo STANDBY para RX...");

  for (uint32_t i = 0; i < allNodes.GetN(); ++i)
  {
    Ptr<Node> node = allNodes.Get(i);
    Ptr<NetDevice> netDev = node->GetDevice(0);
    Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev = 
        DynamicCast<ns3::lorawan::MeshLoraNetDevice>(netDev);
    
    if (meshDev)
    {
      Ptr<ns3::lorawan::LoraPhy> basePhy = meshDev->GetPhy();
      Ptr<ns3::lorawan::SimpleEndDeviceLoraPhy> phy = 
          DynamicCast<ns3::lorawan::SimpleEndDeviceLoraPhy>(basePhy);
      
      if (phy)
      {
        phy->SwitchToStandby();
        NS_LOG_UNCOND("✓ Node " << i << " PHY forzado a STANDBY (modo RX)");
      }
    }
  }

  NS_LOG_UNCOND("=== TODOS LOS NODOS EN STANDBY, LISTOS PARA RX ===");

  Simulator::Run ();
  NS_LOG_INFO ("=== Simulación completada ===");

  // ========================================================================
  // EXPORTAR MÉTRICAS
  // ========================================================================
  if (g_metricsCollector)
  {
    std::cerr << "\n>>> Exportando métricas...\n";
    g_metricsCollector->PrintStatistics ();
    g_metricsCollector->ExportToCSV ("mesh_dv_metrics");
    std::cerr << ">>> Archivos CSV exportados:\n";
    std::cerr << "    - mesh_dv_metrics_tx.csv\n";
    std::cerr << "    - mesh_dv_metrics_rx.csv\n";
    std::cerr << "    - mesh_dv_metrics_routes.csv\n";
    delete g_metricsCollector;
    g_metricsCollector = nullptr;
  }

  Simulator::Destroy ();
  return 0;
}
