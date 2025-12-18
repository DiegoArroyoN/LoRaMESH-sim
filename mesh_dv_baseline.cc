#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/loramesh-helper.h"
#include "mesh_dv_app.h"
#include "metrics_collector.h"

using namespace ns3;

NS_LOG_COMPONENT_DEFINE ("MeshDvBaseline");

int main (int argc, char *argv[])
{
  loramesh::LoraMeshConfig cfg;
  cfg.nEd = 3;
  cfg.simTimeSec = 150.0;
  cfg.enableAdr = true;
  cfg.enableDutyCycle = true;
  cfg.dutyLimit = 0.10;
  cfg.spacing = 30.0;
  cfg.gwHeight = 12.0;
  cfg.enableCsma = true;
  bool enablePcap = true;
  const Time warmupTime = Seconds (60.0);

  CommandLine cmd;
  cmd.AddValue ("nEd", "Número de end-devices", cfg.nEd);
  cmd.AddValue ("spacing", "Separación entre EDs [m]", cfg.spacing);
  cmd.AddValue ("gwHeight", "Altura del gateway [m]", cfg.gwHeight);
  cmd.AddValue ("stopSec", "Tiempo de simulación [s]", cfg.simTimeSec);
  cmd.AddValue ("enablePcap", "Generar archivos pcap por nodo (TX/RX)", enablePcap);
  cmd.AddValue ("enableAdr", "Habilitar ADR hop-by-hop", cfg.enableAdr);
  cmd.AddValue ("enableDuty", "Habilitar duty cycle estricto", cfg.enableDutyCycle);
  cmd.AddValue ("dutyLimit", "Límite de duty cycle (0.01 = 1%)", cfg.dutyLimit);
  cmd.AddValue ("enableCsma", "Habilitar CSMA/CAD en capa MAC", cfg.enableCsma);
  cmd.Parse (argc, argv);

  LogComponentEnable ("MeshDvBaseline", LOG_LEVEL_INFO);
  LogComponentEnable ("MeshDvApp", LOG_LEVEL_INFO);

  NS_LOG_INFO ("=== Mesh DV Baseline - Peer-to-Peer Network ===");
  NS_LOG_INFO ("Total nodes: " << (cfg.nEd + 1) << " (all identical mesh nodes)");
  NS_LOG_INFO ("Duration: " << cfg.simTimeSec << " s");
  NS_LOG_INFO ("Warm-up DV only until " << warmupTime.GetSeconds () << "s, data apps start at " << warmupTime.GetSeconds () << "s");

  // Crear recolector ANTES de la simulación
  g_metricsCollector = new MetricsCollector ();

  NodeContainer nodes;
  nodes.Create (cfg.nEd + 1);

  Ptr<loramesh::LoraMeshHelper> helper = CreateObject<loramesh::LoraMeshHelper> ();
  helper->SetConfig (cfg);
  if (enablePcap)
    {
      helper->EnablePcap ("mesh_dv_node");
    }
  helper->Install (nodes);
  // Ajustar inicio/fin de las apps de datos: DV sigue activo desde t=0
  for (uint32_t i = 0; i < nodes.GetN (); ++i)
    {
      Ptr<Node> n = nodes.Get (i);
      for (uint32_t a = 0; a < n->GetNApplications (); ++a)
        {
          Ptr<MeshDvApp> app = DynamicCast<MeshDvApp> (n->GetApplication (a));
          if (app)
            {
              app->SetStartTime (Seconds (0.0)); // beacons desde t=0
              app->SetStopTime (Seconds (cfg.simTimeSec));
            }
        }
    }

  NS_LOG_INFO ("Starting simulation for " << cfg.simTimeSec << " seconds...");
  Simulator::Stop (Seconds (cfg.simTimeSec));

  // Aumentar potencia de datos a 20 dBm antes de correr la simulación
  bool txPowerApplied = Config::SetDefaultFailSafe ("ns3::LoRaMeshDataPhy::TxPower",
                                                    DoubleValue (20.0));
  if (!txPowerApplied)
    {
      NS_LOG_WARN ("No se pudo aplicar Config::SetDefault para LoRaMeshDataPhy::TxPower; "
                   "se usarán los valores por defecto del dispositivo");
    }

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
