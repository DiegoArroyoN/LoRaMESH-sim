#include "mesh_dv_app.h"
#include "metrics_collector.h"
#include "ns3/log.h"
#include "ns3/node.h"
#include "ns3/simulator.h"
#include <cmath>
#include <algorithm>
#include <sstream>
#include <vector>
#include "ns3/net-device.h"
#include "ns3/packet.h"
#include "ns3/random-variable-stream.h"
#include "ns3/attribute.h"
#include "mesh_lora_net_device.h" 
#include "ns3/end-device-lora-phy.h"
#include "ns3/lora-tag.h"
#include "ns3/mac48-address.h"
#include <cstring> // Para memcpy, memset, etc.
#include <string>




namespace ns3 {

NS_LOG_COMPONENT_DEFINE ("MeshDvApp");
NS_OBJECT_ENSURE_REGISTERED (MeshDvApp);
// Variable global declarada en mesh_dv_baseline.cc
extern MetricsCollector* g_metricsCollector;

// Implementación correcta del destructor:
MeshDvApp::~MeshDvApp() {
  // Limpieza si necesitas; normalmente vacío en NS-3
  NS_LOG_FUNCTION(this);
}

// Implementación del constructor:
MeshDvApp::MeshDvApp() {
  NS_LOG_FUNCTION(this);
  m_mac = CreateObject<loramesh::CsmaCadMac> ();
  m_energyModel = CreateObject<loramesh::EnergyModel> ();
  m_routing = CreateObject<loramesh::RoutingDv> ();
  m_compositeMetric.SetEnergyModel (m_energyModel);
  UpdateRouteTimeout ();
}

TypeId
MeshDvApp::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::MeshDvApp")
    .SetParent<Application> ()
    .SetGroupName ("Applications")
    .AddConstructor<MeshDvApp> ();
  return tid;
}

void
MeshDvApp::UpdateRouteTimeout ()
{
  double seconds = m_period.GetSeconds () * m_routeTimeoutFactor;
  if (seconds <= 0.0)
  {
    seconds = std::max (1.0, m_period.GetSeconds ());
  }
  m_routeTimeout = Seconds (seconds);
  if (m_routing)
  {
    m_routing->SetRouteTimeout (m_routeTimeout);
  }
  NS_LOG_INFO ("Route timeout actualizado a " << m_routeTimeout.GetSeconds ()
               << "s (factor=" << m_routeTimeoutFactor << ")");
}

uint32_t
MeshDvApp::GetBeaconRouteCapacity () const
{
  uint32_t mtu = 255;
  Ptr<Node> node = GetNode ();
  if (node)
  {
    for (uint32_t i = 0; i < node->GetNDevices (); ++i)
    {
      Ptr<NetDevice> dev = node->GetDevice (i);
      if (dev)
      {
        uint16_t devMtu = dev->GetMtu ();
        if (devMtu > 0)
        {
          mtu = std::min (mtu, static_cast<uint32_t>(devMtu));
        }
      }
    }
  }

  const uint32_t entrySize = sizeof (MeshMetricTag::RoutePayloadEntry);
  if (entrySize == 0 || mtu <= 1)
  {
    return 1;
  }
  uint32_t maxRoutes = (mtu - 1) / entrySize;
  return std::max (1u, maxRoutes);
}

void
MeshDvApp::SetPeriod (Time t)
{
  m_period = t;
  UpdateRouteTimeout ();
}

void
MeshDvApp::SetRouteTimeoutFactor (double factor)
{
  m_routeTimeoutFactor = std::max (1.0, factor);
  UpdateRouteTimeout ();
}

// Construye y transmite el beacon DV periódico con las mejores rutas conocidas.
void
MeshDvApp::BuildAndSendDv (uint8_t sf)
{
  MeshMetricTag tag;
  tag.SetSrc (GetNode ()->GetId ());
  tag.SetDst (0xFFFF);
  tag.SetSeq (++m_seq);
  tag.SetTtl (m_initTtl);
  tag.SetHops (0);
  tag.SetSf (sf);
  if (m_routing)
  {
    m_routing->SetSequence (m_seq);
  }

  int16_t realRSSI = GetRealRSSI ();
  uint16_t realBatt = GetBatteryVoltageMv ();
  tag.SetRssiDbm (realRSSI);
  tag.SetBatt_mV (realBatt);

  // ---------------------- NUEVO: Top-N mejor score ------------------------
  const uint32_t maxRoutes = GetBeaconRouteCapacity ();
  NS_LOG_INFO ("Beacon capacity (max routes)=" << maxRoutes);
  if (m_routing)
  {
    m_routing->SetMaxRoutes (maxRoutes);
  }

  std::vector<MeshMetricTag::RoutePayloadEntry> payload;
  if (m_routing)
  {
    auto announcements = m_routing->GetBestRoutes (maxRoutes);
    payload.reserve (announcements.size ());
    for (const auto& ann : announcements)
    {
      MeshMetricTag::RoutePayloadEntry r;
      r.dst = ann.destination;
      r.hops = ann.hops;
      r.sf = ann.sf;
      r.score = ann.scoreX100;
      r.batt_mV = ann.batt_mV;
      r.rssi_dBm = ann.rssiDbm;
      payload.push_back (r);
    }
  }
  
  // ========================================================================
  // FIX CRÍTICO: Crear paquete con al menos 1 byte si payload está vacío
  // ========================================================================
  size_t len = payload.size() * sizeof(MeshMetricTag::RoutePayloadEntry);
  uint32_t payloadSizeBytes = std::max<uint32_t> (len, 13); // mínimo seguro
  Ptr<Packet> p;

  std::vector<uint8_t> buffer (payloadSizeBytes, 0);
  if (len > 0)
  {
    std::memcpy (buffer.data (), payload.data (), std::min<size_t>(len, buffer.size ()));
    NS_LOG_INFO ("Node " << GetNode()->GetId() 
                 << " Beacon con " << payload.size() << " rutas en payload");
  }
  else
  {
    NS_LOG_INFO ("Node " << GetNode()->GetId() 
                 << " Beacon sin rutas, enviando payload dummy de " << payloadSizeBytes << " bytes");
  }
  p = Create<Packet> (buffer.data (), buffer.size ());
  // ------------------- FIN FIX --------------------------------------------

  const uint32_t toaUs = ComputeLoRaToAUs (sf, m_bw, m_cr, payloadSizeBytes);
  tag.SetToaUs (toaUs);

  if (!m_mac || !m_mac->CanTransmitNow (toaUs / 1e6))
  {
    NS_LOG_WARN ("Node " << GetNode ()->GetId () 
                 << " BEACON DROP: Duty cycle limit exceeded");
    return;
  }

  const uint16_t scoreX100 = ComputeScoreX100 (tag);
  tag.SetScoreX100 (scoreX100);

  p->AddPacketTag (tag);

  std::ostringstream oss; tag.Print (oss);
  NS_LOG_INFO ("DV OUT: " << oss.str());
  NS_LOG_UNCOND ("DV_TX node" << GetNode()->GetId ()
                 << " dst=" << tag.GetDst ()
                 << " metric=" << scoreX100
                 << " nextHop=" << -1);

  double energyJ = m_energyModel ? m_energyModel->GetRemainingEnergy (GetNode ()->GetId ()) : -1.0;
  double energyFrac = m_energyModel ? m_energyModel->GetEnergyFraction (GetNode ()->GetId ()) : -1.0;
  LogTxEvent (m_seq, tag.GetDst (), m_initTtl, 0, realRSSI, realBatt, scoreX100, tag.GetSf (), energyJ, energyFrac, true);

  SendWithCSMA (p, tag, Address ());
}


// Inicializa timers, callbacks y generación de tráfico.
void
MeshDvApp::StartApplication ()
{
  uint32_t nodeId = GetNode ()->GetId ();
  
  NS_LOG_INFO ("StartApplication(): node=" << nodeId
               << " period=" << m_period.GetSeconds () << "s"
               << " ttl=" << unsigned(m_initTtl)
               << " csmaEnabled=" << m_csmaEnabled);

  Ptr<Node> n = GetNode ();
  bool callbackRegistered = false;
  
  for (uint32_t i = 0; i < n->GetNDevices (); ++i)
  {
    Ptr<NetDevice> dev = n->GetDevice (i);
    if (!dev)
    {
      NS_LOG_DEBUG ("  Device " << i << " is null, skipping");
      continue;
    }
    
    // Registrar callback en TODOS los dispositivos
    dev->SetReceiveCallback (MakeCallback (&MeshDvApp::L2Receive, this));
    Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev = DynamicCast<ns3::lorawan::MeshLoraNetDevice> (dev);
    if (meshDev)
    {
      if (m_mac)
      {
        meshDev->SetMac (m_mac);
      }
      if (m_energyModel)
      {
        meshDev->SetEnergyModel (m_energyModel);
      }
    }
    callbackRegistered = true;
    
    NS_LOG_INFO ("  >>> Callback L2Receive registered on node " << nodeId 
                 << " device " << i << " (type=" << dev->GetInstanceTypeId ().GetName () << ")");
  }
  
  if (!callbackRegistered)
  {
    NS_LOG_ERROR ("  !!! NO callback registered on node " << nodeId);
  }

  // RNG debe estar disponible antes de cualquier CAD/CSMA.
  m_rng = CreateObject<UniformRandomVariable> ();
  if (!m_mac)
  {
    m_mac = CreateObject<loramesh::CsmaCadMac> ();
    m_compositeMetric.SetEnergyModel (m_energyModel);
  }
  m_mac->SetRandomStream (m_rng);
  m_mac->SetDutyCycleWindow (Hours (1));
  m_mac->SetDutyCycleLimit (0.10);
  m_mac->SetCadDuration (m_cadDuration);
  m_mac->SetDifsCadCount (m_difsCadCount);
  m_mac->SetBackoffWindow (m_backoffWindow);

  if (m_energyModel)
  {
    m_energyModel->RegisterNode (nodeId);
  }

  if (!m_routing)
  {
    m_routing = CreateObject<loramesh::RoutingDv> ();
  }
  m_routing->SetNodeId (nodeId);
  m_routing->SetRouteTimeout (m_routeTimeout);
  m_routing->SetInitTtl (m_initTtl);
  m_routing->SetMaxRoutes (GetBeaconRouteCapacity ());
  m_routing->SetSequence (m_seq);
  m_routing->SetRouteChangeCallback (MakeCallback (&MeshDvApp::HandleRouteChange, this));
  m_routing->SetFloodCallback (MakeCallback (&MeshDvApp::HandleFloodRequest, this));

  m_beaconWarmupEnd = Simulator::Now () + Seconds (60);

  // Enviar un primer beacon inmediato en SF control y programar ciclos multi-SF
  BuildAndSendDv (m_sfControl);
  ScheduleDvCycle (m_sfControl, m_evtSf12);
  ScheduleDvCycle (10, m_evtSf10);
  ScheduleDvCycle (9,  m_evtSf9);

  m_purgeEvt = Simulator::Schedule (Seconds (30), &MeshDvApp::PurgeExpiredRoutes, this);
  if (nodeId < 3)
  {
    m_periodicDumpEvt = Simulator::Schedule (Seconds (30), &MeshDvApp::SchedulePeriodicDump, this);
  }
  
  Simulator::Schedule (Seconds (20), &MeshDvApp::PrintRoutingTable, this);
  Simulator::Schedule (Seconds (40), &MeshDvApp::PrintRoutingTable, this);

  // Programar generación de datos (solo para ED, no para GW)
  if (nodeId < 3)
  {
    NS_LOG_INFO ("  >>> Node " << nodeId << " (ED) - Data generation scheduled at t=60s");
    double jitter = m_rng->GetValue(0.0, 5.0);  // 0-5 segundos de jitter
    m_dataGenerationEvt = Simulator::Schedule (Seconds (60.0 + jitter), 
                                           &MeshDvApp::GenerateDataTraffic, this);
  }
  else
  {
    NS_LOG_INFO ("  >>> Node " << nodeId << " (GW) - No data generation (receiver only)");
  }
}



// Cancela eventos y reporta estadísticas cuando se detiene la app.
void
MeshDvApp::StopApplication ()
{
  NS_LOG_INFO ("StopApplication(): node=" << GetNode ()->GetId ());
  
  if (m_evt.IsPending())
    Simulator::Cancel (m_evt);
  if (m_evtSf9.IsPending())
    Simulator::Cancel (m_evtSf9);
  if (m_evtSf10.IsPending())
    Simulator::Cancel (m_evtSf10);
  if (m_evtSf12.IsPending())
    Simulator::Cancel (m_evtSf12);
  
  if (m_purgeEvt.IsPending())
    Simulator::Cancel (m_purgeEvt);
  
  if (m_backoffEvt.IsPending())
    Simulator::Cancel (m_backoffEvt);
  
  // ========================================================================
  // NUEVO: Reportar estadísticas de datos al finalizar
  // ========================================================================
  if (GetNode ()->GetId () < 3)  // Si es ED
  {
    NS_LOG_INFO ("=== DATA STATISTICS Node " << GetNode ()->GetId () << " ===");
    NS_LOG_INFO ("  Data packets generated: " << m_dataPacketsGenerated);
    NS_LOG_INFO ("  Data packets delivered: " << m_dataPacketsDelivered);
    NS_LOG_INFO ("  Data no-route: " << m_dataNoRoute);
    double pdr = (m_dataPacketsGenerated > 0) 
                 ? (100.0 * m_dataPacketsDelivered / m_dataPacketsGenerated) 
                 : 0.0;
    NS_LOG_INFO ("  PDR: " << pdr << "%");
  }
  else if (GetNode ()->GetId () == 3)  // Si es GW
  {
    NS_LOG_INFO ("=== GATEWAY RECEIVED ===");
    NS_LOG_INFO ("  Total data packets received: " << m_dataPacketsDelivered);
    NS_LOG_INFO ("  Data no-route (local count): " << m_dataNoRoute);
  }

  if (g_metricsCollector)
  {
    double dutyUsed = m_mac ? m_mac->GetDutyCycleUsed () : 0.0;
    g_metricsCollector->RecordDuty (GetNode ()->GetId (), dutyUsed, m_txCount, m_backoffCount);
    double energyJ = m_energyModel ? m_energyModel->GetRemainingEnergy (GetNode ()->GetId ()) : -1.0;
    double energyFrac = m_energyModel ? m_energyModel->GetEnergyFraction (GetNode ()->GetId ()) : -1.0;
    g_metricsCollector->RecordEnergySnapshot (GetNode ()->GetId (), energyJ, energyFrac);
  }
  
  PrintRoutingTable ();
  if (m_routing)
  {
    m_routing->DebugDumpRoutingTable ();
  }
}


void
MeshDvApp::Tick ()
{
  NS_LOG_INFO ("Tick(): node=" << GetNode ()->GetId ()
               << " t=" << Simulator::Now ().GetSeconds () << "s");

  BuildAndSendDv(m_sfControl);
  m_evt = Simulator::Schedule (m_period, &MeshDvApp::Tick, this);
}


// Procesa cada recepción L2 y decide si actualizar rutas o reenviar.
bool
MeshDvApp::L2Receive (Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address &from)
{
  NS_LOG_UNCOND("L2Receive ACTIVADA node=" << GetNode()->GetId()
              << " proto=" << proto << " kProtoMesh=" << kProtoMesh);
  if (proto != kProtoMesh)
    return false;

  MeshMetricTag tag;
  bool has = p->PeekPacketTag (tag);
  NS_LOG_UNCOND("PeekPacketTag: node=" << GetNode()->GetId() << " has=" << has << " dst=" << tag.GetDst() << " src=" << tag.GetSrc());

  if (!has)
  {
    NS_LOG_INFO ("RX (node=" << GetNode()->GetId() << "): packet sin MeshMetricTag");
    return true;
  }

  // Trazas detalladas de recepción para correlacionar rutas planeadas vs recorridas.
  NS_LOG_UNCOND ("FWDTRACE rx time=" << Simulator::Now ().GetSeconds ()
                 << " node=" << GetNode ()->GetId ()
                 << " src=" << tag.GetSrc ()
                 << " dst=" << tag.GetDst ()
                 << " seq=" << tag.GetSeq ()
                 << " ttl=" << unsigned (tag.GetTtl ())
                 << " hopsSeen=" << unsigned (tag.GetHops ())
                 << " sf=" << unsigned (tag.GetSf ())
                 << " rssi=" << tag.GetRssiDbm ());

  // Aprender MAC del emisor para mapear ID lógico -> MAC (necesario para next-hop)
  Mac48Address fromMac = Mac48Address::ConvertFrom (from);
  m_macTable[tag.GetSrc()] = fromMac;

  uint32_t myId = GetNode ()->GetId ();
  uint32_t dst = tag.GetDst ();
  double energyJ = m_energyModel ? m_energyModel->GetRemainingEnergy (myId) : -1.0;
  double energyFrac = m_energyModel ? m_energyModel->GetEnergyFraction (myId) : -1.0;
  bool isData = (dst != 0xFFFF);
  // Filtro único de duplicados/loops para datos
  if (isData)
    {
      CleanOldSeenData ();
      auto key = std::make_tuple (tag.GetSrc (), tag.GetDst (), tag.GetSeq ());
      if (m_seenData.find (key) != m_seenData.end ())
        {
          NS_LOG_UNCOND ("FWDTRACE drop_seen_once time=" << Simulator::Now ().GetSeconds ()
                         << " node=" << myId
                         << " src=" << tag.GetSrc ()
                         << " dst=" << dst
                         << " seq=" << tag.GetSeq ()
                         << " reason=seen_data");
          return true;
        }
      m_seenData[key] = Simulator::Now ();
    }
  if (isData)
    {
      // Solo datos usan seenData; DV no pasa por aquí.
    }

  // ========================================================================
  // CASO 1: Paquete llegó a su destino final (YO soy el destino)
  // ========================================================================
  if (myId == dst)
  {
    // Entrega en sink con control de duplicados
    auto deliveredKey = std::make_tuple (tag.GetSrc (), tag.GetDst (), tag.GetSeq ());
    if (m_deliveredSet.find (deliveredKey) != m_deliveredSet.end ())
      {
        NS_LOG_UNCOND ("FWDTRACE drop_dup_sink_delivered time=" << Simulator::Now ().GetSeconds ()
                       << " node=" << myId
                       << " src=" << tag.GetSrc ()
                       << " dst=" << tag.GetDst ()
                       << " seq=" << tag.GetSeq ());
        return true;
      }
    m_deliveredSet.insert (deliveredKey);
    if (myId == 3)  // Si soy el GW
    {
      m_dataPacketsDelivered++;
      NS_LOG_INFO (">>> DATA DELIVERED to GW: src=" << tag.GetSrc () 
                   << " seq=" << tag.GetSeq () 
                   << " hops=" << (int)tag.GetHops ()
                   << " RSSI=" << tag.GetRssiDbm () << "dBm");

      // Registrar recepción final (no forward).
      LogRxEvent (tag.GetSrc (), tag.GetDst (), tag.GetSeq (), tag.GetTtl (),
                  tag.GetHops (), tag.GetRssiDbm (), tag.GetBatt_mV (),
                  tag.GetScoreX100 (), tag.GetSf (), energyJ, energyFrac, false);

      if (g_metricsCollector)
      {
        double txTime = g_metricsCollector->GetFirstTxTime (tag.GetSrc (), tag.GetDst (), tag.GetSeq ());
        double delaySec = (txTime >= 0.0) ? (Simulator::Now ().GetSeconds () - txTime) : -1.0;
        g_metricsCollector->RecordE2eDelay (tag.GetSrc (), tag.GetDst (), tag.GetSeq (),
                                            tag.GetHops (), delaySec, p->GetSize (), tag.GetSf (), true);
        g_metricsCollector->RecordEnergySnapshot (myId, energyJ, energyFrac);
      }
      NS_LOG_UNCOND ("FWDTRACE deliver time=" << Simulator::Now ().GetSeconds ()
                     << " node=" << myId
                     << " src=" << tag.GetSrc ()
                     << " dst=" << tag.GetDst ()
                     << " seq=" << tag.GetSeq ()
                     << " hops=" << unsigned (tag.GetHops ())
                     << " reason=dst_local");
    }
    return true;  // NO forward si ya llegó a destino
  }

  // ========================================================================
  // CASO 2: Beacon DV (broadcast) - Actualizar tabla de rutas
  // ========================================================================
  if (dst == 0xFFFF)
  {
    NS_LOG_UNCOND ("DV_RX node" << myId
                   << " from=" << tag.GetSrc ()
                   << " dst=" << tag.GetDst ()
                   << " metric=" << tag.GetScoreX100 ()
                   << " hops=" << unsigned (tag.GetHops ()));
    // 1. Aprende ruta directa hacia el vecino que transmitió el beacon
    NS_LOG_INFO("Intento aprender ruta directa: src=" << tag.GetSrc() << " en node=" << GetNode()->GetId());
    int16_t neighborRssi = tag.GetRssiDbm();
    // Usar el SF con el que se recibió el beacon para reflejar el enlace real.
    uint8_t sfForNeighbor = tag.GetSf ();
    uint32_t toaUsNeighbor = ComputeLoRaToAUs (sfForNeighbor, m_bw, m_cr, p->GetSize ());
    NS_LOG_INFO ("  → RSSI=" << neighborRssi << "dBm SF=" << unsigned(sfForNeighbor)
                 << " toa=" << toaUsNeighbor << "us");
    ProcessDvPayload (p, tag, fromMac, toaUsNeighbor);
    uint32_t currentRoutes = m_routing ? m_routing->GetRouteCount () : 0;
    NS_LOG_INFO("Tabla rutas tras aprender: size=" << currentRoutes);

    // Tu forward de beacons sigue igual...
    uint8_t ttl = tag.GetTtl ();
    if (ttl == 0)
        return true;
    ttl -= 1;
    uint8_t hops = tag.GetHops () + 1;
    MeshMetricTag newTag = tag;
    newTag.SetTtl(ttl);
    newTag.SetHops(hops);
    newTag.SetSf(m_sf);
    newTag.SetRssiDbm(GetRealRSSI());
    newTag.SetBatt_mV(GetBatteryVoltageMv());
    newTag.SetToaUs (ComputeLoRaToAUs (newTag.GetSf (), m_bw, m_cr, p->GetSize ()));
    newTag.SetScoreX100(ComputeScoreX100(newTag));
    if (ttl > 0)
        Simulator::Schedule(MilliSeconds(5 + (myId % 5)),
                            &MeshDvApp::ForwardWithTtl, this, p, newTag);
    return true;
  }

  // ========================================================================
  // CASO 3: Datos unicast - Verificar si debemos forward
  // ========================================================================
  const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute (dst) : nullptr;
  uint8_t ttl = tag.GetTtl ();
  uint8_t hops = tag.GetHops () + 1;

  bool canForward = (route != nullptr) && (ttl > 0);

  // Registrar RX aun si no se reenvía (para métricas de PDR).
  LogRxEvent (tag.GetSrc (), tag.GetDst (), tag.GetSeq (), tag.GetTtl (),
              tag.GetHops (), tag.GetRssiDbm (), tag.GetBatt_mV (),
              tag.GetScoreX100 (), tag.GetSf (), energyJ, energyFrac, canForward);

  if (!route)
  {
    NS_LOG_WARN ("Node " << myId << " RX DROP: No route to dst=" << dst);
    NS_LOG_UNCOND ("FWDTRACE DATA_NOROUTE time=" << Simulator::Now ().GetSeconds ()
                   << " node=" << myId
                   << " src=" << tag.GetSrc ()
                   << " dst=" << dst
                   << " seq=" << tag.GetSeq ()
                   << " reason=no_route_rx");
    DumpFullTable ("DATA_NOROUTE_RX");
    return true;  // Drop
  }

  // ========================================================================
  // FILTRO CRÍTICO: Solo forward si SOY uno de los posibles next-hops
  // En broadcast, todos reciben, pero solo nodos con ruta válida forwardean
  // ========================================================================
  if (ttl == 0)
  {
    NS_LOG_WARN ("Node " << myId << " RX DROP: TTL=0");
    NS_LOG_UNCOND ("FWDTRACE drop_ttl time=" << Simulator::Now ().GetSeconds ()
                   << " node=" << myId
                   << " src=" << tag.GetSrc ()
                   << " dst=" << dst
                   << " seq=" << tag.GetSeq ()
                   << " reason=ttl_expired");
    return true;
  }

  ttl -= 1;

  MeshMetricTag newTag = tag;
  newTag.SetTtl (ttl);
  newTag.SetHops (hops);
  uint8_t selectedSf = route->sf ? route->sf : SelectSfFromAdr (dst, tag.GetRssiDbm ());
  newTag.SetSf (selectedSf);
  newTag.SetToaUs (ComputeLoRaToAUs (selectedSf, m_bw, m_cr, p->GetSize ()));
  newTag.SetRssiDbm (GetRealRSSI ());
  newTag.SetBatt_mV (GetBatteryVoltageMv ());
  newTag.SetScoreX100 (ComputeScoreX100 (newTag));

  NS_LOG_INFO ("RX DATA: src=" << tag.GetSrc () << " dst=" << dst 
               << " seq=" << tag.GetSeq () << " hops=" << (int)tag.GetHops ());
  NS_LOG_INFO ("FWD DATA: src=" << newTag.GetSrc () << " dst=" << dst 
               << " seq=" << newTag.GetSeq () << " hops=" << (int)hops 
               << " nextHop=" << route->nextHop);
  NS_LOG_UNCOND ("FWDTRACE plan time=" << Simulator::Now ().GetSeconds ()
                 << " node=" << myId
                 << " src=" << newTag.GetSrc ()
                 << " dst=" << dst
                 << " seq=" << newTag.GetSeq ()
                 << " ttlAfter=" << unsigned (ttl)
                 << " hopsPlanned=" << unsigned (route->hops)
                 << " nextHop=" << route->nextHop
                 << " reason=route_found");

  LogRxEvent (tag.GetSrc (), tag.GetDst (), tag.GetSeq (), tag.GetTtl (),
              tag.GetHops (), tag.GetRssiDbm (), tag.GetBatt_mV (),
              tag.GetScoreX100 (), tag.GetSf (), energyJ, energyFrac, true);

  // Forward datos con delay para evitar colisiones
  Simulator::Schedule (MilliSeconds (10 + (myId % 5)),
                       &MeshDvApp::ForwardWithTtl, this, p, newTag);

  return true;
}


// Realiza el forwarding cuando el TTL lo permite.
void
MeshDvApp::ForwardWithTtl (Ptr<const Packet> pIn, const MeshMetricTag& inTag)
{
  uint32_t myId = GetNode ()->GetId ();
  uint32_t dst = inTag.GetDst ();
  double energyJ = m_energyModel ? m_energyModel->GetRemainingEnergy (myId) : -1.0;
  double energyFrac = m_energyModel ? m_energyModel->GetEnergyFraction (myId) : -1.0;

  // ========================================================================
  // CASO 1: Si YO soy el destino final, contabilizar entrega y NO forward
  // ========================================================================
  if (myId == dst)
  {
    if (myId == 3)  // Si soy el GW
    {
      m_dataPacketsDelivered++;
      NS_LOG_INFO (">>> DATA DELIVERED to GW: src=" << inTag.GetSrc () 
                   << " seq=" << inTag.GetSeq () 
                   << " hops=" << (int)inTag.GetHops ()
                   << " RSSI=" << inTag.GetRssiDbm () << "dBm");

      // Registrar entrega final (no forward).
      LogRxEvent (inTag.GetSrc (), inTag.GetDst (), inTag.GetSeq (), inTag.GetTtl (),
                  inTag.GetHops (), inTag.GetRssiDbm (), inTag.GetBatt_mV (),
                  inTag.GetScoreX100 (), inTag.GetSf (), energyJ, energyFrac, false);

      if (g_metricsCollector)
      {
        double txTime = g_metricsCollector->GetFirstTxTime (inTag.GetSrc (), inTag.GetDst (), inTag.GetSeq ());
        double delaySec = (txTime >= 0.0) ? (Simulator::Now ().GetSeconds () - txTime) : -1.0;
        g_metricsCollector->RecordE2eDelay (inTag.GetSrc (), inTag.GetDst (), inTag.GetSeq (),
                                            inTag.GetHops (), delaySec, pIn->GetSize (), inTag.GetSf (), true);
        g_metricsCollector->RecordEnergySnapshot (myId, energyJ, energyFrac);
      }
      NS_LOG_UNCOND ("FWDTRACE deliver time=" << Simulator::Now ().GetSeconds ()
                     << " node=" << myId
                     << " src=" << inTag.GetSrc ()
                     << " dst=" << inTag.GetDst ()
                     << " seq=" << inTag.GetSeq ()
                     << " hops=" << unsigned (inTag.GetHops ())
                     << " reason=dst_local");
    }
    return;  // NO forward
  }

  // ========================================================================
  // CASO ESPECIAL: Broadcast (DV) -> inundación sin consulta de rutas
  // ========================================================================
  if (dst == 0xFFFF)
  {
    if (inTag.GetTtl () == 0)
    {
      return;
    }

    uint32_t toaUs = ComputeLoRaToAUs (inTag.GetSf (), m_bw, m_cr, pIn->GetSize ());
    if (!m_mac || !m_mac->CanTransmitNow (toaUs / 1e6))
    {
      NS_LOG_WARN ("Node " << myId << " BEACON DROP: Duty cycle exceeded");
      return;
    }

    Ptr<Packet> pkt = pIn->Copy ();
    MeshMetricTag fwdTag = inTag;
    fwdTag.SetToaUs (toaUs);
    pkt->RemoveAllPacketTags ();
    pkt->AddPacketTag (fwdTag);

    NS_LOG_INFO ("Node " << myId << " FWD DV broadcast ttl=" << unsigned (fwdTag.GetTtl ())
                 << " sf=" << unsigned (fwdTag.GetSf ()) << " toaUs=" << toaUs);
    SendWithCSMA (pkt, fwdTag, Address ());
    return;
  }

  // ========================================================================
  // CASO 2: Verificar si tenemos ruta al destino
  // Flujo esperado: GetRoute() -> usar route->nextHop tal cual (se resuelve a MAC abajo)
  // ========================================================================
  const loramesh::RouteEntry* routePreview = m_routing ? m_routing->GetRoute (dst) : nullptr;
  NS_LOG_UNCOND ("FWD_CHECK node" << myId
                 << " dst=" << dst
                 << " route=" << (routePreview ? ("OK nextHop=" + std::to_string (routePreview->nextHop)) : "NULL"));
  const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute (dst) : nullptr;
  if (!route)
  {
    NS_LOG_WARN ("Node " << myId << " FWD DROP: No route to dst=" << dst);
    NS_LOG_UNCOND ("FWDTRACE drop_noroute time=" << Simulator::Now ().GetSeconds ()
                   << " node=" << myId
                   << " src=" << inTag.GetSrc ()
                   << " dst=" << dst
                   << " seq=" << inTag.GetSeq ()
                   << " reason=no_route");
    DumpFullTable ("DATA_NOROUTE_TX");
    return;
  }
  // Log detallado de la ruta usada
  NS_LOG_UNCOND ("FWDTRACE route time=" << Simulator::Now ().GetSeconds ()
                 << " node=" << myId
                 << " dst=" << dst
                 << " route_dst=" << route->destination
                 << " nextHop=" << route->nextHop
                 << " hops=" << unsigned(route->hops)
                 << " score=" << route->scoreX100
                 << " seqNum=" << route->seqNum
                 << " sf=" << unsigned(route->sf));

  // ========================================================================
  // CASO 3: Verificar duty cycle para forward
  // ========================================================================
  uint8_t sfForRoute = route->sf ? route->sf : SelectSfFromAdr (dst, inTag.GetRssiDbm ());
  uint32_t toaUs = ComputeLoRaToAUs (sfForRoute, m_bw, m_cr, pIn->GetSize ());
  if (!m_mac || !m_mac->CanTransmitNow (toaUs / 1e6))
  {
    NS_LOG_WARN ("Node " << myId << " FWD DROP: Duty cycle exceeded");
    NS_LOG_UNCOND ("FWDTRACE drop_duty time=" << Simulator::Now ().GetSeconds ()
                   << " node=" << myId
                   << " src=" << inTag.GetSrc ()
                   << " dst=" << dst
                   << " seq=" << inTag.GetSeq ()
                   << " dutyUsed=" << (m_mac ? m_mac->GetDutyCycleUsed () : -1.0)
                   << " dutyLimit=" << (m_mac ? m_mac->GetDutyCycleLimit () : -1.0)
                   << " reason=duty_block");
    return;
  }

  // ========================================================================
  // CASO 4: Preparar paquete para forward
  // NO se modifica nextHop después de resolver la ruta; solo se resuelve a dirección MAC.
  // ========================================================================
  std::ostringstream oss;
  oss << "TX FWD: src=" << inTag.GetSrc()
      << " dst=" << dst
      << " seq=" << inTag.GetSeq()
      << " hops=" << (int)inTag.GetHops()
      << " nextHop=" << route->nextHop;
  NS_LOG_INFO(oss.str());
  Address dstAddr = (route->nextHopMac != Mac48Address())
                        ? Address (route->nextHopMac)
                        : ResolveNextHopAddress (route->nextHop);
  const bool broadcastFallback = (route->nextHopMac == Mac48Address ());
  if (broadcastFallback || route->nextHop == 0)
    {
      NS_LOG_UNCOND ("FWDTRACE DATA_NOROUTE time=" << Simulator::Now ().GetSeconds ()
                     << " node=" << myId
                     << " src=" << inTag.GetSrc ()
                     << " dst=" << dst
                     << " seq=" << inTag.GetSeq ()
                     << " nextHop=" << route->nextHop
                     << " reason=no_mac_for_unicast");
      DumpFullTable ("DATA_NOROUTE_TX");
      return;
    }

  NS_LOG_UNCOND ("FWDTRACE fwd time=" << Simulator::Now ().GetSeconds ()
                 << " node=" << myId
                 << " src=" << inTag.GetSrc ()
                 << " dst=" << dst
                 << " seq=" << inTag.GetSeq ()
                 << " ttl=" << unsigned (inTag.GetTtl ())
                 << " ttlAfter=" << unsigned (inTag.GetTtl ())
                 << " nextHop=" << route->nextHop
                 << " hopsPlanned=" << unsigned (route->hops)
                 << " tx_mode=unicast"
                 << " reason=ok");

  // Snapshot de ruta usada en el momento del forward
  if (g_metricsCollector)
  {
    g_metricsCollector->RecordRouteUsed (myId, dst, route->nextHop,
                                         route->hops, route->scoreX100, route->seqNum);
  }

  Ptr<Packet> modifiablePkt = pIn->Copy();
  MeshMetricTag outTag = inTag;
  outTag.SetSf (sfForRoute);
  outTag.SetToaUs (toaUs);
  outTag.SetRssiDbm (GetRealRSSI ());
  outTag.SetBatt_mV (GetBatteryVoltageMv ());
  outTag.SetScoreX100 (ComputeScoreX100 (outTag));

  // ==== CORRECCIÓN RSSI POR HOP ====
  ns3::lorawan::LoraTag loraTag;
  if (modifiablePkt->PeekPacketTag(loraTag)) {
      modifiablePkt->RemovePacketTag(loraTag);
  }
  ns3::lorawan::LoraTag emptyLoraTag;
  modifiablePkt->AddPacketTag(emptyLoraTag);
// ==== FIN CORRECCIÓN ====


  // Quitar y agregar tags de métrica
  modifiablePkt->RemoveAllPacketTags();
  modifiablePkt->AddPacketTag(outTag);

  NS_LOG_UNCOND("Enviando con kProtoMesh=" << kProtoMesh);
  SendWithCSMA(modifiablePkt, outTag, dstAddr);
}


uint32_t
MeshDvApp::ComputeLoRaToAUs (uint8_t sf, uint32_t bw, uint8_t cr, uint32_t pl) const
{
  const double bwHz = static_cast<double>(bw);
  const double tSym = std::pow (2.0, sf) / bwHz;

  const bool ih = m_ih;
  const bool de = m_de;
  const bool crc = m_crc;

  const int8_t  sf_i  = static_cast<int8_t>(sf);
  const double num   = (8.0*static_cast<double>(pl) - 4.0*sf_i + 28.0 + (crc?16.0:0.0) - (ih?20.0:0.0));
  const double den   = 4.0 * (sf_i - (de?2.0:0.0));
  const double ce    = std::ceil (std::max (num / den, 0.0));
  const double paySym= 8.0 + ce * (cr + 4.0);

  const double tPreamble = (8.0 + 4.25) * tSym;
  const double tPayload  = paySym * tSym;
  const double tTot = tPreamble + tPayload;
  return static_cast<uint32_t>(tTot * 1e6 + 0.5);
}

uint16_t
MeshDvApp::ComputeScoreX100 (const MeshMetricTag& t) const
{
  NodeId srcId = 0;
  Ptr<Node> node = GetNode ();
  if (node)
  {
    srcId = node->GetId ();
  }

  loramesh::LinkStats stats;
  stats.toaUs = t.GetToaUs ();
  stats.hops = t.GetHops ();
  stats.rssiDbm = t.GetRssiDbm ();
  stats.batteryMv = t.GetBatt_mV ();
  stats.energyFraction = m_energyModel ? m_energyModel->GetEnergyFraction (srcId) : -1.0;

  const double cost = m_compositeMetric.ComputeLinkCost (srcId, t.GetDst (), stats);
  const double score = std::clamp (1.0 - cost, 0.0, 1.0);
  return static_cast<uint16_t> (std::round (score * 100.0));
}

void
MeshDvApp::PrintRoutingTable ()
{
  double remainingEnergy = GetRemainingEnergyJ ();
  double voltageAvg = (loramesh::EnergyModel::kDefaultVoltageMaxMv
                      + loramesh::EnergyModel::kDefaultVoltageMinMv) / 2000.0;
  double totalEnergy = (loramesh::EnergyModel::kDefaultCapacityMah / 1000.0) * voltageAvg * 3600.0;
  double energyConsumed = std::max (0.0, totalEnergy - remainingEnergy);
  uint16_t batteryMv = GetBatteryVoltageMv ();
  uint32_t entries = m_routing ? static_cast<uint32_t> (m_routing->GetRouteCount ()) : 0;
  
  NS_LOG_INFO ("=== ROUTING TABLE Node " << GetNode ()->GetId () 
               << " (entries=" << entries << ")"
               << " Energy=" << energyConsumed << "J"
               << " Battery=" << batteryMv << "mV ===");
  
  if (!m_routing)
  {
    NS_LOG_INFO ("  (routing module not initialized)");
    return;
  }

  if (entries == 0)
  {
    NS_LOG_INFO ("  (empty)");
    return;
  }

  m_routing->PrintRoutingTable ();
}

void
MeshDvApp::PurgeExpiredRoutes ()
{
  if (m_routing)
  {
    m_routing->PurgeExpiredRoutes ();
  }
  m_purgeEvt = Simulator::Schedule (Seconds (30), &MeshDvApp::PurgeExpiredRoutes, this);
}

void
MeshDvApp::SendDataToDestination (uint32_t dst, Ptr<Packet> payload)
{
  const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute (dst) : nullptr;
  if (!route || route->nextHop == 0)
  {
    uint32_t nextSeq = m_dataSeq + 1;
    NS_LOG_WARN ("No route to dst=" << dst);
    NS_LOG_UNCOND ("FWDTRACE DATA_NOROUTE time=" << Simulator::Now ().GetSeconds ()
                   << " node=" << GetNode()->GetId ()
                   << " src=" << GetNode()->GetId ()
                   << " dst=" << dst
                   << " seq=" << nextSeq
                   << " reason=no_route");
    DumpFullTable ("DATA_NOROUTE_SRC");
    DumpRoute (dst, "DATA_NOROUTE");
    m_dataNoRoute++;
    return;
  }
  
  MeshMetricTag dataTag;
  dataTag.SetSrc (GetNode ()->GetId ());
  dataTag.SetDst (dst);
  dataTag.SetSeq (++m_dataSeq);
  dataTag.SetTtl (m_initTtl);
  dataTag.SetHops (0);
  uint8_t legacySf = route->sf ? route->sf : m_sf;
  dataTag.SetSf (legacySf);
  dataTag.SetToaUs (ComputeLoRaToAUs (legacySf, m_bw, m_cr, payload->GetSize ()));
  dataTag.SetRssiDbm (-90);
  dataTag.SetBatt_mV (3300);
  dataTag.SetScoreX100 (ComputeScoreX100 (dataTag));
  
  payload->AddPacketTag (dataTag);

  // Verificar MAC disponible para el nextHop antes de resolver dirección
  auto macIt = m_macTable.find (route->nextHop);
  NS_LOG_UNCOND ("MAC_CHECK node" << GetNode()->GetId ()
                 << " nextHop=" << route->nextHop
                 << " macFound=" << (macIt != m_macTable.end () ? "YES" : "NO"));
  if (macIt == m_macTable.end ())
    {
      NS_LOG_UNCOND ("NO_MAC node" << GetNode()->GetId ()
                     << " for dst=" << dst
                     << " nextHop=" << route->nextHop);
      NS_LOG_UNCOND ("MAC_TABLE node" << GetNode()->GetId ()
                     << " size=" << m_macTable.size ());
      for (const auto& kv : m_macTable)
        {
          NS_LOG_UNCOND ("  entry nextHopId=" << kv.first << " mac=" << kv.second);
        }
    }
  
  Ptr<Node> n = GetNode ();
  Ptr<NetDevice> dev = nullptr;
  for (uint32_t i = 0; i < n->GetNDevices (); ++i)
  {
    Ptr<NetDevice> d = n->GetDevice (i);
    if (d) { dev = d; break; }
  }
  if (!dev) return;
  
  Address dstAddr = ResolveNextHopAddress (route->nextHop);
  Ptr<NetDevice> dev0 = (n->GetNDevices() > 0) ? n->GetDevice(0) : nullptr;
  bool isBroadcast = (!dstAddr.IsInvalid ()) && dev0 && dstAddr == dev0->GetBroadcast ();
  if (isBroadcast || route->nextHop == 0)
    {
      NS_LOG_UNCOND ("FWDTRACE DATA_NOROUTE time=" << Simulator::Now ().GetSeconds ()
                     << " node=" << GetNode()->GetId ()
                     << " src=" << dataTag.GetSrc ()
                     << " dst=" << dst
                     << " seq=" << dataTag.GetSeq ()
                     << " nextHop=" << route->nextHop
                     << " reason=no_mac_for_unicast");
      DumpFullTable ("DATA_NOROUTE_SRC");
      DumpRoute (dst, "DATA_NOROUTE");
      m_dataNoRoute++;
      return;
    }

  const bool ok = dev->Send (payload, dstAddr, kProtoMesh);
  
  NS_LOG_INFO ("DATA TX: dst=" << dst << " via=" << route->nextHop 
               << " seq=" << m_dataSeq << " ok=" << ok);
}

Address
MeshDvApp::ResolveNextHopAddress (uint32_t nextHopId) const
{
  auto it = m_macTable.find (nextHopId);
  if (it != m_macTable.end ())
  {
    return it->second;
  }
  Ptr<Node> n = GetNode ();
  for (uint32_t i = 0; i < n->GetNDevices (); ++i)
  {
    Ptr<NetDevice> d = n->GetDevice (i);
    if (d)
    {
      return d->GetBroadcast ();
    }
  }
  return Address ();
}

void
MeshDvApp::ScheduleDvCycle (uint8_t sf, EventId& evtSlot)
{
  Time interval = GetBeaconInterval ();
  if (interval.IsZero ())
  {
    return;
  }
  evtSlot = Simulator::Schedule (interval, [this, sf, &evtSlot]() mutable {
    Time currentInterval = GetBeaconInterval ();
    NS_LOG_UNCOND ("BEACON_PHASE node" << GetNode ()->GetId ()
                   << " sf=" << unsigned (sf)
                   << " phase=" << GetBeaconPhaseLabel ()
                   << " interval=" << currentInterval.GetSeconds () << "s");
    if (m_routing)
    {
      m_routing->DebugDumpRoutingTable ();
    }
    BuildAndSendDv (sf);
    ScheduleDvCycle (sf, evtSlot);
  });
}

Time
MeshDvApp::GetBeaconInterval () const
{
  Time now = Simulator::Now ();
  return (now < m_beaconWarmupEnd) ? m_beaconIntervalWarm : m_beaconIntervalStable;
}

void
MeshDvApp::SchedulePeriodicDump ()
{
  uint32_t nodeId = GetNode ()->GetId ();
  if (nodeId < 3 && m_routing)
    {
      NS_LOG_UNCOND ("DV_DUMP_PERIODIC node" << nodeId
                     << " t=" << Simulator::Now ().GetSeconds ());
      DumpFullTable ("PERIODIC_30s");
    }
  m_periodicDumpEvt = Simulator::Schedule (Seconds (30), &MeshDvApp::SchedulePeriodicDump, this);
}

void
MeshDvApp::DumpRoute (uint32_t dst, const std::string& tag)
{
  if (!m_routing)
    {
      return;
    }
  const loramesh::RouteEntry* r = m_routing->GetRoute (dst);
  if (r)
    {
      NS_LOG_UNCOND ("DV_SNAPSHOT tag=" << tag
                     << " node" << GetNode()->GetId ()
                     << " t=" << Simulator::Now ().GetSeconds ()
                     << " dst=" << dst
                     << " nextHop=" << r->nextHop
                     << " hops=" << unsigned (r->hops)
                     << " score=" << r->scoreX100
                     << " seq=" << r->seqNum);
    }
  else
    {
      NS_LOG_UNCOND ("DV_SNAPSHOT tag=" << tag
                     << " node" << GetNode()->GetId ()
                     << " t=" << Simulator::Now ().GetSeconds ()
                     << " dst=" << dst
                     << " route=NULL");
    }
}

void
MeshDvApp::DumpFullTable (const std::string& tag) const
{
  if (!m_routing)
    {
      return;
    }
  uint32_t nodeId = GetNode () ? GetNode ()->GetId () : 0;
  NS_LOG_UNCOND ("DV_TABLE_FULL tag=" << tag
                 << " node" << nodeId
                 << " t=" << Simulator::Now ().GetSeconds ());
  m_routing->DebugDumpRoutingTable ();

  const loramesh::RouteEntry* gw = m_routing->GetRoute (3);
  if (gw)
    {
      NS_LOG_UNCOND ("DV_TABLE_FULL_GW node" << nodeId
                     << " dst=3 nextHop=" << gw->nextHop
                     << " hops=" << unsigned (gw->hops)
                     << " score=" << gw->scoreX100
                     << " seq=" << gw->seqNum
                     << " age=" << (Simulator::Now () - gw->lastUpdate).GetSeconds () << "s");
    }
  else
    {
      NS_LOG_UNCOND ("DV_TABLE_FULL_GW node" << nodeId << " dst=3 route=NULL");
    }
}

std::string
MeshDvApp::GetBeaconPhaseLabel () const
{
  return (Simulator::Now () < m_beaconWarmupEnd) ? "warmup(2s)" : "stable(30s)";
}

// Encola o envía directamente según si CSMA está habilitado.
void
MeshDvApp::SendWithCSMA (Ptr<Packet> packet, const MeshMetricTag& tag, Address dstAddr)
{
  if (!m_csmaEnabled)
  {
    Ptr<Node> n = GetNode ();
    Ptr<NetDevice> dev = nullptr;
    for (uint32_t i = 0; i < n->GetNDevices (); ++i)
    {
      Ptr<NetDevice> d = n->GetDevice (i);
      if (d) { dev = d; break; }
    }
    if (!dev) return;
    
    Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev = DynamicCast<ns3::lorawan::MeshLoraNetDevice> (dev);
    if (meshDev)
    {
      if (m_mac)
      {
        meshDev->SetMac (m_mac);
      }
      if (m_energyModel)
      {
        meshDev->SetEnergyModel (m_energyModel);
      }
    }
    Address dst = (dstAddr == Address()) ? dev->GetBroadcast () : dstAddr;
    bool ok = dev->Send (packet, dst, kProtoMesh);
    NS_LOG_INFO ("CSMA disabled: direct TX result=" << ok);
    if (ok)
    {
      OnPacketTransmitted (tag.GetToaUs ());
    }
    return;
  }
  
  TxQueueEntry entry;
  entry.packet = packet;
  entry.tag = tag;
  entry.retries = 0;
  entry.dstAddr = dstAddr;
  
  m_txQueue.push (entry);
  NS_LOG_INFO ("CSMA: Paquete en cola (size=" << m_txQueue.size () << ")");
  
  ProcessTxQueue ();
}

// Ejecuta detección de canal y transmisión efectiva de la cola CSMA.
void
MeshDvApp::ProcessTxQueue ()
{
  if (m_txQueue.empty ())
  {
    NS_LOG_DEBUG ("CSMA: Cola vacía");
    return;
  }
  
  if (m_txBusy)
  {
    NS_LOG_DEBUG ("CSMA: PHY ocupado, esperar");
    return;
  }
  
  TxQueueEntry& entry = m_txQueue.front ();
  
  uint8_t difsCount = m_mac ? m_mac->GetDifsCadCount () : m_difsCadCount;
  NS_LOG_INFO ("CSMA: Iniciando DIFS con " << unsigned(difsCount) << " CADs");
  
  bool channelBusy = m_mac ? m_mac->PerformChannelAssessment () : false;
  
  if (channelBusy)
  {
    NS_LOG_INFO ("CSMA: Canal ocupado detectado, aplicando backoff");
    m_backoffCount++;
    uint32_t backoffSlots = m_mac ? m_mac->GetBackoffSlots ()
                                  : m_rng->GetInteger (0, (1 << m_backoffWindow) - 1);
    Time cadDuration = m_mac ? m_mac->GetCadDuration () : m_cadDuration;
    Time backoffTime = cadDuration * backoffSlots;
    
    NS_LOG_INFO ("CSMA: Backoff " << backoffSlots << " slots ("
                 << backoffTime.GetMilliSeconds () << "ms)");
    
    m_backoffEvt = Simulator::Schedule (backoffTime, &MeshDvApp::ProcessTxQueue, this);
  }
  else
  {
    NS_LOG_INFO ("CSMA: Canal libre, transmitiendo");
    m_txBusy = true;
    
    Ptr<Node> n = GetNode ();
    Ptr<NetDevice> dev = nullptr;
    for (uint32_t i = 0; i < n->GetNDevices (); ++i)
    {
      Ptr<NetDevice> d = n->GetDevice (i);
      if (d) { dev = d; break; }
    }
    if (!dev)
    {
      // Sin dispositivo no podemos transmitir; liberar el bloqueo para no colgar la cola.
      NS_LOG_ERROR ("CSMA: NetDevice no disponible, descartando paquete en cabeza de cola");
      m_txBusy = false;
      m_txQueue.pop ();
      ProcessTxQueue ();
      return;
    }
    
    Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev = DynamicCast<ns3::lorawan::MeshLoraNetDevice> (dev);
    if (meshDev)
    {
      if (m_mac)
      {
        meshDev->SetMac (m_mac);
      }
      if (m_energyModel)
      {
        meshDev->SetEnergyModel (m_energyModel);
      }
    }

    Ptr<Packet> p = entry.packet->Copy ();
    p->RemoveAllPacketTags ();
    p->AddPacketTag (entry.tag);
    
    Address dst = (entry.dstAddr == Address()) ? dev->GetBroadcast () : entry.dstAddr;
    const bool ok = dev->Send (p, dst, kProtoMesh);
    
    NS_LOG_INFO ("CSMA: TX ok=" << ok);
    if (ok)
    {
      OnPacketTransmitted (entry.tag.GetToaUs ());
    }

    uint32_t toaUs = entry.tag.GetToaUs ();
    Time txDuration = MicroSeconds (toaUs);
    
    Simulator::Schedule (txDuration, [this]() {
      m_txBusy = false;
      m_txQueue.pop ();
      ProcessTxQueue ();
    });
  }
}

// Registra TX reales para duty-cycle
void
MeshDvApp::OnPacketTransmitted (uint32_t toaUs)
{
  NS_LOG_INFO ("OnPacketTransmitted(): node=" << GetNode ()->GetId ()
               << " toaUs=" << toaUs);
  m_txCount++;
  double duty = m_mac ? m_mac->GetDutyCycleUsed () : 0.0;
  NS_LOG_INFO ("Duty cycle actualizado=" << (duty * 100.0) << "%");
}

// Maneja el temporizador de backoff cuando expira.
void
MeshDvApp::OnBackoffTimer ()
{
  NS_LOG_DEBUG ("CSMA: Backoff timer expirado");
  ProcessTxQueue ();
}

int16_t
MeshDvApp::GetRealRSSI ()
{
  Ptr<Node> node = GetNode ();
  
  // Obtener el NetDevice
  Ptr<NetDevice> dev = node->GetDevice (0);
  
  // Usar namespace completo: ns3::lorawan::MeshLoraNetDevice
  Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev = DynamicCast<ns3::lorawan::MeshLoraNetDevice> (dev);
  
  if (!meshDev)
  {
    NS_LOG_WARN ("No MeshLoraNetDevice found, usando RSSI por defecto");
    return -95;
  }
  
  // Obtener el RSSI del último paquete recibido
  double rssi = meshDev->GetLastRxRssi ();
  
  // Convertir a int16_t
  int16_t rssiInt = static_cast<int16_t> (std::round (rssi));
  
  NS_LOG_DEBUG ("RSSI from last RX: " << rssiInt << " dBm");

  return rssiInt;
}

uint16_t
MeshDvApp::GetBatteryVoltageMv () const
{
  if (!m_energyModel)
  {
    return 0;
  }
  double voltage = m_energyModel->GetVoltageMv (GetNode ()->GetId ());
  return static_cast<uint16_t> (std::round (voltage));
}

double
MeshDvApp::GetRemainingEnergyJ () const
{
  if (!m_energyModel)
  {
    return 0.0;
  }
  return m_energyModel->GetRemainingEnergy (GetNode ()->GetId ());
}

double
MeshDvApp::ComputeSnrFromRssi (int16_t rssiDbm) const
{
  static const double referenceSensitivity = lorawan::EndDeviceLoraPhy::sensitivity[5];
  return static_cast<double> (rssiDbm) - referenceSensitivity;
}

uint8_t
MeshDvApp::SelectSfFromAdr (uint32_t dst, int16_t rssiDbm) const
{
  const double snr = ComputeSnrFromRssi (rssiDbm) - m_sfMarginDb;
  if (!m_adr.IsValidLink (snr))
  {
    NS_LOG_DEBUG ("Node " << GetNode ()->GetId ()
                  << " ADR link below minimum SNR=" << snr
                  << " dB toward dst=" << dst << ", forcing SF12");
  }
  return static_cast<uint8_t> (m_adr.SelectSf (GetNode ()->GetId (), dst, snr));
}

void
MeshDvApp::HandleRouteChange (const loramesh::RouteEntry& entry, const std::string& action)
{
  NS_LOG_INFO ("Route " << action << ": dst=" << entry.destination
               << " via=" << entry.nextHop
               << " score=" << entry.scoreX100
               << " seq=" << entry.seqNum);
  if (g_metricsCollector && action != "NONE")
  {
    g_metricsCollector->RecordRoute (GetNode ()->GetId (), entry.destination,
                                     entry.nextHop, entry.hops,
                                     entry.scoreX100, entry.seqNum, action);
  }
}

void
MeshDvApp::HandleFloodRequest (const loramesh::DvMessage& msg)
{
  NS_LOG_DEBUG ("FloodDvUpdate requested for node=" << GetNode ()->GetId ()
                << " seq=" << msg.sequence << " entries=" << msg.entries.size ());
}

loramesh::NeighborLinkInfo
MeshDvApp::BuildNeighborLinkInfo (const MeshMetricTag& tag,
                                  uint32_t toaUs,
                                  Mac48Address fromMac) const
{
  loramesh::NeighborLinkInfo link;
  link.neighbor = tag.GetSrc ();
  link.sequence = tag.GetSeq ();
  link.hops = tag.GetHops ();
  link.sf = tag.GetSf ();
  link.toaUs = toaUs;
  link.rssiDbm = tag.GetRssiDbm ();
  link.batt_mV = tag.GetBatt_mV ();
  link.scoreX100 = tag.GetScoreX100 ();
  link.mac = fromMac;
  return link;
}

std::vector<loramesh::DvEntry>
MeshDvApp::DecodeDvEntries (Ptr<const Packet> p,
                            const MeshMetricTag& tag,
                            uint32_t toaUsNeighbor) const
{
  std::vector<loramesh::DvEntry> entries;
  const size_t payloadLen = p->GetSize ();
  const size_t entrySize = sizeof (MeshMetricTag::RoutePayloadEntry);

  if (entrySize == 0 || payloadLen < entrySize)
  {
    if (payloadLen > 0)
    {
      NS_LOG_DEBUG("Beacon payload demasiado pequeño (" << payloadLen
                    << " bytes), sin rutas que procesar");
    }
    return entries;
  }

  const size_t n = payloadLen / entrySize;
  std::vector<MeshMetricTag::RoutePayloadEntry> receivedRoutes (n);
  p->CopyData (reinterpret_cast<uint8_t*> (receivedRoutes.data ()), n * entrySize);

  for (const auto& route : receivedRoutes)
  {
    if (route.dst == GetNode()->GetId())
    {
      continue;  // No aprender ruta a sí mismo
    }

    uint8_t totalHops = route.hops + 1;
    if (totalHops > m_initTtl)
    {
      NS_LOG_DEBUG("Descartando ruta por hops excesivos ("
                   << unsigned(totalHops) << " > TTL)");
      continue;
    }

    uint16_t accumulatedScore = static_cast<uint16_t>(
      std::clamp<uint32_t>((route.score + tag.GetScoreX100()) / 2, 0u, 100u));

    loramesh::DvEntry entry;
    entry.destination = route.dst;
    entry.hops = totalHops;
    entry.sf = tag.GetSf ();
    entry.scoreX100 = accumulatedScore;
    entry.toaUs = toaUsNeighbor;
    entry.rssiDbm = route.rssi_dBm;
    entry.batt_mV = route.batt_mV;
    entries.push_back (entry);
  }
  return entries;
}

void
MeshDvApp::ProcessDvPayload (Ptr<const Packet> p,
                             const MeshMetricTag& tag,
                             const Mac48Address& fromMac,
                             uint32_t toaUsNeighbor)
{
  if (!m_routing)
  {
    return;
  }

  loramesh::NeighborLinkInfo link = BuildNeighborLinkInfo (tag, toaUsNeighbor, fromMac);
  loramesh::DvMessage msg;
  msg.origin = tag.GetSrc ();
  msg.sequence = tag.GetSeq ();
  msg.entries = DecodeDvEntries (p, tag, toaUsNeighbor);
  m_routing->UpdateFromDvMsg (msg, link);
}

// ========================================================================

void
MeshDvApp::LogTxEvent (uint32_t seq, uint32_t dst, uint8_t ttl, uint8_t hops,
                       int16_t rssi, uint16_t battery, uint16_t score, uint8_t sf,
                       double energyJ, double energyFrac, bool ok)
{
  if (g_metricsCollector)
  {
    g_metricsCollector->RecordTx (GetNode ()->GetId (), seq, dst, ttl, hops, 
                                  rssi, battery, score, sf, energyJ, energyFrac, ok);
    g_metricsCollector->RecordEnergySnapshot (GetNode ()->GetId (), energyJ, energyFrac);
  }
}

void
MeshDvApp::LogRxEvent (uint32_t src, uint32_t dst, uint32_t seq, uint8_t ttl,
                       uint8_t hops, int16_t rssi, uint16_t battery, uint16_t score, uint8_t sf,
                       double energyJ, double energyFrac, bool forwarded)
{
  if (g_metricsCollector)
  {
    g_metricsCollector->RecordRx (GetNode ()->GetId (), src, dst, seq, ttl, hops,
                                  rssi, battery, score, sf, energyJ, energyFrac, forwarded);
    g_metricsCollector->RecordEnergySnapshot (GetNode ()->GetId (), energyJ, energyFrac);
  }
}

void
MeshDvApp::CleanOldSeenPackets ()
{
  if (m_seenPackets.empty ())
  {
    return;
  }

  Time threshold = Simulator::Now () - m_seenPacketWindow;
  for (auto it = m_seenPackets.begin (); it != m_seenPackets.end (); )
  {
    if (it->second < threshold)
    {
      it = m_seenPackets.erase (it);
    }
    else
    {
      ++it;
    }
  }
}

void
MeshDvApp::CleanOldSeenData ()
{
  if (m_seenData.empty ())
    {
      return;
    }
  Time threshold = Simulator::Now () - m_seenDataWindow;
  for (auto it = m_seenData.begin (); it != m_seenData.end (); )
    {
      if (it->second < threshold)
        {
          it = m_seenData.erase (it);
        }
      else
        {
          ++it;
        }
    }
}
// ========================================================================
// Data Traffic Generation
// ========================================================================

// Genera tráfico de datos hacia el gateway cuando corresponde.
void
MeshDvApp::GenerateDataTraffic ()
{
  // Solo nodos ED generan datos (no el GW)
  if (GetNode ()->GetId () < 3)
  {
    uint32_t dst = 3;  // Gateway
    SendDataPacket (dst);
    
    double jitter = m_rng->GetValue(0.0, 5.0);
    m_dataGenerationEvt = Simulator::Schedule (m_dataGenerationPeriod + Seconds(jitter),
                                           &MeshDvApp::GenerateDataTraffic, this);
  }
}

// Construye y envía un paquete de datos unicast.
void
MeshDvApp::SendDataPacket (uint32_t dst)
{
  uint32_t nextSeq = m_dataSeqPerNode + 1;
  uint32_t myId = GetNode ()->GetId ();

  NS_LOG_UNCOND ("APP_SEND_DATA src=" << myId
                 << " dst=" << dst
                 << " seq=" << nextSeq
                 << " time=" << Simulator::Now ().GetSeconds ());

  // Dump de tabla DV en la ventana de interés (70s-90s)
  double nowSec = Simulator::Now ().GetSeconds ();
  if (nowSec > 70.0 && nowSec < 90.0 && m_routing)
    {
      NS_LOG_UNCOND ("DV_DUMP_FULL node" << myId << " t=" << nowSec);
      m_routing->DebugDumpRoutingTable ();
    }

  const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute (dst) : nullptr;
  bool routeExists = (route != nullptr);
  uint32_t routeNextHop = routeExists ? route->nextHop : 0;
  uint8_t routeHops = routeExists ? route->hops : 0;

  NS_LOG_UNCOND ("FWDTRACE data_tx_attempt time=" << Simulator::Now ().GetSeconds ()
                 << " node=" << myId
                 << " src=" << myId
                 << " dst=" << dst
                 << " seq=" << nextSeq
                 << " routeExists=" << routeExists
                 << " nextHop=" << routeNextHop
                 << " hops=" << unsigned (routeHops));

  m_dataPacketsGenerated++;

  if (!route)
  {
    NS_LOG_UNCOND ("FWDTRACE DATA_NOROUTE time=" << Simulator::Now ().GetSeconds ()
                   << " node=" << myId
                   << " src=" << myId
                   << " dst=" << dst
                   << " seq=" << nextSeq
                   << " reason=no_route");
    m_dataNoRoute++;
    return;
  }

  // Crear paquete de datos
  MeshMetricTag dataTag;
  dataTag.SetSrc (myId);
  dataTag.SetDst (dst);  // Destino final (GW)
  dataTag.SetSeq (nextSeq);
  m_dataSeqPerNode = nextSeq;
  dataTag.SetTtl (m_initTtl);
  dataTag.SetHops (0);
  uint8_t sfForRoute = route->sf ? route->sf : SelectSfFromAdr (dst, GetRealRSSI ());
  dataTag.SetSf (sfForRoute);

  const uint32_t toaUs = ComputeLoRaToAUs (sfForRoute, m_bw, m_cr, m_dataPayloadSize);
  dataTag.SetToaUs (toaUs);
  
  int16_t realRSSI = GetRealRSSI ();
  uint16_t realBatt = GetBatteryVoltageMv ();
  
  dataTag.SetRssiDbm (realRSSI);
  dataTag.SetBatt_mV (realBatt);
  dataTag.SetScoreX100 (ComputeScoreX100 (dataTag));

  Ptr<Packet> p = Create<Packet> (m_dataPayloadSize);
  p->AddPacketTag (dataTag);

  // Verificar MAC disponible para el nextHop antes de resolver dirección
  auto macIt = m_macTable.find (route->nextHop);
  NS_LOG_UNCOND ("MAC_CHECK node" << myId
                 << " nextHop=" << route->nextHop
                 << " macFound=" << (macIt != m_macTable.end () ? "YES" : "NO"));
  NS_LOG_UNCOND ("MAC_TABLE node" << myId
                 << " size=" << m_macTable.size ());
  for (const auto& kv : m_macTable)
    {
      NS_LOG_UNCOND ("  entry nextHopId=" << kv.first << " mac=" << kv.second);
    }
  if (macIt == m_macTable.end ())
    {
      NS_LOG_UNCOND ("NO_MAC node" << myId
                     << " for dst=" << dst
                     << " nextHop=" << route->nextHop);
    }

  NS_LOG_INFO ("DATA src=" << dataTag.GetSrc () 
               << " dst=" << dataTag.GetDst () 
               << " seq=" << dataTag.GetSeq ()
               << " nextHop=" << route->nextHop  // ← USAR NEXT-HOP
               << " toaUs=" << dataTag.GetToaUs ()
               << " sf=" << unsigned (dataTag.GetSf ()));

  // Log de ruta usada para este envío (snapshot coherente con routes_raw)
  if (g_metricsCollector)
  {
    g_metricsCollector->RecordRouteUsed (myId, dataTag.GetDst (),
                                         route->nextHop, route->hops,
                                         route->scoreX100, route->seqNum);
  }

  LogTxEvent (dataTag.GetSeq (), dataTag.GetDst (), dataTag.GetTtl (),
              dataTag.GetHops (), dataTag.GetRssiDbm (), dataTag.GetBatt_mV (),
              dataTag.GetScoreX100 (), dataTag.GetSf (),
              m_energyModel ? m_energyModel->GetRemainingEnergy (myId) : -1.0,
              m_energyModel ? m_energyModel->GetEnergyFraction (myId) : -1.0,
              true);

  // Log explícito de TX con MAC resuelta
  std::string macStr = "(unset)";
  Mac48Address macAddr = route->nextHopMac;
  if (macAddr == Mac48Address ())
    {
      auto itMac = m_macTable.find (route->nextHop);
      if (itMac != m_macTable.end ())
        {
          macAddr = itMac->second;
        }
    }
  {
    std::ostringstream macOss;
    macOss << macAddr;
    macStr = macOss.str ();
  }
  NS_LOG_UNCOND ("FWD_TX node" << myId
                 << " dst=" << dst
                 << " nextHop=" << route->nextHop
                 << " mac=" << macStr);
  NS_LOG_UNCOND ("MAC_DUMP node" << myId
                 << " nextHop=" << route->nextHop
                 << " mac=" << macStr);
  
  // Verificar duty cycle
  if (!m_mac || !m_mac->CanTransmitNow (toaUs / 1e6))
  {
    NS_LOG_UNCOND ("FWDTRACE DATA_NOROUTE time=" << Simulator::Now ().GetSeconds ()
                   << " node=" << myId
                   << " src=" << dataTag.GetSrc ()
                   << " dst=" << dst
                   << " seq=" << dataTag.GetSeq ()
                   << " reason=duty_block");
    m_dataNoRoute++;
    return;
  }

  // Los nodos intermedios deben capturar y forward según dst
  // ========================================================================
  Address dstAddr = (route->nextHopMac != Mac48Address())
                        ? Address (route->nextHopMac)
                        : ResolveNextHopAddress (route->nextHop);
  // No enviar datos en broadcast; si no hay MAC válida, tratar como sin ruta
  Ptr<Node> n = GetNode ();
  Ptr<NetDevice> dev0 = (n->GetNDevices() > 0) ? n->GetDevice(0) : nullptr;
  bool isBroadcast = (!dstAddr.IsInvalid ()) && dev0 && dstAddr == dev0->GetBroadcast ();
  if (isBroadcast || route->nextHop == 0)
    {
      NS_LOG_UNCOND ("FWDTRACE DATA_NOROUTE time=" << Simulator::Now ().GetSeconds ()
                     << " node=" << myId
                     << " src=" << dataTag.GetSrc ()
                     << " dst=" << dst
                     << " seq=" << dataTag.GetSeq ()
                     << " nextHop=" << route->nextHop
                     << " reason=no_mac_for_unicast");
      m_dataNoRoute++;
      return;
    }

  NS_LOG_UNCOND ("FWDTRACE fwd time=" << Simulator::Now ().GetSeconds ()
                 << " node=" << myId
                 << " src=" << dataTag.GetSrc ()
                 << " dst=" << dst
                 << " seq=" << dataTag.GetSeq ()
                 << " nextHop=" << route->nextHop
                 << " tx_mode=unicast"
                 << " reason=ok");

  SendWithCSMA (p, dataTag, dstAddr);
}

} // namespace ns3
