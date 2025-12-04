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
#include <deque>  
#include "mesh_lora_net_device.h" 
#include "ns3/end-device-lora-phy.h"
#include "ns3/lora-tag.h"
#include "ns3/mac48-address.h"
#include <algorithm>
#include <cstring> // Para memcpy, memset, etc.




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

  int16_t realRSSI = GetRealRSSI ();
  uint16_t realBatt = GetCurrentBatteryMv ();
  tag.SetRssiDbm (realRSSI);
  tag.SetBatt_mV (realBatt);

  // ---------------------- NUEVO: Top-N mejor score ------------------------
  const uint32_t maxRoutes = GetBeaconRouteCapacity ();
  NS_LOG_INFO ("Beacon capacity (max routes)=" << maxRoutes);

  // Prepara vector de (score, dst) para ordenamiento
  std::vector<std::pair<uint16_t, uint16_t>> routes_for_sort;
  for (const auto& [dst, entry] : m_routingTable)
  {
    if (dst == GetNode()->GetId()) continue; // omite a sí mismo
    routes_for_sort.push_back({entry.scoreX100, dst});
  }

  // Ordena score descendente (mejor score primero)
  std::sort(routes_for_sort.begin(), routes_for_sort.end(),
            [](const auto& a, const auto& b) { return a.first > b.first; });

  // Arma el payload de solo los N mejores
  std::vector<MeshMetricTag::RoutePayloadEntry> payload;
  size_t count = 0;
  for (const auto& [score, dst] : routes_for_sort)
  {
    if (count >= maxRoutes) break;
    const auto& entry = m_routingTable[dst];
    MeshMetricTag::RoutePayloadEntry r;
    r.dst = dst;
    r.hops = entry.hops;
    r.sf = entry.sf;
    r.score = entry.scoreX100;
    r.batt_mV = entry.batt_mV;
    r.rssi_dBm = entry.rssiDbm;
    payload.push_back(r);
    count++;
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

  if (!CanTransmit (toaUs))
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

  LogTxEvent (m_seq, tag.GetDst (), m_initTtl, 0, realRSSI, realBatt, scoreX100, true);

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

  // Enviar un primer beacon inmediato en SF control y programar ciclos multi-SF
  BuildAndSendDv (m_sfControl);
  Time base = m_period;
  ScheduleDvCycle (m_sfControl, base, m_evtSf12);
  ScheduleDvCycle (10, base / 2, m_evtSf10);
  ScheduleDvCycle (9,  base / 4, m_evtSf9);

  m_purgeEvt = Simulator::Schedule (Seconds (30), &MeshDvApp::PurgeExpiredRoutes, this);
  
  Simulator::Schedule (Seconds (20), &MeshDvApp::PrintRoutingTable, this);
  Simulator::Schedule (Seconds (40), &MeshDvApp::PrintRoutingTable, this);

  InitializeBatteryModel ();
  NS_LOG_INFO ("Battery model inicializado para node=" << nodeId);
  
  // Programar generación de datos (solo para ED, no para GW)
  if (nodeId < 3)
  {
    NS_LOG_INFO ("  >>> Node " << nodeId << " (ED) - Data generation scheduled at t=60s");
    // Desfase inicial para evitar colisión con los beacons (que van cada 60s).
    // Lanzamos la primera ráfaga ~20s + jitter, y luego cada periodo.
    double jitter = m_rng->GetValue(0.0, 5.0);  // 0-5 segundos de jitter
    m_dataGenerationEvt = Simulator::Schedule (Seconds (20.0 + jitter), 
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
  
  if (m_battery.pendingTxEnd.IsPending ())
    m_battery.pendingTxEnd.Cancel ();

  UpdateBatteryEstimate ("StopApplication");

  // ========================================================================
  // NUEVO: Reportar estadísticas de datos al finalizar
  // ========================================================================
  if (GetNode ()->GetId () < 3)  // Si es ED
  {
    NS_LOG_INFO ("=== DATA STATISTICS Node " << GetNode ()->GetId () << " ===");
    NS_LOG_INFO ("  Data packets generated: " << m_dataPacketsGenerated);
    NS_LOG_INFO ("  Data packets delivered: " << m_dataPacketsDelivered);
    double pdr = (m_dataPacketsGenerated > 0) 
                 ? (100.0 * m_dataPacketsDelivered / m_dataPacketsGenerated) 
                 : 0.0;
    NS_LOG_INFO ("  PDR: " << pdr << "%");
  }
  else if (GetNode ()->GetId () == 3)  // Si es GW
  {
    NS_LOG_INFO ("=== GATEWAY RECEIVED ===");
    NS_LOG_INFO ("  Total data packets received: " << m_dataPacketsDelivered);
  }
  
  PrintRoutingTable ();
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

  // Aprender MAC del emisor para mapear ID lógico -> MAC (necesario para next-hop)
  Mac48Address fromMac = Mac48Address::ConvertFrom (from);
  m_macTable[tag.GetSrc()] = fromMac;

  uint32_t myId = GetNode ()->GetId ();
  uint32_t dst = tag.GetDst ();

  // ========================================================================
  // CASO 1: Paquete llegó a su destino final (YO soy el destino)
  // ========================================================================
  if (myId == dst)
  {
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
                  tag.GetScoreX100 (), false);
    }
    return true;  // NO forward si ya llegó a destino
  }

  // ========================================================================
  // CASO 2: Beacon DV (broadcast) - Actualizar tabla de rutas
  // ========================================================================
  if (dst == 0xFFFF)
  {
    // 1. Aprende ruta directa hacia el vecino que transmitió el beacon
    NS_LOG_INFO("Intento aprender ruta directa: src=" << tag.GetSrc() << " en node=" << GetNode()->GetId());
    int16_t neighborRssi = tag.GetRssiDbm();
    // Usar el SF con el que se recibió el beacon para reflejar el enlace real.
    uint8_t sfForNeighbor = tag.GetSf ();
    uint32_t toaUsNeighbor = ComputeLoRaToAUs (sfForNeighbor, m_bw, m_cr, p->GetSize ());
    NS_LOG_INFO ("  → RSSI=" << neighborRssi << "dBm SF=" << unsigned(sfForNeighbor)
                 << " toa=" << toaUsNeighbor << "us");
    UpdateRoutingTable(
      tag.GetSrc(),         // dst
      tag.GetSrc(),         // via (directo)
      tag.GetSeq(),         // seqNum
      tag.GetScoreX100(),   // score
      tag.GetHops(),        // hops
      sfForNeighbor,
      toaUsNeighbor,        // toaUs
      tag.GetRssiDbm(),     // rssi
      tag.GetBatt_mV(),     // batt
      fromMac               // mac del vecino (next-hop)
    );
    NS_LOG_INFO("Tabla rutas tras aprender: size=" << m_routingTable.size());

    // 2. Decodifica las rutas indirectas del payload
    size_t payloadLen = p->GetSize();
    size_t entrySize = sizeof(MeshMetricTag::RoutePayloadEntry);
    size_t n = (entrySize > 0) ? (payloadLen / entrySize) : 0;

    if (n > 0)
    {
      std::vector<MeshMetricTag::RoutePayloadEntry> receivedRoutes(n);
      p->CopyData(reinterpret_cast<uint8_t*>(receivedRoutes.data()), n * entrySize);

      for (const auto& route : receivedRoutes)
      {
        if (route.dst == GetNode()->GetId())
        {
          continue;  // No aprender ruta a sí mismo
        }

        // Acotar hops para evitar rutas imposibles.
        uint8_t totalHops = route.hops + 1;
        if (totalHops > m_initTtl)
        {
          NS_LOG_DEBUG("Descartando ruta por hops excesivos ("
                       << unsigned(totalHops) << " > TTL)");
          continue;
        }

        // Acotar score: usar promedio para evitar suma ilimitada y clamp 0-100.
        uint16_t accumulated_score = static_cast<uint16_t>(
          std::clamp<uint32_t>((route.score + tag.GetScoreX100()) / 2, 0u, 100u));

        UpdateRoutingTable(
          route.dst,
          tag.GetSrc(),
          tag.GetSeq(),
          accumulated_score,
          totalHops,
          sfForNeighbor,
          toaUsNeighbor,
          route.rssi_dBm,
          route.batt_mV,
          fromMac); // MAC del vecino que anunció la ruta
      }
    }
    else if (payloadLen > 0)
    {
      NS_LOG_DEBUG("Beacon payload demasiado pequeño (" << payloadLen
                    << " bytes), sin rutas que procesar");
    }

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
    newTag.SetBatt_mV(GetCurrentBatteryMv());
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
  RouteEntry* route = GetRoute (dst);
  uint8_t ttl = tag.GetTtl ();
  uint8_t hops = tag.GetHops () + 1;

  bool canForward = (route != nullptr) && (ttl > 0);

  // Registrar RX aun si no se reenvía (para métricas de PDR).
  LogRxEvent (tag.GetSrc (), tag.GetDst (), tag.GetSeq (), tag.GetTtl (),
              tag.GetHops (), tag.GetRssiDbm (), tag.GetBatt_mV (),
              tag.GetScoreX100 (), canForward);

  if (!route)
  {
    NS_LOG_WARN ("Node " << myId << " RX DROP: No route to dst=" << dst);
    return true;  // Drop
  }

  // ========================================================================
  // FILTRO CRÍTICO: Solo forward si SOY uno de los posibles next-hops
  // En broadcast, todos reciben, pero solo nodos con ruta válida forwardean
  // ========================================================================
  if (ttl == 0)
  {
    NS_LOG_WARN ("Node " << myId << " RX DROP: TTL=0");
    return true;
  }

  ttl -= 1;

  MeshMetricTag newTag = tag;
  newTag.SetTtl (ttl);
  newTag.SetHops (hops);
  uint8_t selectedSf = route->sf ? route->sf : SelectSfForRssi (tag.GetRssiDbm ());
  newTag.SetSf (selectedSf);
  newTag.SetToaUs (ComputeLoRaToAUs (selectedSf, m_bw, m_cr, p->GetSize ()));
  newTag.SetRssiDbm (GetRealRSSI ());
  newTag.SetBatt_mV (GetCurrentBatteryMv ());
  newTag.SetScoreX100 (ComputeScoreX100 (newTag));

  NS_LOG_INFO ("RX DATA: src=" << tag.GetSrc () << " dst=" << dst 
               << " seq=" << tag.GetSeq () << " hops=" << (int)tag.GetHops ());
  NS_LOG_INFO ("FWD DATA: src=" << newTag.GetSrc () << " dst=" << dst 
               << " seq=" << newTag.GetSeq () << " hops=" << (int)hops 
               << " nextHop=" << route->nextHop);

  LogRxEvent (tag.GetSrc (), tag.GetDst (), tag.GetSeq (), tag.GetTtl (),
              tag.GetHops (), tag.GetRssiDbm (), tag.GetBatt_mV (),
              tag.GetScoreX100 (), true);

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
                  inTag.GetScoreX100 (), false);
    }
    return;  // NO forward
  }

  // ========================================================================
  // Deduplicación - SOLO para reenvíos, NO para mi propio origen
  // ========================================================================
  if (inTag.GetSrc() != myId)  // ← Solo si NO soy el origen
  {
    CleanOldSeenPackets ();
    std::pair<uint32_t, uint32_t> pktId = {inTag.GetSrc(), inTag.GetSeq()};
    auto seenIt = m_seenPackets.find (pktId);
    if (seenIt != m_seenPackets.end())
    {
      NS_LOG_DEBUG("Node " << myId << " FWD DROP: Paquete duplicado (src=" 
                   << inTag.GetSrc() << " seq=" << inTag.GetSeq() << ")");
      return;
    }
    m_seenPackets[pktId] = Simulator::Now ();
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
    if (!CanTransmit (toaUs))
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
  // ========================================================================
  RouteEntry* route = GetRoute (dst);
  if (!route)
  {
    NS_LOG_WARN ("Node " << myId << " FWD DROP: No route to dst=" << dst);
    return;
  }

  // ========================================================================
  // CASO 3: Verificar duty cycle para forward
  // ========================================================================
  uint8_t sfForRoute = route->sf ? route->sf : SelectSfForRssi (inTag.GetRssiDbm ());
  uint32_t toaUs = ComputeLoRaToAUs (sfForRoute, m_bw, m_cr, pIn->GetSize ());
  if (!CanTransmit (toaUs))
  {
    NS_LOG_WARN ("Node " << myId << " FWD DROP: Duty cycle exceeded");
    return;
  }

  // ========================================================================
  // CASO 4: Preparar paquete para forward
  // ========================================================================
  std::ostringstream oss;
  oss << "TX FWD: src=" << inTag.GetSrc()
      << " dst=" << dst
      << " seq=" << inTag.GetSeq()
      << " hops=" << (int)inTag.GetHops()
      << " nextHop=" << route->nextHop;
  NS_LOG_INFO(oss.str());

  Ptr<Packet> modifiablePkt = pIn->Copy();
  MeshMetricTag outTag = inTag;
  outTag.SetSf (sfForRoute);
  outTag.SetToaUs (toaUs);
  outTag.SetRssiDbm (GetRealRSSI ());
  outTag.SetBatt_mV (GetCurrentBatteryMv ());
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
  Address dstAddr = (route->nextHopMac != Mac48Address())
                        ? Address (route->nextHopMac)
                        : ResolveNextHopAddress (route->nextHop);
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
  // Pesos: se penaliza más el RSSI para desincentivar enlaces largos/débiles.
  const double w_toa = 0.3;
  const double w_hop = 0.2;
  const double w_rssi= 0.4;
  const double w_bat = 0.1;

  const double toa_ms   = t.GetToaUs() / 1000.0;
  const double toa_norm = std::min (toa_ms / 2000.0, 1.0);
  const double hop_norm = std::min (t.GetHops() / 10.0, 1.0);

  // Penalización más agresiva de RSSI (umbral suave desde -90 hasta -130 dBm).
  const double rssi_penalty = std::clamp ((-t.GetRssiDbm() - 90.0) / 40.0, 0.0, 1.0);
  const double batt_norm = std::clamp ((t.GetBatt_mV() - 3000.0)/1200.0, 0.0, 1.0);

  const double cost = w_toa*toa_norm + w_hop*hop_norm
                    + w_rssi*rssi_penalty + w_bat*(1.0 - batt_norm);

  const double score = std::clamp (1.0 - cost, 0.0, 1.0);
  return static_cast<uint16_t>(std::round (score * 100.0));
}

void
MeshDvApp::UpdateRoutingTable(uint32_t dst, uint32_t viaNode,
                              uint32_t seqNum, uint16_t scoreX100,
                              uint8_t hops, uint8_t sf, uint32_t toaUs,
                              int16_t rssiDbm, uint16_t batt_mV,
                              Mac48Address nextHopMac)
{
  if (dst == GetNode()->GetId())
    return;

  auto it = m_routingTable.find(dst);
  bool shouldUpdate = false;
  std::string action = "NONE";

  if (it == m_routingTable.end())
  {
    shouldUpdate = true;
    action = "NEW";
    NS_LOG_INFO("Route NEW: dst=" << dst << " via=" << viaNode
                 << " score=" << scoreX100 << " seq=" << seqNum);
  }
  else
  {
    RouteEntry& oldEntry = it->second;

    if (seqNum > oldEntry.seqNum)
    {
      shouldUpdate = true;
      action = "UPDATE";
      NS_LOG_INFO("Route UPDATE (newer seq): dst=" << dst << " via=" << viaNode
                   << " score=" << scoreX100 << " seq=" << seqNum);
    }
    else if (seqNum == oldEntry.seqNum)
    {
      if (scoreX100 > oldEntry.scoreX100)
      {
        shouldUpdate = true;
        action = "UPDATE";
        NS_LOG_INFO("Route UPDATE (better score): dst=" << dst << " via=" << viaNode
                     << " score=" << scoreX100);
      }
      else if (scoreX100 == oldEntry.scoreX100 && hops < oldEntry.hops)
      {
        shouldUpdate = true;
        action = "UPDATE";
        NS_LOG_INFO("Route UPDATE (tie-break hops): dst=" << dst << " via=" << viaNode
                     << " hops=" << unsigned(hops));
      }
      else if (scoreX100 == oldEntry.scoreX100 && hops == oldEntry.hops && sf < oldEntry.sf)
      {
        shouldUpdate = true;
        action = "UPDATE";
        NS_LOG_INFO("Route UPDATE (tie-break SF): dst=" << dst << " via=" << viaNode
                     << " sf=" << unsigned(sf));
      }
    }
  }

  if (shouldUpdate)
  {
    RouteEntry entry;
    entry.destination = dst;
    entry.nextHop = viaNode;
    entry.seqNum = seqNum;
    entry.hops = std::min<uint8_t> (hops, m_initTtl);
    entry.sf = sf;
    entry.toaUs = toaUs;
    entry.rssiDbm = rssiDbm;
    entry.batt_mV = batt_mV;
    entry.scoreX100 = std::clamp<uint16_t> (scoreX100, 0, 100);
    entry.lastUpdate = Simulator::Now();
    entry.expiryTime = Simulator::Now() + m_routeTimeout;
    entry.nextHopMac = nextHopMac;
    m_routingTable[dst] = entry;

    // Registrar el evento de ruta para el recolector.
    if (g_metricsCollector && action != "NONE")
    {
      g_metricsCollector->RecordRoute (GetNode ()->GetId (), dst, viaNode,
                                       hops, scoreX100, seqNum, action);
    }
  }
}

RouteEntry*
MeshDvApp::GetRoute (uint32_t destination)
{
  auto it = m_routingTable.find (destination);
  if (it == m_routingTable.end ())
    return nullptr;
  
  RouteEntry& entry = it->second;
  
  if (Simulator::Now () > entry.expiryTime)
  {
    NS_LOG_WARN ("Route to " << destination << " expired");
    return nullptr;
  }
  
  return &entry;
}

void
MeshDvApp::PrintRoutingTable ()
{
  double energyConsumed = GetEnergyConsumptionJoules ();
  uint16_t batteryMv = GetCurrentBatteryMv ();
  
  NS_LOG_INFO ("=== ROUTING TABLE Node " << GetNode ()->GetId () 
               << " (entries=" << m_routingTable.size () << ")"
               << " Energy=" << energyConsumed << "J"
               << " Battery=" << batteryMv << "mV ===");
  
  if (m_routingTable.empty ())
  {
    NS_LOG_INFO ("  (empty)");
    return;
  }
  
  for (const auto& kv : m_routingTable)
  {
    const RouteEntry& e = kv.second;
    NS_LOG_INFO ("  dst=" << e.destination 
                 << " via=" << e.nextHop
                 << " hops=" << unsigned(e.hops)
                 << " sf=" << unsigned(e.sf)
                 << " score=" << e.scoreX100
                 << " seq=" << e.seqNum
                 << " age=" << (Simulator::Now () - e.lastUpdate).GetSeconds () << "s");
  }
}

void
MeshDvApp::PurgeExpiredRoutes ()
{
  Time now = Simulator::Now ();
  size_t removed = 0;
  
  for (auto it = m_routingTable.begin (); it != m_routingTable.end (); )
  {
    if (now > it->second.expiryTime)
    {
      if (g_metricsCollector)
      {
        g_metricsCollector->RecordRoute (GetNode ()->GetId (), it->second.destination,
                                         it->second.nextHop, it->second.hops,
                                         it->second.scoreX100, it->second.seqNum,
                                         "PURGE");
      }
      NS_LOG_INFO ("Route PURGE: dst=" << it->second.destination);
      it = m_routingTable.erase (it);
      removed++;
    }
    else
    {
      ++it;
    }
  }
  
  if (removed > 0)
    NS_LOG_INFO ("Purged " << removed << " expired routes");
  
  m_purgeEvt = Simulator::Schedule (Seconds (30), &MeshDvApp::PurgeExpiredRoutes, this);
}

void
MeshDvApp::SendDataToDestination (uint32_t dst, Ptr<Packet> payload)
{
  RouteEntry* route = GetRoute (dst);
  if (!route)
  {
    NS_LOG_WARN ("No route to dst=" << dst);
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
  
  Ptr<Node> n = GetNode ();
  Ptr<NetDevice> dev = nullptr;
  for (uint32_t i = 0; i < n->GetNDevices (); ++i)
  {
    Ptr<NetDevice> d = n->GetDevice (i);
    if (d) { dev = d; break; }
  }
  if (!dev) return;
  
  Address dstAddr = ResolveNextHopAddress (route->nextHop);
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
MeshDvApp::ScheduleDvCycle (uint8_t sf, Time period, EventId& evtSlot)
{
  if (period.IsZero ())
  {
    return;
  }
  evtSlot = Simulator::Schedule (period, [this, sf, period, &evtSlot]() mutable {
    BuildAndSendDv (sf);
    ScheduleDvCycle (sf, period, evtSlot);
  });
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
  
  NS_LOG_INFO ("CSMA: Iniciando DIFS con " << unsigned(m_difsCadCount) << " CADs");
  
  bool channelBusy = false;
  for (uint8_t i = 0; i < m_difsCadCount; ++i)
  {
    if (PerformCAD ())
    {
      channelBusy = true;
      NS_LOG_INFO ("CSMA: Canal ocupado detectado en CAD " << unsigned(i));
      break;
    }
  }
  
  if (channelBusy)
  {
    uint32_t backoffSlots = m_rng->GetInteger (0, (1 << m_backoffWindow) - 1);
    Time backoffTime = backoffSlots * m_cadDuration;
    
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

// Registra TX reales para duty-cycle y actualiza la batería.
void
MeshDvApp::OnPacketTransmitted (uint32_t toaUs)
{
  NS_LOG_INFO ("OnPacketTransmitted(): node=" << GetNode ()->GetId ()
               << " toaUs=" << toaUs);
  RecordTransmission (toaUs);
  BeginRadioTx (MicroSeconds (toaUs));
  NS_LOG_INFO ("Duty cycle actualizado=" << (GetCurrentDutyCycle () * 100.0) << "%");
}

// Evalúa el canal mediante CAD simplificado.
bool
MeshDvApp::PerformCAD ()
{
  double randomVal = m_rng->GetValue (0.0, 1.0);
  bool channelDetected = (randomVal < 0.2);
  
  NS_LOG_DEBUG ("CSMA: CAD resultado=" << (channelDetected ? "BUSY" : "FREE"));
  
  return channelDetected;
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

uint8_t
MeshDvApp::SelectSfForRssi (int16_t rssiDbm) const
{
  static const uint8_t sfValues[] = {7, 8, 9, 10, 11, 12};
  for (size_t i = 0; i < sizeof(sfValues) / sizeof(uint8_t); ++i)
  {
    double threshold = lorawan::EndDeviceLoraPhy::sensitivity[i] + m_sfMarginDb;
    if (rssiDbm >= threshold)
    {
      return sfValues[i];
    }
  }
  return 12;
}

// Reinicia el modelo energético por nodo.
void
MeshDvApp::InitializeBatteryModel ()
{
  if (m_battery.pendingTxEnd.IsPending ())
  {
    m_battery.pendingTxEnd.Cancel ();
  }
  m_battery.remainingMah = BATTERY_CAPACITY_MAH;
  m_battery.lastUpdate = Simulator::Now ();
  m_battery.state = RadioState::RX;
  m_currentBatteryMv = static_cast<uint16_t> (BATTERY_VOLTAGE_MAX_MV);
  NS_LOG_INFO ("Battery inicializada: capacidad=" << BATTERY_CAPACITY_MAH
               << "mAh node=" << GetNode ()->GetId ());
}

// Aplica el consumo acumulado desde la última actualización.
void
MeshDvApp::UpdateBatteryEstimate (const std::string& reason)
{
  Time now = Simulator::Now ();
  if (now <= m_battery.lastUpdate)
  {
    return;
  }

  double dtSeconds = (now - m_battery.lastUpdate).GetSeconds ();
  double currentMa = IDLE_POWER_MA;
  switch (m_battery.state)
  {
    case RadioState::TX:
      currentMa = TX_POWER_MA;
      break;
    case RadioState::RX:
      currentMa = RX_POWER_MA;
      break;
    default:
      currentMa = IDLE_POWER_MA;
      break;
  }

  double consumedMah = currentMa * dtSeconds / 3600.0;
  m_battery.remainingMah = std::max (0.0, m_battery.remainingMah - consumedMah);
  m_battery.lastUpdate = now;

  NS_LOG_INFO ("Battery update [" << reason << "] state=" << RadioStateToString ()
               << " dt=" << dtSeconds << "s consumed=" << consumedMah
               << "mAh remaining=" << m_battery.remainingMah);
}

// Marca el inicio de una transmisión para consumir energía a potencia de TX.
void
MeshDvApp::BeginRadioTx (Time duration)
{
  UpdateBatteryEstimate ("BeginTX");
  m_battery.state = RadioState::TX;
  if (m_battery.pendingTxEnd.IsPending ())
  {
    m_battery.pendingTxEnd.Cancel ();
  }
  m_battery.pendingTxEnd = Simulator::Schedule (duration, &MeshDvApp::EndRadioTx, this);
  NS_LOG_INFO ("Battery: estado TX durante " << duration.GetMilliSeconds () << "ms");
}

// Retorna al modo de escucha RX después de transmitir.
void
MeshDvApp::EndRadioTx ()
{
  UpdateBatteryEstimate ("EndTX");
  m_battery.state = RadioState::RX;
  NS_LOG_INFO ("Battery: regreso a estado RX");
}

// Porcentaje de batería restante [0-100].
double
MeshDvApp::GetBatteryPercent () const
{
  double percent = (m_battery.remainingMah / BATTERY_CAPACITY_MAH) * 100.0;
  if (percent < 0.0)
  {
    return 0.0;
  }
  if (percent > 100.0)
  {
    return 100.0;
  }
  return percent;
}

// Utilidad para imprimir el estado actual de radio/batería.
std::string
MeshDvApp::RadioStateToString () const
{
  switch (m_battery.state)
  {
    case RadioState::TX:
      return "TX";
    case RadioState::RX:
      return "RX";
    case RadioState::IDLE:
    default:
      return "IDLE";
  }
}

uint16_t
MeshDvApp::GetCurrentBatteryMv ()
{
  UpdateBatteryEstimate ("ReadVoltage");
  double percent = GetBatteryPercent ();
  double voltageSpan = BATTERY_VOLTAGE_MAX_MV - BATTERY_VOLTAGE_MIN_MV;
  double voltageMv = BATTERY_VOLTAGE_MIN_MV + (voltageSpan * (percent / 100.0));
  voltageMv = std::clamp (voltageMv, BATTERY_VOLTAGE_MIN_MV, BATTERY_VOLTAGE_MAX_MV);
  m_currentBatteryMv = static_cast<uint16_t> (voltageMv);
  
  NS_LOG_INFO ("Battery voltage=" << m_currentBatteryMv << "mV (" << percent << "%)");
  
  return m_currentBatteryMv;
}


double
MeshDvApp::GetEnergyConsumptionJoules ()
{
  UpdateBatteryEstimate ("EnergyQuery");
  double consumedMah = BATTERY_CAPACITY_MAH - m_battery.remainingMah;
  double consumedAh = consumedMah / 1000.0;
  double averageVoltage = (BATTERY_VOLTAGE_MAX_MV + BATTERY_VOLTAGE_MIN_MV) / 2000.0; // volts
  double totalConsumed = consumedAh * averageVoltage * 3600.0;
  NS_LOG_INFO ("Energy consumed=" << totalConsumed << "J (consumedMah=" << consumedMah << ")");
  return totalConsumed;
}

void
MeshDvApp::LogTxEvent (uint32_t seq, uint32_t dst, uint8_t ttl, uint8_t hops,
                       int16_t rssi, uint16_t battery, uint16_t score, bool ok)
{
  if (g_metricsCollector)
  {
    g_metricsCollector->RecordTx (GetNode ()->GetId (), seq, dst, ttl, hops, 
                                  rssi, battery, score, ok);
  }
}

void
MeshDvApp::LogRxEvent (uint32_t src, uint32_t dst, uint32_t seq, uint8_t ttl,
                       uint8_t hops, int16_t rssi, uint16_t battery, uint16_t score, bool forwarded)
{
  if (g_metricsCollector)
  {
    g_metricsCollector->RecordRx (GetNode ()->GetId (), src, dst, seq, ttl, hops,
                                  rssi, battery, score, forwarded);
  }
}

// Duty Cycle Management
// ========================================================================

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
MeshDvApp::CleanOldTxHistory ()
{
  Time now = Simulator::Now ();
  Time threshold = now - m_dutyCycleWindow;
  
  // Eliminar transmisiones antiguas fuera de la ventana
  while (!m_txHistory.empty () && m_txHistory.front ().first < threshold)
  {
    m_txHistory.pop_front ();
  }
}

double
MeshDvApp::GetCurrentDutyCycle ()
{
  CleanOldTxHistory ();
  
  if (m_txHistory.empty ())
    return 0.0;
  
  // Sumar duración de todas las transmisiones en la ventana
  Time totalTxTime = Seconds (0);
  for (const auto& tx : m_txHistory)
  {
    totalTxTime += tx.second;  // duración
  }
  
  // Duty Cycle = tiempo_tx / ventana
  double dutyCycle = totalTxTime.GetSeconds () / m_dutyCycleWindow.GetSeconds ();
  
  return dutyCycle;
}

bool
MeshDvApp::CanTransmit (uint32_t toaUs)
{
  CleanOldTxHistory ();
  
  Time txDuration = MicroSeconds (toaUs);
  double currentDC = GetCurrentDutyCycle ();
  double projectedDC = currentDC + (txDuration.GetSeconds () / m_dutyCycleWindow.GetSeconds ());
  
  bool canTx = (projectedDC <= m_dutyCycleLimit);
  
  if (!canTx)
  {
    NS_LOG_WARN ("Node " << GetNode ()->GetId () 
                 << " DUTY CYCLE EXCEEDED: current=" << (currentDC * 100) << "% "
                 << "projected=" << (projectedDC * 100) << "% "
                 << "limit=" << (m_dutyCycleLimit * 100) << "%");
  }
  
  return canTx;
}

void
MeshDvApp::RecordTransmission (uint32_t toaUs)
{
  Time now = Simulator::Now ();
  Time duration = MicroSeconds (toaUs);
  
  m_txHistory.push_back (std::make_pair (now, duration));
  
  NS_LOG_DEBUG ("Node " << GetNode ()->GetId () 
                << " TX recorded: " << (duration.GetMilliSeconds ()) << "ms, "
                << "DutyCycle=" << (GetCurrentDutyCycle () * 100) << "%");
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
  // Verificar si hay ruta al destino
  RouteEntry* route = GetRoute (dst);
  if (!route)
  {
    NS_LOG_WARN ("Node " << GetNode ()->GetId () 
                 << " DATA DROP: No route to dst=" << dst);
    m_dataPacketsGenerated++;
    return;
  }

  // Crear paquete de datos
  MeshMetricTag dataTag;
  dataTag.SetSrc (GetNode ()->GetId ());
  dataTag.SetDst (dst);  // Destino final (GW)
  dataTag.SetSeq (++m_dataSeqPerNode);
  dataTag.SetTtl (m_initTtl);
  dataTag.SetHops (0);
  uint8_t sfForRoute = route->sf ? route->sf : SelectSfForRssi (GetRealRSSI ());
  dataTag.SetSf (sfForRoute);

  const uint32_t toaUs = ComputeLoRaToAUs (sfForRoute, m_bw, m_cr, m_dataPayloadSize);
  dataTag.SetToaUs (toaUs);
  
  int16_t realRSSI = GetRealRSSI ();
  uint16_t realBatt = GetCurrentBatteryMv ();
  
  dataTag.SetRssiDbm (realRSSI);
  dataTag.SetBatt_mV (realBatt);
  dataTag.SetScoreX100 (ComputeScoreX100 (dataTag));

  Ptr<Packet> p = Create<Packet> (m_dataPayloadSize);
  p->AddPacketTag (dataTag);

  NS_LOG_INFO ("DATA src=" << dataTag.GetSrc () 
               << " dst=" << dataTag.GetDst () 
               << " seq=" << dataTag.GetSeq ()
               << " nextHop=" << route->nextHop  // ← USAR NEXT-HOP
               << " toaUs=" << dataTag.GetToaUs ()
               << " sf=" << unsigned (dataTag.GetSf ()));

  LogTxEvent (dataTag.GetSeq (), dataTag.GetDst (), dataTag.GetTtl (),
              dataTag.GetHops (), dataTag.GetRssiDbm (), dataTag.GetBatt_mV (),
              dataTag.GetScoreX100 (), true);

  m_dataPacketsGenerated++;
  
  // Verificar duty cycle
  if (!CanTransmit (toaUs))
  {
    NS_LOG_WARN ("Node " << GetNode ()->GetId () 
                 << " DATA DROP: Duty cycle exceeded");
    return;
  }

  // Los nodos intermedios deben capturar y forward según dst
  // ========================================================================
  NS_LOG_UNCOND("Enviando con kProtoMesh=" << kProtoMesh);
  Address dstAddr = (route->nextHopMac != Mac48Address())
                        ? Address (route->nextHopMac)
                        : ResolveNextHopAddress (route->nextHop);
  SendWithCSMA (p, dataTag, dstAddr);
}

} // namespace ns3
