#include "loramesh-routing-protocol.h"
#include "ns3/log.h"
#include "ns3/simulator.h"
#include "ns3/node.h"
#include "ns3/double.h"
#include "ns3/uinteger.h"
#include "ns3/pointer.h"
#include <algorithm>
#include <cmath>

namespace ns3 {

NS_LOG_COMPONENT_DEFINE ("LorameshRoutingProtocol");
NS_OBJECT_ENSURE_REGISTERED (LorameshRoutingProtocol);

TypeId
LorameshRoutingProtocol::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::LorameshRoutingProtocol")
    .SetParent<Application> ()
    .SetGroupName ("Loramesh")
    .AddConstructor<LorameshRoutingProtocol> ()
    .AddAttribute ("BeaconInterval", "Interval between DV beacons",
                   TimeValue (Seconds (60)),
                   MakeTimeAccessor (&LorameshRoutingProtocol::m_beaconInterval),
                   MakeTimeChecker ())
    .AddAttribute ("DataGenerationPeriod", "Interval between data packets",
                   TimeValue (Seconds (600)),
                   MakeTimeAccessor (&LorameshRoutingProtocol::m_dataGenerationPeriod),
                   MakeTimeChecker ())
    .AddAttribute ("GatewayId", "Node ID of the gateway (sink)",
                   UintegerValue (0),
                   MakeUintegerAccessor (&LorameshRoutingProtocol::m_gatewayId),
                   MakeUintegerChecker<uint32_t> ())
    .AddAttribute ("InitTtl", "Initial TTL for packets",
                   UintegerValue (10),
                   MakeUintegerAccessor (&LorameshRoutingProtocol::m_initTtl),
                   MakeUintegerChecker<uint8_t> ())
    .AddAttribute ("WeightToa", "Weight for ToA metric",
                   DoubleValue (0.4),
                   MakeDoubleAccessor (&LorameshRoutingProtocol::m_weightToa),
                   MakeDoubleChecker<double> ())
    .AddAttribute ("WeightHops", "Weight for Hops metric",
                   DoubleValue (0.2),
                   MakeDoubleAccessor (&LorameshRoutingProtocol::m_weightHops),
                   MakeDoubleChecker<double> ())
    .AddAttribute ("WeightRssi", "Weight for RSSI metric",
                   DoubleValue (0.2),
                   MakeDoubleAccessor (&LorameshRoutingProtocol::m_weightRssi),
                   MakeDoubleChecker<double> ())
    .AddAttribute ("WeightBatt", "Weight for Battery metric",
                   DoubleValue (0.2),
                   MakeDoubleAccessor (&LorameshRoutingProtocol::m_weightBatt),
                   MakeDoubleChecker<double> ());
  return tid;
}

LorameshRoutingProtocol::LorameshRoutingProtocol ()
  : m_beaconInterval (Seconds (60)),
    m_dataGenerationPeriod (Seconds (600)),
    m_routeTimeout (Seconds (180)),
    m_initTtl (10),
    m_gatewayId (0),
    m_weightToa (0.4),
    m_weightHops (0.2),
    m_weightRssi (0.2),
    m_weightBatt (0.2),
    m_txBusy (false),
    m_difsCadCount (3),
    m_cadDuration (MilliSeconds (5.5)), // Typical LoRa CAD
    m_backoffWindow (8),
    m_maxRetries (5),
    m_seq (0),
    m_dataSeq (0),
    m_sf (7),
    m_sfControl (12),
    m_bw (125000),
    m_cr (1),
    m_crc (true),
    m_ih (false),
    m_de (false),
    m_dutyCycleWindow (Hours (1)),
    m_dutyCycleLimit (0.01) // 1%
{
  m_rng = CreateObject<UniformRandomVariable> ();
}

LorameshRoutingProtocol::~LorameshRoutingProtocol ()
{
}

void
LorameshRoutingProtocol::SetEnergyModel (Ptr<LorameshEnergyModel> model)
{
  m_energyModel = model;
}

Ptr<LorameshEnergyModel>
LorameshRoutingProtocol::GetEnergyModel () const
{
  return m_energyModel;
}

void
LorameshRoutingProtocol::SetTracer (Ptr<LorameshTracer> tracer)
{
  m_tracer = tracer;
}

void
LorameshRoutingProtocol::StartApplication ()
{
  NS_LOG_FUNCTION (this);
  
  // Register receive callback on all NetDevices
  Ptr<Node> node = GetNode ();
  for (uint32_t i = 0; i < node->GetNDevices (); ++i)
    {
      Ptr<NetDevice> dev = node->GetDevice (i);
      dev->SetReceiveCallback (MakeCallback (&LorameshRoutingProtocol::L2Receive, this));
    }

  // Schedule initial beacon with jitter
  Time jitter = Seconds (m_rng->GetValue (0.0, 5.0));
  m_beaconEvt = Simulator::Schedule (Seconds (1.0) + jitter, &LorameshRoutingProtocol::BeaconTick, this);

  // Schedule data generation if NOT gateway (or even if gateway, but usually GW doesn't send data to itself)
  if (node->GetId () != m_gatewayId)
    {
      Time dataStart = Seconds (10.0) + jitter;
      m_dataEvt = Simulator::Schedule (dataStart, &LorameshRoutingProtocol::GenerateDataTraffic, this);
    }

  m_purgeEvt = Simulator::Schedule (Seconds (30), &LorameshRoutingProtocol::PurgeExpiredRoutes, this);
}

void
LorameshRoutingProtocol::StopApplication ()
{
  NS_LOG_FUNCTION (this);
  Simulator::Cancel (m_beaconEvt);
  Simulator::Cancel (m_dataEvt);
  Simulator::Cancel (m_purgeEvt);
  Simulator::Cancel (m_backoffEvt);
}

// ============================================================================
// LOGIC
// ============================================================================

void
LorameshRoutingProtocol::BeaconTick ()
{
  BuildAndSendBeacon ();
  // Schedule next
  Time next = m_beaconInterval + Seconds (m_rng->GetValue (-1.0, 1.0)); // Small jitter
  m_beaconEvt = Simulator::Schedule (next, &LorameshRoutingProtocol::BeaconTick, this);
}

void
LorameshRoutingProtocol::GenerateDataTraffic ()
{
  if (GetNode ()->GetId () != m_gatewayId)
    {
      SendDataPacket (m_gatewayId);
    }
  
  // Schedule next
  Time next = m_dataGenerationPeriod + Seconds (m_rng->GetValue (-1.0, 1.0));
  m_dataEvt = Simulator::Schedule (next, &LorameshRoutingProtocol::GenerateDataTraffic, this);
}

void
LorameshRoutingProtocol::BuildAndSendBeacon ()
{
  MeshMetricTag tag;
  tag.SetSrc (GetNode ()->GetId ());
  tag.SetDst (0xFFFF); // Broadcast
  tag.SetSeq (++m_seq);
  tag.SetTtl (1); // Beacons only go 1 hop (neighbors) - Wait, DV beacons usually propagate? 
                  // In this implementation, we send our table to neighbors. 
                  // Neighbors update their table and send their own beacons.
                  // So TTL=1 is correct for the *packet*, but the *info* propagates.
  tag.SetHops (0);
  tag.SetSf (m_sfControl);
  tag.SetRssiDbm (GetRealRSSI ());
  tag.SetBatt_mV (GetCurrentBatteryMv ());

  // Payload: Best routes
  std::vector<MeshMetricTag::RoutePayloadEntry> payload;
  uint32_t maxRoutes = GetBeaconRouteCapacity ();
  
  // Sort routes by score
  std::vector<std::pair<uint16_t, uint32_t>> sortedRoutes;
  for (const auto& kv : m_routingTable)
    {
      if (kv.first == GetNode ()->GetId ()) continue;
      sortedRoutes.push_back ({kv.second.scoreX100, kv.first});
    }
  std::sort (sortedRoutes.rbegin (), sortedRoutes.rend ()); // Descending

  for (const auto& item : sortedRoutes)
    {
      if (payload.size () >= maxRoutes) break;
      const RouteEntry& re = m_routingTable[item.second];
      MeshMetricTag::RoutePayloadEntry e;
      e.dst = re.destination;
      e.hops = re.hops;
      e.sf = re.sf;
      e.score = re.scoreX100;
      e.batt_mV = re.batt_mV;
      e.rssi_dBm = re.rssiDbm;
      payload.push_back (e);
    }

  size_t len = payload.size () * sizeof (MeshMetricTag::RoutePayloadEntry);
  Ptr<Packet> p;
  if (len == 0) p = Create<Packet> (1); // Dummy byte
  else p = Create<Packet> ((uint8_t*)payload.data (), len);

  uint32_t toa = ComputeLoRaToAUs (m_sfControl, m_bw, m_cr, p->GetSize ());
  tag.SetToaUs (toa);
  tag.SetScoreX100 (ComputeScoreX100 (tag));

  p->AddPacketTag (tag);
  
  if (m_tracer) 
    m_tracer->TraceTx (GetNode ()->GetId (), m_seq, 0xFFFF, 1, 
                       tag.GetRssiDbm (), tag.GetBatt_mV (), tag.GetScoreX100 (), true);

  SendWithCSMA (p, tag);
}

void
LorameshRoutingProtocol::SendDataPacket (uint32_t dst)
{
  RouteEntry* route = GetRoute (dst);
  if (!route)
    {
      NS_LOG_WARN ("No route to " << dst << ", dropping data packet");
      return;
    }

  MeshMetricTag tag;
  tag.SetSrc (GetNode ()->GetId ());
  tag.SetDst (dst);
  tag.SetSeq (++m_dataSeq);
  tag.SetTtl (m_initTtl);
  tag.SetHops (0);
  tag.SetSf (m_sf); // Data SF
  tag.SetRssiDbm (GetRealRSSI ());
  tag.SetBatt_mV (GetCurrentBatteryMv ());
  
  Ptr<Packet> p = Create<Packet> (20); // 20 bytes payload
  uint32_t toa = ComputeLoRaToAUs (m_sf, m_bw, m_cr, 20);
  tag.SetToaUs (toa);
  tag.SetScoreX100 (ComputeScoreX100 (tag));

  p->AddPacketTag (tag);

  if (m_tracer)
    m_tracer->TraceTx (GetNode ()->GetId (), m_dataSeq, dst, m_initTtl,
                       tag.GetRssiDbm (), tag.GetBatt_mV (), tag.GetScoreX100 (), true);

  SendWithCSMA (p, tag);
}

// ============================================================================
// L2 RECEIVE
// ============================================================================

bool
LorameshRoutingProtocol::L2Receive (Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address &from)
{
  if (proto != kProtoMesh) return false;

  MeshMetricTag tag;
  if (!p->PeekPacketTag (tag)) return true;

  uint32_t myId = GetNode ()->GetId ();
  uint32_t src = tag.GetSrc ();
  uint32_t dst = tag.GetDst ();

  // Energy: RX
  if (m_energyModel)
    {
      m_energyModel->ChangeState (LorameshEnergyModel::RX);
      // Ideally we should know duration, but here we just switch state.
      // We need to switch back to IDLE/SLEEP after packet duration.
      // For simplicity, we assume RX happens instantaneously in state model or we schedule end.
      // Better: The PHY should handle state changes. But we are in App.
      // Let's just charge a fixed cost or rely on PHY if we had one.
      // Since we use a custom NetDevice, we can simulate RX duration here.
      uint32_t durationUs = tag.GetToaUs ();
      Simulator::Schedule (MicroSeconds (durationUs), &LorameshEnergyModel::ChangeState, m_energyModel, LorameshEnergyModel::IDLE);
    }

  // 1. Beacon (Broadcast)
  if (dst == 0xFFFF)
    {
      // Update route to neighbor
      UpdateRoutingTable (src, src, tag.GetSeq (), tag.GetScoreX100 (), 
                          tag.GetHops (), tag.GetSf (), tag.GetToaUs (), 
                          tag.GetRssiDbm (), tag.GetBatt_mV ());
      
      // Process payload
      size_t len = p->GetSize ();
      size_t entrySize = sizeof (MeshMetricTag::RoutePayloadEntry);
      if (len >= entrySize)
        {
          std::vector<MeshMetricTag::RoutePayloadEntry> entries (len / entrySize);
          p->CopyData ((uint8_t*)entries.data (), len);
          
          for (const auto& e : entries)
            {
              if (e.dst == myId) continue;
              
              // Composite score calculation for multi-hop
              // Simple approach: Average score or Min score?
              // The tag has the neighbor's score. The entry has the neighbor's view of the target.
              // We need to combine them.
              // New Score = (NeighborLinkScore + RouteScore) / 2 ?
              // Or just use the RouteScore penalized by the link?
              
              // Let's use a weighted cost approach implicitly via ComputeScore logic
              // But here we just have scalar scores 0-100.
              // Let's average them for now as in original code.
              uint16_t newScore = (tag.GetScoreX100 () + e.score) / 2;
              
              UpdateRoutingTable (e.dst, src, tag.GetSeq (), newScore,
                                  e.hops + 1, tag.GetSf (), tag.GetToaUs (), // ToA is link ToA? Or path ToA?
                                  e.rssi_dBm, e.batt_mV);
            }
        }
      
      if (m_tracer)
        m_tracer->TraceRx (myId, src, dst, tag.GetSeq (), tag.GetTtl (), 
                           tag.GetHops (), tag.GetRssiDbm (), tag.GetBatt_mV (), 
                           tag.GetScoreX100 (), false);
      return true;
    }

  // 2. Unicast Data
  if (dst == myId)
    {
      NS_LOG_INFO ("Node " << myId << " RECEIVED Data from " << src);
      if (m_tracer)
        m_tracer->TraceRx (myId, src, dst, tag.GetSeq (), tag.GetTtl (), 
                           tag.GetHops (), tag.GetRssiDbm (), tag.GetBatt_mV (), 
                           tag.GetScoreX100 (), false);
      return true;
    }

  // 3. Forwarding
  if (tag.GetTtl () > 1)
    {
      RouteEntry* route = GetRoute (dst);
      if (route && route->nextHop != src) // Split horizon simple check
        {
          if (m_tracer)
            m_tracer->TraceRx (myId, src, dst, tag.GetSeq (), tag.GetTtl (), 
                               tag.GetHops (), tag.GetRssiDbm (), tag.GetBatt_mV (), 
                               tag.GetScoreX100 (), true);
          
          ForwardWithTtl (p, tag);
        }
    }

  return true;
}

void
LorameshRoutingProtocol::ForwardWithTtl (Ptr<const Packet> pIn, const MeshMetricTag& inTag)
{
  // Deduplication
  std::pair<uint32_t, uint32_t> pid = {inTag.GetSrc (), inTag.GetSeq ()};
  if (m_seenPackets.find (pid) != m_seenPackets.end ()) return;
  m_seenPackets[pid] = Simulator::Now ();

  uint32_t dst = inTag.GetDst ();
  RouteEntry* route = GetRoute (dst);
  if (!route) return;

  MeshMetricTag newTag = inTag;
  newTag.SetTtl (inTag.GetTtl () - 1);
  newTag.SetHops (inTag.GetHops () + 1);
  newTag.SetRssiDbm (GetRealRSSI ());
  newTag.SetBatt_mV (GetCurrentBatteryMv ());
  newTag.SetScoreX100 (ComputeScoreX100 (newTag));

  Ptr<Packet> p = pIn->Copy ();
  p->RemoveAllPacketTags ();
  p->AddPacketTag (newTag);

  // Jitter forward to avoid collisions
  Simulator::Schedule (MilliSeconds (10 + m_rng->GetValue (0, 10)), 
                       &LorameshRoutingProtocol::SendWithCSMA, this, p, newTag);
}

// ============================================================================
// ROUTING
// ============================================================================

void
LorameshRoutingProtocol::UpdateRoutingTable (uint32_t dst, uint32_t viaNode,
                                             uint32_t seqNum, uint16_t scoreX100,
                                             uint8_t hops, uint8_t sf, uint32_t toaUs,
                                             int16_t rssiDbm, uint16_t batt_mV)
{
  auto it = m_routingTable.find (dst);
  bool update = false;
  std::string action = "NONE";

  if (it == m_routingTable.end ())
    {
      update = true;
      action = "NEW";
    }
  else
    {
      RouteEntry& e = it->second;
      if (seqNum > e.seqNum) { update = true; action = "UPDATE_SEQ"; }
      else if (seqNum == e.seqNum)
        {
          if (scoreX100 > e.scoreX100) { update = true; action = "UPDATE_SCORE"; }
          else if (scoreX100 == e.scoreX100 && hops < e.hops) { update = true; action = "UPDATE_HOPS"; }
        }
    }

  if (update)
    {
      RouteEntry e;
      e.destination = dst;
      e.nextHop = viaNode;
      e.seqNum = seqNum;
      e.hops = hops;
      e.sf = sf;
      e.toaUs = toaUs;
      e.rssiDbm = rssiDbm;
      e.batt_mV = batt_mV;
      e.scoreX100 = scoreX100;
      e.lastUpdate = Simulator::Now ();
      e.expiryTime = Simulator::Now () + m_routeTimeout;
      
      m_routingTable[dst] = e;
      
      if (m_tracer)
        m_tracer->TraceRouteUpdate (GetNode ()->GetId (), dst, viaNode, hops, scoreX100, seqNum, action);
    }
}

void
LorameshRoutingProtocol::PurgeExpiredRoutes ()
{
  Time now = Simulator::Now ();
  for (auto it = m_routingTable.begin (); it != m_routingTable.end (); )
    {
      if (now > it->second.expiryTime)
        {
          if (m_tracer)
            m_tracer->TraceRouteUpdate (GetNode ()->GetId (), it->first, 0, 0, 0, 0, "EXPIRED");
          it = m_routingTable.erase (it);
        }
      else
        {
          ++it;
        }
    }
  Simulator::Schedule (Seconds (30), &LorameshRoutingProtocol::PurgeExpiredRoutes, this);
}

RouteEntry*
LorameshRoutingProtocol::GetRoute (uint32_t destination)
{
  auto it = m_routingTable.find (destination);
  if (it != m_routingTable.end () && Simulator::Now () <= it->second.expiryTime)
    return &it->second;
  return nullptr;
}

// ============================================================================
// CSMA/CA & PHY
// ============================================================================

void
LorameshRoutingProtocol::SendWithCSMA (Ptr<Packet> packet, const MeshMetricTag& tag)
{
  TxQueueEntry e;
  e.packet = packet;
  e.tag = tag;
  e.retries = 0;
  m_txQueue.push (e);
  
  if (!m_txBusy)
    ProcessTxQueue ();
}

void
LorameshRoutingProtocol::ProcessTxQueue ()
{
  if (m_txQueue.empty ()) return;
  
  m_txBusy = true;
  if (PerformCAD ())
    {
      // Channel busy, backoff
      OnBackoffTimer ();
    }
  else
    {
      // Channel free, transmit
      TxQueueEntry& e = m_txQueue.front ();
      Ptr<NetDevice> dev = GetNode ()->GetDevice (0); // Assume 0 is LoRa
      dev->Send (e.packet, dev->GetBroadcast (), kProtoMesh);
      
      uint32_t toa = e.tag.GetToaUs ();
      
      if (m_energyModel)
        {
          m_energyModel->ChangeState (LorameshEnergyModel::TX);
          Simulator::Schedule (MicroSeconds (toa), &LorameshEnergyModel::ChangeState, m_energyModel, LorameshEnergyModel::IDLE);
        }

      Simulator::Schedule (MicroSeconds (toa), &LorameshRoutingProtocol::OnPacketTransmitted, this, toa);
    }
}

bool
LorameshRoutingProtocol::PerformCAD ()
{
  // Simulate CAD: Check if any other node is transmitting nearby?
  // In simple simulation, we can't easily peek channel state without PHY helper.
  // We will assume channel is free for now or use a random chance based on load?
  // Better: If we had a real PHY, we'd ask it.
  // For now, return false (Channel Free) to allow sending, 
  // OR implement a global "ChannelBusy" oracle if we wanted perfect CSMA.
  // Let's assume false (Free) but rely on collisions at RX side.
  return false; 
}

void
LorameshRoutingProtocol::OnBackoffTimer ()
{
  // Simple random backoff
  Time backoff = MilliSeconds (m_rng->GetValue (10, 100));
  m_backoffEvt = Simulator::Schedule (backoff, &LorameshRoutingProtocol::ProcessTxQueue, this);
}

void
LorameshRoutingProtocol::OnPacketTransmitted (uint32_t toaUs)
{
  m_txBusy = false;
  if (!m_txQueue.empty ())
    {
      m_txQueue.pop (); // Done
      ProcessTxQueue (); // Next
    }
}

// ============================================================================
// METRICS & HELPERS
// ============================================================================

uint16_t
LorameshRoutingProtocol::ComputeScoreX100 (const MeshMetricTag& t) const
{
  double toa_norm = std::min (t.GetToaUs () / 1000000.0, 1.0); // 1s max
  double hop_norm = std::min (t.GetHops () / 10.0, 1.0);
  double rssi_norm = std::clamp ((t.GetRssiDbm () + 120.0) / 60.0, 0.0, 1.0);
  double batt_norm = std::clamp ((t.GetBatt_mV () - 3000.0) / 1200.0, 0.0, 1.0);

  double cost = m_weightToa * toa_norm + 
                m_weightHops * hop_norm + 
                m_weightRssi * (1.0 - rssi_norm) + 
                m_weightBatt * (1.0 - batt_norm);

  double score = std::clamp (1.0 - cost, 0.0, 1.0);
  return static_cast<uint16_t> (std::round (score * 100.0));
}

uint32_t
LorameshRoutingProtocol::ComputeLoRaToAUs (uint8_t sf, uint32_t bw, uint8_t cr, uint32_t pl) const
{
  // Standard LoRa ToA calculation
  double tSym = std::pow (2.0, sf) / bw;
  double tPreamble = (8.0 + 4.25) * tSym;
  double payloadSym = 8.0 + std::max (std::ceil ((8.0 * pl - 4.0 * sf + 28.0 + 16.0) / (4.0 * sf)), 0.0) * (cr + 4.0);
  double tPayload = payloadSym * tSym;
  return static_cast<uint32_t> ((tPreamble + tPayload) * 1e6);
}

int16_t
LorameshRoutingProtocol::GetRealRSSI ()
{
  // Mock RSSI or get from device if supported
  return -80; // Placeholder
}

uint16_t
LorameshRoutingProtocol::GetCurrentBatteryMv ()
{
  if (m_energyModel)
    {
      // Map percentage to voltage 3.0 - 4.2V
      double pct = m_energyModel->GetBatteryPercentage ();
      return static_cast<uint16_t> (3000 + (pct / 100.0) * 1200);
    }
  return 4200;
}

uint32_t
LorameshRoutingProtocol::GetBeaconRouteCapacity () const
{
  return 10; // Fixed for now
}

} // namespace ns3
