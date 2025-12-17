#include "loramesh-routing-dv.h"

#include "ns3/log.h"
#include "ns3/simulator.h"

#include <algorithm>

namespace ns3
{
namespace loramesh
{

NS_LOG_COMPONENT_DEFINE ("LoraMeshRoutingDv");
NS_OBJECT_ENSURE_REGISTERED (RoutingDv);

TypeId
RoutingDv::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::loramesh::RoutingDv")
    .SetParent<Object> ()
    .AddConstructor<RoutingDv> ();
  return tid;
}

RoutingDv::RoutingDv ()
{
}

void
RoutingDv::SetNodeId (NodeId id)
{
  m_nodeId = id;
}

void
RoutingDv::SetRouteTimeout (Time timeout)
{
  m_routeTimeout = timeout;
}

void
RoutingDv::SetInitTtl (uint8_t ttl)
{
  m_initTtl = ttl;
}

void
RoutingDv::SetMaxRoutes (uint32_t maxRoutes)
{
  m_maxRoutes = maxRoutes;
}

void
RoutingDv::SetSequence (uint32_t seq)
{
  m_sequence = seq;
}

void
RoutingDv::SetRouteChangeCallback (RouteChangeCallback cb)
{
  m_routeChangeCallback = cb;
}

void
RoutingDv::SetFloodCallback (FloodCallback cb)
{
  m_floodCallback = cb;
}

bool
RoutingDv::HasRoute (NodeId dest) const
{
  const RouteEntry* route = GetRoute (dest);
  return route != nullptr;
}

NodeId
RoutingDv::LookupNextHop (NodeId dest) const
{
  const RouteEntry* route = GetRoute (dest);
  return route ? route->nextHop : 0;
}

double
RoutingDv::GetRouteCost (NodeId dest) const
{
  const RouteEntry* route = GetRoute (dest);
  if (!route)
    {
      return 1.0;
    }
  return 1.0 - (route->scoreX100 / 100.0);
}

const RouteEntry*
RoutingDv::GetRoute (NodeId dest) const
{
  auto it = m_routes.find (dest);
  if (it == m_routes.end ())
    {
      return nullptr;
    }
  if (IsExpired (it->second))
    {
      return nullptr;
    }
  return &it->second;
}

std::size_t
RoutingDv::GetRouteCount () const
{
  return m_routes.size ();
}

void
RoutingDv::UpdateFromDvMsg (const DvMessage& msg, const NeighborLinkInfo& link)
{
  if (msg.origin == m_nodeId)
    {
      return;
    }

  RouteEntry direct;
  direct.destination = link.neighbor;
  direct.nextHop = link.neighbor;
  direct.seqNum = link.sequence;
  direct.hops = std::min<uint8_t> (link.hops, m_initTtl);
  direct.sf = link.sf;
  direct.toaUs = link.toaUs;
  direct.rssiDbm = link.rssiDbm;
  direct.batt_mV = link.batt_mV;
  direct.scoreX100 = link.scoreX100;
  direct.lastUpdate = Simulator::Now ();
  direct.expiryTime = direct.lastUpdate + m_routeTimeout;
  direct.nextHopMac = link.mac;
  UpdateRoute (direct);

  for (const auto& entry : msg.entries)
    {
      if (entry.destination == m_nodeId)
        {
          continue;
        }

      RouteEntry candidate;
      candidate.destination = entry.destination;
      candidate.nextHop = msg.origin;
      candidate.seqNum = msg.sequence;
      candidate.hops = std::min<uint8_t> (entry.hops + 1, m_initTtl);
      candidate.sf = entry.sf;
      candidate.toaUs = entry.toaUs;
      candidate.rssiDbm = entry.rssiDbm;
      candidate.batt_mV = entry.batt_mV;
      candidate.scoreX100 = entry.scoreX100;
      candidate.lastUpdate = Simulator::Now ();
      candidate.expiryTime = candidate.lastUpdate + m_routeTimeout;
      candidate.nextHopMac = link.mac;
      UpdateRoute (candidate);
    }
}

void
RoutingDv::FloodDvUpdate ()
{
  if (m_floodCallback.IsNull ())
    {
      return;
    }
  m_floodCallback (BuildDvMessage ());
}

void
RoutingDv::PrintRoutingTable () const
{
  if (m_routes.empty ())
    {
      NS_LOG_INFO ("RoutingDv: table empty for node " << m_nodeId);
      return;
    }

  for (const auto& kv : m_routes)
    {
      const RouteEntry& e = kv.second;
      NS_LOG_INFO ("  dst=" << e.destination << " via=" << e.nextHop
                   << " hops=" << unsigned (e.hops)
                   << " sf=" << unsigned (e.sf)
                   << " score=" << e.scoreX100
                   << " seq=" << e.seqNum
                   << " age=" << (Simulator::Now () - e.lastUpdate).GetSeconds () << "s");
    }
}

void
RoutingDv::PurgeExpiredRoutes ()
{
  Time now = Simulator::Now ();
  for (auto it = m_routes.begin (); it != m_routes.end (); )
    {
      if (now > it->second.expiryTime)
        {
          NotifyChange (it->second, "PURGE");
          it = m_routes.erase (it);
        }
      else
        {
          ++it;
        }
    }
}

std::vector<RouteAnnouncement>
RoutingDv::GetBestRoutes (uint32_t maxRoutes) const
{
  std::vector<std::pair<uint16_t, const RouteEntry*>> ranked;
  for (const auto& kv : m_routes)
    {
      if (IsExpired (kv.second))
        {
          continue;
        }
      ranked.emplace_back (kv.second.scoreX100, &kv.second);
    }

  std::sort (ranked.begin (), ranked.end (),
             [] (const auto& a, const auto& b) {
               if (a.first != b.first)
                 {
                   return a.first > b.first;
                 }
               return a.second->destination < b.second->destination;
             });

  std::vector<RouteAnnouncement> announcements;
  const uint32_t limit = std::min<uint32_t> (maxRoutes, ranked.size ());
  for (uint32_t i = 0; i < limit; ++i)
    {
      const RouteEntry* entry = ranked[i].second;
      RouteAnnouncement ann;
      ann.destination = entry->destination;
      ann.hops = entry->hops;
      ann.sf = entry->sf;
      ann.scoreX100 = entry->scoreX100;
      ann.batt_mV = entry->batt_mV;
      ann.rssiDbm = entry->rssiDbm;
      announcements.push_back (ann);
    }
  return announcements;
}

void
RoutingDv::UpdateRoute (const RouteEntry& candidate)
{
  if (candidate.destination == m_nodeId)
    {
      return;
    }

  auto it = m_routes.find (candidate.destination);
  bool shouldUpdate = false;
  std::string action = "NONE";

  if (it == m_routes.end ())
    {
      shouldUpdate = true;
      action = "NEW";
    }
  else
    {
      RouteEntry& oldEntry = it->second;
      if (candidate.seqNum > oldEntry.seqNum)
        {
          shouldUpdate = true;
          action = "UPDATE";
        }
      else if (candidate.seqNum == oldEntry.seqNum)
        {
          if (candidate.scoreX100 > oldEntry.scoreX100)
            {
              shouldUpdate = true;
              action = "UPDATE";
            }
          else if (candidate.scoreX100 == oldEntry.scoreX100 &&
                   candidate.hops < oldEntry.hops)
            {
              shouldUpdate = true;
              action = "UPDATE";
            }
          else if (candidate.scoreX100 == oldEntry.scoreX100 &&
                   candidate.hops == oldEntry.hops &&
                   candidate.sf < oldEntry.sf)
            {
              shouldUpdate = true;
              action = "UPDATE";
            }
        }
    }

  if (shouldUpdate)
    {
      m_routes[candidate.destination] = candidate;
      NotifyChange (candidate, action);
    }
}

RouteEntry*
RoutingDv::LookupRouteMutable (NodeId dest)
{
  auto it = m_routes.find (dest);
  if (it == m_routes.end ())
    {
      return nullptr;
    }
  if (IsExpired (it->second))
    {
      return nullptr;
    }
  return &it->second;
}

bool
RoutingDv::IsExpired (const RouteEntry& entry) const
{
  return Simulator::Now () > entry.expiryTime;
}

void
RoutingDv::NotifyChange (const RouteEntry& entry, const std::string& action) const
{
  if (!m_routeChangeCallback.IsNull ())
    {
      m_routeChangeCallback (entry, action);
    }
}

DvMessage
RoutingDv::BuildDvMessage () const
{
  DvMessage msg;
  msg.origin = m_nodeId;
  msg.sequence = m_sequence;
  msg.entries.reserve (m_maxRoutes);
  auto announcements = GetBestRoutes (m_maxRoutes);
  for (const auto& ann : announcements)
    {
      DvEntry entry;
      entry.destination = ann.destination;
      entry.hops = ann.hops;
      entry.sf = ann.sf;
      entry.scoreX100 = ann.scoreX100;
      entry.rssiDbm = ann.rssiDbm;
      entry.batt_mV = ann.batt_mV;
      entry.toaUs = 0;
      msg.entries.push_back (entry);
    }
  return msg;
}

} // namespace loramesh
} // namespace ns3
