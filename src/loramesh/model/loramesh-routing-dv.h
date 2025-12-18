#ifndef LORAMESH_ROUTING_DV_H
#define LORAMESH_ROUTING_DV_H

#include "ns3/object.h"
#include "ns3/nstime.h"
#include "ns3/mac48-address.h"
#include "ns3/callback.h"

#include <map>
#include <vector>

namespace ns3
{

using NodeId = uint32_t;

namespace loramesh
{

struct RouteEntry
{
  NodeId destination {0};
  NodeId nextHop {0};
  uint32_t seqNum {0};
  uint8_t hops {0};
  uint8_t sf {0};
  uint32_t toaUs {0};
  int16_t rssiDbm {0};
  uint16_t batt_mV {0};
  uint16_t scoreX100 {0};
  Time lastUpdate { Seconds (0) };
  Time expiryTime { Seconds (0) };
  Mac48Address nextHopMac;
};

struct RouteAnnouncement
{
  NodeId destination {0};
  uint8_t hops {0};
  uint8_t sf {0};
  uint16_t scoreX100 {0};
  uint16_t batt_mV {0};
  int16_t rssiDbm {0};
};

struct DvEntry
{
  NodeId destination {0};
  uint8_t hops {0};
  uint8_t sf {0};
  uint16_t scoreX100 {0};
  uint32_t toaUs {0};
  int16_t rssiDbm {0};
  uint16_t batt_mV {0};
};

struct DvMessage
{
  NodeId origin {0};
  uint32_t sequence {0};
  std::vector<DvEntry> entries;
};

struct NeighborLinkInfo
{
  NodeId neighbor {0};
  uint32_t sequence {0};
  uint8_t hops {0};
  uint8_t sf {0};
  uint32_t toaUs {0};
  int16_t rssiDbm {0};
  uint16_t batt_mV {0};
  uint16_t scoreX100 {0};
  Mac48Address mac;
};

class RoutingDv : public Object
{
public:
  using RouteChangeCallback = Callback<void, const RouteEntry&, const std::string&>;
  using FloodCallback = Callback<void, const DvMessage&>;

  static TypeId GetTypeId ();

  RoutingDv ();
  ~RoutingDv () override = default;

  void SetNodeId (NodeId id);
  void SetRouteTimeout (Time timeout);
  void SetInitTtl (uint8_t ttl);
  void SetMaxRoutes (uint32_t maxRoutes);
  void SetSequence (uint32_t seq);
  void SetExpireWindow (Time expire);
  void SetMaxHops (uint8_t maxHops);
  void SetRouteChangeCallback (RouteChangeCallback cb);
  void SetFloodCallback (FloodCallback cb);

  bool HasRoute (NodeId dest) const;
  NodeId LookupNextHop (NodeId dest) const;
  double GetRouteCost (NodeId dest) const;
  const RouteEntry* GetRoute (NodeId dest) const;
  std::size_t GetRouteCount () const;

  void UpdateFromDvMsg (const DvMessage& msg, const NeighborLinkInfo& link);
  void FloodDvUpdate ();
  void PrintRoutingTable () const;
  void PrintRouteTo (NodeId dest) const;
  void PurgeExpiredRoutes ();

  std::vector<RouteAnnouncement> GetBestRoutes (uint32_t maxRoutes) const;

private:
  void UpdateRoute (const RouteEntry& candidate);
  RouteEntry* LookupRouteMutable (NodeId dest);
  bool IsExpired (const RouteEntry& entry) const;
  void NotifyChange (const RouteEntry& entry, const std::string& action) const;
  DvMessage BuildDvMessage () const;

  NodeId m_nodeId {0};
  Time m_routeTimeout { Seconds (180) };
  Time m_expireWindow { Seconds (180) };
  uint8_t m_maxHops { 7 };
  uint8_t m_initTtl { 10 };
  uint32_t m_maxRoutes { 25 };
  uint32_t m_sequence { 0 };
  RouteChangeCallback m_routeChangeCallback;
  FloodCallback m_floodCallback;
  std::map<NodeId, RouteEntry> m_routes;
};

} // namespace loramesh
} // namespace ns3

#endif /* LORAMESH_ROUTING_DV_H */
