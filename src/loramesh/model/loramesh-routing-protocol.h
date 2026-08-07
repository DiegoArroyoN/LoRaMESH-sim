#ifndef LORAMESH_ROUTING_PROTOCOL_H
#define LORAMESH_ROUTING_PROTOCOL_H

#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/nstime.h"
#include "ns3/net-device.h"
#include "ns3/packet.h"
#include "ns3/random-variable-stream.h"
#include "ns3/traced-callback.h"
#include "mesh-metric-tag.h"
#include "loramesh-energy-model.h"
#include "loramesh-tracer.h"
#include <map>
#include <queue>
#include <deque>

namespace ns3 {

struct RouteEntry {
  uint32_t destination;
  uint32_t nextHop;
  uint32_t seqNum;
  uint8_t  hops;
  uint8_t  sf;
  uint32_t toaUs;
  int16_t  rssiDbm;
  uint16_t batt_mV;
  uint16_t scoreX100;
  Time lastUpdate;
  Time expiryTime;
  
  RouteEntry() 
    : destination(0), nextHop(0), seqNum(0), hops(0), 
      sf(7), toaUs(0), rssiDbm(-120), batt_mV(0), scoreX100(0) {}
};

struct TxQueueEntry {
  Ptr<Packet> packet;
  MeshMetricTag tag;
  uint32_t retries;
};

class LorameshRoutingProtocol : public Application
{
public:
  static TypeId GetTypeId ();
  LorameshRoutingProtocol ();
  ~LorameshRoutingProtocol () override;

  void SetEnergyModel (Ptr<LorameshEnergyModel> model);
  Ptr<LorameshEnergyModel> GetEnergyModel () const;

  void SetTracer (Ptr<LorameshTracer> tracer);

  // Attributes
  void SetBeaconInterval (Time t);
  void SetDataGenerationPeriod (Time t);
  void SetWeights (double toa, double hops, double rssi, double batt);

protected:
  void StartApplication () override;
  void StopApplication () override;

private:
  // Logic
  void BeaconTick ();
  void GenerateDataTraffic ();
  void SendDataPacket (uint32_t dst);
  void BuildAndSendBeacon ();
  
  // L2 Handling
  bool L2Receive (Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address &from);
  void ForwardWithTtl (Ptr<const Packet> pIn, const MeshMetricTag& inTag);
  
  // CSMA/CA
  void SendWithCSMA (Ptr<Packet> packet, const MeshMetricTag& tag);
  void ProcessTxQueue ();
  bool PerformCAD ();
  void OnBackoffTimer ();
  void OnPacketTransmitted (uint32_t toaUs);

  // Routing
  void UpdateRoutingTable(uint32_t dst, uint32_t viaNode,
                          uint32_t seqNum, uint16_t scoreX100,
                          uint8_t hops, uint8_t sf, uint32_t toaUs,
                          int16_t rssiDbm, uint16_t batt_mV);
  void PurgeExpiredRoutes ();
  RouteEntry* GetRoute (uint32_t destination);
  
  // Metrics
  uint16_t ComputeScoreX100 (const MeshMetricTag& t) const;
  uint32_t ComputeLoRaToAUs (uint8_t sf, uint32_t bw, uint8_t cr, uint32_t pl) const;

  // Helpers
  int16_t GetRealRSSI ();
  uint16_t GetCurrentBatteryMv ();
  bool CanTransmit (uint32_t toaUs);
  uint32_t GetBeaconRouteCapacity () const;

  // Members
  Ptr<LorameshEnergyModel> m_energyModel;
  Ptr<LorameshTracer> m_tracer;
  Ptr<UniformRandomVariable> m_rng;

  // Configuration
  Time m_beaconInterval;
  Time m_dataGenerationPeriod;
  Time m_routeTimeout;
  uint8_t m_initTtl;
  uint32_t m_gatewayId; // If we want to target a specific GW, or just use 0/1/2 etc.
                        // Plan says "identification del nodo que funcionará como receptor final"
                        // We can make this an attribute.

  // Weights
  double m_weightToa;
  double m_weightHops;
  double m_weightRssi;
  double m_weightBatt;

  // State
  std::map<uint32_t, RouteEntry> m_routingTable;
  std::map<std::pair<uint32_t, uint32_t>, Time> m_seenPackets;
  std::queue<TxQueueEntry> m_txQueue;
  
  // CSMA
  bool m_txBusy;
  EventId m_backoffEvt;
  uint8_t m_difsCadCount;
  Time m_cadDuration;
  uint8_t m_backoffWindow;
  uint8_t m_maxRetries;

  // Events
  EventId m_beaconEvt;
  EventId m_dataEvt;
  EventId m_purgeEvt;
  EventId m_txEndEvt;

  // Sequence numbers
  uint32_t m_seq;
  uint32_t m_dataSeq;

  // PHY Params (mirrored from device/helper for ToA calc)
  uint8_t m_sf;
  uint8_t m_sfControl;
  uint32_t m_bw;
  uint8_t m_cr;
  bool m_crc;
  bool m_ih;
  bool m_de;

  // Duty Cycle
  std::deque<std::pair<Time, Time>> m_txHistory;
  Time m_dutyCycleWindow;
  double m_dutyCycleLimit;

  static constexpr uint16_t kProtoMesh = 0x88B5;
};

} // namespace ns3

#endif // LORAMESH_ROUTING_PROTOCOL_H
