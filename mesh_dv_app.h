#pragma once
#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/simulator.h"
#include "ns3/log.h"
#include "ns3/nstime.h"
#include "mesh_metric_tag.h"
#include <cstdint>
#include <map>
#include <queue>
#include <vector>
#include <tuple>
#include <set>
#include <string>
#include "ns3/net-device.h"
#include "ns3/packet.h"
#include "ns3/address.h"
#include "ns3/mac48-address.h"
#include "ns3/random-variable-stream.h"
#include "ns3/loramesh-metric-composite.h"
#include "ns3/loramesh-adr-hopbyhop.h"
#include "ns3/loramesh-mac-csma-cad.h"
#include "ns3/loramesh-routing-dv.h"
#include "ns3/loramesh-energy-model.h"
#include <string>
#include <utility>

namespace ns3 {

struct TxQueueEntry {
  Ptr<Packet> packet;
  MeshMetricTag tag;
  uint32_t retries;
  Address dstAddr;
};

class MeshDvApp : public Application
{
public:
  static TypeId GetTypeId ();
  MeshDvApp ();
  ~MeshDvApp () override;

  void SetPeriod (Time t);
  void SetInitTtl (uint8_t ttl)        { m_initTtl = ttl; }
  void SetInitScoreX100 (uint16_t s)   { m_initScoreX100 = s; }
  void SetCsmaEnabled (bool enabled)   { m_csmaEnabled = enabled; }
  void SetRouteTimeoutFactor (double factor);

  void StartApplication () override;
  void StopApplication () override;

private:
  void Tick ();
  void BuildAndSendDv (uint8_t sf);
  void ForwardWithTtl (Ptr<const Packet> pIn, const MeshMetricTag& inTag);
  bool L2Receive (Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address &from);

  void SendWithCSMA (Ptr<Packet> packet, const MeshMetricTag& tag, Address dstAddr);
  void ProcessTxQueue ();
  void OnBackoffTimer ();
  void OnPacketTransmitted (uint32_t toaUs);
  Address ResolveNextHopAddress (uint32_t nextHopId) const;
  void ScheduleDvCycle (uint8_t sf, EventId& evtSlot);
  Time GetBeaconInterval () const;
  std::string GetBeaconPhaseLabel () const;
  
  void PrintRoutingTable ();
  void PurgeExpiredRoutes ();
  void SendDataToDestination (uint32_t dst, Ptr<Packet> payload);

  uint32_t ComputeLoRaToAUs (uint8_t sf, uint32_t bw, uint8_t cr, uint32_t pl) const;
  uint16_t ComputeScoreX100 (const MeshMetricTag& t) const;

    // Cache de deduplicación
  std::map<std::tuple<uint32_t, uint32_t, uint32_t>, Time> m_seenPackets;  // {src, dst, seq} -> timestamp

  // Métricas reales
  int16_t GetRealRSSI ();
  uint16_t GetBatteryVoltageMv () const;
  double GetRemainingEnergyJ () const;

  // Recolección de métricas - SIMPLIFICADO (solo funciones)
  void LogTxEvent (uint32_t seq, uint32_t dst, uint8_t ttl, uint8_t hops,
                   int16_t rssi, uint16_t battery, uint16_t score, uint8_t sf,
                   double energyJ, double energyFrac, bool ok);
  void LogRxEvent (uint32_t src, uint32_t dst, uint32_t seq, uint8_t ttl,
                   uint8_t hops, int16_t rssi, uint16_t battery, uint16_t score, uint8_t sf,
                   double energyJ, double energyFrac, bool forwarded);

  double ComputeSnrFromRssi (int16_t rssiDbm) const;
  uint8_t SelectSfFromAdr (uint32_t dst, int16_t rssiDbm) const;
  void HandleRouteChange (const loramesh::RouteEntry& entry, const std::string& action);
  void HandleFloodRequest (const loramesh::DvMessage& msg);
  void ProcessDvPayload (Ptr<const Packet> p,
                         const MeshMetricTag& tag,
                         const Mac48Address& fromMac,
                         uint32_t toaUsNeighbor);
  loramesh::NeighborLinkInfo BuildNeighborLinkInfo (const MeshMetricTag& tag,
                                                    uint32_t toaUs,
                                                    Mac48Address fromMac) const;
  std::vector<loramesh::DvEntry> DecodeDvEntries (Ptr<const Packet> p,
                                                  const MeshMetricTag& tag,
                                                  uint32_t toaUsNeighbor) const;

  Time     m_period { Seconds (60) };
  uint8_t  m_initTtl { 10 };
  uint16_t m_initScoreX100 { 100 };

  uint8_t  m_sf  { 9 };          // SF para datos
  uint8_t  m_sfControl { 12 };    // SF robusto para beacons DV
  uint32_t m_bw  { 125000 };
  uint8_t  m_cr  { 1 };
  uint8_t  m_pl  { 20 };
  bool     m_crc { true };
  bool     m_ih  { false };
  bool     m_de  { true };

  uint32_t m_seq { 0 };
  uint32_t m_dataSeq { 0 };
  Time     m_beaconWarmupEnd { Seconds (60) };
  Time     m_beaconIntervalWarm { Seconds (10) };
  Time     m_beaconIntervalStable { Seconds (60) };
  loramesh::CompositeMetric m_compositeMetric;
  loramesh::HopByHopAdr m_adr;
  Ptr<loramesh::EnergyModel> m_energyModel;
  Ptr<loramesh::RoutingDv> m_routing;

  bool m_csmaEnabled { true };
  bool m_txBusy { false };
  uint32_t m_txCount { 0 };
  uint32_t m_backoffCount { 0 };
  
  uint8_t m_difsCadCount { 3 };
  Time m_cadDuration { MilliSeconds (5.5) };
  uint8_t m_backoffWindow { 8 };
  uint8_t m_maxRetries { 5 };
  
  std::queue<TxQueueEntry> m_txQueue;
  Ptr<UniformRandomVariable> m_rng;
  
  std::map<uint32_t, Mac48Address> m_macTable; // id lógico -> MAC conocida
  Time m_routeTimeout { Seconds (300) };

  static constexpr uint16_t kProtoMesh = 0x88B5;
 
  Ptr<loramesh::CsmaCadMac> m_mac;
  void CleanOldSeenPackets ();
  void UpdateRouteTimeout ();
  uint32_t GetBeaconRouteCapacity () const;

  // NUEVO: Data Traffic Generation
  // ========================================================================
  EventId m_dataGenerationEvt;
  Time m_dataGenerationPeriod { Seconds (60) };  // Generar datos cada 60s
  uint32_t m_dataPayloadSize { 20 };             // 20 bytes por paquete
  uint32_t m_dataSeqPerNode { 0 };               // Secuencia de datos por nodo
  uint32_t m_dataPacketsGenerated { 0 };         // Contador de datos generados
  uint32_t m_dataPacketsDelivered { 0 };         // Contador de datos entregados
  uint32_t m_dataNoRoute { 0 };                  // Contador de datos descartados por no ruta
  // Deduplicación de datos por nodo (no afecta DV)
  std::map<std::tuple<uint32_t,uint32_t,uint32_t>, Time> m_seenData; // {src,dst,seq} -> first seen
  Time m_seenDataWindow { Seconds (900) };         // 15 minutos
  Time m_seenPacketWindow { Minutes (10) };      // Ventana para deduplicación (solo sink)
  double m_routeTimeoutFactor { 5.0 };
  double m_sfMarginDb { 2.0 };
  std::set<std::tuple<uint32_t,uint32_t,uint32_t>> m_deliveredSet; // solo sink
  std::set<std::tuple<uint32_t,uint32_t,uint32_t>> m_seenOnce;     // datos reenviados una vez por nodo
  
  void GenerateDataTraffic ();
  void SendDataPacket (uint32_t dst);
  void CleanOldSeenData ();

  void SchedulePeriodicDump ();
  void DumpRoute (uint32_t dst, const std::string& tag);
  void DumpFullTable (const std::string& tag) const;

  // Timers
  EventId  m_evt;       // DV base (legacy)
  EventId  m_evtSf9;    // DV en SF9
  EventId  m_evtSf10;   // DV en SF10
  EventId  m_evtSf12;   // DV en SF12
  EventId  m_purgeEvt;  // Limpieza de rutas expiradas
  EventId  m_backoffEvt;
  EventId  m_periodicDumpEvt;
};

} // namespace ns3
