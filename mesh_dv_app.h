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
#include "ns3/net-device.h"
#include "ns3/packet.h"
#include "ns3/address.h"
#include "ns3/mac48-address.h"
#include "ns3/random-variable-stream.h"
#include <string>
#include <deque>
#include <utility>

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
  Mac48Address nextHopMac;
  
  RouteEntry() 
    : destination(0), nextHop(0), seqNum(0), hops(0), 
      sf(9), toaUs(0), rssiDbm(-120), batt_mV(0), scoreX100(0) {}
};

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
  bool PerformCAD ();
  void OnBackoffTimer ();
  void OnPacketTransmitted (uint32_t toaUs);
  Address ResolveNextHopAddress (uint32_t nextHopId) const;
  void ScheduleDvCycle (uint8_t sf, Time period, EventId& evtSlot);
  
  void UpdateRoutingTable(uint32_t dst, uint32_t viaNode,
                       uint32_t seqNum, uint16_t scoreX100,
                       uint8_t hops, uint8_t sf, uint32_t toaUs,
                       int16_t rssiDbm, uint16_t batt_mV,
                       Mac48Address nextHopMac);

  void PrintRoutingTable ();
  void PurgeExpiredRoutes ();
  RouteEntry* GetRoute (uint32_t destination);
  void SendDataToDestination (uint32_t dst, Ptr<Packet> payload);

  uint32_t ComputeLoRaToAUs (uint8_t sf, uint32_t bw, uint8_t cr, uint32_t pl) const;
  uint16_t ComputeScoreX100 (const MeshMetricTag& t) const;

    // Cache de deduplicación
  std::map<std::pair<uint32_t, uint32_t>, Time> m_seenPackets;  // {src, seq} -> timestamp

    // ========== ENERGY MODEL ==========
  uint16_t m_initialBatteryMv = 4200;  // Batería inicial (4.2V)
  uint16_t m_currentBatteryMv = 4200;  // Batería actual
  
  // Consumo de energía por operación (mA)
  static constexpr double TX_POWER_MA   = 120.0;   // TX @ 20dBm
  static constexpr double RX_POWER_MA   = 15.0;    // RX
  static constexpr double IDLE_POWER_MA = 0.1;     // Idle
  static constexpr double BATTERY_VOLTAGE_MAX_MV = 4200.0;
  static constexpr double BATTERY_VOLTAGE_MIN_MV = 3000.0;
  
  // Capacidad de batería (mAh)
  static constexpr double BATTERY_CAPACITY_MAH = 2000.0;

  // Métricas reales
  int16_t GetRealRSSI ();
  uint16_t GetCurrentBatteryMv ();
  double GetEnergyConsumptionJoules ();

  // Recolección de métricas - SIMPLIFICADO (solo funciones)
  void LogTxEvent (uint32_t seq, uint32_t dst, uint8_t ttl, uint8_t hops,
                   int16_t rssi, uint16_t battery, uint16_t score, bool ok);
  void LogRxEvent (uint32_t src, uint32_t dst, uint32_t seq, uint8_t ttl,
                   uint8_t hops, int16_t rssi, uint16_t battery, uint16_t score, bool forwarded);

  void InitializeBatteryModel ();
  void UpdateBatteryEstimate (const std::string& reason);
  void BeginRadioTx (Time duration);
  void EndRadioTx ();
  double GetBatteryPercent () const;
  std::string RadioStateToString () const;
  uint8_t SelectSfForRssi (int16_t rssiDbm) const;

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

  enum class RadioState
  {
    IDLE,
    RX,
    TX
  };

  struct BatteryTracker
  {
    double remainingMah = BATTERY_CAPACITY_MAH;
    Time lastUpdate { Seconds (0) };
    RadioState state { RadioState::RX };
    EventId pendingTxEnd;
  } m_battery;

  bool m_csmaEnabled { true };
  bool m_txBusy { false };
  
  uint8_t m_difsCadCount { 3 };
  Time m_cadDuration { MilliSeconds (5.5) };
  uint8_t m_backoffWindow { 8 };
  uint8_t m_maxRetries { 5 };
  
  std::queue<TxQueueEntry> m_txQueue;
  Ptr<UniformRandomVariable> m_rng;
  
  std::map<uint32_t, RouteEntry> m_routingTable;
  std::map<uint32_t, Mac48Address> m_macTable; // id lógico -> MAC conocida
  Time m_routeTimeout { Seconds (180) };

  static constexpr uint16_t kProtoMesh = 0x88B5;
 
 // NUEVO: Duty Cycle Tracking
  Time m_dutyCycleWindow { Hours (1) };      // Ventana de observación (1h)
  double m_dutyCycleLimit { 0.10 };          // Límite 1%
  
  std::deque<std::pair<Time, Time>> m_txHistory;  // <timestamp, duration>
  
  double GetCurrentDutyCycle ();
  bool CanTransmit (uint32_t toaUs);
  void RecordTransmission (uint32_t toaUs);
  void CleanOldTxHistory ();
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
  Time m_seenPacketWindow { Minutes (30) };      // Ventana para deduplicación
  double m_routeTimeoutFactor { 3.0 };
  double m_sfMarginDb { 2.0 };
  
  void GenerateDataTraffic ();
  void SendDataPacket (uint32_t dst);

  // Timers
  EventId  m_evt;       // DV base (legacy)
  EventId  m_evtSf9;    // DV en SF9
  EventId  m_evtSf10;   // DV en SF10
  EventId  m_evtSf12;   // DV en SF12
  EventId  m_purgeEvt;  // Limpieza de rutas expiradas
  EventId  m_backoffEvt;
};

} // namespace ns3
