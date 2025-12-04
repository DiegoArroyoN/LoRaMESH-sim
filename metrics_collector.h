#pragma once
#include <fstream>
#include <vector>
#include <map>
#include "ns3/core-module.h"

namespace ns3 {

/**
 * \brief Recolector de métricas para análisis posterior
 * 
 * Captura eventos de TX/RX y exporta a CSV
 */
class MetricsCollector
{
public:
  struct TxEvent {
    Time timestamp;
    uint32_t nodeId;
    uint32_t seq;
    uint32_t dst;
    uint8_t ttl;
    uint8_t hops;
    int16_t rssi;
    uint16_t battery;
    uint16_t score;
    bool ok;
  };

  struct RxEvent {
    Time timestamp;
    uint32_t nodeId;
    uint32_t src;
    uint32_t dst;
    uint32_t seq;
    uint8_t ttl;
    uint8_t hops;
    int16_t rssi;
    uint16_t battery;
    uint16_t score;
    bool isForwarded;
  };

  struct RouteEvent {
    Time timestamp;
    uint32_t nodeId;
    uint32_t destination;
    uint32_t nextHop;
    uint8_t hops;
    uint16_t score;
    uint32_t seq;
    std::string action;  // "NEW", "UPDATE", "PURGE"
  };

  struct DataDeliveryEvent {
  Time timestamp;
  uint32_t src;
  uint32_t seq;
  uint8_t hops;
  bool delivered;  // true si llegó al GW
};

  struct DelayEvent {
    Time timestamp;
    uint32_t src;
    uint32_t dst;
    uint32_t seq;
    uint8_t hops;
    double delaySec;
    uint32_t bytes;
    uint8_t sf;
    bool delivered;
  };

  struct OverheadEvent {
    Time timestamp;
    uint32_t nodeId;
    std::string kind; // "beacon" | "data"
    uint32_t bytes;
    uint32_t src;
    uint32_t dst;
    uint32_t seq;
    uint8_t hops;
    uint8_t sf;
  };

  MetricsCollector ();
  ~MetricsCollector ();

  // Registrar eventos
  void RecordTx (uint32_t nodeId, uint32_t seq, uint32_t dst, uint8_t ttl, 
                 uint8_t hops, int16_t rssi, uint16_t battery, uint16_t score, bool ok);
  void RecordRx (uint32_t nodeId, uint32_t src, uint32_t dst, uint32_t seq, 
                 uint8_t ttl, uint8_t hops, int16_t rssi, uint16_t battery, 
                 uint16_t score, bool isForwarded);
  void RecordRoute (uint32_t nodeId, uint32_t destination, uint32_t nextHop, 
                    uint8_t hops, uint16_t score, uint32_t seq, std::string action);
  void RecordDataPacket (uint32_t src, uint32_t seq, uint8_t hops, bool delivered);
  void RecordE2eDelay (uint32_t src, uint32_t dst, uint32_t seq, uint8_t hops,
                       double delaySec, uint32_t bytes, uint8_t sf, bool delivered);
  void RecordOverhead (uint32_t nodeId, const std::string& kind, uint32_t bytes,
                       uint32_t src, uint32_t dst, uint32_t seq, uint8_t hops, uint8_t sf);
                  

  // Exportar a CSV
  void ExportToCSV (std::string prefix = "mesh_dv");

  // Estadísticas
  void PrintStatistics ();

private:
  std::vector<TxEvent> m_txEvents;
  std::vector<RxEvent> m_rxEvents;
  std::vector<RouteEvent> m_routeEvents;
  std::vector<DelayEvent> m_delayEvents;
  std::vector<OverheadEvent> m_overheadEvents;

  void ExportTxCSV (std::string filename);
  void ExportRxCSV (std::string filename);
  void ExportRouteCSV (std::string filename);
  void ExportDelayCSV (std::string filename);
  void ExportOverheadCSV (std::string filename);
};

} // namespace ns3
