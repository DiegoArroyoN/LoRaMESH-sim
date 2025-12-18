#pragma once
#include <fstream>
#include <vector>
#include <map>
#include <tuple>
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
    uint8_t sf;
    double energyJ;
    double energyFrac;
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
    uint8_t sf;
    double energyJ;
    double energyFrac;
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

  struct DutyEvent {
    uint32_t nodeId;
    double dutyUsed;
    uint32_t txCount;
    uint32_t backoffCount;
  };

  struct EnergySummary {
    double initialJ {-1.0};
    double remainingJ {-1.0};
    double frac {-1.0};
  };

  MetricsCollector ();
  ~MetricsCollector ();

  // Registrar eventos
  void RecordTx (uint32_t nodeId, uint32_t seq, uint32_t dst, uint8_t ttl, 
                 uint8_t hops, int16_t rssi, uint16_t battery, uint16_t score, uint8_t sf,
                 double energyJ, double energyFrac, bool ok);
  void RecordRx (uint32_t nodeId, uint32_t src, uint32_t dst, uint32_t seq, 
                 uint8_t ttl, uint8_t hops, int16_t rssi, uint16_t battery, 
                 uint16_t score, uint8_t sf, double energyJ, double energyFrac, bool isForwarded);
  void RecordRoute (uint32_t nodeId, uint32_t destination, uint32_t nextHop, 
                    uint8_t hops, uint16_t score, uint32_t seq, std::string action);
  void RecordDataPacket (uint32_t src, uint32_t seq, uint8_t hops, bool delivered);
  void RecordE2eDelay (uint32_t src, uint32_t dst, uint32_t seq, uint8_t hops,
                       double delaySec, uint32_t bytes, uint8_t sf, bool delivered);
  void RecordOverhead (uint32_t nodeId, const std::string& kind, uint32_t bytes,
                       uint32_t src, uint32_t dst, uint32_t seq, uint8_t hops, uint8_t sf);
  void RecordDuty (uint32_t nodeId, double dutyUsed, uint32_t txCount, uint32_t backoffCount);
  void RecordEnergySnapshot (uint32_t nodeId, double energyJ, double energyFrac);
  void RecordRouteUsed (uint32_t nodeId, uint32_t destination, uint32_t nextHop,
                        uint8_t hops, uint16_t score, uint32_t seq);
  double GetFirstTxTime (uint32_t src, uint32_t dst, uint32_t seq) const;
                  

  // Exportar a CSV
  void ExportToCSV (std::string prefix = "mesh_dv");

  // Estadísticas
  void PrintStatistics ();

private:
  std::vector<TxEvent> m_txEvents;
  std::vector<RxEvent> m_rxEvents;
  std::vector<RouteEvent> m_routeEvents;
  std::vector<RouteEvent> m_routeUsedEvents;
  std::vector<DelayEvent> m_delayEvents;
  std::vector<OverheadEvent> m_overheadEvents;
  std::vector<DutyEvent> m_dutyEvents;
  std::map<std::tuple<uint32_t, uint32_t, uint32_t>, double> m_firstTxTime;
  std::map<uint32_t, EnergySummary> m_energySummary;

  void ExportTxCSV (std::string filename);
  void ExportRxCSV (std::string filename);
  void ExportRouteCSV (std::string filename);
  void ExportRouteUsedCSV (std::string filename);
  void ExportDelayCSV (std::string filename);
  void ExportOverheadCSV (std::string filename);
  void ExportDutyCSV (std::string filename);
  void ExportEnergyCSV (std::string filename);
};

extern MetricsCollector* g_metricsCollector;

} // namespace ns3
