#pragma once
#include "ns3/core-module.h"

#include <fstream>
#include <map>
#include <set>
#include <tuple>
#include <vector>
#include <cstdint>

namespace ns3
{

/**
 * \brief Recolector de métricas para análisis posterior
 *
 * Captura eventos de TX/RX y exporta a CSV
 */
class MetricsCollector
{
  public:
    struct TxEvent
    {
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

    struct RxEvent
    {
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

    struct RouteEvent
    {
        Time timestamp;
        uint32_t nodeId;
        uint32_t destination;
        uint32_t nextHop;
        uint8_t hops;
        uint16_t score;
        uint32_t seq;
        std::string action; // "NEW", "UPDATE", "PURGE"
    };

    struct DataDeliveryEvent
    {
        Time timestamp;
        uint32_t src;
        uint32_t seq;
        uint8_t hops;
        bool delivered; // true si llegó al GW
    };

    struct DelayEvent
    {
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

    struct OverheadEvent
    {
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

    struct DutyEvent
    {
        uint32_t nodeId;
        double dutyUsed;
        uint32_t txCount;
        uint32_t backoffCount;
    };

    struct EnergySummary
    {
        double initialJ{-1.0};
        double remainingJ{-1.0};
        double frac{-1.0};
    };

    struct RunConfigMetadata
    {
        std::string simVersion{"wire_v2"};
        uint32_t nNodes{0};
        std::string topology{"unknown"};
        double areaWidthM{0.0};
        double areaHeightM{0.0};
        uint32_t rngRun{0};

        bool enableCsma{false};
        bool enableDuty{false};
        double dutyLimit{0.0};
        double dutyWindowSec{3600.0};

        double dataStartSec{0.0};
        double dataStopSec{-1.0};
        double stopSec{0.0};
        double pdrEndWindowSec{0.0};

        std::string trafficLoad{"unknown"};
        double trafficIntervalS{0.0};
        uint32_t payloadBytes{0};
        double dedupWindowSec{600.0};

        double beaconIntervalWarmS{0.0};
        double beaconIntervalStableS{0.0};
        double routeTimeoutFactor{0.0};
        double routeTimeoutSec{0.0};

        std::string interferenceModel{"unknown"};
        std::string propModel{"unknown"};
        double txPowerDbm{0.0};
        uint32_t channelCount{1};
    };

    struct RuntimeNodeStats
    {
        uint32_t nodeId{0};
        uint32_t txQueueLenEnd{0};
        uint32_t queuedPacketsEnd{0};
        uint64_t cadBusyEvents{0};
        uint64_t dutyBlockedEvents{0};
        double totalWaitTimeDueToDutyS{0.0};
        uint64_t dropNoRoute{0};
        uint64_t dropTtlExpired{0};
        uint64_t dropQueueOverflow{0};
        uint64_t dropBacktrack{0};
        uint64_t dropOther{0};
        uint64_t beaconScheduled{0};
        uint64_t beaconTxSent{0};
        uint64_t beaconBlockedByDuty{0};
        uint64_t rpGapLargeEvents{0};
    };

    // ========================================================================
    // THESIS METRICS: T50 and FND (First Node Death)
    // ========================================================================
    struct NodeDeathEvent
    {
        Time timestamp;
        uint32_t nodeId;
        double energyFrac;  // Energy fraction at death (should be ~0)
        std::string reason; // "energy" or "no_route"
    };

    struct ConnectivityEvent
    {
        Time timestamp;
        uint32_t nodeId;
        uint32_t destination;
        bool hasRoute; // true if node has valid route to destination
    };

    MetricsCollector();
    ~MetricsCollector();

    // Registrar eventos
    void RecordTx(uint32_t nodeId,
                  uint32_t seq,
                  uint32_t dst,
                  uint8_t ttl,
                  uint8_t hops,
                  int16_t rssi,
                  uint16_t battery,
                  uint16_t score,
                  uint8_t sf,
                  double energyJ,
                  double energyFrac,
                  bool ok);
    void RecordRx(uint32_t nodeId,
                  uint32_t src,
                  uint32_t dst,
                  uint32_t seq,
                  uint8_t ttl,
                  uint8_t hops,
                  int16_t rssi,
                  uint16_t battery,
                  uint16_t score,
                  uint8_t sf,
                  double energyJ,
                  double energyFrac,
                  bool isForwarded);
    void RecordRoute(uint32_t nodeId,
                     uint32_t destination,
                     uint32_t nextHop,
                     uint8_t hops,
                     uint16_t score,
                     uint32_t seq,
                     std::string action);
    void RecordDataPacket(uint32_t src, uint32_t seq, uint8_t hops, bool delivered);
    void RecordDataGenerated(uint32_t src, uint32_t dst, uint32_t seq);
    void RecordE2eDelay(uint32_t src,
                        uint32_t dst,
                        uint32_t seq,
                        uint8_t hops,
                        double delaySec,
                        uint32_t bytes,
                        uint8_t sf,
                        bool delivered);
    void RecordOverhead(uint32_t nodeId,
                        const std::string& kind,
                        uint32_t bytes,
                        uint32_t src,
                        uint32_t dst,
                        uint32_t seq,
                        uint8_t hops,
                        uint8_t sf);
    void RecordDuty(uint32_t nodeId, double dutyUsed, uint32_t txCount, uint32_t backoffCount);
    void RecordEnergySnapshot(uint32_t nodeId, double energyJ, double energyFrac);
    void RecordRouteUsed(uint32_t nodeId,
                         uint32_t destination,
                         uint32_t nextHop,
                         uint8_t hops,
                         uint16_t score,
                         uint32_t seq);
    double GetFirstTxTime(uint32_t src, uint32_t dst, uint32_t seq) const;

    // Exportar a CSV
    void ExportToCSV(std::string prefix = "mesh_dv");

    // Exportar a JSON (structured logging)
    void ExportToJson(std::string prefix = "mesh_dv");

    // FIX D1: Flush periódico para evitar OOM
    void StartPeriodicFlush(Time interval, std::string prefix = "mesh_dv");
    void StopPeriodicFlush();
    void FlushToDisk(); // Exporta y limpia vectores

    // Estadísticas
    void PrintStatistics();

    // ========================================================================
    // THESIS METRICS: T50 and FND
    // ========================================================================
    void RecordNodeDeath(uint32_t nodeId, double energyFrac, const std::string& reason);
    void RecordConnectivity(uint32_t nodeId, uint32_t destination, bool hasRoute);
    double GetT50() const; // Time when >=50% of nodes are dead (E_i <= 0)
    double GetFND() const; // First Node Death time

    void SetTotalNodes(uint32_t nNodes)
    {
        m_totalNodes = nNodes;
    }

    void SetSinkNodeId(uint32_t sinkId)
    {
        m_sinkNodeId = sinkId;
    }

    void SetSimulationStopSec(double stopSec)
    {
        m_simulationStopSec = stopSec;
    }

    void SetEndWindowSec(double windowSec)
    {
        m_endWindowSec = windowSec;
    }

    void SetWireFormatMetadata(const std::string& wireFormat,
                               uint32_t dataHeaderBytes,
                               uint32_t beaconHeaderBytes,
                               uint32_t dvEntryBytes)
    {
        m_wireFormat = wireFormat;
        m_dataHeaderBytes = dataHeaderBytes;
        m_beaconHeaderBytes = beaconHeaderBytes;
        m_dvEntryBytes = dvEntryBytes;
    }

    void SetRunConfigMetadata(const RunConfigMetadata& meta)
    {
        m_runConfig = meta;
        m_hasRunConfig = true;
    }

    void RecordRuntimeNodeStats(const RuntimeNodeStats& stats)
    {
        m_runtimeStatsByNode[stats.nodeId] = stats;
    }

    void RecordBeaconDelay(double delaySec)
    {
        if (delaySec >= 0.0)
        {
            m_beaconDelaySamples.push_back(delaySec);
        }
    }

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
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>> m_generatedDataKeys;
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, double> m_generatedDataTime;
    double m_simulationStopSec{-1.0};
    double m_endWindowSec{0.0};
    std::string m_wireFormat{"v1"};
    uint32_t m_dataHeaderBytes{0};
    uint32_t m_beaconHeaderBytes{0};
    uint32_t m_dvEntryBytes{0};
    RunConfigMetadata m_runConfig;
    bool m_hasRunConfig{false};
    std::map<uint32_t, RuntimeNodeStats> m_runtimeStatsByNode;
    std::vector<double> m_beaconDelaySamples;
    uint64_t m_routeNewEvents{0};
    uint64_t m_routeUpdateEvents{0};
    uint64_t m_routePoisonEvents{0};
    uint64_t m_routeExpireEvents{0};
    uint64_t m_routePurgeEvents{0};
    uint64_t m_routeUsedEventsCount{0};

    // FIX D1: Flush periódico
    EventId m_flushEvent;
    std::string m_csvPrefix{"mesh_dv"};
    Time m_flushInterval{Seconds(30)};
    uint32_t m_flushCount{0};
    bool m_appendMode{false}; // Después del primer flush, usar append

    // THESIS METRICS: T50 and FND
    std::vector<NodeDeathEvent> m_nodeDeathEvents;
    std::vector<ConnectivityEvent> m_connectivityEvents;
    std::set<uint32_t> m_deadNodes;
    uint32_t m_totalNodes{0};
    uint32_t m_sinkNodeId{0};
    double m_t50Cached{-1.0};
    double m_fndCached{-1.0};

    void ScheduleNextFlush();
    void ExportTxCSV(std::string filename);
    void ExportRxCSV(std::string filename);
    void ExportRouteCSV(std::string filename);
    void ExportRouteUsedCSV(std::string filename);
    void ExportDelayCSV(std::string filename);
    void ExportOverheadCSV(std::string filename);
    void ExportDutyCSV(std::string filename);
    void ExportEnergyCSV(std::string filename);
    void ExportLifetimeCSV(std::string filename);
};

extern MetricsCollector* g_metricsCollector;

} // namespace ns3
