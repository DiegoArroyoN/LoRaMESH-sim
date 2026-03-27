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
        uint32_t src;
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
        double delayFirstTxSec;
        double delayGenSec;
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
        std::string simVersion{"pueyo_paper_like_default"};
        std::string gitCommit{""};
        uint32_t nNodes{0};
        std::string topology{"unknown"};
        std::string topologyPreset{""};
        uint32_t gridSide{0};
        double gridSpacingXM{0.0};
        double gridSpacingYM{0.0};
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
        std::string trafficMode{"periodic_any_to_any"};
        double trafficIntervalS{0.0};
        bool enableDataRandomDest{true};
        int32_t onlyGenerateFromNodeId{-1};
        int32_t forcedDataDestinationId{-1};
        uint32_t payloadBytes{0};
        uint32_t packetsPerPair{0};
        double dedupWindowSec{600.0};

        double beaconIntervalWarmS{0.0};
        double beaconIntervalStableS{0.0};
        double routeTimeoutFactor{0.0};
        double routeTimeoutSec{0.0};

        std::string interferenceModel{"unknown"};
        std::string propModel{"unknown"};
        std::string profile{"pueyo2024_paper_like"};
        uint32_t frequencyHz{868000000};
        uint32_t bandwidthHz{125000};
        std::string codingRate{"4/5"};
        uint32_t sfMin{7};
        uint32_t sfMax{12};
        uint32_t preambleSymbols{0};
        std::string routeAdvertPolicy{"top_score"};
        std::string routeMetricMode{"composite_score"};
        std::string costEncoding{"cost255"};
        std::string sfLinkMode{"observed_rxsf"};
        double sfLinkMarginDb{0.0};
        double compositeWToa{1.0};
        double compositeWHop{1.0};
        double compositeWEnergy{2.0};
        double compositeCostStep{1.0};
        double energyLo{0.20};
        double energyHi{0.50};
        double energyPow{3.0};
        double energyMaxPenalty{50.0};
        uint32_t maxRoutesPerDestination{1};
        uint32_t maxTotalRoutes{1024};
        uint32_t dvPayloadMaxBytes{0};
        bool beaconLatestOnly{true};
        bool prioritizeBeacons{true};
        bool pueyoStrictQueueScheduler{false};
        double controlBackoffFactor{0.5};
        double dataBackoffFactor{1.0};
        double sfScanEdThresholdDbm{-120.0};
        bool sfScanResetOnNewSignal{true};
        bool enableSfScanRx{true};
        bool pueyoFloraLikeRx{false};
        bool enableNs3EnergyFramework{false};
        double shadowingSigmaDb{3.57};
        double txPowerDbm{0.0};
        uint32_t channelCount{1};
        uint32_t receptionPaths{1};
    };

    struct RuntimeNodeStats
    {
        uint32_t nodeId{0};
        uint32_t txQueueLenEnd{0};
        uint32_t queuedPacketsEnd{0};
        uint64_t cadBusyEvents{0};
        uint64_t dutyBlockedEvents{0};
        uint64_t dutyBlockedControl{0};
        uint64_t dutyBlockedData{0};
        double totalWaitTimeDueToDutyS{0.0};
        uint64_t dropNoRoute{0};
        uint64_t dropNoRouteSrc{0};
        uint64_t dropNoRouteRelay{0};
        uint64_t dropTtlExpired{0};
        uint64_t dropQueueOverflow{0};
        uint64_t dropBacktrack{0};
        uint64_t dropOther{0};
        uint64_t beaconScheduled{0};
        uint64_t beaconTxSent{0};
        uint64_t dataTxSent{0};
        uint64_t beaconBlockedByDuty{0};
        uint64_t beaconSupersededLatestOnly{0};
        uint64_t rpGapLargeEvents{0};
        uint64_t cadBusyEventsLocalPower{0};
        uint64_t cadBusyEventsOracle{0};
        uint64_t rxScanAttempts{0};
        uint64_t rxScanLocks{0};
        uint64_t rxScanMissBeforeLock{0};
        uint64_t rxPostLockInterferenceFail{0};
        uint64_t rxNoMoreDemodDrops{0};
        uint64_t pueyoSameSfOverlapEvents{0};
        uint64_t pueyoDestructiveOverlapDrops{0};
        uint64_t pueyoCaptureOrTimingSurvivals{0};
        uint64_t pueyoCrossSfIgnoredOverlaps{0};
        uint64_t goursaudDeterministicDrops{0};
        uint64_t goursaudCrossSfCaptureSuccesses{0};
        uint64_t goursaudCrossSfCaptureFails{0};
        uint64_t beaconRxOk{0};
        double rxScanTimeTotalS{0.0};
        double rxAcquisitionDelayMeanS{0.0};
        double rxAcquisitionDelayP95S{0.0};
        double firstUsableRouteTimeSec{-1.0};
        double coverage80RouteTimeSec{-1.0};
        uint64_t routesAtDataStart{0};
        uint64_t beaconTxAtDataStart{0};
        uint64_t beaconRxAtDataStart{0};
        uint64_t routesAtMidpoint{0};
        uint64_t beaconTxAtMidpoint{0};
        uint64_t beaconRxAtMidpoint{0};
        uint64_t routesAtDataStop{0};
        uint64_t beaconTxAtDataStop{0};
        uint64_t beaconRxAtDataStop{0};
        uint64_t queuedPacketsAtDataStop{0};
        uint64_t sfLinkSamples{0};
        uint64_t sfLinkObservedMismatch{0};
        uint64_t routesTotal{0};
        uint64_t routesPrimaryTotal{0};
        uint64_t routesBackupTotal{0};
        uint64_t destinationsWithBackupTotal{0};
        uint64_t routeEvictionsTotal{0};
        uint64_t backupPromotionsTotal{0};
    };

    struct QuantizationSample
    {
        uint32_t nodeId{0};
        uint32_t destination{0};
        uint32_t nextHop{0};
        bool isBackup{false};
        double rawMetric{0.0};
        uint64_t rawMetricMilli{0};
        uint16_t scoreQuantized{0};
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
                  uint32_t src,
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
                        double delayFirstTxSec,
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
    void RecordQuantizationSample(uint32_t nodeId,
                                  uint32_t destination,
                                  uint32_t nextHop,
                                  bool isBackup,
                                  double rawMetric,
                                  uint16_t scoreQuantized);
    double GetFirstTxTime(uint32_t src, uint32_t dst, uint32_t seq) const;

    // Exportar a CSV
    void ExportToCSV(std::string prefix = "mesh_dv");

    // Exportar a JSON (structured logging)
    void ExportToJson(std::string prefix = "mesh_dv");
    void SetEssentialMetricsOnly(bool value)
    {
        m_essentialMetricsOnly = value;
    }

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
    std::vector<QuantizationSample> m_quantizationSamples;
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
    bool m_essentialMetricsOnly{false};

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
