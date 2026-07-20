/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_STATS_SINK_H
#define DV_CL_STATS_SINK_H

#include "ns3/object.h"

#include <cstdint>
#include <string>

namespace ns3
{
namespace dvcl
{

/**
 * \ingroup dv-cl
 * \brief Per-node runtime counters reported by the application at stop.
 *
 * Field-for-field the campaign collector's RuntimeNodeStats (including
 * the LossFine terminal-fate counters), so existing analysis pipelines
 * map 1:1.
 */
struct DvClNodeStats
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
    uint64_t dropMaxCsmaRetries{0};
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
    uint64_t dataCollisionDrops{0};
    uint64_t beaconCollisionDrops{0};
    uint64_t dataBusyDrops{0};
    uint64_t beaconBusyDrops{0};
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
    uint64_t originPendingAtStop{0};
    uint64_t originPendingDuty{0};
    uint64_t originPendingEnergy{0};
    uint64_t relayPendingEnd{0};
    uint64_t sfLinkSamples{0};
    uint64_t sfLinkObservedMismatch{0};
    uint64_t routesTotal{0};
    uint64_t routesPrimaryTotal{0};
    uint64_t routesBackupTotal{0};
    uint64_t destinationsWithBackupTotal{0};
    uint64_t routeEvictionsTotal{0};
    uint64_t backupPromotionsTotal{0};
};

/**
 * \ingroup dv-cl
 * \brief Pluggable measurement sink for the DV-CL application.
 *
 * The module emits primitive measurement events through this interface
 * instead of a global collector: experiments plug their own sink (the
 * campaign collector implements it verbatim — signatures match its
 * Record* methods 1:1), tests plug probes, and a trace-source adapter
 * can wrap it without touching the protocol code. A null sink is legal:
 * the application guards every call.
 */
class DvClStatsSink : public Object
{
  public:
    static TypeId GetTypeId();
    ~DvClStatsSink() override = default;

    virtual void RecordTx(uint32_t nodeId,
                          uint32_t src,
                          uint32_t seq,
                          uint32_t dst,
                          uint8_t ttl,
                          uint8_t hops,
                          int16_t rssi,
                          uint16_t battery,
                          uint16_t score,
                          uint8_t sf,
                          uint32_t toaUs,
                          double energyJ,
                          double energyFrac,
                          bool ok) = 0;
    virtual void RecordRx(uint32_t nodeId,
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
                          bool isForwarded) = 0;
    /// The generation ledger (F2.4): every generated data packet.
    virtual void RecordDataGenerated(uint32_t src, uint32_t dst, uint32_t seq) = 0;
    virtual void RecordRoute(uint32_t nodeId,
                             uint32_t destination,
                             uint32_t nextHop,
                             uint8_t hops,
                             uint16_t score,
                             uint32_t seq,
                             std::string action) = 0;
    virtual void RecordRouteUsed(uint32_t nodeId,
                                 uint32_t destination,
                                 uint32_t nextHop,
                                 uint8_t hops,
                                 uint16_t score,
                                 uint32_t seq) = 0;
    virtual void RecordEnergySnapshot(uint32_t nodeId, double energyJ, double energyFrac) = 0;
    virtual void RecordQuantizationSample(uint32_t nodeId,
                                          uint32_t destination,
                                          uint32_t nextHop,
                                          bool isBackup,
                                          double rawMetric,
                                          uint16_t scoreQuantized) = 0;
    virtual void RecordRuntimeNodeStats(const DvClNodeStats& stats) = 0;
    virtual void RecordOverhead(uint32_t nodeId,
                                const std::string& kind,
                                uint32_t bytes,
                                uint32_t src,
                                uint32_t dst,
                                uint32_t seq,
                                uint8_t hops,
                                uint8_t sf) = 0;
    virtual void RecordDuty(uint32_t nodeId,
                            double dutyUsed,
                            uint32_t txCount,
                            uint32_t backoffCount) = 0;
    virtual void RecordConnectivity(uint32_t nodeId, uint32_t destination, bool hasRoute) = 0;
    /// End-to-end delay sample of a delivered/expired data packet.
    virtual void RecordE2eDelay(uint32_t src,
                                uint32_t dst,
                                uint32_t seq,
                                uint8_t hops,
                                double delayFirstTxSec,
                                uint32_t bytes,
                                uint8_t sf,
                                bool delivered) = 0;
    /// Beacon scheduling-to-transmission delay sample, seconds.
    virtual void RecordBeaconDelay(double delaySec) = 0;
    /// First-transmission time of a packet, or a negative value if unknown.
    virtual double GetFirstTxTime(uint32_t src, uint32_t dst, uint32_t seq) const = 0;
};

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_STATS_SINK_H */
