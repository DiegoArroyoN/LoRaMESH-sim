#ifndef LORAMESH_ROUTING_DV_H
#define LORAMESH_ROUTING_DV_H

#include "ns3/callback.h"
#include "ns3/mac48-address.h"
#include "ns3/nstime.h"
#include "ns3/object.h"
#include "ns3/ptr.h"
#include "ns3/random-variable-stream.h"

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ns3
{

using NodeId = uint32_t;

namespace loramesh
{

struct RouteEntry
{
    NodeId destination{0};
    NodeId nextHop{0};
    uint32_t seqNum{0};
    uint8_t hops{0};
    uint8_t sf{0};
    uint32_t toaUs{0};
    // REMOVED: rssiDbm - no se usa en métrica compuesta
    uint16_t batt_mV{0};
    uint16_t scoreX100{0};
    uint16_t costX1000{1000};
    uint32_t toaCostUnits{0};
    double rawMetric{0.0};
    Time lastUpdate{Seconds(0)};
    Time expiryTime{Seconds(0)};
    Mac48Address nextHopMac;
};

struct RouteAnnouncement
{
    NodeId destination{0};
    uint8_t hops{0};
    uint8_t sf{0};
    uint16_t scoreX100{0};
    uint16_t batt_mV{0};
    // REMOVED: rssiDbm
};

struct DvEntry
{
    NodeId destination{0};
    // NOTE (wireFormat=v2): only destination+score are authoritative on-air.
    // The remaining fields are internal/legacy carriers and must not be treated
    // as advertised beacon truth in score-only decoding.
    uint8_t hops{0};
    uint8_t sf{0};
    uint16_t scoreX100{0};
    uint32_t toaUs{0};
    // REMOVED: rssiDbm
    uint16_t batt_mV{0};
};

struct DvMessage
{
    NodeId origin{0};
    uint32_t sequence{0};
    std::vector<DvEntry> entries;
};

struct NeighborLinkInfo
{
    NodeId neighbor{0};
    uint32_t sequence{0};
    uint8_t hops{0};
    uint8_t sf{0};
    uint32_t toaUs{0};
    // REMOVED: rssiDbm - receptor obtiene RSSI de PHY
    uint16_t batt_mV{0};
    uint16_t scoreX100{0};
    Mac48Address mac;
};

class RoutingDv : public Object
{
  public:
    using RouteChangeCallback = Callback<void, const RouteEntry&, const std::string&>;
    using FloodCallback = Callback<void, const DvMessage&>;
    using LocalEnergyFractionCallback = Callback<double>;

    enum class AdvertRoutePolicy
    {
        TOP_SCORE,
        UNIFORM,
        COST_WEIGHTED
    };

    enum class MetricMode
    {
        COMPOSITE_SCORE,
        TOA_ONLY
    };

    enum class CostEncoding
    {
        SCORE100,
        COST255,
        SCORE255
    };

    static TypeId GetTypeId();

    RoutingDv();
    ~RoutingDv() override = default;

    void SetNodeId(NodeId id);
    void SetRouteTimeout(Time timeout);
    void SetRouteSwitchMinDeltaX100(uint16_t deltaX100);
    void SetInitTtl(uint8_t ttl);
    void SetMaxRoutes(uint32_t maxRoutes);
    void SetSequence(uint32_t seq);
    void SetExpireWindow(Time expire);
    void SetMaxHops(uint8_t maxHops);
    void SetAdvertiseAllRoutes(bool enable);
    bool GetAdvertiseAllRoutes() const;
    void SetAdvertRoutePolicy(const std::string& policy);
    std::string GetAdvertRoutePolicy() const;
    void SetMetricMode(const std::string& mode);
    std::string GetMetricMode() const;
    void SetCostEncoding(const std::string& mode);
    std::string GetCostEncoding() const;
    void SetMaxRoutesPerDestination(uint32_t maxRoutesPerDestination);
    void SetMaxTotalRoutes(uint32_t maxTotalRoutes);
    void SetSinkNodeId(NodeId id);
    void NotifyDestinationActive(NodeId dest);
    bool IsDestinationActive(NodeId dest) const;
    bool HasAnyRoute(NodeId dest) const;
    bool IsRouteExpired(NodeId dest) const;
    void SetRouteChangeCallback(RouteChangeCallback cb);
    void SetFloodCallback(FloodCallback cb);
    void SetLocalEnergyFractionCallback(LocalEnergyFractionCallback cb);

    bool HasRoute(NodeId dest);              // Non-const: llama GetRoute()
    NodeId LookupNextHop(NodeId dest);       // Non-const: llama GetRoute()
    double GetRouteCost(NodeId dest);        // Non-const: llama GetRoute()
    const RouteEntry* GetRoute(NodeId dest); // Non-const: permite lazy cleanup
    std::size_t GetRouteCount() const;
    std::size_t GetPrimaryRouteCount() const;
    std::size_t GetBackupRouteCount() const;
    std::size_t GetDestinationsWithBackupCount() const;
    uint64_t GetRouteEvictionsTotal() const;
    uint64_t GetBackupPromotionsTotal() const;
    std::vector<RouteEntry> GetPrimaryRoutesSnapshot() const;
    std::vector<RouteEntry> GetBackupRoutesSnapshot() const;

    void UpdateFromDvMsg(const DvMessage& msg, const NeighborLinkInfo& link);
    void FloodDvUpdate();
    void PrintRoutingTable() const;
    void PrintRouteTo(NodeId dest) const;
    void DebugDumpRoutingTable() const;
    void PurgeExpiredRoutes();

    std::vector<RouteAnnouncement> GetBestRoutes(uint32_t maxRoutes) const;
    std::vector<RouteAnnouncement> GetBestRoutesPueyo(uint32_t maxRoutes) const;

  private:
    static constexpr uint8_t kDefaultMaxHops = 10; // Thesis: H_max default

    void UpdateRoute(const RouteEntry& candidate);
    RouteEntry* LookupRouteMutable(NodeId dest);
    bool IsExpired(const RouteEntry& entry) const;
    Time GetEffectiveTimeout(const RouteEntry& entry) const;
    void NotifyChange(const RouteEntry& entry, const std::string& action) const;
    DvMessage BuildDvMessage() const;
    uint16_t CombineScores(uint16_t linkScoreX100, uint16_t pathScoreX100, uint8_t hops) const;
    double CombineCost(uint16_t linkScoreX100, uint16_t pathScoreX100, uint8_t hops) const;
    uint16_t ScoreToCostX1000(uint16_t scoreX100) const;
    uint16_t ToaHopCostUnits(uint8_t sf) const;
    uint16_t ScoreToToaPathUnits(uint16_t scoreX100) const;
    uint16_t ToaUnitsToScoreX100(uint32_t units) const;
    uint16_t ToaUnitsToCostX1000(uint32_t units) const;
    uint16_t ToaUnitsToAdvertMetric(uint32_t units) const;
    uint16_t AdvertMetricToToaPathUnits(uint16_t metric) const;
    uint16_t MaxAdvertMetricValue() const;
    uint16_t EncodeCompositeMetric(double rawMetric) const;
    uint32_t QuantizeCompositeMetric(double rawMetric) const;
    double DecodeCompositeMetric(uint16_t metric) const;
    double GetComparableMetric(const RouteEntry& entry) const;
    double ComputeCompositeEnergyPenalty(double energyFraction) const;
    double ComputeCompositeLinkIncrement(uint8_t sf) const;
    double ComputeCompositeRawCost(uint32_t toaUnitsPath,
                                   uint8_t hopsPath,
                                   double energyFractionAdvertiser) const;
    double GetLocalEnergyFraction() const;
    bool IsRouteUsable(const RouteEntry& entry, uint8_t hopLimit) const;
    bool IsCandidateBetter(const RouteEntry& candidate, const RouteEntry& current, bool* tie) const;
    bool SelectWorseRouteToEvict(NodeId* outDest, bool* outBackup) const;
    void EnforceRouteTableLimits();
    bool IsInHoldDown(NodeId dest) const; // FIX B2
    void SetHoldDown(NodeId dest);        // FIX B2
    void CleanExpiredHoldDowns();         // FIX B2

    NodeId m_nodeId{0};
    Time m_routeTimeout{Seconds(300)}; // Thesis: route expiry = 300s
    Time m_expireWindow{Seconds(180)};
    Time m_holdDownTime{Seconds(30)}; // FIX B2: duración del hold-down
    uint8_t m_maxHops{12};
    uint8_t m_initTtl{10};
    uint32_t m_maxRoutes{25};
    uint32_t m_sequence{0};
    double m_pathHopWeight{0.05};
    double m_linkWeight{0.70};
    double m_pathWeight{0.25};
    uint16_t m_routeSwitchMinDeltaX100{5};
    bool m_advertiseAllRoutes{false};
    AdvertRoutePolicy m_advertRoutePolicy{AdvertRoutePolicy::TOP_SCORE};
    MetricMode m_metricMode{MetricMode::COMPOSITE_SCORE};
    CostEncoding m_costEncoding{CostEncoding::COST255};
    double m_compositeWToa{1.0};
    double m_compositeWHop{1.0};
    double m_compositeWEnergy{2.0};
    double m_compositeCostStep{1.0};
    double m_energyLo{0.20};
    double m_energyHi{0.50};
    double m_energyPow{3.0};
    double m_energyMaxPenalty{50.0};
    uint32_t m_maxRoutesPerDestination{1};
    uint32_t m_maxTotalRoutes{1024};
    double m_activeTimeoutFactor{2.0};
    Time m_activeDestWindow{Seconds(600)};
    bool m_hasSink{false};
    NodeId m_sinkNodeId{0};
    bool m_pueyoValidationTrace{false};
    RouteChangeCallback m_routeChangeCallback;
    FloodCallback m_floodCallback;
    LocalEnergyFractionCallback m_localEnergyFractionCallback;
    std::map<NodeId, RouteEntry> m_routes;
    std::map<NodeId, RouteEntry> m_backupRoutes;
    std::map<NodeId, RouteEntry> m_inboundRoutes;
    uint64_t m_routeEvictionsTotal{0};
    uint64_t m_backupPromotionsTotal{0};
    std::map<NodeId, uint32_t> m_lastSeq;
    std::unordered_set<NodeId> m_activeDestinations;
    std::unordered_map<NodeId, Time> m_lastDestActivity;
    std::unordered_map<NodeId, Time> m_holdDownUntil; // FIX B2: destinos en hold-down
    Ptr<UniformRandomVariable> m_rng;
};

} // namespace loramesh
} // namespace ns3

#endif /* LORAMESH_ROUTING_DV_H */
