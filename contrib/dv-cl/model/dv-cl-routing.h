/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_ROUTING_H
#define DV_CL_ROUTING_H

#include "dv-cl-metric.h"

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

namespace dvcl
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
    // §DC-aware: DC restante [0-100] del next-hop de esta ruta (0xFF = N/A).
    // Usado por el filtro de factibilidad: rutas cuyo next-hop tiene DC bajo
    // se posponen frente a alternativas factibles (fallback si no las hay).
    uint8_t nextHopDcRemaining{0xFF};
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
    // RSSI con el que ESTE receptor oye al vecino, dBm. Medicion local del
    // enlace (no viaja en el beacon), poblada desde el PHY al recibir. 0 = sin
    // medir. Solo la usa la metrica RSSI de referencia; la compuesta la ignora.
    double rssiDbm{0.0};
    uint16_t batt_mV{0};
    uint16_t scoreX100{0};
    Mac48Address mac;
    // §DC-aware: DC restante [0-100] del vecino emisor, leído del beacon (0xFF = N/A).
    uint8_t dc_remaining{0xFF};
};

class DvClRouting : public Object
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
        TOA_ONLY,
        RSSI //!< linea de referencia de una sola capa (DoE Q1)
    };

    enum class CostEncoding
    {
        SCORE100,
        COST255,
        SCORE255
    };

    static TypeId GetTypeId();

    DvClRouting();
    ~DvClRouting() override = default;

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
    // §DC-aware: filtro de factibilidad por presupuesto de duty cycle del next-hop.
    void SetUseDcAwareRouting(bool enable);
    void SetDcFeasibilityThreshold(uint8_t thresholdPct);
    void NotifyDestinationActive(NodeId dest);
    bool IsDestinationActive(NodeId dest) const;
    bool HasAnyRoute(NodeId dest) const;
    bool IsRouteExpired(NodeId dest) const;
    void SetRouteChangeCallback(RouteChangeCallback cb);
    void SetFloodCallback(FloodCallback cb);
    void SetLocalEnergyFractionCallback(LocalEnergyFractionCallback cb);
    /// F6.1 pluggable-metric seam: replace the link-cost metric. The legacy
    /// Composite*/Energy* attributes only reach the built-in
    /// DvClCompositeMetric; a custom metric owns its own configuration.
    void SetMetric(Ptr<DvClRoutingMetric> metric);
    Ptr<DvClRoutingMetric> GetMetric() const;

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

    /**
     * \brief Airtime a DATA frame would take over a link at this spreading
     *        factor, microseconds. Zero (the default) means "not set".
     *
     * The link cost should price what it costs to carry data over the link,
     * not what the beacon that advertised it happened to cost. Feeding the
     * beacon's airtime was doubly wrong: beacons are far larger than data, so
     * the normalisation saturated, and their size varies with how many routes
     * they carry rather than with the link (VALIDATION.md 2026-07-25). The
     * application knows the payload size and radio parameters, so it fills
     * this table once at startup.
     */
    void SetDataToaForSf(uint8_t sf, double toaUs);

  private:
    static constexpr uint8_t kDefaultMaxHops = 10; // Thesis: H_max default

    void UpdateRoute(const RouteEntry& candidate);
    RouteEntry* LookupRouteMutable(NodeId dest);
    bool IsExpired(const RouteEntry& entry) const;
    Time GetEffectiveTimeout(const RouteEntry& entry) const;
    void NotifyChange(const RouteEntry& entry, const std::string& action) const;
    DvMessage BuildDvMessage() const;
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
    /// Convert received batt_mV to energy fraction [0,1] (0 mV = unknown -> 1.0 = no penalty).
    static double BattMvToEFrac(uint16_t battMv);
    /// Single-link incremental cost, delegated to the pluggable metric (F6.1).
    double ComputeThesisLinkCost(double toaUs,
                                 uint8_t sf,
                                 double neighborEFrac,
                                 double rssiDbm = 0.0) const;

    /// The built-in composite metric if that is what is plugged, else nullptr.
    DvClCompositeMetric* BuiltinMetricOrNull() const;
    void SetAttrWToa(double v);
    double GetAttrWToa() const;
    void SetAttrWHop(double v);
    double GetAttrWHop() const;
    void SetAttrWEnergy(double v);
    double GetAttrWEnergy() const;
    void SetAttrEnergyLo(double v);
    double GetAttrEnergyLo() const;
    void SetAttrEnergyHi(double v);
    double GetAttrEnergyHi() const;
    void SetAttrEnergyPow(double v);
    double GetAttrEnergyPow() const;
    void SetAttrEnergyMaxPenalty(double v);
    double GetAttrEnergyMaxPenalty() const;
    double GetLocalEnergyFraction() const;
    bool IsRouteUsable(const RouteEntry& entry, uint8_t hopLimit) const;
    /// §DC-aware: ¿el next-hop tiene presupuesto de DC por encima del umbral?
    /// Si el filtro está desactivado, o no hay info del vecino, o el valor es
    /// 0xFF (N/A), se considera factible (no penaliza por falta de información).
    bool IsNextHopDcFeasible(NodeId nextHop) const;
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
    /// ToA de un paquete de DATOS por SF (indice 0 = SF7). 0 = sin fijar, en
    /// cuyo caso se usa el ToA que llegue (comportamiento heredado).
    double m_dataToaBySf[6]{0, 0, 0, 0, 0, 0};

    MetricMode m_metricMode{MetricMode::COMPOSITE_SCORE};
    CostEncoding m_costEncoding{CostEncoding::COST255};
    double m_compositeCostStep{0.025}; // quantization step for COST255 byte
    Ptr<DvClRoutingMetric> m_metric;   // F6.1: pluggable link-cost metric
    uint32_t m_maxRoutesPerDestination{1};
    uint32_t m_maxTotalRoutes{1024};
    double m_activeTimeoutFactor{2.0};
    Time m_activeDestWindow{Seconds(600)};
    bool m_hasSink{false};
    NodeId m_sinkNodeId{0};
    bool m_pueyoValidationTrace{false};
    // §BatBeacon: toggle — whether SoC from received beacons influences composite cost.
    // true (default for CMP profiles): use real neighbor SoC, Psi(b_j) from beacon.
    // false (TOA profiles or ablation): assume full battery, Psi(b_j)=0.
    bool m_useBeaconSoC{true};
    // §BatBeacon: minimum SoC change [0-50%] to trigger composite cost update.
    // 0=no hysteresis (every beacon). Recommended 5 for CMP stability.
    uint8_t m_socHysteresisPercent{0};
    // §BatBeacon: last committed SoC [0-100] per neighbor for hysteresis tracking.
    std::map<NodeId, uint8_t> m_lastNeighborSoC;
    // §DC-aware: toggle — whether the routing applies the DC feasibility filter.
    // false (default, TOA/CMP sin DC-aware): el routing ignora el DC del next-hop.
    // true (DV-CL completo): rutas a través de next-hops con DC < umbral se posponen.
    bool m_useDcAwareRouting{false};
    // §DC-aware: umbral de factibilidad [0-100]. Un next-hop con dc_remaining por
    // debajo de este umbral se considera "no factible" y se evita si hay alternativa.
    uint8_t m_dcFeasibilityThreshold{20};
    // §DC-aware: último dc_remaining [0-100] conocido por vecino directo (del beacon).
    std::map<NodeId, uint8_t> m_neighborDcRemaining;
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

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_ROUTING_H */
