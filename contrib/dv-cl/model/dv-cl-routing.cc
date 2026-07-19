/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-routing.h"

#include "ns3/boolean.h"
#include "ns3/double.h"
#include "ns3/log.h"
#include "ns3/string.h"
#include "ns3/simulator.h"
#include "ns3/uinteger.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cmath>
#include <cstdint>
#include <limits>

namespace ns3
{
namespace dvcl
{

NS_LOG_COMPONENT_DEFINE("DvClRouting");
NS_OBJECT_ENSURE_REGISTERED(DvClRouting);

TypeId
DvClRouting::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClRouting")
            .SetParent<Object>()
            .AddConstructor<DvClRouting>()
            .AddAttribute("PathHopWeight",
                          "Legacy attribute (deprecated, no-op in pure composite DV mode).",
                          DoubleValue(0.05),
                          MakeDoubleAccessor(&DvClRouting::m_pathHopWeight),
                          MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute("LinkWeight",
                          "Legacy attribute (deprecated, no-op in pure composite DV mode).",
                          DoubleValue(0.70),
                          MakeDoubleAccessor(&DvClRouting::m_linkWeight),
                          MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute("PathWeight",
                          "Legacy attribute (deprecated, no-op in pure composite DV mode).",
                          DoubleValue(0.25),
                          MakeDoubleAccessor(&DvClRouting::m_pathWeight),
                          MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute("AdvertiseAllRoutes",
                          "Advertise all known routes in DV messages (subject to maxRoutes).",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClRouting::m_advertiseAllRoutes),
                          MakeBooleanChecker())
            .AddAttribute(
                "AdvertRoutePolicy",
                "Route selection policy for beacon payload truncation: top_score | uniform | cost_weighted.",
                StringValue("top_score"),
                MakeStringAccessor(&DvClRouting::SetAdvertRoutePolicy, &DvClRouting::GetAdvertRoutePolicy),
                MakeStringChecker())
            .AddAttribute("MetricMode",
                          "Route metric mode: composite_score | toa_only.",
                          StringValue("composite_score"),
                          MakeStringAccessor(&DvClRouting::SetMetricMode, &DvClRouting::GetMetricMode),
                          MakeStringChecker())
            .AddAttribute("CostEncoding",
                          "On-air byte encoding for announced metric: score100 | cost255 | score255.",
                          StringValue("cost255"),
                          MakeStringAccessor(&DvClRouting::SetCostEncoding, &DvClRouting::GetCostEncoding),
                          MakeStringChecker())
            .AddAttribute("CompositeWToa",
                          "Weight alpha applied to normalised T_hat in composite_score mode.",
                          DoubleValue(0.60),
                          MakeDoubleAccessor(&DvClRouting::m_compositeWToa),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("CompositeWHop",
                          "Fixed per-hop cost beta added once per link in composite_score mode.",
                          DoubleValue(0.15),
                          MakeDoubleAccessor(&DvClRouting::m_compositeWHop),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("CompositeWEnergy",
                          "Energy penalty weight delta in composite_score mode (thesis 4.2).",
                          DoubleValue(0.25),
                          MakeDoubleAccessor(&DvClRouting::m_compositeWEnergy),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("CompositeCostStep",
                          "Scalar step to quantize composite raw cost into a COST255 byte.",
                          DoubleValue(0.025),
                          MakeDoubleAccessor(&DvClRouting::m_compositeCostStep),
                          MakeDoubleChecker<double>(1e-9))
            .AddAttribute("EnergyLo",
                          "Lower energy threshold for composite energy penalty.",
                          DoubleValue(0.20),
                          MakeDoubleAccessor(&DvClRouting::m_energyLo),
                          MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute("EnergyHi",
                          "Upper energy threshold for composite energy penalty.",
                          DoubleValue(0.50),
                          MakeDoubleAccessor(&DvClRouting::m_energyHi),
                          MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute("EnergyPow",
                          "Exponent p in piecewise energy penalty Psi (thesis Eq.3).",
                          DoubleValue(2.0),
                          MakeDoubleAccessor(&DvClRouting::m_energyPow),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("EnergyMaxPenalty",
                          "Psi_max: maximum piecewise penalty before scaling by CompositeWEnergy.",
                          DoubleValue(1.0),
                          MakeDoubleAccessor(&DvClRouting::m_energyMaxPenalty),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("MaxRoutesPerDestination",
                          "Maximum number of route candidates stored per destination.",
                          UintegerValue(1),
                          MakeUintegerAccessor(&DvClRouting::SetMaxRoutesPerDestination),
                          MakeUintegerChecker<uint32_t>(1, 2))
            .AddAttribute("MaxTotalRoutes",
                          "Maximum number of route entries across the whole table.",
                          UintegerValue(1024),
                          MakeUintegerAccessor(&DvClRouting::SetMaxTotalRoutes),
                          MakeUintegerChecker<uint32_t>(1, 4096))
            .AddAttribute("ActiveTimeoutFactor",
                          "Multiplier applied to route timeout for active destinations.",
                          DoubleValue(2.0),
                          MakeDoubleAccessor(&DvClRouting::m_activeTimeoutFactor),
                          MakeDoubleChecker<double>(1.0, 10.0))
            .AddAttribute("ActiveDestWindow",
                          "Time window to consider a destination active.",
                          TimeValue(Seconds(600)),
                          MakeTimeAccessor(&DvClRouting::m_activeDestWindow),
                          MakeTimeChecker())
            .AddAttribute("PueyoValidationTrace",
                          "Emit detailed validation traces for Pueyo beacon-path auditing.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClRouting::m_pueyoValidationTrace),
                          MakeBooleanChecker())
            // §BatBeacon attributes ───────────────────────────────────────────────────
            .AddAttribute("UseBeaconSoC",
                          "§BatBeacon: use neighbor SoC from received beacons for Psi(b_j) in composite cost. true=real SoC (default CMP). false=full battery.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClRouting::m_useBeaconSoC),
                          MakeBooleanChecker())
            .AddAttribute("SoCHysteresisPercent",
                          "§BatBeacon: min SoC change [0-50%] before updating composite link cost. 0=no hysteresis (default). 5=recommended for CMP stability.",
                          UintegerValue(0),
                          MakeUintegerAccessor(&DvClRouting::m_socHysteresisPercent),
                          MakeUintegerChecker<uint8_t>(0, 50))
            // §DC-aware attributes ────────────────────────────────────────────────────
            .AddAttribute("UseDcAwareRouting",
                          "§DC-aware: apply duty-cycle feasibility filter in next-hop selection. true=avoid next-hops with DC budget below threshold (DV-CL). false=ignore DC in routing (default).",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClRouting::m_useDcAwareRouting),
                          MakeBooleanChecker())
            .AddAttribute("DcFeasibilityThreshold",
                          "§DC-aware: DC-remaining threshold [0-100%]. Next-hops below this are deemed infeasible and avoided unless no alternative exists. Default 20%.",
                          UintegerValue(20),
                          MakeUintegerAccessor(&DvClRouting::m_dcFeasibilityThreshold),
                          MakeUintegerChecker<uint8_t>(0, 100));
    return tid;
}

DvClRouting::DvClRouting()
{
    m_rng = CreateObject<UniformRandomVariable>();
}

void
DvClRouting::SetNodeId(NodeId id)
{
    m_nodeId = id;
    // [B4] Stream determinista por-nodo para reproducibilidad del tie-break DV.
    // Rango routing: 2_000_000 + nodeId.
    if (m_rng)
    {
        m_rng->SetStream(static_cast<int64_t>(2000000) + static_cast<int64_t>(id));
    }
}

void
DvClRouting::SetRouteTimeout(Time timeout)
{
    m_routeTimeout = timeout;
    m_expireWindow = timeout;
}

void
DvClRouting::SetRouteSwitchMinDeltaX100(uint16_t deltaX100)
{
    m_routeSwitchMinDeltaX100 = deltaX100;
}

void
DvClRouting::SetInitTtl(uint8_t ttl)
{
    m_initTtl = ttl;
}

void
DvClRouting::SetMaxRoutes(uint32_t maxRoutes)
{
    m_maxRoutes = maxRoutes;
}

void
DvClRouting::SetSequence(uint32_t seq)
{
    m_sequence = seq;
}

void
DvClRouting::SetExpireWindow(Time expire)
{
    m_expireWindow = expire;
}

void
DvClRouting::SetMaxHops(uint8_t maxHops)
{
    m_maxHops = maxHops;
}

void
DvClRouting::SetAdvertiseAllRoutes(bool enable)
{
    m_advertiseAllRoutes = enable;
}

bool
DvClRouting::GetAdvertiseAllRoutes() const
{
    return m_advertiseAllRoutes;
}

void
DvClRouting::SetAdvertRoutePolicy(const std::string& policy)
{
    std::string normalized = policy;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](char ch) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    });
    if (normalized == "uniform")
    {
        m_advertRoutePolicy = AdvertRoutePolicy::UNIFORM;
        return;
    }
    if (normalized == "cost_weighted")
    {
        m_advertRoutePolicy = AdvertRoutePolicy::COST_WEIGHTED;
        return;
    }
    m_advertRoutePolicy = AdvertRoutePolicy::TOP_SCORE;
}

std::string
DvClRouting::GetAdvertRoutePolicy() const
{
    switch (m_advertRoutePolicy)
    {
    case AdvertRoutePolicy::UNIFORM:
        return "uniform";
    case AdvertRoutePolicy::COST_WEIGHTED:
        return "cost_weighted";
    case AdvertRoutePolicy::TOP_SCORE:
    default:
        return "top_score";
    }
}

void
DvClRouting::SetMetricMode(const std::string& mode)
{
    std::string normalized = mode;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](char ch) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    });
    if (normalized == "toa_only")
    {
        m_metricMode = MetricMode::TOA_ONLY;
        return;
    }
    m_metricMode = MetricMode::COMPOSITE_SCORE;
}

std::string
DvClRouting::GetMetricMode() const
{
    return (m_metricMode == MetricMode::TOA_ONLY) ? "toa_only" : "composite_score";
}

void
DvClRouting::SetCostEncoding(const std::string& mode)
{
    std::string normalized = mode;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](char ch) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    });
    if (normalized == "cost255")
    {
        m_costEncoding = CostEncoding::COST255;
        return;
    }
    if (normalized == "score255")
    {
        m_costEncoding = CostEncoding::SCORE255;
        return;
    }
    m_costEncoding = CostEncoding::SCORE100;
}

std::string
DvClRouting::GetCostEncoding() const
{
    switch (m_costEncoding)
    {
    case CostEncoding::COST255:
        return "cost255";
    case CostEncoding::SCORE255:
        return "score255";
    case CostEncoding::SCORE100:
    default:
        return "score100";
    }
}

void
DvClRouting::SetMaxRoutesPerDestination(uint32_t maxRoutesPerDestination)
{
    m_maxRoutesPerDestination = std::clamp<uint32_t>(maxRoutesPerDestination, 1u, 2u);
    if (m_maxRoutesPerDestination <= 1)
    {
        m_backupRoutes.clear();
    }
}

void
DvClRouting::SetMaxTotalRoutes(uint32_t maxTotalRoutes)
{
    m_maxTotalRoutes = std::max<uint32_t>(1u, maxTotalRoutes);
    EnforceRouteTableLimits();
}

void
DvClRouting::SetSinkNodeId(NodeId id)
{
    m_sinkNodeId = id;
    m_hasSink = true;
}

void
DvClRouting::SetUseDcAwareRouting(bool enable)
{
    m_useDcAwareRouting = enable;
}

void
DvClRouting::SetDcFeasibilityThreshold(uint8_t thresholdPct)
{
    m_dcFeasibilityThreshold = std::min<uint8_t>(thresholdPct, 100);
}

void
DvClRouting::NotifyDestinationActive(NodeId dest)
{
    m_activeDestinations.insert(dest);
    m_lastDestActivity[dest] = Simulator::Now();
}

bool
DvClRouting::IsDestinationActive(NodeId dest) const
{
    auto it = m_lastDestActivity.find(dest);
    if (it == m_lastDestActivity.end())
    {
        return false;
    }
    return (Simulator::Now() - it->second) <= m_activeDestWindow;
}

bool
DvClRouting::HasAnyRoute(NodeId dest) const
{
    return (m_routes.find(dest) != m_routes.end()) || (m_backupRoutes.find(dest) != m_backupRoutes.end());
}

bool
DvClRouting::IsRouteExpired(NodeId dest) const
{
    auto it = m_routes.find(dest);
    if (it != m_routes.end())
    {
        return IsExpired(it->second);
    }
    auto itBackup = m_backupRoutes.find(dest);
    if (itBackup != m_backupRoutes.end())
    {
        return IsExpired(itBackup->second);
    }
    return false;
}

bool
DvClRouting::IsRouteUsable(const RouteEntry& entry, uint8_t hopLimit) const
{
    if (entry.scoreX100 == 0)
    {
        return false;
    }
    if (entry.hops == 0 || entry.hops > hopLimit)
    {
        return false;
    }
    if (IsExpired(entry))
    {
        return false;
    }
    return true;
}

bool
DvClRouting::IsNextHopDcFeasible(NodeId nextHop) const
{
    // Filtro desactivado → toda ruta es factible (comportamiento heredado).
    if (!m_useDcAwareRouting)
    {
        return true;
    }
    auto it = m_neighborDcRemaining.find(nextHop);
    if (it == m_neighborDcRemaining.end())
    {
        return true; // sin info del vecino → no penalizar por desconocimiento
    }
    if (it->second == 0xFF)
    {
        return true; // 0xFF = N/A → tratar como factible
    }
    return it->second > m_dcFeasibilityThreshold;
}

const RouteEntry*
DvClRouting::GetRoute(NodeId dest)
{
    const uint8_t hopLimit = std::max<uint8_t>(1, m_maxHops);
    auto itPrimary = m_routes.find(dest);
    auto itBackup = m_backupRoutes.find(dest);

    if (itPrimary == m_routes.end() && itBackup == m_backupRoutes.end())
    {
        return nullptr;
    }

    // §DC-aware: filtro de factibilidad por presupuesto de duty cycle.
    // Si la ruta primaria es usable pero su next-hop NO es factible por DC,
    // y existe un backup usable cuyo next-hop SÍ es factible, se prefiere el
    // backup (intercambio primaria↔backup). Si ningún candidato es factible,
    // no se hace nada y se cae al comportamiento normal (fallback a primaria).
    if (m_useDcAwareRouting && itPrimary != m_routes.end() &&
        IsRouteUsable(itPrimary->second, hopLimit) &&
        !IsNextHopDcFeasible(itPrimary->second.nextHop))
    {
        auto itBk = m_backupRoutes.find(dest);
        if (itBk != m_backupRoutes.end() && IsRouteUsable(itBk->second, hopLimit) &&
            IsNextHopDcFeasible(itBk->second.nextHop))
        {
            std::swap(itPrimary->second, itBk->second);
            NotifyChange(itPrimary->second, "DC_FEASIBILITY_SWAP");
            return &itPrimary->second;
        }
        // Sin backup factible → fallback: continúa y devuelve la primaria igual.
    }

    if (itPrimary != m_routes.end() && IsRouteUsable(itPrimary->second, hopLimit))
    {
        return &itPrimary->second;
    }

    if (itPrimary != m_routes.end())
    {
        RouteEntry expired = itPrimary->second;
        m_routes.erase(itPrimary);
        if (expired.scoreX100 > 0)
        {
            expired.scoreX100 = 0;
            expired.costX1000 = 1000;
            expired.rawMetric = 0.0;
            expired.lastUpdate = Simulator::Now();
            expired.expiryTime = Simulator::Now() + m_routeTimeout;
            SetHoldDown(expired.destination);
            NotifyChange(expired, "POISON");
        }
    }

    itBackup = m_backupRoutes.find(dest);
    if (itBackup != m_backupRoutes.end() && IsRouteUsable(itBackup->second, hopLimit))
    {
        m_routes[dest] = itBackup->second;
        m_backupRoutes.erase(itBackup);
        m_backupPromotionsTotal++;
        NotifyChange(m_routes[dest], "PROMOTE_BACKUP");
        return &m_routes[dest];
    }

    if (itBackup != m_backupRoutes.end())
    {
        m_backupRoutes.erase(itBackup);
    }
    return nullptr;
}

std::size_t
DvClRouting::GetRouteCount() const
{
    return m_routes.size() + m_backupRoutes.size();
}

std::size_t
DvClRouting::GetPrimaryRouteCount() const
{
    return m_routes.size();
}

std::size_t
DvClRouting::GetBackupRouteCount() const
{
    return m_backupRoutes.size();
}

std::size_t
DvClRouting::GetDestinationsWithBackupCount() const
{
    return m_backupRoutes.size();
}

uint64_t
DvClRouting::GetRouteEvictionsTotal() const
{
    return m_routeEvictionsTotal;
}

uint64_t
DvClRouting::GetBackupPromotionsTotal() const
{
    return m_backupPromotionsTotal;
}

std::vector<RouteEntry>
DvClRouting::GetPrimaryRoutesSnapshot() const
{
    std::vector<RouteEntry> out;
    out.reserve(m_routes.size());
    for (const auto& kv : m_routes)
    {
        out.push_back(kv.second);
    }
    return out;
}

std::vector<RouteEntry>
DvClRouting::GetBackupRoutesSnapshot() const
{
    std::vector<RouteEntry> out;
    out.reserve(m_backupRoutes.size());
    for (const auto& kv : m_backupRoutes)
    {
        out.push_back(kv.second);
    }
    return out;
}

Time
DvClRouting::GetEffectiveTimeout(const RouteEntry& entry) const
{
    if (IsDestinationActive(entry.destination) && m_activeTimeoutFactor > 1.0)
    {
        return m_routeTimeout * m_activeTimeoutFactor;
    }
    return m_routeTimeout;
}

bool
DvClRouting::IsInHoldDown(NodeId dest) const
{
    auto it = m_holdDownUntil.find(dest);
    if (it == m_holdDownUntil.end())
    {
        return false;
    }
    return Simulator::Now() < it->second;
}

void
DvClRouting::SetHoldDown(NodeId dest)
{
    m_holdDownUntil[dest] = Simulator::Now() + m_holdDownTime;
}

void
DvClRouting::CleanExpiredHoldDowns()
{
    const Time now = Simulator::Now();
    for (auto it = m_holdDownUntil.begin(); it != m_holdDownUntil.end();)
    {
        if (now >= it->second)
        {
            it = m_holdDownUntil.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void
DvClRouting::SetRouteChangeCallback(RouteChangeCallback cb)
{
    m_routeChangeCallback = cb;
}

void
DvClRouting::SetFloodCallback(FloodCallback cb)
{
    m_floodCallback = cb;
}

void
DvClRouting::SetLocalEnergyFractionCallback(LocalEnergyFractionCallback cb)
{
    m_localEnergyFractionCallback = cb;
}

bool
DvClRouting::HasRoute(NodeId dest)
{
    const RouteEntry* route = GetRoute(dest);
    return route != nullptr;
}

NodeId
DvClRouting::LookupNextHop(NodeId dest)
{
    const RouteEntry* route = GetRoute(dest);
    return route ? route->nextHop : 0;
}

double
DvClRouting::GetRouteCost(NodeId dest)
{
    const RouteEntry* route = GetRoute(dest);
    if (!route)
    {
        return 1.0;
    }
    return GetComparableMetric(*route);
}

double
DvClRouting::GetComparableMetric(const RouteEntry& entry) const
{
    if (m_metricMode == MetricMode::TOA_ONLY)
    {
        return static_cast<double>(entry.toaCostUnits);
    }
    return entry.rawMetric;
}

double
DvClRouting::GetLocalEnergyFraction() const
{
    if (m_localEnergyFractionCallback.IsNull())
    {
        return -1.0;
    }
    return m_localEnergyFractionCallback();
}

// SoC-wire: out-of-class definition required for C++14 ODR with static constexpr arrays.
constexpr double DvClRouting::kMaxToaUs[6];

double
DvClRouting::NormalizeToaUs(double toaUs, uint8_t sf)
{
    // T_hat_ij = ToA_ij / ToA_max(SF)  in [0, 1]  -- thesis 4.2
    const uint8_t sfClamped = std::clamp<uint8_t>(sf, 7, 12);
    const size_t idx = static_cast<size_t>(sfClamped - 7);
    const double maxToaForSf = kMaxToaUs[idx];
    return std::min(toaUs / maxToaForSf, 1.0);
}

double
DvClRouting::BattMvToEFrac(uint16_t battMv)
{
    // Li-Ion 18650: 3000 mV = 0%, 4200 mV = 100%.
    // battMv == 0 means "unknown" (legacy) -> treat as full battery (no penalty).
    if (battMv == 0)
    {
        return 1.0;
    }
    constexpr double vMin = 3000.0;
    constexpr double vMax = 4200.0;
    return std::clamp((static_cast<double>(battMv) - vMin) / (vMax - vMin), 0.0, 1.0);
}

double
DvClRouting::ComputeThesisLinkCost(double toaUs, uint8_t sf, double neighborEFrac) const
{
    // Thesis 4.2: incremental link cost  Delta_C_ij = alpha*T_hat_ij + beta + delta*Psi(b_j)
    //   alpha = m_compositeWToa   (0.60)  -- ToA normalisation weight
    //   beta  = m_compositeWHop   (0.15)  -- fixed per-hop constant (NOT multiplied by hops)
    //   delta = m_compositeWEnergy (0.25) -- energy penalty weight
    //   Psi(b_j) = piecewise penalty [0,1] on NEXT-HOP battery b_j
    const double toaNorm    = NormalizeToaUs(toaUs, sf);              // T_hat in [0,1]
    const double toaCost    = m_compositeWToa * toaNorm;               // alpha * T_hat
    const double hopCost    = m_compositeWHop;                         // beta (constant)
    const double energyCost = ComputeCompositeEnergyPenalty(neighborEFrac); // delta*Psi(b_j)
    return toaCost + hopCost + energyCost;
}

double
DvClRouting::ComputeCompositeEnergyPenalty(double energyFraction) const
{
    if (!std::isfinite(energyFraction) || energyFraction < 0.0)
    {
        return 0.0;
    }

    const double lo = std::clamp(std::min(m_energyLo, m_energyHi), 0.0, 1.0);
    const double hi = std::clamp(std::max(m_energyLo, m_energyHi), 0.0, 1.0);
    const double e = std::clamp(energyFraction, 0.0, 1.0);
    if (e >= hi)
    {
        return 0.0;
    }
    if (e <= lo)
    {
        return m_compositeWEnergy * m_energyMaxPenalty;
    }

    const double denom = std::max(hi - lo, 1e-9);
    const double x = std::clamp((hi - e) / denom, 0.0, 1.0);
    const double penalty = m_energyMaxPenalty * std::pow(x, m_energyPow);
    return m_compositeWEnergy * penalty;
}

uint32_t
DvClRouting::QuantizeCompositeMetric(double rawMetric) const
{
    if (!std::isfinite(rawMetric) || rawMetric <= 0.0)
    {
        return 0;
    }

    const uint32_t maxMetric = MaxAdvertMetricValue();
    const double step = std::max(m_compositeCostStep, 1e-9);
    const uint32_t q = static_cast<uint32_t>(std::llround(rawMetric / step));
    return std::clamp<uint32_t>(q, 1u, maxMetric);
}

uint16_t
DvClRouting::EncodeCompositeMetric(double rawMetric) const
{
    const uint32_t q = QuantizeCompositeMetric(rawMetric);
    if (q == 0)
    {
        return 0;
    }

    const uint32_t maxMetric = MaxAdvertMetricValue();
    if (m_costEncoding == CostEncoding::COST255)
    {
        return static_cast<uint16_t>(q);
    }
    return static_cast<uint16_t>((maxMetric + 1u) - q);
}

double
DvClRouting::DecodeCompositeMetric(uint16_t metric) const
{
    if (metric == 0)
    {
        return std::numeric_limits<double>::infinity();
    }

    uint32_t q = metric;
    const uint32_t maxMetric = MaxAdvertMetricValue();
    if (m_costEncoding == CostEncoding::SCORE255 || m_costEncoding == CostEncoding::SCORE100)
    {
        q = (maxMetric + 1u) - std::clamp<uint32_t>(metric, 1u, maxMetric);
    }
    return static_cast<double>(q) * std::max(m_compositeCostStep, 1e-9);
}

uint16_t
DvClRouting::ScoreToCostX1000(uint16_t scoreX100) const
{
    const double score = std::clamp(static_cast<double>(scoreX100) / 100.0, 0.0, 1.0);
    const double cost = std::clamp(1.0 - score, 0.0, 1.0);
    return static_cast<uint16_t>(std::round(cost * 1000.0));
}

uint16_t
DvClRouting::ToaHopCostUnits(uint8_t sf) const
{
    const uint8_t sfNorm = std::clamp<uint8_t>(sf, 7, 12);
    const uint8_t shift = static_cast<uint8_t>(sfNorm - 7);
    return static_cast<uint16_t>(1u << shift);
}

uint16_t
DvClRouting::ScoreToToaPathUnits(uint16_t scoreX100) const
{
    if (scoreX100 == 0)
    {
        return std::numeric_limits<uint16_t>::max();
    }
    const uint32_t maxHopUnits = 1u << 5; // SF7..SF12 -> 2^(12-7)=32
    const uint32_t maxPathUnits = std::max<uint32_t>(1u, static_cast<uint32_t>(m_initTtl) * maxHopUnits);
    const double normCost = std::clamp(1.0 - (static_cast<double>(scoreX100) / 100.0), 0.0, 1.0);
    const uint32_t units = static_cast<uint32_t>(std::round(normCost * static_cast<double>(maxPathUnits)));
    return static_cast<uint16_t>(std::max<uint32_t>(1u, std::min<uint32_t>(units, 0xFFFFu)));
}

uint16_t
DvClRouting::ToaUnitsToScoreX100(uint32_t units) const
{
    const uint32_t maxHopUnits = 1u << 5;
    const uint32_t maxPathUnits = std::max<uint32_t>(1u, static_cast<uint32_t>(m_initTtl) * maxHopUnits);
    const double normCost =
        std::clamp(static_cast<double>(units) / static_cast<double>(maxPathUnits), 0.0, 1.0);
    const uint16_t score = static_cast<uint16_t>(std::round((1.0 - normCost) * 100.0));
    return std::max<uint16_t>(1, score);
}

uint16_t
DvClRouting::ToaUnitsToCostX1000(uint32_t units) const
{
    const uint32_t maxHopUnits = 1u << 5;
    const uint32_t maxPathUnits = std::max<uint32_t>(1u, static_cast<uint32_t>(m_initTtl) * maxHopUnits);
    const double normCost =
        std::clamp(static_cast<double>(units) / static_cast<double>(maxPathUnits), 0.0, 1.0);
    return static_cast<uint16_t>(std::round(normCost * 1000.0));
}

uint16_t
DvClRouting::MaxAdvertMetricValue() const
{
    if (m_costEncoding == CostEncoding::COST255 || m_costEncoding == CostEncoding::SCORE255)
    {
        return 255;
    }
    return 100;
}

uint16_t
DvClRouting::ToaUnitsToAdvertMetric(uint32_t units) const
{
    if (units == 0)
    {
        return 0;
    }
    if (m_costEncoding == CostEncoding::COST255)
    {
        return static_cast<uint16_t>(std::clamp<uint32_t>(units, 1u, 255u));
    }
    if (m_costEncoding == CostEncoding::SCORE255)
    {
        const uint32_t sat = std::clamp<uint32_t>(units, 1u, 255u);
        return static_cast<uint16_t>(std::clamp<int32_t>(256 - static_cast<int32_t>(sat), 1, 255));
    }
    return ToaUnitsToScoreX100(units);
}

uint16_t
DvClRouting::AdvertMetricToToaPathUnits(uint16_t metric) const
{
    if (metric == 0)
    {
        return std::numeric_limits<uint16_t>::max();
    }
    if (m_costEncoding == CostEncoding::COST255)
    {
        return static_cast<uint16_t>(std::clamp<uint16_t>(metric, 1, 255));
    }
    if (m_costEncoding == CostEncoding::SCORE255)
    {
        return static_cast<uint16_t>(std::clamp<int32_t>(256 - static_cast<int32_t>(metric), 1, 255));
    }
    return ScoreToToaPathUnits(metric);
}

bool
DvClRouting::IsCandidateBetter(const RouteEntry& candidate, const RouteEntry& current, bool* tie) const
{
    if (tie)
    {
        *tie = false;
    }
    if (candidate.scoreX100 == 0 && current.scoreX100 > 0)
    {
        return false;
    }
    if (candidate.scoreX100 > 0 && current.scoreX100 == 0)
    {
        return true;
    }

    const double candidateMetric = GetComparableMetric(candidate);
    const double currentMetric = GetComparableMetric(current);
    if (candidateMetric < currentMetric)
    {
        return true;
    }
    if (candidateMetric > currentMetric)
    {
        return false;
    }

    if (candidate.sf < current.sf)
    {
        return true;
    }
    if (candidate.sf > current.sf)
    {
        return false;
    }

    if (tie)
    {
        *tie = true;
    }
    if (candidate.nextHop == current.nextHop)
    {
        return false;
    }
    return m_rng ? (m_rng->GetValue() < 0.5) : false;
}

bool
DvClRouting::SelectWorseRouteToEvict(NodeId* outDest, bool* outBackup) const
{
    const RouteEntry* worst = nullptr;
    NodeId worstDest = 0;
    bool worstIsBackup = false;
    auto consider = [&](NodeId dest, const RouteEntry& entry, bool backup) {
        if (!worst)
        {
            worst = &entry;
            worstDest = dest;
            worstIsBackup = backup;
            return;
        }
        if (GetComparableMetric(entry) > GetComparableMetric(*worst))
        {
            worst = &entry;
            worstDest = dest;
            worstIsBackup = backup;
            return;
        }
        if (GetComparableMetric(entry) == GetComparableMetric(*worst) &&
            entry.lastUpdate < worst->lastUpdate)
        {
            worst = &entry;
            worstDest = dest;
            worstIsBackup = backup;
        }
    };

    for (const auto& kv : m_backupRoutes)
    {
        consider(kv.first, kv.second, true);
    }
    if (!worst)
    {
        for (const auto& kv : m_routes)
        {
            consider(kv.first, kv.second, false);
        }
    }
    if (!worst)
    {
        return false;
    }
    if (outDest)
    {
        *outDest = worstDest;
    }
    if (outBackup)
    {
        *outBackup = worstIsBackup;
    }
    return true;
}

void
DvClRouting::EnforceRouteTableLimits()
{
    while ((m_routes.size() + m_backupRoutes.size()) > m_maxTotalRoutes)
    {
        NodeId evictDest = 0;
        bool evictBackup = false;
        if (!SelectWorseRouteToEvict(&evictDest, &evictBackup))
        {
            break;
        }

        if (evictBackup)
        {
            auto it = m_backupRoutes.find(evictDest);
            if (it != m_backupRoutes.end())
            {
                NotifyChange(it->second, "EVICT_GLOBAL_LIMIT");
                m_backupRoutes.erase(it);
                m_routeEvictionsTotal++;
            }
            continue;
        }

        auto it = m_routes.find(evictDest);
        if (it != m_routes.end())
        {
            NotifyChange(it->second, "EVICT_GLOBAL_LIMIT");
            m_routes.erase(it);
            m_routeEvictionsTotal++;
        }
    }
}

void
DvClRouting::UpdateFromDvMsg(const DvMessage& msg, const NeighborLinkInfo& link)
{
    // FIX A3: Validación de entradas
    static constexpr uint32_t kMaxEntriesPerMsg = 100; // Límite razonable
    static constexpr uint8_t kMinSf = 7;
    static constexpr uint8_t kMaxSf = 12;
    static constexpr NodeId kBroadcastId = 0xFFFF;

    // Validar origen (no aceptar mensajes de nosotros mismos ni broadcast)
    if (msg.origin == m_nodeId)
    {
        return;
    }
    // NOTA: NodeId=0 es VÁLIDO en ns-3 (es el primer nodo)
    if (msg.origin == kBroadcastId)
    {
        NS_LOG_DEBUG("UpdateFromDvMsg: invalid origin=" << msg.origin);
        return;
    }

    // Validar vecino (NodeId=0 es válido en ns-3)
    if (link.neighbor == kBroadcastId)
    {
        NS_LOG_DEBUG("UpdateFromDvMsg: invalid neighbor=" << link.neighbor);
        return;
    }

    // Validar número de entradas (prevenir DoS)
    if (msg.entries.size() > kMaxEntriesPerMsg)
    {
        NS_LOG_WARN("UpdateFromDvMsg: too many entries=" << msg.entries.size() << " from origin="
                                                         << msg.origin << ", truncating");
    }

    // Validar secuencia (evitar replay de mensajes viejos)
    auto itSeq = m_lastSeq.find(msg.origin);
    if (itSeq != m_lastSeq.end() && msg.sequence <= itSeq->second)
    {
        NS_LOG_INFO("DVTRACE_RX_DROP reason=old_seq node=" << m_nodeId << " origin=" << msg.origin
                                                             << " seq=" << msg.sequence
                                                             << " last=" << itSeq->second);
        NS_LOG_DEBUG("Ignore old seq=" << msg.sequence << " <= last=" << itSeq->second
                                       << " from src=" << msg.origin);
        return;
    }
    m_lastSeq[msg.origin] = msg.sequence;

    // Validar y sanitizar link info
    const uint8_t hopLimit = std::max<uint8_t>(1, m_maxHops);
    uint8_t linkSf = std::clamp(link.sf, kMinSf, kMaxSf);
    const uint16_t kMaxAdvertMetric = MaxAdvertMetricValue();
    uint8_t linkHops = std::min(link.hops, hopLimit);
    // SoC-wire: local energy fraction no longer used for routing cost; b_j comes from
    // link.batt_mV (set by the SoC wire byte in the received beacon). Kept below only
    // for potential future diagnostics -- suppressed with (void) to avoid unused-variable error.
    const double localEnergyFraction = GetLocalEnergyFraction();
    (void)localEnergyFraction;

    // §DC-aware: registrar el DC restante del vecino emisor (del beacon recibido).
    // Este mapa es la fuente fresca consultada por IsNextHopDcFeasible() en GetRoute().
    m_neighborDcRemaining[link.neighbor] = link.dc_remaining;

    // Crear entrada para ruta directa al vecino
    RouteEntry direct;
    direct.destination = link.neighbor;
    direct.nextHop = link.neighbor;
    direct.seqNum = link.sequence;
    direct.hops = std::min<uint8_t>(linkHops, m_initTtl);
    direct.sf = linkSf;
    direct.toaUs = link.toaUs;
    // REMOVED: direct.rssiDbm - eliminado de struct
    direct.batt_mV = link.batt_mV;
    direct.nextHopDcRemaining = link.dc_remaining;  // §DC-aware
    if (m_metricMode == MetricMode::TOA_ONLY)
    {
        const uint32_t units = ToaHopCostUnits(linkSf);
        direct.toaCostUnits = units;
        direct.rawMetric = static_cast<double>(units);
        direct.costX1000 = ToaUnitsToCostX1000(units);
        direct.scoreX100 = ToaUnitsToAdvertMetric(units);
    }
    else
    {
        // §BatBeacon: b_j = next-hop battery fraction for Psi(b_j) in composite cost.
        // UseBeaconSoC=false -> assume full battery (no energy penalty, TOA-like).
        // SoCHysteresisPercent>0 -> only update cost when SoC changed >= threshold.
        double neighborEFrac = 1.0; // default: full battery, Psi=0
        if (m_useBeaconSoC)
        {
            const double rawFrac = BattMvToEFrac(link.batt_mV);
            if (m_socHysteresisPercent == 0)
            {
                neighborEFrac = rawFrac; // no hysteresis: always use current SoC
            }
            else
            {
                const uint8_t newSoc8 = static_cast<uint8_t>(
                    std::clamp(std::round(rawFrac * 100.0), 0.0, 100.0));
                auto& lastSoc = m_lastNeighborSoC[link.neighbor]; // inserts 0 first time
                const uint8_t delta = (newSoc8 >= lastSoc)
                                      ? static_cast<uint8_t>(newSoc8 - lastSoc)
                                      : static_cast<uint8_t>(lastSoc - newSoc8);
                if (delta >= m_socHysteresisPercent)
                {
                    lastSoc = newSoc8; // significant change: commit new SoC
                }
                // else: reuse lastSoc without updating (suppresses micro-churn)
                neighborEFrac = static_cast<double>(lastSoc) / 100.0;
            }
        }
        direct.toaCostUnits = 0;  // unused in COMPOSITE_SCORE mode
        direct.rawMetric = ComputeThesisLinkCost(link.toaUs,
                                                  static_cast<uint8_t>(linkSf),
                                                  neighborEFrac);
        direct.costX1000 = static_cast<uint16_t>(
            std::clamp<uint32_t>(static_cast<uint32_t>(std::llround(direct.rawMetric * 1000.0)),
                                 0u,
                                 std::numeric_limits<uint16_t>::max()));
        direct.scoreX100 = EncodeCompositeMetric(direct.rawMetric);
    }
    direct.lastUpdate = Simulator::Now();
    direct.expiryTime = direct.lastUpdate + m_expireWindow;
    direct.nextHopMac = link.mac;
    m_inboundRoutes[direct.destination] = direct;
    if (m_pueyoValidationTrace)
    {
        NS_LOG_INFO("PUEYO_VAL_INBOUND node=" << m_nodeId << " origin=" << msg.origin
                                                << " dest=" << direct.destination
                                                << " nextHop=" << direct.nextHop
                                                << " sf=" << unsigned(direct.sf)
                                                << " score=" << direct.scoreX100
                                                << " raw=" << direct.rawMetric
                                                << " hops=" << unsigned(direct.hops));
    }
    if (!IsInHoldDown(direct.destination))
    {
        UpdateRoute(direct);
    }
    else
    {
        NS_LOG_DEBUG("UpdateFromDvMsg: ignoring direct update for dst=" << direct.destination
                                                                        << " (in hold-down)");
    }

    // Procesar entradas (con límite)
    const size_t maxToProcess = std::min<size_t>(msg.entries.size(), kMaxEntriesPerMsg);
    for (size_t i = 0; i < maxToProcess; ++i)
    {
        const auto& entry = msg.entries[i];

        // Validar destino
        if (entry.destination == m_nodeId)
        {
            continue; // No aprender ruta a sí mismo
        }
        if (entry.destination == kBroadcastId)
        {
            continue; // Destino broadcast inválido
        }

        // FIX B2: Verificar hold-down (ignorar actualizaciones de rutas recientemente falladas)
        if (IsInHoldDown(entry.destination))
        {
            NS_LOG_DEBUG("UpdateFromDvMsg: ignoring update for dst=" << entry.destination
                                                                     << " (in hold-down)");
            continue;
        }

        // v2 score-only: destination+score vienen on-air; el resto se toma del enlace local.
        uint16_t entryScore = std::min(entry.scoreX100, kMaxAdvertMetric);
        if (m_pueyoValidationTrace)
        {
            NS_LOG_INFO("PUEYO_VAL_UPDATE_INPUT node=" << m_nodeId << " origin=" << msg.origin
                                                         << " dest=" << entry.destination
                                                         << " entryScore=" << entryScore
                                                         << " linkSf=" << unsigned(linkSf));
        }
        const uint16_t candidateHopsRaw = static_cast<uint16_t>(linkHops) + 1;
        if (candidateHopsRaw > hopLimit)
        {
            continue;
        }
        const uint8_t candidateHops = static_cast<uint8_t>(candidateHopsRaw);

        RouteEntry candidate;
        candidate.destination = entry.destination;
        candidate.nextHop = msg.origin;
        candidate.seqNum = msg.sequence;
        // En score-only, hops/sf/toa son bookkeeping local del salto hacia next-hop.
        candidate.hops = std::min<uint8_t>(candidateHops, m_initTtl);
        candidate.sf = linkSf;
        candidate.toaUs = link.toaUs;
        // REMOVED: candidate.rssiDbm - eliminado de struct
        candidate.batt_mV = 0;
        if (entryScore == 0)
        {
            // Preserve poison semantics: unreachable path must stay unreachable.
            candidate.toaCostUnits = 0;
            candidate.rawMetric = 0.0;
            candidate.scoreX100 = 0;
            candidate.costX1000 = 1000;
            candidate.batt_mV = 0;
        }
        else
        {
            if (m_metricMode == MetricMode::TOA_ONLY)
            {
                const uint32_t linkUnits = ToaHopCostUnits(linkSf);
                const uint32_t pathUnits = AdvertMetricToToaPathUnits(entryScore);
                if (pathUnits == std::numeric_limits<uint16_t>::max())
                {
                    candidate.toaCostUnits = 0;
                    candidate.rawMetric = 0.0;
                    candidate.scoreX100 = 0;
                    candidate.costX1000 = 1000;
                }
                else
                {
                    const uint32_t totalUnits = std::min<uint32_t>(linkUnits + pathUnits, 0xFFFFu);
                    candidate.toaCostUnits = totalUnits;
                    candidate.rawMetric = static_cast<double>(totalUnits);
                    candidate.costX1000 = ToaUnitsToCostX1000(totalUnits);
                    candidate.scoreX100 = ToaUnitsToAdvertMetric(totalUnits);
                }
            }
            else
            {
                const double pathRaw = DecodeCompositeMetric(entryScore);
                if (!std::isfinite(pathRaw))
                {
                    candidate.toaCostUnits = 0;
                    candidate.rawMetric = 0.0;
                    candidate.scoreX100 = 0;
                    candidate.costX1000 = 1000;
                }
                else
                {
                    // SoC-wire fix: b_j = next-hop energy from link.batt_mV (SoC wire byte).
                    const double neighborEFrac = BattMvToEFrac(link.batt_mV);
                    const double rawMetric =
                        pathRaw + ComputeThesisLinkCost(link.toaUs,
                                                        linkSf,
                                                        neighborEFrac);
                    candidate.toaCostUnits = 0;
                    candidate.rawMetric = rawMetric;
                    candidate.costX1000 = static_cast<uint16_t>(
                        std::clamp<uint32_t>(static_cast<uint32_t>(std::llround(rawMetric * 1000.0)),
                                             0u,
                                             std::numeric_limits<uint16_t>::max()));
                    candidate.scoreX100 = EncodeCompositeMetric(rawMetric);
                }
            }
        }
        candidate.lastUpdate = Simulator::Now();
        candidate.expiryTime = candidate.lastUpdate + m_expireWindow;
        candidate.nextHopMac = link.mac;
        if (m_pueyoValidationTrace)
        {
            NS_LOG_INFO("PUEYO_VAL_UPDATE_CAND node=" << m_nodeId << " origin=" << msg.origin
                                                        << " dest=" << candidate.destination
                                                        << " nextHop=" << candidate.nextHop
                                                        << " sf=" << unsigned(candidate.sf)
                                                        << " score=" << candidate.scoreX100
                                                        << " raw=" << candidate.rawMetric
                                                        << " toaUnits=" << candidate.toaCostUnits
                                                        << " hops=" << unsigned(candidate.hops));
        }
        NS_LOG_INFO("DV_UPDATE node"
                      << m_nodeId << " from=" << msg.origin << " entry.dst=" << entry.destination
                      << " hops=" << unsigned(candidate.hops) << " score=" << candidate.scoreX100);
        UpdateRoute(candidate);
    }
}

void
DvClRouting::FloodDvUpdate()
{
    if (m_floodCallback.IsNull())
    {
        return;
    }
    m_floodCallback(BuildDvMessage());
}

void
DvClRouting::PrintRoutingTable() const
{
    if (m_routes.empty() && m_backupRoutes.empty())
    {
        NS_LOG_INFO("DvClRouting: table empty for node " << m_nodeId);
        return;
    }

    for (const auto& kv : m_routes)
    {
        const RouteEntry& e = kv.second;
        NS_LOG_INFO("  dst=" << e.destination << " via=" << e.nextHop
                             << " hops=" << unsigned(e.hops) << " sf=" << unsigned(e.sf)
                             << " score=" << e.scoreX100 << " seq=" << e.seqNum
                             << " age=" << (Simulator::Now() - e.lastUpdate).GetSeconds() << "s");
    }
    for (const auto& kv : m_backupRoutes)
    {
        const RouteEntry& e = kv.second;
        NS_LOG_INFO("  [backup] dst=" << e.destination << " via=" << e.nextHop
                                      << " hops=" << unsigned(e.hops) << " sf=" << unsigned(e.sf)
                                      << " score=" << e.scoreX100 << " seq=" << e.seqNum
                                      << " age=" << (Simulator::Now() - e.lastUpdate).GetSeconds()
                                      << "s");
    }
}

void
DvClRouting::DebugDumpRoutingTable() const
{
    NS_LOG_INFO("DV_TABLE node" << m_nodeId << ":");
    if (m_routes.empty() && m_backupRoutes.empty())
    {
        NS_LOG_INFO("  (vacía)");
        return;
    }
    for (const auto& kv : m_routes)
    {
        const RouteEntry& e = kv.second;
        NS_LOG_INFO("  dst=" << e.destination << " nextHop=" << e.nextHop
                               << " hops=" << unsigned(e.hops) << " sf=" << unsigned(e.sf)
                               << " score=" << e.scoreX100 << " seq=" << e.seqNum
                               << " age=" << (Simulator::Now() - e.lastUpdate).GetSeconds() << "s");
    }
    for (const auto& kv : m_backupRoutes)
    {
        const RouteEntry& e = kv.second;
        NS_LOG_INFO("  [backup] dst=" << e.destination << " nextHop=" << e.nextHop
                                        << " hops=" << unsigned(e.hops) << " sf="
                                        << unsigned(e.sf) << " score=" << e.scoreX100
                                        << " seq=" << e.seqNum << " age="
                                        << (Simulator::Now() - e.lastUpdate).GetSeconds() << "s");
    }
}

void
DvClRouting::PrintRouteTo(NodeId dest) const
{
    auto it = m_routes.find(dest);
    if (it == m_routes.end() || IsExpired(it->second))
    {
        NS_LOG_INFO("DvClRouting: no route to dest=" << dest << " for node " << m_nodeId);
        return;
    }
    const RouteEntry& e = it->second;
    NS_LOG_INFO("DvClRouting SNAPSHOT node=" << m_nodeId << " dest=" << dest << " nextHop="
                                           << e.nextHop << " hops=" << unsigned(e.hops)
                                           << " sf=" << unsigned(e.sf) << " score=" << e.scoreX100
                                           << " seq=" << e.seqNum);
}

void
DvClRouting::PurgeExpiredRoutes()
{
    auto purgeMap = [this](std::map<NodeId, RouteEntry>& table, const char* poisonAction, const char* expireAction) {
        for (auto it = table.begin(); it != table.end();)
        {
            RouteEntry& entry = it->second;
            if (!IsExpired(entry))
            {
                ++it;
                continue;
            }

            if (entry.scoreX100 > 0)
            {
                entry.scoreX100 = 0;
                entry.costX1000 = 1000;
                entry.rawMetric = 0.0;
                entry.batt_mV = 0;
                entry.lastUpdate = Simulator::Now();
                entry.expiryTime = Simulator::Now() + m_routeTimeout;
                SetHoldDown(entry.destination);
                NotifyChange(entry, poisonAction);
                ++it;
                continue;
            }

            NotifyChange(entry, expireAction);
            it = table.erase(it);
        }
    };

    purgeMap(m_routes, "POISON", "EXPIRE");
    purgeMap(m_backupRoutes, "POISON_BACKUP", "EXPIRE_BACKUP");
    for (auto it = m_inboundRoutes.begin(); it != m_inboundRoutes.end();)
    {
        if (IsExpired(it->second))
        {
            it = m_inboundRoutes.erase(it);
        }
        else
        {
            ++it;
        }
    }
    CleanExpiredHoldDowns();
    EnforceRouteTableLimits();
}

std::vector<RouteAnnouncement>
DvClRouting::GetBestRoutes(uint32_t maxRoutes) const
{
    std::vector<RouteAnnouncement> announcements;
    if (maxRoutes == 0)
    {
        return announcements;
    }
    announcements.reserve(std::min<uint32_t>(maxRoutes, m_routes.size()));
    std::unordered_set<NodeId> included;

    auto appendEntry = [&](const RouteEntry& entry) {
        if (included.count(entry.destination))
        {
            return;
        }
        if (IsExpired(entry))
        {
            return;
        }
        RouteAnnouncement ann;
        ann.destination = entry.destination;
        ann.hops = entry.hops;
        ann.sf = entry.sf;
        ann.scoreX100 = entry.scoreX100;
        ann.batt_mV = entry.batt_mV;
        // REMOVED: ann.rssiDbm - eliminado de struct
        announcements.push_back(ann);
        included.insert(entry.destination);
    };

    // 1) Priorizar destinos activos para no perder rutas usadas por datos.
    if (!m_activeDestinations.empty())
    {
        std::vector<const RouteEntry*> active;
        active.reserve(m_activeDestinations.size());
        for (const NodeId dest : m_activeDestinations)
        {
            if (!IsDestinationActive(dest))
            {
                continue;
            }
            auto it = m_routes.find(dest);
            if (it == m_routes.end() || IsExpired(it->second))
            {
                continue;
            }
            active.push_back(&it->second);
        }
        // FIX C4: Usar partial_sort en lugar de sort+resize para optimización O(n log k) vs O(n log
        // n)
        auto sortEnd = active.begin() + std::min<size_t>(maxRoutes, active.size());
        if (m_metricMode == MetricMode::TOA_ONLY)
        {
            std::partial_sort(active.begin(),
                              sortEnd,
                              active.end(),
                              [this](const RouteEntry* a, const RouteEntry* b) {
                                  if (GetComparableMetric(*a) != GetComparableMetric(*b))
                                  {
                                      return GetComparableMetric(*a) < GetComparableMetric(*b);
                                  }
                                  if (a->sf != b->sf)
                                  {
                                      return a->sf < b->sf;
                                  }
                                  return a->destination < b->destination;
                              });
        }
        else
        {
            std::partial_sort(active.begin(),
                              sortEnd,
                              active.end(),
                              [this](const RouteEntry* a, const RouteEntry* b) {
                                  if (GetComparableMetric(*a) != GetComparableMetric(*b))
                                  {
                                      return GetComparableMetric(*a) < GetComparableMetric(*b);
                                  }
                                  if (a->sf != b->sf)
                                  {
                                      return a->sf < b->sf;
                                  }
                                  return a->destination < b->destination;
                              });
        }
        active.resize(sortEnd - active.begin());
        for (const RouteEntry* entry : active)
        {
            appendEntry(*entry);
            if (announcements.size() >= maxRoutes)
            {
                return announcements;
            }
        }
    }

    // 2) Incluir siempre el sink si existe ruta y no entró como activo.
    if (m_hasSink && !included.count(m_sinkNodeId))
    {
        auto itSink = m_routes.find(m_sinkNodeId);
        if (itSink != m_routes.end() && !IsExpired(itSink->second))
        {
            appendEntry(itSink->second);
            if (announcements.size() >= maxRoutes)
            {
                return announcements;
            }
        }
    }

    // 3) Completar con las rutas mejor puntuadas restantes hasta K.
    // FIX B1: Piggybacked Poisoning - Asegurar que las rutas envenenadas (score=0)
    // tengan prioridad MÁXIMA para ser anunciadas y cortar bucles.

    std::vector<const RouteEntry*> poisonRoutes;
    std::vector<const RouteEntry*> regularRoutes;
    for (const auto& kv : m_routes)
    {
        if (IsExpired(kv.second))
        {
            continue;
        }
        if (included.count(kv.first))
        {
            continue;
        }
        if (kv.second.scoreX100 == 0)
        {
            poisonRoutes.push_back(&kv.second);
        }
        else
        {
            regularRoutes.push_back(&kv.second);
        }
    }

    // Poison announcements keep highest priority to quickly invalidate stale paths.
    std::sort(poisonRoutes.begin(), poisonRoutes.end(), [](const RouteEntry* a, const RouteEntry* b) {
        return a->destination < b->destination;
    });
    for (const RouteEntry* route : poisonRoutes)
    {
        appendEntry(*route);
        if (announcements.size() >= maxRoutes)
        {
            return announcements;
        }
    }

    const uint32_t needed = maxRoutes - announcements.size();
    if (needed == 0 || regularRoutes.empty())
    {
        return announcements;
    }

    if (m_advertRoutePolicy == AdvertRoutePolicy::TOP_SCORE)
    {
        auto sortEnd = regularRoutes.begin() + std::min<size_t>(needed, regularRoutes.size());
        if (m_metricMode == MetricMode::TOA_ONLY)
        {
            std::partial_sort(regularRoutes.begin(),
                              sortEnd,
                              regularRoutes.end(),
                              [this](const RouteEntry* a, const RouteEntry* b) {
                                  if (GetComparableMetric(*a) != GetComparableMetric(*b))
                                  {
                                      return GetComparableMetric(*a) < GetComparableMetric(*b);
                                  }
                                  if (a->sf != b->sf)
                                  {
                                      return a->sf < b->sf;
                                  }
                                  return a->destination < b->destination;
                              });
        }
        else
        {
            std::partial_sort(regularRoutes.begin(),
                              sortEnd,
                              regularRoutes.end(),
                              [this](const RouteEntry* a, const RouteEntry* b) {
                                  if (GetComparableMetric(*a) != GetComparableMetric(*b))
                                  {
                                      return GetComparableMetric(*a) < GetComparableMetric(*b);
                                  }
                                  if (a->sf != b->sf)
                                  {
                                      return a->sf < b->sf;
                                  }
                                  return a->destination < b->destination;
                              });
        }
        regularRoutes.resize(sortEnd - regularRoutes.begin());
        for (const RouteEntry* route : regularRoutes)
        {
            appendEntry(*route);
            if (announcements.size() >= maxRoutes)
            {
                break;
            }
        }
        return announcements;
    }

    // Uniform / cost-weighted stochastic selection without replacement.
    std::vector<const RouteEntry*> pool = regularRoutes;
    while (!pool.empty() && announcements.size() < maxRoutes)
    {
        std::vector<double> weights;
        weights.reserve(pool.size());
        double totalWeight = 0.0;
        for (const RouteEntry* route : pool)
        {
            double w = 1.0;
            if (m_advertRoutePolicy == AdvertRoutePolicy::COST_WEIGHTED)
            {
                w = 1.0 / (0.05 + std::max(0.0, GetComparableMetric(*route)));
            }
            weights.push_back(w);
            totalWeight += w;
        }
        if (totalWeight <= 0.0)
        {
            break;
        }

        const double r = m_rng ? m_rng->GetValue(0.0, totalWeight) : 0.0;
        double acc = 0.0;
        std::size_t chosen = 0;
        for (std::size_t i = 0; i < weights.size(); ++i)
        {
            acc += weights[i];
            if (r <= acc)
            {
                chosen = i;
                break;
            }
        }

        appendEntry(*pool[chosen]);
        pool.erase(pool.begin() + static_cast<std::ptrdiff_t>(chosen));
    }
    return announcements;
}

std::vector<RouteAnnouncement>
DvClRouting::GetBestRoutesPueyo(uint32_t maxRoutes) const
{
    std::vector<RouteAnnouncement> announcements;
    if (m_pueyoValidationTrace)
    {
        NS_LOG_INFO("PUEYO_VAL_GETBEST node=" << m_nodeId << " maxRoutes=" << maxRoutes);
    }
    if (maxRoutes == 0)
    {
        return announcements;
    }

    std::map<NodeId, RouteEntry> candidates;
    auto maybeInsert = [&](const RouteEntry& entry) {
        if (IsExpired(entry))
        {
            return;
        }
        auto it = candidates.find(entry.destination);
        if (it == candidates.end())
        {
            candidates.emplace(entry.destination, entry);
            return;
        }
        const double candidateMetric = (entry.scoreX100 == 0)
                                           ? static_cast<double>(MaxAdvertMetricValue())
                                           : GetComparableMetric(entry);
        const double currentMetric = (it->second.scoreX100 == 0)
                                         ? static_cast<double>(MaxAdvertMetricValue())
                                         : GetComparableMetric(it->second);
        if (candidateMetric < currentMetric ||
            (candidateMetric == currentMetric && entry.sf < it->second.sf))
        {
            it->second = entry;
        }
    };

    for (const auto& kv : m_routes)
    {
        if (m_pueyoValidationTrace)
        {
            const auto& entry = kv.second;
            NS_LOG_INFO("PUEYO_VAL_ROUTE_POOL node=" << m_nodeId << " source=route"
                                                       << " dest=" << entry.destination
                                                       << " score=" << entry.scoreX100
                                                       << " sf=" << unsigned(entry.sf)
                                                       << " raw=" << entry.rawMetric);
        }
        maybeInsert(kv.second);
    }
    for (const auto& kv : m_inboundRoutes)
    {
        if (m_pueyoValidationTrace)
        {
            const auto& entry = kv.second;
            NS_LOG_INFO("PUEYO_VAL_ROUTE_POOL node=" << m_nodeId << " source=inbound"
                                                       << " dest=" << entry.destination
                                                       << " score=" << entry.scoreX100
                                                       << " sf=" << unsigned(entry.sf)
                                                       << " raw=" << entry.rawMetric);
        }
        maybeInsert(kv.second);
    }

    if (candidates.empty())
    {
        return announcements;
    }

    std::vector<RouteEntry> pool;
    pool.reserve(candidates.size());
    for (const auto& kv : candidates)
    {
        pool.push_back(kv.second);
    }

    auto appendEntry = [&](const RouteEntry& entry) {
        RouteAnnouncement ann;
        ann.destination = entry.destination;
        ann.hops = entry.hops;
        ann.sf = entry.sf;
        ann.scoreX100 = entry.scoreX100;
        ann.batt_mV = entry.batt_mV;
        announcements.push_back(ann);
    };

    if (pool.size() <= maxRoutes)
    {
        std::sort(pool.begin(), pool.end(), [this](const RouteEntry& a, const RouteEntry& b) {
            const double aMetric = (a.scoreX100 == 0) ? static_cast<double>(MaxAdvertMetricValue())
                                                      : GetComparableMetric(a);
            const double bMetric = (b.scoreX100 == 0) ? static_cast<double>(MaxAdvertMetricValue())
                                                      : GetComparableMetric(b);
            if (aMetric != bMetric)
            {
                return aMetric < bMetric;
            }
            if (a.sf != b.sf)
            {
                return a.sf < b.sf;
            }
            return a.destination < b.destination;
        });
        for (const auto& entry : pool)
        {
            if (m_pueyoValidationTrace)
            {
                const double metric = (entry.scoreX100 == 0)
                                          ? static_cast<double>(MaxAdvertMetricValue())
                                          : GetComparableMetric(entry);
                NS_LOG_INFO("PUEYO_VAL_ROUTE_CHOSEN node=" << m_nodeId << " mode=all_fit"
                                                             << " dest=" << entry.destination
                                                             << " score=" << entry.scoreX100
                                                             << " metric=" << metric);
            }
            appendEntry(entry);
        }
        return announcements;
    }

    while (!pool.empty() && announcements.size() < maxRoutes)
    {
        std::vector<double> weights;
        weights.reserve(pool.size());
        double totalWeight = 0.0;
        for (const auto& entry : pool)
        {
            const double metric = (entry.scoreX100 == 0)
                                      ? static_cast<double>(MaxAdvertMetricValue())
                                      : std::max(0.0, GetComparableMetric(entry));
            const double w = 1.0 / (0.05 + metric);
            if (m_pueyoValidationTrace)
            {
                NS_LOG_INFO("PUEYO_VAL_ROUTE_WEIGHT node=" << m_nodeId << " dest="
                                                             << entry.destination << " score="
                                                             << entry.scoreX100 << " metric="
                                                             << metric << " weight=" << w);
            }
            weights.push_back(w);
            totalWeight += w;
        }
        if (totalWeight <= 0.0)
        {
            break;
        }

        const double r = m_rng ? m_rng->GetValue(0.0, totalWeight) : 0.0;
        double acc = 0.0;
        std::size_t chosen = 0;
        for (std::size_t i = 0; i < weights.size(); ++i)
        {
            acc += weights[i];
            if (r <= acc)
            {
                chosen = i;
                break;
            }
        }

        if (m_pueyoValidationTrace)
        {
            const auto& chosenEntry = pool[chosen];
            NS_LOG_INFO("PUEYO_VAL_ROUTE_CHOSEN node=" << m_nodeId << " mode=weighted"
                                                         << " dest=" << chosenEntry.destination
                                                         << " score=" << chosenEntry.scoreX100
                                                         << " index=" << chosen);
        }
        appendEntry(pool[chosen]);
        pool.erase(pool.begin() + static_cast<std::ptrdiff_t>(chosen));
    }

    return announcements;
}

void
DvClRouting::UpdateRoute(const RouteEntry& candidate)
{
    if (candidate.destination == m_nodeId)
    {
        return;
    }

    auto itPrimary = m_routes.find(candidate.destination);
    auto itBackup = m_backupRoutes.find(candidate.destination);

    if (candidate.scoreX100 == 0)
    {
        bool poisoned = false;
        if (itPrimary != m_routes.end() && itPrimary->second.nextHop == candidate.nextHop)
        {
            m_routes[candidate.destination] = candidate;
            NotifyChange(candidate, "POISON");
            poisoned = true;
        }
        if (itBackup != m_backupRoutes.end() && itBackup->second.nextHop == candidate.nextHop)
        {
            m_backupRoutes[candidate.destination] = candidate;
            NotifyChange(candidate, "POISON_BACKUP");
            poisoned = true;
        }
        if (poisoned)
        {
            EnforceRouteTableLimits();
        }
        return;
    }

    if (itPrimary == m_routes.end())
    {
        m_routes[candidate.destination] = candidate;
        NotifyChange(candidate, "NEW");
        EnforceRouteTableLimits();
        return;
    }

    RouteEntry& primary = itPrimary->second;
    if (candidate.nextHop == primary.nextHop)
    {
        bool tie = false;
        const bool better = IsCandidateBetter(candidate, primary, &tie);
        if (better || candidate.seqNum > primary.seqNum || tie)
        {
            primary = candidate;
            NotifyChange(candidate, better ? "UPDATE" : "REFRESH");
            EnforceRouteTableLimits();
        }
        return;
    }

    bool tie = false;
    const bool candidateBeatsPrimary = IsCandidateBetter(candidate, primary, &tie);
    const bool applyHysteresis = (m_metricMode == MetricMode::COMPOSITE_SCORE);
    const uint32_t candidateQuant = QuantizeCompositeMetric(candidate.rawMetric);
    const uint32_t primaryQuant = QuantizeCompositeMetric(primary.rawMetric);
    const int32_t quantizedImprovement =
        static_cast<int32_t>(primaryQuant) - static_cast<int32_t>(candidateQuant);
    const bool switchBlockedByHysteresis =
        applyHysteresis && candidateBeatsPrimary &&
        quantizedImprovement < static_cast<int32_t>(m_routeSwitchMinDeltaX100);

    if (candidateBeatsPrimary && !switchBlockedByHysteresis)
    {
        if (m_maxRoutesPerDestination > 1)
        {
            m_backupRoutes[candidate.destination] = primary;
        }
        primary = candidate;
        NotifyChange(candidate, "SWITCH_PRIMARY");
        EnforceRouteTableLimits();
        return;
    }

    if (m_maxRoutesPerDestination <= 1)
    {
        return;
    }

    if (itBackup == m_backupRoutes.end())
    {
        m_backupRoutes[candidate.destination] = candidate;
        NotifyChange(candidate, "ADD_BACKUP");
        EnforceRouteTableLimits();
        return;
    }

    RouteEntry& backup = itBackup->second;
    if (candidate.nextHop == backup.nextHop)
    {
        bool tieBackup = false;
        const bool betterBackup = IsCandidateBetter(candidate, backup, &tieBackup);
        if (betterBackup || candidate.seqNum > backup.seqNum || tieBackup)
        {
            backup = candidate;
            NotifyChange(candidate, "REFRESH_BACKUP");
            EnforceRouteTableLimits();
        }
        return;
    }

    bool tieVsBackup = false;
    if (IsCandidateBetter(candidate, backup, &tieVsBackup))
    {
        backup = candidate;
        NotifyChange(candidate, "REPLACE_BACKUP");
        EnforceRouteTableLimits();
    }
}

RouteEntry*
DvClRouting::LookupRouteMutable(NodeId dest)
{
    auto it = m_routes.find(dest);
    if (it == m_routes.end())
    {
        return nullptr;
    }
    if (IsExpired(it->second))
    {
        return nullptr;
    }
    return &it->second;
}

bool
DvClRouting::IsExpired(const RouteEntry& entry) const
{
    const Time now = Simulator::Now();
    const Time timeout = GetEffectiveTimeout(entry);
    return (now - entry.lastUpdate) > timeout;
}

void
DvClRouting::NotifyChange(const RouteEntry& entry, const std::string& action) const
{
    if (!m_routeChangeCallback.IsNull())
    {
        m_routeChangeCallback(entry, action);
    }
}

DvMessage
DvClRouting::BuildDvMessage() const
{
    DvMessage msg;
    msg.origin = m_nodeId;
    msg.sequence = m_sequence;
    msg.entries.reserve(m_maxRoutes);
    auto announcements = GetBestRoutes(m_maxRoutes);
    for (const auto& ann : announcements)
    {
        DvEntry entry;
        entry.destination = ann.destination;
        entry.hops = ann.hops;
        entry.sf = ann.sf;
        entry.scoreX100 = ann.scoreX100;
        // REMOVED: entry.rssiDbm - eliminado de struct
        entry.batt_mV = ann.batt_mV;
        entry.toaUs = 0;
        msg.entries.push_back(entry);
    }
    return msg;
}

} // namespace dvcl
} // namespace ns3
