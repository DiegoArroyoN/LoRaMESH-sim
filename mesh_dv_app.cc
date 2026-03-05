#include "mesh_dv_app.h"

#include "mesh_lora_net_device.h"
#include "metrics_collector.h"

#include "ns3/attribute.h"
#include "ns3/double.h"
#include "ns3/end-device-lora-phy.h"
#include "ns3/log.h"
#include "ns3/lora-tag.h"
#include "ns3/mac48-address.h"
#include "ns3/net-device.h"
#include "ns3/node-list.h"
#include "ns3/node.h"
#include "ns3/packet.h"
#include "ns3/random-variable-stream.h"
#include "ns3/simulator.h"
#include "ns3/string.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring> // Para memcpy, memset, etc.
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace
{
std::string
ToLower(std::string value)
{
    for (char& ch : value)
    {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

std::string
NormalizeWireFormat(std::string value)
{
    value = ToLower(std::move(value));
    if (value != "v2")
    {
        value = "v1";
    }
    return value;
}

constexpr double kBatteryMvMin = 3000.0;
constexpr double kBatteryMvMax = 4200.0;

uint16_t
EnergyFractionToBatteryMv(double energyFraction)
{
    const double frac = std::clamp(energyFraction, 0.0, 1.0);
    const double battMv = kBatteryMvMin + frac * (kBatteryMvMax - kBatteryMvMin);
    return static_cast<uint16_t>(std::round(battMv));
}

double
BatteryMvToEnergyFraction(uint16_t battMv)
{
    const double mv = static_cast<double>(battMv);
    const double frac = (mv - kBatteryMvMin) / (kBatteryMvMax - kBatteryMvMin);
    return std::clamp(frac, 0.0, 1.0);
}

double
Percentile95Double(std::vector<double> values)
{
    if (values.empty())
    {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const std::size_t idx = static_cast<std::size_t>(std::ceil(0.95 * values.size())) - 1;
    return values[std::min(idx, values.size() - 1)];
}
} // namespace

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("MeshDvApp");
NS_OBJECT_ENSURE_REGISTERED(MeshDvApp);
// Variable global declarada en mesh_dv_baseline.cc
extern MetricsCollector* g_metricsCollector;

// FIX C2: Helper seguro para acceder a MetricsCollector con null-check
inline MetricsCollector*
SafeMetrics()
{
    return g_metricsCollector;
}

// Macro para simplificar null-checks en métodos Record*
#define SAFE_RECORD(method, ...)                                                                   \
    do                                                                                             \
    {                                                                                              \
        if (g_metricsCollector)                                                                    \
            g_metricsCollector->method(__VA_ARGS__);                                               \
    } while (0)

// Implementación correcta del destructor:
MeshDvApp::~MeshDvApp()
{
    // Limpieza si necesitas; normalmente vacío en NS-3
    NS_LOG_FUNCTION(this);
}

// Implementación del constructor:
MeshDvApp::MeshDvApp()
{
    NS_LOG_FUNCTION(this);
    m_mac = CreateObject<loramesh::CsmaCadMac>();
    m_energyModel = CreateObject<loramesh::EnergyModel>();
    m_routing = CreateObject<loramesh::RoutingDv>();
    m_compositeMetric.SetEnergyModel(m_energyModel);
    UpdateRouteTimeout();
}

TypeId
MeshDvApp::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::MeshDvApp")
            .SetParent<Application>()
            .SetGroupName("Applications")
            .AddConstructor<MeshDvApp>()
            .AddAttribute("InitialDvDelayBase",
                          "Base delay before first DV/beacon.",
                          DoubleValue(1.0),
                          MakeDoubleAccessor(&MeshDvApp::m_initialDvDelayBase),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("InitialDvJitterMax",
                          "Max additional random jitter before first DV/beacon.",
                          DoubleValue(8.0),
                          MakeDoubleAccessor(&MeshDvApp::m_initialDvJitterMax),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("InitialDvNodeSpacing",
                          "Extra per-node offset applied to first DV/beacon.",
                          DoubleValue(0.5),
                          MakeDoubleAccessor(&MeshDvApp::m_initialDvNodeSpacing),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataStartTimeSec",
                          "Start time for data generation (seconds).",
                          DoubleValue(90.0),
                          MakeDoubleAccessor(&MeshDvApp::m_dataStartTimeSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataStopTimeSec",
                          "Stop time for data generation (seconds). -1 disables stop window.",
                          DoubleValue(-1.0),
                          MakeDoubleAccessor(&MeshDvApp::m_dataStopTimeSec),
                          MakeDoubleChecker<double>(-1.0))
            .AddAttribute("BeaconWarmupSec",
                          "Warm-up duration used for beacon phase transitions (seconds).",
                          DoubleValue(60.0),
                          MakeDoubleAccessor(&MeshDvApp::m_beaconWarmupSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute(
                "TrafficLoad",
                "Data traffic load: low/medium/high/saturation.",
                StringValue("medium"),
                MakeStringAccessor(&MeshDvApp::SetTrafficLoad, &MeshDvApp::GetTrafficLoad),
                MakeStringChecker())
            .AddAttribute("EnableDataRandomDest",
                          "Enable random destination selection for any-to-any traffic.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&MeshDvApp::m_enableDataRandomDest),
                          MakeBooleanChecker())
            .AddAttribute("DataPeriodJitterMax",
                          "Max extra jitter applied to data period (seconds).",
                          DoubleValue(0.5),
                          MakeDoubleAccessor(&MeshDvApp::m_dataPeriodJitterMax),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("BatteryFullCapacityJ",
                          "Nominal full battery capacity [J] used to compute SoC from remaining energy.",
                          DoubleValue(38880.0),
                          MakeDoubleAccessor(&MeshDvApp::m_batteryFullCapacityJ),
                          MakeDoubleChecker<double>(1.0))
            .AddAttribute("EnableDataSlots",
                          "Enable local micro-slots for data transmissions.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&MeshDvApp::m_enableDataSlots),
                          MakeBooleanChecker())
            .AddAttribute("DataSlotPeriodSec",
                          "Slot period used to align data transmissions (seconds).",
                          DoubleValue(0.0),
                          MakeDoubleAccessor(&MeshDvApp::m_dataSlotPeriodSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataSlotJitterSec",
                          "Jitter applied around data slot boundaries (seconds).",
                          DoubleValue(0.0),
                          MakeDoubleAccessor(&MeshDvApp::m_dataSlotJitterSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("PrioritizeBeacons",
                          "If true, enqueue DV beacons ahead of data in CSMA queue.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&MeshDvApp::m_prioritizeBeacons),
                          MakeBooleanChecker())
            .AddAttribute("ControlBackoffFactor",
                          "Backoff multiplier for control (beacon) frames.",
                          DoubleValue(0.5),
                          MakeDoubleAccessor(&MeshDvApp::m_controlBackoffFactor),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataBackoffFactor",
                          "Backoff multiplier for data frames.",
                          DoubleValue(1.0),
                          MakeDoubleAccessor(&MeshDvApp::m_dataBackoffFactor),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("EnableControlGuard",
                          "Delay data transmissions shortly after DV beacon activity.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&MeshDvApp::m_enableControlGuard),
                          MakeBooleanChecker())
            .AddAttribute("ControlGuardSec",
                          "Guard window (seconds) after DV TX/RX before starting data TX.",
                          DoubleValue(0.0),
                          MakeDoubleAccessor(&MeshDvApp::m_controlGuardSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("EnableDvFlooding",
                          "Enable explicit flooding of received DV beacons.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&MeshDvApp::m_enableDvFlooding),
                          MakeBooleanChecker())
            .AddAttribute("AdvertiseAllRoutes",
                          "If true, include all known routes in DV announcements.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&MeshDvApp::m_advertiseAllRoutes),
                          MakeBooleanChecker())
            .AddAttribute("UseEmpiricalSfForData",
                          "If true, prefer empirical SF learned from successful beacon reception.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&MeshDvApp::m_useEmpiricalSfForData),
                          MakeBooleanChecker())
            .AddAttribute("AllowStaleMacForUnicastData",
                          "Legacy alias for AllowStaleLinkAddrForUnicastData.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&MeshDvApp::m_allowStaleLinkAddrForUnicastData),
                          MakeBooleanChecker())
            .AddAttribute("AllowStaleLinkAddrForUnicastData",
                          "If true, allow unicast data TX using stale learned link-layer address for nextHop.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&MeshDvApp::m_allowStaleLinkAddrForUnicastData),
                          MakeBooleanChecker())
            .AddAttribute("EmpiricalSfMinSamples",
                          "Minimum recent beacon samples per SF required when using robust_min mode.",
                          UintegerValue(2),
                          MakeUintegerAccessor(&MeshDvApp::m_empiricalSfMinSamples),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("EmpiricalSfSelectMode",
                          "Empirical SF selector mode: min | robust_min.",
                          StringValue("robust_min"),
                          MakeStringAccessor(&MeshDvApp::m_empiricalSfSelectMode),
                          MakeStringChecker())
            .AddAttribute("DvBeaconMaxRoutes",
                          "Upper bound for routes included in DV beacons (0 = MTU-derived).",
                          UintegerValue(0),
                          MakeUintegerAccessor(&MeshDvApp::m_dvBeaconMaxRoutes),
                          MakeUintegerChecker<uint32_t>())
            .AddAttribute("DvBeaconOverheadBytes",
                          "Reserved bytes when computing beacon route capacity.",
                          UintegerValue(1),
                          MakeUintegerAccessor(&MeshDvApp::m_dvBeaconOverheadBytes),
                          MakeUintegerChecker<uint32_t>())
            .AddAttribute("ExtraDvBeaconMinGap",
                          "Minimum time gap between DV beacons.",
                          TimeValue(Seconds(0.5)),
                          MakeTimeAccessor(&MeshDvApp::m_extraDvBeaconMinGap),
                          MakeTimeChecker())
            .AddAttribute("ExtraDvBeaconSecondDelay",
                          "Delay between extra DV beacons after a new active destination.",
                          TimeValue(Seconds(1.0)),
                          MakeTimeAccessor(&MeshDvApp::m_extraDvBeaconSecondDelay),
                          MakeTimeChecker())
            .AddAttribute("ExtraDvBeaconJitterMax",
                          "Max random jitter added to extra DV beacons (seconds).",
                          DoubleValue(0.2),
                          MakeDoubleAccessor(&MeshDvApp::m_extraDvBeaconJitterMax),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("ExtraDvBeaconWindow",
                          "Time window to limit extra DV beacons.",
                          TimeValue(Seconds(10)),
                          MakeTimeAccessor(&MeshDvApp::m_extraDvBeaconWindow),
                          MakeTimeChecker())
            .AddAttribute("ExtraDvBeaconMaxPerWindow",
                          "Maximum extra DV beacons per window.",
                          UintegerValue(2),
                          MakeUintegerAccessor(&MeshDvApp::m_extraDvBeaconMaxPerWindow),
                          MakeUintegerChecker<uint32_t>())
            .AddAttribute("DisableExtraAfterWarmup",
                          "Disable extra DV beacons after warmup period.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&MeshDvApp::m_disableExtraAfterWarmup),
                          MakeBooleanChecker())
            .AddAttribute("MacCacheWindow",
                          "Legacy alias for LinkAddrCacheWindow.",
                          TimeValue(Seconds(300)),
                          MakeTimeAccessor(&MeshDvApp::m_linkAddrCacheWindow),
                          MakeTimeChecker())
            .AddAttribute("LinkAddrCacheWindow",
                          "Time window to keep learned link-layer address entries for nextHop.",
                          TimeValue(Seconds(300)),
                          MakeTimeAccessor(&MeshDvApp::m_linkAddrCacheWindow),
                          MakeTimeChecker())
            .AddAttribute("DedupWindowSec",
                          "Dedup cache TTL for dataplane keys (src,dst,seq16).",
                          TimeValue(Seconds(600)),
                          MakeTimeAccessor(&MeshDvApp::m_dedupWindow),
                          MakeTimeChecker())
            .AddAttribute("AutoTimeoutsFromBeacon",
                          "Auto-scale route timeout and neighbor link freshness from active beacon interval.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&MeshDvApp::m_autoTimeoutsFromBeacon),
                          MakeBooleanChecker())
            .AddAttribute("NeighborLinkTimeoutFactor",
                          "Neighbor link freshness expressed in beacon intervals (auto-timeout mode).",
                          DoubleValue(1.0),
                          MakeDoubleAccessor(&MeshDvApp::m_neighborLinkTimeoutFactor),
                          MakeDoubleChecker<double>(0.1))
            .AddAttribute("NeighborLinkTimeout",
                          "Manual validity window for empirical per-SF neighbor history (used when auto-timeout is disabled).",
                          TimeValue(Seconds(60)),
                          MakeTimeAccessor(&MeshDvApp::m_neighborLinkTimeoutConfigured),
                          MakeTimeChecker())
            .AddAttribute("BeaconIntervalWarm",
                          "DV beacon interval during warmup phase.",
                          TimeValue(Seconds(10)),
                          MakeTimeAccessor(&MeshDvApp::m_beaconIntervalWarm),
                          MakeTimeChecker())
            .AddAttribute("BeaconIntervalStable",
                          "DV beacon interval during stable phase.",
                          TimeValue(Seconds(60)),
                          MakeTimeAccessor(&MeshDvApp::m_beaconIntervalStable),
                          MakeTimeChecker())
            .AddAttribute("RouteTimeoutFactor",
                          "Multiplier used to derive route timeout from beacon intervals.",
                          DoubleValue(6.0),
                          MakeDoubleAccessor(&MeshDvApp::m_routeTimeoutFactor),
                          MakeDoubleChecker<double>(1.0))
            .AddAttribute("RouteSwitchMinDeltaX100",
                          "Minimum score delta required to switch next-hop (hysteresis).",
                          UintegerValue(5),
                          MakeUintegerAccessor(&MeshDvApp::m_routeSwitchMinDeltaX100),
                          MakeUintegerChecker<uint16_t>())
            .AddAttribute("AvoidImmediateBacktrack",
                          "Avoid forwarding data back to the packet prevHop (loop guard).",
                          BooleanValue(true),
                          MakeBooleanAccessor(&MeshDvApp::m_avoidImmediateBacktrack),
                          MakeBooleanChecker())
            .AddAttribute("WireFormat",
                          "Packet wire format selector: v1 (legacy) | v2 (real on-air).",
                          StringValue("v2"),
                          MakeStringAccessor(&MeshDvApp::SetWireFormat, &MeshDvApp::GetWireFormat),
                          MakeStringChecker());
    return tid;
}

void
MeshDvApp::UpdateRouteTimeout()
{
    const Time oldRouteTimeout = m_routeTimeout;
    const Time oldNeighborTimeout = m_neighborLinkTimeout;

    const double baseBeaconSec = std::max(0.001, GetBeaconInterval().GetSeconds());
    double routeTimeoutSec = baseBeaconSec * std::max(1.0, m_routeTimeoutFactor);
    if (routeTimeoutSec <= 0.0)
    {
        routeTimeoutSec = std::max(1.0, baseBeaconSec);
    }
    m_routeTimeout = Seconds(std::max(1.0, routeTimeoutSec));

    if (m_autoTimeoutsFromBeacon)
    {
        const double linkTimeoutSec =
            std::max(1.0, baseBeaconSec * std::max(0.1, m_neighborLinkTimeoutFactor));
        m_neighborLinkTimeout = Seconds(linkTimeoutSec);
    }
    else
    {
        m_neighborLinkTimeout = m_neighborLinkTimeoutConfigured;
    }

    if (m_routing)
    {
        m_routing->SetRouteTimeout(m_routeTimeout);
    }
    if (oldRouteTimeout != m_routeTimeout || oldNeighborTimeout != m_neighborLinkTimeout)
    {
        if (GetNode())
        {
            NS_LOG_UNCOND("ADAPT_TIMEOUT node" << GetNode()->GetId()
                                               << " phase=" << GetBeaconPhaseLabel()
                                               << " beacon=" << baseBeaconSec << "s"
                                               << " routeTimeout=" << m_routeTimeout.GetSeconds() << "s"
                                               << " linkFreshness=" << m_neighborLinkTimeout.GetSeconds() << "s"
                                               << " routeFactor=" << m_routeTimeoutFactor
                                               << " linkFactor=" << m_neighborLinkTimeoutFactor
                                               << " auto=" << (m_autoTimeoutsFromBeacon ? "on" : "off"));
        }
    }
}

void
MeshDvApp::SetTrafficLoad(std::string load)
{
    const std::string normalized = ToLower(load);
    if (normalized == "low")
    {
        m_trafficLoadMode = TrafficLoadMode::LOW;
    }
    else if (normalized == "high")
    {
        m_trafficLoadMode = TrafficLoadMode::HIGH;
    }
    else if (normalized == "saturation")
    {
        m_trafficLoadMode = TrafficLoadMode::SATURATION;
    }
    else
    {
        m_trafficLoadMode = TrafficLoadMode::MEDIUM;
    }
    UpdateDataPeriod();
}

std::string
MeshDvApp::GetTrafficLoad() const
{
    switch (m_trafficLoadMode)
    {
    case TrafficLoadMode::LOW:
        return "low";
    case TrafficLoadMode::HIGH:
        return "high";
    case TrafficLoadMode::SATURATION:
        return "saturation";
    case TrafficLoadMode::MEDIUM:
    default:
        return "medium";
    }
}

void
MeshDvApp::UpdateDataPeriod()
{
    // Data generation period is driven only by traffic load presets.
    switch (m_trafficLoadMode)
    {
    case TrafficLoadMode::LOW:
        m_dataGenerationPeriod = Seconds(100.0);
        break;
    case TrafficLoadMode::HIGH:
        m_dataGenerationPeriod = Seconds(1.0);
        break;
    case TrafficLoadMode::SATURATION:
        m_dataGenerationPeriod = Seconds(0.1);
        break;
    case TrafficLoadMode::MEDIUM:
    default:
        m_dataGenerationPeriod = Seconds(10.0);
        break;
    }
}

void
MeshDvApp::InitDataDestinations()
{
    m_dataDestinations.clear();
    m_nextDestIndex = 0;

    Ptr<Node> node = GetNode();
    if (!node)
    {
        return;
    }

    const uint32_t myId = node->GetId();
    const uint32_t totalNodes = NodeList::GetNNodes();
    for (uint32_t i = 0; i < totalNodes; ++i)
    {
        if (i == myId)
        {
            continue;
        }
        m_dataDestinations.push_back(i);
    }

    if (m_enableDataRandomDest && m_rng && m_dataDestinations.size() > 1)
    {
        for (std::size_t i = m_dataDestinations.size() - 1; i > 0; --i)
        {
            const uint32_t j = m_rng->GetInteger(0, static_cast<uint32_t>(i));
            std::swap(m_dataDestinations[i], m_dataDestinations[j]);
        }
    }
}

void
MeshDvApp::InitDataSlots()
{
    m_dataSlotOffsetSec = 0.0;
    if (!m_enableDataSlots || m_dataSlotPeriodSec <= 0.0)
    {
        return;
    }

    Ptr<Node> node = GetNode();
    const uint32_t nodeId = node ? node->GetId() : 0;
    const uint32_t totalNodes = std::max(1u, NodeList::GetNNodes());
    const double period = m_dataSlotPeriodSec;
    const double slotWidth = period / static_cast<double>(totalNodes);
    m_dataSlotOffsetSec = std::fmod(static_cast<double>(nodeId) * slotWidth, period);

    NS_LOG_INFO("DATA slots enabled: node=" << nodeId << " period=" << period
                                            << " offset=" << m_dataSlotOffsetSec);
}

void
MeshDvApp::TrackActiveDestination(uint32_t dst)
{
    if (dst == 0xFFFF)
    {
        return;
    }
    m_activeDestinations.insert(dst);
    bool wasActive = false;
    if (m_routing)
    {
        wasActive = m_routing->IsDestinationActive(dst);
        m_routing->NotifyDestinationActive(dst);
    }
    if (m_routing && !wasActive)
    {
        ScheduleExtraDvBeacons("new_active_dst");
    }
}

void
MeshDvApp::ScheduleExtraDvBeacons(const std::string& reason)
{
    if (!m_enableDvBroadcast || m_extraDvBeaconMaxPerWindow == 0)
    {
        return;
    }
    if (m_disableExtraAfterWarmup && Simulator::Now() >= m_beaconWarmupEnd)
    {
        return;
    }

    const Time now = Simulator::Now();
    if ((now - m_extraDvWindowStart) > m_extraDvBeaconWindow)
    {
        m_extraDvWindowStart = now;
        m_extraDvCountInWindow = 0;
    }

    uint32_t remaining = 0;
    if (m_extraDvBeaconMaxPerWindow > m_extraDvCountInWindow)
    {
        remaining = m_extraDvBeaconMaxPerWindow - m_extraDvCountInWindow;
    }
    if (remaining == 0)
    {
        return;
    }

    if (!m_rng)
    {
        m_rng = CreateObject<UniformRandomVariable>();
    }

    const Time sinceLast =
        m_lastDvBeaconTime.IsZero() ? m_extraDvBeaconMinGap : (now - m_lastDvBeaconTime);
    const Time baseDelay =
        (sinceLast < m_extraDvBeaconMinGap) ? (m_extraDvBeaconMinGap - sinceLast) : Seconds(0);

    auto scheduleOne = [&](Time delay) {
        Simulator::Schedule(delay, &MeshDvApp::SendExtraDvBeacon, this, reason);
        m_extraDvCountInWindow++;
    };

    const double jitter1 = m_rng->GetValue(0.0, m_extraDvBeaconJitterMax);
    scheduleOne(baseDelay + Seconds(jitter1));
    remaining--;
    if (remaining > 0)
    {
        const double jitter2 = m_rng->GetValue(0.0, m_extraDvBeaconJitterMax);
        scheduleOne(baseDelay + m_extraDvBeaconSecondDelay + Seconds(jitter2));
    }
}

void
MeshDvApp::SendExtraDvBeacon(const std::string& reason)
{
    NS_LOG_INFO("EXTRA_DV_BEACON node=" << GetNode()->GetId() << " reason=" << reason
                                        << " t=" << Simulator::Now().GetSeconds());
    // B3: usar SF probabilístico si está habilitado
    const uint8_t sf = m_useProbabilisticSf ? SelectRandomSfProbabilistic() : m_sfControl;
    BuildAndSendDv(sf);
}

uint32_t
MeshDvApp::GetBeaconRouteCapacity() const
{
    uint32_t mtu = 255;
    Ptr<Node> node = GetNode();
    if (node)
    {
        for (uint32_t i = 0; i < node->GetNDevices(); ++i)
        {
            Ptr<NetDevice> dev = node->GetDevice(i);
            if (dev)
            {
                uint16_t devMtu = dev->GetMtu();
                if (devMtu > 0)
                {
                    mtu = std::min(mtu, static_cast<uint32_t>(devMtu));
                }
            }
        }
    }

    const uint32_t entrySize = (m_wireFormat == "v2") ? BeaconWireHeaderV2::kEntrySize
                                                       : MeshMetricTag::GetRoutePayloadEntrySize();
    if (entrySize == 0 || mtu <= 1)
    {
        return 1;
    }
    uint32_t overheadBytes = m_dvBeaconOverheadBytes;
    if (m_wireFormat == "v2")
    {
        overheadBytes = std::max<uint32_t>(overheadBytes, BeaconWireHeaderV2::kSerializedSize);
    }
    const uint32_t overhead = std::min(mtu, overheadBytes);
    const uint32_t usable = (mtu > overhead) ? (mtu - overhead) : 0;
    uint32_t maxRoutes = (usable / entrySize);
    if (m_dvBeaconMaxRoutes > 0)
    {
        maxRoutes = std::min(maxRoutes, m_dvBeaconMaxRoutes);
    }
    return std::max(1u, maxRoutes);
}

void
MeshDvApp::SetPeriod(Time t)
{
    m_period = t;
    UpdateRouteTimeout();
}

void
MeshDvApp::SetRouteTimeoutFactor(double factor)
{
    m_routeTimeoutFactor = std::max(1.0, factor);
    UpdateRouteTimeout();
}

void
MeshDvApp::SetWireFormat(std::string format)
{
    m_wireFormat = NormalizeWireFormat(std::move(format));
}

std::string
MeshDvApp::GetWireFormat() const
{
    return m_wireFormat;
}

void
MeshDvApp::BootstrapLinkAddrTableFromRx()
{
    // Link-layer address table is now learned on RX (beacons/data) instead of being preloaded at startup.
    // Keep this hook for backward compatibility/log traceability.
    m_linkAddrTable.clear();
    m_linkAddrLastSeen.clear();
    if (Ptr<Node> self = GetNode())
    {
        NS_LOG_UNCOND("LINKADDR_BOOTSTRAP node=" << self->GetId()
                                            << " entries=0 mode=rx_learning_only");
    }
}

// Construye y transmite el beacon DV periódico con las mejores rutas conocidas.
void
MeshDvApp::BuildAndSendDv(uint8_t sf)
{
    if (m_wireFormat == "v2")
    {
        BuildAndSendDvV2(sf);
        return;
    }

    // Si DV broadcasts están deshabilitados, no hacer nada
    if (!m_enableDvBroadcast)
    {
        return;
    }

    const uint8_t beaconSf = sf;
    MeshMetricTag tag;
    tag.SetSrc(GetNode()->GetId());
    tag.SetDst(0xFFFF);
    tag.SetSeq(++m_seq);
    tag.SetPrevHop(GetNode()->GetId());
    tag.SetTtl(m_initTtl);
    tag.SetHops(0);
    tag.SetSf(beaconSf);
    if (m_routing)
    {
        m_routing->SetSequence(m_seq);
    }

    uint16_t realBatt = GetBatteryVoltageMv();
    // REMOVED: tag.SetRssiDbm - no se serializa, el receptor mide RSSI
    tag.SetBatt_mV(realBatt);

    // ---------------------- NUEVO: Top-N mejor score ------------------------
    const uint32_t maxRoutes = GetBeaconRouteCapacity();
    NS_LOG_INFO("Beacon capacity (max routes)=" << maxRoutes);
    if (m_routing)
    {
        m_routing->SetMaxRoutes(maxRoutes);
    }

    std::vector<MeshMetricTag::RoutePayloadEntry> payload;
    if (m_routing)
    {
        auto announcements = m_routing->GetBestRoutes(maxRoutes);
        payload.reserve(announcements.size());
        for (const auto& ann : announcements)
        {
            MeshMetricTag::RoutePayloadEntry r;
            r.dst = ann.destination;
            r.hops = ann.hops;
            r.sf = ann.sf;
            r.score = ann.scoreX100;
            r.batt_mV = ann.batt_mV;
            // REMOVED: rssi_dBm - no se usa en métrica
            payload.push_back(r);
        }
    }

    // ========================================================================
    // FIX CRÍTICO: Crear paquete con al menos 1 byte si payload está vacío
    // ========================================================================
    size_t len = payload.size() * MeshMetricTag::GetRoutePayloadEntrySize();
    uint32_t payloadSizeBytes = static_cast<uint32_t>(len);
    if (payloadSizeBytes == 0)
    {
        // Beacon without routes: use 1-byte dummy to avoid empty packet
        payloadSizeBytes = 1;
    }
    Ptr<Packet> p;

    std::vector<uint8_t> buffer(payloadSizeBytes, 0);
    if (len > 0)
    {
        MeshMetricTag::SerializeRoutePayload(payload, buffer.data(), buffer.size());
        NS_LOG_INFO("Node " << GetNode()->GetId() << " Beacon con " << payload.size()
                            << " rutas en payload");
    }
    else
    {
        NS_LOG_INFO("Node " << GetNode()->GetId() << " Beacon sin rutas, enviando payload dummy de "
                            << payloadSizeBytes << " bytes");
    }
    p = Create<Packet>(buffer.data(), buffer.size());
    // ------------------- FIN FIX --------------------------------------------

    NS_LOG_UNCOND("DVTRACE_TX time=" << Simulator::Now().GetSeconds()
                                     << " node=" << GetNode()->GetId() << " seq=" << tag.GetSeq()
                                     << " entries=" << payload.size() << " bytes=" << buffer.size()
                                     << " maxRoutes=" << maxRoutes
                                     << " active=" << m_activeDestinations.size()
                                     << " phase=" << GetBeaconPhaseLabel());

    const uint32_t toaUs = ComputeLoRaToAUs(beaconSf, m_bw, m_cr, payloadSizeBytes);
    tag.SetToaUs(toaUs);

    if (m_mac && !m_mac->CanTransmitNow(toaUs / 1e6))
    {
        NS_LOG_UNCOND("FWDTRACE duty_defer time="
                      << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                      << " src=" << tag.GetSrc() << " dst=" << tag.GetDst() << " seq="
                      << tag.GetSeq() << " dutyUsed=" << m_mac->GetDutyCycleUsed()
                      << " dutyLimit=" << m_mac->GetDutyCycleLimit()
                      << " reason=duty_wait_queue");
    }

    m_lastDvBeaconTime = Simulator::Now();

    const uint16_t scoreX100 = ComputeScoreX100(tag);
    tag.SetScoreX100(scoreX100);

    p->AddPacketTag(tag);

    std::ostringstream oss;
    tag.Print(oss);
    NS_LOG_INFO("DV OUT: " << oss.str());
    NS_LOG_UNCOND("BEACON_TX SF" << unsigned(beaconSf) << " node=" << GetNode()->GetId()
                                 << " time=" << Simulator::Now().GetSeconds());
    NS_LOG_UNCOND("DV_TX node" << GetNode()->GetId() << " dst=" << tag.GetDst()
                               << " metric=" << scoreX100 << " nextHop=" << -1);

    RecordBeaconScheduled(tag.GetSeq());
    SendWithCSMA(p, tag, Address(), true);
}

void
MeshDvApp::BuildAndSendDvV2(uint8_t sf)
{
    if (!m_enableDvBroadcast)
    {
        return;
    }

    const uint8_t beaconSf = sf;
    const uint16_t myId = static_cast<uint16_t>(GetNode()->GetId());
    const uint32_t maxRoutes = GetBeaconRouteCapacity();
    if (m_routing)
    {
        m_routing->SetMaxRoutes(maxRoutes);
    }

    std::vector<DvEntryWireV2> entries;
    if (m_routing)
    {
        const auto announcements = m_routing->GetBestRoutes(maxRoutes);
        entries.reserve(announcements.size());
        for (const auto& ann : announcements)
        {
            DvEntryWireV2 e;
            e.destination = static_cast<uint16_t>(ann.destination);
            e.score = static_cast<uint8_t>(std::min<uint16_t>(ann.scoreX100, 100));
            entries.push_back(e);
        }
    }

    const uint32_t payloadSizeBytes = static_cast<uint32_t>(entries.size() * BeaconWireHeaderV2::kEntrySize);
    std::vector<uint8_t> payload(payloadSizeBytes, 0);
    if (!entries.empty())
    {
        BeaconWireHeaderV2::SerializeDvEntries(entries, payload.data(), payload.size());
    }

    Ptr<Packet> p = Create<Packet>(payload.data(), payload.size());

    BeaconWireHeaderV2 hdr;
    hdr.SetSrc(myId);
    hdr.SetDst(0xFFFF);
    // v2 wire semantics bugfix:
    // rp_counter is assigned/committed only on real TX (right before dev->Send succeeds).
    // Here we keep BEACON type and use a placeholder counter.
    hdr.SetFlagsTtl(PackFlagsTtlV2(WirePacketTypeV2::BEACON, 0));
    p->AddHeader(hdr);

    MeshMetricTag traceTag;
    traceTag.SetSrc(myId);
    traceTag.SetDst(0xFFFF);
    traceTag.SetSeq(++m_seq);
    traceTag.SetPrevHop(myId);
    traceTag.SetTtl(std::min<uint8_t>(m_initTtl, 63));
    traceTag.SetHops(0);
    traceTag.SetSf(beaconSf);
    traceTag.SetToaUs(ComputeLoRaToAUs(beaconSf, m_bw, m_cr, p->GetSize()));
    traceTag.SetBatt_mV(GetBatteryVoltageMv());
    traceTag.SetScoreX100(0);
    p->AddPacketTag(traceTag);

    if (m_routing)
    {
        m_routing->SetSequence(m_seq);
    }

    NS_LOG_UNCOND("DVTRACE_TX_V2 time=" << Simulator::Now().GetSeconds()
                                        << " node=" << GetNode()->GetId() << " seq=" << traceTag.GetSeq()
                                        << " rp_counter=deferred_tx_real"
                                        << " entries=" << entries.size() << " bytes=" << p->GetSize()
                                        << " maxRoutes=" << maxRoutes
                                        << " phase=" << GetBeaconPhaseLabel());

    m_lastDvBeaconTime = Simulator::Now();
    RecordBeaconScheduled(traceTag.GetSeq());
    SendWithCSMA(p, traceTag, Address(), true);
}

// Inicializa timers, callbacks y generación de tráfico.
void
MeshDvApp::StartApplication()
{
    uint32_t nodeId = GetNode()->GetId();

    NS_LOG_INFO("StartApplication(): node=" << nodeId << " period=" << m_period.GetSeconds() << "s"
                                            << " ttl=" << unsigned(m_initTtl)
                                            << " csmaEnabled=" << m_csmaEnabled);

    Ptr<Node> n = GetNode();
    bool callbackRegistered = false;

    for (uint32_t i = 0; i < n->GetNDevices(); ++i)
    {
        Ptr<NetDevice> dev = n->GetDevice(i);
        if (!dev)
        {
            NS_LOG_DEBUG("  Device " << i << " is null, skipping");
            continue;
        }

        // Registrar callback en TODOS los dispositivos
        dev->SetReceiveCallback(MakeCallback(&MeshDvApp::L2Receive, this));
        Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev =
            DynamicCast<ns3::lorawan::MeshLoraNetDevice>(dev);
        if (meshDev)
        {
            if (!m_meshDevice)
            {
                m_meshDevice = dev;
            }
            meshDev->SetWireFormat(m_wireFormat);
            if (m_mac)
            {
                meshDev->SetMac(m_mac);
            }
            if (m_energyModel)
            {
                meshDev->SetEnergyModel(m_energyModel);
            }
        }
        callbackRegistered = true;

        NS_LOG_INFO("  >>> Callback L2Receive registered on node "
                    << nodeId << " device " << i << " (type=" << dev->GetInstanceTypeId().GetName()
                    << ")");
    }

    if (!callbackRegistered)
    {
        NS_LOG_ERROR("  !!! NO callback registered on node " << nodeId);
    }

    // RNG debe estar disponible antes de cualquier CAD/CSMA.
    m_rng = CreateObject<UniformRandomVariable>();
    UpdateDataPeriod();
    InitDataDestinations();
    InitDataSlots();
    if (!m_mac)
    {
        m_mac = CreateObject<loramesh::CsmaCadMac>();
        m_compositeMetric.SetEnergyModel(m_energyModel);
    }
    m_mac->SetRandomStream(m_rng);
    m_mac->SetDutyCycleWindow(Hours(1));
    m_mac->SetCadDuration(m_cadDuration);
    m_mac->SetDifsCadCount(m_difsCadCount);
    m_mac->SetBackoffWindow(m_backoffWindow);

    // CRÍTICO: enlazar CsmaCadMac con LoraPhy para CAD real.
    if (m_mac)
    {
        bool meshDevFound = false;
        bool phyLinked = false;
        for (uint32_t i = 0; i < n->GetNDevices(); ++i)
        {
            Ptr<NetDevice> dev = n->GetDevice(i);
            Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev =
                DynamicCast<ns3::lorawan::MeshLoraNetDevice>(dev);
            if (!meshDev)
            {
                continue;
            }
            meshDevFound = true;
            Ptr<ns3::lorawan::LoraPhy> phy = meshDev->GetPhy();
            if (!phy)
            {
                NS_LOG_WARN("StartApplication(): node="
                            << nodeId
                            << " MeshLoraNetDevice sin PHY, no se puede enlazar CAD real");
                continue;
            }
            m_mac->SetPhy(phy);
            phyLinked = true;
            break; // enlazar solo una vez
        }
        if (!meshDevFound)
        {
            NS_LOG_WARN("StartApplication(): node="
                        << nodeId << " sin MeshLoraNetDevice, no se puede enlazar CAD real");
        }
        else if (!phyLinked)
        {
            NS_LOG_WARN("StartApplication(): node="
                        << nodeId << " MeshLoraNetDevice sin PHY valido para CAD real");
        }
    }

    if (m_energyModel)
    {
        m_energyModel->RegisterNode(nodeId);
    }

    UpdateRouteTimeout();

    if (!m_routing)
    {
        m_routing = CreateObject<loramesh::RoutingDv>();
    }
    m_routing->SetNodeId(nodeId);
    m_routing->SetRouteTimeout(m_routeTimeout);
    m_routing->SetInitTtl(m_initTtl);
    m_routing->SetMaxRoutes(GetBeaconRouteCapacity());
    m_routing->SetSequence(m_seq);
    m_routing->SetAdvertiseAllRoutes(m_advertiseAllRoutes);
    m_routing->SetSinkNodeId(m_collectorNodeId);
    m_routing->SetRouteSwitchMinDeltaX100(m_routeSwitchMinDeltaX100);
    m_routing->SetRouteChangeCallback(MakeCallback(&MeshDvApp::HandleRouteChange, this));
    m_routing->SetFloodCallback(MakeCallback(&MeshDvApp::HandleFloodRequest, this));
    BootstrapLinkAddrTableFromRx();

    // MEJORADO: Inicializar tabla de vecinos (nextHopId -> NetDevice)
    // Esta tabla permite mapeo robusto sin dependencias de heurísticas implícitas
    Ptr<Node> thisNode = GetNode();
    if (thisNode)
    {
        for (uint32_t i = 0; i < thisNode->GetNDevices(); ++i)
        {
            Ptr<NetDevice> dev = thisNode->GetDevice(i);
            if (dev)
            {
                Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev =
                    DynamicCast<ns3::lorawan::MeshLoraNetDevice>(dev);
                if (meshDev)
                {
                    // Mapear el LoRa device del nodo actual
                    // En una topología simple, asumimos solo un LoRa device por nodo
                    // Para topologías complejas, expandir la lógica aquí
                    m_neighborDevices[nodeId] = dev;
                    NS_LOG_INFO("Tabla vecinos: nodo=" << nodeId
                                                       << " registrado con MeshLoraNetDevice");
                    break;
                }
            }
        }
    }

    // Warm-up is controlled explicitly and decoupled from data traffic start time.
    m_beaconWarmupEnd = Simulator::Now() + Seconds(std::max(0.0, m_beaconWarmupSec));
    UpdateRouteTimeout();

    // Arranque asíncrono: primer beacon con retardo base + offset por nodo + jitter.
    const double jitterMax = std::max(0.0, m_initialDvJitterMax);
    const double jitter = m_rng ? m_rng->GetValue(0.0, jitterMax) : 0.0;
    const double nodeOffset = static_cast<double>(nodeId) * std::max(0.0, m_initialDvNodeSpacing);
    const double baseDelay = std::max(0.0, m_initialDvDelayBase);
    Time firstDvTime = Seconds(baseDelay + nodeOffset + jitter);
    NS_LOG_INFO("DV initial scheduled: node=" << nodeId << " t=" << firstDvTime.GetSeconds() << "s"
                                              << " base=" << baseDelay << "s"
                                              << " nodeOffset=" << nodeOffset << "s"
                                              << " jitter=" << jitter << "s");
    Simulator::Schedule(firstDvTime, &MeshDvApp::SendInitialDv, this);

    m_purgeEvt = Simulator::Schedule(Seconds(30), &MeshDvApp::PurgeExpiredRoutes, this);
    if (nodeId < 3)
    {
        m_periodicDumpEvt =
            Simulator::Schedule(Seconds(30), &MeshDvApp::SchedulePeriodicDump, this);
    }

    Simulator::Schedule(Seconds(20), &MeshDvApp::PrintRoutingTable, this);
    Simulator::Schedule(Seconds(40), &MeshDvApp::PrintRoutingTable, this);

    // Programar generación de datos (controlada por TrafficMode)
    const double dataDelay = std::max(0.0, m_dataStartTimeSec);
    const double dataJitter = m_rng ? m_rng->GetValue(0.0, 5.0) : 0.0; // 0-5 segundos de jitter
    m_dataStopLogged = false;
    if (!m_dataDestinations.empty())
    {
        Time dataStartDelay = Seconds(dataDelay + dataJitter);
        if (m_enableDataSlots && m_dataSlotPeriodSec > 0.0)
        {
            dataStartDelay = ComputeNextDataSlotDelay(dataStartDelay);
        }
        const double firstDataTimeSec = (Simulator::Now() + dataStartDelay).GetSeconds();
        if (m_dataStopTimeSec >= 0.0 && firstDataTimeSec >= m_dataStopTimeSec)
        {
            NS_LOG_INFO("  >>> Node " << nodeId
                                      << " - Data generation disabled by DataStopTimeSec="
                                      << m_dataStopTimeSec << "s"
                                      << " (firstDataAt=" << firstDataTimeSec << "s)");
        }
        else
        {
            NS_LOG_INFO("  >>> Node " << nodeId << " - Data generation scheduled at t="
                                      << firstDataTimeSec << "s (mode=any-to-any"
                                      << " load=" << GetTrafficLoad()
                                      << " period=" << m_dataGenerationPeriod.GetSeconds() << "s"
                                      << " stop=" << m_dataStopTimeSec << "s)");
            m_dataGenerationEvt =
                Simulator::Schedule(dataStartDelay, &MeshDvApp::GenerateDataTraffic, this);
        }
    }
    else
    {
        NS_LOG_INFO("  >>> Node " << nodeId << " - No data generation (mode=any-to-any"
                                  << " collectorNodeId=" << m_collectorNodeId << ")");
    }
}

// Cancela eventos y reporta estadísticas cuando se detiene la app.
void
MeshDvApp::StopApplication()
{
    NS_LOG_INFO("StopApplication(): node=" << GetNode()->GetId());

    if (m_evt.IsPending())
    {
        Simulator::Cancel(m_evt);
    }
    if (m_evtSf9.IsPending())
    {
        Simulator::Cancel(m_evtSf9);
    }
    if (m_evtSf10.IsPending())
    {
        Simulator::Cancel(m_evtSf10);
    }
    if (m_evtSf12.IsPending())
    {
        Simulator::Cancel(m_evtSf12);
    }

    if (m_purgeEvt.IsPending())
    {
        Simulator::Cancel(m_purgeEvt);
    }

    if (m_backoffEvt.IsPending())
    {
        Simulator::Cancel(m_backoffEvt);
    }

    if (!m_txQueue.empty())
    {
        const uint32_t myId = GetNode()->GetId();
        for (const auto& entry : m_txQueue)
        {
            if (entry.tag.GetDst() == 0xFFFF || entry.tag.GetSrc() != myId)
            {
                continue;
            }

            std::string reason = entry.pendingReason.empty() ? "queue_pending_end"
                                                             : entry.pendingReason;
            if (reason == "tx_attempt_air")
            {
                reason = "inflight_pending_end";
            }

            NS_LOG_UNCOND("FWDTRACE ORIGIN_PENDING_END time="
                          << Simulator::Now().GetSeconds() << " node=" << myId
                          << " src=" << entry.tag.GetSrc() << " dst=" << entry.tag.GetDst()
                          << " seq=" << entry.tag.GetSeq() << " sf=" << unsigned(entry.tag.GetSf())
                          << " reason=" << reason << " deferCount=" << entry.deferCount
                          << " queueSize=" << m_txQueue.size());
        }
    }

    // ========================================================================
    // NUEVO: Reportar estadísticas de datos al finalizar
    // ========================================================================
    if (GetNode()->GetId() < 3) // Si es ED
    {
        NS_LOG_INFO("=== DATA STATISTICS Node " << GetNode()->GetId() << " ===");
        NS_LOG_INFO("  Data packets generated: " << m_dataPacketsGenerated);
        NS_LOG_INFO("  Data packets delivered: " << m_dataPacketsDelivered);
        NS_LOG_INFO("  Data no-route: " << m_dataNoRoute);
        double pdr = (m_dataPacketsGenerated > 0)
                         ? (100.0 * m_dataPacketsDelivered / m_dataPacketsGenerated)
                         : 0.0;
        NS_LOG_INFO("  PDR: " << pdr << "%");
    }
    else if (GetNode()->GetId() == m_collectorNodeId) // Si es GW
    {
        NS_LOG_INFO("=== GATEWAY RECEIVED ===");
        NS_LOG_INFO("  Total data packets received: " << m_dataPacketsDelivered);
        NS_LOG_INFO("  Data no-route (local count): " << m_dataNoRoute);
    }

    if (g_metricsCollector)
    {
        double dutyUsed = m_mac ? m_mac->GetDutyCycleUsed() : 0.0;
        g_metricsCollector->RecordDuty(GetNode()->GetId(), dutyUsed, m_txCount, m_backoffCount);
        double energyJ = GetRemainingEnergyJ();
        double energyFrac = GetEnergyFraction();
        g_metricsCollector->RecordEnergySnapshot(GetNode()->GetId(), energyJ, energyFrac);
        MetricsCollector::RuntimeNodeStats runtimeStats;
        runtimeStats.nodeId = GetNode()->GetId();
        runtimeStats.txQueueLenEnd = static_cast<uint32_t>(m_txQueue.size());
        runtimeStats.queuedPacketsEnd = static_cast<uint32_t>(m_txQueue.size());
        runtimeStats.cadBusyEvents = m_cadBusyEvents;
        runtimeStats.dutyBlockedEvents = m_dutyBlockedEvents;
        runtimeStats.totalWaitTimeDueToDutyS = m_totalWaitTimeDueToDutySec;
        runtimeStats.dropNoRoute = m_dropNoRoute;
        runtimeStats.dropTtlExpired = m_dropTtlExpired;
        runtimeStats.dropQueueOverflow = m_dropQueueOverflow;
        runtimeStats.dropBacktrack = m_dropBacktrack;
        runtimeStats.dropOther = m_dropOther;
        runtimeStats.beaconScheduled = m_beaconScheduled;
        runtimeStats.beaconTxSent = m_beaconTxSent;
        runtimeStats.beaconBlockedByDuty = m_beaconBlockedByDuty;
        runtimeStats.rpGapLargeEvents = m_rpGapLargeEvents;
        g_metricsCollector->RecordRuntimeNodeStats(runtimeStats);
    }

    PrintRoutingTable();
    if (m_routing)
    {
        m_routing->DebugDumpRoutingTable();
    }
}

void
MeshDvApp::Tick()
{
    NS_LOG_INFO("Tick(): node=" << GetNode()->GetId() << " t=" << Simulator::Now().GetSeconds()
                                << "s");

    BuildAndSendDv(m_sfControl);
    m_evt = Simulator::Schedule(m_period, &MeshDvApp::Tick, this);
}

// Procesa cada recepción L2 y decide si actualizar rutas o reenviar.
bool
MeshDvApp::L2Receive(Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address& from)
{
    NS_LOG_UNCOND("L2Receive ACTIVADA node=" << GetNode()->GetId() << " proto=" << proto
                                             << " kProtoMesh=" << kProtoMesh);
    if (proto != kProtoMesh)
    {
        return false;
    }

    if (m_wireFormat == "v2")
    {
        return L2ReceiveV2(dev, p, proto, from);
    }

    MeshMetricTag tag;
    bool has = p->PeekPacketTag(tag);
    NS_LOG_UNCOND("PeekPacketTag: node=" << GetNode()->GetId() << " has=" << has
                                         << " dst=" << tag.GetDst() << " src=" << tag.GetSrc());

    if (!has)
    {
        NS_LOG_INFO("RX (node=" << GetNode()->GetId() << "): packet sin MeshMetricTag");
        return true;
    }

    // Trazas detalladas de recepción para correlacionar rutas planeadas vs recorridas.
    NS_LOG_UNCOND("FWDTRACE rx time="
                  << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                  << " src=" << tag.GetSrc() << " dst=" << tag.GetDst() << " seq=" << tag.GetSeq()
                  << " ttl=" << unsigned(tag.GetTtl()) << " hopsSeen=" << unsigned(tag.GetHops())
                  << " sf=" << unsigned(tag.GetSf()));

    // Aprender dirección de enlace del emisor (wrapper Mac48 interno) para mapear ID lógico -> next-hop.
    Mac48Address fromMac = Mac48Address::ConvertFrom(from);
    uint32_t prevHop = tag.GetPrevHop();
    if (prevHop == 0xFFFF)
    {
        prevHop = tag.GetSrc();
    }
    m_linkAddrTable[prevHop] = fromMac;
    m_linkAddrLastSeen[prevHop] = Simulator::Now();

    uint32_t myId = GetNode()->GetId();
    uint32_t dst = tag.GetDst();
    double energyJ = GetRemainingEnergyJ();
    double energyFrac = GetEnergyFraction();
    bool isData = (dst != 0xFFFF);
    bool seenBefore = false;
    SeenDataInfo* seenInfo = nullptr;
    std::tuple<uint32_t, uint32_t, uint32_t> dataKey;
    if (isData)
    {
        // DEBUG: Log para rastrear flujo de datos
        NS_LOG_UNCOND("L2RX_DATA_ENTRY node=" << myId << " src=" << tag.GetSrc() << " dst=" << dst
                                              << " seq=" << tag.GetSeq() << " myId==dst? "
                                              << (myId == dst ? "YES" : "NO"));
        NS_LOG_INFO("DATA_RX detail: node=" << myId << " src=" << tag.GetSrc() << " dst=" << dst
                                            << " seq=" << tag.GetSeq()
                                            << " time=" << Simulator::Now().GetSeconds() << "s"
                                            << " sf=" << unsigned(tag.GetSf()));
        CleanOldDedupCaches();
        CleanOldSeenData();
        dataKey = std::make_tuple(tag.GetSrc(), tag.GetDst(), tag.GetSeq());
        auto itSeen = m_seenData.find(dataKey);
        if (itSeen != m_seenData.end())
        {
            seenBefore = true;
            seenInfo = &itSeen->second;
        }
    }

    // ========================================================================
    // CASO 1: Paquete llegó a su destino final (YO soy el destino)
    // ========================================================================
    if (myId == dst)
    {
        if (isData)
        {
            if (!seenBefore)
            {
                SeenDataInfo info;
                info.firstSeen = Simulator::Now();
                info.hadRoute = true;
                info.forwarded = false;
                m_seenData[dataKey] = info;
            }
            else if (seenInfo)
            {
                seenInfo->hadRoute = true;
            }
        }
        // Entrega en sink con control de duplicados
        auto deliveredKey = std::make_tuple(tag.GetSrc(), tag.GetDst(), tag.GetSeq());
        if (m_deliveredSet.find(deliveredKey) != m_deliveredSet.end())
        {
            NS_LOG_UNCOND("FWDTRACE drop_dup_sink_delivered time="
                          << Simulator::Now().GetSeconds() << " node=" << myId << " src="
                          << tag.GetSrc() << " dst=" << tag.GetDst() << " seq=" << tag.GetSeq());
            return true;
        }
        m_deliveredSet[deliveredKey] = Simulator::Now();
        m_dataPacketsDelivered++;
        NS_LOG_INFO(">>> DATA DELIVERED: node=" << myId << " src=" << tag.GetSrc() << " dst="
                                                << tag.GetDst() << " seq=" << tag.GetSeq()
                                                << " hops=" << (int)tag.GetHops());

        // Registrar recepción final como ENTREGADA en red mesh (no forward).
        LogRxEvent(tag.GetSrc(),
                   tag.GetDst(),
                   tag.GetSeq(),
                   tag.GetTtl(),
                   tag.GetHops(),
                   tag.GetBatt_mV(),
                   tag.GetScoreX100(),
                   tag.GetSf(),
                   energyJ,
                   energyFrac,
                   false);

        if (g_metricsCollector)
        {
            double txTime =
                g_metricsCollector->GetFirstTxTime(tag.GetSrc(), tag.GetDst(), tag.GetSeq());
            double delaySec = (txTime >= 0.0) ? (Simulator::Now().GetSeconds() - txTime) : -1.0;
            g_metricsCollector->RecordE2eDelay(tag.GetSrc(),
                                               tag.GetDst(),
                                               tag.GetSeq(),
                                               tag.GetHops(),
                                               delaySec,
                                               p->GetSize(),
                                               tag.GetSf(),
                                               true);
            g_metricsCollector->RecordEnergySnapshot(myId, energyJ, energyFrac);
        }
        NS_LOG_UNCOND("FWDTRACE deliver time=" << Simulator::Now().GetSeconds() << " node=" << myId
                                               << " src=" << tag.GetSrc() << " dst=" << tag.GetDst()
                                               << " seq=" << tag.GetSeq() << " hops="
                                               << unsigned(tag.GetHops()) << " reason=dst_local");
        return true; // NO forward si ya llegó a destino
    }

    // ========================================================================
    // CASO 2: Beacon DV (broadcast) - Actualizar tabla de rutas
    // ========================================================================
    if (dst == 0xFFFF)
    {
        // SF EMPÍRICO: aprender SF solo desde beacons de control, no desde datos.
        UpdateNeighborLinkSf(prevHop, tag.GetSf());

        NS_LOG_UNCOND("DV_RX node" << myId << " from=" << tag.GetSrc() << " dst=" << tag.GetDst()
                                   << " metric=" << tag.GetScoreX100()
                                   << " hops=" << unsigned(tag.GetHops()));
        NS_LOG_UNCOND("DVTRACE_RX_OK time="
                      << Simulator::Now().GetSeconds() << " node=" << myId << " src="
                      << tag.GetSrc() << " seq=" << tag.GetSeq() << " size=" << p->GetSize()
                      << " prevHop=" << tag.GetPrevHop() << " ttl=" << unsigned(tag.GetTtl())
                      << " hopsSeen=" << unsigned(tag.GetHops()) << " sf=" << unsigned(tag.GetSf())
                      << " sf=" << unsigned(tag.GetSf()));
        // 1. Aprende ruta directa hacia el vecino que transmitió el beacon
        NS_LOG_INFO("Intento aprender ruta directa: src=" << tag.GetSrc()
                                                          << " en node=" << GetNode()->GetId());
        // REMOVED: neighborRssi - no se usa en métrica, obtenemos de PHY si necesario
        // Usar el SF con el que se recibió el beacon para reflejar el enlace real.
        uint8_t sfForNeighbor = tag.GetSf();
        uint32_t toaUsNeighbor = ComputeLoRaToAUs(sfForNeighbor, m_bw, m_cr, p->GetSize());
        NS_LOG_INFO("  → SF=" << unsigned(sfForNeighbor) << " toa=" << toaUsNeighbor << "us");
        ProcessDvPayload(p, tag, fromMac, toaUsNeighbor);
        m_lastDvRxTime = Simulator::Now();
        uint32_t currentRoutes = m_routing ? m_routing->GetRouteCount() : 0;
        NS_LOG_INFO("Tabla rutas tras aprender: size=" << currentRoutes);

        LogRxEvent(tag.GetSrc(),
                   tag.GetDst(),
                   tag.GetSeq(),
                   tag.GetTtl(),
                   tag.GetHops(),
                   tag.GetBatt_mV(),
                   tag.GetScoreX100(),
                   tag.GetSf(),
                   energyJ,
                   energyFrac,
                   false);

        if (m_enableDvFlooding)
        {
            uint8_t ttl = tag.GetTtl();
            if (ttl == 0)
            {
                return true;
            }
            ttl -= 1;
            uint8_t hops = tag.GetHops() + 1;
            MeshMetricTag newTag = tag;
            newTag.SetTtl(ttl);
            newTag.SetHops(hops);
            newTag.SetPrevHop(myId);
            newTag.SetSf(m_sfControl);
            // REMOVED: SetRssiDbm - no se serializa
            newTag.SetBatt_mV(GetBatteryVoltageMv());
            newTag.SetToaUs(ComputeLoRaToAUs(newTag.GetSf(), m_bw, m_cr, p->GetSize()));
            newTag.SetScoreX100(ComputeScoreX100(newTag));
            if (ttl > 0)
            {
                Simulator::Schedule(MilliSeconds(5 + (myId % 5)),
                                    &MeshDvApp::ForwardWithTtl,
                                    this,
                                    p,
                                    newTag);
            }
        }
        return true;
    }

    // ========================================================================
    // CASO 3: Datos unicast - Verificar si debemos forward
    // ========================================================================
    const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute(dst) : nullptr;
    uint8_t ttl = tag.GetTtl();
    uint8_t hops = tag.GetHops() + 1;

    bool allowForward = true;
    bool noRoute = false;
    bool seenDrop = false;
    if (isData)
    {
        if (seenBefore)
        {
            if (route && seenInfo && !seenInfo->forwarded)
            {
                seenInfo->forwarded = true;
                seenInfo->hadRoute = true;
            }
            else
            {
                seenDrop = true;
                allowForward = false;
            }
        }
        else
        {
            SeenDataInfo info;
            info.firstSeen = Simulator::Now();
            info.hadRoute = (route != nullptr);
            info.forwarded = (route != nullptr);
            m_seenData[dataKey] = info;
            if (!route)
            {
                noRoute = true;
                allowForward = false;
            }
        }
    }

    bool canForward = allowForward && (route != nullptr) && (ttl > 0);
    bool backtrackDrop = false;
    if (allowForward && route && m_avoidImmediateBacktrack)
    {
        const uint32_t incomingPrevHop = tag.GetPrevHop();
        if (incomingPrevHop != 0xFFFF && route->nextHop == incomingPrevHop)
        {
            backtrackDrop = true;
            allowForward = false;
            canForward = false;
        }
    }

    // Registrar RX aun si no se reenvía (para métricas de PDR).
    LogRxEvent(tag.GetSrc(),
               tag.GetDst(),
               tag.GetSeq(),
               tag.GetTtl(),
               tag.GetHops(),
               tag.GetBatt_mV(),
               tag.GetScoreX100(),
               tag.GetSf(),
               energyJ,
               energyFrac,
               canForward);

    if (!allowForward)
    {
        if (noRoute)
        {
            NS_LOG_WARN("Node " << myId << " RX DROP: No route to dst=" << dst);
            const uint32_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
            const bool hasGwRoute = m_routing ? m_routing->HasRoute(m_collectorNodeId) : false;
            RouteStatus rs = ValidateRoute(dst); // REFACTORING: usar helper
            NS_LOG_INFO("DATA_NOROUTE detail: node="
                        << myId << " src=" << tag.GetSrc() << " dst=" << dst << " seq="
                        << tag.GetSeq() << " time=" << Simulator::Now().GetSeconds() << "s"
                        << " routesKnown=" << routeCount << " hasGwRoute=" << (hasGwRoute ? 1 : 0)
                        << " hasEntry=" << (rs.exists ? 1 : 0) << " expired="
                        << (rs.expired ? 1 : 0) << " collectorNodeId=" << m_collectorNodeId);
            NS_LOG_UNCOND("FWDTRACE DATA_NOROUTE time=" << Simulator::Now().GetSeconds() << " node="
                                                        << myId << " src=" << tag.GetSrc()
                                                        << " dst=" << dst << " seq=" << tag.GetSeq()
                                                        << " reason=no_route_rx");
            m_dropNoRoute++;
            DumpFullTable("DATA_NOROUTE_RX");
        }
        else if (seenDrop)
        {
            NS_LOG_UNCOND("FWDTRACE drop_seen_once time="
                          << Simulator::Now().GetSeconds() << " node=" << myId
                          << " src=" << tag.GetSrc() << " dst=" << dst << " seq=" << tag.GetSeq()
                          << " reason=seen_data");
        }
        else if (backtrackDrop)
        {
            NS_LOG_UNCOND("FWDTRACE backtrack_drop time="
                          << Simulator::Now().GetSeconds() << " node=" << myId
                          << " src=" << tag.GetSrc() << " dst=" << dst << " seq=" << tag.GetSeq()
                          << " nextHop=" << (route ? route->nextHop : 0)
                          << " prevHopInPacket=" << tag.GetPrevHop() << " reason=avoid_backtrack");
            m_dropBacktrack++;
        }
        return true;
    }

    // ========================================================================
    // FILTRO CRÍTICO: Solo forward si SOY uno de los posibles next-hops
    // En broadcast, todos reciben, pero solo nodos con ruta válida forwardean
    // ========================================================================
    if (ttl == 0)
    {
        NS_LOG_WARN("Node " << myId << " RX DROP: TTL=0");
        NS_LOG_UNCOND("FWDTRACE drop_ttl time=" << Simulator::Now().GetSeconds() << " node=" << myId
                                                << " src=" << tag.GetSrc() << " dst=" << dst
                                                << " seq=" << tag.GetSeq()
                                                << " reason=ttl_expired");
        m_dropTtlExpired++;
        return true;
    }

    ttl -= 1;

    MeshMetricTag newTag = tag;
    newTag.SetTtl(ttl);
    newTag.SetHops(hops);
    newTag.SetPrevHop(myId);
    uint8_t selectedSf = m_sf;
    newTag.SetSf(selectedSf);
    newTag.SetToaUs(ComputeLoRaToAUs(selectedSf, m_bw, m_cr, p->GetSize()));
    // REMOVED: SetRssiDbm - no se serializa, receptor mide RSSI
    newTag.SetBatt_mV(GetBatteryVoltageMv());
    newTag.SetScoreX100(ComputeScoreX100(newTag));

    NS_LOG_INFO("RX DATA: src=" << tag.GetSrc() << " dst=" << dst << " seq=" << tag.GetSeq()
                                << " hops=" << (int)tag.GetHops());
    NS_LOG_INFO("FWD DATA: src=" << newTag.GetSrc() << " dst=" << dst << " seq=" << newTag.GetSeq()
                                 << " hops=" << (int)hops << " nextHop=" << route->nextHop);
    NS_LOG_UNCOND("FWDTRACE plan time="
                  << Simulator::Now().GetSeconds() << " node=" << myId << " src=" << newTag.GetSrc()
                  << " dst=" << dst << " seq=" << newTag.GetSeq() << " ttlAfter=" << unsigned(ttl)
                  << " hopsPlanned=" << unsigned(route->hops) << " nextHop=" << route->nextHop
                  << " reason=route_found");

    // Forward datos con delay para evitar colisiones
    Simulator::Schedule(MilliSeconds(10 + (myId % 5)), &MeshDvApp::ForwardWithTtl, this, p, newTag);

    return true;
}

// Realiza el forwarding cuando el TTL lo permite.
void
MeshDvApp::ForwardWithTtl(Ptr<const Packet> pIn, const MeshMetricTag& inTag)
{
    uint32_t myId = GetNode()->GetId();
    uint32_t dst = inTag.GetDst();
    double energyJ = GetRemainingEnergyJ();
    double energyFrac = GetEnergyFraction();

    // ========================================================================
    // CASO 1: Si YO soy el destino final, contabilizar entrega y NO forward
    // ========================================================================
    if (myId == dst)
    {
        if (myId == m_collectorNodeId) // Si soy el GW
        {
            m_dataPacketsDelivered++;
            NS_LOG_INFO(">>> DATA DELIVERED to GW: src=" << inTag.GetSrc()
                                                         << " seq=" << inTag.GetSeq()
                                                         << " hops=" << (int)inTag.GetHops());

            // Registrar entrega final (no forward).
            LogRxEvent(inTag.GetSrc(),
                       inTag.GetDst(),
                       inTag.GetSeq(),
                       inTag.GetTtl(),
                       inTag.GetHops(),
                       inTag.GetBatt_mV(),
                       inTag.GetScoreX100(),
                       inTag.GetSf(),
                       energyJ,
                       energyFrac,
                       false);

            if (g_metricsCollector)
            {
                double txTime = g_metricsCollector->GetFirstTxTime(inTag.GetSrc(),
                                                                   inTag.GetDst(),
                                                                   inTag.GetSeq());
                double delaySec = (txTime >= 0.0) ? (Simulator::Now().GetSeconds() - txTime) : -1.0;
                g_metricsCollector->RecordE2eDelay(inTag.GetSrc(),
                                                   inTag.GetDst(),
                                                   inTag.GetSeq(),
                                                   inTag.GetHops(),
                                                   delaySec,
                                                   pIn->GetSize(),
                                                   inTag.GetSf(),
                                                   true);
                g_metricsCollector->RecordEnergySnapshot(myId, energyJ, energyFrac);
            }
            NS_LOG_UNCOND("FWDTRACE deliver time="
                          << Simulator::Now().GetSeconds() << " node=" << myId
                          << " src=" << inTag.GetSrc() << " dst=" << inTag.GetDst()
                          << " seq=" << inTag.GetSeq() << " hops=" << unsigned(inTag.GetHops())
                          << " reason=dst_local");
        }
        return; // NO forward
    }

    // ========================================================================
    // CASO ESPECIAL: Broadcast (DV) -> inundación sin consulta de rutas
    // ========================================================================
    if (dst == 0xFFFF)
    {
        if (inTag.GetTtl() == 0)
        {
            return;
        }

        uint32_t toaUs = ComputeLoRaToAUs(inTag.GetSf(), m_bw, m_cr, pIn->GetSize());

        Ptr<Packet> pkt = pIn->Copy();
        MeshMetricTag fwdTag = inTag;
        fwdTag.SetToaUs(toaUs);
        fwdTag.SetPrevHop(myId);
        pkt->RemoveAllPacketTags();
        pkt->AddPacketTag(fwdTag);

        NS_LOG_INFO("Node " << myId << " FWD DV broadcast ttl=" << unsigned(fwdTag.GetTtl())
                            << " sf=" << unsigned(fwdTag.GetSf()) << " toaUs=" << toaUs);
        SendWithCSMA(pkt, fwdTag, Address());
        return;
    }

    // ========================================================================
    // CASO 2: Verificar si tenemos ruta al destino
    // Flujo esperado: GetRoute() -> usar route->nextHop tal cual (se resuelve a linkAddr abajo)
    // ========================================================================
    const loramesh::RouteEntry* routePreview = m_routing ? m_routing->GetRoute(dst) : nullptr;
    NS_LOG_UNCOND("FWD_CHECK node"
                  << myId << " dst=" << dst << " route="
                  << (routePreview ? ("OK nextHop=" + std::to_string(routePreview->nextHop))
                                   : "NULL"));
    const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute(dst) : nullptr;
    if (!route)
    {
        NS_LOG_WARN("Node " << myId << " FWD DROP: No route to dst=" << dst);
        const uint32_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
        const bool hasGwRoute = m_routing ? m_routing->HasRoute(m_collectorNodeId) : false;
        RouteStatus rs = ValidateRoute(dst); // REFACTORING: usar helper
        NS_LOG_INFO("DATA_NOROUTE detail: node="
                    << myId << " src=" << inTag.GetSrc() << " dst=" << dst
                    << " seq=" << inTag.GetSeq() << " time=" << Simulator::Now().GetSeconds() << "s"
                    << " routesKnown=" << routeCount << " hasGwRoute=" << (hasGwRoute ? 1 : 0)
                    << " hasEntry=" << (rs.exists ? 1 : 0) << " expired=" << (rs.expired ? 1 : 0)
                    << " collectorNodeId=" << m_collectorNodeId);
        NS_LOG_UNCOND("FWDTRACE drop_noroute time=" << Simulator::Now().GetSeconds()
                                                    << " node=" << myId << " src=" << inTag.GetSrc()
                                                    << " dst=" << dst << " seq=" << inTag.GetSeq()
                                                    << " reason=no_route");
        DumpFullTable("DATA_NOROUTE_TX");
        return;
    }
    // Log detallado de la ruta usada
    NS_LOG_UNCOND("FWDTRACE route time="
                  << Simulator::Now().GetSeconds() << " node=" << myId << " dst=" << dst
                  << " route_dst=" << route->destination << " nextHop=" << route->nextHop
                  << " hops=" << unsigned(route->hops) << " score=" << route->scoreX100
                  << " seqNum=" << route->seqNum << " sf=" << unsigned(route->sf));

    // ========================================================================
    // CASO 3: Verificar duty cycle para forward
    // ========================================================================
    // CASE 3: Check duty cycle for forward
    // ========================================================================
    uint8_t sfForRoute = m_sf;
    if (m_useRouteSfForData && route)
    {
        sfForRoute = route->sf;
    }
    if (m_useEmpiricalSfForData && route)
    {
        // Selección por salto local también en relay para evitar SF optimista de ruta remota.
        sfForRoute = GetDataSfForNeighbor(route->nextHop);
    }
    sfForRoute = std::clamp<uint8_t>(sfForRoute, m_sfMin, m_sfMax);
    uint32_t toaUs = ComputeLoRaToAUs(sfForRoute, m_bw, m_cr, pIn->GetSize());
    if (m_mac && !m_mac->CanTransmitNow(toaUs / 1e6))
    {
        NS_LOG_UNCOND("FWDTRACE duty_defer time="
                      << Simulator::Now().GetSeconds() << " node=" << myId
                      << " src=" << inTag.GetSrc() << " dst=" << dst << " seq=" << inTag.GetSeq()
                      << " dutyUsed=" << m_mac->GetDutyCycleUsed()
                      << " dutyLimit=" << m_mac->GetDutyCycleLimit()
                      << " reason=duty_wait_queue");
    }

    // ========================================================================
    // CASO 4: Preparar paquete para forward
    // NO se modifica nextHop después de resolver la ruta; solo se resuelve a dirección de enlace.
    // ========================================================================
    std::ostringstream oss;
    oss << "TX FWD: src=" << inTag.GetSrc() << " dst=" << dst << " seq=" << inTag.GetSeq()
        << " hops=" << (int)inTag.GetHops() << " nextHop=" << route->nextHop;
    NS_LOG_INFO(oss.str());
    Mac48Address routeMac;
    bool usingStaleMac = false;
    const bool hasUsableMac = ResolveUnicastNextHopLinkAddr(route->nextHop, &routeMac, &usingStaleMac);
    NS_LOG_UNCOND("LINKADDR_CHECK node" << myId << " nextHop=" << route->nextHop
                                   << " linkAddrFound=" << (m_linkAddrTable.find(route->nextHop) != m_linkAddrTable.end() ? "YES" : "NO")
                                   << " linkAddrFresh=" << (IsLinkAddrFresh(route->nextHop) ? "YES" : "NO")
                                   << " staleAllowed=" << (m_allowStaleLinkAddrForUnicastData ? "YES" : "NO")
                                   << " usingStale=" << (usingStaleMac ? "YES" : "NO"));
    Address dstAddr = hasUsableMac ? Address(routeMac) : Address();
    if (!hasUsableMac || dstAddr.IsInvalid())
    {
        const uint32_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
        const bool hasGwRoute = m_routing ? m_routing->HasRoute(m_collectorNodeId) : false;
        RouteStatus rs = ValidateRoute(dst); // REFACTORING: usar helper
        NS_LOG_INFO("DATA_NOROUTE detail: node="
                    << myId << " src=" << inTag.GetSrc() << " dst=" << dst
                    << " seq=" << inTag.GetSeq() << " time=" << Simulator::Now().GetSeconds() << "s"
                    << " routesKnown=" << routeCount << " hasGwRoute=" << (hasGwRoute ? 1 : 0)
                    << " hasEntry=" << (rs.exists ? 1 : 0) << " expired=" << (rs.expired ? 1 : 0)
                    << " collectorNodeId=" << m_collectorNodeId << " reason=no_link_addr_for_unicast");
        NS_LOG_UNCOND("FWDTRACE DATA_NOROUTE time="
                      << Simulator::Now().GetSeconds() << " node=" << myId
                      << " src=" << inTag.GetSrc() << " dst=" << dst << " seq=" << inTag.GetSeq()
                      << " nextHop=" << route->nextHop << " reason=no_link_addr_for_unicast");
        DumpFullTable("DATA_NOROUTE_TX");
        return;
    }

    double txPowerDbm = -1.0;
    Ptr<ns3::lorawan::MeshLoraNetDevice> meshDevTx =
        DynamicCast<ns3::lorawan::MeshLoraNetDevice>(m_meshDevice);
    if (meshDevTx)
    {
        txPowerDbm = meshDevTx->GetTxPowerDbm();
    }
    const uint32_t cadFailures = m_mac ? m_mac->GetFailureCount() : 0;
    const uint32_t lastBackoff = m_mac ? m_mac->GetLastBackoffSlots() : 0;
    const uint32_t lastWindow = m_mac ? m_mac->GetLastBackoffWindowSlots() : 0;
    const double cadLoad = m_mac ? m_mac->GetCadLoadEstimate() : 0.0;
    NS_LOG_INFO("DATA_TX detail: node="
                << myId << " src=" << inTag.GetSrc() << " dst=" << dst << " seq=" << inTag.GetSeq()
                << " time=" << Simulator::Now().GetSeconds() << "s"
                << " nextHop=" << route->nextHop << " sf=" << unsigned(sfForRoute)
                << " txPowerDbm=" << txPowerDbm << " cadFailures=" << cadFailures
                << " backoffSlotsLast=" << lastBackoff << " windowSlotsLast=" << lastWindow
                << " cadLoad=" << cadLoad);

    NS_LOG_UNCOND("FWDTRACE fwd time="
                  << Simulator::Now().GetSeconds() << " node=" << myId << " src=" << inTag.GetSrc()
                  << " dst=" << dst << " seq=" << inTag.GetSeq()
                  << " ttl=" << unsigned(inTag.GetTtl()) << " ttlAfter=" << unsigned(inTag.GetTtl())
                  << " nextHop=" << route->nextHop << " hopsPlanned=" << unsigned(route->hops)
                  << " tx_mode=unicast"
                  << " reason=ok");
    NS_LOG_UNCOND("DATA_TX SF" << unsigned(sfForRoute) << " node=" << myId << " src="
                               << inTag.GetSrc() << " dst=" << dst << " seq=" << inTag.GetSeq());

    // Snapshot de ruta usada en el momento del forward
    if (g_metricsCollector)
    {
        g_metricsCollector->RecordRouteUsed(myId,
                                            dst,
                                            route->nextHop,
                                            route->hops,
                                            route->scoreX100,
                                            route->seqNum);
    }

    Ptr<Packet> modifiablePkt = pIn->Copy();
    MeshMetricTag outTag = inTag;
    outTag.SetSf(sfForRoute);
    outTag.SetToaUs(toaUs);
    // REMOVED: SetRssiDbm - no se serializa
    outTag.SetBatt_mV(GetBatteryVoltageMv());
    outTag.SetScoreX100(ComputeScoreX100(outTag));
    outTag.SetPrevHop(myId);
    outTag.SetExpectedNextHop(route->nextHop);

    // ==== CORRECCIÓN RSSI POR HOP ====
    ns3::lorawan::LoraTag loraTag;
    if (modifiablePkt->PeekPacketTag(loraTag))
    {
        modifiablePkt->RemovePacketTag(loraTag);
    }
    ns3::lorawan::LoraTag emptyLoraTag;
    modifiablePkt->AddPacketTag(emptyLoraTag);
    // ==== FIN CORRECCIÓN ====

    // Quitar y agregar tags de métrica
    modifiablePkt->RemoveAllPacketTags();
    modifiablePkt->AddPacketTag(outTag);

    NS_LOG_UNCOND("Enviando con kProtoMesh=" << kProtoMesh);
    SendWithCSMA(modifiablePkt, outTag, dstAddr);
}

uint32_t
MeshDvApp::ComputeLoRaToAUs(uint8_t sf, uint32_t bw, uint8_t cr, uint32_t pl) const
{
    const double bwHz = static_cast<double>(bw);
    const double tSym = std::pow(2.0, sf) / bwHz;

    const bool ih = m_ih;
    const bool de = m_de;
    const bool crc = m_crc;

    const int8_t sf_i = static_cast<int8_t>(sf);
    const double num = (8.0 * static_cast<double>(pl) - 4.0 * sf_i + 28.0 + (crc ? 16.0 : 0.0) -
                        (ih ? 20.0 : 0.0));
    const double den = 4.0 * (sf_i - (de ? 2.0 : 0.0));
    const double ce = std::ceil(std::max(num / den, 0.0));
    const double paySym = 8.0 + ce * (cr + 4.0);

    const double tPreamble = (8.0 + 4.25) * tSym;
    const double tPayload = paySym * tSym;
    const double tTot = tPreamble + tPayload;
    return static_cast<uint32_t>(tTot * 1e6 + 0.5);
}

uint16_t
MeshDvApp::ComputeScoreX100(const MeshMetricTag& t) const
{
    NodeId srcId = 0;
    Ptr<Node> node = GetNode();
    if (node)
    {
        srcId = node->GetId();
    }

    loramesh::LinkStats stats;
    stats.toaUs = t.GetToaUs();
    stats.hops = t.GetHops();
    stats.sf = t.GetSf();
    // REMOVED: SNR/RSSI not used in metric - link quality embedded in SF selection
    stats.snrDb = 0.0;
    stats.batteryMv = t.GetBatt_mV();
    stats.energyFraction = GetEnergyFraction();

    const double cost = m_compositeMetric.ComputeLinkCost(srcId, t.GetDst(), stats);
    const double score = std::clamp(1.0 - cost, 0.0, 1.0);
    return static_cast<uint16_t>(std::round(score * 100.0));
}

// ========================================================================
// B3: Selección probabilística de SF según el paper original
// SF más bajos tienen mayor probabilidad (2x por cada nivel)
// Distribución aproximada: SF7=50%, SF8=25%, SF9=12.5%, etc.
// ========================================================================
uint8_t
MeshDvApp::SelectRandomSfProbabilistic() const
{
    NS_ASSERT(m_rng != nullptr);

    // Algoritmo del paper: cada SF tiene 50% de ser seleccionado
    // Si falla, pasa al siguiente. SF12 es fallback.
    for (uint8_t sf = m_sfMin; sf <= m_sfMax; ++sf)
    {
        double val = m_rng->GetValue(0.0, 1.0);
        if (val < 0.5)
        {
            // NS_LOG_UNCOND("SF_PROB select=" << unsigned(sf) << " val=" << val);
            return sf;
        }
    }
    // NS_LOG_UNCOND("SF_PROB fallback=12");
    return m_sfMax; // Fallback si ninguno fue seleccionado (probabilidad ~1.5%)
}

void
MeshDvApp::PrintRoutingTable()
{
    double remainingEnergy = GetRemainingEnergyJ();
    double voltageAvg = (loramesh::EnergyModel::kDefaultVoltageMaxMv +
                         loramesh::EnergyModel::kDefaultVoltageMinMv) /
                        2000.0;
    double totalEnergy =
        (loramesh::EnergyModel::kDefaultCapacityMah / 1000.0) * voltageAvg * 3600.0;
    double energyConsumed = std::max(0.0, totalEnergy - remainingEnergy);
    uint16_t batteryMv = GetBatteryVoltageMv();
    uint32_t entries = m_routing ? static_cast<uint32_t>(m_routing->GetRouteCount()) : 0;

    NS_LOG_INFO("=== ROUTING TABLE Node " << GetNode()->GetId() << " (entries=" << entries << ")"
                                          << " Energy=" << energyConsumed << "J"
                                          << " Battery=" << batteryMv << "mV ===");

    if (!m_routing)
    {
        NS_LOG_INFO("  (routing module not initialized)");
        return;
    }

    if (entries == 0)
    {
        NS_LOG_INFO("  (empty)");
        return;
    }

    m_routing->PrintRoutingTable();
}

void
MeshDvApp::PurgeExpiredRoutes()
{
    if (m_routing)
    {
        m_routing->PurgeExpiredRoutes();
    }
    m_purgeEvt = Simulator::Schedule(Seconds(30), &MeshDvApp::PurgeExpiredRoutes, this);
}

void
MeshDvApp::SendDataToDestination(uint32_t dst, Ptr<Packet> payload)
{
    if (m_wireFormat == "v2")
    {
        (void)payload;
        SendDataPacketV2(dst);
        return;
    }

    TrackActiveDestination(dst);
    const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute(dst) : nullptr;
    if (!route)
    {
        uint32_t nextSeq = m_dataSeq + 1;
        NS_LOG_WARN("No route to dst=" << dst);
        const uint32_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
        const bool hasGwRoute = m_routing ? m_routing->HasRoute(m_collectorNodeId) : false;
        RouteStatus rs = ValidateRoute(dst); // REFACTORING: usar helper
        NS_LOG_INFO("DATA_NOROUTE detail: node="
                    << GetNode()->GetId() << " src=" << GetNode()->GetId() << " dst=" << dst
                    << " seq=" << nextSeq << " time=" << Simulator::Now().GetSeconds() << "s"
                    << " routesKnown=" << routeCount << " hasGwRoute=" << (hasGwRoute ? 1 : 0)
                    << " hasEntry=" << (rs.exists ? 1 : 0) << " expired=" << (rs.expired ? 1 : 0)
                    << " collectorNodeId=" << m_collectorNodeId);
        NS_LOG_UNCOND("FWDTRACE DATA_NOROUTE time="
                      << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                      << " src=" << GetNode()->GetId() << " dst=" << dst << " seq=" << nextSeq
                      << " reason=no_route");
        DumpFullTable("DATA_NOROUTE_SRC");
        DumpRoute(dst, "DATA_NOROUTE");
        m_dataNoRoute++;
        m_dropNoRoute++;
        return;
    }

    MeshMetricTag dataTag;
    dataTag.SetSrc(GetNode()->GetId());
    dataTag.SetDst(dst);
    dataTag.SetSeq(++m_dataSeq);
    dataTag.SetPrevHop(GetNode()->GetId());
    dataTag.SetTtl(m_initTtl);
    dataTag.SetHops(0);
    uint8_t dataSf = m_sf;
    if (m_useRouteSfForData)
    {
        dataSf = route->sf;
    }
    if (m_useEmpiricalSfForData)
    {
        dataSf = GetDataSfForNeighbor(route->nextHop);
    }
    dataSf = std::clamp<uint8_t>(dataSf, m_sfMin, m_sfMax);
    NS_LOG_DEBUG("DATA_SF_SELECT node="
                 << GetNode()->GetId() << " nextHop=" << route->nextHop
                 << " sf=" << unsigned(dataSf) << " routeSf=" << unsigned(route->sf)
                 << " empirical=" << (m_useEmpiricalSfForData ? 1 : 0));

    dataTag.SetSf(dataSf);
    dataTag.SetToaUs(ComputeLoRaToAUs(dataSf, m_bw, m_cr, payload->GetSize()));
    // REMOVED: SetRssiDbm - no se serializa
    dataTag.SetBatt_mV(GetBatteryVoltageMv());
    dataTag.SetScoreX100(ComputeScoreX100(dataTag));
    // FILTRADO UNICAST: Indicar quién debe recibir este paquete
    dataTag.SetExpectedNextHop(route->nextHop);

    if (g_metricsCollector)
    {
        g_metricsCollector->RecordDataGenerated(dataTag.GetSrc(), dataTag.GetDst(), dataTag.GetSeq());
    }

    payload->AddPacketTag(dataTag);

    Mac48Address routeMac;
    bool usingStaleMac = false;
    const bool hasUsableMac = ResolveUnicastNextHopLinkAddr(route->nextHop, &routeMac, &usingStaleMac);
    const bool macFound = (m_linkAddrTable.find(route->nextHop) != m_linkAddrTable.end());
    const bool macFresh = IsLinkAddrFresh(route->nextHop);
    NS_LOG_UNCOND("LINKADDR_CHECK node" << GetNode()->GetId() << " nextHop=" << route->nextHop
                                   << " linkAddrFound=" << (macFound ? "YES" : "NO")
                                   << " linkAddrFresh=" << (macFresh ? "YES" : "NO")
                                   << " staleAllowed="
                                   << (m_allowStaleLinkAddrForUnicastData ? "YES" : "NO")
                                   << " usingStale=" << (usingStaleMac ? "YES" : "NO"));

    Ptr<Node> n = GetNode();
    Ptr<NetDevice> dev = m_meshDevice;
    if (!dev)
    {
        for (uint32_t i = 0; i < n->GetNDevices(); ++i)
        {
            Ptr<NetDevice> d = n->GetDevice(i);
            if (d)
            {
                dev = d;
                break;
            }
        }
    }
    if (!dev)
    {
        return;
    }

    Address dstAddr = hasUsableMac ? Address(routeMac) : Address();
    if (!hasUsableMac || dstAddr.IsInvalid())
    {
        const uint32_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
        const bool hasGwRoute = m_routing ? m_routing->HasRoute(m_collectorNodeId) : false;
        RouteStatus rs = ValidateRoute(dst); // REFACTORING: usar helper
        NS_LOG_INFO("DATA_NOROUTE detail: node="
                    << GetNode()->GetId() << " src=" << dataTag.GetSrc() << " dst=" << dst
                    << " seq=" << dataTag.GetSeq() << " time=" << Simulator::Now().GetSeconds()
                    << "s"
                    << " routesKnown=" << routeCount << " hasGwRoute=" << (hasGwRoute ? 1 : 0)
                    << " hasEntry=" << (rs.exists ? 1 : 0) << " expired=" << (rs.expired ? 1 : 0)
                    << " collectorNodeId=" << m_collectorNodeId << " reason=no_link_addr_for_unicast");
        NS_LOG_UNCOND("FWDTRACE DATA_NOROUTE time="
                      << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId() << " src="
                      << dataTag.GetSrc() << " dst=" << dst << " seq=" << dataTag.GetSeq()
                      << " nextHop=" << route->nextHop << " reason=no_link_addr_for_unicast");
        DumpFullTable("DATA_NOROUTE_SRC");
        DumpRoute(dst, "DATA_NOROUTE");
        m_dataNoRoute++;
        m_dropNoRoute++;
        return;
    }

    const bool ok = dev->Send(payload, dstAddr, kProtoMesh);
    NS_LOG_UNCOND("DATA_TX SF" << unsigned(dataTag.GetSf()) << " node=" << GetNode()->GetId()
                               << " src=" << dataTag.GetSrc() << " dst=" << dst
                               << " seq=" << dataTag.GetSeq());

    NS_LOG_INFO("DATA TX: dst=" << dst << " via=" << route->nextHop << " seq=" << m_dataSeq
                                << " ok=" << ok);
}

Address
MeshDvApp::ResolveNextHopAddress(uint32_t nextHopId) const
{
    Mac48Address mac;
    bool stale = false;
    if (!ResolveUnicastNextHopLinkAddr(nextHopId, &mac, &stale))
    {
        return Address();
    }
    return Address(mac);
}

bool
MeshDvApp::IsLinkAddrFresh(uint32_t nextHopId) const
{
    auto it = m_linkAddrLastSeen.find(nextHopId);
    if (it == m_linkAddrLastSeen.end())
    {
        return false;
    }
    return (Simulator::Now() - it->second) <= m_linkAddrCacheWindow;
}

bool
MeshDvApp::ResolveUnicastNextHopLinkAddr(uint32_t nextHopId, Mac48Address* outMac, bool* outStale) const
{
    if (outStale)
    {
        *outStale = false;
    }
    if (!outMac)
    {
        return false;
    }

    auto it = m_linkAddrTable.find(nextHopId);
    if (it == m_linkAddrTable.end())
    {
        return false;
    }

    const bool fresh = IsLinkAddrFresh(nextHopId);
    if (!fresh && !m_allowStaleLinkAddrForUnicastData)
    {
        return false;
    }

    *outMac = it->second;
    if (outStale)
    {
        *outStale = !fresh;
    }
    return true;
}

bool
MeshDvApp::TryGetBestRecentSf(const NeighborLinkInfo& link, Time now, uint8_t* outSf) const
{
    if (!outSf)
    {
        return false;
    }

    const uint8_t minSf = std::clamp<uint8_t>(m_sfMin, static_cast<uint8_t>(7), static_cast<uint8_t>(12));
    const uint8_t maxSf = std::clamp<uint8_t>(m_sfMax, static_cast<uint8_t>(7), static_cast<uint8_t>(12));

    const std::string mode = ToLower(m_empiricalSfSelectMode);
    const bool robustMode = (mode != "min");
    const uint32_t minSamples = std::max<uint32_t>(1, m_empiricalSfMinSamples);

    for (uint8_t sf = minSf; sf <= maxSf; ++sf)
    {
        const size_t idx = static_cast<size_t>(sf - 7);
        const Time seenAt = link.lastSeenBySf[idx];
        if (seenAt.IsZero())
        {
            continue;
        }

        if ((now - seenAt) > m_neighborLinkTimeout)
        {
            continue;
        }

        if (!robustMode)
        {
            *outSf = sf;
            return true;
        }

        uint32_t recentSamples = 0;
        for (const Time& ts : link.rxTimesBySf[idx])
        {
            if ((now - ts) <= m_neighborLinkTimeout)
            {
                recentSamples++;
            }
        }
        if (recentSamples >= minSamples)
        {
            *outSf = sf;
            return true;
        }
    }

    return false;
}

// ========================================================================
// SF EMPÍRICO: Basado en historial de beacons exitosos (según paper)
// ========================================================================
void
MeshDvApp::UpdateNeighborLinkSf(uint32_t neighborId, uint8_t rxSf)
{
    auto& link = m_neighborLinks[neighborId];
    const Time now = Simulator::Now();

    uint8_t previousBestSf = 12;
    const bool hadPreviousBest = TryGetBestRecentSf(link, now, &previousBestSf);

    const uint8_t observedSf =
        std::clamp<uint8_t>(rxSf, static_cast<uint8_t>(7), static_cast<uint8_t>(12));
    const size_t sfIdx = static_cast<size_t>(observedSf - 7);
    link.lastSeenBySf[sfIdx] = now;
    link.rxTimesBySf[sfIdx].push_back(now);
    if (link.rxTimesBySf[sfIdx].size() > 64)
    {
        link.rxTimesBySf[sfIdx].pop_front();
    }

    const Time threshold = now - m_neighborLinkTimeout;
    for (auto& samples : link.rxTimesBySf)
    {
        while (!samples.empty() && samples.front() < threshold)
        {
            samples.pop_front();
        }
    }
    link.lastUpdate = now;
    link.lastRxSf = observedSf;

    uint8_t newBestSf = 12;
    const bool hasNewBest = TryGetBestRecentSf(link, now, &newBestSf);

    if (!hadPreviousBest && hasNewBest)
    {
        NS_LOG_DEBUG("Node " << GetNode()->GetId() << " UpdateNeighborLinkSf: neighbor="
                             << neighborId << " learned initial empirical SF="
                             << unsigned(newBestSf));
    }
    else if (hadPreviousBest && hasNewBest && newBestSf != previousBestSf)
    {
        NS_LOG_DEBUG("Node " << GetNode()->GetId() << " UpdateNeighborLinkSf: neighbor="
                             << neighborId << " empirical SF changed from "
                             << unsigned(previousBestSf) << " to " << unsigned(newBestSf));
    }
    else if (!hasNewBest)
    {
        NS_LOG_DEBUG("Node " << GetNode()->GetId() << " UpdateNeighborLinkSf: neighbor=" << neighborId
                             << " no robust SF yet (mode=" << m_empiricalSfSelectMode
                             << " minSamples=" << m_empiricalSfMinSamples << ")");
    }
}

uint8_t
MeshDvApp::GetDataSfForNeighbor(uint32_t nextHopId) const
{
    auto it = m_neighborLinks.find(nextHopId);
    if (it != m_neighborLinks.end())
    {
        const Time now = Simulator::Now();
        uint8_t bestRecentSf = 12;
        if (TryGetBestRecentSf(it->second, now, &bestRecentSf))
        {
            return bestRecentSf;
        }

        // Sin SF vigente en ventana: fallback robusto.
        NS_LOG_DEBUG("Node " << GetNode()->GetId() << " GetDataSfForNeighbor: neighbor="
                             << nextHopId << " no recent SF in window="
                             << m_neighborLinkTimeout.GetSeconds() << "s"
                             << ", lastRxSf=" << unsigned(it->second.lastRxSf)
                             << ", using SF12");
        return 12;
    }

    // Sin historial del vecino, usar SF conservador
    NS_LOG_DEBUG("Node " << GetNode()->GetId() << " GetDataSfForNeighbor: neighbor=" << nextHopId
                         << " no history, using SF12");
    return 12;
}

// REFACTORING: Método helper para validación de rutas
// Reemplaza código duplicado en 8+ lugares
RouteStatus
MeshDvApp::ValidateRoute(uint32_t dst) const
{
    RouteStatus status;

    if (!m_routing)
    {
        return status; // Sin routing, todo false
    }

    status.exists = m_routing->HasAnyRoute(dst);
    status.expired = m_routing->IsRouteExpired(dst);
    status.route = m_routing->GetRoute(dst);
    status.valid = status.exists && !status.expired && (status.route != nullptr);

    return status;
}

void
MeshDvApp::ScheduleDvCycle(uint8_t sf, EventId* evtSlot)
{
    if (!evtSlot)
    {
        NS_LOG_WARN("ScheduleDvCycle: null evtSlot pointer");
        return;
    }
    Time interval = GetBeaconInterval();
    if (interval.IsZero())
    {
        return;
    }
    // FIX A2: Capturar puntero (estable) en lugar de referencia (inestable)
    *evtSlot = Simulator::Schedule(interval, [this, sf, evtSlot]() {
        UpdateRouteTimeout();
        Time currentInterval = GetBeaconInterval();
        NS_LOG_UNCOND("BEACON_PHASE node" << GetNode()->GetId() << " sf=" << unsigned(sf)
                                          << " phase=" << GetBeaconPhaseLabel()
                                          << " interval=" << currentInterval.GetSeconds() << "s");
        const bool warmup = Simulator::Now() < m_beaconWarmupEnd;
        const bool routeChanged = m_routeChangePending;
        const bool forcePeriodic = true; // Always periodic for mesh network
        if (routeChanged || warmup || forcePeriodic)
        {
            if (m_routing)
            {
                m_routing->DebugDumpRoutingTable();
            }
            // B3: usar SF probabilístico si está habilitado
            uint8_t beaconSf = m_useProbabilisticSf ? SelectRandomSfProbabilistic() : sf;
            BuildAndSendDv(beaconSf);
            m_routeChangePending = false;
        }
        ScheduleDvCycle(sf, evtSlot); // Puntero es seguro aquí
    });
}

Time
MeshDvApp::GetBeaconInterval() const
{
    Time now = Simulator::Now();
    return (now < m_beaconWarmupEnd) ? m_beaconIntervalWarm : m_beaconIntervalStable;
}

void
MeshDvApp::SendInitialDv()
{
    // B3: usar SF probabilístico si está habilitado
    uint8_t beaconSf = m_useProbabilisticSf ? SelectRandomSfProbabilistic() : m_sfControl;
    BuildAndSendDv(beaconSf);
    ScheduleDvCycle(m_sfControl, &m_evtSf12); // FIX A2: pasar puntero
}

void
MeshDvApp::SchedulePeriodicDump()
{
    uint32_t nodeId = GetNode()->GetId();
    if (nodeId < 3 && m_routing)
    {
        NS_LOG_UNCOND("DV_DUMP_PERIODIC node" << nodeId << " t=" << Simulator::Now().GetSeconds());
        DumpFullTable("PERIODIC_30s");
    }
    m_periodicDumpEvt = Simulator::Schedule(Seconds(30), &MeshDvApp::SchedulePeriodicDump, this);
}

void
MeshDvApp::DumpRoute(uint32_t dst, const std::string& tag)
{
    if (!m_routing)
    {
        return;
    }
    const loramesh::RouteEntry* r = m_routing->GetRoute(dst);
    if (r)
    {
        NS_LOG_UNCOND("DV_SNAPSHOT tag=" << tag << " node" << GetNode()->GetId()
                                         << " t=" << Simulator::Now().GetSeconds() << " dst=" << dst
                                         << " nextHop=" << r->nextHop
                                         << " hops=" << unsigned(r->hops)
                                         << " score=" << r->scoreX100 << " seq=" << r->seqNum);
    }
    else
    {
        NS_LOG_UNCOND("DV_SNAPSHOT tag=" << tag << " node" << GetNode()->GetId()
                                         << " t=" << Simulator::Now().GetSeconds() << " dst=" << dst
                                         << " route=NULL");
    }
}

void
MeshDvApp::DumpFullTable(const std::string& tag) const
{
    if (!m_routing)
    {
        return;
    }
    uint32_t nodeId = GetNode() ? GetNode()->GetId() : 0;
    NS_LOG_UNCOND("DV_TABLE_FULL tag=" << tag << " node" << nodeId
                                       << " t=" << Simulator::Now().GetSeconds());
    m_routing->DebugDumpRoutingTable();

    const loramesh::RouteEntry* gw = m_routing->GetRoute(m_collectorNodeId);
    if (gw)
    {
        NS_LOG_UNCOND("DV_TABLE_FULL_GW node"
                      << nodeId << " dst=" << m_collectorNodeId << " nextHop=" << gw->nextHop
                      << " hops=" << unsigned(gw->hops) << " score=" << gw->scoreX100
                      << " seq=" << gw->seqNum
                      << " age=" << (Simulator::Now() - gw->lastUpdate).GetSeconds() << "s");
    }
    else
    {
        NS_LOG_UNCOND("DV_TABLE_FULL_GW node" << nodeId << " dst=" << m_collectorNodeId
                                              << " route=NULL");
    }
}

std::string
MeshDvApp::GetBeaconPhaseLabel() const
{
    std::ostringstream oss;
    const bool warm = (Simulator::Now() < m_beaconWarmupEnd);
    const double intervalSec = warm ? m_beaconIntervalWarm.GetSeconds() : m_beaconIntervalStable.GetSeconds();
    oss << (warm ? "warmup(" : "stable(") << intervalSec << "s)";
    return oss.str();
}

void
MeshDvApp::RecordBeaconScheduled(uint32_t seq)
{
    m_beaconScheduled++;
    m_beaconScheduledAtBySeq[seq] = Simulator::Now();
}

void
MeshDvApp::RecordBeaconTxSent(uint32_t seq)
{
    m_beaconTxSent++;
    auto it = m_beaconScheduledAtBySeq.find(seq);
    if (it != m_beaconScheduledAtBySeq.end())
    {
        const double delaySec = (Simulator::Now() - it->second).GetSeconds();
        if (delaySec >= 0.0)
        {
            m_beaconDelaySumSec += delaySec;
            m_beaconDelaySamplesSec.push_back(delaySec);
            SAFE_RECORD(RecordBeaconDelay, delaySec);
        }
        m_beaconScheduledAtBySeq.erase(it);
    }
}

double
MeshDvApp::ComputeBeaconDelayP95() const
{
    return Percentile95Double(m_beaconDelaySamplesSec);
}

// Encola o envía directamente según si CSMA está habilitado.
void
MeshDvApp::SendWithCSMA(Ptr<Packet> packet,
                        const MeshMetricTag& tag,
                        Address dstAddr,
                        bool logTxMetrics)
{
    // Incluso con CSMA deshabilitado usamos la misma cola TX para evitar descartar
    // paquetes por duty-cycle. En ese modo, ProcessTxQueue omite CAD/backoff y
    // mantiene solo la lógica de cola + defer por duty.

    TxQueueEntry entry;
    entry.packet = packet;
    entry.tag = tag;
    entry.retries = 0;
    entry.dstAddr = dstAddr;
    entry.logTxMetrics = logTxMetrics;
    entry.pendingReason = m_csmaEnabled ? "queued_wait_cad" : "queued_wait_tx";
    entry.deferCount = 0;
    entry.lastStateChange = Simulator::Now();

    if (entry.tag.GetDst() != 0xFFFF)
    {
        // DATOS: Insertar después de beacons pendientes pero antes de otros datos
        // Esto evita starvation cuando beacons constantemente llegan
        if (m_prioritizeBeacons)
        {
            // Buscar la primera posición después de todos los beacons en cola
            auto it = m_txQueue.begin();
            while (it != m_txQueue.end() && it->tag.GetDst() == 0xFFFF)
            {
                ++it;
            }
            m_txQueue.insert(it, entry); // Insertar después de beacons existentes
        }
        else
        {
            m_txQueue.push_front(entry); // Priorizar datos frente a DV.
        }
    }
    else
    {
        // BEACONS: Siempre al frente cuando prioritizeBeacons=true
        if (m_prioritizeBeacons)
        {
            m_txQueue.push_front(entry);
        }
        else
        {
            m_txQueue.push_back(entry);
        }
    }
    NS_LOG_INFO("CSMA: Paquete en cola (size=" << m_txQueue.size() << ")");
    NS_LOG_UNCOND("FWDTRACE QUEUE_ENQUEUE time="
                  << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                  << " src=" << entry.tag.GetSrc() << " dst=" << entry.tag.GetDst()
                  << " seq=" << entry.tag.GetSeq() << " sf=" << unsigned(entry.tag.GetSf())
                  << " reason=" << entry.pendingReason << " queueSize=" << m_txQueue.size());

    ProcessTxQueue();
}

// Ejecuta detección de canal y transmisión efectiva de la cola CSMA.
void
MeshDvApp::ProcessTxQueue()
{
    if (m_txQueue.empty())
    {
        NS_LOG_DEBUG("CSMA: Cola vacía");
        return;
    }

    auto setPendingReason = [this](TxQueueEntry& e, const char* reason) {
        if (e.pendingReason != reason)
        {
            e.pendingReason = reason;
            e.lastStateChange = Simulator::Now();
            NS_LOG_UNCOND("FWDTRACE QUEUE_STATE time="
                          << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                          << " src=" << e.tag.GetSrc() << " dst=" << e.tag.GetDst()
                          << " seq=" << e.tag.GetSeq() << " sf=" << unsigned(e.tag.GetSf())
                          << " reason=" << reason << " deferCount=" << e.deferCount
                          << " queueSize=" << m_txQueue.size());
        }
    };

    if (m_txBusy)
    {
        if (m_txQueue.size() > 1)
        {
            auto it = m_txQueue.begin();
            ++it; // el front está en aire; marcamos el primer paquete realmente en espera
            setPendingReason(*it, "phy_tx_busy_queue");
            it->deferCount++;
        }
        NS_LOG_DEBUG("CSMA: PHY ocupado, esperar");
        return;
    }

    std::deque<TxQueueEntry>::iterator entryIt;

    // FIX STARVATION: Si hay datos en cola, procesarlos después de un beacon máximo
    // Buscar primer beacon y primer dato en la cola
    std::deque<TxQueueEntry>::iterator firstBeacon = m_txQueue.end();
    std::deque<TxQueueEntry>::iterator firstData = m_txQueue.end();
    for (auto it = m_txQueue.begin(); it != m_txQueue.end(); ++it)
    {
        if (it->tag.GetDst() == 0xFFFF && firstBeacon == m_txQueue.end())
        {
            firstBeacon = it;
        }
        else if (it->tag.GetDst() != 0xFFFF && firstData == m_txQueue.end())
        {
            firstData = it;
        }
        if (firstBeacon != m_txQueue.end() && firstData != m_txQueue.end())
        {
            break;
        }
    }

    // Alternar: procesar beacon primero, pero si hay datos pendientes, procesarlos
    // después de procesar hasta 2 beacons consecutivos
    static std::map<uint32_t, uint32_t> s_consecutiveBeacons;
    uint32_t myId = GetNode()->GetId();

    if (firstData != m_txQueue.end() && s_consecutiveBeacons[myId] >= 1)
    {
        // Hay datos y ya procesamos suficientes beacons, procesar dato
        entryIt = firstData;
        s_consecutiveBeacons[myId] = 0;
    }
    else if (firstBeacon != m_txQueue.end())
    {
        // Procesar beacon
        entryIt = firstBeacon;
        s_consecutiveBeacons[myId]++;
    }
    else
    {
        // Solo hay datos (o cola vacía después de búsqueda)
        entryIt = m_txQueue.begin();
        s_consecutiveBeacons[myId] = 0;
    }

    // Mover el elemento seleccionado al frente para que pop_front() funcione
    if (entryIt != m_txQueue.begin())
    {
        TxQueueEntry entryToMove = *entryIt;
        m_txQueue.erase(entryIt);
        m_txQueue.push_front(entryToMove);
    }

    TxQueueEntry& entry = m_txQueue.front();
    if (m_enableControlGuard && entry.tag.GetDst() != 0xFFFF)
    {
        const Time lastDvActivity =
            (m_lastDvTxTime > m_lastDvRxTime) ? m_lastDvTxTime : m_lastDvRxTime;
        if (lastDvActivity > Seconds(0))
        {
            const Time since = Simulator::Now() - lastDvActivity;
            const Time guard = Seconds(m_controlGuardSec);
            if (since < guard)
            {
                Time wait = guard - since;
                if (m_rng)
                {
                    const double jitterMax = std::min(0.1, wait.GetSeconds() * 0.1);
                    if (jitterMax > 0.0)
                    {
                        wait += Seconds(m_rng->GetValue(0.0, jitterMax));
                    }
                }
                NS_LOG_INFO("CSMA: Control guard active, delaying data TX by " << wait.GetSeconds()
                                                                               << "s");
                setPendingReason(entry, "control_guard_wait");
                entry.deferCount++;
                if (!m_backoffEvt.IsPending())
                {
                    m_backoffEvt = Simulator::Schedule(wait, &MeshDvApp::ProcessTxQueue, this);
                }
                return;
            }
        }
    }
    if (m_mac)
    {
        const double toaSeconds = entry.tag.GetToaUs() / 1e6;
        if (entry.tag.GetDst() == 0xFFFF)
        {
            m_mac->UpdateTypicalCtrlToaSeconds(toaSeconds);
        }
        else
        {
            m_mac->UpdateTypicalDataToaSeconds(toaSeconds);
        }
    }

    bool channelBusy = false;
    if (m_csmaEnabled)
    {
        uint8_t difsCount = m_mac ? m_mac->GetDifsCadCount() : m_difsCadCount;
        NS_LOG_INFO("CSMA: Iniciando DIFS con " << unsigned(difsCount) << " CADs");
        channelBusy = m_mac ? m_mac->PerformChannelAssessment() : false;
    }

    if (m_csmaEnabled && channelBusy)
    {
        NS_LOG_INFO("CSMA: Canal ocupado detectado, aplicando backoff");
        m_backoffCount++;
        m_cadBusyEvents++;
        uint32_t backoffSlots =
            m_mac ? m_mac->GetBackoffSlots() : m_rng->GetInteger(0, (1 << m_backoffWindow) - 1);
        const bool isControl = (entry.tag.GetDst() == 0xFFFF);
        const double factor = isControl ? m_controlBackoffFactor : m_dataBackoffFactor;
        const uint32_t scaledSlots = (factor <= 0.0)
                                         ? 0u
                                         : std::max<uint32_t>(1,
                                                              static_cast<uint32_t>(std::ceil(
                                                                  backoffSlots * factor)));
        Time cadDuration = m_mac ? m_mac->GetCadDuration() : m_cadDuration;
        Time backoffTime = (scaledSlots == 0u) ? MicroSeconds(1) : (cadDuration * scaledSlots);

        NS_LOG_INFO("CSMA: Backoff " << backoffSlots << " slots (" << backoffTime.GetMilliSeconds()
                                     << "ms)"
                                     << " factor=" << factor << " scaledSlots=" << scaledSlots
                                     << " type=" << (isControl ? "ctrl" : "data"));
        setPendingReason(entry, "cad_busy_backoff");
        entry.deferCount++;

        m_backoffEvt = Simulator::Schedule(backoffTime, &MeshDvApp::ProcessTxQueue, this);
    }
    else
    {
        NS_LOG_INFO("CSMA: Canal libre, transmitiendo");

        Ptr<Node> n = GetNode();
        Ptr<NetDevice> dev = m_meshDevice;
        if (!dev)
        {
            for (uint32_t i = 0; i < n->GetNDevices(); ++i)
            {
                Ptr<NetDevice> d = n->GetDevice(i);
                if (d)
                {
                    dev = d;
                    break;
                }
            }
        }
        if (!dev)
        {
            // Sin dispositivo no podemos transmitir; liberar el bloqueo para no colgar la cola.
            NS_LOG_ERROR("CSMA: NetDevice no disponible, descartando paquete en cabeza de cola");
            m_dropOther++;
            m_txQueue.pop_front();
            ProcessTxQueue();
            return;
        }

        if (m_mac && !m_mac->CanTransmitNow(entry.tag.GetToaUs() / 1e6))
        {
            NS_LOG_WARN("CSMA: Duty check failed at dequeue, deferring packet");
            setPendingReason(entry, "duty_wait_queue");
            entry.deferCount++;
            m_dutyBlockedEvents++;
            if (entry.tag.GetDst() == 0xFFFF)
            {
                m_beaconBlockedByDuty++;
            }
            m_totalWaitTimeDueToDutySec += 1.0;
            if (!m_backoffEvt.IsPending())
            {
                m_backoffEvt = Simulator::Schedule(Seconds(1), &MeshDvApp::ProcessTxQueue, this);
            }
            return;
        }

        setPendingReason(entry, "tx_attempt_air");
        m_txBusy = true;

        Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev =
            DynamicCast<ns3::lorawan::MeshLoraNetDevice>(dev);
        if (meshDev)
        {
            if (m_mac)
            {
                meshDev->SetMac(m_mac);
            }
            if (m_energyModel)
            {
                meshDev->SetEnergyModel(m_energyModel);
            }
        }

        Ptr<Packet> p = entry.packet->Copy();
        p->RemoveAllPacketTags();
        p->AddPacketTag(entry.tag);

        if (entry.tag.GetDst() == 0xFFFF && m_wireFormat == "v2")
        {
            if (!entry.beaconRpCounterAssigned)
            {
                entry.beaconRpCounter = static_cast<uint8_t>(m_beaconRpCounterTx & 0x3F);
                entry.beaconRpCounterAssigned = true;
            }
            BeaconWireHeaderV2 beaconHdr;
            Ptr<Packet> beaconPayload;
            if (ParseBeaconWirePacketV2(p, &beaconHdr, &beaconPayload))
            {
                beaconHdr.SetFlagsTtl(
                    PackFlagsTtlV2(WirePacketTypeV2::BEACON, entry.beaconRpCounter));
                Ptr<Packet> rebuilt = beaconPayload->Copy();
                rebuilt->AddHeader(beaconHdr);
                p = rebuilt;
            }
            else
            {
                NS_LOG_ERROR("CSMA: invalid beacon v2 packet while assigning rp_counter; dropping");
                m_dropOther++;
                m_txQueue.pop_front();
                ProcessTxQueue();
                return;
            }
        }

        Address dst = (entry.dstAddr == Address()) ? dev->GetBroadcast() : entry.dstAddr;
        if (entry.tag.GetDst() == 0xFFFF)
        {
            m_lastDvTxTime = Simulator::Now();
        }
        const bool ok = dev->Send(p, dst, kProtoMesh);
        // DEBUG: Log para ver qué paquetes salen de la cola
        NS_LOG_UNCOND("CSMA_TX_QUEUE_OUT node=" << GetNode()->GetId()
                                                << " tagDst=" << entry.tag.GetDst()
                                                << " tagSrc=" << entry.tag.GetSrc() << " ok=" << ok
                                                << " sf=" << unsigned(entry.tag.GetSf()));
        if (m_mac)
        {
            m_mac->NotifyTxResult(ok);
        }

        NS_LOG_INFO("CSMA: TX ok=" << ok);
        if (ok)
        {
            OnPacketTransmitted(entry.tag.GetToaUs());
            if (entry.tag.GetDst() == 0xFFFF)
            {
                if (m_wireFormat == "v2" && entry.beaconRpCounterAssigned)
                {
                    m_beaconRpCounterTx = static_cast<uint8_t>((entry.beaconRpCounter + 1) & 0x3F);
                    NS_LOG_UNCOND("DVTRACE_TX_V2_AIR time=" << Simulator::Now().GetSeconds()
                                                            << " node=" << GetNode()->GetId()
                                                            << " seq=" << entry.tag.GetSeq()
                                                            << " rp_counter="
                                                            << unsigned(entry.beaconRpCounter)
                                                            << " bytes=" << p->GetSize());
                }
                RecordBeaconTxSent(entry.tag.GetSeq());
            }
            if (entry.logTxMetrics)
            {
                const double energyJ = GetRemainingEnergyJ();
                const double energyFrac = GetEnergyFraction();
                LogTxEvent(entry.tag.GetSeq(),
                           entry.tag.GetDst(),
                           entry.tag.GetTtl(),
                           entry.tag.GetHops(),
                           entry.tag.GetBatt_mV(),
                           entry.tag.GetScoreX100(),
                           entry.tag.GetSf(),
                           energyJ,
                           energyFrac,
                           true);
            }
            // Registrar overhead (beacon vs data)
            if (g_metricsCollector)
            {
                std::string kind = (entry.tag.GetDst() == 0xFFFF) ? "beacon" : "data";
                g_metricsCollector->RecordOverhead(GetNode()->GetId(),
                                                   kind,
                                                   entry.packet->GetSize(),
                                                   entry.tag.GetSrc(),
                                                   entry.tag.GetDst(),
                                                   entry.tag.GetSeq(),
                                                   entry.tag.GetHops(),
                                                   entry.tag.GetSf());
            }
        }
        else
        {
            m_dropOther++;
        }

        uint32_t toaUs = entry.tag.GetToaUs();
        Time txDuration = MicroSeconds(toaUs);

        Simulator::Schedule(txDuration, [this]() {
            m_txBusy = false;
            m_txQueue.pop_front();
            ProcessTxQueue();
        });
    }
}

// Registra TX reales para duty-cycle
void
MeshDvApp::OnPacketTransmitted(uint32_t toaUs)
{
    NS_LOG_INFO("OnPacketTransmitted(): node=" << GetNode()->GetId() << " toaUs=" << toaUs);
    m_txCount++;
    double duty = m_mac ? m_mac->GetDutyCycleUsed() : 0.0;
    NS_LOG_INFO("Duty cycle actualizado=" << (duty * 100.0) << "%");
}

// Maneja el temporizador de backoff cuando expira.
void
MeshDvApp::OnBackoffTimer()
{
    NS_LOG_DEBUG("CSMA: Backoff timer expirado");
    ProcessTxQueue();
}

int16_t
MeshDvApp::GetRealRSSI() const
{
    Ptr<Node> node = GetNode();

    // Obtener el NetDevice
    Ptr<NetDevice> dev = node->GetDevice(0);

    // Usar namespace completo: ns3::lorawan::MeshLoraNetDevice
    Ptr<ns3::lorawan::MeshLoraNetDevice> meshDev =
        DynamicCast<ns3::lorawan::MeshLoraNetDevice>(dev);

    if (!meshDev)
    {
        NS_LOG_WARN("No MeshLoraNetDevice found, usando RSSI por defecto");
        return -95;
    }

    // Obtener el RSSI del último paquete recibido
    double rssi = meshDev->GetLastRxRssi();

    // Convertir a int16_t
    int16_t rssiInt = static_cast<int16_t>(std::round(rssi));

    NS_LOG_DEBUG("RSSI from last RX: " << rssiInt << " dBm");

    return rssiInt;
}

uint16_t
MeshDvApp::GetBatteryVoltageMv() const
{
    const double frac = GetEnergyFraction();
    if (frac >= 0.0)
    {
        return EnergyFractionToBatteryMv(frac);
    }
    return static_cast<uint16_t>(kBatteryMvMin);
}

double
MeshDvApp::GetRemainingEnergyJ() const
{
    if (m_energyModel)
    {
        return m_energyModel->GetRemainingEnergy(GetNode()->GetId());
    }
    return -1.0;
}

double
MeshDvApp::GetEnergyFraction() const
{
    const double remainingJ = GetRemainingEnergyJ();
    if (remainingJ >= 0.0 && m_batteryFullCapacityJ > 0.0)
    {
        return std::clamp(remainingJ / m_batteryFullCapacityJ, 0.0, 1.0);
    }
    if (m_energyModel)
    {
        return m_energyModel->GetEnergyFraction(GetNode()->GetId());
    }
    return -1.0;
}

void
MeshDvApp::HandleRouteChange(const loramesh::RouteEntry& entry, const std::string& action)
{
    bool significant = false;
    auto it = m_lastRouteSnapshot.find(entry.destination);
    if (it == m_lastRouteSnapshot.end())
    {
        significant = true;
    }
    else
    {
        const loramesh::RouteEntry& prev = it->second;
        const uint8_t hopDelta =
            (entry.hops > prev.hops) ? (entry.hops - prev.hops) : (prev.hops - entry.hops);
        const uint16_t scoreDelta = (entry.scoreX100 > prev.scoreX100)
                                        ? static_cast<uint16_t>(entry.scoreX100 - prev.scoreX100)
                                        : static_cast<uint16_t>(prev.scoreX100 - entry.scoreX100);
        if (hopDelta > 1 || scoreDelta > 10)
        {
            significant = true;
        }
    }
    m_lastRouteSnapshot[entry.destination] = entry;
    if (significant)
    {
        m_routeChangePending = true;
    }

    NS_LOG_INFO("Route " << action << ": dst=" << entry.destination << " via=" << entry.nextHop
                         << " score=" << entry.scoreX100 << " seq=" << entry.seqNum);
    if (g_metricsCollector && action != "NONE")
    {
        g_metricsCollector->RecordRoute(GetNode()->GetId(),
                                        entry.destination,
                                        entry.nextHop,
                                        entry.hops,
                                        entry.scoreX100,
                                        entry.seqNum,
                                        action);

        // =====================================================================
        // THESIS METRIC T50: Track connectivity to sink node
        // Record when a node gains/loses route to the sink (collector node)
        // =====================================================================
        if (entry.destination == m_collectorNodeId)
        {
            const bool invalidAction =
                (action == "POISON" || action == "EXPIRE" || action == "PURGE");
            bool hasRoute = (!invalidAction && entry.nextHop != 0xFFFFFFFF && entry.scoreX100 > 0);
            g_metricsCollector->RecordConnectivity(GetNode()->GetId(), m_collectorNodeId, hasRoute);
            if (!hasRoute)
            {
                NS_LOG_WARN("T50_TRACKER: Node "
                            << GetNode()->GetId() << " LOST route to sink " << m_collectorNodeId
                            << " at t=" << Simulator::Now().GetSeconds() << "s");
            }
        }
    }
}

void
MeshDvApp::HandleFloodRequest(const loramesh::DvMessage& msg)
{
    if (msg.entries.empty())
    {
        return;
    }

    NS_LOG_DEBUG("FloodDvUpdate requested for node=" << GetNode()->GetId()
                                                     << " seq=" << msg.sequence
                                                     << " entries=" << msg.entries.size());

    // Serializar entries para el payload
    std::vector<MeshMetricTag::RoutePayloadEntry> payloadEntries;
    payloadEntries.reserve(msg.entries.size());
    for (const auto& entry : msg.entries)
    {
        MeshMetricTag::RoutePayloadEntry plEntry;
        plEntry.dst = static_cast<uint16_t>(entry.destination);
        plEntry.hops = entry.hops;
        plEntry.sf = entry.sf;
        plEntry.score = entry.scoreX100;
        // REMOVED: plEntry.rssi_dBm
        plEntry.batt_mV = entry.batt_mV;
        payloadEntries.push_back(plEntry);
    }

    size_t len = payloadEntries.size() * MeshMetricTag::GetRoutePayloadEntrySize();
    uint32_t payloadSizeBytes = std::max<uint32_t>(len, 13);
    std::vector<uint8_t> buffer(payloadSizeBytes, 0);
    MeshMetricTag::SerializeRoutePayload(payloadEntries, buffer.data(), buffer.size());

    Ptr<Packet> p = Create<Packet>(buffer.data(), buffer.size());

    // Configurar Tag
    MeshMetricTag tag;
    tag.SetSrc(msg.origin); // Debería ser mi ID
    tag.SetDst(0xFFFF);     // Broadcast
    tag.SetSeq(msg.sequence);
    tag.SetPrevHop(GetNode()->GetId());
    tag.SetTtl(m_initTtl); // TTL max para propagación
    tag.SetHops(0);
    tag.SetSf(m_sfControl); // Usar SF robusto para mensajes críticos

    // Calcular ToA
    const uint32_t toaUs = ComputeLoRaToAUs(m_sfControl, m_bw, m_cr, payloadSizeBytes);
    tag.SetToaUs(toaUs);

    if (m_mac && !m_mac->CanTransmitNow(toaUs / 1e6))
    {
        NS_LOG_UNCOND("FWDTRACE duty_defer time="
                      << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                      << " src=" << tag.GetSrc() << " dst=" << tag.GetDst() << " seq="
                      << tag.GetSeq() << " dutyUsed=" << m_mac->GetDutyCycleUsed()
                      << " dutyLimit=" << m_mac->GetDutyCycleLimit()
                      << " reason=duty_wait_queue");
    }

    // REMOVED: tag.SetRssiDbm - no se serializa
    tag.SetBatt_mV(GetBatteryVoltageMv());
    tag.SetScoreX100(0); // Score del paquete en sí irrelevante, importa el payload

    p->AddPacketTag(tag);

    NS_LOG_UNCOND("POISON_TX node=" << GetNode()->GetId() << " entries=" << msg.entries.size()
                                    << " seq=" << msg.sequence);

    SendWithCSMA(p, tag, Address());
}

loramesh::NeighborLinkInfo
MeshDvApp::BuildNeighborLinkInfo(const MeshMetricTag& tag,
                                 uint32_t toaUs,
                                 Mac48Address fromMac) const
{
    loramesh::NeighborLinkInfo link;
    link.neighbor = tag.GetSrc();
    link.sequence = tag.GetSeq();
    link.hops = std::min<uint8_t>(static_cast<uint8_t>(tag.GetHops() + 1), m_initTtl);
    link.sf = tag.GetSf();
    link.toaUs = toaUs;
    // REMOVED: link.rssiDbm - receptor obtiene de PHY
    link.batt_mV = tag.GetBatt_mV();
    loramesh::LinkStats stats;
    stats.toaUs = toaUs;
    stats.hops = link.hops;
    stats.sf = link.sf;
    // REMOVED: SNR/RSSI not used in metric - link quality embedded in SF
    stats.snrDb = 0.0;
    stats.batteryMv = link.batt_mV;
    stats.energyFraction = BatteryMvToEnergyFraction(link.batt_mV);
    const double cost = m_compositeMetric.ComputeLinkCost(GetNode()->GetId(), link.neighbor, stats);
    link.scoreX100 = static_cast<uint16_t>(std::round(std::clamp(1.0 - cost, 0.0, 1.0) * 100.0));
    link.mac = fromMac;
    return link;
}

std::vector<loramesh::DvEntry>
MeshDvApp::DecodeDvEntries(Ptr<const Packet> p,
                           const MeshMetricTag& tag,
                           uint32_t toaUsNeighbor) const
{
    std::vector<loramesh::DvEntry> entries;
    const size_t payloadLen = p->GetSize();
    const size_t entrySize = MeshMetricTag::GetRoutePayloadEntrySize();

    if (entrySize == 0 || payloadLen < entrySize)
    {
        if (payloadLen > 0)
        {
            NS_LOG_DEBUG("Beacon payload demasiado pequeño (" << payloadLen
                                                              << " bytes), sin rutas que procesar");
            NS_LOG_UNCOND("DVTRACE_RX_PARSE time=" << Simulator::Now().GetSeconds() << " node="
                                                   << GetNode()->GetId() << " src=" << tag.GetSrc()
                                                   << " seq=" << tag.GetSeq() << " total=0"
                                                   << " accepted=0"
                                                   << " drop_small=1"
                                                   << " payloadBytes=" << payloadLen);
        }
        return entries;
    }

    std::vector<MeshMetricTag::RoutePayloadEntry> receivedRoutes;
    std::vector<uint8_t> buffer(payloadLen);
    p->CopyData(buffer.data(), payloadLen);
    MeshMetricTag::DeserializeRoutePayload(buffer.data(), buffer.size(), receivedRoutes);

    const uint32_t total = receivedRoutes.size();
    uint32_t dropSelf = 0;
    uint32_t dropTtl = 0;
    uint32_t accepted = 0;
    for (const auto& route : receivedRoutes)
    {
        if (route.dst == GetNode()->GetId())
        {
            dropSelf++;
            continue; // No aprender ruta a sí mismo
        }

        uint8_t totalHops = route.hops; // RoutingDv::UpdateFromDvMsg adds +1
        if (totalHops >= m_initTtl)
        {
            NS_LOG_DEBUG("Descartando ruta por hops excesivos (" << unsigned(totalHops)
                                                                 << " > TTL)");
            dropTtl++;
            continue;
        }

        loramesh::DvEntry entry;
        entry.destination = route.dst;
        entry.hops = totalHops;
        entry.sf = route.sf; // Use per-route SF from DV payload, not beacon header SF
        entry.scoreX100 = route.score;
        entry.toaUs = toaUsNeighbor;
        // REMOVED: entry.rssiDbm
        entry.batt_mV = route.batt_mV;
        entries.push_back(entry);
        accepted++;
    }
    NS_LOG_UNCOND("DVTRACE_RX_PARSE time="
                  << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                  << " src=" << tag.GetSrc() << " seq=" << tag.GetSeq() << " total=" << total
                  << " accepted=" << accepted << " drop_self=" << dropSelf
                  << " drop_ttl=" << dropTtl << " payloadBytes=" << payloadLen);
    return entries;
}

void
MeshDvApp::ProcessDvPayload(Ptr<const Packet> p,
                            const MeshMetricTag& tag,
                            const Mac48Address& fromMac,
                            uint32_t toaUsNeighbor)
{
    if (!m_routing)
    {
        return;
    }

    loramesh::NeighborLinkInfo link = BuildNeighborLinkInfo(tag, toaUsNeighbor, fromMac);
    loramesh::DvMessage msg;
    msg.origin = tag.GetSrc();
    msg.sequence = tag.GetSeq();
    msg.entries = DecodeDvEntries(p, tag, toaUsNeighbor);
    m_routing->UpdateFromDvMsg(msg, link);
}

bool
MeshDvApp::ParseDataWirePacketV2(Ptr<const Packet> p,
                                 DataWireHeaderV2* outHdr,
                                 Ptr<Packet>* outPayload) const
{
    if (!outHdr || !p || p->GetSize() < DataWireHeaderV2::kSerializedSize)
    {
        return false;
    }

    Ptr<Packet> copy = p->Copy();
    DataWireHeaderV2 hdr;
    const uint32_t removed = copy->RemoveHeader(hdr);
    if (removed != DataWireHeaderV2::kSerializedSize)
    {
        return false;
    }
    if (GetPacketTypeV2(hdr.GetFlagsTtl()) != WirePacketTypeV2::DATA)
    {
        return false;
    }
    *outHdr = hdr;
    if (outPayload)
    {
        *outPayload = copy;
    }
    return true;
}

bool
MeshDvApp::ParseBeaconWirePacketV2(Ptr<const Packet> p,
                                   BeaconWireHeaderV2* outHdr,
                                   Ptr<Packet>* outPayload) const
{
    if (!outHdr || !p || p->GetSize() < BeaconWireHeaderV2::kSerializedSize)
    {
        return false;
    }

    Ptr<Packet> copy = p->Copy();
    BeaconWireHeaderV2 hdr;
    const uint32_t removed = copy->RemoveHeader(hdr);
    if (removed != BeaconWireHeaderV2::kSerializedSize)
    {
        return false;
    }
    if (GetPacketTypeV2(hdr.GetFlagsTtl()) != WirePacketTypeV2::BEACON)
    {
        return false;
    }
    *outHdr = hdr;
    if (outPayload)
    {
        *outPayload = copy;
    }
    return true;
}

uint32_t
MeshDvApp::ResolveBeaconSequenceFromRpCounter(uint32_t origin, uint8_t rpCounter)
{
    const uint8_t counter6 = static_cast<uint8_t>(rpCounter & 0x3F);

    auto itLast = m_lastBeaconRpCounterRx.find(origin);
    auto itExt = m_beaconRpExtendedSeqRx.find(origin);
    if (itLast == m_lastBeaconRpCounterRx.end() || itExt == m_beaconRpExtendedSeqRx.end())
    {
        m_lastBeaconRpCounterRx[origin] = counter6;
        m_beaconRpExtendedSeqRx[origin] = counter6;
        return counter6;
    }

    const uint8_t last = itLast->second;
    const uint8_t forwardDelta = static_cast<uint8_t>((counter6 - last) & 0x3F);
    uint32_t extended = itExt->second;

    // Robust wrap handling for duty-limited environments:
    // any non-zero modulo delta is accepted as forward progress.
    if (forwardDelta == 0)
    {
        return extended; // duplicate
    }
    if (forwardDelta > 32)
    {
        // Telemetry-only: large gap likely means many missed beacons (or rare reorder).
        m_rpGapLargeEvents++;
    }

    extended += forwardDelta;
    m_lastBeaconRpCounterRx[origin] = counter6;
    m_beaconRpExtendedSeqRx[origin] = extended;

    return m_beaconRpExtendedSeqRx[origin];
}

std::vector<loramesh::DvEntry>
MeshDvApp::DecodeDvEntriesV2(Ptr<const Packet> p,
                             uint32_t payloadOffset,
                             uint32_t toaUsNeighbor,
                             uint8_t rxSf) const
{
    (void)toaUsNeighbor;
    (void)rxSf;
    std::vector<loramesh::DvEntry> entries;
    if (!p)
    {
        return entries;
    }

    const uint32_t totalSize = p->GetSize();
    if (payloadOffset >= totalSize)
    {
        return entries;
    }

    const uint32_t payloadLen = totalSize - payloadOffset;
    if (payloadLen < BeaconWireHeaderV2::kEntrySize)
    {
        return entries;
    }

    std::vector<uint8_t> buf(totalSize);
    p->CopyData(buf.data(), totalSize);

    std::vector<DvEntryWireV2> received;
    BeaconWireHeaderV2::DeserializeDvEntries(buf.data() + payloadOffset, payloadLen, received);
    entries.reserve(received.size());
    for (const auto& route : received)
    {
        if (route.destination == GetNode()->GetId())
        {
            continue;
        }
        loramesh::DvEntry e;
        e.destination = route.destination;
        // v2 on-air beacon entry is score-only: (destination, score).
        // Non-announced fields stay neutral and are not used for candidate construction.
        e.hops = 0;
        e.sf = 0;
        e.scoreX100 = route.score;
        e.toaUs = 0;
        e.batt_mV = 0;
        entries.push_back(e);
    }
    return entries;
}

bool
MeshDvApp::L2ReceiveV2(Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address& from)
{
    (void)dev;
    if (proto != kProtoMesh || !p)
    {
        return false;
    }

    const uint32_t myId = GetNode()->GetId();
    const double energyJ = GetRemainingEnergyJ();
    const double energyFrac = GetEnergyFraction();
    uint8_t rxSf = m_sfControl;
    lorawan::LoraTag loraTag;
    if (p->PeekPacketTag(loraTag))
    {
        const uint8_t sf = loraTag.GetSpreadingFactor();
        if (sf >= m_sfMin && sf <= m_sfMax)
        {
            rxSf = sf;
        }
    }

    BeaconWireHeaderV2 beaconHdr;
    Ptr<Packet> payload;
    if (ParseBeaconWirePacketV2(p, &beaconHdr, &payload))
    {
        const uint16_t src = beaconHdr.GetSrc();
        const uint8_t rpCounter = GetTtlFromFlagsV2(beaconHdr.GetFlagsTtl());
        if (src == myId)
        {
            return true;
        }

        if (Mac48Address::IsMatchingType(from))
        {
            const Mac48Address fromMac = Mac48Address::ConvertFrom(from);
            const auto itPrev = m_linkAddrTable.find(src);
            if (itPrev == m_linkAddrTable.end() || itPrev->second != fromMac)
            {
                NS_LOG_UNCOND("LINKADDR_LEARN node=" << myId << " peer=" << src
                                                     << " linkAddr learned=" << fromMac);
            }
            m_linkAddrTable[src] = fromMac;
            m_linkAddrLastSeen[src] = Simulator::Now();
        }
        else
        {
            NS_LOG_WARN("L2ReceiveV2 beacon: node=" << myId << " src=" << src
                                                    << " non-Mac48 'from' address, cannot learn linkAddr wrapper");
        }
        UpdateNeighborLinkSf(src, rxSf);

        const uint32_t toaUsNeighbor = ComputeLoRaToAUs(rxSf, m_bw, m_cr, p->GetSize());
        const uint32_t seqFromRp = ResolveBeaconSequenceFromRpCounter(src, rpCounter);

        loramesh::NeighborLinkInfo link;
        link.neighbor = src;
        link.sequence = seqFromRp;
        link.hops = 1;
        link.sf = rxSf;
        link.toaUs = toaUsNeighbor;
        link.batt_mV = 0;
        loramesh::LinkStats stats;
        stats.toaUs = toaUsNeighbor;
        stats.hops = 1;
        stats.sf = rxSf;
        stats.snrDb = 0.0;
        stats.batteryMv = 0;
        stats.energyFraction = 1.0;
        const double cost = m_compositeMetric.ComputeLinkCost(myId, src, stats);
        link.scoreX100 =
            static_cast<uint16_t>(std::round(std::clamp(1.0 - cost, 0.0, 1.0) * 100.0));
        auto itMac = m_linkAddrTable.find(src);
        link.mac = (itMac != m_linkAddrTable.end()) ? itMac->second : Mac48Address();

        loramesh::DvMessage msg;
        msg.origin = src;
        msg.sequence = seqFromRp;
        msg.entries = DecodeDvEntriesV2(payload, 0, toaUsNeighbor, rxSf);
        NS_LOG_UNCOND("DVTRACE_RX_V2 time=" << Simulator::Now().GetSeconds() << " node=" << myId
                                            << " src=" << src << " rp_counter="
                                            << unsigned(rpCounter) << " seq_ext=" << seqFromRp
                                            << " entries=" << msg.entries.size());
        if (m_routing)
        {
            m_routing->UpdateFromDvMsg(msg, link);
        }
        m_lastDvRxTime = Simulator::Now();
        return true;
    }

    DataWireHeaderV2 dataHdr;
    if (!ParseDataWirePacketV2(p, &dataHdr, &payload))
    {
        return true;
    }

    const uint16_t src = dataHdr.GetSrc();
    const uint16_t dst = dataHdr.GetDst();
    const uint16_t via = dataHdr.GetVia();
    const uint16_t seq16 = dataHdr.GetSeq16();
    const uint8_t ttl = dataHdr.GetTtl();
    const uint8_t hopsSeen = (m_initTtl > ttl) ? static_cast<uint8_t>(m_initTtl - ttl) : 0;

    if (Mac48Address::IsMatchingType(from))
    {
        const Mac48Address fromMac = Mac48Address::ConvertFrom(from);
        for (const auto& kv : m_linkAddrTable)
        {
            if (kv.second == fromMac)
            {
                m_linkAddrLastSeen[kv.first] = Simulator::Now();
                break;
            }
        }
    }

    if (myId != via && myId != dst)
    {
        return true;
    }

    const auto key = std::make_tuple(static_cast<uint32_t>(src),
                                     static_cast<uint32_t>(dst),
                                     static_cast<uint32_t>(seq16));
    CleanOldDedupCaches();

    if (myId == dst)
    {
        if (m_deliveredSet.find(key) != m_deliveredSet.end())
        {
            return true;
        }
        m_deliveredSet[key] = Simulator::Now();
        m_dataPacketsDelivered++;

        LogRxEvent(src,
                   dst,
                   seq16,
                   ttl,
                   hopsSeen,
                   0,
                   0,
                   rxSf,
                   energyJ,
                   energyFrac,
                   false);
        if (g_metricsCollector)
        {
            double txTime = g_metricsCollector->GetFirstTxTime(src, dst, seq16);
            double delaySec = (txTime >= 0.0) ? (Simulator::Now().GetSeconds() - txTime) : -1.0;
            g_metricsCollector->RecordE2eDelay(src,
                                               dst,
                                               seq16,
                                               hopsSeen,
                                               delaySec,
                                               payload ? payload->GetSize() : 0,
                                               rxSf,
                                               true);
            g_metricsCollector->RecordEnergySnapshot(myId, energyJ, energyFrac);
        }
        return true;
    }

    if (ttl == 0)
    {
        m_dropTtlExpired++;
        return true;
    }

    if (m_seenOnce.find(key) != m_seenOnce.end())
    {
        return true;
    }
    m_seenOnce[key] = Simulator::Now();

    LogRxEvent(src,
               dst,
               seq16,
               ttl,
               hopsSeen,
               0,
               0,
               rxSf,
               energyJ,
               energyFrac,
               true);

    ForwardWithTtlV2(payload, src, dst, seq16, ttl);
    return true;
}

void
MeshDvApp::ForwardWithTtlV2(Ptr<const Packet> pIn, uint16_t src, uint16_t dst, uint16_t seq16, uint8_t ttl)
{
    const uint32_t myId = GetNode()->GetId();
    if (ttl == 0)
    {
        m_dropTtlExpired++;
        return;
    }
    const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute(dst) : nullptr;
    if (!route)
    {
        m_dropNoRoute++;
        return;
    }

    uint8_t sfForRoute = m_sf;
    if (m_useRouteSfForData)
    {
        sfForRoute = route->sf;
    }
    if (m_useEmpiricalSfForData)
    {
        sfForRoute = GetDataSfForNeighbor(route->nextHop);
    }
    sfForRoute = std::clamp<uint8_t>(sfForRoute, m_sfMin, m_sfMax);

    const uint8_t nextTtl = ttl - 1;
    Ptr<Packet> payload = pIn ? pIn->Copy() : Create<Packet>(0);
    Ptr<Packet> out = payload->Copy();

    DataWireHeaderV2 outHdr;
    outHdr.SetSrc(src);
    outHdr.SetDst(dst);
    outHdr.SetVia(static_cast<uint16_t>(route->nextHop));
    outHdr.SetFlagsTtl(PackFlagsTtlV2(WirePacketTypeV2::DATA, nextTtl));
    outHdr.SetSeq16(seq16);
    out->AddHeader(outHdr);

    MeshMetricTag traceTag;
    traceTag.SetSrc(src);
    traceTag.SetDst(dst);
    traceTag.SetSeq(seq16);
    traceTag.SetPrevHop(myId);
    traceTag.SetExpectedNextHop(route->nextHop);
    traceTag.SetTtl(nextTtl);
    traceTag.SetHops((m_initTtl > nextTtl) ? static_cast<uint8_t>(m_initTtl - nextTtl) : 0);
    traceTag.SetSf(sfForRoute);
    traceTag.SetToaUs(ComputeLoRaToAUs(sfForRoute, m_bw, m_cr, out->GetSize()));
    traceTag.SetBatt_mV(GetBatteryVoltageMv());
    traceTag.SetScoreX100(ComputeScoreX100(traceTag));
    MeshMetricTag previousTrace;
    out->RemovePacketTag(previousTrace);
    out->AddPacketTag(traceTag);

    Mac48Address routeMac;
    bool usingStale = false;
    const bool hasUsableMac = ResolveUnicastNextHopLinkAddr(route->nextHop, &routeMac, &usingStale);
    (void)usingStale;
    if (!hasUsableMac)
    {
        m_dropNoRoute++;
        return;
    }
    SendWithCSMA(out, traceTag, Address(routeMac), true);
}

// ========================================================================

void
MeshDvApp::LogTxEvent(uint32_t seq,
                      uint32_t dst,
                      uint8_t ttl,
                      uint8_t hops,
                      uint16_t battery,
                      uint16_t score,
                      uint8_t sf,
                      double energyJ,
                      double energyFrac,
                      bool ok)
{
    if (g_metricsCollector)
    {
        g_metricsCollector->RecordTx(GetNode()->GetId(),
                                     seq,
                                     dst,
                                     ttl,
                                     hops,
                                     0, // rssi removed
                                     battery,
                                     score,
                                     sf,
                                     energyJ,
                                     energyFrac,
                                     ok);
        g_metricsCollector->RecordEnergySnapshot(GetNode()->GetId(), energyJ, energyFrac);
    }
}

void
MeshDvApp::LogRxEvent(uint32_t src,
                      uint32_t dst,
                      uint32_t seq,
                      uint8_t ttl,
                      uint8_t hops,
                      uint16_t battery,
                      uint16_t score,
                      uint8_t sf,
                      double energyJ,
                      double energyFrac,
                      bool forwarded)
{
    if (g_metricsCollector)
    {
        g_metricsCollector->RecordRx(GetNode()->GetId(),
                                     src,
                                     dst,
                                     seq,
                                     ttl,
                                     hops,
                                     0, // rssi removed
                                     battery,
                                     score,
                                     sf,
                                     energyJ,
                                     energyFrac,
                                     forwarded);
        g_metricsCollector->RecordEnergySnapshot(GetNode()->GetId(), energyJ, energyFrac);
    }
}

void
MeshDvApp::CleanOldSeenPackets()
{
    if (m_seenPackets.empty())
    {
        return;
    }

    Time threshold = Simulator::Now() - m_seenPacketWindow;
    for (auto it = m_seenPackets.begin(); it != m_seenPackets.end();)
    {
        if (it->second < threshold)
        {
            it = m_seenPackets.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void
MeshDvApp::CleanOldDedupCaches()
{
    const Time now = Simulator::Now();
    const Time threshold = now - m_dedupWindow;

    for (auto it = m_seenOnce.begin(); it != m_seenOnce.end();)
    {
        if (it->second < threshold)
        {
            it = m_seenOnce.erase(it);
        }
        else
        {
            ++it;
        }
    }

    for (auto it = m_deliveredSet.begin(); it != m_deliveredSet.end();)
    {
        if (it->second < threshold)
        {
            it = m_deliveredSet.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void
MeshDvApp::CleanOldSeenData()
{
    if (m_seenData.empty())
    {
        return;
    }
    Time threshold = Simulator::Now() - m_seenDataWindow;
    for (auto it = m_seenData.begin(); it != m_seenData.end();)
    {
        if (it->second.firstSeen < threshold)
        {
            it = m_seenData.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

// ========================================================================
// Data Traffic Generation
// ========================================================================

Time
MeshDvApp::ComputeNextDataSlotDelay(Time baseDelay)
{
    if (!m_enableDataSlots || m_dataSlotPeriodSec <= 0.0)
    {
        return baseDelay;
    }

    const double period = m_dataSlotPeriodSec;
    const double offset = std::fmod(m_dataSlotOffsetSec, period);
    const double baseAbs = (Simulator::Now() + baseDelay).GetSeconds();
    double k = std::ceil((baseAbs - offset) / period);
    if (k < 0.0)
    {
        k = 0.0;
    }
    const double slotTime = offset + k * period;

    const double jitterLimit = std::min(std::max(0.0, m_dataSlotJitterSec), period * 0.49);
    const double jitter =
        (m_rng && jitterLimit > 0.0) ? m_rng->GetValue(-jitterLimit, jitterLimit) : 0.0;

    double sendTime = slotTime + jitter;
    if (sendTime < baseAbs)
    {
        sendTime = slotTime;
    }
    if (sendTime < baseAbs)
    {
        sendTime = baseAbs;
    }
    if (sendTime >= slotTime + period)
    {
        sendTime = slotTime + period * 0.99;
    }

    const Time delay = Seconds(sendTime) - Simulator::Now();
    if (delay < Seconds(0))
    {
        return baseDelay;
    }
    return delay;
}

// Genera tráfico de datos hacia el gateway cuando corresponde.
void
MeshDvApp::GenerateDataTraffic()
{
    if (m_dataStopTimeSec >= 0.0 && Simulator::Now().GetSeconds() >= m_dataStopTimeSec)
    {
        if (!m_dataStopLogged)
        {
            m_dataStopLogged = true;
            NS_LOG_INFO("Data generation stopped by DataStopTimeSec at t="
                        << Simulator::Now().GetSeconds() << "s"
                        << " node=" << GetNode()->GetId());
        }
        return;
    }

    if (m_dataDestinations.empty())
    {
        return;
    }

    uint32_t dst = m_dataDestinations.front();
    if (!m_dataDestinations.empty())
    {
        if (m_enableDataRandomDest && m_rng)
        {
            const uint32_t idx =
                m_rng->GetInteger(0, static_cast<uint32_t>(m_dataDestinations.size() - 1));
            dst = m_dataDestinations[idx];
        }
        else
        {
            dst = m_dataDestinations[m_nextDestIndex];
            m_nextDestIndex = (m_nextDestIndex + 1) % m_dataDestinations.size();
        }
    }

    TrackActiveDestination(dst);
    SendDataPacket(dst);

    const double jitterMax = std::max(0.0, m_dataPeriodJitterMax);
    const double jitter = m_rng ? m_rng->GetValue(0.0, jitterMax) : 0.0;
    Time nextDelay = m_dataGenerationPeriod + Seconds(jitter);
    if (m_enableDataSlots && m_dataSlotPeriodSec > 0.0)
    {
        nextDelay = ComputeNextDataSlotDelay(nextDelay);
    }
    const double nextEventSec = (Simulator::Now() + nextDelay).GetSeconds();
    if (m_dataStopTimeSec >= 0.0 && nextEventSec >= m_dataStopTimeSec)
    {
        if (!m_dataStopLogged)
        {
            m_dataStopLogged = true;
            NS_LOG_INFO("Data generation window closed before next event at t="
                        << nextEventSec << "s"
                        << " node=" << GetNode()->GetId());
        }
        return;
    }

    m_dataGenerationEvt = Simulator::Schedule(nextDelay, &MeshDvApp::GenerateDataTraffic, this);
}

// Construye y envía un paquete de datos unicast.
void
MeshDvApp::SendDataPacket(uint32_t dst)
{
    if (m_wireFormat == "v2")
    {
        SendDataPacketV2(dst);
        return;
    }

    TrackActiveDestination(dst);
    uint32_t nextSeq = m_dataSeqPerNode + 1;
    uint32_t myId = GetNode()->GetId();

    NS_LOG_UNCOND("APP_SEND_DATA src=" << myId << " dst=" << dst << " seq=" << nextSeq
                                       << " time=" << Simulator::Now().GetSeconds());

    // Dump de tabla DV en la ventana de interés (70s-90s)
    double nowSec = Simulator::Now().GetSeconds();
    if (nowSec > 70.0 && nowSec < 90.0 && m_routing)
    {
        NS_LOG_UNCOND("DV_DUMP_FULL node" << myId << " t=" << nowSec);
        m_routing->DebugDumpRoutingTable();
    }

    const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute(dst) : nullptr;
    bool routeExists = (route != nullptr);
    uint32_t routeNextHop = routeExists ? route->nextHop : 0;
    uint8_t routeHops = routeExists ? route->hops : 0;

    NS_LOG_UNCOND("FWDTRACE data_tx_attempt time="
                  << Simulator::Now().GetSeconds() << " node=" << myId << " src=" << myId
                  << " dst=" << dst << " seq=" << nextSeq << " routeExists=" << routeExists
                  << " nextHop=" << routeNextHop << " hops=" << unsigned(routeHops));

    m_dataPacketsGenerated++;
    if (g_metricsCollector)
    {
        g_metricsCollector->RecordDataGenerated(myId, dst, nextSeq);
    }

    if (!route)
    {
        const uint32_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
        const bool hasGwRoute = m_routing ? m_routing->HasRoute(m_collectorNodeId) : false;
        RouteStatus rs = ValidateRoute(dst); // REFACTORING: usar helper
        NS_LOG_INFO("DATA_NOROUTE detail: node="
                    << myId << " src=" << myId << " dst=" << dst << " seq=" << nextSeq
                    << " time=" << Simulator::Now().GetSeconds() << "s"
                    << " routesKnown=" << routeCount << " hasGwRoute=" << (hasGwRoute ? 1 : 0)
                    << " hasEntry=" << (rs.exists ? 1 : 0) << " expired=" << (rs.expired ? 1 : 0)
                    << " collectorNodeId=" << m_collectorNodeId);
        NS_LOG_UNCOND("FWDTRACE DATA_NOROUTE time=" << Simulator::Now().GetSeconds() << " node="
                                                    << myId << " src=" << myId << " dst=" << dst
                                                    << " seq=" << nextSeq << " reason=no_route");
        m_dataNoRoute++;
        m_dropNoRoute++;
        return;
    }

    // Crear paquete de datos
    MeshMetricTag dataTag;
    dataTag.SetSrc(myId);
    dataTag.SetDst(dst); // Destino final (GW)
    dataTag.SetSeq(nextSeq);
    dataTag.SetPrevHop(myId);
    m_dataSeqPerNode = nextSeq;
    dataTag.SetTtl(m_initTtl);
    dataTag.SetHops(0);
    uint8_t dataSf = m_sf;
    if (m_useRouteSfForData && route)
    {
        dataSf = route->sf;
    }
    if (m_useEmpiricalSfForData && route)
    {
        dataSf = GetDataSfForNeighbor(route->nextHop);
    }
    dataSf = std::clamp<uint8_t>(dataSf, m_sfMin, m_sfMax);
    dataTag.SetSf(dataSf);

    const uint32_t toaUs = ComputeLoRaToAUs(dataSf, m_bw, m_cr, m_dataPayloadSize);
    dataTag.SetToaUs(toaUs);

    uint16_t realBatt = GetBatteryVoltageMv();

    // REMOVED: dataTag.SetRssiDbm - no se serializa
    dataTag.SetBatt_mV(realBatt);
    dataTag.SetScoreX100(ComputeScoreX100(dataTag));
    // FILTRADO UNICAST: Indicar quién debe recibir este paquete
    dataTag.SetExpectedNextHop(route->nextHop);

    Ptr<Packet> p = Create<Packet>(m_dataPayloadSize);
    p->AddPacketTag(dataTag);

    Mac48Address routeMac;
    bool usingStaleMac = false;
    const bool hasUsableMac = ResolveUnicastNextHopLinkAddr(route->nextHop, &routeMac, &usingStaleMac);
    const bool macFound = (m_linkAddrTable.find(route->nextHop) != m_linkAddrTable.end());
    const bool macFresh = IsLinkAddrFresh(route->nextHop);
    NS_LOG_UNCOND("LINKADDR_CHECK node" << myId << " nextHop=" << route->nextHop
                                   << " linkAddrFound=" << (macFound ? "YES" : "NO")
                                   << " linkAddrFresh=" << (macFresh ? "YES" : "NO")
                                   << " staleAllowed="
                                   << (m_allowStaleLinkAddrForUnicastData ? "YES" : "NO")
                                   << " usingStale=" << (usingStaleMac ? "YES" : "NO"));

    NS_LOG_INFO("DATA src=" << dataTag.GetSrc() << " dst=" << dataTag.GetDst() << " seq="
                            << dataTag.GetSeq() << " nextHop=" << route->nextHop // ← USAR NEXT-HOP
                            << " toaUs=" << dataTag.GetToaUs()
                            << " sf=" << unsigned(dataTag.GetSf()));

    // Log de ruta usada para este envío (snapshot coherente con routes_raw)
    if (g_metricsCollector)
    {
        g_metricsCollector->RecordRouteUsed(myId,
                                            dataTag.GetDst(),
                                            route->nextHop,
                                            route->hops,
                                            route->scoreX100,
                                            route->seqNum);
    }

    // Log explícito de TX con linkAddr resuelta (Mac48Address como wrapper interno).
    std::string macStr = "(unset)";
    {
        std::ostringstream macOss;
        macOss << routeMac;
        macStr = macOss.str();
    }
    NS_LOG_UNCOND("FWD_TX node" << myId << " dst=" << dst << " nextHop=" << route->nextHop
                                << " linkAddrWrapper=" << macStr);
    NS_LOG_UNCOND("LINKADDR_DUMP node" << myId << " nextHop=" << route->nextHop
                                       << " linkAddrWrapper=" << macStr);

    if (m_mac && !m_mac->CanTransmitNow(toaUs / 1e6))
    {
        NS_LOG_UNCOND("FWDTRACE duty_defer time="
                      << Simulator::Now().GetSeconds() << " node=" << myId
                      << " src=" << dataTag.GetSrc() << " dst=" << dst << " seq="
                      << dataTag.GetSeq() << " dutyUsed=" << m_mac->GetDutyCycleUsed()
                      << " dutyLimit=" << m_mac->GetDutyCycleLimit()
                      << " reason=duty_wait_queue");
    }

    // No enviar datos sin dirección de enlace unicast resolvible (stale permitido por política).
    Address dstAddr = hasUsableMac ? Address(routeMac) : Address();
    if (!hasUsableMac || dstAddr.IsInvalid())
    {
        const uint32_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
        const bool hasGwRoute = m_routing ? m_routing->HasRoute(m_collectorNodeId) : false;
        RouteStatus rs = ValidateRoute(dst); // REFACTORING: usar helper
        NS_LOG_INFO("DATA_NOROUTE detail: node="
                    << myId << " src=" << dataTag.GetSrc() << " dst=" << dst << " seq="
                    << dataTag.GetSeq() << " time=" << Simulator::Now().GetSeconds() << "s"
                    << " routesKnown=" << routeCount << " hasGwRoute=" << (hasGwRoute ? 1 : 0)
                    << " hasEntry=" << (rs.exists ? 1 : 0) << " expired=" << (rs.expired ? 1 : 0)
                    << " collectorNodeId=" << m_collectorNodeId << " reason=no_link_addr_for_unicast");
        NS_LOG_UNCOND("FWDTRACE DATA_NOROUTE time="
                      << Simulator::Now().GetSeconds() << " node=" << myId << " src="
                      << dataTag.GetSrc() << " dst=" << dst << " seq=" << dataTag.GetSeq()
                      << " nextHop=" << route->nextHop << " reason=no_link_addr_for_unicast");
        m_dataNoRoute++;
        m_dropNoRoute++;
        return;
    }

    double txPowerDbm = -1.0;
    Ptr<ns3::lorawan::MeshLoraNetDevice> meshDevTx =
        DynamicCast<ns3::lorawan::MeshLoraNetDevice>(GetNode()->GetDevice(0));
    if (meshDevTx)
    {
        txPowerDbm = meshDevTx->GetTxPowerDbm();
    }
    const uint32_t cadFailures = m_mac ? m_mac->GetFailureCount() : 0;
    const uint32_t lastBackoff = m_mac ? m_mac->GetLastBackoffSlots() : 0;
    const uint32_t lastWindow = m_mac ? m_mac->GetLastBackoffWindowSlots() : 0;
    const double cadLoad = m_mac ? m_mac->GetCadLoadEstimate() : 0.0;
    NS_LOG_INFO("DATA_TX detail: node="
                << myId << " src=" << dataTag.GetSrc() << " dst=" << dst
                << " seq=" << dataTag.GetSeq() << " time=" << Simulator::Now().GetSeconds() << "s"
                << " nextHop=" << route->nextHop << " sf=" << unsigned(dataTag.GetSf())
                << " txPowerDbm=" << txPowerDbm << " cadFailures=" << cadFailures
                << " backoffSlotsLast=" << lastBackoff << " windowSlotsLast=" << lastWindow
                << " cadLoad=" << cadLoad);
    NS_LOG_UNCOND("FWDTRACE fwd time=" << Simulator::Now().GetSeconds() << " node=" << myId
                                       << " src=" << dataTag.GetSrc() << " dst=" << dst
                                       << " seq=" << dataTag.GetSeq()
                                       << " nextHop=" << route->nextHop << " tx_mode=unicast"
                                       << " reason=ok");
    NS_LOG_UNCOND("DATA_TX SF" << unsigned(dataTag.GetSf()) << " node=" << myId
                               << " src=" << dataTag.GetSrc() << " dst=" << dst
                               << " seq=" << dataTag.GetSeq());

    SendWithCSMA(p, dataTag, dstAddr, true);
}

void
MeshDvApp::SendDataPacketV2(uint32_t dst)
{
    TrackActiveDestination(dst);
    const uint32_t myId = GetNode()->GetId();
    const uint16_t seq16 = static_cast<uint16_t>((++m_dataSeqPerNode) & 0xFFFF);

    NS_LOG_UNCOND("APP_SEND_DATA src=" << myId << " dst=" << dst << " seq=" << seq16
                                       << " time=" << Simulator::Now().GetSeconds());

    m_dataPacketsGenerated++;
    if (g_metricsCollector)
    {
        g_metricsCollector->RecordDataGenerated(myId, dst, seq16);
    }

    const loramesh::RouteEntry* route = m_routing ? m_routing->GetRoute(dst) : nullptr;
    if (!route)
    {
        const uint32_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
        const bool hasGwRoute = m_routing ? m_routing->HasRoute(m_collectorNodeId) : false;
        RouteStatus rs = ValidateRoute(dst);
        NS_LOG_INFO("DATA_NOROUTE detail: node="
                    << myId << " src=" << myId << " dst=" << dst << " seq=" << seq16
                    << " time=" << Simulator::Now().GetSeconds() << "s"
                    << " routesKnown=" << routeCount << " hasGwRoute=" << (hasGwRoute ? 1 : 0)
                    << " hasEntry=" << (rs.exists ? 1 : 0) << " expired=" << (rs.expired ? 1 : 0)
                    << " collectorNodeId=" << m_collectorNodeId << " reason=no_route_v2");
        NS_LOG_UNCOND("FWDTRACE DATA_NOROUTE time="
                      << Simulator::Now().GetSeconds() << " node=" << myId << " src=" << myId
                      << " dst=" << dst << " seq=" << seq16 << " reason=no_route_v2");
        m_dataNoRoute++;
        m_dropNoRoute++;
        return;
    }

    uint8_t dataSf = m_sf;
    if (m_useRouteSfForData)
    {
        dataSf = route->sf;
    }
    if (m_useEmpiricalSfForData)
    {
        dataSf = GetDataSfForNeighbor(route->nextHop);
    }
    dataSf = std::clamp<uint8_t>(dataSf, m_sfMin, m_sfMax);

    DataWireHeaderV2 hdr;
    hdr.SetSrc(static_cast<uint16_t>(myId));
    hdr.SetDst(static_cast<uint16_t>(dst));
    hdr.SetVia(static_cast<uint16_t>(route->nextHop));
    hdr.SetFlagsTtl(PackFlagsTtlV2(WirePacketTypeV2::DATA, std::min<uint8_t>(m_initTtl, 63)));
    hdr.SetSeq16(seq16);

    Ptr<Packet> p = Create<Packet>(m_dataPayloadSize);
    p->AddHeader(hdr);

    MeshMetricTag traceTag;
    traceTag.SetSrc(static_cast<uint16_t>(myId));
    traceTag.SetDst(static_cast<uint16_t>(dst));
    traceTag.SetSeq(seq16);
    traceTag.SetPrevHop(static_cast<uint16_t>(myId));
    traceTag.SetExpectedNextHop(static_cast<uint16_t>(route->nextHop));
    traceTag.SetTtl(std::min<uint8_t>(m_initTtl, 63));
    traceTag.SetHops(0);
    traceTag.SetSf(dataSf);
    traceTag.SetToaUs(ComputeLoRaToAUs(dataSf, m_bw, m_cr, p->GetSize()));
    traceTag.SetBatt_mV(GetBatteryVoltageMv());
    traceTag.SetScoreX100(ComputeScoreX100(traceTag));
    p->AddPacketTag(traceTag);

    Mac48Address routeMac;
    bool usingStale = false;
    const bool hasUsableMac = ResolveUnicastNextHopLinkAddr(route->nextHop, &routeMac, &usingStale);
    (void)usingStale;
    if (!hasUsableMac)
    {
        const uint32_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
        const bool hasGwRoute = m_routing ? m_routing->HasRoute(m_collectorNodeId) : false;
        RouteStatus rs = ValidateRoute(dst);
        NS_LOG_INFO("DATA_NOROUTE detail: node="
                    << myId << " src=" << myId << " dst=" << dst << " seq=" << seq16
                    << " time=" << Simulator::Now().GetSeconds() << "s"
                    << " routesKnown=" << routeCount << " hasGwRoute=" << (hasGwRoute ? 1 : 0)
                    << " hasEntry=" << (rs.exists ? 1 : 0) << " expired=" << (rs.expired ? 1 : 0)
                    << " collectorNodeId=" << m_collectorNodeId
                    << " nextHop=" << route->nextHop << " reason=no_link_addr_for_unicast_v2");
        NS_LOG_UNCOND("FWDTRACE DATA_NOROUTE time="
                      << Simulator::Now().GetSeconds() << " node=" << myId << " src=" << myId
                      << " dst=" << dst << " seq=" << seq16 << " nextHop=" << route->nextHop
                      << " reason=no_link_addr_for_unicast_v2");
        m_dataNoRoute++;
        m_dropNoRoute++;
        return;
    }

    if (g_metricsCollector)
    {
        g_metricsCollector->RecordRouteUsed(myId,
                                            dst,
                                            route->nextHop,
                                            route->hops,
                                            route->scoreX100,
                                            route->seqNum);
    }
    SendWithCSMA(p, traceTag, Address(routeMac), true);
}

} // namespace ns3
