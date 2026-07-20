/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-app.h"

#include "dv-cl-lora-net-device.h"
#include "dv-cl-stats-sink.h"

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
#include "ns3/simple-gateway-lora-phy.h"
#include "ns3/simulator.h"
#include "ns3/string.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring> // Para memcpy, memset, etc.
#include <iomanip>
#include <iterator>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace
{
struct DvEntryWirePueyo
{
    uint16_t destination{0};
    uint8_t score{0}; // 0 => unreachable/poison, 1..255 => advertised metric
};

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
BytesToHex(const uint8_t* data, size_t len)
{
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i)
    {
        if (i != 0)
        {
            oss << ' ';
        }
        oss << std::setw(2) << static_cast<unsigned>(data[i]);
    }
    return oss.str();
}

std::string
FormatDvEntriesPueyo(const std::vector<DvEntryWirePueyo>& entries)
{
    std::ostringstream oss;
    for (size_t i = 0; i < entries.size(); ++i)
    {
        if (i != 0)
        {
            oss << ',';
        }
        oss << entries[i].destination << ':' << unsigned(entries[i].score);
    }
    return oss.str();
}

std::vector<DvEntryWirePueyo>
ParseSyntheticDvEntriesPueyo(const std::string& spec, uint32_t maxEntries)
{
    std::vector<DvEntryWirePueyo> entries;
    if (spec.empty())
    {
        return entries;
    }

    std::stringstream ss(spec);
    std::string token;
    while (std::getline(ss, token, ','))
    {
        if (token.empty())
        {
            continue;
        }
        const auto pos = token.find(':');
        if (pos == std::string::npos)
        {
            continue;
        }
        const std::string dstStr = token.substr(0, pos);
        const std::string scoreStr = token.substr(pos + 1);
        DvEntryWirePueyo entry;
        entry.destination = static_cast<uint16_t>(std::stoul(dstStr));
        entry.score =
            static_cast<uint8_t>(std::clamp<unsigned long>(std::stoul(scoreStr), 0ul, 255ul));
        entries.push_back(entry);
        if (maxEntries > 0 && entries.size() >= maxEntries)
        {
            break;
        }
    }
    return entries;
}

std::string
FormatDecodedEntries(const std::vector<ns3::dvcl::DvEntry>& entries)
{
    std::ostringstream oss;
    for (size_t i = 0; i < entries.size(); ++i)
    {
        if (i != 0)
        {
            oss << ',';
        }
        oss << entries[i].destination << ':' << unsigned(entries[i].scoreX100);
    }
    return oss.str();
}

constexpr double kBatteryMvMin = 3000.0;
constexpr double kBatteryMvMax = 4200.0;
constexpr uint32_t kPueyoBeaconHeaderBytes = 6; // 6B: 5 base + 1 SoC (DC dropped)
constexpr uint32_t kPueyoBeaconEntryBytes = 3;

void
SerializeDvEntriesPueyo(const std::vector<DvEntryWirePueyo>& entries, uint8_t* out, size_t maxlen)
{
    if (!out || maxlen < kPueyoBeaconEntryBytes)
    {
        return;
    }

    size_t offset = 0;
    for (const auto& e : entries)
    {
        if (offset + kPueyoBeaconEntryBytes > maxlen)
        {
            break;
        }
        out[offset + 0] = static_cast<uint8_t>(e.destination & 0xFF);
        out[offset + 1] = static_cast<uint8_t>((e.destination >> 8) & 0xFF);
        out[offset + 2] = e.score;
        offset += kPueyoBeaconEntryBytes;
    }
}

void
DeserializeDvEntriesPueyo(const uint8_t* in, size_t len, std::vector<DvEntryWirePueyo>& out)
{
    out.clear();
    if (!in || len < kPueyoBeaconEntryBytes)
    {
        return;
    }

    const size_t count = len / kPueyoBeaconEntryBytes;
    out.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        const size_t offset = i * kPueyoBeaconEntryBytes;
        DvEntryWirePueyo e;
        e.destination =
            static_cast<uint16_t>(in[offset + 0]) | (static_cast<uint16_t>(in[offset + 1]) << 8);
        e.score = in[offset + 2];
        out.push_back(e);
    }
}

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

// §SoC-wire helpers ──────────────────────────────────────────────────────────
// Convert a normalised energy fraction [0,1] to the 1-byte SoC field (0-100).
// 0xFF is the "unknown / N/A" sentinel → receiver treats as full battery.
inline uint8_t
FractionToSoC8(double frac)
{
    return static_cast<uint8_t>(std::clamp(std::round(frac * 100.0), 0.0, 100.0));
}

// Convert a received SoC byte back to an energy fraction [0,1].
// 0xFF = unknown → treat as 1.0 (full), so Ψ(b_j) = 0 (no penalty).
inline double
SoC8ToFraction(uint8_t soc)
{
    if (soc == 0xFF)
    {
        return 1.0;
    }
    return std::clamp(static_cast<double>(soc) / 100.0, 0.0, 1.0);
}

// ────────────────────────────────────────────────────────────────────────────

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
namespace dvcl
{
using namespace ns3::lorawan;

NS_LOG_COMPONENT_DEFINE("DvClApp");
NS_OBJECT_ENSURE_REGISTERED(DvClApp);

// Implementación correcta del destructor:
DvClApp::~DvClApp()
{
    // Limpieza si necesitas; normalmente vacío en NS-3
    NS_LOG_FUNCTION(this);
}

// Implementación del constructor:
DvClApp::DvClApp()
{
    NS_LOG_FUNCTION(this);
    m_mac = CreateObject<DvClCsmaCadMac>();
    m_energyModel = CreateObject<DvClEnergyRegistry>();
    m_routing = CreateObject<DvClRouting>();
    m_routing->SetLocalEnergyFractionCallback(MakeCallback(&DvClApp::GetEnergyFraction, this));
    UpdateRouteTimeout();
}

TypeId
DvClApp::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClApp")
            .SetParent<Application>()
            .SetGroupName("DvCl")
            .AddConstructor<DvClApp>()
            .AddAttribute("InitialDvDelayBase",
                          "Base delay before first DV/beacon.",
                          DoubleValue(1.0),
                          MakeDoubleAccessor(&DvClApp::m_initialDvDelayBase),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("InitialDvJitterMax",
                          "Max additional random jitter before first DV/beacon.",
                          DoubleValue(8.0),
                          MakeDoubleAccessor(&DvClApp::m_initialDvJitterMax),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("InitialDvNodeSpacing",
                          "Extra per-node offset applied to first DV/beacon.",
                          DoubleValue(0.5),
                          MakeDoubleAccessor(&DvClApp::m_initialDvNodeSpacing),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataStartTimeSec",
                          "Start time for data generation (seconds).",
                          DoubleValue(90.0),
                          MakeDoubleAccessor(&DvClApp::m_dataStartTimeSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataStopTimeSec",
                          "Stop time for data generation (seconds). -1 disables stop window.",
                          DoubleValue(-1.0),
                          MakeDoubleAccessor(&DvClApp::m_dataStopTimeSec),
                          MakeDoubleChecker<double>(-1.0))
            .AddAttribute("BeaconWarmupSec",
                          "Warm-up duration used for beacon phase transitions (seconds).",
                          DoubleValue(60.0),
                          MakeDoubleAccessor(&DvClApp::m_beaconWarmupSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("TrafficLoad",
                          "Data traffic load: low/medium/high/saturation.",
                          StringValue("medium"),
                          MakeStringAccessor(&DvClApp::SetTrafficLoad, &DvClApp::GetTrafficLoad),
                          MakeStringChecker())
            .AddAttribute("TrafficMode",
                          "Traffic generator mode: periodic_any_to_any | pueyo_all_to_all.",
                          StringValue("periodic_any_to_any"),
                          MakeStringAccessor(&DvClApp::SetTrafficMode, &DvClApp::GetTrafficMode),
                          MakeStringChecker())
            .AddAttribute("SfLinkMode",
                          "How SF_lm is inferred for routing updates: observed_rxsf | "
                          "deterministic_sensitivity.",
                          StringValue("observed_rxsf"),
                          MakeStringAccessor(&DvClApp::SetSfLinkMode, &DvClApp::GetSfLinkMode),
                          MakeStringChecker())
            .AddAttribute(
                "SfLinkMarginDb",
                "Margin [dB] added over sensitivity when SfLinkMode=deterministic_sensitivity.",
                DoubleValue(0.0),
                MakeDoubleAccessor(&DvClApp::m_sfLinkMarginDb),
                MakeDoubleChecker<double>(-10.0, 20.0))
            .AddAttribute("PueyoPacketsPerPair",
                          "Packets generated per source-destination pair in pueyo_all_to_all mode.",
                          UintegerValue(100),
                          MakeUintegerAccessor(&DvClApp::m_pueyoPacketsPerPair),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("EnableDataRandomDest",
                          "Enable random destination selection for any-to-any traffic.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_enableDataRandomDest),
                          MakeBooleanChecker())
            .AddAttribute("OnlyGenerateFromNodeId",
                          "If >=0, only this node generates application data traffic.",
                          IntegerValue(-1),
                          MakeIntegerAccessor(&DvClApp::m_onlyGenerateFromNodeId),
                          MakeIntegerChecker<int32_t>(-1))
            .AddAttribute("ForcedDataDestinationId",
                          "If >=0, generated data uses only this final destination.",
                          IntegerValue(-1),
                          MakeIntegerAccessor(&DvClApp::m_forcedDataDestinationId),
                          MakeIntegerChecker<int32_t>(-1))
            .AddAttribute("DataPeriodJitterMax",
                          "Max extra jitter applied to data period (seconds).",
                          DoubleValue(0.5),
                          MakeDoubleAccessor(&DvClApp::m_dataPeriodJitterMax),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataPeriodJitterSymmetric",
                          "If true, apply zero-mean jitter to successive data periods.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_dataPeriodJitterSymmetric),
                          MakeBooleanChecker())
            .AddAttribute(
                "DataStartPhaseMaxSec",
                "Initial per-node random phase offset added before the first data packet.",
                DoubleValue(0.0),
                MakeDoubleAccessor(&DvClApp::m_dataStartPhaseMaxSec),
                MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataPayloadSizeBytes",
                          "Application payload size [bytes] for generated unicast data packets.",
                          UintegerValue(20),
                          MakeUintegerAccessor(&DvClApp::m_dataPayloadSize),
                          MakeUintegerChecker<uint32_t>(1, 250))
            .AddAttribute("PreambleSymbols",
                          "LoRa preamble symbols used by internal ToA estimator.",
                          UintegerValue(8),
                          MakeUintegerAccessor(&DvClApp::m_preambleSymbols),
                          MakeUintegerChecker<uint32_t>(6, 64))
            .AddAttribute(
                "BatteryFullCapacityJ",
                "Nominal full battery capacity [J] used to compute SoC from remaining energy.",
                DoubleValue(38880.0),
                MakeDoubleAccessor(&DvClApp::m_batteryFullCapacityJ),
                MakeDoubleChecker<double>(1.0))
            .AddAttribute("EnableDataSlots",
                          "Enable local micro-slots for data transmissions.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_enableDataSlots),
                          MakeBooleanChecker())
            .AddAttribute("DataSlotPeriodSec",
                          "Slot period used to align data transmissions (seconds).",
                          DoubleValue(0.0),
                          MakeDoubleAccessor(&DvClApp::m_dataSlotPeriodSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataSlotJitterSec",
                          "Jitter applied around data slot boundaries (seconds).",
                          DoubleValue(0.0),
                          MakeDoubleAccessor(&DvClApp::m_dataSlotJitterSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataStartPhaseOnly",
                          "If true, data slots are applied only to the first generated packet.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_dataStartPhaseOnly),
                          MakeBooleanChecker())
            .AddAttribute("DataFixedPhaseCadence",
                          "If true, preserve the node phase on every data period.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_dataFixedPhaseCadence),
                          MakeBooleanChecker())
            .AddAttribute("PrioritizeBeacons",
                          "If true, enqueue DV beacons ahead of data in CSMA queue.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClApp::m_prioritizeBeacons),
                          MakeBooleanChecker())
            .AddAttribute("BeaconLatestOnly",
                          "If true, keep only the latest pending DV beacon in queue.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClApp::m_beaconLatestOnly),
                          MakeBooleanChecker())
            .AddAttribute("PueyoStrictQueueScheduler",
                          "Enable strict queue scheduler (Routing/Data=10:1, Forward/Local=10:1).",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_usePueyoStrictQueueScheduler),
                          MakeBooleanChecker())
            .AddAttribute("ControlBackoffFactor",
                          "Backoff multiplier for control (beacon) frames.",
                          DoubleValue(0.5),
                          MakeDoubleAccessor(&DvClApp::m_controlBackoffFactor),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("DataBackoffFactor",
                          "Backoff multiplier for data frames.",
                          DoubleValue(1.0),
                          MakeDoubleAccessor(&DvClApp::m_dataBackoffFactor),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("CsmaMaxRetries",
                          "Maximum CSMA/CAD retries per TX queue entry before dropping. "
                          "Entry is popped from the head when retries exceed this limit on "
                          "sustained CAD-busy outcomes; counted as drop_max_csma_retries.",
                          UintegerValue(8),
                          MakeUintegerAccessor(&DvClApp::m_csmaMaxRetries),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("CsmaTxQueueMax",
                          "Maximum TX queue depth for CSMA/CAD path. On overflow the oldest "
                          "non-beacon entry is dropped (counted as drop_queue_overflow).",
                          UintegerValue(32),
                          MakeUintegerAccessor(&DvClApp::m_csmaTxQueueMax),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("EnableControlGuard",
                          "Delay data transmissions shortly after DV beacon activity.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_enableControlGuard),
                          MakeBooleanChecker())
            .AddAttribute("ControlGuardSec",
                          "Guard window (seconds) after DV TX/RX before starting data TX.",
                          DoubleValue(0.0),
                          MakeDoubleAccessor(&DvClApp::m_controlGuardSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute(
                "StudyForwardSpreadEnable",
                "Study-only: spread relay forwarding attempts over a deterministic delay window.",
                BooleanValue(false),
                MakeBooleanAccessor(&DvClApp::m_studyForwardSpreadEnable),
                MakeBooleanChecker())
            .AddAttribute("StudyForwardBaseDelayMs",
                          "Study-only: base relay forwarding delay in milliseconds.",
                          DoubleValue(10.0),
                          MakeDoubleAccessor(&DvClApp::m_studyForwardBaseDelayMs),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute(
                "StudyForwardSpreadMs",
                "Study-only: extra deterministic relay forwarding spread window in milliseconds.",
                DoubleValue(240.0),
                MakeDoubleAccessor(&DvClApp::m_studyForwardSpreadMs),
                MakeDoubleChecker<double>(0.0))
            .AddAttribute(
                "StudySuperframeEnable",
                "Study-only: enforce a lightweight control/data superframe at dequeue time.",
                BooleanValue(false),
                MakeBooleanAccessor(&DvClApp::m_studySuperframeEnable),
                MakeBooleanChecker())
            .AddAttribute("StudySuperframePeriodSec",
                          "Study-only: superframe period in seconds.",
                          DoubleValue(1.0),
                          MakeDoubleAccessor(&DvClApp::m_studySuperframePeriodSec),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute(
                "StudySuperframeCtrlWindowSec",
                "Study-only: control-only window length inside each superframe period (seconds).",
                DoubleValue(0.2),
                MakeDoubleAccessor(&DvClApp::m_studySuperframeCtrlWindowSec),
                MakeDoubleChecker<double>(0.0))
            .AddAttribute("EnableDvFlooding",
                          "Enable explicit flooding of received DV beacons.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_enableDvFlooding),
                          MakeBooleanChecker())
            .AddAttribute("AdvertiseAllRoutes",
                          "If true, include all known routes in DV announcements.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClApp::m_advertiseAllRoutes),
                          MakeBooleanChecker())
            .AddAttribute("UseEmpiricalSfForData",
                          "If true, prefer empirical SF learned from successful beacon reception.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClApp::m_useEmpiricalSfForData),
                          MakeBooleanChecker())
            .AddAttribute("UseProbabilisticSfForBeacons",
                          "If true, select beacon SF using probabilistic policy over SfMin..SfMax.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClApp::m_useProbabilisticSf),
                          MakeBooleanChecker())
            .AddAttribute(
                "SfControl",
                "Fixed SF used for control beacons when probabilistic selection is disabled.",
                UintegerValue(12),
                MakeUintegerAccessor(&DvClApp::m_sfControl),
                MakeUintegerChecker<uint8_t>(7, 12))
            .AddAttribute("SfMin",
                          "Minimum SF used by probabilistic beacon selection.",
                          UintegerValue(7),
                          MakeUintegerAccessor(&DvClApp::m_sfMin),
                          MakeUintegerChecker<uint8_t>(7, 12))
            .AddAttribute("SfMax",
                          "Maximum SF used by probabilistic beacon selection.",
                          UintegerValue(12),
                          MakeUintegerAccessor(&DvClApp::m_sfMax),
                          MakeUintegerChecker<uint8_t>(7, 12))
            .AddAttribute("AllowStaleMacForUnicastData",
                          "Legacy alias for AllowStaleLinkAddrForUnicastData.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClApp::m_allowStaleLinkAddrForUnicastData),
                          MakeBooleanChecker())
            .AddAttribute("AllowStaleLinkAddrForUnicastData",
                          "If true, allow unicast data TX using stale learned link-layer address "
                          "for nextHop.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClApp::m_allowStaleLinkAddrForUnicastData),
                          MakeBooleanChecker())
            .AddAttribute(
                "EmpiricalSfMinSamples",
                "Minimum recent beacon samples per SF required when using robust_min mode.",
                UintegerValue(2),
                MakeUintegerAccessor(&DvClApp::m_empiricalSfMinSamples),
                MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("EmpiricalSfSelectMode",
                          "Empirical SF selector mode: min | robust_min.",
                          StringValue("robust_min"),
                          MakeStringAccessor(&DvClApp::m_empiricalSfSelectMode),
                          MakeStringChecker())
            .AddAttribute("DvBeaconMaxRoutes",
                          "Upper bound for routes included in DV beacons (0 = MTU-derived).",
                          UintegerValue(0),
                          MakeUintegerAccessor(&DvClApp::m_dvBeaconMaxRoutes),
                          MakeUintegerChecker<uint32_t>())
            .AddAttribute("DvBeaconOverheadBytes",
                          "Reserved bytes when computing beacon route capacity.",
                          UintegerValue(1),
                          MakeUintegerAccessor(&DvClApp::m_dvBeaconOverheadBytes),
                          MakeUintegerChecker<uint32_t>())
            .AddAttribute(
                "DvPayloadMaxBytes",
                "Hard cap for DV payload bytes in beacon route entries (0 = MTU-derived).",
                UintegerValue(0),
                MakeUintegerAccessor(&DvClApp::m_dvPayloadMaxBytes),
                MakeUintegerChecker<uint32_t>())
            .AddAttribute("RouteAdvertPolicy",
                          "Route selection policy for beacon payload truncation: top_score | "
                          "uniform | cost_weighted.",
                          StringValue("top_score"),
                          MakeStringAccessor(&DvClApp::m_routeAdvertPolicy),
                          MakeStringChecker())
            .AddAttribute("ExtraDvBeaconMinGap",
                          "Minimum time gap between DV beacons.",
                          TimeValue(Seconds(0.5)),
                          MakeTimeAccessor(&DvClApp::m_extraDvBeaconMinGap),
                          MakeTimeChecker())
            .AddAttribute("ExtraDvBeaconSecondDelay",
                          "Delay between extra DV beacons after a new active destination.",
                          TimeValue(Seconds(1.0)),
                          MakeTimeAccessor(&DvClApp::m_extraDvBeaconSecondDelay),
                          MakeTimeChecker())
            .AddAttribute("ExtraDvBeaconJitterMax",
                          "Max random jitter added to extra DV beacons (seconds).",
                          DoubleValue(0.2),
                          MakeDoubleAccessor(&DvClApp::m_extraDvBeaconJitterMax),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("ExtraDvBeaconWindow",
                          "Time window to limit extra DV beacons.",
                          TimeValue(Seconds(10)),
                          MakeTimeAccessor(&DvClApp::m_extraDvBeaconWindow),
                          MakeTimeChecker())
            .AddAttribute("ExtraDvBeaconMaxPerWindow",
                          "Maximum extra DV beacons per window.",
                          UintegerValue(2),
                          MakeUintegerAccessor(&DvClApp::m_extraDvBeaconMaxPerWindow),
                          MakeUintegerChecker<uint32_t>())
            .AddAttribute("DisableExtraAfterWarmup",
                          "Disable extra DV beacons after warmup period.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_disableExtraAfterWarmup),
                          MakeBooleanChecker())
            .AddAttribute("MacCacheWindow",
                          "Legacy alias for LinkAddrCacheWindow.",
                          TimeValue(Seconds(300)),
                          MakeTimeAccessor(&DvClApp::m_linkAddrCacheWindow),
                          MakeTimeChecker())
            .AddAttribute("LinkAddrCacheWindow",
                          "Time window to keep learned link-layer address entries for nextHop.",
                          TimeValue(Seconds(300)),
                          MakeTimeAccessor(&DvClApp::m_linkAddrCacheWindow),
                          MakeTimeChecker())
            .AddAttribute("DedupWindowSec",
                          "Dedup cache TTL for dataplane keys (src,dst,seq16).",
                          TimeValue(Seconds(600)),
                          MakeTimeAccessor(&DvClApp::m_dedupWindow),
                          MakeTimeChecker())
            .AddAttribute(
                "AutoTimeoutsFromBeacon",
                "Auto-scale route timeout and neighbor link freshness from active beacon interval.",
                BooleanValue(true),
                MakeBooleanAccessor(&DvClApp::m_autoTimeoutsFromBeacon),
                MakeBooleanChecker())
            .AddAttribute(
                "NeighborLinkTimeoutFactor",
                "Neighbor link freshness expressed in beacon intervals (auto-timeout mode).",
                DoubleValue(1.0),
                MakeDoubleAccessor(&DvClApp::m_neighborLinkTimeoutFactor),
                MakeDoubleChecker<double>(0.1))
            .AddAttribute("NeighborLinkTimeout",
                          "Manual validity window for empirical per-SF neighbor history (used when "
                          "auto-timeout is disabled).",
                          TimeValue(Seconds(60)),
                          MakeTimeAccessor(&DvClApp::m_neighborLinkTimeoutConfigured),
                          MakeTimeChecker())
            .AddAttribute("BeaconIntervalWarm",
                          "DV beacon interval during warmup phase.",
                          TimeValue(Seconds(10)),
                          MakeTimeAccessor(&DvClApp::m_beaconIntervalWarm),
                          MakeTimeChecker())
            .AddAttribute("BeaconIntervalStable",
                          "DV beacon interval during stable phase.",
                          TimeValue(Seconds(60)),
                          MakeTimeAccessor(&DvClApp::m_beaconIntervalStable),
                          MakeTimeChecker())
            .AddAttribute("RouteTimeoutFactor",
                          "Multiplier used to derive route timeout from beacon intervals.",
                          DoubleValue(6.0),
                          MakeDoubleAccessor(&DvClApp::m_routeTimeoutFactor),
                          MakeDoubleChecker<double>(1.0))
            .AddAttribute("RouteSwitchMinDeltaX100",
                          "Minimum score delta required to switch next-hop (hysteresis).",
                          UintegerValue(5),
                          MakeUintegerAccessor(&DvClApp::m_routeSwitchMinDeltaX100),
                          MakeUintegerChecker<uint16_t>())
            .AddAttribute("AvoidImmediateBacktrack",
                          "Avoid forwarding data back to the packet prevHop (loop guard).",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClApp::m_avoidImmediateBacktrack),
                          MakeBooleanChecker())
            .AddAttribute(
                "PurePueyoBaselineMode",
                "Enable strict Pueyo baseline semantics for beacon selection/advertisement.",
                BooleanValue(false),
                MakeBooleanAccessor(&DvClApp::m_purePueyoBaselineMode),
                MakeBooleanChecker())
            .AddAttribute("PueyoValidationTrace",
                          "Emit detailed validation traces for Pueyo beacon path.",
                          BooleanValue(false),
                          MakeBooleanAccessor(&DvClApp::m_pueyoValidationTrace),
                          MakeBooleanChecker())
            .AddAttribute(
                "EnableGapAuditTrace",
                "Emit and retain extra control-plane maturity counters for PDR gap audit.",
                BooleanValue(false),
                MakeBooleanAccessor(&DvClApp::m_enableGapAuditTrace),
                MakeBooleanChecker())
            .AddAttribute("PueyoSyntheticEntriesNodeId",
                          "If >=0 and validation trace is enabled, this node injects synthetic "
                          "beacon entries.",
                          IntegerValue(-1),
                          MakeIntegerAccessor(&DvClApp::m_pueyoSyntheticEntriesNodeId),
                          MakeIntegerChecker<int32_t>())
            .AddAttribute(
                "PueyoSyntheticEntries",
                "Synthetic Pueyo beacon entries as dst:score,dst:score,... for validation.",
                StringValue(""),
                MakeStringAccessor(&DvClApp::m_pueyoSyntheticEntries),
                MakeStringChecker());
    return tid;
}

void
DvClApp::UpdateRouteTimeout()
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
            NS_LOG_INFO("ADAPT_TIMEOUT node"
                        << GetNode()->GetId() << " phase=" << GetBeaconPhaseLabel()
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
DvClApp::SetTrafficLoad(std::string load)
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

void
DvClApp::SetTrafficMode(std::string mode)
{
    const std::string normalized = ToLower(mode);
    if (normalized == "pueyo_all_to_all")
    {
        m_trafficMode = TrafficMode::PUEYO_ALL_TO_ALL;
    }
    else
    {
        m_trafficMode = TrafficMode::PERIODIC_ANY_TO_ANY;
    }
}

void
DvClApp::SetSfLinkMode(std::string mode)
{
    const std::string normalized = ToLower(mode);
    if (normalized == "deterministic_sensitivity")
    {
        m_sfLinkMode = SfLinkMode::DETERMINISTIC_SENSITIVITY;
        return;
    }
    m_sfLinkMode = SfLinkMode::OBSERVED_RXSF;
}

std::string
DvClApp::GetSfLinkMode() const
{
    return (m_sfLinkMode == SfLinkMode::DETERMINISTIC_SENSITIVITY) ? "deterministic_sensitivity"
                                                                   : "observed_rxsf";
}

std::string
DvClApp::GetTrafficLoad() const
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

std::string
DvClApp::GetTrafficMode() const
{
    return (m_trafficMode == TrafficMode::PUEYO_ALL_TO_ALL) ? "pueyo_all_to_all"
                                                            : "periodic_any_to_any";
}

void
DvClApp::UpdateDataPeriod()
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
DvClApp::BuildPueyoTrafficSchedule()
{
    m_pueyoTrafficSchedule.clear();
    m_pueyoTrafficIndex = 0;
    if (m_dataDestinations.empty())
    {
        return;
    }

    const uint32_t packetsPerPair = std::max<uint32_t>(1, m_pueyoPacketsPerPair);
    const uint64_t total = static_cast<uint64_t>(packetsPerPair) * m_dataDestinations.size();
    m_pueyoTrafficSchedule.reserve(static_cast<size_t>(total));
    for (uint32_t dst : m_dataDestinations)
    {
        for (uint32_t k = 0; k < packetsPerPair; ++k)
        {
            m_pueyoTrafficSchedule.push_back(dst);
        }
    }

    if (m_rng && m_pueyoTrafficSchedule.size() > 1)
    {
        for (std::size_t i = m_pueyoTrafficSchedule.size() - 1; i > 0; --i)
        {
            const uint32_t j = m_rng->GetInteger(0, static_cast<uint32_t>(i));
            std::swap(m_pueyoTrafficSchedule[i], m_pueyoTrafficSchedule[j]);
        }
    }
}

void
DvClApp::InitDataDestinations()
{
    m_dataDestinations.clear();
    m_nextDestIndex = 0;
    m_pueyoTrafficSchedule.clear();
    m_pueyoTrafficIndex = 0;

    Ptr<Node> node = GetNode();
    if (!node)
    {
        return;
    }

    const uint32_t myId = node->GetId();
    if (m_onlyGenerateFromNodeId >= 0 && static_cast<uint32_t>(m_onlyGenerateFromNodeId) != myId)
    {
        return;
    }
    const uint32_t totalNodes = NodeList::GetNNodes();
    if (m_forcedDataDestinationId >= 0)
    {
        const uint32_t forcedDst = static_cast<uint32_t>(m_forcedDataDestinationId);
        if (forcedDst < totalNodes && forcedDst != myId)
        {
            m_dataDestinations.push_back(forcedDst);
        }
    }
    else
    {
        for (uint32_t i = 0; i < totalNodes; ++i)
        {
            if (i == myId)
            {
                continue;
            }
            m_dataDestinations.push_back(i);
        }
    }

    if (m_enableDataRandomDest && m_rng && m_dataDestinations.size() > 1)
    {
        for (std::size_t i = m_dataDestinations.size() - 1; i > 0; --i)
        {
            const uint32_t j = m_rng->GetInteger(0, static_cast<uint32_t>(i));
            std::swap(m_dataDestinations[i], m_dataDestinations[j]);
        }
    }

    if (m_trafficMode == TrafficMode::PUEYO_ALL_TO_ALL)
    {
        BuildPueyoTrafficSchedule();
    }
}

void
DvClApp::CountDropNoRouteSrc()
{
    m_dropNoRoute++;
    m_dropNoRouteSrc++;
}

void
DvClApp::CountDropNoRouteRelay()
{
    m_dropNoRoute++;
    m_dropNoRouteRelay++;
}

void
DvClApp::InitDataSlots()
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
DvClApp::TrackActiveDestination(uint32_t dst)
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
DvClApp::ScheduleExtraDvBeacons(const std::string& reason)
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
        // [B4] Stream determinista por-nodo (coherente con StartApplication).
        m_rng->SetStream(static_cast<int64_t>(1000000) + static_cast<int64_t>(GetNode()->GetId()));
    }

    const Time sinceLast =
        m_lastDvBeaconTime.IsZero() ? m_extraDvBeaconMinGap : (now - m_lastDvBeaconTime);
    const Time baseDelay =
        (sinceLast < m_extraDvBeaconMinGap) ? (m_extraDvBeaconMinGap - sinceLast) : Seconds(0);

    auto scheduleOne = [&](Time delay) {
        Simulator::Schedule(delay, &DvClApp::SendExtraDvBeacon, this, reason);
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
DvClApp::SendExtraDvBeacon(const std::string& reason)
{
    NS_LOG_INFO("EXTRA_DV_BEACON node=" << GetNode()->GetId() << " reason=" << reason
                                        << " t=" << Simulator::Now().GetSeconds());
    // B3: usar SF probabilístico si está habilitado
    const uint8_t sf = m_useProbabilisticSf ? SelectRandomSfProbabilistic() : m_sfControl;
    BuildAndSendDv(sf);
}

uint32_t
DvClApp::GetBeaconRouteCapacity() const
{
    if (m_dvPayloadMaxBytes > 0)
    {
        return std::max(1u, m_dvPayloadMaxBytes / std::max(1u, kPueyoBeaconEntryBytes));
    }

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

    const uint32_t entrySize = kPueyoBeaconEntryBytes;
    if (entrySize == 0 || mtu <= 1)
    {
        return 1;
    }
    const uint32_t overheadBytes =
        std::max<uint32_t>(m_dvBeaconOverheadBytes, kPueyoBeaconHeaderBytes);
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
DvClApp::SetPeriod(Time t)
{
    m_period = t;
    UpdateRouteTimeout();
}

void
DvClApp::SetRouteTimeoutFactor(double factor)
{
    m_routeTimeoutFactor = std::max(1.0, factor);
    UpdateRouteTimeout();
}

void
DvClApp::BootstrapLinkAddrTableFromRx()
{
    // Link-layer address table is now learned on RX (beacons/data) instead of being preloaded at
    // startup. Keep this hook for backward compatibility/log traceability.
    m_linkAddrTable.clear();
    m_linkAddrLastSeen.clear();
    if (Ptr<Node> self = GetNode())
    {
        NS_LOG_INFO("LINKADDR_BOOTSTRAP node=" << self->GetId()
                                               << " entries=0 mode=rx_learning_only");
    }
}

// Construye y transmite el beacon DV periódico con las mejores rutas conocidas.
void
DvClApp::BuildAndSendDv(uint8_t sf)
{
    BuildAndSendDvPueyo(sf);
}

void
DvClApp::BuildAndSendDvPueyo(uint8_t sf)
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

    std::vector<DvEntryWirePueyo> entries;
    if (m_routing)
    {
        bool usedSynthetic = false;
        if (m_pueyoValidationTrace && !m_pueyoSyntheticEntries.empty() &&
            m_pueyoSyntheticEntriesNodeId >= 0 &&
            static_cast<uint32_t>(m_pueyoSyntheticEntriesNodeId) == GetNode()->GetId())
        {
            entries = ParseSyntheticDvEntriesPueyo(m_pueyoSyntheticEntries, maxRoutes);
            usedSynthetic = true;
        }
        else
        {
            const auto announcements = m_purePueyoBaselineMode
                                           ? m_routing->GetBestRoutesPueyo(maxRoutes)
                                           : m_routing->GetBestRoutes(maxRoutes);
            entries.reserve(announcements.size());
            for (const auto& ann : announcements)
            {
                DvEntryWirePueyo e;
                e.destination = static_cast<uint16_t>(ann.destination);
                e.score = static_cast<uint8_t>(std::min<uint16_t>(ann.scoreX100, 255));
                entries.push_back(e);
            }
        }

        if (m_pueyoValidationTrace)
        {
            NS_LOG_INFO("PUEYO_VAL_TX_INTERNAL node=" << GetNode()->GetId()
                                                      << " synthetic=" << (usedSynthetic ? 1 : 0)
                                                      << " maxRoutes=" << maxRoutes << " entries="
                                                      << FormatDvEntriesPueyo(entries));
        }
    }

    const uint32_t payloadSizeBytes =
        static_cast<uint32_t>(entries.size() * kPueyoBeaconEntryBytes);
    std::vector<uint8_t> payload(payloadSizeBytes, 0);
    if (!entries.empty())
    {
        SerializeDvEntriesPueyo(entries, payload.data(), payload.size());
        if (m_pueyoValidationTrace)
        {
            NS_LOG_INFO("PUEYO_VAL_TX_PAYLOAD node=" << GetNode()->GetId() << " payload_hex="
                                                     << BytesToHex(payload.data(), payload.size()));
        }
    }

    Ptr<Packet> p = Create<Packet>(payload.data(), payload.size());

    // §DC-wire: presupuesto de DC restante del emisor [0-100]; 0xFF si MAC no disponible.
    uint8_t dcRem8 = 0xFF;
    if (m_mac)
    {
        const double dcLim = m_mac->GetDutyCycleLimit();
        const double dcUsed = m_mac->GetDutyCycleUsed();
        const double dcFrac = (dcLim > 0.0) ? std::clamp(1.0 - dcUsed / dcLim, 0.0, 1.0) : 1.0;
        dcRem8 = static_cast<uint8_t>(std::round(dcFrac * 100.0));
    }

    DvClBeaconHeader hdr;
    hdr.SetSrc(myId);
    hdr.SetDst(0xFFFF);
    hdr.SetFlagsTtl(PackFlagsTtl(DvClPacketType::BEACON, 0));
    hdr.SetSoc(FractionToSoC8(GetEnergyFraction())); // §SoC-wire
    (void)dcRem8;                                    // DC byte removed from the 6B wire contract
    p->AddHeader(hdr);

    DvClMetricTag traceTag;
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
    traceTag.SetDcRemaining(dcRem8); // §DC-wire
    p->AddPacketTag(traceTag);

    if (m_routing)
    {
        m_routing->SetSequence(m_seq);
    }

    NS_LOG_INFO("DVTRACE_TX_PUEYO time="
                << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId() << " seq="
                << traceTag.GetSeq() << " entries=" << entries.size() << " bytes=" << p->GetSize()
                << " maxRoutes=" << maxRoutes << " phase=" << GetBeaconPhaseLabel());

    m_lastDvBeaconTime = Simulator::Now();
    RecordBeaconScheduled(traceTag.GetSeq());
    SendWithCSMA(p, traceTag, Address(), true);
}

// Inicializa timers, callbacks y generación de tráfico.
void
DvClApp::OnPhyCollisionDrop(Ptr<const Packet> packet, uint32_t /*nodeId*/)
{
    DvClMetricTag tag;
    if (packet->PeekPacketTag(tag) && tag.GetDst() == 0xFFFF)
    {
        ++m_beaconCollisionDrops;
    }
    else
    {
        ++m_dataCollisionDrops;
    }
}

void
DvClApp::OnPhyBusyDrop(Ptr<const Packet> packet, uint32_t /*nodeId*/)
{
    DvClMetricTag tag;
    if (packet->PeekPacketTag(tag) && tag.GetDst() == 0xFFFF)
    {
        ++m_beaconBusyDrops;
    }
    else
    {
        ++m_dataBusyDrops;
    }
}

void
DvClApp::StartApplication()
{
    uint32_t nodeId = GetNode()->GetId();
    m_lastAppliedRxScanTimeS = 0.0;

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
        dev->SetReceiveCallback(MakeCallback(&DvClApp::L2Receive, this));
        Ptr<DvClLoraNetDevice> meshDev = DynamicCast<DvClLoraNetDevice>(dev);
        if (meshDev)
        {
            if (!m_meshDevice)
            {
                m_meshDevice = dev;
            }
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
    // [B4] Stream determinista por-nodo para reproducibilidad al variar N.
    // Rango app: 1_000_000 + nodeId (colisión libre para N<=65535).
    m_rng->SetStream(static_cast<int64_t>(1000000) + static_cast<int64_t>(GetNode()->GetId()));
    ResetStrictRoutingDataBudgets();
    ResetStrictForwardLocalBudgets();
    UpdateDataPeriod();
    InitDataDestinations();
    InitDataSlots();
    if (!m_mac)
    {
        m_mac = CreateObject<DvClCsmaCadMac>();
    }
    m_mac->SetRandomStream(m_rng);
    m_mac->SetDutyCycleWindow(Hours(1));
    m_mac->SetCadDuration(m_cadDuration);
    m_mac->SetDifsCadCount(m_difsCadCount);

    // CRÍTICO: enlazar DvClCsmaCadMac con LoraPhy para CAD real.
    if (m_mac)
    {
        bool meshDevFound = false;
        bool phyLinked = false;
        for (uint32_t i = 0; i < n->GetNDevices(); ++i)
        {
            Ptr<NetDevice> dev = n->GetDevice(i);
            Ptr<DvClLoraNetDevice> meshDev = DynamicCast<DvClLoraNetDevice>(dev);
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
                            << " DvClLoraNetDevice sin PHY, no se puede enlazar CAD real");
                continue;
            }
            m_mac->SetPhy(phy);
            // Connect exact PHY drop counters (data vs beacon)
            {
                Ptr<ns3::lorawan::SimpleGatewayLoraPhy> sgPhy =
                    DynamicCast<ns3::lorawan::SimpleGatewayLoraPhy>(phy);
                if (sgPhy)
                {
                    sgPhy->TraceConnectWithoutContext(
                        "LostPacketBecauseInterference",
                        MakeCallback(&DvClApp::OnPhyCollisionDrop, this));
                    sgPhy->TraceConnectWithoutContext("LostPacketBecauseNoMoreReceivers",
                                                      MakeCallback(&DvClApp::OnPhyBusyDrop, this));
                }
            }
            phyLinked = true;
            break; // enlazar solo una vez
        }
        if (!meshDevFound)
        {
            NS_LOG_WARN("StartApplication(): node="
                        << nodeId << " sin DvClLoraNetDevice, no se puede enlazar CAD real");
        }
        else if (!phyLinked)
        {
            NS_LOG_WARN("StartApplication(): node="
                        << nodeId << " DvClLoraNetDevice sin PHY valido para CAD real");
        }
    }

    if (m_energyModel)
    {
        m_energyModel->RegisterNode(nodeId);
    }

    UpdateRouteTimeout();

    if (!m_routing)
    {
        m_routing = CreateObject<DvClRouting>();
    }
    m_routing->SetNodeId(nodeId);
    m_routing->SetRouteTimeout(m_routeTimeout);
    m_routing->SetInitTtl(m_initTtl);
    m_routing->SetMaxHops(m_initTtl);
    m_routing->SetMaxRoutes(GetBeaconRouteCapacity());
    m_routing->SetSequence(m_seq);
    m_routing->SetAdvertiseAllRoutes(m_advertiseAllRoutes);
    m_routing->SetAdvertRoutePolicy(m_routeAdvertPolicy);
    m_routing->SetSinkNodeId(m_collectorNodeId);
    m_routing->SetRouteSwitchMinDeltaX100(m_routeSwitchMinDeltaX100);
    m_routing->SetRouteChangeCallback(MakeCallback(&DvClApp::HandleRouteChange, this));
    m_routing->SetFloodCallback(MakeCallback(&DvClApp::HandleFloodRequest, this));
    m_routing->SetLocalEnergyFractionCallback(MakeCallback(&DvClApp::GetEnergyFraction, this));
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
                Ptr<DvClLoraNetDevice> meshDev = DynamicCast<DvClLoraNetDevice>(dev);
                if (meshDev)
                {
                    // Mapear el LoRa device del nodo actual
                    // En una topología simple, asumimos solo un LoRa device por nodo
                    // Para topologías complejas, expandir la lógica aquí
                    m_neighborDevices[nodeId] = dev;
                    NS_LOG_INFO("Tabla vecinos: nodo=" << nodeId
                                                       << " registrado con DvClLoraNetDevice");
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
    Simulator::Schedule(firstDvTime, &DvClApp::SendInitialDv, this);

    m_purgeEvt = Simulator::Schedule(Seconds(30), &DvClApp::PurgeExpiredRoutes, this);
    if (nodeId < 3)
    {
        m_periodicDumpEvt = Simulator::Schedule(Seconds(30), &DvClApp::SchedulePeriodicDump, this);
    }

    Simulator::Schedule(Seconds(20), &DvClApp::PrintRoutingTable, this);
    Simulator::Schedule(Seconds(40), &DvClApp::PrintRoutingTable, this);

    // Programar generación de datos (controlada por TrafficMode)
    const double dataDelay = std::max(0.0, m_dataStartTimeSec);
    const double dataJitterMax = std::max(0.0, m_dataPeriodJitterMax);
    const double dataJitter =
        (!m_dataPeriodJitterSymmetric && m_rng) ? m_rng->GetValue(0.0, dataJitterMax) : 0.0;
    const double dataStartPhaseMax = std::max(0.0, m_dataStartPhaseMaxSec);
    const double dataStartPhase = m_rng ? m_rng->GetValue(0.0, dataStartPhaseMax) : 0.0;
    m_dataStopLogged = false;
    if (!m_dataDestinations.empty())
    {
        Time dataStartDelay = Seconds(dataDelay + dataStartPhase + dataJitter);
        if (m_enableDataSlots && m_dataSlotPeriodSec > 0.0)
        {
            dataStartDelay = ComputeNextDataSlotDelay(dataStartDelay);
        }
        m_nextDataNominalTimeSec = (Simulator::Now() + dataStartDelay).GetSeconds();
        const double firstDataTimeSec = (Simulator::Now() + dataStartDelay).GetSeconds();
        if (m_dataStopTimeSec >= 0.0 && firstDataTimeSec >= m_dataStopTimeSec)
        {
            NS_LOG_INFO("  >>> Node " << nodeId << " - Data generation disabled by DataStopTimeSec="
                                      << m_dataStopTimeSec << "s"
                                      << " (firstDataAt=" << firstDataTimeSec << "s)");
        }
        else
        {
            NS_LOG_INFO("  >>> Node "
                        << nodeId << " - Data generation scheduled at t=" << firstDataTimeSec
                        << "s (mode=" << GetTrafficMode() << " load=" << GetTrafficLoad()
                        << " period=" << m_dataGenerationPeriod.GetSeconds() << "s"
                        << " stop=" << m_dataStopTimeSec << "s)");
            m_dataGenerationEvt =
                Simulator::Schedule(dataStartDelay, &DvClApp::GenerateDataTraffic, this);
        }
    }
    else
    {
        NS_LOG_INFO("  >>> Node " << nodeId << " - No data generation (mode=" << GetTrafficMode()
                                  << " collectorNodeId=" << m_collectorNodeId << ")");
    }

    if (m_enableGapAuditTrace)
    {
        UpdateControlPlaneAuditState();
        const double dataStartSec = std::max(0.0, m_dataStartTimeSec);
        Simulator::Schedule(Seconds(dataStartSec),
                            &DvClApp::CaptureControlPlaneAuditSnapshot,
                            this);
        if (m_dataStopTimeSec > dataStartSec)
        {
            const double midpointSec = dataStartSec + 0.5 * (m_dataStopTimeSec - dataStartSec);
            Simulator::Schedule(Seconds(midpointSec),
                                &DvClApp::CaptureControlPlaneAuditMidpointSnapshot,
                                this);
            Simulator::Schedule(Seconds(m_dataStopTimeSec),
                                &DvClApp::CaptureControlPlaneAuditDataStopSnapshot,
                                this);
        }
    }
}

// Cancela eventos y reporta estadísticas cuando se detiene la app.
void
DvClApp::ForceFinalFlush()
{
    StopApplication(); // §LossFine: reuse StopApplication reporting on early stop
}

void
DvClApp::StopApplication()
{
    NS_LOG_INFO("StopApplication(): node=" << GetNode()->GetId());
    if (m_finalFlushed)
    {
        return;
    } // §LossFine
    m_finalFlushed = true;

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

    if (m_gapAuditEvt.IsPending())
    {
        Simulator::Cancel(m_gapAuditEvt);
    }

    if (!m_txQueue.empty())
    {
        const uint32_t myId = GetNode()->GetId();
        const bool nodeDeadAtStop = (GetEnergyFraction() <= 1e-6); // §LossFine
        for (const auto& entry : m_txQueue)
        {
            if (entry.tag.GetDst() == 0xFFFF)
            {
                continue;
            }
            if (entry.tag.GetSrc() != myId)
            {
                m_relayPendingEnd++; // §LossFine: in-transit stuck at relay
                continue;
            }

            // [B1] Paquete de origen mío aún pendiente al corte: se contabiliza
            // para poder calcular PDR efectivo = delivered / (generated - originPending).
            m_originPendingAtStop++;
            if (nodeDeadAtStop)
            {
                m_originPendingEnergy++;
            }
            else
            {
                m_originPendingDuty++;
            } // §LossFine

            std::string reason =
                entry.pendingReason.empty() ? "queue_pending_end" : entry.pendingReason;
            if (reason == "tx_attempt_air")
            {
                reason = "inflight_pending_end";
            }

            NS_LOG_INFO("FWDTRACE ORIGIN_PENDING_END time="
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

    AccountRxScanEnergyDelta();

    if (m_stats)
    {
        double dutyUsed = m_mac ? m_mac->GetDutyCycleUsed() : 0.0;
        m_stats->RecordDuty(GetNode()->GetId(), dutyUsed, m_txCount, m_backoffCount);
        double energyJ = GetRemainingEnergyJ();
        double energyFrac = GetEnergyFraction();
        m_stats->RecordEnergySnapshot(GetNode()->GetId(), energyJ, energyFrac);
        DvClNodeStats runtimeStats;
        runtimeStats.nodeId = GetNode()->GetId();
        runtimeStats.txQueueLenEnd = static_cast<uint32_t>(m_txQueue.size());
        runtimeStats.queuedPacketsEnd = static_cast<uint32_t>(m_txQueue.size());
        runtimeStats.cadBusyEvents = m_cadBusyEvents;
        runtimeStats.dutyBlockedEvents = m_dutyBlockedEvents;
        runtimeStats.dutyBlockedControl = m_controlDutyBlocked;
        runtimeStats.dutyBlockedData = m_dataDutyBlocked;
        runtimeStats.totalWaitTimeDueToDutyS = m_totalWaitTimeDueToDutySec;
        runtimeStats.dropNoRoute = m_dropNoRoute;
        runtimeStats.dropNoRouteSrc = m_dropNoRouteSrc;
        runtimeStats.dropNoRouteRelay = m_dropNoRouteRelay;
        runtimeStats.dropTtlExpired = m_dropTtlExpired;
        runtimeStats.dropQueueOverflow = m_dropQueueOverflow;
        runtimeStats.dropMaxCsmaRetries = m_dropMaxCsmaRetries;
        runtimeStats.dropBacktrack = m_dropBacktrack;
        runtimeStats.dropOther = m_dropOther;
        runtimeStats.beaconScheduled = m_beaconScheduled;
        runtimeStats.beaconTxSent = m_beaconTxSent;
        runtimeStats.dataTxSent = m_dataTxSent;
        runtimeStats.beaconBlockedByDuty = m_beaconBlockedByDuty;
        runtimeStats.beaconSupersededLatestOnly = m_beaconSupersededLatestOnly;
        runtimeStats.rpGapLargeEvents = m_rpGapLargeEvents;
        runtimeStats.cadBusyEventsLocalPower = m_mac ? m_mac->GetCadBusyEventsLocalPower() : 0;
        runtimeStats.cadBusyEventsOracle = m_mac ? m_mac->GetCadBusyEventsOracle() : 0;

        Ptr<DvClLoraNetDevice> meshDev = DynamicCast<DvClLoraNetDevice>(m_meshDevice);
        Ptr<ns3::lorawan::SimpleGatewayLoraPhy> phy =
            meshDev ? DynamicCast<ns3::lorawan::SimpleGatewayLoraPhy>(meshDev->GetPhy()) : nullptr;
        if (phy)
        {
            runtimeStats.rxScanAttempts = phy->GetRxScanAttempts();
            runtimeStats.rxScanLocks = phy->GetRxScanLocks();
            runtimeStats.rxScanMissBeforeLock = phy->GetRxScanMissBeforeLock();
            runtimeStats.rxPostLockInterferenceFail = phy->GetRxPostLockInterferenceFail();
            runtimeStats.rxNoMoreDemodDrops = phy->GetNoMoreDemodulatorsDrops();
            runtimeStats.rxScanTimeTotalS = phy->GetRxScanTimeTotalSeconds();
            runtimeStats.rxAcquisitionDelayMeanS = phy->GetRxAcquisitionDelayMeanSeconds();
            runtimeStats.rxAcquisitionDelayP95S = phy->GetRxAcquisitionDelayP95Seconds();
            const auto ifs = phy->GetInterferenceStats();
            runtimeStats.pueyoSameSfOverlapEvents = ifs.pueyoSameSfOverlapEvents;
            runtimeStats.pueyoDestructiveOverlapDrops = ifs.pueyoDestructiveOverlapDrops;
            runtimeStats.pueyoCaptureOrTimingSurvivals = ifs.pueyoCaptureOrTimingSurvivals;
            runtimeStats.pueyoCrossSfIgnoredOverlaps = ifs.pueyoCrossSfIgnoredOverlaps;
            runtimeStats.goursaudDeterministicDrops = ifs.goursaudDeterministicDrops;
            runtimeStats.goursaudCrossSfCaptureSuccesses = ifs.goursaudCrossSfCaptureSuccesses;
            runtimeStats.goursaudCrossSfCaptureFails = ifs.goursaudCrossSfCaptureFails;
        }

        runtimeStats.beaconRxOk = m_beaconRxOk;
        runtimeStats.dataCollisionDrops = m_dataCollisionDrops;
        runtimeStats.beaconCollisionDrops = m_beaconCollisionDrops;
        runtimeStats.dataBusyDrops = m_dataBusyDrops;
        runtimeStats.beaconBusyDrops = m_beaconBusyDrops;
        runtimeStats.firstUsableRouteTimeSec = m_firstUsableRouteTimeSec;
        runtimeStats.coverage80RouteTimeSec = m_coverage80RouteTimeSec;
        runtimeStats.routesAtDataStart = m_routesAtDataStart;
        runtimeStats.beaconTxAtDataStart = m_beaconTxAtDataStart;
        runtimeStats.beaconRxAtDataStart = m_beaconRxAtDataStart;
        runtimeStats.routesAtMidpoint = m_routesAtMidpoint;
        runtimeStats.beaconTxAtMidpoint = m_beaconTxAtMidpoint;
        runtimeStats.beaconRxAtMidpoint = m_beaconRxAtMidpoint;
        runtimeStats.routesAtDataStop = m_routesAtDataStop;
        runtimeStats.beaconTxAtDataStop = m_beaconTxAtDataStop;
        runtimeStats.beaconRxAtDataStop = m_beaconRxAtDataStop;
        runtimeStats.queuedPacketsAtDataStop = m_queuedPacketsAtDataStop;
        runtimeStats.originPendingAtStop = m_originPendingAtStop; // [B1]
        runtimeStats.originPendingDuty = m_originPendingDuty;     // §LossFine
        runtimeStats.originPendingEnergy = m_originPendingEnergy; // §LossFine
        runtimeStats.relayPendingEnd = m_relayPendingEnd;         // §LossFine
        runtimeStats.sfLinkSamples = m_sfLinkSamples;
        runtimeStats.sfLinkObservedMismatch = m_sfLinkObservedMismatch;
        if (m_routing)
        {
            runtimeStats.routesTotal = m_routing->GetRouteCount();
            runtimeStats.routesPrimaryTotal = m_routing->GetPrimaryRouteCount();
            runtimeStats.routesBackupTotal = m_routing->GetBackupRouteCount();
            runtimeStats.destinationsWithBackupTotal = m_routing->GetDestinationsWithBackupCount();
            runtimeStats.routeEvictionsTotal = m_routing->GetRouteEvictionsTotal();
            runtimeStats.backupPromotionsTotal = m_routing->GetBackupPromotionsTotal();

            const auto primary = m_routing->GetPrimaryRoutesSnapshot();
            for (const auto& r : primary)
            {
                m_stats->RecordQuantizationSample(GetNode()->GetId(),
                                                  r.destination,
                                                  r.nextHop,
                                                  false,
                                                  r.rawMetric,
                                                  r.scoreX100);
            }
            const auto backup = m_routing->GetBackupRoutesSnapshot();
            for (const auto& r : backup)
            {
                m_stats->RecordQuantizationSample(GetNode()->GetId(),
                                                  r.destination,
                                                  r.nextHop,
                                                  true,
                                                  r.rawMetric,
                                                  r.scoreX100);
            }
        }
        m_stats->RecordRuntimeNodeStats(runtimeStats);
    }

    PrintRoutingTable();
    if (m_routing)
    {
        m_routing->DebugDumpRoutingTable();
    }
}

void
DvClApp::Tick()
{
    NS_LOG_INFO("Tick(): node=" << GetNode()->GetId() << " t=" << Simulator::Now().GetSeconds()
                                << "s");

    BuildAndSendDv(m_sfControl);
    m_evt = Simulator::Schedule(m_period, &DvClApp::Tick, this);
}

void
DvClApp::AccountRxScanEnergyDelta()
{
    if (!m_energyModel || !GetNode())
    {
        return;
    }

    Ptr<DvClLoraNetDevice> meshDev = DynamicCast<DvClLoraNetDevice>(m_meshDevice);
    Ptr<ns3::lorawan::SimpleGatewayLoraPhy> phy =
        meshDev ? DynamicCast<ns3::lorawan::SimpleGatewayLoraPhy>(meshDev->GetPhy()) : nullptr;
    if (!phy)
    {
        return;
    }

    const double totalScanSec = phy->GetRxScanTimeTotalSeconds();
    if (totalScanSec <= m_lastAppliedRxScanTimeS)
    {
        return;
    }

    const double deltaSec = totalScanSec - m_lastAppliedRxScanTimeS;
    m_lastAppliedRxScanTimeS = totalScanSec;
    m_energyModel->UpdateCadEnergy(GetNode()->GetId(), deltaSec);
}

// Procesa cada recepción L2 y decide si actualizar rutas o reenviar.
bool
DvClApp::L2Receive(Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address& from)
{
    AccountRxScanEnergyDelta();

    NS_LOG_INFO("L2Receive ACTIVADA node=" << GetNode()->GetId() << " proto=" << proto
                                           << " kProtoMesh=" << kProtoMesh);
    if (proto != kProtoMesh)
    {
        return false;
    }

    return L2ReceiveWire(dev, p, proto, from);
}

// Realiza el forwarding cuando el TTL lo permite.
void
DvClApp::ForwardWithTtl(Ptr<const Packet> pIn, const DvClMetricTag& inTag)
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
        // [B5] Antes: solo contaba si myId == m_collectorNodeId (rompía PDR en
        // pueyo_all_to_all con múltiples destinos). Ahora cuenta siempre que
        // soy destino final, alineado con el path pueyo7b (mesh_dv_app.cc:4660+).
        {
            m_dataPacketsDelivered++;
            NS_LOG_INFO(">>> DATA DELIVERED: src=" << inTag.GetSrc() << " dst=" << inTag.GetDst()
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

            if (m_stats)
            {
                double txTime =
                    m_stats->GetFirstTxTime(inTag.GetSrc(), inTag.GetDst(), inTag.GetSeq());
                double delaySec = (txTime >= 0.0) ? (Simulator::Now().GetSeconds() - txTime) : -1.0;
                m_stats->RecordE2eDelay(inTag.GetSrc(),
                                        inTag.GetDst(),
                                        inTag.GetSeq(),
                                        inTag.GetHops(),
                                        delaySec,
                                        pIn->GetSize(),
                                        inTag.GetSf(),
                                        true);
                m_stats->RecordEnergySnapshot(myId, energyJ, energyFrac);
            }
            NS_LOG_INFO("FWDTRACE deliver time="
                        << Simulator::Now().GetSeconds() << " node=" << myId << " src="
                        << inTag.GetSrc() << " dst=" << inTag.GetDst() << " seq=" << inTag.GetSeq()
                        << " hops=" << unsigned(inTag.GetHops()) << " reason=dst_local");
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
        DvClMetricTag fwdTag = inTag;
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
    const RouteEntry* routePreview = m_routing ? m_routing->GetRoute(dst) : nullptr;
    NS_LOG_INFO("FWD_CHECK node" << myId << " dst=" << dst << " route="
                                 << (routePreview
                                         ? ("OK nextHop=" + std::to_string(routePreview->nextHop))
                                         : "NULL"));
    const RouteEntry* route = m_routing ? m_routing->GetRoute(dst) : nullptr;
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
        NS_LOG_INFO("FWDTRACE drop_noroute time=" << Simulator::Now().GetSeconds()
                                                  << " node=" << myId << " src=" << inTag.GetSrc()
                                                  << " dst=" << dst << " seq=" << inTag.GetSeq()
                                                  << " reason=no_route");
        DumpFullTable("DATA_NOROUTE_TX");
        return;
    }
    // Log detallado de la ruta usada
    NS_LOG_INFO("FWDTRACE route time="
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
        NS_LOG_INFO("FWDTRACE duty_defer time="
                    << Simulator::Now().GetSeconds() << " node=" << myId
                    << " src=" << inTag.GetSrc() << " dst=" << dst << " seq=" << inTag.GetSeq()
                    << " dutyUsed=" << m_mac->GetDutyCycleUsed()
                    << " dutyLimit=" << m_mac->GetDutyCycleLimit() << " reason=duty_wait_queue");
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
    const bool hasUsableMac =
        ResolveUnicastNextHopLinkAddr(route->nextHop, &routeMac, &usingStaleMac);
    NS_LOG_INFO("LINKADDR_CHECK node"
                << myId << " nextHop=" << route->nextHop << " linkAddrFound="
                << (m_linkAddrTable.find(route->nextHop) != m_linkAddrTable.end() ? "YES" : "NO")
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
                    << " collectorNodeId=" << m_collectorNodeId
                    << " reason=no_link_addr_for_unicast");
        NS_LOG_INFO("FWDTRACE DATA_NOROUTE time="
                    << Simulator::Now().GetSeconds() << " node=" << myId
                    << " src=" << inTag.GetSrc() << " dst=" << dst << " seq=" << inTag.GetSeq()
                    << " nextHop=" << route->nextHop << " reason=no_link_addr_for_unicast");
        DumpFullTable("DATA_NOROUTE_TX");
        return;
    }

    double txPowerDbm = -1.0;
    Ptr<DvClLoraNetDevice> meshDevTx = DynamicCast<DvClLoraNetDevice>(m_meshDevice);
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

    NS_LOG_INFO("FWDTRACE fwd time="
                << Simulator::Now().GetSeconds() << " node=" << myId << " src=" << inTag.GetSrc()
                << " dst=" << dst << " seq=" << inTag.GetSeq()
                << " ttl=" << unsigned(inTag.GetTtl()) << " ttlAfter=" << unsigned(inTag.GetTtl())
                << " nextHop=" << route->nextHop << " hopsPlanned=" << unsigned(route->hops)
                << " tx_mode=unicast"
                << " reason=ok");
    NS_LOG_INFO("DATA_TX SF" << unsigned(sfForRoute) << " node=" << myId << " src="
                             << inTag.GetSrc() << " dst=" << dst << " seq=" << inTag.GetSeq());

    // Snapshot de ruta usada en el momento del forward
    if (m_stats)
    {
        m_stats->RecordRouteUsed(myId,
                                 dst,
                                 route->nextHop,
                                 route->hops,
                                 route->scoreX100,
                                 route->seqNum);
    }

    Ptr<Packet> modifiablePkt = pIn->Copy();
    DvClMetricTag outTag = inTag;
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

    NS_LOG_INFO("Enviando con kProtoMesh=" << kProtoMesh);
    SendWithCSMA(modifiablePkt, outTag, dstAddr);
}

uint32_t
DvClApp::ComputeLoRaToAUs(uint8_t sf, uint32_t bw, uint8_t cr, uint32_t pl) const
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

    const double nPreamble = static_cast<double>(std::max<uint32_t>(6, m_preambleSymbols));
    const double tPreamble = (nPreamble + 4.25) * tSym;
    const double tPayload = paySym * tSym;
    const double tTot = tPreamble + tPayload;
    return static_cast<uint32_t>(tTot * 1e6 + 0.5);
}

uint16_t
DvClApp::ComputeScoreX100(const DvClMetricTag& t) const
{
    NodeId srcId = 0;
    Ptr<Node> node = GetNode();
    if (node)
    {
        srcId = node->GetId();
    }

    LinkStats stats;
    stats.toaUs = t.GetToaUs();
    stats.hops = t.GetHops();
    stats.sf = t.GetSf();
    // REMOVED: SNR/RSSI not used in metric - link quality embedded in SF selection
    stats.snrDb = 0.0;
    stats.batteryMv = t.GetBatt_mV();
    stats.energyFraction = GetEnergyFraction();

    LinkInputs metricIn;
    metricIn.toaUs = stats.toaUs;
    metricIn.sf = stats.sf;
    metricIn.energyFraction = ResolveEnergyFraction(stats.energyFraction, srcId);
    metricIn.batteryMv = stats.batteryMv;
    const double cost = m_routing->GetMetric()->ComputeLinkCost(metricIn);
    const double score = std::clamp(1.0 - cost, 0.0, 1.0);
    return static_cast<uint16_t>(std::round(score * 100.0));
}

// ========================================================================
// B3: Selección probabilística de SF según el paper original
// SF más bajos tienen mayor probabilidad (2x por cada nivel)
// Distribución aproximada: SF7=50%, SF8=25%, SF9=12.5%, etc.
// ========================================================================
uint8_t
DvClApp::SelectRandomSfProbabilistic() const
{
    NS_ASSERT(m_rng != nullptr);
    const uint8_t sfMin = std::min<uint8_t>(m_sfMin, m_sfMax);
    const uint8_t sfMax = std::max<uint8_t>(m_sfMin, m_sfMax);
    const uint8_t count = static_cast<uint8_t>(sfMax - sfMin + 1);
    if (count == 0)
    {
        return m_sfMin;
    }

    // Pueyo-like geometric PMF over [sfMin..sfMax]:
    // weight(sfMin + i) = 2^(count-1-i), normalized by (2^count - 1).
    // For SF7..SF12 => {32,16,8,4,2,1}/63.
    uint32_t totalWeight = 0;
    std::array<uint32_t, 6> weights{0, 0, 0, 0, 0, 0};
    for (uint8_t i = 0; i < count && i < weights.size(); ++i)
    {
        const uint8_t exp = static_cast<uint8_t>(count - 1 - i);
        weights[i] = static_cast<uint32_t>(1u << exp);
        totalWeight += weights[i];
    }
    if (totalWeight == 0)
    {
        return sfMax;
    }

    const uint32_t draw = m_rng->GetInteger(1, totalWeight);
    uint32_t accum = 0;
    for (uint8_t i = 0; i < count && i < weights.size(); ++i)
    {
        accum += weights[i];
        if (draw <= accum)
        {
            return static_cast<uint8_t>(sfMin + i);
        }
    }
    return sfMax;
}

void
DvClApp::PrintRoutingTable()
{
    double remainingEnergy = GetRemainingEnergyJ();
    double voltageAvg =
        (DvClEnergyRegistry::kDefaultVoltageMaxMv + DvClEnergyRegistry::kDefaultVoltageMinMv) /
        2000.0;
    double totalEnergy = (DvClEnergyRegistry::kDefaultCapacityMah / 1000.0) * voltageAvg * 3600.0;
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
DvClApp::PurgeExpiredRoutes()
{
    if (m_routing)
    {
        m_routing->PurgeExpiredRoutes();
    }
    m_purgeEvt = Simulator::Schedule(Seconds(30), &DvClApp::PurgeExpiredRoutes, this);
}

void
DvClApp::SendDataToDestination(uint32_t dst, Ptr<Packet> payload)
{
    (void)payload;
    SendDataPacketPueyo7b(dst);
}

Address
DvClApp::ResolveNextHopAddress(uint32_t nextHopId) const
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
DvClApp::IsLinkAddrFresh(uint32_t nextHopId) const
{
    auto it = m_linkAddrLastSeen.find(nextHopId);
    if (it == m_linkAddrLastSeen.end())
    {
        return false;
    }
    return (Simulator::Now() - it->second) <= m_linkAddrCacheWindow;
}

bool
DvClApp::ResolveUnicastNextHopLinkAddr(uint32_t nextHopId,
                                       Mac48Address* outMac,
                                       bool* outStale) const
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
DvClApp::TryGetBestRecentSf(const AppNeighborLink& link, Time now, uint8_t* outSf) const
{
    if (!outSf)
    {
        return false;
    }

    const uint8_t minSf =
        std::clamp<uint8_t>(m_sfMin, static_cast<uint8_t>(7), static_cast<uint8_t>(12));
    const uint8_t maxSf =
        std::clamp<uint8_t>(m_sfMax, static_cast<uint8_t>(7), static_cast<uint8_t>(12));

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

uint8_t
DvClApp::ComputeMinSfBySensitivity(double rxPowerDbm) const
{
    // SX1276 sensitivity table at 125 kHz BW for SF7..SF12.
    // §sens-fix: alineada con EndDeviceLoraPhy::sensitivity (PHY de recepcion real)
    // y con la Tabla 3 de Pueyo-Centelles 2024. Antes estaba 1 dB desalineada del PHY.
    static const double kSensitivityDbm[6] = {-124.0, -127.0, -130.0, -133.0, -135.0, -137.0};
    const double margin = m_sfLinkMarginDb;
    for (uint8_t sf = 7; sf <= 12; ++sf)
    {
        const double required = kSensitivityDbm[sf - 7] + margin;
        if (rxPowerDbm >= required)
        {
            return sf;
        }
    }
    return 12;
}

uint8_t
DvClApp::ResolveSfForLink(uint8_t observedSf, double rxPowerDbm) const
{
    const uint8_t obs =
        std::clamp<uint8_t>(observedSf, static_cast<uint8_t>(7), static_cast<uint8_t>(12));
    if (m_sfLinkMode != SfLinkMode::DETERMINISTIC_SENSITIVITY)
    {
        return obs;
    }

    const uint8_t det = ComputeMinSfBySensitivity(rxPowerDbm);
    return std::clamp<uint8_t>(det, static_cast<uint8_t>(m_sfMin), static_cast<uint8_t>(m_sfMax));
}

// ========================================================================
// SF EMPÍRICO: Basado en historial de beacons exitosos (según paper)
// ========================================================================
void
DvClApp::UpdateNeighborLinkSf(uint32_t neighborId, uint8_t rxSf)
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
        NS_LOG_DEBUG("Node " << GetNode()->GetId()
                             << " UpdateNeighborLinkSf: neighbor=" << neighborId
                             << " learned initial empirical SF=" << unsigned(newBestSf));
    }
    else if (hadPreviousBest && hasNewBest && newBestSf != previousBestSf)
    {
        NS_LOG_DEBUG("Node " << GetNode()->GetId() << " UpdateNeighborLinkSf: neighbor="
                             << neighborId << " empirical SF changed from "
                             << unsigned(previousBestSf) << " to " << unsigned(newBestSf));
    }
    else if (!hasNewBest)
    {
        NS_LOG_DEBUG("Node " << GetNode()->GetId() << " UpdateNeighborLinkSf: neighbor="
                             << neighborId << " no robust SF yet (mode=" << m_empiricalSfSelectMode
                             << " minSamples=" << m_empiricalSfMinSamples << ")");
    }
}

uint8_t
DvClApp::GetDataSfForNeighbor(uint32_t nextHopId) const
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
        NS_LOG_DEBUG(
            "Node " << GetNode()->GetId() << " GetDataSfForNeighbor: neighbor=" << nextHopId
                    << " no recent SF in window=" << m_neighborLinkTimeout.GetSeconds() << "s"
                    << ", lastRxSf=" << unsigned(it->second.lastRxSf) << ", using SF12");
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
DvClApp::ValidateRoute(uint32_t dst) const
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
DvClApp::ScheduleDvCycle(uint8_t sf, EventId* evtSlot)
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
        NS_LOG_INFO("BEACON_PHASE node" << GetNode()->GetId() << " sf=" << unsigned(sf)
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
DvClApp::GetBeaconInterval() const
{
    Time now = Simulator::Now();
    return (now < m_beaconWarmupEnd) ? m_beaconIntervalWarm : m_beaconIntervalStable;
}

void
DvClApp::SendInitialDv()
{
    // B3: usar SF probabilístico si está habilitado
    uint8_t beaconSf = m_useProbabilisticSf ? SelectRandomSfProbabilistic() : m_sfControl;
    BuildAndSendDv(beaconSf);
    ScheduleDvCycle(m_sfControl, &m_evtSf12); // FIX A2: pasar puntero
}

void
DvClApp::SchedulePeriodicDump()
{
    uint32_t nodeId = GetNode()->GetId();
    if (nodeId < 3 && m_routing)
    {
        NS_LOG_INFO("DV_DUMP_PERIODIC node" << nodeId << " t=" << Simulator::Now().GetSeconds());
        DumpFullTable("PERIODIC_30s");
    }
    m_periodicDumpEvt = Simulator::Schedule(Seconds(30), &DvClApp::SchedulePeriodicDump, this);
}

void
DvClApp::DumpRoute(uint32_t dst, const std::string& tag)
{
    if (!m_routing)
    {
        return;
    }
    const RouteEntry* r = m_routing->GetRoute(dst);
    if (r)
    {
        NS_LOG_INFO("DV_SNAPSHOT tag=" << tag << " node" << GetNode()->GetId()
                                       << " t=" << Simulator::Now().GetSeconds() << " dst=" << dst
                                       << " nextHop=" << r->nextHop << " hops=" << unsigned(r->hops)
                                       << " score=" << r->scoreX100 << " seq=" << r->seqNum);
    }
    else
    {
        NS_LOG_INFO("DV_SNAPSHOT tag=" << tag << " node" << GetNode()->GetId()
                                       << " t=" << Simulator::Now().GetSeconds() << " dst=" << dst
                                       << " route=NULL");
    }
}

void
DvClApp::DumpFullTable(const std::string& tag) const
{
    if (!m_routing)
    {
        return;
    }
    uint32_t nodeId = GetNode() ? GetNode()->GetId() : 0;
    NS_LOG_INFO("DV_TABLE_FULL tag=" << tag << " node" << nodeId
                                     << " t=" << Simulator::Now().GetSeconds());
    m_routing->DebugDumpRoutingTable();

    const RouteEntry* gw = m_routing->GetRoute(m_collectorNodeId);
    if (gw)
    {
        NS_LOG_INFO("DV_TABLE_FULL_GW node"
                    << nodeId << " dst=" << m_collectorNodeId << " nextHop=" << gw->nextHop
                    << " hops=" << unsigned(gw->hops) << " score=" << gw->scoreX100
                    << " seq=" << gw->seqNum
                    << " age=" << (Simulator::Now() - gw->lastUpdate).GetSeconds() << "s");
    }
    else
    {
        NS_LOG_INFO("DV_TABLE_FULL_GW node" << nodeId << " dst=" << m_collectorNodeId
                                            << " route=NULL");
    }
}

std::string
DvClApp::GetBeaconPhaseLabel() const
{
    std::ostringstream oss;
    const bool warm = (Simulator::Now() < m_beaconWarmupEnd);
    const double intervalSec =
        warm ? m_beaconIntervalWarm.GetSeconds() : m_beaconIntervalStable.GetSeconds();
    oss << (warm ? "warmup(" : "stable(") << intervalSec << "s)";
    return oss.str();
}

void
DvClApp::RecordBeaconScheduled(uint32_t seq)
{
    m_beaconScheduled++;
    m_beaconScheduledAtBySeq[seq] = Simulator::Now();
}

void
DvClApp::RecordBeaconTxSent(uint32_t seq)
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
            if (m_stats)
            {
                m_stats->RecordBeaconDelay(delaySec);
            }
        }
        m_beaconScheduledAtBySeq.erase(it);
    }
}

void
DvClApp::CaptureControlPlaneAuditSnapshot()
{
    m_routesAtDataStart = m_routing ? m_routing->GetRouteCount() : 0;
    m_beaconTxAtDataStart = m_beaconTxSent;
    m_beaconRxAtDataStart = m_beaconRxOk;
    UpdateControlPlaneAuditState();
}

void
DvClApp::CaptureControlPlaneAuditMidpointSnapshot()
{
    m_routesAtMidpoint = m_routing ? m_routing->GetRouteCount() : 0;
    m_beaconTxAtMidpoint = m_beaconTxSent;
    m_beaconRxAtMidpoint = m_beaconRxOk;
    UpdateControlPlaneAuditState();
}

void
DvClApp::CaptureControlPlaneAuditDataStopSnapshot()
{
    m_routesAtDataStop = m_routing ? m_routing->GetRouteCount() : 0;
    m_beaconTxAtDataStop = m_beaconTxSent;
    m_beaconRxAtDataStop = m_beaconRxOk;
    m_queuedPacketsAtDataStop = static_cast<uint64_t>(m_txQueue.size());
    UpdateControlPlaneAuditState();
}

void
DvClApp::UpdateControlPlaneAuditState()
{
    if (!m_enableGapAuditTrace)
    {
        return;
    }

    const uint64_t routeCount = m_routing ? m_routing->GetRouteCount() : 0;
    const double nowSec = Simulator::Now().GetSeconds();
    if (m_firstUsableRouteTimeSec < 0.0 && routeCount > 0)
    {
        m_firstUsableRouteTimeSec = nowSec;
    }

    const uint32_t totalNodes = NodeList::GetNNodes();
    const uint64_t targetRoutes =
        totalNodes > 1 ? static_cast<uint64_t>(std::ceil(0.8 * static_cast<double>(totalNodes - 1)))
                       : 0u;
    if (targetRoutes > 0 && m_coverage80RouteTimeSec < 0.0 && routeCount >= targetRoutes)
    {
        m_coverage80RouteTimeSec = nowSec;
    }

    m_gapAuditEvt = Simulator::Schedule(Seconds(1), &DvClApp::UpdateControlPlaneAuditState, this);
}

double
DvClApp::ComputeBeaconDelayP95() const
{
    return Percentile95Double(m_beaconDelaySamplesSec);
}

bool
DvClApp::IsControlQueueEntry(const TxQueueEntry& entry) const
{
    return entry.tag.GetDst() == 0xFFFF;
}

bool
DvClApp::IsForwardedDataQueueEntry(const TxQueueEntry& entry) const
{
    Ptr<Node> node = GetNode();
    const uint32_t myId = node ? node->GetId() : 0;
    return (entry.tag.GetDst() != 0xFFFF) && (entry.tag.GetSrc() != myId);
}

void
DvClApp::ResetStrictRoutingDataBudgets()
{
    m_strictRoutingBudgetRemaining = 10;
    m_strictDataBudgetRemaining = 1;
}

void
DvClApp::ResetStrictForwardLocalBudgets()
{
    m_strictForwardBudgetRemaining = 10;
    m_strictLocalBudgetRemaining = 1;
}

void
DvClApp::RotateQueueEntryToFront(std::size_t idx)
{
    if (idx == 0 || idx >= m_txQueue.size())
    {
        return;
    }
    auto it = m_txQueue.begin();
    std::advance(it, static_cast<std::ptrdiff_t>(idx));
    TxQueueEntry selected = *it;
    m_txQueue.erase(it);
    m_txQueue.push_front(selected);
}

bool
DvClApp::SelectStrictQueueHead(std::size_t* outIndex)
{
    if (!outIndex || m_txQueue.empty())
    {
        return false;
    }
    auto findFirst = [&](auto&& pred) -> std::size_t {
        std::size_t i = 0;
        for (const auto& e : m_txQueue)
        {
            if (pred(e))
            {
                return i;
            }
            ++i;
        }
        return m_txQueue.size();
    };

    const std::size_t firstControl =
        findFirst([&](const TxQueueEntry& e) { return IsControlQueueEntry(e); });
    const std::size_t firstData =
        findFirst([&](const TxQueueEntry& e) { return !IsControlQueueEntry(e); });
    const bool hasControl = (firstControl < m_txQueue.size());
    const bool hasData = (firstData < m_txQueue.size());

    if (!hasControl && !hasData)
    {
        return false;
    }

    if (m_strictRoutingBudgetRemaining == 0 && m_strictDataBudgetRemaining == 0)
    {
        ResetStrictRoutingDataBudgets();
    }

    bool pickControl = false;
    if (hasControl && m_strictRoutingBudgetRemaining > 0)
    {
        pickControl = true;
    }
    else if (!hasData && hasControl)
    {
        pickControl = true;
    }

    if (pickControl)
    {
        m_strictRoutingBudgetRemaining =
            (m_strictRoutingBudgetRemaining > 0)
                ? static_cast<uint8_t>(m_strictRoutingBudgetRemaining - 1)
                : 0;
        *outIndex = firstControl;
        return true;
    }

    if (hasData)
    {
        m_strictDataBudgetRemaining = (m_strictDataBudgetRemaining > 0)
                                          ? static_cast<uint8_t>(m_strictDataBudgetRemaining - 1)
                                          : 0;

        if (m_strictForwardBudgetRemaining == 0 && m_strictLocalBudgetRemaining == 0)
        {
            ResetStrictForwardLocalBudgets();
        }

        const std::size_t firstForward =
            findFirst([&](const TxQueueEntry& e) { return IsForwardedDataQueueEntry(e); });
        const std::size_t firstLocal = findFirst([&](const TxQueueEntry& e) {
            return (!IsControlQueueEntry(e) && !IsForwardedDataQueueEntry(e));
        });
        const bool hasForward = (firstForward < m_txQueue.size());
        const bool hasLocal = (firstLocal < m_txQueue.size());

        if (hasForward && m_strictForwardBudgetRemaining > 0)
        {
            m_strictForwardBudgetRemaining =
                static_cast<uint8_t>(m_strictForwardBudgetRemaining - 1);
            *outIndex = firstForward;
            return true;
        }
        if (hasLocal && m_strictLocalBudgetRemaining > 0)
        {
            m_strictLocalBudgetRemaining = static_cast<uint8_t>(m_strictLocalBudgetRemaining - 1);
            *outIndex = firstLocal;
            return true;
        }
        if (hasForward)
        {
            *outIndex = firstForward;
            return true;
        }
        if (hasLocal)
        {
            *outIndex = firstLocal;
            return true;
        }
    }

    // Fallback robusto.
    *outIndex = hasControl ? firstControl : firstData;
    return true;
}

// Encola o envía directamente según si CSMA está habilitado.
void
DvClApp::SendWithCSMA(Ptr<Packet> packet,
                      const DvClMetricTag& tag,
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

    // CSMA/CAD queue cap: if full, drop the oldest non-beacon entry (preserve control plane).
    // The in-air head (m_txBusy + pendingReason==tx_attempt_air) is never evicted.
    if (m_txQueue.size() >= m_csmaTxQueueMax)
    {
        bool evicted = false;
        for (auto it = m_txQueue.begin(); it != m_txQueue.end(); ++it)
        {
            const bool isBeacon = (it->tag.GetDst() == 0xFFFF);
            const bool isInAirHead =
                (it == m_txQueue.begin() && m_txBusy && it->pendingReason == "tx_attempt_air");
            if (!isBeacon && !isInAirHead)
            {
                NS_LOG_WARN("CSMA: tx queue cap reached ("
                            << m_txQueue.size()
                            << "), dropping oldest data entry seq=" << it->tag.GetSeq());
                m_txQueue.erase(it);
                m_dropQueueOverflow++;
                evicted = true;
                break;
            }
        }
        if (!evicted)
        {
            // Only beacons or in-air head — drop the incoming packet itself.
            NS_LOG_WARN("CSMA: tx queue cap reached ("
                        << m_txQueue.size()
                        << "), no evictable entry; dropping incoming packet seq="
                        << entry.tag.GetSeq());
            m_dropQueueOverflow++;
            return;
        }
    }

    const bool strictSchedulerActive = m_usePueyoStrictQueueScheduler;

    if (strictSchedulerActive)
    {
        // Strict profile: preserve arrival order and let scheduler choose dequeue class.
        m_txQueue.push_back(entry);
    }
    else if (entry.tag.GetDst() != 0xFFFF)
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
        if (m_beaconLatestOnly)
        {
            uint64_t superseded = 0;
            for (auto it = m_txQueue.begin(); it != m_txQueue.end();)
            {
                const bool isBeacon = (it->tag.GetDst() == 0xFFFF);
                const bool isInAirHead =
                    (it == m_txQueue.begin() && m_txBusy && it->pendingReason == "tx_attempt_air");
                if (isBeacon && !isInAirHead)
                {
                    it = m_txQueue.erase(it);
                    superseded++;
                    continue;
                }
                ++it;
            }
            m_beaconSupersededLatestOnly += superseded;
            if (superseded > 0)
            {
                NS_LOG_INFO("CSMA: latest-only beacon replaced " << superseded
                                                                 << " pending beacons");
            }
        }
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
    NS_LOG_INFO("FWDTRACE QUEUE_ENQUEUE time="
                << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                << " src=" << entry.tag.GetSrc() << " dst=" << entry.tag.GetDst()
                << " seq=" << entry.tag.GetSeq() << " sf=" << unsigned(entry.tag.GetSf())
                << " reason=" << entry.pendingReason << " queueSize=" << m_txQueue.size());

    ProcessTxQueue();
}

// Ejecuta detección de canal y transmisión efectiva de la cola CSMA.
void
DvClApp::ProcessTxQueue()
{
    AccountRxScanEnergyDelta();

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
            NS_LOG_INFO("FWDTRACE QUEUE_STATE time="
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

    if (m_usePueyoStrictQueueScheduler && m_txQueue.size() > 1)
    {
        std::size_t strictIdx = 0;
        if (SelectStrictQueueHead(&strictIdx))
        {
            RotateQueueEntryToFront(strictIdx);
        }
    }
    else
    {
        // Legacy anti-starvation selector.
        std::deque<TxQueueEntry>::iterator entryIt;
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

        static std::map<uint32_t, uint32_t> s_consecutiveBeacons;
        uint32_t myId = GetNode()->GetId();

        if (firstData != m_txQueue.end() && s_consecutiveBeacons[myId] >= 1)
        {
            entryIt = firstData;
            s_consecutiveBeacons[myId] = 0;
        }
        else if (firstBeacon != m_txQueue.end())
        {
            entryIt = firstBeacon;
            s_consecutiveBeacons[myId]++;
        }
        else
        {
            entryIt = m_txQueue.begin();
            s_consecutiveBeacons[myId] = 0;
        }

        if (entryIt != m_txQueue.begin())
        {
            TxQueueEntry entryToMove = *entryIt;
            m_txQueue.erase(entryIt);
            m_txQueue.push_front(entryToMove);
        }
    }

    TxQueueEntry& entry = m_txQueue.front();
    Time studySuperframeWait = Seconds(0);
    std::string studySuperframeReason;
    if (ComputeStudySuperframeWait(entry, &studySuperframeWait, &studySuperframeReason))
    {
        setPendingReason(entry, studySuperframeReason.c_str());
        entry.deferCount++;
        if (!m_backoffEvt.IsPending())
        {
            m_backoffEvt = Simulator::Schedule(studySuperframeWait, &DvClApp::ProcessTxQueue, this);
        }
        return;
    }
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
                    m_backoffEvt = Simulator::Schedule(wait, &DvClApp::ProcessTxQueue, this);
                }
                return;
            }
        }
    }
    if (m_mac)
    {
        m_mac->SetCadContext(entry.tag.GetSf(), m_bw);
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

        // CSMA/CAD retry cap: bound per-packet retries on sustained busy channel.
        entry.retries++;
        if (entry.retries > m_csmaMaxRetries)
        {
            NS_LOG_WARN("CSMA: max retries ("
                        << m_csmaMaxRetries << ") exceeded, dropping seq=" << entry.tag.GetSeq()
                        << " dst=0x" << std::hex << entry.tag.GetDst() << std::dec);
            m_dropMaxCsmaRetries++;
            m_txQueue.pop_front();
            ProcessTxQueue();
            return;
        }
        uint32_t backoffSlots = m_mac ? m_mac->GetBackoffSlots() : m_rng->GetInteger(0, 63);
        const bool isControl = (entry.tag.GetDst() == 0xFFFF);
        const double factor = isControl ? m_controlBackoffFactor : m_dataBackoffFactor;
        const uint32_t scaledSlots =
            (factor <= 0.0)
                ? 0u
                : std::max<uint32_t>(1, static_cast<uint32_t>(std::ceil(backoffSlots * factor)));
        Time cadDuration = m_mac ? m_mac->GetCadDuration() : m_cadDuration;
        Time backoffTime = (scaledSlots == 0u) ? MicroSeconds(1) : (cadDuration * scaledSlots);

        NS_LOG_INFO("CSMA: Backoff " << backoffSlots << " slots (" << backoffTime.GetMilliSeconds()
                                     << "ms)"
                                     << " factor=" << factor << " scaledSlots=" << scaledSlots
                                     << " type=" << (isControl ? "ctrl" : "data"));
        setPendingReason(entry, "cad_busy_backoff");
        entry.deferCount++;

        m_backoffEvt = Simulator::Schedule(backoffTime, &DvClApp::ProcessTxQueue, this);
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

        // Spend the airtime the radio will actually consume, not the
        // application's own estimate of it: the two are computed by different
        // code paths and disagree (see VALIDATION.md), which let the rolling
        // duty window overshoot the limit.
        double gateToaSec = entry.tag.GetToaUs() / 1e6;
        if (auto gateDev = DynamicCast<DvClLoraNetDevice>(dev))
        {
            gateToaSec = gateDev->GetOnAirTimeFor(entry.packet, entry.tag.GetSf()).GetSeconds();
        }
        if (m_mac && !m_mac->CanTransmitNow(gateToaSec))
        {
            NS_LOG_WARN("CSMA: Duty check failed at dequeue, deferring packet");
            setPendingReason(entry, "duty_wait_queue");
            entry.deferCount++;
            m_dutyBlockedEvents++;
            if (entry.tag.GetDst() == 0xFFFF)
            {
                m_controlDutyBlocked++;
                m_beaconBlockedByDuty++;
            }
            else
            {
                m_dataDutyBlocked++;
            }
            m_totalWaitTimeDueToDutySec += 1.0;
            if (!m_backoffEvt.IsPending())
            {
                m_backoffEvt = Simulator::Schedule(Seconds(1), &DvClApp::ProcessTxQueue, this);
            }
            return;
        }

        setPendingReason(entry, "tx_attempt_air");
        m_txBusy = true;

        Ptr<DvClLoraNetDevice> meshDev = DynamicCast<DvClLoraNetDevice>(dev);
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

        if (entry.tag.GetDst() == 0xFFFF)
        {
            if (!entry.beaconRpCounterAssigned)
            {
                entry.beaconRpCounter = static_cast<uint8_t>(m_beaconRpCounterTx & 0x3F);
                entry.beaconRpCounterAssigned = true;
            }
            DvClBeaconHeader beaconHdr;
            Ptr<Packet> beaconPayload;
            const bool parsedBeacon = ParseBeaconWirePacketPueyo(p, &beaconHdr, &beaconPayload);
            if (parsedBeacon)
            {
                beaconHdr.SetFlagsTtl(PackFlagsTtl(DvClPacketType::BEACON, entry.beaconRpCounter));
                Ptr<Packet> rebuilt = beaconPayload->Copy();
                // The header parsed off the air is the header re-emitted: no
                // rebuild into a second class, which is what used to shift the
                // DV entries (see dv-cl-wire.h).
                rebuilt->AddHeader(beaconHdr);
                p = rebuilt;
            }
            else
            {
                NS_LOG_ERROR("CSMA: invalid beacon packet while assigning rp_counter; dropping");
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
        NS_LOG_INFO("CSMA_TX_QUEUE_OUT node=" << GetNode()->GetId()
                                              << " tagDst=" << entry.tag.GetDst()
                                              << " tagSrc=" << entry.tag.GetSrc() << " ok=" << ok
                                              << " sf=" << unsigned(entry.tag.GetSf()));
        if (m_mac)
        {
        }

        NS_LOG_INFO("CSMA: TX ok=" << ok);
        if (ok)
        {
            OnPacketTransmitted(entry.tag.GetToaUs());
            if (entry.tag.GetDst() == 0xFFFF)
            {
                if (entry.beaconRpCounterAssigned)
                {
                    m_beaconRpCounterTx = static_cast<uint8_t>((entry.beaconRpCounter + 1) & 0x3F);
                    const char* traceLabel = "DVTRACE_TX_PUEYO_AIR";
                    NS_LOG_INFO(traceLabel << " time=" << Simulator::Now().GetSeconds() << " node="
                                           << GetNode()->GetId() << " seq=" << entry.tag.GetSeq()
                                           << " rp_counter=" << unsigned(entry.beaconRpCounter)
                                           << " bytes=" << p->GetSize());
                    if (m_pueyoValidationTrace)
                    {
                        std::vector<uint8_t> raw(p->GetSize());
                        p->CopyData(raw.data(), raw.size());
                        NS_LOG_INFO("PUEYO_VAL_TX_AIR node=" << GetNode()->GetId() << " packet_hex="
                                                             << BytesToHex(raw.data(), raw.size()));
                    }
                }
                RecordBeaconTxSent(entry.tag.GetSeq());
            }
            else
            {
                m_dataTxSent++;
            }
            if (entry.logTxMetrics)
            {
                const double energyJ = GetRemainingEnergyJ();
                const double energyFrac = GetEnergyFraction();
                LogTxEvent(entry.tag.GetSrc(),
                           entry.tag.GetSeq(),
                           entry.tag.GetDst(),
                           entry.tag.GetTtl(),
                           entry.tag.GetHops(),
                           entry.tag.GetBatt_mV(),
                           entry.tag.GetScoreX100(),
                           entry.tag.GetSf(),
                           entry.tag.GetToaUs(),
                           energyJ,
                           energyFrac,
                           true);
            }
            // Registrar overhead (beacon vs data)
            if (m_stats)
            {
                std::string kind = (entry.tag.GetDst() == 0xFFFF) ? "beacon" : "data";
                m_stats->RecordOverhead(GetNode()->GetId(),
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
DvClApp::OnPacketTransmitted(uint32_t toaUs)
{
    NS_LOG_INFO("OnPacketTransmitted(): node=" << GetNode()->GetId() << " toaUs=" << toaUs);
    m_txCount++;
    double duty = m_mac ? m_mac->GetDutyCycleUsed() : 0.0;
    NS_LOG_INFO("Duty cycle actualizado=" << (duty * 100.0) << "%");
}

// Maneja el temporizador de backoff cuando expira.
void
DvClApp::OnBackoffTimer()
{
    NS_LOG_DEBUG("CSMA: Backoff timer expirado");
    ProcessTxQueue();
}

int16_t
DvClApp::GetRealRSSI() const
{
    Ptr<Node> node = GetNode();

    // Obtener el NetDevice
    Ptr<NetDevice> dev = node->GetDevice(0);

    // Usar namespace completo: DvClLoraNetDevice
    Ptr<DvClLoraNetDevice> meshDev = DynamicCast<DvClLoraNetDevice>(dev);

    if (!meshDev)
    {
        NS_LOG_WARN("No DvClLoraNetDevice found, usando RSSI por defecto");
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
DvClApp::GetBatteryVoltageMv() const
{
    const double frac = GetEnergyFraction();
    if (frac >= 0.0)
    {
        return EnergyFractionToBatteryMv(frac);
    }
    return static_cast<uint16_t>(kBatteryMvMin);
}

double
DvClApp::GetRemainingEnergyJ() const
{
    if (m_energyModel)
    {
        return m_energyModel->GetRemainingEnergy(GetNode()->GetId());
    }
    return -1.0;
}

double
DvClApp::GetEnergyFraction() const
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
DvClApp::HandleRouteChange(const RouteEntry& entry, const std::string& action)
{
    bool significant = false;
    auto it = m_lastRouteSnapshot.find(entry.destination);
    if (it == m_lastRouteSnapshot.end())
    {
        significant = true;
    }
    else
    {
        const RouteEntry& prev = it->second;
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
    if (m_stats && action != "NONE")
    {
        m_stats->RecordRoute(GetNode()->GetId(),
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
            m_stats->RecordConnectivity(GetNode()->GetId(), m_collectorNodeId, hasRoute);
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
DvClApp::HandleFloodRequest(const DvMessage& msg)
{
    if (msg.entries.empty())
    {
        return;
    }

    NS_LOG_DEBUG("FloodDvUpdate requested for node=" << GetNode()->GetId()
                                                     << " seq=" << msg.sequence
                                                     << " entries=" << msg.entries.size());

    // Serializar entries para el payload
    std::vector<DvClMetricTag::RoutePayloadEntry> payloadEntries;
    payloadEntries.reserve(msg.entries.size());
    for (const auto& entry : msg.entries)
    {
        DvClMetricTag::RoutePayloadEntry plEntry;
        plEntry.dst = static_cast<uint16_t>(entry.destination);
        plEntry.hops = entry.hops;
        plEntry.sf = entry.sf;
        plEntry.score = entry.scoreX100;
        // REMOVED: plEntry.rssi_dBm
        plEntry.batt_mV = entry.batt_mV;
        payloadEntries.push_back(plEntry);
    }

    size_t len = payloadEntries.size() * DvClMetricTag::GetRoutePayloadEntrySize();
    uint32_t payloadSizeBytes = std::max<uint32_t>(len, 13);
    std::vector<uint8_t> buffer(payloadSizeBytes, 0);
    DvClMetricTag::SerializeRoutePayload(payloadEntries, buffer.data(), buffer.size());

    Ptr<Packet> p = Create<Packet>(buffer.data(), buffer.size());

    // Configurar Tag
    DvClMetricTag tag;
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
        NS_LOG_INFO("FWDTRACE duty_defer time="
                    << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                    << " src=" << tag.GetSrc() << " dst=" << tag.GetDst() << " seq=" << tag.GetSeq()
                    << " dutyUsed=" << m_mac->GetDutyCycleUsed()
                    << " dutyLimit=" << m_mac->GetDutyCycleLimit() << " reason=duty_wait_queue");
    }

    // REMOVED: tag.SetRssiDbm - no se serializa
    tag.SetBatt_mV(GetBatteryVoltageMv());
    tag.SetScoreX100(0); // Score del paquete en sí irrelevante, importa el payload

    p->AddPacketTag(tag);

    NS_LOG_INFO("POISON_TX node=" << GetNode()->GetId() << " entries=" << msg.entries.size()
                                  << " seq=" << msg.sequence);

    SendWithCSMA(p, tag, Address());
}

NeighborLinkInfo
DvClApp::BuildNeighborLinkInfo(const DvClMetricTag& tag,
                               uint32_t toaUs,
                               Mac48Address fromMac,
                               uint8_t linkSf) const
{
    NeighborLinkInfo link;
    link.neighbor = tag.GetSrc();
    link.sequence = tag.GetSeq();
    link.hops = std::min<uint8_t>(static_cast<uint8_t>(tag.GetHops() + 1), m_initTtl);
    link.sf = std::clamp<uint8_t>(linkSf, static_cast<uint8_t>(7), static_cast<uint8_t>(12));
    link.toaUs = toaUs;
    // REMOVED: link.rssiDbm - receptor obtiene de PHY
    link.batt_mV = tag.GetBatt_mV();
    link.dc_remaining = tag.GetDcRemaining(); // §DC-aware: DC del vecino emisor
    LinkStats stats;
    stats.toaUs = toaUs;
    stats.hops = link.hops;
    stats.sf = link.sf;
    // REMOVED: SNR/RSSI not used in metric - link quality embedded in SF
    stats.snrDb = 0.0;
    stats.batteryMv = link.batt_mV;
    stats.energyFraction = BatteryMvToEnergyFraction(link.batt_mV);
    LinkInputs metricIn;
    metricIn.toaUs = stats.toaUs;
    metricIn.sf = stats.sf;
    metricIn.energyFraction = ResolveEnergyFraction(stats.energyFraction, GetNode()->GetId());
    metricIn.batteryMv = stats.batteryMv;
    const double cost = m_routing->GetMetric()->ComputeLinkCost(metricIn);
    link.scoreX100 = static_cast<uint16_t>(std::round(std::clamp(1.0 - cost, 0.0, 1.0) * 100.0));
    link.mac = fromMac;
    return link;
}

std::vector<DvEntry>
DvClApp::DecodeDvEntries(Ptr<const Packet> p,
                         const DvClMetricTag& tag,
                         uint32_t toaUsNeighbor) const
{
    std::vector<DvEntry> entries;
    const size_t payloadLen = p->GetSize();
    const size_t entrySize = DvClMetricTag::GetRoutePayloadEntrySize();

    if (entrySize == 0 || payloadLen < entrySize)
    {
        if (payloadLen > 0)
        {
            NS_LOG_DEBUG("Beacon payload demasiado pequeño (" << payloadLen
                                                              << " bytes), sin rutas que procesar");
            NS_LOG_INFO("DVTRACE_RX_PARSE time=" << Simulator::Now().GetSeconds() << " node="
                                                 << GetNode()->GetId() << " src=" << tag.GetSrc()
                                                 << " seq=" << tag.GetSeq() << " total=0"
                                                 << " accepted=0"
                                                 << " drop_small=1"
                                                 << " payloadBytes=" << payloadLen);
        }
        return entries;
    }

    std::vector<DvClMetricTag::RoutePayloadEntry> receivedRoutes;
    std::vector<uint8_t> buffer(payloadLen);
    p->CopyData(buffer.data(), payloadLen);
    DvClMetricTag::DeserializeRoutePayload(buffer.data(), buffer.size(), receivedRoutes);

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

        uint8_t totalHops = route.hops; // DvClRouting::UpdateFromDvMsg adds +1
        if (totalHops >= m_initTtl)
        {
            NS_LOG_DEBUG("Descartando ruta por hops excesivos (" << unsigned(totalHops)
                                                                 << " > TTL)");
            dropTtl++;
            continue;
        }

        DvEntry entry;
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
    NS_LOG_INFO("DVTRACE_RX_PARSE time="
                << Simulator::Now().GetSeconds() << " node=" << GetNode()->GetId()
                << " src=" << tag.GetSrc() << " seq=" << tag.GetSeq() << " total=" << total
                << " accepted=" << accepted << " drop_self=" << dropSelf << " drop_ttl=" << dropTtl
                << " payloadBytes=" << payloadLen);
    return entries;
}

void
DvClApp::ProcessDvPayload(Ptr<const Packet> p,
                          const DvClMetricTag& tag,
                          const Mac48Address& fromMac,
                          uint32_t toaUsNeighbor)
{
    if (!m_routing)
    {
        return;
    }

    Ptr<DvClLoraNetDevice> meshDev = DynamicCast<DvClLoraNetDevice>(m_meshDevice);
    const double rxPowerDbm = meshDev ? meshDev->GetLastRxRssi() : -120.0;
    const uint8_t observedSf =
        std::clamp<uint8_t>(tag.GetSf(), static_cast<uint8_t>(7), static_cast<uint8_t>(12));
    const uint8_t linkSf = ResolveSfForLink(observedSf, rxPowerDbm);
    if (m_sfLinkMode == SfLinkMode::DETERMINISTIC_SENSITIVITY)
    {
        m_sfLinkSamples++;
        if (linkSf != observedSf)
        {
            m_sfLinkObservedMismatch++;
        }
    }

    NeighborLinkInfo link = BuildNeighborLinkInfo(tag, toaUsNeighbor, fromMac, linkSf);
    DvMessage msg;
    msg.origin = tag.GetSrc();
    msg.sequence = tag.GetSeq();
    msg.entries = DecodeDvEntries(p, tag, toaUsNeighbor);
    m_routing->UpdateFromDvMsg(msg, link);
}

bool
DvClApp::ParseDataWirePacketPueyo7b(Ptr<const Packet> p,
                                    DvClDataHeader* outHdr,
                                    Ptr<Packet>* outPayload) const
{
    if (!outHdr || !p || p->GetSize() < DvClDataHeader::kSerializedSize)
    {
        return false;
    }

    Ptr<Packet> copy = p->Copy();
    DvClDataHeader hdr;
    const uint32_t removed = copy->RemoveHeader(hdr);
    if (removed != DvClDataHeader::kSerializedSize)
    {
        return false;
    }
    if (UnpackType(hdr.GetFlagsTtl()) != DvClPacketType::DATA)
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
DvClApp::ParseBeaconWirePacketPueyo(Ptr<const Packet> p,
                                    DvClBeaconHeader* outHdr,
                                    Ptr<Packet>* outPayload) const
{
    if (!outHdr || !p || p->GetSize() < kPueyoBeaconHeaderBytes)
    {
        return false;
    }

    Ptr<Packet> copy = p->Copy();
    DvClBeaconHeader hdr;
    const uint32_t removed = copy->RemoveHeader(hdr);
    if (removed != kPueyoBeaconHeaderBytes)
    {
        return false;
    }
    if (UnpackType(hdr.GetFlagsTtl()) != DvClPacketType::BEACON)
    {
        return false;
    }
    outHdr->SetSrc(hdr.GetSrc());
    outHdr->SetDst(hdr.GetDst());
    outHdr->SetFlagsTtl(hdr.GetFlagsTtl());
    outHdr->SetSoc(hdr.GetSoc()); // §SoC-wire
    if (outPayload)
    {
        *outPayload = copy;
    }
    return true;
}

uint32_t
DvClApp::ResolveBeaconSequenceFromRpCounter(uint32_t origin, uint8_t rpCounter)
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

std::vector<DvEntry>
DvClApp::DecodeDvEntriesPueyo(Ptr<const Packet> p,
                              uint32_t payloadOffset,
                              uint32_t toaUsNeighbor,
                              uint8_t rxSf) const
{
    (void)toaUsNeighbor;
    (void)rxSf;
    std::vector<DvEntry> entries;
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
    if (payloadLen < kPueyoBeaconEntryBytes)
    {
        return entries;
    }

    std::vector<uint8_t> buf(totalSize);
    p->CopyData(buf.data(), totalSize);

    std::vector<DvEntryWirePueyo> received;
    DeserializeDvEntriesPueyo(buf.data() + payloadOffset, payloadLen, received);
    entries.reserve(received.size());
    if (m_pueyoValidationTrace)
    {
        NS_LOG_INFO("PUEYO_VAL_RX_DECODE node="
                    << GetNode()->GetId()
                    << " payload_hex=" << BytesToHex(buf.data() + payloadOffset, payloadLen)
                    << " entries=" << FormatDvEntriesPueyo(received));
    }
    for (const auto& route : received)
    {
        if (route.destination == GetNode()->GetId())
        {
            continue;
        }
        DvEntry e;
        e.destination = route.destination;
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
DvClApp::L2ReceiveWire(Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address& from)
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
    double rxPowerDbm = -120.0;
    lorawan::LoraTag loraTag;
    if (p->PeekPacketTag(loraTag))
    {
        const uint8_t sf = loraTag.GetSpreadingFactor();
        if (sf >= m_sfMin && sf <= m_sfMax)
        {
            rxSf = sf;
        }
        rxPowerDbm = loraTag.GetReceivePower();
    }

    DvClBeaconHeader beaconHdr;
    Ptr<Packet> payload;
    const bool isPueyoBeacon = ParseBeaconWirePacketPueyo(p, &beaconHdr, &payload);
    if (isPueyoBeacon)
    {
        const uint16_t src = beaconHdr.GetSrc();
        const uint8_t rpCounter = UnpackTtl(beaconHdr.GetFlagsTtl());
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
                NS_LOG_INFO("LINKADDR_LEARN node=" << myId << " peer=" << src
                                                   << " linkAddr learned=" << fromMac);
            }
            m_linkAddrTable[src] = fromMac;
            m_linkAddrLastSeen[src] = Simulator::Now();
        }
        else
        {
            NS_LOG_WARN("L2ReceiveWire beacon: node="
                        << myId << " src=" << src
                        << " non-Mac48 'from' address, cannot learn linkAddr wrapper");
        }
        UpdateNeighborLinkSf(src, rxSf);
        const uint8_t linkSf = ResolveSfForLink(rxSf, rxPowerDbm);
        if (m_sfLinkMode == SfLinkMode::DETERMINISTIC_SENSITIVITY)
        {
            m_sfLinkSamples++;
            if (linkSf != rxSf)
            {
                m_sfLinkObservedMismatch++;
            }
        }

        const uint32_t toaUsNeighbor = ComputeLoRaToAUs(rxSf, m_bw, m_cr, p->GetSize());
        const uint32_t seqFromRp = ResolveBeaconSequenceFromRpCounter(src, rpCounter);

        NeighborLinkInfo link;
        link.neighbor = src;
        link.sequence = seqFromRp;
        link.hops = 1;
        link.sf = linkSf;
        link.toaUs = toaUsNeighbor;
        const double rxEnergyFrac = SoC8ToFraction(beaconHdr.GetSoc()); // §SoC-wire fix
        link.batt_mV = static_cast<uint16_t>(EnergyFractionToBatteryMv(rxEnergyFrac));
        link.dc_remaining = 0xFF; // no DC byte on the 6B wire: always "not available"  // §DC-wire
                                  // fix: was defaulting to 0xFF
        LinkStats stats;
        stats.toaUs = toaUsNeighbor;
        stats.hops = 1;
        stats.sf = rxSf;
        stats.snrDb = 0.0;
        stats.batteryMv = static_cast<double>(link.batt_mV);
        stats.energyFraction = rxEnergyFrac; // §SoC-wire fix: was hardcoded 1.0
        LinkInputs metricIn;
        metricIn.toaUs = stats.toaUs;
        metricIn.sf = stats.sf;
        metricIn.energyFraction = ResolveEnergyFraction(stats.energyFraction, myId);
        metricIn.batteryMv = stats.batteryMv;
        const double cost = m_routing->GetMetric()->ComputeLinkCost(metricIn);
        link.scoreX100 =
            static_cast<uint16_t>(std::round(std::clamp(1.0 - cost, 0.0, 1.0) * 100.0));
        auto itMac = m_linkAddrTable.find(src);
        link.mac = (itMac != m_linkAddrTable.end()) ? itMac->second : Mac48Address();

        DvMessage msg;
        msg.origin = src;
        msg.sequence = seqFromRp;
        msg.entries = DecodeDvEntriesPueyo(payload, 0, toaUsNeighbor, rxSf);
        if (m_pueyoValidationTrace)
        {
            NS_LOG_INFO("PUEYO_VAL_RX_MSG node="
                        << myId << " origin=" << src << " linkSf=" << unsigned(linkSf)
                        << " rp_counter=" << unsigned(rpCounter) << " seq_ext=" << seqFromRp
                        << " entries=" << FormatDecodedEntries(msg.entries));
        }
        NS_LOG_INFO("DVTRACE_RX_V2 time=" << Simulator::Now().GetSeconds() << " node=" << myId
                                          << " src=" << src << " rp_counter=" << unsigned(rpCounter)
                                          << " seq_ext=" << seqFromRp
                                          << " entries=" << msg.entries.size());
        if (m_routing)
        {
            m_routing->UpdateFromDvMsg(msg, link);
        }
        m_beaconRxOk++;
        UpdateControlPlaneAuditState();
        m_lastDvRxTime = Simulator::Now();
        return true;
    }

    uint16_t src = 0;
    uint16_t dst = 0;
    uint16_t via = 0;
    uint16_t seq16 = 0;
    uint8_t ttl = 0;
    DvClDataHeader dataHdr;
    if (!ParseDataWirePacketPueyo7b(p, &dataHdr, &payload))
    {
        return true;
    }
    src = dataHdr.GetSrc();
    dst = dataHdr.GetDst();
    via = dataHdr.GetVia();
    ttl = dataHdr.GetTtl();
    DvClMetricTag traceTag;
    if (p->PeekPacketTag(traceTag))
    {
        seq16 = static_cast<uint16_t>(traceTag.GetSeq() & 0xFFFF);
    }

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

        LogRxEvent(src, dst, seq16, ttl, hopsSeen, 0, 0, rxSf, energyJ, energyFrac, false);
        if (m_stats)
        {
            double txTime = m_stats->GetFirstTxTime(src, dst, seq16);
            double delaySec = (txTime >= 0.0) ? (Simulator::Now().GetSeconds() - txTime) : -1.0;
            m_stats->RecordE2eDelay(src,
                                    dst,
                                    seq16,
                                    hopsSeen,
                                    delaySec,
                                    payload ? payload->GetSize() : 0,
                                    rxSf,
                                    true);
            m_stats->RecordEnergySnapshot(myId, energyJ, energyFrac);
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

    LogRxEvent(src, dst, seq16, ttl, hopsSeen, 0, 0, rxSf, energyJ, energyFrac, true);

    ForwardWithTtlV2(payload, src, dst, seq16, ttl);
    return true;
}

void
DvClApp::ForwardWithTtlV2(Ptr<const Packet> pIn,
                          uint16_t src,
                          uint16_t dst,
                          uint16_t seq16,
                          uint8_t ttl)
{
    const uint32_t myId = GetNode()->GetId();
    if (ttl == 0)
    {
        m_dropTtlExpired++;
        return;
    }
    const RouteEntry* route = m_routing ? m_routing->GetRoute(dst) : nullptr;
    if (!route)
    {
        CountDropNoRouteRelay();
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

    DvClDataHeader outHdr;
    outHdr.SetSrc(src);
    outHdr.SetDst(dst);
    outHdr.SetVia(static_cast<uint16_t>(route->nextHop));
    outHdr.SetFlagsTtl(PackFlagsTtl(DvClPacketType::DATA, nextTtl));
    out->AddHeader(outHdr);

    DvClMetricTag traceTag;
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
    DvClMetricTag previousTrace;
    out->RemovePacketTag(previousTrace);
    out->AddPacketTag(traceTag);

    Mac48Address routeMac;
    bool usingStale = false;
    const bool hasUsableMac = ResolveUnicastNextHopLinkAddr(route->nextHop, &routeMac, &usingStale);
    (void)usingStale;
    if (!hasUsableMac)
    {
        CountDropNoRouteRelay();
        return;
    }
    SendWithCSMA(out, traceTag, Address(routeMac), true);
}

// ========================================================================

void
DvClApp::LogTxEvent(uint32_t src,
                    uint32_t seq,
                    uint32_t dst,
                    uint8_t ttl,
                    uint8_t hops,
                    uint16_t battery,
                    uint16_t score,
                    uint8_t sf,
                    uint32_t toaUs,
                    double energyJ,
                    double energyFrac,
                    bool ok)
{
    if (m_stats)
    {
        m_stats->RecordTx(GetNode()->GetId(),
                          src,
                          seq,
                          dst,
                          ttl,
                          hops,
                          0, // rssi removed
                          battery,
                          score,
                          sf,
                          toaUs,
                          energyJ,
                          energyFrac,
                          ok);
        m_stats->RecordEnergySnapshot(GetNode()->GetId(), energyJ, energyFrac);
    }
}

void
DvClApp::LogRxEvent(uint32_t src,
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
    if (m_stats)
    {
        m_stats->RecordRx(GetNode()->GetId(),
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
        m_stats->RecordEnergySnapshot(GetNode()->GetId(), energyJ, energyFrac);
    }
}

void
DvClApp::CleanOldSeenPackets()
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
DvClApp::CleanOldDedupCaches()
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
DvClApp::CleanOldSeenData()
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
DvClApp::ComputeNextDataSlotDelay(Time baseDelay)
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
Time
DvClApp::ComputeStudyForwardDelay(const DvClMetricTag& tag) const
{
    const double baseMs = std::max(0.0, m_studyForwardBaseDelayMs);
    if (!m_studyForwardSpreadEnable)
    {
        return MilliSeconds(baseMs + static_cast<double>(GetNode()->GetId() % 5));
    }

    const double spreadMs = std::max(0.0, m_studyForwardSpreadMs);
    const uint32_t src = tag.GetSrc();
    const uint32_t dst = tag.GetDst();
    const uint32_t seq = tag.GetSeq();
    const uint32_t nodeId = GetNode()->GetId();
    const uint32_t salt =
        (nodeId * 1103515245u) ^ (src * 2654435761u) ^ (dst * 2246822519u) ^ (seq * 3266489917u);
    const double extraMs =
        (spreadMs <= 0.0) ? 0.0 : std::fmod(static_cast<double>(salt), spreadMs + 1.0);
    return MilliSeconds(baseMs + extraMs);
}

bool
DvClApp::ComputeStudySuperframeWait(const TxQueueEntry& entry,
                                    Time* outWait,
                                    std::string* outReason) const
{
    if (!m_studySuperframeEnable || m_studySuperframePeriodSec <= 0.0)
    {
        return false;
    }

    const double period = m_studySuperframePeriodSec;
    const double ctrlWindow = std::min(std::max(0.0, m_studySuperframeCtrlWindowSec), period);
    const bool isControl = (entry.tag.GetDst() == 0xFFFF);
    const double now = Simulator::Now().GetSeconds();
    const double phase = std::fmod(now, period);

    double waitSec = 0.0;
    std::string reason;
    if (isControl)
    {
        if (phase >= ctrlWindow)
        {
            waitSec = period - phase;
            reason = "study_superframe_ctrl_wait";
        }
    }
    else if (phase < ctrlWindow)
    {
        waitSec = ctrlWindow - phase;
        reason = "study_superframe_data_wait";
    }

    if (waitSec <= 0.0)
    {
        return false;
    }

    if (outWait)
    {
        *outWait = Seconds(waitSec);
    }
    if (outReason)
    {
        *outReason = reason;
    }
    return true;
}

void
DvClApp::GenerateDataTraffic()
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
    if (m_trafficMode == TrafficMode::PUEYO_ALL_TO_ALL)
    {
        if (m_pueyoTrafficIndex >= m_pueyoTrafficSchedule.size())
        {
            return;
        }
        dst = m_pueyoTrafficSchedule[m_pueyoTrafficIndex++];
    }
    else if (!m_dataDestinations.empty())
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

    Time nextDelay = Seconds(0);
    if (m_enableDataSlots && !m_dataStartPhaseOnly && m_dataFixedPhaseCadence)
    {
        const double periodSec = m_dataGenerationPeriod.GetSeconds();
        if (m_nextDataNominalTimeSec < 0.0)
        {
            m_nextDataNominalTimeSec = Simulator::Now().GetSeconds();
        }
        m_nextDataNominalTimeSec += periodSec;
        const double jitterLimit =
            std::min(std::max(0.0, m_dataSlotJitterSec), std::max(0.0, periodSec) * 0.49);
        const double jitter =
            (m_rng && jitterLimit > 0.0) ? m_rng->GetValue(-jitterLimit, jitterLimit) : 0.0;
        double sendTime = m_nextDataNominalTimeSec + jitter;
        if (sendTime < Simulator::Now().GetSeconds())
        {
            sendTime = m_nextDataNominalTimeSec;
        }
        nextDelay = Seconds(sendTime) - Simulator::Now();
        if (nextDelay < Seconds(0))
        {
            nextDelay = Seconds(0);
        }
    }
    else
    {
        const double jitterMax = std::max(0.0, m_dataPeriodJitterMax);
        double jitter = 0.0;
        if (m_rng && jitterMax > 0.0)
        {
            jitter = m_dataPeriodJitterSymmetric ? m_rng->GetValue(-jitterMax, jitterMax)
                                                 : m_rng->GetValue(0.0, jitterMax);
        }
        nextDelay = m_dataGenerationPeriod + Seconds(jitter);
        if (m_enableDataSlots && !m_dataStartPhaseOnly && m_dataSlotPeriodSec > 0.0)
        {
            nextDelay = ComputeNextDataSlotDelay(nextDelay);
        }
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

    m_dataGenerationEvt = Simulator::Schedule(nextDelay, &DvClApp::GenerateDataTraffic, this);
}

// Construye y envía un paquete de datos unicast.
void
DvClApp::SendDataPacket(uint32_t dst)
{
    SendDataPacketPueyo7b(dst);
}

void
DvClApp::SendDataPacketPueyo7b(uint32_t dst)
{
    TrackActiveDestination(dst);
    const uint32_t myId = GetNode()->GetId();
    const uint16_t seq16 = static_cast<uint16_t>((++m_dataSeqPerNode) & 0xFFFF);

    NS_LOG_INFO("APP_SEND_DATA src=" << myId << " dst=" << dst << " seq=" << seq16
                                     << " time=" << Simulator::Now().GetSeconds());

    m_dataPacketsGenerated++;
    if (m_stats)
    {
        m_stats->RecordDataGenerated(myId, dst, seq16);
    }

    const RouteEntry* route = m_routing ? m_routing->GetRoute(dst) : nullptr;
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
                    << " collectorNodeId=" << m_collectorNodeId << " reason=no_route_pueyo7b");
        NS_LOG_INFO("FWDTRACE DATA_NOROUTE time="
                    << Simulator::Now().GetSeconds() << " node=" << myId << " src=" << myId
                    << " dst=" << dst << " seq=" << seq16 << " reason=no_route_pueyo7b");
        m_dataNoRoute++;
        CountDropNoRouteSrc();
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

    DvClDataHeader hdr;
    hdr.SetSrc(static_cast<uint16_t>(myId));
    hdr.SetDst(static_cast<uint16_t>(dst));
    hdr.SetVia(static_cast<uint16_t>(route->nextHop));
    hdr.SetFlagsTtl(PackFlagsTtl(DvClPacketType::DATA, std::min<uint8_t>(m_initTtl, 63)));

    Ptr<Packet> p = Create<Packet>(m_dataPayloadSize);
    p->AddHeader(hdr);

    DvClMetricTag traceTag;
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
                    << " collectorNodeId=" << m_collectorNodeId << " nextHop=" << route->nextHop
                    << " reason=no_link_addr_for_unicast_pueyo7b");
        NS_LOG_INFO("FWDTRACE DATA_NOROUTE time="
                    << Simulator::Now().GetSeconds() << " node=" << myId << " src=" << myId
                    << " dst=" << dst << " seq=" << seq16 << " nextHop=" << route->nextHop
                    << " reason=no_link_addr_for_unicast_pueyo7b");
        m_dataNoRoute++;
        CountDropNoRouteSrc();
        return;
    }

    if (m_stats)
    {
        m_stats->RecordRouteUsed(myId,
                                 dst,
                                 route->nextHop,
                                 route->hops,
                                 route->scoreX100,
                                 route->seqNum);
    }
    SendWithCSMA(p, traceTag, Address(routeMac), true);
}

} // namespace dvcl
} // namespace ns3
