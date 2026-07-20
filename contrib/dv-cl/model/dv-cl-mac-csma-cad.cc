/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-mac-csma-cad.h"

#include "ns3/boolean.h"
#include "ns3/double.h"
#include "ns3/string.h"
#include "ns3/enum.h"
#include "ns3/log.h"
#include "ns3/lora-channel.h"
#include "ns3/lora-phy.h"
#include "ns3/net-device.h"
#include "ns3/node.h"
#include "ns3/simulator.h"
#include "ns3/uinteger.h"

#include <algorithm>
#include <cmath>

namespace ns3
{
namespace dvcl
{

NS_LOG_COMPONENT_DEFINE("DvClCsmaCadMac");
NS_OBJECT_ENSURE_REGISTERED(DvClCsmaCadMac);

TypeId
DvClCsmaCadMac::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClCsmaCadMac")
            .SetParent<Object>()
            .AddConstructor<DvClCsmaCadMac>()
            .AddAttribute("MinBackoffSlots",
                          "Minimum backoff window in slots (W_min).",
                          UintegerValue(4),
                          MakeUintegerAccessor(&DvClCsmaCadMac::m_minBackoffSlots),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("MaxBackoffSlots",
                          "Maximum backoff window in slots (W_max) before ToA scaling.",
                          UintegerValue(kDefaultMaxBackoffSlots),
                          MakeUintegerAccessor(&DvClCsmaCadMac::SetMaxBackoffSlots,
                                               &DvClCsmaCadMac::GetMaxBackoffSlots),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("MaxBackoffSlotsAbs",
                          "Hard cap for backoff window in slots.",
                          UintegerValue(kDefaultMaxBackoffSlotsAbs),
                          MakeUintegerAccessor(&DvClCsmaCadMac::SetMaxBackoffSlotsAbs,
                                               &DvClCsmaCadMac::GetMaxBackoffSlotsAbs),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("BackoffStep",
                          "Linear backoff step in slots (Delta W) per failure.",
                          UintegerValue(4),
                          MakeUintegerAccessor(&DvClCsmaCadMac::m_backoffStep),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("CadHistoryWindow",
                          "Number of CAD samples to estimate channel load.",
                          UintegerValue(20),
                          MakeUintegerAccessor(&DvClCsmaCadMac::m_cadHistoryWindow),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("CadLoadWeight",
                          "Weight [0..1] applied to channel load in backoff window.",
                          DoubleValue(0.5),
                          MakeDoubleAccessor(&DvClCsmaCadMac::m_loadWeight),
                          MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute("ToaEmaDataSeconds",
                          "Initial typical data ToA used to scale max backoff.",
                          DoubleValue(0.2),
                          MakeDoubleAccessor(&DvClCsmaCadMac::m_toaEmaDataSeconds),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("ToaEmaCtrlSeconds",
                          "Initial typical control ToA used when data ToA is unknown.",
                          DoubleValue(0.2),
                          MakeDoubleAccessor(&DvClCsmaCadMac::m_toaEmaCtrlSeconds),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute("ToaEmaAlpha",
                          "EMA alpha for ToA tracking.",
                          DoubleValue(0.2),
                          MakeDoubleAccessor(&DvClCsmaCadMac::m_toaEmaAlpha),
                          MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute("ToaMaxFactor",
                          "Multiplier for ToA-based backoff cap.",
                          DoubleValue(2.0),
                          MakeDoubleAccessor(&DvClCsmaCadMac::m_toaMaxFactor),
                          MakeDoubleChecker<double>(0.0))
            .AddAttribute(
                "DutyCycleLimit",
                "Duty cycle limit as fraction (0.01 = 1%).",
                DoubleValue(0.01),
                MakeDoubleAccessor(&DvClCsmaCadMac::SetDutyCycleLimit, &DvClCsmaCadMac::GetDutyCycleLimit),
                MakeDoubleChecker<double>(0.0, 1.0))
            .AddAttribute(
                "DutyCycleWindow",
                "Window of time for duty cycle enforcement.",
                TimeValue(Hours(1)),
                MakeTimeAccessor(&DvClCsmaCadMac::SetDutyCycleWindow, &DvClCsmaCadMac::GetDutyCycleWindow),
                MakeTimeChecker())
            .AddAttribute(
                "DutyEnforcement",
                "How the duty cycle is enforced: 'time_off_air' follows ETSI EN 300 220 "
                "and LoRaWAN (after transmitting T the radio stays silent until T/limit "
                "has elapsed, which bounds the ratio over every window); 'sliding_window' "
                "keeps the legacy trailing-sum gate, which is only checked at transmission "
                "instants and lets the true rolling peak exceed the limit.",
                StringValue("time_off_air"),
                MakeStringAccessor(&DvClCsmaCadMac::SetDutyEnforcement,
                                   &DvClCsmaCadMac::GetDutyEnforcement),
                MakeStringChecker())
            .AddAttribute("DutyCycleEnabled",
                          "Enable/disable duty cycle enforcement.",
                          BooleanValue(true),
                          MakeBooleanAccessor(&DvClCsmaCadMac::m_dutyCycleEnabled),
                          MakeBooleanChecker())
            .AddAttribute("CadDecisionModel",
                          "CAD busy decision model: oracle | local_power.",
                          EnumValue(DvClCsmaCadMac::CAD_LOCAL_POWER),
                          MakeEnumAccessor<DvClCsmaCadMac::CadDecisionModel>(
                              &DvClCsmaCadMac::m_cadDecisionModel),
                          MakeEnumChecker(DvClCsmaCadMac::CAD_ORACLE,
                                          "oracle",
                                          DvClCsmaCadMac::CAD_LOCAL_POWER,
                                          "local_power"))
            .AddAttribute("CadSymbols",
                          "Number of CAD symbols used for one CAD operation.",
                          UintegerValue(2),
                          MakeUintegerAccessor(&DvClCsmaCadMac::m_cadSymbols),
                          MakeUintegerChecker<uint8_t>(1, 32))
            .AddAttribute("CadSf",
                          "Spreading factor used by CAD sensing (SF-aware CAD).",
                          UintegerValue(9),
                          MakeUintegerAccessor(&DvClCsmaCadMac::m_cadSf),
                          MakeUintegerChecker<uint8_t>(7, 12))
            .AddAttribute("CadBandwidthHz",
                          "Bandwidth used by CAD timing and sensitivity context.",
                          UintegerValue(125000),
                          MakeUintegerAccessor(&DvClCsmaCadMac::m_cadBandwidthHz),
                          MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("CadDurationMode",
                          "CAD duration mode: fixed | sf_bw.",
                          EnumValue(DvClCsmaCadMac::CAD_DURATION_SF_BW),
                          MakeEnumAccessor<DvClCsmaCadMac::CadDurationMode>(
                              &DvClCsmaCadMac::m_cadDurationMode),
                          MakeEnumChecker(DvClCsmaCadMac::CAD_DURATION_FIXED,
                                          "fixed",
                                          DvClCsmaCadMac::CAD_DURATION_SF_BW,
                                          "sf_bw"))
            .AddAttribute("CadSenseMarginDb",
                          "Margin [dB] above sensitivity to declare CAD busy in local_power mode.",
                          DoubleValue(3.0),  // Lowered from 6.0; see PerformChannelAssessment / FIXES_AND_METHODOLOGY.md §8.7 item 1.
                          MakeDoubleAccessor(&DvClCsmaCadMac::m_cadSenseMarginDb),
                          MakeDoubleChecker<double>());
    return tid;
}

DvClCsmaCadMac::DvClCsmaCadMac()
    : m_rng(CreateObject<UniformRandomVariable>()),
      m_dutyCycleWindow(Hours(1)),
      m_dutyCycleLimit(0.01),
      m_cadDuration(MilliSeconds(5.5)),
      m_difsCadCount(3),
      m_cadHistoryWindow(20),
      m_minBackoffSlots(4),
      m_maxBackoffSlots(kDefaultMaxBackoffSlots),
      m_maxBackoffSlotsAbs(kDefaultMaxBackoffSlotsAbs),
      m_backoffStep(4),
      m_failures(0),
      m_lastBackoffSlots(0),
      m_lastBackoffWindowSlots(0),
      m_loadWeight(0.5),
      m_toaEmaDataSeconds(0.2),
      m_toaEmaCtrlSeconds(0.2),
      m_toaEmaAlpha(0.2),
      m_toaMaxFactor(2.0),
      m_dutyCycleEnabled(true),
      m_cadDecisionModel(CAD_LOCAL_POWER),
      m_cadDurationMode(CAD_DURATION_SF_BW),
      m_cadSymbols(2),
      m_cadSf(9),
      m_cadBandwidthHz(125000),
      m_cadSenseMarginDb(3.0),  // ctor default mirrors AddAttribute
      m_cadBusyEventsLocalPower(0),
      m_cadBusyEventsOracle(0)
{
}

bool
DvClCsmaCadMac::CanTransmitNow()
{
    return CanTransmitNow(0.0);
}

bool
DvClCsmaCadMac::CanTransmitNow(double toaSeconds)
{
    // If duty cycle is disabled, always allow transmission
    if (!m_dutyCycleEnabled)
    {
        return true;
    }

    if (m_dutyEnforcement == DutyEnforcement::TIME_OFF_AIR)
    {
        const bool ready = Simulator::Now() >= m_nextTxAllowed;
        if (ready)
        {
            m_lastGrantAt = Simulator::Now();
            m_lastGrantToa = toaSeconds;
        }
        if (!ready)
        {
            NS_LOG_WARN("DvClCsmaCadMac: still off air until "
                        << m_nextTxAllowed.GetSeconds() << "s");
        }
        return ready;
    }

    CleanOldTxHistory();
    const double dutyCycle = GetDutyCycleUsed();
    double projected = dutyCycle;
    if (toaSeconds > 0.0 && m_dutyCycleWindow.GetSeconds() > 0.0)
    {
        projected += toaSeconds / m_dutyCycleWindow.GetSeconds();
    }

    const bool allowed = projected <= m_dutyCycleLimit;
    if (allowed)
    {
        m_lastGrantAt = Simulator::Now();
        m_lastGrantToa = toaSeconds;
    }
    if (!allowed)
    {
        NS_LOG_WARN("DvClCsmaCadMac: DUTY limit exceeded current="
                    << dutyCycle * 100.0 << "% projected=" << projected * 100.0
                    << "% limit=" << m_dutyCycleLimit * 100.0 << "%");
    }
    return allowed;
}

void
DvClCsmaCadMac::SetDutyEnforcement(const std::string& mode)
{
    m_dutyEnforcement = (mode == "sliding_window") ? DutyEnforcement::SLIDING_WINDOW
                                                   : DutyEnforcement::TIME_OFF_AIR;
}

std::string
DvClCsmaCadMac::GetDutyEnforcement() const
{
    return (m_dutyEnforcement == DutyEnforcement::SLIDING_WINDOW) ? "sliding_window"
                                                                  : "time_off_air";
}

void
DvClCsmaCadMac::NotifyTxStart(double toaSeconds)
{
    CleanOldTxHistory();
    const Time now = Simulator::Now();
    const Time duration = Seconds(toaSeconds);
    // ETSI EN 300 220: pay for the air just used before taking any more. Mirrors
    // ns-3's own lorawan module (LogicalLoraChannelHelper::AddEvent).
    if (m_dutyCycleEnabled && m_lastGrantAt != now)
    {
        ++m_ungatedTx;
        NS_LOG_WARN("UNGATED_TX t=" << now.GetSeconds() << " toa=" << toaSeconds
                                    << " lastGrantAt=" << m_lastGrantAt.GetSeconds()
                                    << " grantedToa=" << m_lastGrantToa);
    }
    else if (m_dutyCycleEnabled)
    {
        ++m_gatedTx;
        if (toaSeconds > m_lastGrantToa + 1e-9)
        {
            NS_LOG_WARN("UNDERPRICED_TX t=" << now.GetSeconds() << " actual=" << toaSeconds
                                            << " granted=" << m_lastGrantToa);
        }
    }
    if (m_dutyCycleLimit > 0.0)
    {
        m_nextTxAllowed = now + Seconds(toaSeconds / m_dutyCycleLimit);
    }
    m_txHistory.emplace_back(now, duration);
    NS_LOG_DEBUG("DvClCsmaCadMac: TX recorded duration=" << duration.GetSeconds() << "s dc="
                                                     << GetDutyCycleUsed() * 100.0 << "%");
}

void
DvClCsmaCadMac::NotifyRxStart(double durationSeconds)
{
    NS_LOG_DEBUG("DvClCsmaCadMac: RX duration=" << durationSeconds << "s");
    (void)durationSeconds;
}

uint32_t
DvClCsmaCadMac::GetBackoffSlots()
{
    const uint32_t effectiveWindow = ComputeBackoffWindowSlots();
    m_lastBackoffWindowSlots = effectiveWindow;
    m_lastBackoffSlots = m_rng->GetInteger(0, effectiveWindow - 1);
    return m_lastBackoffSlots;
}

double
DvClCsmaCadMac::GetDutyCycleUsed()
{
    CleanOldTxHistory();
    if (m_txHistory.empty() || m_dutyCycleWindow.IsZero())
    {
        return 0.0;
    }

    const Time now = Simulator::Now();
    const Time windowStart = now - m_dutyCycleWindow;
    Time total{Seconds(0)};
    for (const auto& record : m_txHistory)
    {
        const Time txStart = record.first;
        const Time txEnd = record.first + record.second;
        const Time overlapStart = std::max(txStart, windowStart);
        // Charge airtime that is committed, not merely elapsed: a transmission
        // already on the air will occupy the channel to its end, so clamping
        // the overlap at `now` lets the gate hand out budget that is already
        // spent. Measuring only elapsed airtime is what let the true rolling
        // peak sit above the limit while the gate believed it was at exactly
        // 1% (see VALIDATION.md).
        const Time overlapEnd = txEnd;
        if (overlapEnd > overlapStart)
        {
            total += overlapEnd - overlapStart;
        }
    }
    return total.GetSeconds() / m_dutyCycleWindow.GetSeconds();
}

double
DvClCsmaCadMac::GetDutyCycleLimit() const
{
    return m_dutyCycleLimit;
}

void
DvClCsmaCadMac::SetDutyCycleLimit(double limit)
{
    m_dutyCycleLimit = std::clamp(limit, 0.0, 1.0);
}

void
DvClCsmaCadMac::SetDutyCycleWindow(Time window)
{
    m_dutyCycleWindow = window;
}

Time
DvClCsmaCadMac::GetDutyCycleWindow() const
{
    return m_dutyCycleWindow;
}

void
DvClCsmaCadMac::SetCadDuration(Time duration)
{
    m_cadDuration = duration;
}

void
DvClCsmaCadMac::SetDifsCadCount(uint8_t count)
{
    m_difsCadCount = count;
}

void
DvClCsmaCadMac::SetMaxBackoffSlots(uint32_t slots)
{
    m_maxBackoffSlots = std::max<uint32_t>(1, slots);
}

uint32_t
DvClCsmaCadMac::GetMaxBackoffSlots() const
{
    return m_maxBackoffSlots;
}

void
DvClCsmaCadMac::SetMaxBackoffSlotsAbs(uint32_t slots)
{
    m_maxBackoffSlotsAbs = std::max<uint32_t>(1, slots);
}

uint32_t
DvClCsmaCadMac::GetMaxBackoffSlotsAbs() const
{
    return m_maxBackoffSlotsAbs;
}

void
DvClCsmaCadMac::SetRandomStream(Ptr<UniformRandomVariable> rng)
{
    m_rng = rng ? rng : CreateObject<UniformRandomVariable>();
}

void
DvClCsmaCadMac::SetPhy(Ptr<lorawan::LoraPhy> phy)
{
    m_phy = phy;
}

void
DvClCsmaCadMac::SetCadContext(uint8_t sf, uint32_t bwHz)
{
    if (sf >= 7 && sf <= 12)
    {
        m_cadSf = sf;
    }
    if (bwHz > 0)
    {
        m_cadBandwidthHz = bwHz;
    }
}

void
DvClCsmaCadMac::UpdateTypicalDataToaSeconds(double toaSeconds)
{
    UpdateToaEma(m_toaEmaDataSeconds, toaSeconds);
}

void
DvClCsmaCadMac::UpdateTypicalCtrlToaSeconds(double toaSeconds)
{
    UpdateToaEma(m_toaEmaCtrlSeconds, toaSeconds);
}

void
DvClCsmaCadMac::UpdateToaEma(double& emaSeconds, double toaSeconds)
{
    if (toaSeconds <= 0.0)
    {
        return;
    }
    const double alpha = std::clamp(m_toaEmaAlpha, 0.0, 1.0);
    emaSeconds = alpha * toaSeconds + (1.0 - alpha) * emaSeconds;
}

// Reserved hook for a future unicast-with-ACK MAC. See header for context.
// In the current LoRa broadcast build this method has no callers; m_failures
// is managed by PerformChannelAssessment() instead. Kept (rather than deleted)
// so that an ACK-aware MAC can wire it back without re-introducing the API.
void
DvClCsmaCadMac::NotifyTxResult(bool success)
{
    if (success)
    {
        m_failures = 0;
    }
    else
    {
        m_failures++;
    }
}

uint32_t
DvClCsmaCadMac::GetFailureCount() const
{
    return m_failures;
}

uint32_t
DvClCsmaCadMac::GetLastBackoffSlots() const
{
    return m_lastBackoffSlots;
}

uint32_t
DvClCsmaCadMac::GetLastBackoffWindowSlots() const
{
    return m_lastBackoffWindowSlots;
}

double
DvClCsmaCadMac::GetCadLoadEstimate() const
{
    return GetCadLoad();
}

uint64_t
DvClCsmaCadMac::GetCadBusyEventsLocalPower() const
{
    return m_cadBusyEventsLocalPower;
}

uint64_t
DvClCsmaCadMac::GetCadBusyEventsOracle() const
{
    return m_cadBusyEventsOracle;
}

Time
DvClCsmaCadMac::GetCadDuration() const
{
    if (m_cadDurationMode == CAD_DURATION_SF_BW)
    {
        return ComputeCadDurationFromContext();
    }
    return m_cadDuration;
}

uint8_t
DvClCsmaCadMac::GetDifsCadCount() const
{
    return m_difsCadCount;
}

bool
DvClCsmaCadMac::PerformChannelAssessment()
{
    const uint32_t nodeId =
        (m_phy && m_phy->GetDevice() && m_phy->GetDevice()->GetNode())
            ? m_phy->GetDevice()->GetNode()->GetId()
            : 0xFFFFFFFFu;
    bool busy = false;
    const Time cadDuration = GetCadDuration();
    for (uint8_t i = 0; i < m_difsCadCount; ++i)
    {
        const Time sampleTime = Simulator::Now() + (cadDuration * i);
        if (PerformCadOnce(sampleTime))
        {
            NS_LOG_INFO("DvClCsmaCadMac: CAD busy node="
                        << nodeId << " attempt=" << unsigned(i)
                        << " sampleTime=" << sampleTime.GetSeconds());
            busy = true;
            break;
        }
    }
    if (busy)
    {
        m_failures++;
        const uint32_t windowSlots = ComputeBackoffWindowSlots();
        NS_LOG_INFO("CAD_RESULT detail: node="
                    << nodeId << " time=" << Simulator::Now().GetSeconds()
                    << "s busy=1 failures=" << m_failures << " windowSlots=" << windowSlots
                    << " load=" << GetCadLoad());
        return true;
    }
    // Reset m_failures on clean CAD.
    //
    // NOTE: this is NOT strict 802.11 semantics (which resets the contention
    // window on ACK reception, not on CCA-clean). The "B1 fix" label used in
    // §7.1 of FIXES_AND_METHODOLOGY.md was imprecise. The actual adaptive
    // behaviour under sustained load comes from `loadSlots` in
    // ComputeBackoffWindowSlots() via GetCadLoad(), which keeps the window in
    // the ~30+ slot range when the channel is busy most of the time.
    //
    // m_failures here is only a short-term escalator during a burst of
    // consecutive busy samples; it collapses to 0 as soon as one CAD probe
    // sees a clean channel. Long-term "memory of congestion" lives in the
    // load estimator, not in this counter. See §8.5 of FIXES_AND_METHODOLOGY
    // for the design discussion and the alternative (gradual decrement) that
    // was considered.
    m_failures = 0;
    const uint32_t windowSlots = ComputeBackoffWindowSlots();
    NS_LOG_INFO("CAD_RESULT detail: node="
                << nodeId << " time=" << Simulator::Now().GetSeconds()
                << "s busy=0 failures=" << m_failures << " windowSlots=" << windowSlots
                << " load=" << GetCadLoad());
    return false;
}

bool
DvClCsmaCadMac::PerformCadOnce(Time sampleTime)
{
    Ptr<lorawan::LoraChannel> channel = m_phy ? m_phy->GetChannel() : nullptr;
    if (!channel)
    {
        NS_LOG_WARN("DvClCsmaCadMac: no PHY/channel attached, CAD defaults to busy");
        RecordCadResult(true);
        return true;
    }

    const auto events = channel->GetTxEvents(sampleTime);
    for (const auto& ev : events)
    {
        if (ev.endTime <= sampleTime)
        {
            continue;
        }

        if (m_phy && !m_phy->IsOnFrequency(ev.frequencyHz))
        {
            continue;
        }

        if (m_cadDecisionModel == CAD_ORACLE)
        {
            NS_LOG_DEBUG("DvClCsmaCadMac: CAD(oracle) busy by active tx on freq="
                         << ev.frequencyHz << " sf=" << unsigned(ev.sf));
            m_cadBusyEventsOracle++;
            RecordCadResult(true);
            return true;
        }

        if (ev.sf != m_cadSf)
        {
            continue;
        }

        if (!m_phy || !ev.sender)
        {
            NS_LOG_WARN("DvClCsmaCadMac: missing PHY/sender for local_power CAD, fallback busy");
            m_cadBusyEventsLocalPower++;
            RecordCadResult(true);
            return true;
        }

        Ptr<MobilityModel> senderMobility = ev.sender->GetMobility();
        Ptr<MobilityModel> myMobility = m_phy->GetMobility();
        if (!senderMobility || !myMobility)
        {
            NS_LOG_WARN("DvClCsmaCadMac: missing mobility for local_power CAD, fallback busy");
            m_cadBusyEventsLocalPower++;
            RecordCadResult(true);
            return true;
        }

        const double prxDbm = channel->GetRxPower(ev.txPowerDbm, senderMobility, myMobility);
        const double thresholdDbm = GetCadSensitivityDbm(m_cadSf) + m_cadSenseMarginDb;
        if (prxDbm >= thresholdDbm)
        {
            NS_LOG_DEBUG("DvClCsmaCadMac: CAD(local_power) busy freq="
                         << ev.frequencyHz << " sf=" << unsigned(ev.sf)
                         << " prx=" << prxDbm << "dBm thr=" << thresholdDbm << "dBm");
            m_cadBusyEventsLocalPower++;
            RecordCadResult(true);
            return true;
        }
    }
    NS_LOG_DEBUG("DvClCsmaCadMac: CAD result busy=false");
    RecordCadResult(false);
    return false;
}

void
DvClCsmaCadMac::CleanOldTxHistory()
{
    if (m_txHistory.empty())
    {
        return;
    }

    const Time threshold = Simulator::Now() - m_dutyCycleWindow;
    while (!m_txHistory.empty() && (m_txHistory.front().first + m_txHistory.front().second) <= threshold)
    {
        m_txHistory.pop_front();
    }
}

void
DvClCsmaCadMac::RecordCadResult(bool busy)
{
    m_cadHistory.push_back(busy);
    while (m_cadHistory.size() > m_cadHistoryWindow)
    {
        m_cadHistory.pop_front();
    }
}

double
DvClCsmaCadMac::GetCadLoad() const
{
    if (m_cadHistory.empty())
    {
        return 0.0;
    }
    uint32_t busyCount = 0;
    for (bool busy : m_cadHistory)
    {
        if (busy)
        {
            busyCount++;
        }
    }
    return static_cast<double>(busyCount) / static_cast<double>(m_cadHistory.size());
}

uint32_t
DvClCsmaCadMac::ComputeBackoffWindowSlots() const
{
    const uint32_t maxSlots = ComputeMaxBackoffSlots();
    const uint32_t loadSlots =
        static_cast<uint32_t>(std::round(GetCadLoad() * m_loadWeight * maxSlots));
    const uint32_t window =
        std::min(m_minBackoffSlots + (m_failures * m_backoffStep) + loadSlots, maxSlots);
    return std::max<uint32_t>(1, window);
}

uint32_t
DvClCsmaCadMac::ComputeMaxBackoffSlots() const
{
    uint32_t maxSlots = std::max<uint32_t>(m_minBackoffSlots, m_maxBackoffSlots);
    double toaSeconds = m_toaEmaDataSeconds;
    if (toaSeconds <= 0.0)
    {
        toaSeconds = m_toaEmaCtrlSeconds;
    }
    const Time cadDuration = GetCadDuration();
    if (cadDuration.IsPositive() && toaSeconds > 0.0 && m_toaMaxFactor > 0.0)
    {
        const double slotSeconds = cadDuration.GetSeconds();
        const double toaSlots = (toaSeconds * m_toaMaxFactor) / slotSeconds;
        const uint32_t toaSlotsRounded = static_cast<uint32_t>(std::ceil(toaSlots));
        maxSlots = std::max(maxSlots, toaSlotsRounded);
    }
    const uint32_t hardCap = std::max<uint32_t>(m_minBackoffSlots, m_maxBackoffSlotsAbs);
    maxSlots = std::min(maxSlots, hardCap);
    return std::max<uint32_t>(1, maxSlots);
}

Time
DvClCsmaCadMac::ComputeCadDurationFromContext() const
{
    const uint8_t sf = std::min<uint8_t>(12, std::max<uint8_t>(7, m_cadSf));
    const uint8_t symbols = std::max<uint8_t>(1, m_cadSymbols);
    const uint32_t bwHz = std::max<uint32_t>(1, m_cadBandwidthHz);
    const double tSymSec = std::pow(2.0, static_cast<int>(sf)) / static_cast<double>(bwHz);
    return Seconds(std::max(1e-6, tSymSec * static_cast<double>(symbols)));
}

double
DvClCsmaCadMac::GetCadSensitivityDbm(uint8_t sf) const
{
    // SX1276 sensitivity (dBm) for SF7..SF12 @125kHz.
    static const double kSensitivity[6] = {-123.0, -126.0, -129.0, -132.0, -133.0, -136.0};
    if (sf < 7 || sf > 12)
    {
        return kSensitivity[0];
    }
    return kSensitivity[sf - 7];
}

} // namespace dvcl
} // namespace ns3
