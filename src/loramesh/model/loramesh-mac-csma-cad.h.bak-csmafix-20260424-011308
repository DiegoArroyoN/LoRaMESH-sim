#ifndef LORAMESH_MAC_CSMA_CAD_H
#define LORAMESH_MAC_CSMA_CAD_H

#include "ns3/nstime.h"
#include "ns3/object.h"
#include "ns3/ptr.h"
#include "ns3/random-variable-stream.h"

#include <cstdint>
#include <deque>

namespace ns3
{
namespace lorawan
{
class LoraPhy;
}

namespace loramesh
{

/**
 * \brief Minimal CSMA/CAD MAC helper handling channel sampling, backoff and duty cycle.
 */
class CsmaCadMac : public Object
{
  public:
    enum CadDecisionModel : uint8_t
    {
        CAD_ORACLE = 0,
        CAD_LOCAL_POWER = 1,
    };

    enum CadDurationMode : uint8_t
    {
        CAD_DURATION_FIXED = 0,
        CAD_DURATION_SF_BW = 1,
    };

    static TypeId GetTypeId();
    CsmaCadMac();
    ~CsmaCadMac() override = default;

    bool CanTransmitNow();
    bool CanTransmitNow(double toaSeconds);
    void NotifyTxStart(double toaSeconds);
    void NotifyRxStart(double durationSeconds);
    uint32_t GetBackoffSlots();
    double GetDutyCycleUsed();
    double GetDutyCycleLimit() const;
    void SetDutyCycleLimit(double limit);
    void SetDutyCycleWindow(Time window);
    Time GetDutyCycleWindow() const;
    void SetCadDuration(Time duration);
    void SetDifsCadCount(uint8_t count);
    void SetBackoffWindow(uint8_t window);
    void SetMaxBackoffSlots(uint32_t slots);
    uint32_t GetMaxBackoffSlots() const;
    void SetMaxBackoffSlotsAbs(uint32_t slots);
    uint32_t GetMaxBackoffSlotsAbs() const;
    void SetRandomStream(Ptr<UniformRandomVariable> rng);
    void SetPhy(Ptr<lorawan::LoraPhy> phy);
    void SetCadContext(uint8_t sf, uint32_t bwHz);
    void UpdateTypicalDataToaSeconds(double toaSeconds);
    void UpdateTypicalCtrlToaSeconds(double toaSeconds);
    void NotifyTxResult(bool success);
    uint32_t GetFailureCount() const;
    uint32_t GetLastBackoffSlots() const;
    uint32_t GetLastBackoffWindowSlots() const;
    double GetCadLoadEstimate() const;
    uint64_t GetCadBusyEventsLocalPower() const;
    uint64_t GetCadBusyEventsOracle() const;

    Time GetCadDuration() const;
    uint8_t GetDifsCadCount() const;
    bool PerformChannelAssessment();

  private:
    bool PerformCadOnce(Time sampleTime);
    void CleanOldTxHistory();
    void RecordCadResult(bool busy);
    double GetCadLoad() const;
    uint32_t ComputeBackoffWindowSlots() const;
    uint32_t ComputeMaxBackoffSlots() const;
    void UpdateToaEma(double& emaSeconds, double toaSeconds);
    Time ComputeCadDurationFromContext() const;
    double GetCadSensitivityDbm(uint8_t sf) const;

    static constexpr uint32_t kDefaultMaxBackoffSlots = 64;
    static constexpr uint32_t kDefaultMaxBackoffSlotsAbs = 128;

    Ptr<UniformRandomVariable> m_rng;
    Ptr<lorawan::LoraPhy> m_phy;
    Time m_dutyCycleWindow;
    double m_dutyCycleLimit;
    Time m_cadDuration;
    uint8_t m_difsCadCount;
    uint8_t m_backoffWindow;
    std::deque<std::pair<Time, Time>> m_txHistory;
    std::deque<bool> m_cadHistory;
    uint32_t m_cadHistoryWindow;
    uint32_t m_minBackoffSlots;
    uint32_t m_maxBackoffSlots;
    uint32_t m_maxBackoffSlotsAbs;
    bool m_maxBackoffSlotsExplicit;
    uint32_t m_backoffStep;
    uint32_t m_failures;
    uint32_t m_lastBackoffSlots;
    uint32_t m_lastBackoffWindowSlots;
    double m_loadWeight;
    double m_toaEmaDataSeconds;
    double m_toaEmaCtrlSeconds;
    double m_toaEmaAlpha;
    double m_toaMaxFactor;
    bool m_dutyCycleEnabled;
    CadDecisionModel m_cadDecisionModel;
    CadDurationMode m_cadDurationMode;
    uint8_t m_cadSymbols;
    uint8_t m_cadSf;
    uint32_t m_cadBandwidthHz;
    double m_cadSenseMarginDb;
    uint64_t m_cadBusyEventsLocalPower;
    uint64_t m_cadBusyEventsOracle;
};

} // namespace loramesh
} // namespace ns3

#endif /* LORAMESH_MAC_CSMA_CAD_H */
