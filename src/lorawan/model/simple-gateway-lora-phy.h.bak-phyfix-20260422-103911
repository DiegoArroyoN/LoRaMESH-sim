/*
 * Copyright (c) 2017 University of Padova
 *
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * Author: Davide Magrin <magrinda@dei.unipd.it>
 */

#ifndef SIMPLE_GATEWAY_LORA_PHY_H
#define SIMPLE_GATEWAY_LORA_PHY_H

#include "gateway-lora-phy.h"

#include "ns3/mobility-model.h"
#include "ns3/net-device.h"
#include "ns3/node.h"
#include "ns3/nstime.h"
#include "ns3/event-id.h"
#include "ns3/object.h"
#include "ns3/traced-value.h"

#include <cstdint>
#include <deque>
#include <list>
#include <vector>

namespace ns3
{
namespace lorawan
{

class LoraChannel;

/**
 * @ingroup lorawan
 *
 * Class modeling a Lora SX1301 chip.
 */
class SimpleGatewayLoraPhy : public GatewayLoraPhy
{
  public:
    enum ScanStartSfMode : uint8_t
    {
        SCAN_START_SF_MIN = 0,
        SCAN_START_LAST_LOCKED = 1,
    };

    /**
     *  Register this type.
     *  @return The object TypeId.
     */
    static TypeId GetTypeId();

    SimpleGatewayLoraPhy();           //!< Default constructor
    ~SimpleGatewayLoraPhy() override; //!< Destructor

    void StartReceive(Ptr<Packet> packet,
                      double rxPowerDbm,
                      uint8_t sf,
                      Time duration,
                      uint32_t frequencyHz) override;
    void NotifySignalStart(Ptr<Packet> packet,
                           double rxPowerDbm,
                           Time duration,
                           uint32_t frequencyHz,
                           uint64_t signalId);

    void EndReceive(Ptr<Packet> packet, Ptr<LoraInterferenceHelper::Event> event) override;

    void Send(Ptr<Packet> packet,
              LoraTxParameters txParams,
              uint32_t frequencyHz,
              double txPowerDbm) override;

    // SX1276 sensitivity (dBm) for SF7-SF12 @ BW=125kHz (Table 10, SX1276 datasheet)
    // Shadows GatewayLoraPhy::sensitivity which has SX1301 concentrator values
    static const double sensitivity[6];

    uint64_t GetRxScanAttempts() const;
    uint64_t GetRxScanLocks() const;
    uint64_t GetRxScanMissBeforeLock() const;
    uint64_t GetRxPostLockInterferenceFail() const;
    uint64_t GetNoMoreDemodulatorsDrops() const;
    double GetRxAcquisitionDelayMeanSeconds() const;
    double GetRxAcquisitionDelayP95Seconds() const;
    double GetRxScanTimeTotalSeconds() const;
    LoraInterferenceHelper::Stats GetInterferenceStats() const;

  private:
    enum class RxState : uint8_t
    {
        IDLE = 0,
        SCAN = 1,
        RX_LOCK = 2,
    };

    struct PendingSignal
    {
        Ptr<Packet> packet;
        Ptr<LoraInterferenceHelper::Event> event;
        uint8_t sfTrue{7};
        uint32_t freqHz{0};
        double rxPowerDbm{-200.0};
        Time start{Seconds(0)};
        Time end{Seconds(0)};
    };

    bool TryLockOnScanSf(uint8_t sf);
    void StartScanIfNeeded();
    void HandleScanStep();
    void PurgeExpiredPendingSignals();
    void HandleSignalStart(Ptr<Packet> packet,
                           double rxPowerDbm,
                           uint8_t sfLatent,
                           Time duration,
                           uint32_t frequencyHz);
    Time ComputeCadDurationForSf(uint8_t sf) const;
    uint8_t ClampScanSf(uint8_t sf) const;
    uint8_t GetNextScanSf(uint8_t sf) const;
    double GetSensitivityForSf(uint8_t sf) const;

    bool m_enableSfScanRx{true};
    uint8_t m_sfScanMin{7};
    uint8_t m_sfScanMax{12};
    uint8_t m_sfScanCadSymbols{2};
    uint32_t m_sfScanBandwidthHz{125000};
    double m_sfScanCadMarginDb{0.0};
    double m_sfScanEdThresholdDbm{-120.0};
    bool m_sfScanResetOnNewSignal{true};
    ScanStartSfMode m_sfScanStartMode{SCAN_START_SF_MIN};

    RxState m_rxState{RxState::IDLE};
    uint8_t m_scanCurrentSf{7};
    uint8_t m_lastLockedSf{7};
    std::deque<PendingSignal> m_pendingSignals;
    EventId m_scanEvent;
    Ptr<LoraInterferenceHelper::Event> m_lockedEvent;

    uint64_t m_rxScanAttempts{0};
    uint64_t m_rxScanLocks{0};
    uint64_t m_rxScanMissBeforeLock{0};
    uint64_t m_rxPostLockInterferenceFail{0};
    uint64_t m_noMoreDemodulatorsDrops{0};
    Time m_rxScanTimeTotal{Seconds(0)};
    std::vector<double> m_rxAcquisitionDelaySamples;
};

} // namespace lorawan

} // namespace ns3
#endif /* SIMPLE_GATEWAY_LORA_PHY_H */
