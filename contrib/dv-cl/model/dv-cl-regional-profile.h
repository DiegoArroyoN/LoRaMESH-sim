/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_REGIONAL_PROFILE_H
#define DV_CL_REGIONAL_PROFILE_H

#include "ns3/nstime.h"
#include "ns3/object.h"

#include <cstdint>
#include <string>

namespace ns3
{
namespace dvcl
{

/**
 * \ingroup dv-cl
 * \brief The regulatory and PHY parameters of one region.
 *
 * Third seam of the module, alongside the metric and the measurement sink:
 * a region is an object, so adding one is subclassing rather than editing
 * the protocol.
 *
 * Regions differ in how they bound channel occupancy, and the two rules
 * are not variations of one another:
 *
 * - A **duty cycle** (EU868) bounds the *fraction* of time a radio may
 *   occupy the channel. It is paid after the fact: transmit for T and
 *   stay silent in proportion (ETSI EN 300 220).
 * - A **dwell time** (US915) bounds the *length of a single*
 *   transmission, with no limit on how often. It is checked before the
 *   fact: a packet whose airtime exceeds the cap may never be sent at
 *   all, at any spreading factor.
 *
 * Both are expressed here so a region can impose either, both, or
 * neither.
 */
class DvClRegionalProfile : public Object
{
  public:
    static TypeId GetTypeId();
    ~DvClRegionalProfile() override = default;

    /// Short region identifier, e.g. "EU868".
    virtual std::string GetName() const = 0;

    /// Carrier frequency in Hz.
    virtual uint32_t GetFrequencyHz() const = 0;
    /// Channel bandwidth in Hz.
    virtual uint32_t GetBandwidthHz() const = 0;
    /// Coding rate as the LoRa index (1 = 4/5).
    virtual uint8_t GetCodingRate() const = 0;

    /// Lowest usable spreading factor.
    virtual uint8_t GetSfMin() const = 0;
    /// Highest usable spreading factor.
    virtual uint8_t GetSfMax() const = 0;
    /// Highest permitted transmit power, dBm.
    virtual double GetMaxTxPowerDbm() const = 0;

    /**
     * \brief Duty cycle as a fraction; 1.0 where the region imposes none.
     *
     * A region without a duty cycle returns 1.0 rather than 0, so callers
     * can divide by it unconditionally.
     */
    virtual double GetDutyCycleLimit() const = 0;

    /**
     * \brief Longest single transmission the region permits.
     *
     * Time::Max() where the region caps no such thing. A packet whose
     * airtime exceeds this is refused outright: unlike a duty cycle, no
     * amount of waiting makes it legal.
     */
    virtual Time GetMaxDwellTime() const = 0;

    /// True when \p toa fits under the dwell cap.
    bool FitsDwellTime(Time toa) const { return toa <= GetMaxDwellTime(); }
};

/**
 * \ingroup dv-cl
 * \brief EU868: 868 MHz, 125 kHz, 1% duty cycle, no dwell cap.
 *
 * The module's default, and the region every published DV-CL result was
 * produced under.
 */
class DvClEu868Profile : public DvClRegionalProfile
{
  public:
    static TypeId GetTypeId();

    std::string GetName() const override { return "EU868"; }

    uint32_t GetFrequencyHz() const override { return 868000000; }
    uint32_t GetBandwidthHz() const override { return 125000; }
    uint8_t GetCodingRate() const override { return 1; }

    uint8_t GetSfMin() const override { return 7; }
    uint8_t GetSfMax() const override { return 12; }
    double GetMaxTxPowerDbm() const override { return 14.0; }

    double GetDutyCycleLimit() const override { return m_dutyCycleLimit; }
    Time GetMaxDwellTime() const override { return Time::Max(); }

  private:
    double m_dutyCycleLimit{0.01}; //!< 1% on the sub-bands used here
};

/**
 * \ingroup dv-cl
 * \brief US915: 902-928 MHz, 125 kHz, 400 ms dwell time, no duty cycle.
 *
 * The uplink spreading factors stop at SF10 precisely because SF11 and
 * SF12 cannot fit a useful payload inside the dwell cap.
 */
class DvClUs915Profile : public DvClRegionalProfile
{
  public:
    static TypeId GetTypeId();

    std::string GetName() const override { return "US915"; }

    uint32_t GetFrequencyHz() const override { return 903900000; }
    uint32_t GetBandwidthHz() const override { return 125000; }
    uint8_t GetCodingRate() const override { return 1; }

    uint8_t GetSfMin() const override { return 7; }
    uint8_t GetSfMax() const override { return 10; }
    double GetMaxTxPowerDbm() const override { return 30.0; }

    double GetDutyCycleLimit() const override { return 1.0; } // none
    Time GetMaxDwellTime() const override { return MilliSeconds(400); }
};

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_REGIONAL_PROFILE_H */
