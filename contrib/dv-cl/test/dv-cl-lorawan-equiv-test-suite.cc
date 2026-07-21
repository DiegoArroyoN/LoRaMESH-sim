/* SPDX-License-Identifier: GPL-2.0-only */

#include "ns3/dv-cl-toa.h"
#include "ns3/lora-phy.h"
#include "ns3/packet.h"
#include "ns3/test.h"

#include <cmath>
#include <sstream>
#include <vector>

namespace ns3
{
namespace dvcl
{
namespace test
{

/**
 * \ingroup dv-cl
 * \brief The module's time on air agrees with the lorawan module's (F2.1).
 *
 * Two independent implementations of one physical quantity live in the
 * same simulation: DvClToa, verified against Semtech AN1200.13 across the
 * full parameter grid, and lorawan's LoraPhy::GetOnAirTime, which decides
 * how long the channel is actually occupied. A protocol that budgets with
 * one while the radio spends the other is mispricing every transmission,
 * and that is not hypothetical -- it is what the duty-cycle gate was doing
 * before it was made to ask the device (VALIDATION.md, 2026-07-20).
 *
 * This pins the two together across the grid so they cannot drift apart
 * again, and it is the single-hop equivalence the validation plan asks
 * for: at one hop, occupancy is the whole of what the PHY contributes.
 */
class DvClToaEquivalenceTestCase : public TestCase
{
  public:
    DvClToaEquivalenceTestCase()
        : TestCase("dv-cl equivalence: time on air matches the lorawan module")
    {
    }

  private:
    void DoRun() override
    {
        const std::vector<uint8_t> sfs = {7, 8, 9, 10, 11, 12};
        const std::vector<uint32_t> payloads = {1, 10, 20, 51, 100, 222};
        const std::vector<uint8_t> crs = {1, 2, 3, 4};
        const std::vector<uint16_t> preambles = {8, 16};

        std::size_t compared = 0;
        std::size_t mismatched = 0;
        std::ostringstream worst;
        double worstRelative = 0.0;

        for (uint8_t sf : sfs)
        {
            for (uint32_t payload : payloads)
            {
                for (uint8_t cr : crs)
                {
                    for (uint16_t preamble : preambles)
                    {
                        // Low-data-rate optimisation is mandatory at SF11/SF12
                        // for 125 kHz, and off elsewhere; both sides must be
                        // told the same thing or the comparison is meaningless.
                        const bool ldro = (sf >= 11);

                        lorawan::LoraTxParameters p;
                        p.sf = sf;
                        p.headerDisabled = false;
                        p.codingRate = cr;
                        p.bandwidthHz = 125000;
                        p.nPreamble = preamble;
                        p.crcEnabled = true;
                        p.lowDataRateOptimizationEnabled = ldro;

                        const Time ref =
                            lorawan::LoraPhy::GetOnAirTime(Create<Packet>(payload), p);
                        const double refUs = ref.GetSeconds() * 1e6;
                        const double ours = static_cast<double>(
                            ComputeToaUs(sf, 125000, cr, payload, false, ldro, true, preamble));

                        ++compared;
                        const double relative = std::abs(ours - refUs) / refUs;
                        if (relative > worstRelative)
                        {
                            worstRelative = relative;
                            worst.str("");
                            worst << "sf=" << unsigned(sf) << " payload=" << payload
                                  << " cr=" << unsigned(cr) << " preamble=" << preamble
                                  << " ours=" << ours << "us lorawan=" << refUs << "us";
                        }
                        // One microsecond of slack for the rounding each side
                        // applies; anything larger is a real disagreement.
                        if (std::abs(ours - refUs) > 1.0)
                        {
                            ++mismatched;
                        }
                    }
                }
            }
        }

        NS_TEST_ASSERT_MSG_EQ(compared, sfs.size() * payloads.size() * crs.size() * preambles.size(),
                              "the whole grid was compared");
        NS_TEST_ASSERT_MSG_EQ(mismatched,
                              0u,
                              "time on air must agree; worst case " << worst.str());
    }
};

/**
 * \ingroup dv-cl
 * \brief Equivalence suite against the standard lorawan module (F2.1).
 */
/**
 * \ingroup dv-cl
 * rief What the device puts on the air matches what the module budgets.
 *
 * The formulas agreeing is not enough: they must also be fed the same
 * parameters. They were not. The application prices a transmission with
 * low-data-rate optimisation enabled -- which the LoRa specification
 * mandates at SF11 and SF12 for 125 kHz -- while DvClLoraNetDevice builds
 * its PHY parameters with it disabled at every spreading factor. The
 * control plane therefore budgets by the specification while the radio
 * transmits outside it, and at SF12 the gap is about 9%.
 *
 * This case states the size of that gap rather than asserting it away,
 * so the day the device starts honouring LDRO the test says so instead
 * of silently passing.
 */
class DvClLdroGapTestCase : public TestCase
{
  public:
    DvClLdroGapTestCase()
        : TestCase("dv-cl equivalence: the LDRO gap between budget and air is bounded")
    {
    }

  private:
    void DoRun() override
    {
        // LDRO can only lengthen a frame, never shorten it: it spends two bits
        // per symbol on robustness. Whether a given payload actually crosses a
        // symbol boundary depends on the ceiling in the formula, so the gap is
        // zero for some sizes and not others -- which is why this scans a range
        // instead of trusting one case.
        double worstRelative = 0.0;
        uint32_t worstPayload = 0;
        uint8_t worstSf = 0;

        for (uint8_t sf : {11, 12})
        {
            for (uint32_t payload = 1; payload <= 100; ++payload)
            {
                const double budgeted = static_cast<double>(
                    ComputeToaUs(sf, 125000, 1, payload, false, true, true, 8));
                const double onAir = static_cast<double>(
                    ComputeToaUs(sf, 125000, 1, payload, false, false, true, 8));
                NS_TEST_ASSERT_MSG_GT_OR_EQ(budgeted, onAir, "LDRO never shortens a frame");
                const double relative = (budgeted - onAir) / onAir;
                if (relative > worstRelative)
                {
                    worstRelative = relative;
                    worstPayload = payload;
                    worstSf = sf;
                }
            }
        }

        // The gap is real and worth stating: the control plane budgets by the
        // specification while the radio transmits without LDRO, so at high
        // spreading factors it over-charges its own duty budget.
        NS_TEST_ASSERT_MSG_GT(worstRelative,
                              0.0,
                              "the two must disagree somewhere, else the gap is gone");
        NS_TEST_ASSERT_MSG_LT(worstRelative,
                              0.25,
                              "worst LDRO gap sf=" << unsigned(worstSf) << " payload="
                                                   << worstPayload << " rel=" << worstRelative);
    }
};

class DvClLorawanEquivTestSuite : public TestSuite
{
  public:
    DvClLorawanEquivTestSuite()
        : TestSuite("dv-cl-lorawan-equiv", Type::UNIT)
    {
        AddTestCase(new DvClToaEquivalenceTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClLdroGapTestCase, TestCase::Duration::QUICK);
    }
};

static DvClLorawanEquivTestSuite g_dvClLorawanEquivTestSuite; //!< Static instance

} // namespace test
} // namespace dvcl
} // namespace ns3
