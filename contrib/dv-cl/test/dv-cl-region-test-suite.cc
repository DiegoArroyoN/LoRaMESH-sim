/* SPDX-License-Identifier: GPL-2.0-only */

#include "ns3/double.h"
#include "ns3/dv-cl-mac-csma-cad.h"
#include "ns3/dv-cl-regional-profile.h"
#include "ns3/dv-cl-toa.h"
#include "ns3/simulator.h"
#include "ns3/string.h"
#include "ns3/test.h"

using namespace ns3;
using namespace ns3::dvcl;

/**
 * \ingroup dv-cl
 * \brief EU868 is what the module has always done: 868 MHz, 125 kHz, 1%.
 *
 * Pins the default region against the constants the published results
 * were produced with, so installing the profile abstraction cannot have
 * moved them.
 */
class DvClRegionEu868TestCase : public TestCase
{
  public:
    DvClRegionEu868TestCase()
        : TestCase("dv-cl region: EU868 matches the module's historical constants")
    {
    }

  private:
    void DoRun() override
    {
        auto eu = CreateObject<DvClEu868Profile>();
        NS_TEST_ASSERT_MSG_EQ(eu->GetName(), "EU868", "name");
        NS_TEST_ASSERT_MSG_EQ(eu->GetFrequencyHz(), 868000000, "carrier");
        NS_TEST_ASSERT_MSG_EQ(eu->GetBandwidthHz(), 125000, "bandwidth");
        NS_TEST_ASSERT_MSG_EQ(eu->GetCodingRate(), 1, "coding rate 4/5");
        NS_TEST_ASSERT_MSG_EQ(eu->GetSfMin(), 7, "SF floor");
        NS_TEST_ASSERT_MSG_EQ(eu->GetSfMax(), 12, "SF ceiling");
        NS_TEST_ASSERT_MSG_EQ_TOL(eu->GetDutyCycleLimit(), 0.01, 1e-12, "1% duty");
        // No dwell cap: an SF12 packet is long but legal here.
        NS_TEST_ASSERT_MSG_EQ(eu->FitsDwellTime(Seconds(1.8)), true, "no dwell cap in EU868");
    }
};

/**
 * \ingroup dv-cl
 * \brief US915 bounds transmission length, not transmission frequency.
 *
 * The two regimes are structurally different and the test says so: US915
 * has no duty cycle at all, and its 400 ms dwell cap makes long packets
 * illegal outright rather than merely expensive.
 */
class DvClRegionUs915TestCase : public TestCase
{
  public:
    DvClRegionUs915TestCase()
        : TestCase("dv-cl region: US915 dwell cap bounds a single transmission")
    {
    }

  private:
    void DoRun() override
    {
        auto us = CreateObject<DvClUs915Profile>();
        NS_TEST_ASSERT_MSG_EQ_TOL(us->GetDutyCycleLimit(), 1.0, 1e-12, "no duty cycle");
        NS_TEST_ASSERT_MSG_EQ(us->GetMaxDwellTime(), MilliSeconds(400), "400 ms dwell");
        NS_TEST_ASSERT_MSG_EQ(us->GetSfMax(), 10, "SF stops where dwell does");

        NS_TEST_ASSERT_MSG_EQ(us->FitsDwellTime(MilliSeconds(390)), true, "under the cap");
        NS_TEST_ASSERT_MSG_EQ(us->FitsDwellTime(MilliSeconds(410)), false, "over the cap");

        // The SF ceiling is not arbitrary: a 20-byte payload at SF11 already
        // overruns the cap, which is why uplinks stop at SF10.
        const double sf11Us = ComputeToaUs(11, 125000, 1, 20, false, true, true, 8);
        NS_TEST_ASSERT_MSG_EQ(us->FitsDwellTime(Seconds(sf11Us / 1e6)),
                              false,
                              "SF11 does not fit the dwell cap");
    }
};

/**
 * \ingroup dv-cl
 * \brief A MAC obeys the region installed on it.
 *
 * Under US915 the gate refuses a transmission longer than the dwell cap
 * outright -- waiting does not make it legal -- while admitting a short
 * one back to back, since the region imposes no duty cycle.
 */
class DvClRegionMacTestCase : public TestCase
{
  public:
    DvClRegionMacTestCase()
        : TestCase("dv-cl region: the MAC applies the installed region's rules")
    {
    }

  private:
    Ptr<DvClCsmaCadMac> m_us;
    Ptr<DvClCsmaCadMac> m_eu;

    /// Just after a 0.3 s packet: US915 owes nothing beyond the packet itself.
    void UsAfterPacket()
    {
        NS_TEST_ASSERT_MSG_EQ(m_us->CanTransmitNow(0.3),
                              true,
                              "US915 is free once the packet ends");
    }

    /// Same moment for EU868, which still owes the bulk of 180 s.
    void EuAfterPacket()
    {
        NS_TEST_ASSERT_MSG_EQ(m_eu->CanTransmitNow(0.1),
                              false,
                              "EU868 owes silence long after the packet ends");
    }

    void DoRun() override
    {
        m_us = CreateObject<DvClCsmaCadMac>();
        m_us->SetRegionalProfile(CreateObject<DvClUs915Profile>());
        NS_TEST_ASSERT_MSG_EQ_TOL(m_us->GetDutyCycleLimit(),
                                  1.0,
                                  1e-12,
                                  "region sets the duty cycle");

        // A dwell cap is a hard refusal, not a wait: no delay makes it legal.
        NS_TEST_ASSERT_MSG_EQ(m_us->CanTransmitNow(0.5), false, "over dwell is refused");
        NS_TEST_ASSERT_MSG_EQ(m_us->CanTransmitNow(0.3), true, "under dwell is admitted");

        m_eu = CreateObject<DvClCsmaCadMac>();
        m_eu->SetRegionalProfile(CreateObject<DvClEu868Profile>());
        NS_TEST_ASSERT_MSG_EQ_TOL(m_eu->GetDutyCycleLimit(), 0.01, 1e-12, "EU868 is 1%");
        NS_TEST_ASSERT_MSG_EQ(m_eu->CanTransmitNow(1.8),
                              true,
                              "a packet too long for US915 is legal in EU868");

        m_us->NotifyTxStart(0.3);
        m_eu->NotifyTxStart(1.8);

        // The regimes separate once the packets are over: the US915 radio is
        // busy only while transmitting, the EU868 one owes 100x that in silence.
        Simulator::Schedule(Seconds(0.31), &DvClRegionMacTestCase::UsAfterPacket, this);
        Simulator::Schedule(Seconds(2.0), &DvClRegionMacTestCase::EuAfterPacket, this);
        Simulator::Stop(Seconds(3));
        Simulator::Run();
        Simulator::Destroy();
        m_us = nullptr;
        m_eu = nullptr;
    }
};

/**
 * \ingroup dv-cl
 * \brief Regional profile suite.
 */
class DvClRegionTestSuite : public TestSuite
{
  public:
    DvClRegionTestSuite()
        : TestSuite("dv-cl-region", Type::UNIT)
    {
        AddTestCase(new DvClRegionEu868TestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClRegionUs915TestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClRegionMacTestCase, TestCase::Duration::QUICK);
    }
};

static DvClRegionTestSuite g_dvClRegionTestSuite; //!< Static instance
