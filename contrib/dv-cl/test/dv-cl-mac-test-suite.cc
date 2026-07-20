/* SPDX-License-Identifier: GPL-2.0-only */

#include "ns3/double.h"
#include "ns3/string.h"
#include "ns3/boolean.h"
#include "ns3/dv-cl-mac-csma-cad.h"
#include "ns3/random-variable-stream.h"
#include "ns3/simulator.h"
#include "ns3/test.h"

namespace ns3
{
namespace dvcl
{
namespace test
{

/**
 * \ingroup dv-cl
 * Duty-cycle budget semantics of the ported MAC, as executable spec:
 * rolling-overlap accounting over [now - W, now] (only elapsed airtime
 * counts), projected admission (used + toa/W <= limit), and budget
 * recovery as records age out of the rolling window.
 */
class DvClMacDutyRollingTestCase : public TestCase
{
  public:
    DvClMacDutyRollingTestCase()
        : TestCase("dv-cl mac duty: rolling accounting, projected admission, recovery")
    {
    }

  private:
    Ptr<DvClCsmaCadMac> m_mac;

    void AtT10()
    {
        // tx [0, 3.6] fully elapsed: used = 3.6/3600 = 0.001.
        NS_TEST_ASSERT_MSG_EQ_TOL(m_mac->GetDutyCycleUsed(), 0.001, 1e-9, "used at t=10");
        NS_TEST_ASSERT_MSG_EQ(m_mac->CanTransmitNow(32.4), true,
                              "projected exactly at the limit is allowed");
        NS_TEST_ASSERT_MSG_EQ(m_mac->CanTransmitNow(32.5), false,
                              "projected above the limit is blocked");
        m_mac->NotifyTxStart(32.4);
    }

    void AtT50()
    {
        // both tx fully elapsed: used = (3.6+32.4)/3600 = 0.01 (at cap).
        NS_TEST_ASSERT_MSG_EQ_TOL(m_mac->GetDutyCycleUsed(), 0.01, 1e-9, "used at cap");
        NS_TEST_ASSERT_MSG_EQ(m_mac->CanTransmitNow(0.4), false, "no headroom at cap");
    }

    void AtT3610()
    {
        // record [0,3.6] aged out; [10,42.4] fully inside: used = 32.4/3600.
        NS_TEST_ASSERT_MSG_EQ_TOL(m_mac->GetDutyCycleUsed(), 0.009, 1e-9,
                                  "first record aged out");
        NS_TEST_ASSERT_MSG_EQ(m_mac->CanTransmitNow(3.6), true, "recovered headroom");
        NS_TEST_ASSERT_MSG_EQ(m_mac->CanTransmitNow(3.7), false, "still bounded");
    }

    void AtT3630()
    {
        // window start = 30: overlap of [10,42.4] is [30,42.4] = 12.4 s.
        NS_TEST_ASSERT_MSG_EQ_TOL(m_mac->GetDutyCycleUsed(), 12.4 / 3600.0, 1e-9,
                                  "partial overlap of the sliding window");
        NS_TEST_ASSERT_MSG_EQ(m_mac->CanTransmitNow(20.0), true,
                              "recovery grows as the window slides");
    }

    void DoRun() override
    {
        m_mac = CreateObject<DvClCsmaCadMac>();
        // These cases pin the legacy trailing-sum discipline; the default is now
        // the ETSI time-off-air gate, covered by DvClMacTimeOffAirTestCase.
        m_mac->SetAttribute("DutyEnforcement", StringValue("sliding_window"));
        // defaults: limit 0.01, window 1h, enabled.
        m_mac->NotifyTxStart(3.6);
        Simulator::Schedule(Seconds(10), &DvClMacDutyRollingTestCase::AtT10, this);
        Simulator::Schedule(Seconds(50), &DvClMacDutyRollingTestCase::AtT50, this);
        Simulator::Schedule(Seconds(3610), &DvClMacDutyRollingTestCase::AtT3610, this);
        Simulator::Schedule(Seconds(3630), &DvClMacDutyRollingTestCase::AtT3630, this);
        Simulator::Run();
        Simulator::Destroy();
        m_mac = nullptr;
    }
};

/**
 * \ingroup dv-cl
 * DutyCycleEnabled=false disables the gate entirely.
 */
class DvClMacDutyDisabledTestCase : public TestCase
{
  public:
    DvClMacDutyDisabledTestCase()
        : TestCase("dv-cl mac duty disabled: gate always open")
    {
    }

  private:
    void DoRun() override
    {
        auto mac = CreateObject<DvClCsmaCadMac>();
        mac->SetAttribute("DutyCycleEnabled", BooleanValue(false));
        mac->NotifyTxStart(1000.0);
        NS_TEST_ASSERT_MSG_EQ(mac->CanTransmitNow(1e6), true, "disabled gate never blocks");
        Simulator::Destroy();
    }
};

/**
 * \ingroup dv-cl
 * Backoff draws are bounded by the effective window, which never
 * exceeds the absolute cap.
 */
class DvClMacBackoffBoundsTestCase : public TestCase
{
  public:
    DvClMacBackoffBoundsTestCase()
        : TestCase("dv-cl mac backoff draws bounded by the effective window")
    {
    }

  private:
    void DoRun() override
    {
        auto mac = CreateObject<DvClCsmaCadMac>();
        mac->SetRandomStream(CreateObject<UniformRandomVariable>());
        const uint32_t cap = mac->GetMaxBackoffSlotsAbs();
        for (int i = 0; i < 100; ++i)
        {
            const uint32_t slots = mac->GetBackoffSlots();
            const uint32_t window = mac->GetLastBackoffWindowSlots();
            NS_TEST_ASSERT_MSG_GT(window, 0u, "window at least one slot");
            NS_TEST_ASSERT_MSG_LT(slots, window, "draw within the window");
            NS_TEST_ASSERT_MSG_LT_OR_EQ(window, cap, "window bounded by the absolute cap");
        }
        Simulator::Destroy();
    }
};

/**
 * \ingroup dv-cl
 * The dv-cl-mac test suite.
 */
/**
 * \ingroup dv-cl
 * rief The default duty gate follows ETSI EN 300 220: after transmitting for
 * T the radio stays silent until T/limit has elapsed, which is what ns-3's own
 * lorawan module does (LogicalLoraChannelHelper::AddEvent) and what bounds the
 * ratio over every window rather than only at transmission instants.
 */
class DvClMacTimeOffAirTestCase : public TestCase
{
  public:
    DvClMacTimeOffAirTestCase()
        : TestCase("dv-cl mac duty: ETSI time-off-air gate")
    {
    }

  private:
    Ptr<DvClCsmaCadMac> m_mac;

    void AfterTx()
    {
        // 1.8 s of air at 1% owes 180 s of silence measured from the start.
        NS_TEST_ASSERT_MSG_EQ(m_mac->CanTransmitNow(0.1), false, "still off air at t=100");
    }

    void WhenFree()
    {
        NS_TEST_ASSERT_MSG_EQ(m_mac->CanTransmitNow(0.1), true, "free again at t=181");
    }

    void DoRun() override
    {
        m_mac = CreateObject<DvClCsmaCadMac>();
        m_mac->SetAttribute("DutyCycleLimit", DoubleValue(0.01));
        NS_TEST_ASSERT_MSG_EQ(m_mac->GetDutyEnforcement(), "time_off_air", "default is ETSI");
        NS_TEST_ASSERT_MSG_EQ(m_mac->CanTransmitNow(1.8), true, "idle radio may transmit");
        m_mac->NotifyTxStart(1.8);
        Simulator::Schedule(Seconds(100), &DvClMacTimeOffAirTestCase::AfterTx, this);
        Simulator::Schedule(Seconds(181), &DvClMacTimeOffAirTestCase::WhenFree, this);
        Simulator::Stop(Seconds(200));
        Simulator::Run();
        Simulator::Destroy();
        m_mac = nullptr;
    }
};

class DvClMacTestSuite : public TestSuite
{
  public:
    DvClMacTestSuite()
        : TestSuite("dv-cl-mac", Type::UNIT)
    {
        AddTestCase(new DvClMacTimeOffAirTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClMacDutyRollingTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClMacDutyDisabledTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClMacBackoffBoundsTestCase, TestCase::Duration::QUICK);
    }
};

static DvClMacTestSuite g_dvClMacTestSuite; //!< static suite registration

} // namespace test
} // namespace dvcl
} // namespace ns3
