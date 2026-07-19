/* SPDX-License-Identifier: GPL-2.0-only */

#include "ns3/double.h"
#include "ns3/dv-cl-routing.h"
#include "ns3/simulator.h"
#include "ns3/test.h"

#include <algorithm>

namespace ns3
{
namespace dvcl
{
namespace test
{

/// Build a NeighborLinkInfo for a synthetic beacon from `nb`.
static NeighborLinkInfo
Link(NodeId nb, uint32_t seq, uint32_t toaUs, uint16_t battMv = 4200)
{
    NeighborLinkInfo l;
    l.neighbor = nb;
    l.sequence = seq;
    l.hops = 1; // as built by the app on RX: tag hops + 1 (IsRouteUsable needs >= 1)
    l.sf = 7;
    l.toaUs = toaUs;
    l.batt_mV = battMv;
    l.scoreX100 = 0;
    return l;
}

/// Build a DvMessage from `origin` advertising `entries`.
static DvMessage
Msg(NodeId origin, uint32_t seq, std::vector<DvEntry> entries)
{
    DvMessage m;
    m.origin = origin;
    m.sequence = seq;
    m.entries = std::move(entries);
    return m;
}

/// One advertised entry (cost255 encoding: lower = better, 0 = poison).
static DvEntry
Adv(NodeId dest, uint16_t advertCost, uint8_t hops = 1)
{
    DvEntry e;
    e.destination = dest;
    e.hops = hops;
    e.sf = 7;
    e.scoreX100 = advertCost;
    e.toaUs = 0;
    e.batt_mV = 4200;
    return e;
}

/**
 * \ingroup dv-cl
 * Route install from a beacon: direct route to the neighbor plus a via
 * route for each advertised entry; costs are positive and monotone
 * (two hops cost more than one).
 */
class DvClRoutingInstallTestCase : public TestCase
{
  public:
    DvClRoutingInstallTestCase()
        : TestCase("dv-cl routing installs direct and via routes with monotone costs")
    {
    }

  private:
    void DoRun() override
    {
        auto r = CreateObject<DvClRouting>();
        r->SetNodeId(0);
        r->UpdateFromDvMsg(Msg(1, 1, {Adv(2, 23)}), Link(1, 1, 102656));

        NS_TEST_ASSERT_MSG_EQ(r->HasRoute(1), true, "direct route to neighbor 1");
        NS_TEST_ASSERT_MSG_EQ(r->HasRoute(2), true, "via route to 2 through 1");
        NS_TEST_ASSERT_MSG_EQ(r->LookupNextHop(1), 1u, "next hop to 1 is 1");
        NS_TEST_ASSERT_MSG_EQ(r->LookupNextHop(2), 1u, "next hop to 2 is 1");
        const double c1 = r->GetRouteCost(1);
        const double c2 = r->GetRouteCost(2);
        NS_TEST_ASSERT_MSG_GT(c1, 0.0, "direct cost positive");
        NS_TEST_ASSERT_MSG_GT(c2, c1, "two hops cost more than one");

        const auto anns = r->GetBestRoutesPueyo(8);
        bool saw1 = false;
        bool saw2 = false;
        for (const auto& a : anns)
        {
            if (a.destination == 1 && a.scoreX100 > 0)
            {
                saw1 = true;
            }
            if (a.destination == 2 && a.scoreX100 > 0)
            {
                saw2 = true;
            }
        }
        NS_TEST_ASSERT_MSG_EQ(saw1, true, "announces route to 1");
        NS_TEST_ASSERT_MSG_EQ(saw2, true, "announces route to 2");
        Simulator::Destroy();
    }
};

/**
 * \ingroup dv-cl
 * Poison (advertised score 0) withdraws the via route.
 */
class DvClRoutingPoisonTestCase : public TestCase
{
  public:
    DvClRoutingPoisonTestCase()
        : TestCase("dv-cl routing poison (score 0) withdraws the route")
    {
    }

  private:
    void DoRun() override
    {
        auto r = CreateObject<DvClRouting>();
        r->SetNodeId(0);
        r->UpdateFromDvMsg(Msg(1, 1, {Adv(2, 23)}), Link(1, 1, 102656));
        NS_TEST_ASSERT_MSG_EQ(r->HasRoute(2), true, "route to 2 installed");

        r->UpdateFromDvMsg(Msg(1, 2, {Adv(2, 0)}), Link(1, 2, 102656));
        NS_TEST_ASSERT_MSG_EQ(r->HasRoute(2), false, "route to 2 withdrawn by poison");
        NS_TEST_ASSERT_MSG_EQ(r->HasRoute(1), true, "direct route to 1 survives");
        Simulator::Destroy();
    }
};

/**
 * \ingroup dv-cl
 * A clearly better alternative switches the next hop; a tie does not
 * (hysteresis: candidate must beat the installed route by more than
 * RouteSwitchMinDelta).
 */
class DvClRoutingSwitchHysteresisTestCase : public TestCase
{
  public:
    DvClRoutingSwitchHysteresisTestCase()
        : TestCase("dv-cl routing switches on clear improvement, holds on ties")
    {
    }

  private:
    void DoRun() override
    {
        auto r = CreateObject<DvClRouting>();
        r->SetNodeId(0);
        // Neighbor 1: mediocre path to 2 (high advertised cost, slow link).
        r->UpdateFromDvMsg(Msg(1, 1, {Adv(2, 60)}), Link(1, 1, 143360));
        NS_TEST_ASSERT_MSG_EQ(r->LookupNextHop(2), 1u, "initially via 1");
        // Neighbor 3: clearly better (fast link, cheap advert).
        r->UpdateFromDvMsg(Msg(3, 1, {Adv(2, 10)}), Link(3, 1, 25856));
        NS_TEST_ASSERT_MSG_EQ(r->LookupNextHop(2), 3u, "switches to clearly better 3");
        // Neighbor 1 re-advertises the same mediocre path: must NOT flap back.
        r->UpdateFromDvMsg(Msg(1, 2, {Adv(2, 60)}), Link(1, 2, 143360));
        NS_TEST_ASSERT_MSG_EQ(r->LookupNextHop(2), 3u, "tie/worse does not flap");
        Simulator::Destroy();
    }
};

/**
 * \ingroup dv-cl
 * Routes expire after the configured timeout and are purged.
 */
class DvClRoutingExpiryTestCase : public TestCase
{
  public:
    DvClRoutingExpiryTestCase()
        : TestCase("dv-cl routing expires routes after the timeout")
    {
    }

  private:
    void Check(Ptr<DvClRouting> r)
    {
        r->PurgeExpiredRoutes();
        NS_TEST_ASSERT_MSG_EQ(r->HasRoute(1), false, "direct route expired");
        NS_TEST_ASSERT_MSG_EQ(r->HasRoute(2), false, "via route expired");
    }

    void DoRun() override
    {
        auto r = CreateObject<DvClRouting>();
        r->SetNodeId(0);
        r->SetRouteTimeout(Seconds(5));
        r->UpdateFromDvMsg(Msg(1, 1, {Adv(2, 23)}), Link(1, 1, 102656));
        NS_TEST_ASSERT_MSG_EQ(r->HasRoute(2), true, "installed at t=0");
        Simulator::Schedule(Seconds(60), &DvClRoutingExpiryTestCase::Check, this, r);
        Simulator::Run();
        Simulator::Destroy();
    }
};

/// Test double: a metric that prices every link at a constant.
class ConstMetric : public DvClRoutingMetric
{
  public:
    explicit ConstMetric(double cost)
        : m_cost(cost)
    {
    }

    double ComputeLinkCost(const LinkInputs&) const override
    {
        return m_cost;
    }

  private:
    double m_cost;
};

/// Mirror of DvClRouting::BattMvToEFrac for expected-value computation.
static double
EFrac(uint16_t mv)
{
    if (mv == 0)
    {
        return 1.0;
    }
    const double f = (static_cast<double>(mv) - 3000.0) / 1200.0;
    return std::max(0.0, std::min(1.0, f));
}

/**
 * \ingroup dv-cl
 * F5 step 2b safety net: the installed direct-route rawMetric equals the
 * standalone DvClCompositeMetric on identical inputs (exact doubles);
 * the legacy routing attributes forward into the built-in metric; and a
 * custom metric plugged through SetMetric drives the cost (F6.1 seam).
 */
class DvClRoutingMetricSeamTestCase : public TestCase
{
  public:
    DvClRoutingMetricSeamTestCase()
        : TestCase("dv-cl routing metric seam: numeric equivalence, attribute "
                   "forwarding, custom metric")
    {
    }

  private:
    static double
    InstalledDirectRaw(Ptr<DvClRouting> r, NodeId nb)
    {
        for (const auto& e : r->GetPrimaryRoutesSnapshot())
        {
            if (e.destination == nb)
            {
                return e.rawMetric;
            }
        }
        return -1.0;
    }

    void DoRun() override
    {
        // (1) Numeric equivalence over a sweep of link inputs.
        auto standalone = CreateObject<DvClCompositeMetric>();
        uint32_t seq = 1;
        const uint32_t toas[] = {25856, 102656, 143360, 500000};
        const uint8_t sfs[] = {7, 9, 12};
        const uint16_t mvs[] = {0, 3100, 3400, 3800, 4200};
        for (uint32_t toa : toas)
        {
            for (uint8_t sf : sfs)
            {
                for (uint16_t mv : mvs)
                {
                    auto r = CreateObject<DvClRouting>();
                    r->SetNodeId(0);
                    NeighborLinkInfo l = Link(1, seq, toa, mv);
                    l.sf = sf;
                    r->UpdateFromDvMsg(Msg(1, seq, {}), l);
                    ++seq;
                    LinkInputs in;
                    in.toaUs = toa;
                    in.sf = sf;
                    in.energyFraction = EFrac(mv);
                    const double expected = standalone->ComputeLinkCost(in);
                    const double got = InstalledDirectRaw(r, 1);
                    NS_TEST_ASSERT_MSG_EQ_TOL(got, expected, 1e-12,
                                              "rawMetric == metric at toa=" << toa
                                                  << " sf=" << unsigned(sf) << " mv=" << mv);
                }
            }
        }

        // (2) Legacy attribute forwarding: WEnergy=0 removes the penalty.
        {
            auto r = CreateObject<DvClRouting>();
            r->SetNodeId(0);
            r->SetAttribute("CompositeWEnergy", DoubleValue(0.0));
            r->UpdateFromDvMsg(Msg(1, 1, {}), Link(1, 1, 102656, 3100)); // low battery
            LinkInputs in;
            in.toaUs = 102656;
            in.sf = 7;
            in.energyFraction = 1.0; // no penalty expected
            const double expected = standalone->ComputeLinkCost(in);
            NS_TEST_ASSERT_MSG_EQ_TOL(InstalledDirectRaw(r, 1), expected, 1e-12,
                                      "WEnergy=0 via legacy attribute kills the penalty");
            DoubleValue back;
            r->GetAttribute("CompositeWEnergy", back);
            NS_TEST_ASSERT_MSG_EQ_TOL(back.Get(), 0.0, 1e-12, "attribute reads back");
        }

        // (3) Custom metric plugged through the seam drives the cost.
        {
            auto r = CreateObject<DvClRouting>();
            r->SetNodeId(0);
            r->SetMetric(CreateObject<ConstMetric>(0.42));
            r->UpdateFromDvMsg(Msg(1, 1, {}), Link(1, 1, 102656));
            NS_TEST_ASSERT_MSG_EQ_TOL(InstalledDirectRaw(r, 1), 0.42, 1e-12,
                                      "custom metric prices the direct link");
        }
        Simulator::Destroy();
    }
};

/**
 * \ingroup dv-cl
 * The dv-cl-routing test suite.
 */
class DvClRoutingTestSuite : public TestSuite
{
  public:
    DvClRoutingTestSuite()
        : TestSuite("dv-cl-routing", Type::UNIT)
    {
        AddTestCase(new DvClRoutingInstallTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClRoutingPoisonTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClRoutingSwitchHysteresisTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClRoutingExpiryTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClRoutingMetricSeamTestCase, TestCase::Duration::QUICK);
    }
};

static DvClRoutingTestSuite g_dvClRoutingTestSuite; //!< static suite registration

} // namespace test
} // namespace dvcl
} // namespace ns3
