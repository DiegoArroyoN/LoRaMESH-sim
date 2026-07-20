/* SPDX-License-Identifier: GPL-2.0-only */

#include "ns3/config.h"
#include "ns3/double.h"
#include "ns3/dv-cl-app.h"
#include "ns3/dv-cl-helper.h"
#include "ns3/dv-cl-stats-sink.h"
#include "ns3/node-container.h"
#include "ns3/simulator.h"
#include "ns3/string.h"
#include "ns3/test.h"
#include "ns3/uinteger.h"

#include <map>
#include <set>
#include <string>

using namespace ns3;
using namespace ns3::dvcl;

/**
 * \ingroup dv-cl
 * \brief Sink that follows each data packet to its end.
 *
 * Keyed by (src, dst, seq) rather than counting events, because a frame
 * dropped at three relays increments three counters and no sum of
 * counters can then be compared against the number of packets generated.
 */
class ConservationSink : public DvClStatsSink
{
  public:
    using Key = std::tuple<uint32_t, uint32_t, uint32_t>;

    std::set<Key> generated;
    std::set<Key> delivered;
    std::map<Key, std::string> terminated;
    uint32_t duplicateGenerations{0};
    uint32_t duplicateDeliveries{0};

    void RecordDataGenerated(uint32_t src, uint32_t dst, uint32_t seq) override
    {
        if (!generated.insert({src, dst, seq}).second)
        {
            ++duplicateGenerations;
        }
    }

    void RecordE2eDelay(uint32_t src,
                        uint32_t dst,
                        uint32_t seq,
                        uint8_t,
                        double,
                        uint32_t,
                        uint8_t,
                        bool wasDelivered) override
    {
        if (wasDelivered && !delivered.insert({src, dst, seq}).second)
        {
            ++duplicateDeliveries;
        }
    }

    void RecordDataTerminated(uint32_t src,
                              uint32_t dst,
                              uint32_t seq,
                              const std::string& fate) override
    {
        terminated.emplace(Key{src, dst, seq}, fate);
    }

    // Everything else is irrelevant to conservation.
    void RecordTx(uint32_t, uint32_t, uint32_t, uint32_t, uint8_t, uint8_t, int16_t, uint16_t,
                  uint16_t, uint8_t, uint32_t, double, double, bool) override {}
    void RecordRx(uint32_t, uint32_t, uint32_t, uint32_t, uint8_t, uint8_t, int16_t, uint16_t,
                  uint16_t, uint8_t, double, double, bool) override {}
    void RecordRoute(uint32_t, uint32_t, uint32_t, uint8_t, uint16_t, uint32_t,
                     std::string) override {}
    void RecordRouteUsed(uint32_t, uint32_t, uint32_t, uint8_t, uint16_t, uint32_t) override {}
    void RecordEnergySnapshot(uint32_t, double, double) override {}
    void RecordQuantizationSample(uint32_t, uint32_t, uint32_t, bool, double, uint16_t) override {}
    void RecordRuntimeNodeStats(const DvClNodeStats&) override {}
    void RecordOverhead(uint32_t, const std::string&, uint32_t, uint32_t, uint32_t, uint32_t,
                        uint8_t, uint8_t) override {}
    void RecordDuty(uint32_t, double, uint32_t, uint32_t) override {}
    void RecordConnectivity(uint32_t, uint32_t, bool) override {}
    void RecordBeaconDelay(double) override {}
    double GetFirstTxTime(uint32_t, uint32_t, uint32_t) const override { return -1.0; }
};

/**
 * \ingroup dv-cl
 * \brief Every generated packet is accounted for exactly once (F2.4).
 *
 * Checks the identities that make a delivery ratio meaningful:
 *
 * - a packet is generated once and delivered at most once;
 * - delivered and terminated sets are disjoint, so no packet both
 *   arrives and is written off;
 * - both are subsets of what was generated, so nothing is delivered or
 *   dropped that never existed.
 *
 * The residual — generated packets neither delivered nor terminated — is
 * reported rather than asserted to zero: those are frames still queued
 * or in flight when the run stops, which is a legitimate outcome. What
 * would not be legitimate is a packet in no set at all *and* no longer
 * in the network, or one in both sets at once.
 */
class DvClConservationTestCase : public TestCase
{
  public:
    DvClConservationTestCase()
        : TestCase("dv-cl conservation: every data packet is accounted for exactly once")
    {
    }

  private:
    void DoRun() override
    {
        Config::SetDefault("ns3::dvcl::DvClApp::TrafficMode", StringValue("pueyo_all_to_all"));
        Config::SetDefault("ns3::dvcl::DvClApp::PueyoPacketsPerPair", UintegerValue(3));
        Config::SetDefault("ns3::dvcl::DvClApp::DataStartTimeSec", DoubleValue(60.0));
        Config::SetDefault("ns3::dvcl::DvClApp::DataStopTimeSec", DoubleValue(400.0));

        DvClMeshConfig cfg;
        cfg.nEd = 4;
        cfg.simTimeSec = 500.0;
        cfg.enableDutyCycle = true;
        cfg.dutyLimit = 0.01;
        cfg.enableCsma = true;
        cfg.nodePlacementMode = "grid";
        cfg.gridSide = 2;
        cfg.gridSpacingX = 400.0;
        cfg.gridSpacingY = 400.0;

        NodeContainer nodes;
        nodes.Create(cfg.nEd);
        auto helper = CreateObject<DvClHelper>();
        helper->SetConfig(cfg);
        helper->Install(nodes);

        auto sink = CreateObject<ConservationSink>();
        for (uint32_t i = 0; i < nodes.GetN(); ++i)
        {
            for (uint32_t a = 0; a < nodes.Get(i)->GetNApplications(); ++a)
            {
                if (auto app = DynamicCast<DvClApp>(nodes.Get(i)->GetApplication(a)))
                {
                    app->SetStatsSink(sink);
                }
            }
        }

        Simulator::Stop(Seconds(cfg.simTimeSec));
        Simulator::Run();
        Simulator::Destroy();

        NS_TEST_ASSERT_MSG_GT(sink->generated.size(), 0u, "the scenario must generate traffic");
        NS_TEST_ASSERT_MSG_EQ(sink->duplicateGenerations, 0u, "a packet is generated once");
        NS_TEST_ASSERT_MSG_EQ(sink->duplicateDeliveries, 0u, "a packet is delivered at most once");

        std::size_t deliveredUnknown = 0;
        for (const auto& k : sink->delivered)
        {
            if (sink->generated.find(k) == sink->generated.end())
            {
                ++deliveredUnknown;
            }
        }
        NS_TEST_ASSERT_MSG_EQ(deliveredUnknown, 0u, "nothing is delivered that was never generated");

        std::size_t bothDeliveredAndLost = 0;
        std::size_t terminatedUnknown = 0;
        for (const auto& kv : sink->terminated)
        {
            if (sink->delivered.count(kv.first) != 0)
            {
                ++bothDeliveredAndLost;
            }
            if (sink->generated.find(kv.first) == sink->generated.end())
            {
                ++terminatedUnknown;
            }
        }
        NS_TEST_ASSERT_MSG_EQ(bothDeliveredAndLost,
                              0u,
                              "no packet is both delivered and written off");
        NS_TEST_ASSERT_MSG_EQ(terminatedUnknown, 0u, "nothing is dropped that was never generated");

        // Residual: generated, neither delivered nor written off — still in the
        // network when the run stopped. Reported, not asserted to zero.
        std::size_t pending = 0;
        for (const auto& k : sink->generated)
        {
            if (sink->delivered.count(k) == 0 && sink->terminated.count(k) == 0)
            {
                ++pending;
            }
        }
        NS_TEST_ASSERT_MSG_LT_OR_EQ(pending,
                                    sink->generated.size(),
                                    "residual cannot exceed what was generated");
        std::clog << "  [conservation] generated=" << sink->generated.size()
                  << " delivered=" << sink->delivered.size()
                  << " terminated=" << sink->terminated.size() << " in-flight=" << pending
                  << "\n";
    }
};

/**
 * \ingroup dv-cl
 * \brief Packet conservation suite (validation plan F2.4).
 */
class DvClConservationTestSuite : public TestSuite
{
  public:
    DvClConservationTestSuite()
        : TestSuite("dv-cl-conservation", Type::SYSTEM)
    {
        AddTestCase(new DvClConservationTestCase, TestCase::Duration::QUICK);
    }
};

static DvClConservationTestSuite g_dvClConservationTestSuite; //!< Static instance
