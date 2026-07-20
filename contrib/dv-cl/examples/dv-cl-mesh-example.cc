/* SPDX-License-Identifier: GPL-2.0-only */

/**
 * \file
 * \ingroup dv-cl
 * Minimal end-to-end DV-CL mesh scenario: a 3x3 grid running all-to-all
 * traffic under the EU868 1% duty cycle with the canonical 6-byte wire.
 * Demonstrates the two integration seams of the module: the DvClHelper
 * (topology + stack install) and the DvClStatsSink (measurement).
 *
 * Usage: ./ns3 run "dv-cl-mesh-example --nEd=9 --simTime=600"
 */

#include "ns3/boolean.h"
#include "ns3/command-line.h"
#include "ns3/config.h"
#include "ns3/core-module.h"
#include "ns3/double.h"
#include "ns3/dv-cl-app.h"
#include "ns3/dv-cl-helper.h"
#include "ns3/dv-cl-stats-sink.h"
#include "ns3/node-container.h"
#include "ns3/string.h"
#include "ns3/uinteger.h"

#include <cstdio>

using namespace ns3;
using namespace ns3::dvcl;

/// Example sink: counts generated and delivered packets for a PDR line.
class MiniSink : public DvClStatsSink
{
  public:
    uint64_t generated{0};
    uint64_t delivered{0};
    uint64_t txOk{0};

    void RecordDataGenerated(uint32_t, uint32_t, uint32_t) override
    {
        ++generated;
    }

    void RecordE2eDelay(uint32_t,
                        uint32_t,
                        uint32_t,
                        uint8_t,
                        double,
                        uint32_t,
                        uint8_t,
                        bool wasDelivered) override
    {
        if (wasDelivered)
        {
            ++delivered;
        }
    }

    void RecordTx(uint32_t,
                  uint32_t,
                  uint32_t,
                  uint32_t,
                  uint8_t,
                  uint8_t,
                  int16_t,
                  uint16_t,
                  uint16_t,
                  uint8_t,
                  uint32_t,
                  double,
                  double,
                  bool ok) override
    {
        if (ok)
        {
            ++txOk;
        }
    }

    // The remaining events are ignored by this example.
    void RecordRx(uint32_t,
                  uint32_t,
                  uint32_t,
                  uint32_t,
                  uint8_t,
                  uint8_t,
                  int16_t,
                  uint16_t,
                  uint16_t,
                  uint8_t,
                  double,
                  double,
                  bool) override
    {
    }

    void RecordRoute(uint32_t, uint32_t, uint32_t, uint8_t, uint16_t, uint32_t, std::string)
        override
    {
    }

    void RecordRouteUsed(uint32_t, uint32_t, uint32_t, uint8_t, uint16_t, uint32_t) override
    {
    }

    void RecordEnergySnapshot(uint32_t, double, double) override
    {
    }

    void RecordQuantizationSample(uint32_t, uint32_t, uint32_t, bool, double, uint16_t) override
    {
    }

    void RecordRuntimeNodeStats(const DvClNodeStats&) override
    {
    }

    void RecordOverhead(uint32_t,
                        const std::string&,
                        uint32_t,
                        uint32_t,
                        uint32_t,
                        uint32_t,
                        uint8_t,
                        uint8_t) override
    {
    }

    void RecordDuty(uint32_t, double, uint32_t, uint32_t) override
    {
    }

    void RecordConnectivity(uint32_t, uint32_t, bool) override
    {
    }

    void RecordBeaconDelay(double) override
    {
    }

    double GetFirstTxTime(uint32_t, uint32_t, uint32_t) const override
    {
        return -1.0;
    }
};

int
main(int argc, char* argv[])
{
    uint32_t nEd = 9;
    double simTime = 600.0;
    CommandLine cmd(__FILE__);
    cmd.AddValue("nEd", "number of nodes (perfect square for the grid)", nEd);
    cmd.AddValue("simTime", "simulated seconds", simTime);
    cmd.Parse(argc, argv);

    // Canonical DV-CL configuration: 6-byte wire, all-to-all traffic.
    Config::SetDefault("ns3::dvcl::DvClApp::WireFormat", StringValue("pueyo7b"));
    Config::SetDefault("ns3::dvcl::DvClApp::TrafficMode", StringValue("pueyo_all_to_all"));
    Config::SetDefault("ns3::dvcl::DvClApp::PueyoPacketsPerPair", UintegerValue(5));
    Config::SetDefault("ns3::dvcl::DvClApp::DataStartTimeSec", DoubleValue(60.0));
    Config::SetDefault("ns3::dvcl::DvClApp::DataStopTimeSec", DoubleValue(simTime));

    DvClMeshConfig cfg;
    cfg.nEd = nEd;
    cfg.simTimeSec = simTime;
    cfg.enableDutyCycle = true;
    cfg.dutyLimit = 0.01; // EU868 1%
    cfg.enableCsma = true;
    cfg.nodePlacementMode = "grid";
    cfg.gridSide = 3;
    cfg.gridSpacingX = 500.0;
    cfg.gridSpacingY = 500.0;

    NodeContainer nodes;
    nodes.Create(cfg.nEd);

    auto helper = CreateObject<DvClHelper>();
    helper->SetConfig(cfg);
    helper->Install(nodes);

    auto sink = CreateObject<MiniSink>();
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

    Simulator::Stop(Seconds(simTime));
    Simulator::Run();
    Simulator::Destroy();

    std::printf("dv-cl-mesh-example: nodes=%u sim=%.0fs generated=%llu delivered=%llu "
                "txOk=%llu pdr=%.2f%%\n",
                nEd,
                simTime,
                static_cast<unsigned long long>(sink->generated),
                static_cast<unsigned long long>(sink->delivered),
                static_cast<unsigned long long>(sink->txOk),
                sink->generated ? 100.0 * sink->delivered / sink->generated : 0.0);
    return 0;
}
