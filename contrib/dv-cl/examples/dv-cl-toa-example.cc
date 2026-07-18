/* SPDX-License-Identifier: GPL-2.0-only */

/**
 * \file
 * \ingroup dv-cl
 * Minimal dv-cl example: prints the LoRa time-on-air per SF for a given
 * payload and the composite link cost across the battery range. Runs in
 * well under a second; doubles as a smoke test of the module.
 *
 * Usage: ./ns3 run "dv-cl-toa-example --payload=51"
 */

#include "ns3/command-line.h"
#include "ns3/double.h"
#include "ns3/dv-cl-metric.h"
#include "ns3/dv-cl-toa.h"

#include <cstdio>

using namespace ns3;
using namespace ns3::dvcl;

int
main(int argc, char* argv[])
{
    uint32_t payload = 51;
    CommandLine cmd(__FILE__);
    cmd.AddValue("payload", "PHY payload in bytes", payload);
    cmd.Parse(argc, argv);

    std::printf("ToA (BW125, CR4/5, explicit header, CRC on), payload=%u B\n", payload);
    for (uint8_t sf = 7; sf <= 12; ++sf)
    {
        const bool de = (sf >= 11); // mandatory LDRO at SF11/SF12 on 125 kHz
        const uint32_t us = ComputeToaUs(sf, 125000, 1, payload, false, de, true, 8);
        std::printf("  SF%-2u  %10.3f ms%s\n", sf, us / 1000.0, de ? "  (DE on)" : "");
    }

    auto metric = CreateObject<DvClCompositeMetric>();
    std::printf("\nComposite link cost at SF7, ToA=%u us, battery sweep:\n",
                ComputeToaUs(7, 125000, 1, payload));
    const double toaUs = ComputeToaUs(7, 125000, 1, payload);
    for (double b = 1.0; b >= -0.001; b -= 0.1)
    {
        LinkInputs in;
        in.toaUs = toaUs;
        in.sf = 7;
        in.energyFraction = b;
        std::printf("  b=%4.1f  cost=%.4f\n", b, metric->ComputeLinkCost(in));
    }
    return 0;
}
