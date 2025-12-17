#include "loramesh-adr-hopbyhop.h"

namespace ns3
{
namespace loramesh
{

constexpr double HopByHopAdr::kSnrThresholds[5];
constexpr double HopByHopAdr::kMinSnr;

SpreadingFactor
HopByHopAdr::SelectSf (NodeId src, NodeId dst, double snr) const
{
  (void) src;
  (void) dst;

  static const SpreadingFactor sfs[] = {
    SpreadingFactor::SF7,
    SpreadingFactor::SF8,
    SpreadingFactor::SF9,
    SpreadingFactor::SF10,
    SpreadingFactor::SF11
  };

  for (size_t i = 0; i < sizeof (sfs) / sizeof (sfs[0]); ++i)
    {
      if (snr >= kSnrThresholds[i])
        {
          return sfs[i];
        }
    }

  return SpreadingFactor::SF12;
}

bool
HopByHopAdr::IsValidLink (double snr) const
{
  return snr >= kMinSnr;
}

} // namespace loramesh
} // namespace ns3
