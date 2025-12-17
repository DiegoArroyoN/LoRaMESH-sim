#ifndef LORAMESH_ADR_HOPBYHOP_H
#define LORAMESH_ADR_HOPBYHOP_H

#include <cstdint>
#include <cstddef>

namespace ns3
{

using NodeId = uint32_t;

namespace loramesh
{

enum class SpreadingFactor : uint8_t
{
  SF7 = 7,
  SF8 = 8,
  SF9 = 9,
  SF10 = 10,
  SF11 = 11,
  SF12 = 12
};

class HopByHopAdr
{
public:
  HopByHopAdr () = default;

  SpreadingFactor SelectSf (NodeId src, NodeId dst, double snr) const;
  bool IsValidLink (double snr) const;

private:
  static constexpr double kSnrThresholds[5] = {13.0, 10.0, 7.0, 4.0, 2.0};
  static constexpr double kMinSnr = 0.0;
};

} // namespace loramesh
} // namespace ns3

#endif /* LORAMESH_ADR_HOPBYHOP_H */
