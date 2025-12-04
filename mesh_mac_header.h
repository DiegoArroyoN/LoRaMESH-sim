#pragma once
#include "ns3/header.h"
#include "ns3/mac48-address.h"

namespace ns3 {

class MeshMacHeader : public Header
{
public:
  MeshMacHeader () = default;
  ~MeshMacHeader () override = default;

  void SetSrc (Mac48Address addr) { m_src = addr; }
  void SetDst (Mac48Address addr) { m_dst = addr; }
  Mac48Address GetSrc () const { return m_src; }
  Mac48Address GetDst () const { return m_dst; }

  static TypeId GetTypeId ();
  TypeId GetInstanceTypeId () const override;

  uint32_t GetSerializedSize () const override;
  void Serialize (Buffer::Iterator start) const override;
  uint32_t Deserialize (Buffer::Iterator start) override;
  void Print (std::ostream& os) const override;

private:
  Mac48Address m_src;
  Mac48Address m_dst;
};

} // namespace ns3
