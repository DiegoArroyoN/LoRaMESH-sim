#pragma once

#include "data_wire_header_v2.h"
#include "ns3/header.h"
#include "ns3/type-id.h"

#include <cstdint>

namespace ns3
{

class BeaconWireHeaderPueyo : public Header
{
  public:
    static constexpr uint32_t kSerializedSize = 5;
    static constexpr uint32_t kEntrySize = 3;

    static TypeId GetTypeId()
    {
        static TypeId tid =
            TypeId("ns3::BeaconWireHeaderPueyo").SetParent<Header>().SetGroupName("LoRaMESH-sim");
        return tid;
    }

    TypeId GetInstanceTypeId() const override
    {
        return GetTypeId();
    }

    void SetSrc(uint16_t src)
    {
        m_src = src;
    }

    void SetDst(uint16_t dst)
    {
        m_dst = dst;
    }

    void SetFlagsTtl(uint8_t flagsTtl)
    {
        m_flagsTtl = flagsTtl;
    }

    uint16_t GetSrc() const
    {
        return m_src;
    }

    uint16_t GetDst() const
    {
        return m_dst;
    }

    uint8_t GetFlagsTtl() const
    {
        return m_flagsTtl;
    }

    uint8_t GetTtl() const
    {
        return GetTtlFromFlagsV2(m_flagsTtl);
    }

    uint32_t GetSerializedSize() const override
    {
        return kSerializedSize;
    }

    void Serialize(Buffer::Iterator start) const override
    {
        start.WriteU16(m_src);
        start.WriteU16(m_dst);
        start.WriteU8(m_flagsTtl);
    }

    uint32_t Deserialize(Buffer::Iterator start) override
    {
        m_src = start.ReadU16();
        m_dst = start.ReadU16();
        m_flagsTtl = start.ReadU8();
        return kSerializedSize;
    }

    void Print(std::ostream& os) const override
    {
        os << "src=" << m_src << " dst=" << m_dst << " flags_ttl=" << unsigned(m_flagsTtl);
    }

  private:
    uint16_t m_src{0};
    uint16_t m_dst{0xFFFF};
    uint8_t m_flagsTtl{0};
};

} // namespace ns3
