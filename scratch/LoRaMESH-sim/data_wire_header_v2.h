#pragma once

#include "ns3/header.h"
#include "ns3/type-id.h"

#include <cstdint>

namespace ns3
{

enum class WirePacketTypeV2 : uint8_t
{
    DATA = 0,
    BEACON = 1,
};

inline uint8_t
PackFlagsTtlV2(WirePacketTypeV2 type, uint8_t ttl)
{
    const uint8_t cappedTtl = (ttl > 63) ? 63 : ttl;
    return static_cast<uint8_t>((static_cast<uint8_t>(type) << 6) | (cappedTtl & 0x3F));
}

inline WirePacketTypeV2
GetPacketTypeV2(uint8_t flagsTtl)
{
    return static_cast<WirePacketTypeV2>((flagsTtl >> 6) & 0x03);
}

inline uint8_t
GetTtlFromFlagsV2(uint8_t flagsTtl)
{
    return static_cast<uint8_t>(flagsTtl & 0x3F);
}

class DataWireHeaderV2 : public Header
{
  public:
    static constexpr uint32_t kSerializedSize = 9;

    static TypeId GetTypeId();
    TypeId GetInstanceTypeId() const override;

    void SetSrc(uint16_t src)
    {
        m_src = src;
    }
    void SetDst(uint16_t dst)
    {
        m_dst = dst;
    }
    void SetVia(uint16_t via)
    {
        m_via = via;
    }
    void SetFlagsTtl(uint8_t flagsTtl)
    {
        m_flagsTtl = flagsTtl;
    }
    void SetSeq16(uint16_t seq16)
    {
        m_seq16 = seq16;
    }

    uint16_t GetSrc() const
    {
        return m_src;
    }
    uint16_t GetDst() const
    {
        return m_dst;
    }
    uint16_t GetVia() const
    {
        return m_via;
    }
    uint8_t GetFlagsTtl() const
    {
        return m_flagsTtl;
    }
    uint8_t GetTtl() const
    {
        return GetTtlFromFlagsV2(m_flagsTtl);
    }
    uint16_t GetSeq16() const
    {
        return m_seq16;
    }

    uint32_t GetSerializedSize() const override;
    void Serialize(Buffer::Iterator start) const override;
    uint32_t Deserialize(Buffer::Iterator start) override;
    void Print(std::ostream& os) const override;

  private:
    uint16_t m_src{0};
    uint16_t m_dst{0};
    uint16_t m_via{0};
    uint8_t m_flagsTtl{0};
    uint16_t m_seq16{0};
};

} // namespace ns3

