#pragma once

#include "data_wire_header_v2.h"
#include "ns3/header.h"
#include "ns3/type-id.h"

#include <cstdint>
#include <vector>

namespace ns3
{

struct DvEntryWireV2
{
    uint16_t destination{0};
    uint8_t score{0}; // 0..100
};

class BeaconWireHeaderV2 : public Header
{
  public:
    static constexpr uint32_t kSerializedSize = 5;
    static constexpr uint32_t kEntrySize = 3;

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

    uint32_t GetSerializedSize() const override;
    void Serialize(Buffer::Iterator start) const override;
    uint32_t Deserialize(Buffer::Iterator start) override;
    void Print(std::ostream& os) const override;

    static void SerializeDvEntries(const std::vector<DvEntryWireV2>& entries,
                                   uint8_t* out,
                                   size_t maxlen);
    static void DeserializeDvEntries(const uint8_t* in, size_t len, std::vector<DvEntryWireV2>& out);

  private:
    uint16_t m_src{0};
    uint16_t m_dst{0xFFFF};
    uint8_t m_flagsTtl{0};
};

} // namespace ns3

