#pragma once

#include "data_wire_header_v2.h"
#include "ns3/header.h"
#include "ns3/type-id.h"

#include <cstdint>

namespace ns3
{

class DataWireHeaderPueyo7b : public Header
{
  public:
    static constexpr uint32_t kSerializedSize = 7;

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

    uint32_t GetSerializedSize() const override;
    void Serialize(Buffer::Iterator start) const override;
    uint32_t Deserialize(Buffer::Iterator start) override;
    void Print(std::ostream& os) const override;

  private:
    uint16_t m_src{0};
    uint16_t m_dst{0};
    uint16_t m_via{0};
    uint8_t m_flagsTtl{0};
};

} // namespace ns3
