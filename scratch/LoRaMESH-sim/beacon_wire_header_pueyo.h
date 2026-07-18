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
    // §SoC-wire: beacon header extendido con SoC del emisor (1 byte, 0-100).
    // §DC-wire: +1 byte dc_remaining del emisor (0-100, 0xFF=N/A). Solo lo usa
    //   el routing DC-aware (filtro de factibilidad por presupuesto de DC).
    // Todos los perfiles transmiten ambos bytes; el uso depende del modo de routing.
    static constexpr uint32_t kSerializedSize = 6;  // 6B: 5 base + 1 SoC (DC byte dropped)
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

    void SetSoC(uint8_t soc)
    {
        m_soc = soc;
    }

    void SetDcRemaining(uint8_t dc)  // §DC-wire
    {
        m_dcRemaining = dc;
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

    uint8_t GetSoC() const
    {
        return m_soc;
    }

    uint8_t GetDcRemaining() const  // §DC-wire
    {
        return m_dcRemaining;
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
        start.WriteU8(m_soc);  // 6B: keep SoC (drop DC)
    }

    uint32_t Deserialize(Buffer::Iterator start) override
    {
        m_src = start.ReadU16();
        m_dst = start.ReadU16();
        m_flagsTtl = start.ReadU8();
        m_soc = start.ReadU8();  // 6B: keep SoC (drop DC)
        return kSerializedSize;
    }

    void Print(std::ostream& os) const override
    {
        os << "src=" << m_src << " dst=" << m_dst
           << " flags_ttl=" << unsigned(m_flagsTtl)
           << " soc=" << unsigned(m_soc)
           << " dc_remaining=" << unsigned(m_dcRemaining);
    }

  private:
    uint16_t m_src{0};
    uint16_t m_dst{0xFFFF};
    uint8_t  m_flagsTtl{0};
    uint8_t  m_soc{0xFF};          // State of Charge 0-100; 0xFF = N/A → tratar como full
    uint8_t  m_dcRemaining{0xFF};  // §DC-wire: DC restante 0-100; 0xFF = N/A → tratar como 100%
};

} // namespace ns3
