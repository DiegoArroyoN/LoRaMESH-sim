/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_LEGACY_WIRE_V2_H
#define DV_CL_LEGACY_WIRE_V2_H

// Private legacy carry: the experimental v2 wire classes still referenced
// by the application's v1/v2 branches. Scheduled for excision (F5 step
// 5c-iii); not part of the module's public wire contract (dv-cl-wire.h).

#include "dv-cl-wire.h"

#include "ns3/header.h"

#include <cstdint>

namespace ns3
{
namespace dvcl
{
using WirePacketTypeV2 = DvClPacketType;




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
        return UnpackTtl(m_flagsTtl);
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








struct DvEntryWireV2
{
    uint16_t destination{0};
    uint8_t score{0}; // 0..100
};

class BeaconWireHeaderV2 : public Header
{
  public:
    // §SoC-wire: beacon header extendido con SoC del emisor (1 byte, 0-100).
    // §DC-wire: +1 byte dc_remaining del emisor (0-100, 0xFF=N/A). Solo lo usa
    //   el routing DC-aware (filtro de factibilidad por presupuesto de DC).
    static constexpr uint32_t kSerializedSize = 7;  // 5 base + 1 SoC + 1 dc_remaining
    static constexpr uint32_t kEntrySize = 3;

    static TypeId GetTypeId();
    TypeId GetInstanceTypeId() const override;

    void SetSrc(uint16_t src)      { m_src = src; }
    void SetDst(uint16_t dst)      { m_dst = dst; }
    void SetFlagsTtl(uint8_t f)    { m_flagsTtl = f; }
    void SetSoc(uint8_t soc)       { m_soc = soc; }
    void SetDcRemaining(uint8_t dc){ m_dcRemaining = dc; }  // §DC-wire

    uint16_t GetSrc()      const { return m_src; }
    uint16_t GetDst()      const { return m_dst; }
    uint8_t  GetFlagsTtl() const { return m_flagsTtl; }
    uint8_t  GetTtl()      const { return UnpackTtl(m_flagsTtl); }
    uint8_t  GetSoc()      const { return m_soc; }
    uint8_t  GetDcRemaining() const { return m_dcRemaining; }  // §DC-wire

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
    uint8_t  m_flagsTtl{0};
    uint8_t  m_soc{0xFF};          // State of Charge 0-100; 0xFF = N/A
    uint8_t  m_dcRemaining{0xFF};  // §DC-wire: DC restante 0-100; 0xFF = N/A
};



} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_LEGACY_WIRE_V2_H */
