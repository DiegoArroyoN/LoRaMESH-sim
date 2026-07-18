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
    void SetSoC(uint8_t soc)       { m_soc = soc; }
    void SetDcRemaining(uint8_t dc){ m_dcRemaining = dc; }  // §DC-wire

    uint16_t GetSrc()      const { return m_src; }
    uint16_t GetDst()      const { return m_dst; }
    uint8_t  GetFlagsTtl() const { return m_flagsTtl; }
    uint8_t  GetTtl()      const { return GetTtlFromFlagsV2(m_flagsTtl); }
    uint8_t  GetSoC()      const { return m_soc; }
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

} // namespace ns3
