/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_WIRE_H
#define DV_CL_WIRE_H

#include "ns3/header.h"

#include <cstdint>
#include <vector>

/**
 * \file
 * \ingroup dv-cl
 * The DV-CL wire contract: ONE beacon header and ONE data header.
 *
 * Design note (audit 2026-06/07): the campaign tree carried two
 * byte-identical beacon header classes plus a TX-path rebuild that
 * re-emitted one as the other; shrinking one of them silently shifted
 * the DV entries and collapsed the control plane. This module keeps a
 * single class per packet type — the header that is built is the header
 * on the air — and locks the byte layout with golden-byte tests
 * (test/dv-cl-wire-test-suite.cc).
 *
 * Byte order: multi-byte fields are little-endian on the wire
 * (ns3::Buffer::Iterator::WriteU16), matching the campaign binary.
 */

namespace ns3
{
namespace dvcl
{

/// \ingroup dv-cl DV-CL packet type carried in the flags/TTL byte.
enum class DvClPacketType : uint8_t
{
    DATA = 0,
    BEACON = 1,
};

/// Pack type (2 high bits) and TTL (6 low bits, capped at 63).
inline uint8_t
PackFlagsTtl(DvClPacketType type, uint8_t ttl)
{
    const uint8_t capped = (ttl > 63) ? 63 : ttl;
    return static_cast<uint8_t>((static_cast<uint8_t>(type) << 6) | (capped & 0x3F));
}

/// Extract the packet type from a flags/TTL byte.
inline DvClPacketType
UnpackType(uint8_t flagsTtl)
{
    return static_cast<DvClPacketType>((flagsTtl >> 6) & 0x03);
}

/// Extract the TTL from a flags/TTL byte.
inline uint8_t
UnpackTtl(uint8_t flagsTtl)
{
    return static_cast<uint8_t>(flagsTtl & 0x3F);
}

/**
 * \ingroup dv-cl
 * \brief DV-CL beacon header — 6 bytes on the air.
 *
 * Layout: src(LE16) dst(LE16) flagsTtl(8) soc(8).
 *
 * The SoC byte carries the sender state of charge quantized to
 * [0,100] (0xFF = unknown, priced as full battery); it is the input
 * of the receiver-applied energy term delta*Psi(b_j) of the composite
 * metric. The DV route entries travel as payload after this header
 * (see DvClDvEntry).
 */
class DvClBeaconHeader : public Header
{
  public:
    static constexpr uint32_t kSerializedSize = 6;

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

    void SetSoc(uint8_t soc)
    {
        m_soc = soc;
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

    uint8_t GetSoc() const
    {
        return m_soc;
    }

    uint32_t GetSerializedSize() const override;
    void Serialize(Buffer::Iterator start) const override;
    uint32_t Deserialize(Buffer::Iterator start) override;
    void Print(std::ostream& os) const override;

  private:
    uint16_t m_src{0};
    uint16_t m_dst{0xFFFF};
    uint8_t m_flagsTtl{0};
    uint8_t m_soc{0xFF}; //!< sender SoC 0-100; 0xFF = unknown (full)
};

/**
 * \ingroup dv-cl
 * \brief DV-CL unicast data header — 7 bytes on the air.
 *
 * Layout: src(LE16) dst(LE16) via(LE16) flagsTtl(8). `via` is the
 * link-layer next hop chosen by the forwarder.
 */
class DvClDataHeader : public Header
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
        return UnpackTtl(m_flagsTtl);
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

/**
 * \ingroup dv-cl
 * \brief One advertised DV route entry — 3 bytes in the beacon payload.
 *
 * Layout: destination(LE16) score(8). score = 100 - round(100*cost),
 * clamped to [1,100]; score 0 is route poison (unreachable).
 */
struct DvClDvEntry
{
    uint16_t destination{0};
    uint8_t score{0};

    static constexpr uint32_t kEntrySize = 3;
};

/**
 * \brief Pack DV entries into a byte buffer (truncates at maxLen).
 * \return bytes written (multiple of DvClDvEntry::kEntrySize)
 */
uint32_t SerializeDvEntries(const std::vector<DvClDvEntry>& entries, uint8_t* out, uint32_t maxLen);

/**
 * \brief Unpack DV entries from a byte buffer (floor(len/3) entries).
 */
void DeserializeDvEntries(const uint8_t* in, uint32_t len, std::vector<DvClDvEntry>& out);

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_WIRE_H */
