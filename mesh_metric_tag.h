#pragma once
#include "ns3/nstime.h"
#include "ns3/tag.h"
#include "ns3/type-id.h"
#include "ns3/uinteger.h"

#include <cstdint>
#include <set>
#include <vector>

namespace ns3
{

class MeshMetricTag : public Tag
{
  public:
    static TypeId GetTypeId();
    TypeId GetInstanceTypeId() const override;

    MeshMetricTag() = default;
    ~MeshMetricTag() override = default;

    // --- Campos “core” para Fase 6 ---
    void SetSrc(uint16_t v)
    {
        m_src = v;
    }

    void SetDst(uint16_t v)
    {
        m_dst = v;
    }

    void SetSeq(uint32_t v)
    {
        m_seq = v;
    }

    void SetTtl(uint8_t v)
    {
        m_ttl = v;
    }

    void SetHops(uint8_t v)
    {
        m_hops = v;
    }

    void SetSf(uint8_t v)
    {
        m_sf = v;
    }

    // --- Campos de métricas (serializados OTA para trazabilidad end-to-end) ---
    void SetToaUs(uint32_t v)       { m_toaUs = v; }
    void SetBatt_mV(uint16_t v)     { m_batt_mV = v; }
    void SetScoreX100(uint16_t v)   { m_scoreX100 = v; }
    uint32_t GetToaUs() const       { return m_toaUs; }
    uint16_t GetBatt_mV() const     { return m_batt_mV; }
    uint16_t GetScoreX100() const   { return m_scoreX100; }

    void SetPrevHop(uint16_t v)
    {
        m_prevHop = v;
    }

    void SetExpectedNextHop(uint16_t v)
    {
        m_expectedNextHop = v;
    }

    uint16_t GetSrc() const
    {
        return m_src;
    }

    uint16_t GetDst() const
    {
        return m_dst;
    }

    uint32_t GetSeq() const
    {
        return m_seq;
    }

    uint8_t GetTtl() const
    {
        return m_ttl;
    }

    uint8_t GetHops() const
    {
        return m_hops;
    }

    uint8_t GetSf() const
    {
        return m_sf;
    }

    uint16_t GetPrevHop() const
    {
        return m_prevHop;
    }

    uint16_t GetExpectedNextHop() const
    {
        return m_expectedNextHop;
    }

    // Tag API
    uint32_t GetSerializedSize() const override;
    void Serialize(TagBuffer i) const override;
    void Deserialize(TagBuffer i) override;
    void Print(std::ostream& os) const override;

    // ------ NUEVO: STRUCT DE ENTRADA DE RUTA PARA PAYLOAD ------
    // Struct de entrada de ruta para payload de beacons DV
    struct RoutePayloadEntry
    {
        uint16_t dst; // Destino de la ruta (codificado como uint16_t en el payload)
        uint8_t hops;
        uint8_t sf;
        uint16_t score;   // Score (X100, métrica compuesta; se cuantiza a 0-100)
        uint16_t batt_mV; // Batería (mV, se cuantiza en el payload)
                          // REMOVED: rssi_dBm - no se usa en métrica, receptor lo obtiene de PHY
    };

    // 6 bytes per route entry = dst(2) + hops(1) + sf(1) + score(1) + batt(1)
    // RSSI eliminado: el receptor lo obtiene de la capa PHY
    static constexpr uint8_t kRoutePayloadSize = 6;

    // Máximo voltaje de batería para cuantización (5V cubre LiPo 4.2V + LiFePO4 3.65V +
    // industriales 5V) Usado para mapear mV a uint8_t [0-255] en payload compacto
    static constexpr uint16_t kBattMaxMv = 5000;

    static uint32_t GetRoutePayloadEntrySize()
    {
        return kRoutePayloadSize;
    }

    // Auxiliar para serializar lista
    static void SerializeRoutePayload(const std::vector<RoutePayloadEntry>& entries,
                                      uint8_t* out,
                                      size_t maxlen);
    static void DeserializeRoutePayload(const uint8_t* in,
                                        size_t len,
                                        std::vector<RoutePayloadEntry>& entries);

  private:
    // Layout OTA:
    // src(2)+dst(2)+seq(4)+ttl(1)+hops(1)+sf(1)+toaUs(4)+batt(2)+score(2)+prevHop(2)+expNextHop(2)
    uint16_t m_src = 0;
    uint16_t m_dst = 0;
    uint32_t m_seq = 0;
    uint8_t m_ttl = 0;
    uint8_t m_hops = 0;
    uint8_t m_sf = 10;
    uint32_t m_toaUs = 0;
    uint16_t m_batt_mV = 3300;
    uint16_t m_scoreX100 = 100;
    uint16_t m_prevHop = 0xFFFF;
    uint16_t m_expectedNextHop = 0;
};

} // namespace ns3
