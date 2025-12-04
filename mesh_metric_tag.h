#pragma once
#include "ns3/tag.h"
#include "ns3/type-id.h"
#include "ns3/uinteger.h"
#include "ns3/nstime.h"
#include <cstdint>
#include <vector>

namespace ns3 {

class MeshMetricTag : public Tag
{
public:
  static TypeId GetTypeId ();
  TypeId GetInstanceTypeId () const override;

  MeshMetricTag () = default;
  ~MeshMetricTag () override = default;

  // --- Campos “core” para Fase 6 ---
  void SetSrc (uint16_t v)         { m_src = v; }
  void SetDst (uint16_t v)         { m_dst = v; }
  void SetSeq (uint32_t v)         { m_seq = v; }
  void SetTtl (uint8_t v)          { m_ttl = v; }
  void SetHops (uint8_t v)         { m_hops = v; }
  void SetToaUs (uint32_t v)       { m_toaUs = v; }
  void SetRssiDbm (int16_t v)      { m_rssiDbm = v; }
  void SetBatt_mV (uint16_t v)     { m_batt_mV = v; }
  void SetScoreX100 (uint16_t v)   { m_scoreX100 = v; }
  void SetSf (uint8_t v)           { m_sf = v; }

  uint16_t GetSrc () const       { return m_src; }
  uint16_t GetDst () const       { return m_dst; }
  uint32_t GetSeq () const       { return m_seq; }
  uint8_t  GetTtl () const       { return m_ttl; }
  uint8_t  GetHops () const      { return m_hops; }
  uint32_t GetToaUs () const     { return m_toaUs; }
  int16_t  GetRssiDbm () const   { return m_rssiDbm; }
  uint16_t GetBatt_mV () const   { return m_batt_mV; }
  uint16_t GetScoreX100 () const { return m_scoreX100; }
  uint8_t  GetSf () const        { return m_sf; }

  // Tag API
  uint32_t GetSerializedSize () const override;
  void Serialize (TagBuffer i) const override;
  void Deserialize (TagBuffer i) override;
  void Print (std::ostream &os) const override;

  // ------ NUEVO: STRUCT DE ENTRADA DE RUTA PARA PAYLOAD ------
  struct RoutePayloadEntry
  {
    uint16_t dst;          // Destino de la ruta
    uint8_t hops;
    uint8_t sf;
    uint16_t score;        // Score (X100, métrica compuesta)
    uint16_t batt_mV;      // Métrica energética ejemplo (puedes omitirla)
    int16_t rssi_dBm;

    // Puedes agregar más campos si necesitas (ej: SF, SNR)
  };

  // Auxiliar para serializar lista
  static void SerializeRoutePayload(const std::vector<RoutePayloadEntry>& entries, uint8_t* out, size_t maxlen);
  static void DeserializeRoutePayload(const uint8_t* in, size_t len, std::vector<RoutePayloadEntry>& entries);

private:
  // Tamaño total = 2+2+4+1+1+4+2+2+2 = 20 bytes
  // Con SF (1 byte) = 21 bytes
  uint16_t m_src = 0;
  uint16_t m_dst = 0;
  uint32_t m_seq = 0;
  uint8_t  m_ttl = 0;
  uint8_t  m_hops = 0;
  uint8_t  m_sf = 10;
  uint32_t m_toaUs = 0;
  int16_t  m_rssiDbm = -127;
  uint16_t m_batt_mV = 3300;
  uint16_t m_scoreX100 = 100;
};

} // namespace ns3
