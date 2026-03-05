#pragma once
#include "beacon_wire_header_v2.h"
#include "data_wire_header_v2.h"
#include "mesh_metric_tag.h"

#include "ns3/address.h"
#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/log.h"
#include "ns3/loramesh-energy-model.h"
#include "ns3/loramesh-mac-csma-cad.h"
#include "ns3/loramesh-metric-composite.h"
#include "ns3/loramesh-routing-dv.h"
#include "ns3/mac48-address.h"
#include "ns3/net-device.h"
#include "ns3/nstime.h"
#include "ns3/packet.h"
#include "ns3/random-variable-stream.h"
#include "ns3/simulator.h"

#include <cstdint>
#include <deque>
#include <array>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ns3
{

struct TxQueueEntry
{
    Ptr<Packet> packet;
    MeshMetricTag tag;
    uint32_t retries;
    Address dstAddr;
    bool logTxMetrics{false};
    std::string pendingReason{"queued"};
    uint32_t deferCount{0};
    Time lastStateChange{Seconds(0)};
    bool beaconRpCounterAssigned{false};
    uint8_t beaconRpCounter{0};
};

// REFACTORING: Struct para encapsular resultado de validación de rutas
// Elimina código duplicado en 8+ lugares
struct RouteStatus
{
    const loramesh::RouteEntry* route{nullptr}; // Puntero a la ruta (o nullptr)
    bool exists{false};                         // HasAnyRoute() retornó true
    bool expired{false};                        // IsRouteExpired() retornó true
    bool valid{false};                          // exists && !expired && route != nullptr

    // Helper: true si la ruta es usable para forwarding
    explicit operator bool() const
    {
        return valid;
    }
};

class MeshDvApp : public Application
{
  public:
    static TypeId GetTypeId();
    MeshDvApp();
    ~MeshDvApp() override;

    void SetPeriod(Time t);

    void SetInitTtl(uint8_t ttl)
    {
        m_initTtl = ttl;
    }

    void SetInitScoreX100(uint16_t s)
    {
        m_initScoreX100 = s;
    }

    void SetCsmaEnabled(bool enabled)
    {
        m_csmaEnabled = enabled;
    }

    void SetRouteTimeoutFactor(double factor);

    void SetCollectorNodeId(uint32_t collectorNodeId)
    {
        m_collectorNodeId = collectorNodeId;
    }

    void SetWireFormat(std::string format);
    std::string GetWireFormat() const;

    void StartApplication() override;
    void StopApplication() override;

  private:
    struct NeighborLinkInfo;

    enum class TrafficLoadMode
    {
        LOW,
        MEDIUM,
        HIGH,
        SATURATION
    };

    void Tick();
    void BuildAndSendDv(uint8_t sf);
    void BuildAndSendDvV2(uint8_t sf);
    void ForwardWithTtl(Ptr<const Packet> pIn, const MeshMetricTag& inTag);
    bool L2Receive(Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address& from);
    bool L2ReceiveV2(Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address& from);
    void ForwardWithTtlV2(Ptr<const Packet> pIn,
                          uint16_t src,
                          uint16_t dst,
                          uint16_t seq16,
                          uint8_t ttl);

    void SendWithCSMA(Ptr<Packet> packet,
                      const MeshMetricTag& tag,
                      Address dstAddr,
                      bool logTxMetrics = false);
    void ProcessTxQueue();
    void OnBackoffTimer();
    void OnPacketTransmitted(uint32_t toaUs);
    Address ResolveNextHopAddress(uint32_t nextHopId) const;
    bool IsLinkAddrFresh(uint32_t nextHopId) const;
    bool ResolveUnicastNextHopLinkAddr(uint32_t nextHopId, Mac48Address* outMac, bool* outStale) const;
    bool TryGetBestRecentSf(const NeighborLinkInfo& link, Time now, uint8_t* outSf) const;
    uint8_t SelectRandomSfProbabilistic() const;   // B3: Selección probabilística de SF según paper
    RouteStatus ValidateRoute(uint32_t dst) const; // REFACTORING: Helper para validación de rutas
    void ScheduleDvCycle(uint8_t sf, EventId* evtSlot); // FIX A2: puntero en lugar de ref
    Time GetBeaconInterval() const;
    std::string GetBeaconPhaseLabel() const;
    void SendInitialDv();
    void SendPostWarmupDvBurst(uint32_t remaining);
    void InitDataDestinations();
    void InitDataSlots();
    void TrackActiveDestination(uint32_t dst);
    void ScheduleExtraDvBeacons(const std::string& reason);
    void SendExtraDvBeacon(const std::string& reason);
    void UpdateDataPeriod();
    void SetTrafficLoad(std::string load);
    std::string GetTrafficLoad() const;
    void BootstrapLinkAddrTableFromRx();

    void SetEnableDvBroadcast(bool enable)
    {
        m_enableDvBroadcast = enable;
    }

    bool GetEnableDvBroadcast() const
    {
        return m_enableDvBroadcast;
    }

    void PrintRoutingTable();
    void PurgeExpiredRoutes();
    void SendDataToDestination(uint32_t dst, Ptr<Packet> payload);

    uint32_t ComputeLoRaToAUs(uint8_t sf, uint32_t bw, uint8_t cr, uint32_t pl) const;
    uint16_t ComputeScoreX100(const MeshMetricTag& t) const;

    // Cache de deduplicación
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, Time>
        m_seenPackets; // {src, dst, seq} -> timestamp

    // Métricas reales
    int16_t GetRealRSSI() const;
    uint16_t GetBatteryVoltageMv() const;
    double GetRemainingEnergyJ() const;
    double GetEnergyFraction() const;

    // Recolección de métricas - SIMPLIFICADO (solo funciones)
    // RSSI eliminado: no se usa en métrica, se obtiene de PHY si se necesita
    void LogTxEvent(uint32_t seq,
                    uint32_t dst,
                    uint8_t ttl,
                    uint8_t hops,
                    uint16_t battery,
                    uint16_t score,
                    uint8_t sf,
                    double energyJ,
                    double energyFrac,
                    bool ok);
    void LogRxEvent(uint32_t src,
                    uint32_t dst,
                    uint32_t seq,
                    uint8_t ttl,
                    uint8_t hops,
                    uint16_t battery,
                    uint16_t score,
                    uint8_t sf,
                    double energyJ,
                    double energyFrac,
                    bool forwarded);

    void HandleRouteChange(const loramesh::RouteEntry& entry, const std::string& action);
    void HandleFloodRequest(const loramesh::DvMessage& msg);
    void ProcessDvPayload(Ptr<const Packet> p,
                          const MeshMetricTag& tag,
                          const Mac48Address& fromMac,
                          uint32_t toaUsNeighbor);
    loramesh::NeighborLinkInfo BuildNeighborLinkInfo(const MeshMetricTag& tag,
                                                     uint32_t toaUs,
                                                     Mac48Address fromMac) const;
    std::vector<loramesh::DvEntry> DecodeDvEntries(Ptr<const Packet> p,
                                                   const MeshMetricTag& tag,
                                                   uint32_t toaUsNeighbor) const;
    std::vector<loramesh::DvEntry> DecodeDvEntriesV2(Ptr<const Packet> p,
                                                     uint32_t payloadOffset,
                                                     uint32_t toaUsNeighbor,
                                                     uint8_t rxSf) const;
    bool ParseDataWirePacketV2(Ptr<const Packet> p,
                               DataWireHeaderV2* outHdr,
                               Ptr<Packet>* outPayload) const;
    bool ParseBeaconWirePacketV2(Ptr<const Packet> p,
                                 BeaconWireHeaderV2* outHdr,
                                 Ptr<Packet>* outPayload) const;
    uint32_t ResolveBeaconSequenceFromRpCounter(uint32_t origin, uint8_t rpCounter);

    Time m_period{Seconds(60)};
    uint8_t m_initTtl{10};
    uint16_t m_initScoreX100{100};

    uint8_t m_sf{9};         // SF para datos (fallback si no hay ruta)
    uint8_t m_sfControl{12}; // SF robusto para beacons DV (usado si m_useProbabilisticSf=false)
    uint8_t m_sfMin{7};      // B3: SF mínimo para selección probabilística
    uint8_t m_sfMax{12};     // B3: SF máximo para selección probabilística
    bool m_useProbabilisticSf{true}; // B3: Usar selección probabilística de SF para beacons
    bool m_useRouteSfForData{true};  // B4: Usar SF de la ruta para datos
    bool m_useEmpiricalSfForData{true};
    bool m_allowStaleLinkAddrForUnicastData{true};
    uint32_t m_empiricalSfMinSamples{2};
    std::string m_empiricalSfSelectMode{"robust_min"};
    uint32_t m_bw{125000};
    uint8_t m_cr{1};
    uint8_t m_pl{20};
    bool m_crc{true};
    bool m_ih{false};
    bool m_de{true};

    uint32_t m_seq{0};
    std::string m_wireFormat{"v2"};
    uint32_t m_dataSeq{0};
    Time m_beaconWarmupEnd{Seconds(60)};
    double m_beaconWarmupSec{60.0};
    Time m_beaconIntervalWarm{Seconds(10)};
    Time m_beaconIntervalStable{Seconds(60)};
    double m_initialDvDelayBase{1.0};
    double m_initialDvJitterMax{8.0};
    double m_initialDvNodeSpacing{0.5};
    loramesh::CompositeMetric m_compositeMetric;
    Ptr<loramesh::EnergyModel> m_energyModel;
    Ptr<loramesh::RoutingDv> m_routing;
    std::map<uint32_t, loramesh::RouteEntry> m_lastRouteSnapshot;
    bool m_routeChangePending{false};

    bool m_csmaEnabled{true};
    bool m_txBusy{false};
    uint32_t m_txCount{0};
    uint32_t m_backoffCount{0};

    uint8_t m_difsCadCount{3};
    Time m_cadDuration{MilliSeconds(5.5)};
    uint8_t m_backoffWindow{8};
    uint8_t m_maxRetries{5};

    std::deque<TxQueueEntry> m_txQueue;
    Ptr<UniformRandomVariable> m_rng;

    // Tabla de direcciones de enlace aprendidas por RX.
    // Nota: Mac48Address se usa solo como wrapper interno de ns-3; on-air viajan src/dst/via de 2 bytes.
    std::map<uint32_t, Mac48Address> m_linkAddrTable; // id lógico -> link-layer address conocida
    std::map<uint32_t, Time> m_linkAddrLastSeen;      // id lógico -> última observación de link-layer address
    Time m_linkAddrCacheWindow{Seconds(300)};
    Time m_routeTimeout{Seconds(300)};
    Ptr<NetDevice> m_meshDevice;
    bool m_enableDvFlooding{false};
    uint32_t m_dvBeaconMaxRoutes{0};
    uint32_t m_dvBeaconOverheadBytes{1};
    Time m_lastDvBeaconTime{Seconds(0)};
    Time m_extraDvBeaconMinGap{Seconds(0.5)};
    Time m_extraDvBeaconSecondDelay{Seconds(1.0)};
    double m_extraDvBeaconJitterMax{0.2};
    Time m_extraDvBeaconWindow{Seconds(10)};
    uint32_t m_extraDvBeaconMaxPerWindow{2};
    Time m_extraDvWindowStart{Seconds(0)};
    uint32_t m_extraDvCountInWindow{0};
    bool m_disableExtraAfterWarmup{false};
    // MEJORADO: Tabla de vecinos explícita (nextHopId -> NetDevice)
    std::unordered_map<uint32_t, Ptr<NetDevice>> m_neighborDevices;

    // SF empírico basado en historial de beacons exitosos (según paper)
    struct NeighborLinkInfo
    {
        std::array<Time, 6> lastSeenBySf{};           // Último beacon recibido por SF (SF7..SF12)
        std::array<std::deque<Time>, 6> rxTimesBySf;  // Historial corto de Rx por SF
        Time lastUpdate{Seconds(0)};                  // Última actualización general del vecino
        uint8_t lastRxSf{12};                         // Último SF observado (solo debug)
    };

    std::map<uint32_t, NeighborLinkInfo> m_neighborLinks;
    Time m_neighborLinkTimeout{Seconds(60)}; // Ventana efectiva de vigencia por SF para selección empírica
    Time m_neighborLinkTimeoutConfigured{Seconds(60)}; // Valor manual (si auto-timeout está deshabilitado)
    bool m_autoTimeoutsFromBeacon{true}; // Auto-escalado de route/link timeout según beacon activo
    double m_neighborLinkTimeoutFactor{1.0}; // linkFreshness = factor * beaconInterval
    void UpdateNeighborLinkSf(uint32_t neighborId, uint8_t rxSf);
    uint8_t GetDataSfForNeighbor(uint32_t nextHopId) const;
    uint16_t m_routeSwitchMinDeltaX100{5};
    bool m_avoidImmediateBacktrack{true};

    static constexpr uint16_t kProtoMesh = 0x88B5;

    Ptr<loramesh::CsmaCadMac> m_mac;
    void CleanOldSeenPackets();
    void CleanOldDedupCaches();
    void UpdateRouteTimeout();
    uint32_t GetBeaconRouteCapacity() const;

    // NUEVO: Data Traffic Generation
    // ========================================================================
    EventId m_dataGenerationEvt;
    Time m_dataGenerationPeriod{Seconds(10)}; // Se actualiza según TrafficLoad
    double m_dataPeriodJitterMax{0.5};        // Jitter en segundos
    bool m_enableDataSlots{false};                   // Habilita micro-slots locales
    double m_dataSlotPeriodSec{0.0};                 // Periodo de slots para datos [s]
    double m_dataSlotJitterSec{0.0};                 // Jitter +/- dentro del slot [s]
    double m_dataSlotOffsetSec{0.0};                 // Offset fijo por nodo [s]
    uint32_t m_dataPayloadSize{20};                  // 20 bytes por paquete
    uint32_t m_dataSeqPerNode{0};                    // Secuencia de datos por nodo
    uint32_t m_dataPacketsGenerated{0};              // Contador de datos generados
    uint32_t m_dataPacketsDelivered{0};              // Contador de datos entregados
    uint32_t m_dataNoRoute{0};                       // Contador de datos descartados por no ruta
    uint64_t m_cadBusyEvents{0};
    uint64_t m_dutyBlockedEvents{0};
    double m_totalWaitTimeDueToDutySec{0.0};
    uint64_t m_dropNoRoute{0};
    uint64_t m_dropTtlExpired{0};
    uint64_t m_dropQueueOverflow{0};
    uint64_t m_dropBacktrack{0};
    uint64_t m_dropOther{0};
    uint64_t m_beaconScheduled{0};
    uint64_t m_beaconTxSent{0};
    uint64_t m_beaconBlockedByDuty{0};
    uint64_t m_rpGapLargeEvents{0};
    double m_beaconDelaySumSec{0.0};
    std::vector<double> m_beaconDelaySamplesSec;
    std::unordered_map<uint32_t, Time> m_beaconScheduledAtBySeq;
    uint32_t m_collectorNodeId{3};                   // Data collection node (designated sink)
    double m_batteryFullCapacityJ{38880.0};          // Capacidad nominal total para SOC [J]
    bool m_advertiseAllRoutes{true};
    double m_dataStartTimeSec{90.0};                 // Inicio de datos tras convergencia DV
    double m_dataStopTimeSec{-1.0};                  // Fin de generación de datos (-1 = deshabilitado)
    bool m_dataStopLogged{false};                    // Evita logs repetidos al alcanzar dataStop
    bool m_enableDataRandomDest{false};
    bool m_enableDvBroadcast{true}; // Enable/disable DV discovery broadcasts
    TrafficLoadMode m_trafficLoadMode{TrafficLoadMode::MEDIUM};
    std::vector<uint32_t> m_dataDestinations;
    uint32_t m_nextDestIndex{0};
    std::set<uint32_t> m_activeDestinations;
    Time m_activeDestDvDelay{Seconds(2)};
    uint32_t m_postWarmupDvBursts{2};
    Time m_postWarmupDvGap{Seconds(2)};
    EventId m_activeDestDvEvt;
    EventId m_postWarmupDvEvt;

    struct SeenDataInfo
    {
        Time firstSeen{Seconds(0)};
        bool forwarded{false};
        bool hadRoute{false};
    };

    // Deduplicación de datos por nodo (no afecta DV)
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, SeenDataInfo>
        m_seenData;                       // {src,dst,seq} -> info
    Time m_seenDataWindow{Seconds(60)};   // 60 segundos
    Time m_seenPacketWindow{Minutes(10)}; // Ventana para deduplicación (solo sink)
    Time m_dedupWindow{Seconds(600)};     // Ventana para deduplicación dataplane v2 (seq16)
    double m_routeTimeoutFactor{6.0};     // Default ajustado para escenarios con duty cycle activo
    double m_sfMarginDb{2.0};
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, Time> m_deliveredSet; // solo sink (con TTL)
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, Time>
        m_seenOnce; // datos reenviados una vez por nodo (con TTL)

    // CSMA policy tuning: prioritize control (beacons) vs data.
    bool m_prioritizeBeacons{true};
    double m_controlBackoffFactor{0.5};
    double m_dataBackoffFactor{1.0};
    bool m_enableControlGuard{false};
    double m_controlGuardSec{0.0};
    Time m_lastDvTxTime{Seconds(0)};
    Time m_lastDvRxTime{Seconds(0)};

    void GenerateDataTraffic();
    void SendDataPacket(uint32_t dst);
    void SendDataPacketV2(uint32_t dst);
    void CleanOldSeenData();
    Time ComputeNextDataSlotDelay(Time baseDelay);

    void SchedulePeriodicDump();
    void DumpRoute(uint32_t dst, const std::string& tag);
    void DumpFullTable(const std::string& tag) const;
    void RecordBeaconScheduled(uint32_t seq);
    void RecordBeaconTxSent(uint32_t seq);
    double ComputeBeaconDelayP95() const;

    // Timers
    EventId m_evt;      // DV base (legacy)
    EventId m_evtSf9;   // DV en SF9
    EventId m_evtSf10;  // DV en SF10
    EventId m_evtSf12;  // DV en SF12
    EventId m_purgeEvt; // Limpieza de rutas expiradas
    EventId m_backoffEvt;
    EventId m_periodicDumpEvt;
    // Beacon v2 on-air counter state (flags_ttl lower 6 bits).
    uint8_t m_beaconRpCounterTx{0}; // modulo-64 counter for locally transmitted beacons
    std::unordered_map<uint32_t, uint8_t> m_lastBeaconRpCounterRx; // last raw 6-bit counter seen per origin
    std::unordered_map<uint32_t, uint32_t> m_beaconRpExtendedSeqRx; // locally extended monotonic sequence per origin
};

} // namespace ns3
