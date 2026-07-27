/* SPDX-License-Identifier: GPL-2.0-only */

#pragma once
#include "dv-cl-lora-energy-model.h"
#include "dv-cl-mac-csma-cad.h"
#include "dv-cl-metric-tag.h"
#include "dv-cl-routing.h"
#include "dv-cl-stats-sink.h"
#include "dv-cl-wire.h"

#include "ns3/address.h"
#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/log.h"
#include "ns3/mac48-address.h"
#include "ns3/net-device.h"
#include "ns3/nstime.h"
#include "ns3/packet.h"
#include "ns3/random-variable-stream.h"
#include "ns3/simulator.h"

#include <array>
#include <cstdint>
#include <deque>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ns3
{
namespace dvcl
{

/// Metric-input DTO kept from the campaign tree (feeds LinkInputs).
struct LinkStats
{
    double toaUs{0.0};
    uint8_t hops{0};
    uint8_t sf{7};
    double snrDb{0.0};
    double batteryMv{0.0};
    double energyFraction{-1.0};
};

struct TxQueueEntry
{
    Ptr<Packet> packet;
    DvClMetricTag tag;
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
    const RouteEntry* route{nullptr}; // Puntero a la ruta (o nullptr)
    bool exists{false};               // HasAnyRoute() retornó true
    bool expired{false};              // IsRouteExpired() retornó true
    bool valid{false};                // exists && !expired && route != nullptr

    // Helper: true si la ruta es usable para forwarding
    explicit operator bool() const
    {
        return valid;
    }
};

class DvClApp : public Application
{
  public:
    /// Measurement seam: experiments plug their sink (null is legal).
    void SetStatsSink(Ptr<DvClStatsSink> sink)
    {
        m_stats = sink;
    }

    static TypeId GetTypeId();
    DvClApp();
    ~DvClApp() override;

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

    void StartApplication() override;
    void StopApplication() override;

    /**
     * rief Guarantees the close-out accounting runs exactly once.
     *
     * StopApplication reports the final ledger, but ns-3 need not reach it:
     * a simulation stopped at the same instant the application stops can end
     * before that event fires, silently taking every queued frame out of the
     * accounting. Disposal always happens, so the report is anchored here too
     * and the existing flush guard keeps it from running twice.
     */
    void DoDispose() override;
    void ForceFinalFlush(); // §LossFine: flush final stats on early (death-hook) stop

  private:
    struct AppNeighborLink;

    enum class TrafficLoadMode
    {
        LOW,
        MEDIUM,
        HIGH,
        SATURATION
    };

    enum class TrafficMode
    {
        PERIODIC_ANY_TO_ANY,
        PUEYO_ALL_TO_ALL
    };

    enum class SfLinkMode
    {
        OBSERVED_RXSF,
        DETERMINISTIC_SENSITIVITY
    };

    void Tick();
    void BuildAndSendDv(uint8_t sf);
    void BuildAndSendDvPueyo(uint8_t sf);
    void ForwardWithTtl(Ptr<const Packet> pIn, const DvClMetricTag& inTag);
    bool L2Receive(Ptr<NetDevice> dev, Ptr<const Packet> p, uint16_t proto, const Address& from);
    bool L2ReceiveWire(Ptr<NetDevice> dev,
                       Ptr<const Packet> p,
                       uint16_t proto,
                       const Address& from);
    void ForwardWithTtlV2(Ptr<const Packet> pIn,
                          uint16_t src,
                          uint16_t dst,
                          uint16_t seq16,
                          uint8_t ttl);

    void SendWithCSMA(Ptr<Packet> packet,
                      const DvClMetricTag& tag,
                      Address dstAddr,
                      bool logTxMetrics = false);
    void ProcessTxQueue();
    bool IsControlQueueEntry(const TxQueueEntry& entry) const;
    bool IsForwardedDataQueueEntry(const TxQueueEntry& entry) const;
    bool SelectStrictQueueHead(std::size_t* outIndex);
    void RotateQueueEntryToFront(std::size_t idx);
    void ResetStrictRoutingDataBudgets();
    void ResetStrictForwardLocalBudgets();
    void OnBackoffTimer();
    void OnPacketTransmitted(uint32_t toaUs);
    Address ResolveNextHopAddress(uint32_t nextHopId) const;
    bool IsLinkAddrFresh(uint32_t nextHopId) const;
    bool ResolveUnicastNextHopLinkAddr(uint32_t nextHopId,
                                       Mac48Address* outMac,
                                       bool* outStale) const;
    bool TryGetBestRecentSf(const AppNeighborLink& link, Time now, uint8_t* outSf) const;
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
    void SetTrafficMode(std::string mode);
    std::string GetTrafficMode() const;
    void SetSfLinkMode(std::string mode);
    std::string GetSfLinkMode() const;
    void BuildPueyoTrafficSchedule();
    void BootstrapLinkAddrTableFromRx();
    void CountDropNoRouteSrc();
    void CountDropNoRouteRelay();
    void OnPhyCollisionDrop(Ptr<const Packet> packet, uint32_t nodeId);
    void OnPhyBusyDrop(Ptr<const Packet> packet, uint32_t nodeId);

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
    uint16_t ComputeScoreX100(const DvClMetricTag& t) const;

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
    void LogTxEvent(uint32_t src,
                    uint32_t seq,
                    uint32_t dst,
                    uint8_t ttl,
                    uint8_t hops,
                    uint16_t battery,
                    uint16_t score,
                    uint8_t sf,
                    uint32_t toaUs,
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

    void HandleRouteChange(const RouteEntry& entry, const std::string& action);
    void HandleFloodRequest(const DvMessage& msg);
    void ProcessDvPayload(Ptr<const Packet> p,
                          const DvClMetricTag& tag,
                          const Mac48Address& fromMac,
                          uint32_t toaUsNeighbor);
    NeighborLinkInfo BuildNeighborLinkInfo(const DvClMetricTag& tag,
                                           uint32_t toaUs,
                                           Mac48Address fromMac,
                                           uint8_t linkSf) const;
    std::vector<DvEntry> DecodeDvEntries(Ptr<const Packet> p,
                                         const DvClMetricTag& tag,
                                         uint32_t toaUsNeighbor) const;
    std::vector<DvEntry> DecodeDvEntriesPueyo(Ptr<const Packet> p,
                                              uint32_t payloadOffset,
                                              uint32_t toaUsNeighbor,
                                              uint8_t rxSf) const;
    bool ParseDataWirePacketPueyo7b(Ptr<const Packet> p,
                                    DvClDataHeader* outHdr,
                                    Ptr<Packet>* outPayload) const;
    bool ParseBeaconWirePacketPueyo(Ptr<const Packet> p,
                                    DvClBeaconHeader* outHdr,
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
    bool m_purePueyoBaselineMode{false};
    bool m_allowStaleLinkAddrForUnicastData{true};
    uint32_t m_empiricalSfMinSamples{2};
    std::string m_empiricalSfSelectMode{"robust_min"};
    uint32_t m_bw{125000};
    uint8_t m_cr{1};
    uint32_t m_preambleSymbols{8};
    uint8_t m_pl{20};
    bool m_crc{true};
    bool m_ih{false};
    // El flag DE propio se retiro: la optimizacion de baja tasa la decide
    // LowDataRateOptimizationRequired(), unica regla para toda la pila.

    uint32_t m_seq{0};
    uint32_t m_dataSeq{0};
    Time m_beaconWarmupEnd{Seconds(60)};
    double m_beaconWarmupSec{60.0};
    Time m_beaconIntervalWarm{Seconds(10)};
    Time m_beaconIntervalStable{Seconds(60)};
    double m_initialDvDelayBase{1.0};
    double m_initialDvJitterMax{8.0};
    double m_initialDvNodeSpacing{0.5};

    /**
     * rief Charge of \p nodeId for the metric, mirroring the campaign metric:
     * an unknown fraction (<0) is resolved from the energy registry before the
     * link is priced; the voltage fallback then lives in the metric itself.
     */
    double ResolveEnergyFraction(double statsFraction, uint32_t nodeId) const
    {
        if (statsFraction < 0.0 && m_energyModel)
        {
            return m_energyModel->GetEnergyFraction();
        }
        return statsFraction;
    }

    Ptr<DvClLoraEnergyModel> m_energyModel;
    double m_lastAppliedRxScanTimeS{0.0};
    Ptr<DvClRouting> m_routing;
    Ptr<DvClStatsSink> m_stats; //!< measurement seam (null legal)
    std::map<uint32_t, RouteEntry> m_lastRouteSnapshot;
    bool m_routeChangePending{false};

    bool m_csmaEnabled{true};
    bool m_txBusy{false};
    uint32_t m_txCount{0};
    uint32_t m_backoffCount{0};

    uint8_t m_difsCadCount{3};
    Time m_cadDuration{MilliSeconds(5.5)};
    uint8_t m_maxRetries{5};
    // CSMA/CAD policy: bound retries on sustained busy channel and cap queue depth.
    uint32_t m_csmaMaxRetries{8};
    uint32_t m_csmaTxQueueMax{32};

    std::deque<TxQueueEntry> m_txQueue;
    Ptr<UniformRandomVariable> m_rng;

    // Tabla de direcciones de enlace aprendidas por RX.
    // Nota: Mac48Address se usa solo como wrapper interno de ns-3; on-air viajan src/dst/via de 2
    // bytes.
    std::map<uint32_t, Mac48Address> m_linkAddrTable; // id lógico -> link-layer address conocida
    std::map<uint32_t, Time>
        m_linkAddrLastSeen; // id lógico -> última observación de link-layer address
    Time m_linkAddrCacheWindow{Seconds(300)};
    Time m_routeTimeout{Seconds(300)};
    Ptr<NetDevice> m_meshDevice;
    bool m_enableDvFlooding{false};
    uint32_t m_dvBeaconMaxRoutes{0};
    uint32_t m_dvBeaconOverheadBytes{1};
    uint32_t m_dvPayloadMaxBytes{0}; // 0 = derive from MTU, else hard payload cap for route entries
    std::string m_routeAdvertPolicy{"top_score"};
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
    struct AppNeighborLink
    {
        std::array<Time, 6> lastSeenBySf{};          // Último beacon recibido por SF (SF7..SF12)
        std::array<std::deque<Time>, 6> rxTimesBySf; // Historial corto de Rx por SF
        Time lastUpdate{Seconds(0)};                 // Última actualización general del vecino
        uint8_t lastRxSf{12};                        // Último SF observado (solo debug)
        /// SF que la sensibilidad exige para ESTE enlace, derivado del RSSI con
        /// el que se oye al vecino. Es la via fiable: el SF que reporta el tag
        /// del paquete no llega bien al receptor, pero el RSSI si, y la tabla de
        /// sensibilidad del SX1276 lo traduce al SF minimo que cierra el enlace
        /// (VALIDATION.md 2026-07-27). 0 = sin medir.
        uint8_t sensitivitySf{0};
    };

    std::map<uint32_t, AppNeighborLink> m_neighborLinks;
    Time m_neighborLinkTimeout{
        Seconds(60)}; // Ventana efectiva de vigencia por SF para selección empírica
    Time m_neighborLinkTimeoutConfigured{
        Seconds(60)};                    // Valor manual (si auto-timeout está deshabilitado)
    bool m_autoTimeoutsFromBeacon{true}; // Auto-escalado de route/link timeout según beacon activo
    double m_neighborLinkTimeoutFactor{1.0}; // linkFreshness = factor * beaconInterval
    void UpdateNeighborLinkSf(uint32_t neighborId, uint8_t rxSf);
    uint8_t GetDataSfForNeighbor(uint32_t nextHopId) const;
    uint8_t ComputeMinSfBySensitivity(double rxPowerDbm) const;
    uint8_t ResolveSfForLink(uint8_t observedSf, double rxPowerDbm) const;
    void AccountRxScanEnergyDelta();
    uint16_t m_routeSwitchMinDeltaX100{5};
    bool m_avoidImmediateBacktrack{true};
    bool m_pueyoValidationTrace{false};
    int32_t m_pueyoSyntheticEntriesNodeId{-1};
    std::string m_pueyoSyntheticEntries;

    static constexpr uint16_t kProtoMesh = 0x88B5;

    Ptr<DvClCsmaCadMac> m_mac;
    void CleanOldSeenPackets();
    void CleanOldDedupCaches();
    void UpdateRouteTimeout();
    uint32_t GetBeaconRouteCapacity() const;

    // NUEVO: Data Traffic Generation
    // ========================================================================
    EventId m_dataGenerationEvt;
    Time m_dataGenerationPeriod{Seconds(10)}; // Se actualiza según TrafficLoad
    double m_dataPeriodJitterMax{0.5};        // Jitter en segundos
    bool m_dataPeriodJitterSymmetric{false};  // Jitter de media cero en periodos sucesivos
    double m_dataStartPhaseMaxSec{0.0};       // Fase inicial aleatoria [0,max]
    bool m_enableDataSlots{false};            // Habilita micro-slots locales
    double m_dataSlotPeriodSec{0.0};          // Periodo de slots para datos [s]
    double m_dataSlotJitterSec{0.0};          // Jitter +/- dentro del slot [s]
    double m_dataSlotOffsetSec{0.0};          // Offset fijo por nodo [s]
    bool m_dataStartPhaseOnly{false};         // Usa slots solo para el primer envio
    bool m_dataFixedPhaseCadence{false};      // Mantiene la fase fija en cada periodo
    double m_nextDataNominalTimeSec{-1.0};    // Proximo tiempo nominal de generacion
    uint32_t m_dataPayloadSize{20};           // 20 bytes por paquete
    uint32_t m_dataSeqPerNode{0};             // Secuencia de datos por nodo
    uint32_t m_dataPacketsGenerated{0};       // Contador de datos generados
    uint32_t m_dataPacketsDelivered{0};       // Contador de datos entregados
    uint32_t m_dataNoRoute{0};                // Contador de datos descartados por no ruta
    uint64_t m_cadBusyEvents{0};
    uint64_t m_dutyBlockedEvents{0};
    uint64_t m_controlDutyBlocked{0};
    uint64_t m_dataDutyBlocked{0};
    double m_totalWaitTimeDueToDutySec{0.0};
    uint64_t m_dropNoRoute{0};
    uint64_t m_dropNoRouteSrc{0};
    uint64_t m_dropNoRouteRelay{0};
    uint64_t m_dropTtlExpired{0};
    uint64_t m_dropQueueOverflow{0};
    uint64_t m_dropMaxCsmaRetries{0};
    uint64_t m_dropBacktrack{0};
    uint64_t m_dropOther{0};
    uint64_t m_dataCollisionDrops{0};
    uint64_t m_beaconCollisionDrops{0};
    uint64_t m_dataBusyDrops{0};
    uint64_t m_beaconBusyDrops{0};
    uint64_t m_beaconScheduled{0};
    uint64_t m_beaconTxSent{0};
    uint64_t m_beaconRxOk{0};
    uint64_t m_dataTxSent{0};
    uint64_t m_beaconBlockedByDuty{0};
    uint64_t m_beaconSupersededLatestOnly{0};
    uint64_t m_rpGapLargeEvents{0};
    bool m_enableGapAuditTrace{false};
    double m_firstUsableRouteTimeSec{-1.0};
    double m_coverage80RouteTimeSec{-1.0};
    uint64_t m_routesAtDataStart{0};
    uint64_t m_beaconTxAtDataStart{0};
    uint64_t m_beaconRxAtDataStart{0};
    uint64_t m_routesAtMidpoint{0};
    uint64_t m_beaconTxAtMidpoint{0};
    uint64_t m_beaconRxAtMidpoint{0};
    uint64_t m_routesAtDataStop{0};
    uint64_t m_beaconTxAtDataStop{0};
    uint64_t m_beaconRxAtDataStop{0};
    uint64_t m_queuedPacketsAtDataStop{0};
    // [B1] Paquetes que yo originé pero que seguían en mi txQueue al StopApplication.
    // Se excluyen del denominador de PDR efectivo para no deflactar por cortes artificiales.
    uint64_t m_originPendingAtStop{0};
    uint64_t m_originPendingDuty{0};   // §LossFine: origin pending, node alive (duty-starved)
    uint64_t m_originPendingEnergy{0}; // §LossFine: origin pending, node dead (energy)
    uint64_t m_relayPendingEnd{0};     // §LossFine: in-transit pending at relay queue
    bool m_finalFlushed{false};        // §LossFine: idempotency guard
    double m_beaconDelaySumSec{0.0};
    std::vector<double> m_beaconDelaySamplesSec;
    std::unordered_map<uint32_t, Time> m_beaconScheduledAtBySeq;
    uint32_t m_collectorNodeId{3};          // Data collection node (designated sink)
    double m_batteryFullCapacityJ{38880.0}; // Capacidad nominal total para SOC [J]
    double m_initialSocFraction{-1.0};      // SoC inicial sembrado al registro; <0 = lleno
    bool m_advertiseAllRoutes{true};
    double m_dataStartTimeSec{90.0}; // Inicio de datos tras convergencia DV
    double m_dataStopTimeSec{-1.0};  // Fin de generación de datos (-1 = deshabilitado)
    bool m_dataStopLogged{false};    // Evita logs repetidos al alcanzar dataStop
    bool m_enableDataRandomDest{false};
    int32_t m_onlyGenerateFromNodeId{-1};
    int32_t m_forcedDataDestinationId{-1};
    bool m_enableDvBroadcast{true}; // Enable/disable DV discovery broadcasts
    TrafficLoadMode m_trafficLoadMode{TrafficLoadMode::MEDIUM};
    TrafficMode m_trafficMode{TrafficMode::PERIODIC_ANY_TO_ANY};
    SfLinkMode m_sfLinkMode{SfLinkMode::OBSERVED_RXSF};
    double m_sfLinkMarginDb{0.0};
    uint64_t m_sfLinkSamples{0};
    uint64_t m_sfLinkObservedMismatch{0};
    std::vector<uint32_t> m_dataDestinations;
    uint32_t m_nextDestIndex{0};
    uint32_t m_pueyoPacketsPerPair{100};
    std::vector<uint32_t> m_pueyoTrafficSchedule;
    uint32_t m_pueyoTrafficIndex{0};
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

    /// Plano de datos alternativo: inundación gestionada estilo Meshtastic, la
    /// línea de referencia externa del DoE (E6). Con m_floodingMode activo el
    /// nodo no consulta la tabla de rutas: difunde y cada vecino redifunde una
    /// sola vez (la dedup por {src,dst,seq} de m_seenOnce ya lo garantiza)
    /// hasta agotar el TTL.
    bool m_floodingMode{false};
    /// Espera aleatoria antes de redifundir, en ms. Sin ella todos los vecinos
    /// redifunden a la vez y colisionan entre sí; es el mecanismo de contención
    /// que usa cualquier inundación gestionada real.
    uint32_t m_floodJitterMs{500};
    Ptr<UniformRandomVariable> m_floodJitterRng;
    /// via = 0xFFFF marca "difundido": todo receptor lo procesa, en vez del
    /// filtro por siguiente salto que usa el plano DV.
    static constexpr uint16_t kFloodVia = 0xFFFF;
    void FloodRebroadcast(Ptr<const Packet> payload,
                          uint16_t src,
                          uint16_t dst,
                          uint16_t seq16,
                          uint8_t nextTtl,
                          uint8_t sf);

    // Deduplicación de datos por nodo (no afecta DV)
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, SeenDataInfo>
        m_seenData;                       // {src,dst,seq} -> info
    Time m_seenDataWindow{Seconds(60)};   // 60 segundos
    Time m_seenPacketWindow{Minutes(10)}; // Ventana para deduplicación (solo sink)
    Time m_dedupWindow{
        Seconds(86400)}; // [B2] 24h default: evita purgas en runs paper_like; CLI puede bajar
    double m_routeTimeoutFactor{6.0}; // Default ajustado para escenarios con duty cycle activo
    double m_sfMarginDb{2.0};
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, Time> m_deliveredSet; // solo sink (con TTL)
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, Time>
        m_seenOnce; // datos reenviados una vez por nodo (con TTL)

    // CSMA policy tuning: prioritize control (beacons) vs data.
    bool m_prioritizeBeacons{true};
    bool m_beaconLatestOnly{true};
    bool m_usePueyoStrictQueueScheduler{false};
    uint8_t m_strictRoutingBudgetRemaining{10};
    uint8_t m_strictDataBudgetRemaining{1};
    uint8_t m_strictForwardBudgetRemaining{10};
    uint8_t m_strictLocalBudgetRemaining{1};
    double m_controlBackoffFactor{0.5};
    double m_dataBackoffFactor{1.0};
    bool m_enableControlGuard{false};
    double m_controlGuardSec{0.0};
    bool m_studyForwardSpreadEnable{false};
    double m_studyForwardBaseDelayMs{10.0};
    double m_studyForwardSpreadMs{240.0};
    bool m_studySuperframeEnable{false};
    double m_studySuperframePeriodSec{1.0};
    double m_studySuperframeCtrlWindowSec{0.2};
    Time m_lastDvTxTime{Seconds(0)};
    Time m_lastDvRxTime{Seconds(0)};

    void GenerateDataTraffic();
    void SendDataPacket(uint32_t dst);
    void SendDataPacketPueyo7b(uint32_t dst);
    void CleanOldSeenData();
    Time ComputeNextDataSlotDelay(Time baseDelay);
    Time ComputeStudyForwardDelay(const DvClMetricTag& tag) const;
    bool ComputeStudySuperframeWait(const TxQueueEntry& entry,
                                    Time* outWait,
                                    std::string* outReason) const;

    void SchedulePeriodicDump();
    void DumpRoute(uint32_t dst, const std::string& tag);
    void DumpFullTable(const std::string& tag) const;
    void RecordBeaconScheduled(uint32_t seq);
    void RecordBeaconTxSent(uint32_t seq);
    void CaptureControlPlaneAuditSnapshot();
    void CaptureControlPlaneAuditMidpointSnapshot();
    void CaptureControlPlaneAuditDataStopSnapshot();
    void UpdateControlPlaneAuditState();
    double ComputeBeaconDelayP95() const;

    // Timers
    EventId m_evt;      // DV base (legacy)
    EventId m_evtSf9;   // DV en SF9
    EventId m_evtSf10;  // DV en SF10
    EventId m_evtSf12;  // DV en SF12
    EventId m_purgeEvt; // Limpieza de rutas expiradas
    EventId m_backoffEvt;
    EventId m_periodicDumpEvt;
    EventId m_gapAuditEvt;
    // Beacon v2 on-air counter state (flags_ttl lower 6 bits).
    uint8_t m_beaconRpCounterTx{0}; // modulo-64 counter for locally transmitted beacons
    std::unordered_map<uint32_t, uint8_t>
        m_lastBeaconRpCounterRx; // last raw 6-bit counter seen per origin
    std::unordered_map<uint32_t, uint32_t>
        m_beaconRpExtendedSeqRx; // locally extended monotonic sequence per origin
};

} // namespace dvcl
} // namespace ns3
