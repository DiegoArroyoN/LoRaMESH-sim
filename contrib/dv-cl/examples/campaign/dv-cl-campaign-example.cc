/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-campaign-collector.h"

#include <fstream>
#include <type_traits>
#include <sstream>
#include <set>
#include <map>
#include <iomanip>

#include "ns3/core-module.h"
#include "ns3/dv-cl-app.h"
#include "ns3/dv-cl-helper.h"
#include "ns3/dv-cl-lora-energy-model.h"
#include "ns3/dv-cl-lora-net-device.h"
#include "ns3/energy-module.h"
#include "ns3/lora-interference-helper.h"
#include "ns3/mobility-model.h"
#include "ns3/network-module.h"
#include "ns3/rng-seed-manager.h"

#include <cctype>
#include <cmath>
#include <cstdio>

using namespace ns3;
using namespace ns3::dvcl;

NS_LOG_COMPONENT_DEFINE("MeshDvBaseline");

int
main(int argc, char* argv[])
{
    const auto getGitCommitShort = []() -> std::string {
        constexpr std::size_t kBufSize = 64;
        char buffer[kBufSize] = {};
        FILE* pipe = popen("git -C . rev-parse --short HEAD 2>/dev/null", "r");
        if (!pipe)
        {
            return "nogit";
        }
        std::string out;
        if (fgets(buffer, static_cast<int>(kBufSize), pipe) != nullptr)
        {
            out = buffer;
        }
        pclose(pipe);
        while (!out.empty() && (out.back() == '\n' || out.back() == '\r' || out.back() == ' '))
        {
            out.pop_back();
        }
        return out.empty() ? "nogit" : out;
    };

    DvClMeshConfig cfg;
    cfg.nEd = 10;
    cfg.simTimeSec = 150.0;
    cfg.enableDutyCycle = true;
    cfg.dutyLimit = 0.01;
    double dutyWindowSec = 3600.0;
    cfg.spacing = 30.0;
    cfg.gwHeight = 12.0;
    cfg.enableCsma = true;
    bool enablePcap = true;
    const Time warmupTime = Seconds(60.0);
    double dataStartSec = 90.0;
    bool autoDataStartSec = false; // [C2-B] true => scale by grid diameter
    double dataStopSec = -1.0;
    std::string trafficLoad = "medium";
    bool enableDataRandomDest = true; // Always random destinations in mesh
    int32_t onlyGenerateFromNodeId = -1;
    int32_t forcedDataDestinationId = -1;
    std::string sinkNodeIdsCsv =
        "";                // multisink: CSV of sink node IDs (empty = single-sink default)
    uint32_t numSinks = 0; // multisink: auto-place K spread sinks if sinkNodeIds empty
    bool verboseLogs = true;
    uint32_t minBackoffSlots = 4;
    uint32_t backoffStep = 2;
    // CSMA/CAD tuning: CAD decision margin over SX1276 sensitivity (dB),
    // max per-packet retries on sustained CAD-busy, TX queue depth cap.
    double cadSenseMarginDb =
        3.0; // Lowered from 6.0 (validate_margin3 2026-04-24): +37-47% PDR in high-load N=9/25/49,
             // +/-5% in low-load. See FIXES_AND_METHODOLOGY §8.7 item 1.
    uint32_t csmaMaxRetries = 8;
    uint32_t csmaTxQueueMax = 32;
    double beaconIntervalWarmSec = 10.0;
    double beaconIntervalStableSec = 90.0;
    std::string profile = "pueyo2024_paper_like"; // default operativo; extended queda como legacy
    std::string wireFormat = "pueyo7b";           // default operativo; v2 queda como legacy
    std::string trafficMode = "periodic_any_to_any"; // periodic_any_to_any | pueyo_all_to_all
    uint32_t pueyoPacketsPerPair = 100;
    // Periodo de generacion por nodo [s]. 0 = usar el preajuste de
    // trafficLoad. Existe para barrer la carga de forma continua: los
    // preajustes (100/10/1/0.1 s) van de diez en diez y no permiten situar
    // la rodilla de saturacion, que cae entre 100 s y 10 s.
    double dataPeriodSec = 0.0;
    uint32_t dataPayloadSizeBytes = 20;
    uint32_t dvBeaconMaxRoutes = 0;
    uint32_t dvPayloadMaxBytes = 0;              // 0: MTU-derived
    std::string routeAdvertPolicy = "top_score"; // top_score | uniform | cost_weighted
    double linkAddrCacheWindowSec = 300.0;
    double neighborLinkTimeoutSec = -1.0; // <=0 => auto (1x beacon interval)
    bool allowStaleLinkAddrForUnicastData = true;
    uint32_t empiricalSfMinSamples = 2;
    std::string empiricalSfSelectMode = "robust_min";
    bool useProbabilisticSfForBeacons = true;
    uint32_t sfControl = 12;
    uint32_t sfMin = 7;
    uint32_t sfMax = 12;
    uint32_t initTtl = 10;
    uint32_t routeSwitchMinDeltaX100 = 5;
    // Politica de conmutacion, comun a todos los modos de metrica. Se expone
    // como flag para poder correr el 2x2 metrica x histeresis y separar el
    // efecto de la formula del de la pegajosidad de ruta.
    bool routeSwitchHysteresis = true;
    bool avoidImmediateBacktrack = true;
    double dataPeriodJitterMaxSec = 3.0;
    bool dataPeriodJitterSymmetric = false;
    double dataStartPhaseMaxSec = 0.0;
    bool enableDataSlots = false;
    double dataSlotPeriodSec = 0.0;
    double dataSlotJitterSec = 0.0;
    bool dataStartPhaseOnly = false;
    bool dataFixedPhaseCadence = false;
    bool allowTemporalDesyncVariant = false;
    bool allowPaperLikeSfRangeVariant = false;
    uint32_t extraDvBeaconMaxPerWindow = 1;
    double extraDvBeaconMinGapSec = 0.5;
    bool enableProbabilisticCapture = true; // Croce et al.: cross-SF quasi-orthogonality
    double captureSlope = 0.7;
    double captureMinProb = 0.05;
    double captureMaxProb = 0.95;
    // Modelo de interferencia base: goursaud, que sí penaliza la interferencia
    // entre SF distintos (umbrales -16 a -36 dB) en lugar de ignorarla. El
    // modelo pueyo_fixed_capture supone SF perfectamente ortogonales, lo que
    // medimos que infla el PDR hasta un 40% a 49 nodos (VALIDATION.md
    // 2026-07-20); queda disponible como variante de estudio, no como base.
    std::string interferenceModel = "goursaud";
    // Matriz de colisión (eje independiente del modelo de captura): 'goursaud'
    // es la realista (misma SF sobrevive con 6 dB de ventaja) y 'aloha' la
    // estricta, en la que dos tramas del mismo SF se destruyen siempre. La
    // segunda existe en el módulo lorawan precisamente para contrastar contra
    // la teoría de ALOHA puro (validación F2.3).
    std::string collisionMatrix = "goursaud";
    double puelloCaptureThresholdDb = 6.0;
    double puelloAssumedBandwidthHz = 125000.0;
    double puelloPreambleSymbols = 8.0;
    double sfScanEdThresholdDbm = -120.0;
    bool sfScanResetOnNewSignal = true;
    uint32_t preambleSymbols = 8;
    double txPowerDbm = 14.0;
    bool prioritizeBeacons = true;
    bool beaconLatestOnly = true;
    bool pueyoStrictQueueScheduler = false;
    double controlBackoffFactor = 0.8;
    double dataBackoffFactor = 0.6;
    bool enableControlGuard = false;
    double controlGuardSec = 0.0;
    bool studyForwardSpreadEnable = false;
    double studyForwardBaseDelayMs = 10.0;
    double studyForwardSpreadMs = 240.0;
    bool studySuperframeEnable = false;
    double studySuperframePeriodSec = 1.0;
    double studySuperframeCtrlWindowSec = 0.2;
    bool disableExtraAfterWarmup = true;
    double batteryFullCapacityJ =
        3888.0; // 1.08 Wh -- 300 mAh @ 3.6V avg (thesis node energy budget)
    double socInitMin =
        0.60; // §SoCInit: min initial SoC fraction (override flag, default reproduces U[60,100])
    double socInitMax = 1.00; // §SoCInit: max initial SoC fraction (override flag)
    // §SoCBimodal: reparte la carga inicial en dos grupos (mitad en socInitMin,
    // mitad en socInitMax) en vez de uniforme. Con umbrales b_lo=0.20 y
    // b_hi=0.50, un reparto bimodal 0.20/0.90 pone vecinos con Psi=1 junto a
    // vecinos con Psi=0: el contraste maximo que el termino energetico puede
    // aprovechar. Con carga uniforme estrecha, Psi varia poco entre vecinos.
    bool socInitBimodal = false;
    double routeTimeoutFactor = 6.0;
    double pdrEndWindowSec = 0.0;
    double dedupWindowSec =
        86400.0; // [B2] 24h default; anterior=600 podía causar doble-conteo en saturación
    double dvLinkWeight = 0.70;
    double dvPathWeight = 0.25;
    double dvPathHopWeight = 0.05;
    std::string routeMetricMode = "composite_score"; // composite_score | toa_only
    std::string costEncoding = "cost255";            // score100 | cost255 | score255
    std::string sfLinkMode = "observed_rxsf";        // observed_rxsf | deterministic_sensitivity
    double sfLinkMarginDb = 0.0;
    // ABLACION, no modo de operacion. 0 = desactivado. Ver la puerta
    // --allowFlatBeaconBillingOverride y dv-cl-metric-tag.h.
    uint32_t pueyoFlatBeaconBytes = 0;
    double compositeWToa = 0.60;      // alpha -- thesis 4.2
    double compositeWHop = 0.15;      // beta -- thesis 4.2
    double compositeWEnergy = 0.25;   // delta -- thesis 4.2
    double compositeCostStep = 0.025; // quantization step
    double energyLo = 0.20;
    double energyHi = 0.50;
    double energyPow = 2.0;        // §tesis Ec.3 p=2
    double energyMaxPenalty = 1.0; // Psi_max = 1.0 (scaled by delta=0.25)
    // §BatBeacon: toggle + hysteresis (set per-profile below)
    bool useBeaconBattery = true;      // CMP default; TOA profiles force false
    uint32_t socHysteresisPercent = 0; // 0=off; CMP profiles default to 5
    // §DC-aware: filtro de factibilidad por duty cycle (set per-profile below)
    bool useDcAwareRouting = false;       // DV-CL lo activa; resto lo deja en false
    uint32_t dcFeasibilityThreshold = 20; // umbral [0-100%] de DC restante del next-hop
    // §DC-on-override: permite activar DC enforcement (1%) en profiles que normalmente
    // lo tienen off (escenario de evaluación principal de la tesis). Sin este flag,
    // los profiles validan duty disabled (escenario comparativo con Pueyo).
    bool allowDutyOverride = false;
    // §Beacon-override: permite barrer el periodo de beacon fuera de los 60 s que
    // fija el perfil Pueyo (sensibilidad del control plane / operating envelope).
    bool allowBeaconOverride = false;
    // §Robustness-override (round-6): permiten barrer parametros que el perfil
    // Pueyo fija, para baterias de robustez. Default false => comportamiento
    // IDENTICO al binario congelado (regression-safe).
    bool allowShadowOverride = false;
    bool allowPayloadOverride = false;
    bool allowDvPayloadOverride = false;
    bool allowSfMarginOverride = false;
    bool allowFlatBeaconBillingOverride = false;
    bool allowInterferenceModelOverride = false;
    bool allowPacketsPerPairOverride = false;
    bool allowMetricModeOverride = false;
    bool floodingMode = false;
    uint32_t floodJitterMs = 500;
    // §DC-sweep: porcentaje de duty cycle aplicado cuando allowDutyOverride=true
    // (default 1%). Permite barrer el DC (ej. 10%) para analisis de sensibilidad.
    double dutyOverridePct = 1.0;
    // §energyfw-override: permite override del CLI sobre enableNs3EnergyFramework
    // (default false). Necesario para la replica Pueyo (sin energy framework, sin
    // depletion). Aditivo: no afecta a los profiles que forzan energyfw=true.
    bool allowEnergyFwOverride = false;
    uint32_t maxRoutesPerDestination = 1;
    uint32_t maxTotalRoutes = 1024;
    std::string nodePlacementMode = "random";
    double areaWidth = 1000.0;
    double areaHeight = 1000.0;
    double pueyoGridSpacingM = 178.0;
    uint32_t pueyoGridSide = 0; // 0: auto from nEd (must be perfect square)
    uint32_t rngRun = 1;
    // Thesis specification: FLoRa urban environment model
    double pathLossExponent = 2.08;  // FLoRa urban (thesis: n=2.08)
    double referenceDistance = 40.0; // FLoRa reference (thesis: d₀=40m)
    double referenceLossDb = 127.41; // FLoRa urban L(d₀=40m) = 127.41 dB (thesis specification)
    double shadowingSigmaDb = 3.57;
    // Como se aplica el sombreado. "static" (por defecto) saca una muestra
    // por enlace y la mantiene: es lo que hace que el RSSI de la baliza
    // prediga el de los datos. "per_packet" sortea en cada transmision, que
    // es desvanecimiento rapido y no sombreado; se conserva solo para
    // reproducir las campañas anteriores al 2026-07-28. "none" lo desactiva
    // y deja el canal determinista, que es la condicion de
    // Pueyo-Centelles: su paper no modela sombreado ni fast fading.
    std::string shadowingModel = "static";
    bool enableSfScanRx = true;
    bool pueyoFloraLikeRx = false;
    bool enableNs3EnergyFramework = false;
    bool enableMetricsPeriodicFlush = false;
    double metricsFlushIntervalSec = 3600.0;
    bool enableMetricsEssentialOnly = false;
    bool stopOnFullDepletion = true; // Hook dinámico: parar al agotamiento total
    bool enableGapAuditTrace = false;
    bool pueyoValidationTrace = false;
    int32_t pueyoSyntheticEntriesNodeId = -1;
    std::string pueyoSyntheticEntries;

    CommandLine cmd;
    cmd.AddValue("nEd", "Número de end-devices", cfg.nEd);
    cmd.AddValue("spacing", "Separación entre EDs [m]", cfg.spacing);
    cmd.AddValue("gwHeight", "Altura del gateway [m]", cfg.gwHeight);
    cmd.AddValue("stopSec", "Tiempo de simulación [s]", cfg.simTimeSec);
    cmd.AddValue("enablePcap", "Generar archivos pcap por nodo (TX/RX)", enablePcap);
    cmd.AddValue("enableDuty", "Habilitar duty cycle estricto", cfg.enableDutyCycle);
    cmd.AddValue("dutyLimit", "Límite de duty cycle (0.01 = 1%)", cfg.dutyLimit);
    cmd.AddValue("dutyWindowSec", "Ventana duty-cycle [s] (default 3600)", dutyWindowSec);
    cmd.AddValue("enableCsma", "Habilitar CSMA/CAD en capa MAC", cfg.enableCsma);
    cmd.AddValue("dataStartSec", "Start time for data generation (seconds)", dataStartSec);
    cmd.AddValue("autoDataStartSec",
                 "Option B: auto-compute dataStartSec from grid diameter and beacon interval. "
                 "Overrides --dataStartSec when true. Only applies to pueyo_grid topology.",
                 autoDataStartSec);
    cmd.AddValue("dataStopSec",
                 "Stop time for data generation (seconds). -1 disables stop window",
                 dataStopSec);
    cmd.AddValue("trafficLoad", "low/medium/high/saturation", trafficLoad);
    cmd.AddValue("profile",
                 "Execution profile: pueyo2024_paper_like (default) | pueyo2024 | "
                 "pueyo2024_paper_like_csmacad | csmacad_free_backoff | "
                 "proposal_pueyo_like | proposal_pueyo_like_aloha | proposal_pueyo_like_csmacad | "
                 "proposal_pueyo_like_observed | extended (legacy). "
                 "csmacad_free_backoff is identical to pueyo2024_paper_like_csmacad except "
                 "controlBackoffFactor/dataBackoffFactor are CLI-tunable (NOT Pueyo-comparable).",
                 profile);
    cmd.AddValue("trafficMode",
                 "Traffic generator mode: periodic_any_to_any | pueyo_all_to_all",
                 trafficMode);
    cmd.AddValue("onlyGenerateFromNodeId",
                 "Generate application data only from this node id (-1 = all nodes)",
                 onlyGenerateFromNodeId);
    cmd.AddValue("forcedDataDestinationId",
                 "Force all generated data toward this destination node id (-1 = per-mode default)",
                 forcedDataDestinationId);
    cmd.AddValue("sinkNodeIds",
                 "multisink: comma-separated sink node IDs (e.g. 0,24). Empty=single-sink. "
                 "Each non-sink node sends to its nearest sink (Euclidean, pueyo_grid); sinks do "
                 "not generate.",
                 sinkNodeIdsCsv);
    cmd.AddValue(
        "numSinks",
        "multisink: auto-place K spread sinks (over node bounding box) if sinkNodeIds empty. "
        "0=disabled. Works for pueyo_grid and random.",
        numSinks);
    cmd.AddValue("dataPeriodSec",
                 "Periodo de generacion de datos por nodo [s]. >0 manda sobre trafficLoad; "
                 "0 usa el preajuste.",
                 dataPeriodSec);
    cmd.AddValue("pueyoPacketsPerPair",
                 "Packets generated per source-destination pair in pueyo_all_to_all mode",
                 pueyoPacketsPerPair);
    cmd.AddValue("dataPayloadSizeBytes",
                 "Application payload size [bytes] for generated unicast data packets",
                 dataPayloadSizeBytes);
    cmd.AddValue("verboseLogs",
                 "Enable ns-3 component INFO logs for MeshDvBaseline/DvClApp/DvClCsmaCadMac",
                 verboseLogs);
    cmd.AddValue("enableMetricsPeriodicFlush",
                 "Periodically flush detailed metrics CSVs to disk to bound memory usage",
                 enableMetricsPeriodicFlush);
    cmd.AddValue("metricsFlushIntervalSec",
                 "Simulation-time interval in seconds for periodic metrics flush",
                 metricsFlushIntervalSec);
    cmd.AddValue("enableMetricsEssentialOnly",
                 "Keep only essential metrics detail (TX+delay+summary) and skip heavy "
                 "RX/route/overhead traces",
                 enableMetricsEssentialOnly);
    cmd.AddValue("stopOnFullDepletion",
                 "Hook dinámico: detener simulación cuando 100% de nodos muera (techo stopSec "
                 "sigue vigente)",
                 stopOnFullDepletion);
    cmd.AddValue("beaconIntervalWarmSec",
                 "DV beacon interval during warmup [s]",
                 beaconIntervalWarmSec);
    cmd.AddValue("beaconIntervalStableSec",
                 "DV beacon interval during stable phase [s]",
                 beaconIntervalStableSec);
    cmd.AddValue("wireFormat",
                 "Packet wire format: pueyo7b (default comparable) | v2 (legacy generic) | "
                 "v1 (legacy)",
                 wireFormat);
    cmd.AddValue("dvBeaconMaxRoutes",
                 "Max routes per DV beacon (0 = MTU-derived)",
                 dvBeaconMaxRoutes);
    cmd.AddValue("dvPayloadMaxBytes",
                 "Hard cap for DV payload bytes in beacon route entries (0 = MTU-derived)",
                 dvPayloadMaxBytes);
    cmd.AddValue("routeAdvertPolicy",
                 "Route selection policy when beacon payload truncates entries: top_score | "
                 "uniform | cost_weighted",
                 routeAdvertPolicy);
    cmd.AddValue("macCacheWindowSec",
                 "Legacy alias for linkAddr cache window for nextHop resolution [s]",
                 linkAddrCacheWindowSec);
    cmd.AddValue("linkAddrCacheWindowSec",
                 "Link-layer address cache window for nextHop resolution [s]",
                 linkAddrCacheWindowSec);
    cmd.AddValue("neighborLinkTimeoutSec",
                 "Empirical per-SF neighbor history validity window [s] (<=0 auto-correlates to "
                 "beacon interval)",
                 neighborLinkTimeoutSec);
    cmd.AddValue(
        "allowStaleMacForUnicastData",
        "Legacy alias: allow stale known link-layer address for unicast next-hop resolution",
        allowStaleLinkAddrForUnicastData);
    cmd.AddValue("allowStaleLinkAddrForUnicastData",
                 "Allow stale known link-layer address for unicast data next-hop resolution",
                 allowStaleLinkAddrForUnicastData);
    cmd.AddValue("empiricalSfMinSamples",
                 "Minimum recent samples per SF for robust empirical SF selection",
                 empiricalSfMinSamples);
    cmd.AddValue("empiricalSfSelectMode",
                 "Empirical SF selector mode: min | robust_min",
                 empiricalSfSelectMode);
    cmd.AddValue("useProbabilisticSfForBeacons",
                 "Use probabilistic SF selection for DV beacons",
                 useProbabilisticSfForBeacons);
    cmd.AddValue("sfControl",
                 "Fixed SF for control beacons when probabilistic selection is disabled [7..12]",
                 sfControl);
    cmd.AddValue("sfMin", "Minimum SF for probabilistic beacon selection [7..12]", sfMin);
    cmd.AddValue("sfMax", "Maximum SF for probabilistic beacon selection [7..12]", sfMax);
    cmd.AddValue("initTtl",
                 "Initial data TTL (encoded on 6 bits for v2/pueyo7b wire formats)",
                 initTtl);
    cmd.AddValue("routeSwitchMinDeltaX100",
                 "Minimum score delta to switch next-hop in DV updates",
                 routeSwitchMinDeltaX100);
    cmd.AddValue("routeSwitchHysteresis",
                 "Amortiguacion de conmutacion de ruta, aplicada a TODOS los modos de metrica. "
                 "Es politica del protocolo, no parte de la metrica.",
                 routeSwitchHysteresis);
    cmd.AddValue("avoidImmediateBacktrack",
                 "Drop forwarding decisions that immediately backtrack to prevHop",
                 avoidImmediateBacktrack);
    cmd.AddValue("dataPeriodJitterMaxSec", "Max data period jitter [s]", dataPeriodJitterMaxSec);
    cmd.AddValue("dataPeriodJitterSymmetric",
                 "Apply zero-mean jitter to successive data periods.",
                 dataPeriodJitterSymmetric);
    cmd.AddValue("dataStartPhaseMaxSec",
                 "Initial per-node random phase offset max [s] before first data packet.",
                 dataStartPhaseMaxSec);
    cmd.AddValue("enableDataSlots", "Enable local micro-slots for data TX", enableDataSlots);
    cmd.AddValue("dataSlotPeriodSec",
                 "Data slot period [s] (EnableDataSlots=true)",
                 dataSlotPeriodSec);
    cmd.AddValue("dataSlotJitterSec",
                 "Data slot jitter [s] (EnableDataSlots=true)",
                 dataSlotJitterSec);
    cmd.AddValue("dataStartPhaseOnly",
                 "If true, data slots are used only for the first generated packet.",
                 dataStartPhaseOnly);
    cmd.AddValue("dataFixedPhaseCadence",
                 "If true, preserve the assigned node phase on every generation period.",
                 dataFixedPhaseCadence);
    cmd.AddValue(
        "allowTemporalDesyncVariant",
        "Allow temporal desync experiments that relax the strict paper-like jitter constraint.",
        allowTemporalDesyncVariant);
    cmd.AddValue("allowPaperLikeSfRangeVariant",
                 "Allow experimental SF-range overrides on profile=pueyo2024_paper_like.",
                 allowPaperLikeSfRangeVariant);
    cmd.AddValue("extraDvBeaconMaxPerWindow",
                 "Max extra DV beacons per window (0=disable)",
                 extraDvBeaconMaxPerWindow);
    cmd.AddValue("extraDvBeaconMinGapSec",
                 "Min gap between extra DV beacons [s]",
                 extraDvBeaconMinGapSec);
    cmd.AddValue("batteryFullCapacityJ",
                 "Nominal full battery capacity [J] used for SOC tracking",
                 batteryFullCapacityJ);
    cmd.AddValue("socInitMin",
                 "§SoCInit: min initial SoC fraction for random battery init [0-1]",
                 socInitMin);
    cmd.AddValue("socInitBimodal",
                 "Carga inicial bimodal: mitad de los nodos en socInitMin y mitad en "
                 "socInitMax, en vez de uniforme. Maximiza el contraste que ve Psi.",
                 socInitBimodal);
    cmd.AddValue("socInitMax",
                 "§SoCInit: max initial SoC fraction for random battery init [0-1]",
                 socInitMax);
    cmd.AddValue("enableNs3EnergyFramework",
                 "Enable ns-3 BasicEnergySource + DvClLoraEnergyModel battery depletion framework",
                 enableNs3EnergyFramework);
    cmd.AddValue("routeTimeoutFactor",
                 "Route timeout multiplier based on beacon interval",
                 routeTimeoutFactor);
    cmd.AddValue(
        "pdrEndWindowSec",
        "End-window (seconds) excluded from eligible PDR calculation (0=auto from dataStop)",
        pdrEndWindowSec);
    cmd.AddValue("dedupWindowSec",
                 "Dedup cache TTL [s] for dataplane keys (src,dst,seq16)",
                 dedupWindowSec);
    cmd.AddValue("dvLinkWeight",
                 "Legacy/no-op in pure composite DV mode (kept for CLI compatibility)",
                 dvLinkWeight);
    cmd.AddValue("dvPathWeight",
                 "Legacy/no-op in pure composite DV mode (kept for CLI compatibility)",
                 dvPathWeight);
    cmd.AddValue("dvPathHopWeight",
                 "Legacy/no-op in pure composite DV mode (kept for CLI compatibility)",
                 dvPathHopWeight);
    cmd.AddValue("routeMetricMode",
                 "Routing metric mode: composite_score | toa_only | rssi",
                 routeMetricMode);
    cmd.AddValue("costEncoding",
                 "TOA on-air cost encoding: score100 | cost255 | score255",
                 costEncoding);
    cmd.AddValue("compositeWToa",
                 "Weight applied to ToA units in composite_score mode",
                 compositeWToa);
    cmd.AddValue("compositeWHop",
                 "Weight applied to hop count in composite_score mode",
                 compositeWHop);
    cmd.AddValue("compositeWEnergy",
                 "Weight applied to energy penalty in composite_score mode",
                 compositeWEnergy);
    cmd.AddValue("useBeaconBattery",
                 "§BatBeacon: use neighbor battery SoC from beacons in composite routing cost. ",
                 useBeaconBattery);
    cmd.AddValue("socHysteresis",
                 "§BatBeacon: min SoC change [0-50%] before updating route cost (0=disabled).",
                 socHysteresisPercent);
    cmd.AddValue("useDcAwareRouting",
                 "§DC-aware: enable duty-cycle feasibility filter in next-hop selection.",
                 useDcAwareRouting);
    cmd.AddValue("dcFeasibilityThreshold",
                 "§DC-aware: DC-remaining threshold [0-100%] below which a next-hop is infeasible.",
                 dcFeasibilityThreshold);
    cmd.AddValue("allowDutyOverride",
                 "§Eval scenario: allow profiles with duty=off to activate DC enforcement (1%). "
                 "Tesis principal usa DC on.",
                 allowDutyOverride);
    cmd.AddValue("allowBeaconOverride",
                 "§Eval scenario: allow profiles to use the CLI beaconInterval instead of the "
                 "Pueyo-fixed 60s.",
                 allowBeaconOverride);
    cmd.AddValue("allowShadowOverride",
                 "§Robustness: allow CLI shadowingSigmaDb to override the Pueyo-fixed 3.57 dB.",
                 allowShadowOverride);
    cmd.AddValue("allowPayloadOverride",
                 "§Robustness: allow CLI dataPayloadSizeBytes to override the Pueyo-fixed 20 B.",
                 allowPayloadOverride);
    cmd.AddValue("allowDvPayloadOverride",
                 "Permite apartarse de dvPayloadMaxBytes=251 del perfil comparable. Existe para "
                 "medir el coste del plano de control: el tamano de la baliza fija cuantas rutas "
                 "se anuncian por emision, y con 7 B por entrada dvPayloadMaxBytes=7 da una baliza "
                 "de 12 B, que es exactamente lo que FLoRaMesh factura a sus paquetes de ruteo "
                 "(routingPacketMaxSize=12B) INDEPENDIENTEMENTE de cuantas rutas lleven.",
                 allowDvPayloadOverride);
    cmd.AddValue("allowSfMarginOverride",
                 "Permite apartarse de sfLinkMarginDb=0 del perfil comparable. Existe porque el "
                 "margen 0 hace que el selector elija el SF mas rapido que TEORICAMENTE alcanza, "
                 "sin reserva: medido el 2026-08-03, en separaciones de 240-250 m el enlace queda "
                 "con 0.04-0.40 dB de margen, el selector lo declara viable y la red entrega CERO "
                 "sin dar un solo error. Barrer este parametro caracteriza cuanta reserva hace "
                 "falta de verdad.",
                 allowSfMarginOverride);
    cmd.AddValue(
        "pueyoFlatBeaconBytes",
        "ABLACION DE SUPUESTOS AJENOS, NO ES UN MODO DE OPERACION. 0 = desactivado (por "
        "defecto). Si es > 0, toda baliza se factura al aire con ese tamaño fijo mientras sus "
        "entradas de ruta llegan COMPLETAS al receptor. Reproduce a proposito el "
        "setByteLength(routingPacketMaxSize=12) de FLoRaMesh, que en OMNeT++ deja las entradas "
        "en una estructura que nunca se serializa: informacion de ruteo completa al precio de "
        "un paquete minimo. Esa combinacion NO es fisicamente realizable y el simulador no debe "
        "operar nunca asi; existe solo para poner precio a esa decision de modelado ajena. "
        "Exige --allowFlatBeaconBillingOverride.",
        pueyoFlatBeaconBytes);
    cmd.AddValue("allowFlatBeaconBillingOverride",
                 "Abre --pueyoFlatBeaconBytes. Puerta aparte y nombre explicito a proposito: "
                 "activar la facturacion plana produce numeros que NO son de nuestro protocolo "
                 "sino de una emulacion de un defecto ajeno, y no deben acabar en una figura sin "
                 "decirlo.",
                 allowFlatBeaconBillingOverride);
    cmd.AddValue("floodingMode",
                 "§DoE E6: plano de datos por inundacion gestionada (linea de referencia "
                 "externa). No se consulta la tabla de rutas: se difunde y cada vecino "
                 "redifunde una vez hasta agotar el TTL. El plano de control (balizas DV) "
                 "sigue activo pero no se usa para encaminar datos.",
                 floodingMode);
    cmd.AddValue("floodJitterMs",
                 "§DoE E6: espera aleatoria [0,x) ms antes de redifundir (contencion).",
                 floodJitterMs);
    cmd.AddValue("allowMetricModeOverride",
                 "§DoE E1: permitir que routeMetricMode (y sus pesos) sobreescriba el que "
                 "fija el perfil, para barrer composite/toa/hops bajo un mismo perfil sin cambiar "
                 "la base de comparabilidad.",
                 allowMetricModeOverride);
    cmd.AddValue("allowPacketsPerPairOverride",
                 "§Vida util: permitir que pueyoPacketsPerPair sobreescriba el valor fijado "
                 "por el perfil. Con el valor Pueyo (100) el trafico de datos se agota muy "
                 "pronto y el resto de la corrida es solo balizas, lo que hace que la vida util "
                 "mida sobre todo el balizado en una red vacia (VALIDATION.md, 2026-07-22).",
                 allowPacketsPerPairOverride);
    cmd.AddValue("allowInterferenceModelOverride",
                 "§Robustness: allow CLI interferenceModel to override the profile default.",
                 allowInterferenceModelOverride);
    cmd.AddValue(
        "dutyOverridePct",
        "§DC-sweep: duty-cycle percent applied when allowDutyOverride=true (default 1.0 = 1%).",
        dutyOverridePct);
    cmd.AddValue("allowEnergyFwOverride",
                 "§Replica Pueyo: respect CLI enableNs3EnergyFramework even when profile would "
                 "force it true.",
                 allowEnergyFwOverride);
    cmd.AddValue("compositeCostStep",
                 "Quantization step for composite raw metric to 1-byte advertised metric",
                 compositeCostStep);
    cmd.AddValue("energyLo",
                 "Energy fraction threshold below which maximum composite penalty applies",
                 energyLo);
    cmd.AddValue("energyHi",
                 "Energy fraction threshold above which composite energy penalty is zero",
                 energyHi);
    cmd.AddValue("energyPow",
                 "Exponent applied to composite energy penalty interpolation",
                 energyPow);
    cmd.AddValue("energyMaxPenalty",
                 "Maximum raw-cost energy penalty before applying CompositeWEnergy",
                 energyMaxPenalty);
    cmd.AddValue("sfLinkMode",
                 "SF link inference mode: observed_rxsf | deterministic_sensitivity",
                 sfLinkMode);
    cmd.AddValue(
        "sfLinkMarginDb",
        "Margin [dB] added to sensitivity threshold when sfLinkMode=deterministic_sensitivity",
        sfLinkMarginDb);
    cmd.AddValue("maxRoutesPerDestination",
                 "Max route candidates stored per destination in DV table",
                 maxRoutesPerDestination);
    cmd.AddValue("maxTotalRoutes",
                 "Max total route entries in DV table (primary+backup)",
                 maxTotalRoutes);
    cmd.AddValue("nodePlacementMode",
                 "line | random | pueyo_grid | pueyo_random_equiv",
                 nodePlacementMode);
    cmd.AddValue("areaWidth", "Random placement width [m] (mode=random)", areaWidth);
    cmd.AddValue("areaHeight", "Random placement height [m] (mode=random)", areaHeight);
    cmd.AddValue("pueyoGridSpacingM",
                 "Grid spacing [m] for pueyo_grid / pueyo_random_equiv modes",
                 pueyoGridSpacingM);
    cmd.AddValue("pueyoGridSide", "Grid side for pueyo modes (0=auto from nEd)", pueyoGridSide);
    cmd.AddValue("rngRun", "RNG run number for reproducible placement", rngRun);
    cmd.AddValue("pathLossExponent", "Log-distance path loss exponent", pathLossExponent);
    cmd.AddValue("referenceDistance", "Path loss reference distance [m]", referenceDistance);
    cmd.AddValue("referenceLossDb", "Path loss at reference distance [dB]", referenceLossDb);
    cmd.AddValue("shadowingModel",
                 "Aplicacion del sombreado: static (una muestra por enlace) | per_packet "
                 "(heredado, en realidad fast fading) | none (canal determinista, la "
                 "condicion de Pueyo-Centelles).",
                 shadowingModel);
    cmd.AddValue("shadowingSigmaDb",
                 "Log-normal shadowing sigma [dB] in propagation model (0 disables shadowing)",
                 shadowingSigmaDb);
    cmd.AddValue("enableSfScanRx",
                 "Enable RX scan/lock pending-signal acquisition in SimpleGatewayLoraPhy",
                 enableSfScanRx);
    cmd.AddValue("pueyoFloraLikeRx",
                 "Minimal FLoRa-like receive-start ablation: disable pending scan/lock while "
                 "preserving single-channel and single-demod behavior",
                 pueyoFloraLikeRx);
    cmd.AddValue("enableGapAuditTrace",
                 "Enable extra control-plane maturity counters for PDR gap audit",
                 enableGapAuditTrace);
    cmd.AddValue("pueyoValidationTrace",
                 "Enable detailed validation traces for Pueyo beacon-path auditing",
                 pueyoValidationTrace);
    cmd.AddValue("pueyoSyntheticEntriesNodeId",
                 "If >=0, inject synthetic Pueyo beacon entries on this node for validation",
                 pueyoSyntheticEntriesNodeId);
    cmd.AddValue("pueyoSyntheticEntries",
                 "Synthetic Pueyo beacon entries as dst:score,dst:score,...",
                 pueyoSyntheticEntries);
    cmd.AddValue("minBackoffSlots", "Minimum backoff window in slots", minBackoffSlots);
    cmd.AddValue("backoffStep", "Backoff step increment per failure", backoffStep);
    cmd.AddValue("cadSenseMarginDb",
                 "CAD decision margin over SX1276 sensitivity [dB] (default 3.0; lowered from 6.0 "
                 "in 2026-04-24 — see FIXES_AND_METHODOLOGY.md §8.7).",
                 cadSenseMarginDb);
    cmd.AddValue("csmaMaxRetries",
                 "Max CSMA/CAD retries per TX queue entry before drop (default 8).",
                 csmaMaxRetries);
    cmd.AddValue(
        "csmaTxQueueMax",
        "Max TX queue depth for CSMA/CAD path (default 32, oldest data evicted on overflow).",
        csmaTxQueueMax);
    cmd.AddValue("enableProbabilisticCapture",
                 "Enable probabilistic capture for cross-SF collisions",
                 enableProbabilisticCapture);
    cmd.AddValue("captureSlope", "Logistic slope for probabilistic cross-SF capture", captureSlope);
    cmd.AddValue("captureMinProb",
                 "Minimum success probability for cross-SF capture",
                 captureMinProb);
    cmd.AddValue("captureMaxProb",
                 "Maximum success probability for cross-SF capture",
                 captureMaxProb);
    cmd.AddValue("collisionMatrix",
                 "Matriz de colision SNIR: goursaud (realista, con captura) | aloha "
                 "(estricta: misma SF siempre se destruyen; para el ancla F2.3)",
                 collisionMatrix);
    cmd.AddValue("interferenceModel",
                 "Modelo de interferencia PHY: goursaud (base: penaliza cross-SF) | "
                 "pueyo_fixed_capture (variante de estudio: supone SF ortogonales) | "
                 "puello | pueyo (alias del anterior)",
                 interferenceModel);
    cmd.AddValue("puelloCaptureThresholdDb",
                 "Capture threshold [dB] for puello model",
                 puelloCaptureThresholdDb);
    cmd.AddValue("puelloAssumedBandwidthHz",
                 "Assumed bandwidth [Hz] for puello timing collision",
                 puelloAssumedBandwidthHz);
    cmd.AddValue("puelloPreambleSymbols",
                 "Assumed preamble symbols for puello timing collision",
                 puelloPreambleSymbols);
    cmd.AddValue("sfScanEdThresholdDbm",
                 "RX scan energy-detect threshold [dBm] for pending-signal enqueue",
                 sfScanEdThresholdDbm);
    cmd.AddValue("sfScanResetOnNewSignal",
                 "If true, reset scan start to SfScanMin when a new signal arrives during SCAN",
                 sfScanResetOnNewSignal);
    cmd.AddValue("preambleSymbols", "LoRa preamble symbols used in TX parameters", preambleSymbols);
    cmd.AddValue("txPowerDbm", "Node TX power [dBm]", txPowerDbm);
    cmd.AddValue("prioritizeBeacons",
                 "Prioritize DV beacons ahead of data in CSMA queue",
                 prioritizeBeacons);
    cmd.AddValue("beaconLatestOnly", "Keep only latest pending beacon in queue", beaconLatestOnly);
    cmd.AddValue(
        "pueyoStrictQueueScheduler",
        "Enable strict queue scheduler for Pueyo profile (Routing/Data=10:1, Forward/Local=10:1)",
        pueyoStrictQueueScheduler);
    cmd.AddValue("controlBackoffFactor",
                 "Backoff multiplier for control (DV) frames",
                 controlBackoffFactor);
    cmd.AddValue("dataBackoffFactor", "Backoff multiplier for data frames", dataBackoffFactor);
    cmd.AddValue("enableControlGuard",
                 "Enable guard window around DV activity before data TX",
                 enableControlGuard);
    cmd.AddValue("controlGuardSec",
                 "Guard window (seconds) after DV TX/RX before data TX",
                 controlGuardSec);
    cmd.AddValue("studyForwardSpreadEnable",
                 "Study-only: spread relay forwarding attempts over a deterministic delay window",
                 studyForwardSpreadEnable);
    cmd.AddValue("studyForwardBaseDelayMs",
                 "Study-only: base relay forwarding delay [ms]",
                 studyForwardBaseDelayMs);
    cmd.AddValue("studyForwardSpreadMs",
                 "Study-only: extra deterministic relay forwarding spread window [ms]",
                 studyForwardSpreadMs);
    cmd.AddValue("studySuperframeEnable",
                 "Study-only: enforce a lightweight control/data superframe at dequeue time",
                 studySuperframeEnable);
    cmd.AddValue("studySuperframePeriodSec",
                 "Study-only: superframe period [s]",
                 studySuperframePeriodSec);
    cmd.AddValue("studySuperframeCtrlWindowSec",
                 "Study-only: control-only window length inside the superframe [s]",
                 studySuperframeCtrlWindowSec);
    cmd.AddValue("disableExtraAfterWarmup",
                 "Disable extra DV beacons after warmup period",
                 disableExtraAfterWarmup);
    cmd.Parse(argc, argv);

    // Un perfil fija sus parametros DESPUES de parsear la linea de comandos, y
    // eso es deliberado: es lo que hace comparable la replica. Lo que no es
    // aceptable es que el binario acepte un flag y lo descarte en silencio --
    // ya invalido cuatro experimentos (interferenceModel, collisionMatrix,
    // txPowerDbm, pueyoPacketsPerPair) que parecian medir una cosa y median
    // otra. Aqui se compara lo que el usuario pidio contra lo que quedo, y si
    // el perfil lo piso se aborta nombrando el flag (VALIDATION.md, 2026-07-22).
    std::set<std::string> requestedFlags;
    for (int i = 1; i < argc; ++i)
    {
        std::string tok(argv[i]);
        if (tok.rfind("--", 0) != 0)
        {
            continue;
        }
        tok = tok.substr(2);
        const std::size_t eq = tok.find('=');
        requestedFlags.insert(eq == std::string::npos ? tok : tok.substr(0, eq));
    }

    auto asText = [](const auto& v) {
        std::ostringstream o;
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::string>)
        {
            o << v;
        }
        else if constexpr (std::is_integral_v<T>)
        {
            o << static_cast<long long>(v);
        }
        else
        {
            o << std::setprecision(17) << v;
        }
        return o.str();
    };
#define DVCL_SNAP(v) snap[#v] = asText(v)
// Variante para cuando el flag y la variable no se llaman igual. La clave TIENE
// que ser el nombre del flag: el contraste de mas abajo busca por ese nombre y,
// si no lo encuentra, hace continue y no comprueba nada.
#define DVCL_SNAP_AS(nombre, v) snap[nombre] = asText(v)
    auto snapshotProfileForced = [&]() {
        std::map<std::string, std::string> snap;
        // Los ocho de aqui salieron de la auditoria del 30-jul: los perfiles los
        // sobrescriben DESPUES de leer la linea de comandos y ninguno estaba en
        // esta lista, asi que el bloque que aborta ni los miraba. Se comprobo
        // con huellas bit a bit que --enableDuty=false, --dutyLimit=0.05 y
        // --enableCsma=false producian corridas IDENTICAS a no pasarlos: la
        // campaña decia medir una cosa y media otra, igual que sfMax=8.
        // Duty y MAC son ademas dos de los ejes de la matriz factorial, asi que
        // sin esto la matriz habria tenido dos factores fantasma.
        DVCL_SNAP_AS("enableCsma", cfg.enableCsma);
        DVCL_SNAP_AS("enableDuty", cfg.enableDutyCycle);
        DVCL_SNAP_AS("dutyLimit", cfg.dutyLimit);
        DVCL_SNAP_AS("socHysteresis", socHysteresisPercent);
        DVCL_SNAP(enableSfScanRx);
        DVCL_SNAP(pueyoFloraLikeRx);
        DVCL_SNAP(sfLinkMode);
        DVCL_SNAP(useBeaconBattery);
        DVCL_SNAP(beaconIntervalStableSec);
        DVCL_SNAP(beaconLatestOnly);
        DVCL_SNAP(controlBackoffFactor);
        DVCL_SNAP(costEncoding);
        DVCL_SNAP(dataBackoffFactor);
        DVCL_SNAP(dataFixedPhaseCadence);
        DVCL_SNAP(dataPeriodJitterMaxSec);
        DVCL_SNAP(dataPeriodJitterSymmetric);
        DVCL_SNAP(dataSlotJitterSec);
        DVCL_SNAP(dataSlotPeriodSec);
        DVCL_SNAP(dataStartPhaseMaxSec);
        DVCL_SNAP(dataStartPhaseOnly);
        DVCL_SNAP(dutyWindowSec);
        DVCL_SNAP(dvPayloadMaxBytes);
        DVCL_SNAP(enableDataSlots);
        DVCL_SNAP(initTtl);
        DVCL_SNAP(maxRoutesPerDestination);
        DVCL_SNAP(maxTotalRoutes);
        DVCL_SNAP(preambleSymbols);
        DVCL_SNAP(prioritizeBeacons);
        DVCL_SNAP(puelloPreambleSymbols);
        DVCL_SNAP(pueyoStrictQueueScheduler);
        DVCL_SNAP(routeAdvertPolicy);
        DVCL_SNAP(routeSwitchMinDeltaX100);
        DVCL_SNAP(routeTimeoutFactor);
        DVCL_SNAP(sfLinkMarginDb);
        DVCL_SNAP(pueyoFlatBeaconBytes);
        DVCL_SNAP(sfMax);
        DVCL_SNAP(sfMin);
        DVCL_SNAP(sfScanEdThresholdDbm);
        DVCL_SNAP(sfScanResetOnNewSignal);
        DVCL_SNAP(trafficMode);
        DVCL_SNAP(txPowerDbm);
        DVCL_SNAP(useProbabilisticSfForBeacons);
        DVCL_SNAP(wireFormat);
        DVCL_SNAP(routeMetricMode);
        return snap;
    };
#undef DVCL_SNAP
#undef DVCL_SNAP_AS
    const std::map<std::string, std::string> requestedValues = snapshotProfileForced();


    const std::string profileLower = [&profile]() {
        std::string out = profile;
        for (char& ch : out)
        {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        return out;
    }();
    NS_ABORT_MSG_IF(profileLower != "extended" && profileLower != "pueyo2024" &&
                        profileLower != "pueyo2024_paper_like" &&
                        profileLower != "pueyo2024_paper_like_csmacad" &&
                        profileLower != "csmacad_free_backoff" &&
                        profileLower != "proposal_pueyo_like" &&
                        profileLower != "proposal_pueyo_like_aloha" &&
                        profileLower != "proposal_pueyo_like_csmacad" &&
                        profileLower != "proposal_pueyo_like_observed",
                    "Error: profile debe ser 'pueyo2024_paper_like', 'pueyo2024', "
                    "'pueyo2024_paper_like_csmacad', 'csmacad_free_backoff', "
                    "'proposal_pueyo_like', 'proposal_pueyo_like_aloha', "
                    "'proposal_pueyo_like_csmacad', 'proposal_pueyo_like_observed' o 'extended' "
                    "(legacy), valor actual: "
                        << profile);

    const double cliDataPeriodJitterMaxSec = dataPeriodJitterMaxSec;
    const bool cliDataPeriodJitterSymmetric = dataPeriodJitterSymmetric;
    const double cliDataStartPhaseMaxSec = dataStartPhaseMaxSec;
    const uint32_t cliSfMin = sfMin;
    const uint32_t cliSfMax = sfMax;
    const double cliBeaconWarmSec = beaconIntervalWarmSec;
    const double cliBeaconStableSec = beaconIntervalStableSec;
    const bool cliEnergyFw = enableNs3EnergyFramework;
    const bool cliEnableDataSlots = enableDataSlots;
    const double cliDataSlotPeriodSec = dataSlotPeriodSec;
    const double cliDataSlotJitterSec = dataSlotJitterSec;
    const bool cliDataStartPhaseOnly = dataStartPhaseOnly;
    const bool cliDataFixedPhaseCadence = dataFixedPhaseCadence;
    const double cliShadowingSigmaDb = shadowingSigmaDb;
    const uint32_t cliDataPayloadSizeBytes = dataPayloadSizeBytes;
    const uint32_t cliDvPayloadMaxBytes = dvPayloadMaxBytes;
    const double cliSfLinkMarginDb = sfLinkMarginDb;
    const uint32_t cliPueyoFlatBeaconBytes = pueyoFlatBeaconBytes;
    const std::string cliInterferenceModel = interferenceModel;
    const uint32_t cliPueyoPacketsPerPair = pueyoPacketsPerPair;
    const std::string cliRouteMetricMode = routeMetricMode;

    constexpr double kPueyoDefaultDataStartPhaseMaxSec = 100.0;

    // Foto de la base, para poder decir despues que toco el perfil encima.
    std::map<std::string, std::string> baseSnapshot;
    auto applyPueyoComparableBase = [&]() {
        cfg.enableCsma = false;
        cfg.enableDutyCycle = false; // Equivalent to 100% duty availability (no duty gate).
        cfg.dutyLimit = 1.0;
        dutyWindowSec = 3600.0;
        wireFormat = "pueyo7b";
        txPowerDbm = 20.0;
        preambleSymbols = 16;
        puelloPreambleSymbols = static_cast<double>(preambleSymbols);
        interferenceModel = "goursaud";
        sfMin = 7;
        sfMax = 12;
        initTtl = 63;
        useProbabilisticSfForBeacons = true;
        trafficMode = "pueyo_all_to_all";
        pueyoPacketsPerPair = 100;
        enableDataRandomDest = false;
        beaconIntervalWarmSec = 60.0;
        beaconIntervalStableSec = 60.0;
        routeTimeoutFactor = 5.0;
        beaconLatestOnly = false;
        prioritizeBeacons = false;
        pueyoStrictQueueScheduler = true;
        routeSwitchMinDeltaX100 = 0;
        dataPeriodJitterMaxSec = 0.0;
        dataPeriodJitterSymmetric = false;
        dataStartPhaseMaxSec = kPueyoDefaultDataStartPhaseMaxSec;
        enableDataSlots = false;
        dataSlotPeriodSec = 0.0;
        dataSlotJitterSec = 0.0;
        dataStartPhaseOnly = false;
        dataFixedPhaseCadence = false;
        controlBackoffFactor = 1.0;
        dataBackoffFactor = 10.0;
        routeAdvertPolicy = "cost_weighted";
        costEncoding = "cost255";
        sfLinkMarginDb = 0.0;
        // La base comparable NUNCA factura balizas de forma plana: es un
        // artefacto de ablacion, no una opcion de configuracion. Forzarlo aqui
        // es lo que hace que la puerta muerda de verdad -- el aborto de flags
        // descartados compara lo pedido por CLI contra lo que queda tras el
        // perfil, asi que un parametro que el perfil no toca se cuela sin que
        // salte nada (comprobado: sin esta linea, --pueyoFlatBeaconBytes=12 sin
        // puerta devolvia rc=0 y simulaba con facturacion plana).
        pueyoFlatBeaconBytes = 0;
        maxRoutesPerDestination = 2;
        maxTotalRoutes = 1024;
        dvPayloadMaxBytes = 251;
        sfScanEdThresholdDbm = -120.0;
        sfScanResetOnNewSignal = true;
        shadowingSigmaDb = 3.57;
        dataPayloadSizeBytes = 20;
        enableNs3EnergyFramework = true; // active in all profiles [thesis: FND/T50/Psi(bj)]
        baseSnapshot = snapshotProfileForced();
    };

    if (profileLower == "pueyo2024")
    {
        applyPueyoComparableBase();
        routeMetricMode = "toa_only";
        sfLinkMode = "deterministic_sensitivity";
        // Sin esto el PHY espera una adquisicion por escaneo que esta apagada y
        // descarta casi toda recepcion: 13 beacons en 3000 s y ninguna ruta.
        pueyoFloraLikeRx = true;
        NS_LOG_WARN("Aplicando profile=pueyo2024: defaults estrictos para comparabilidad.");
    }
    else if (profileLower == "pueyo2024_paper_like")
    {
        applyPueyoComparableBase();
        routeMetricMode = "toa_only";
        useBeaconBattery = false; // §BatBeacon: TOA ignores SoC
        socHysteresisPercent = 0;
        sfLinkMode = "deterministic_sensitivity";
        sfMin = 7;
        // SF7-12, el rango de la referencia. Estuvo en 7-8 hasta 2026-07-28 y
        // eso truncaba la red: SF7 alcanza 251 m y SF8 350 m, asi que en
        // rejilla a 178 m los nodos a dos celdas (356 m) y las esquinas
        // opuestas (503 m) quedaban fuera del alcance de TODO SF permitido, y
        // el PDR salia entre un 19% y un 24% por debajo. Pueyo-Centelles usa
        // el rango completo: su tabla de sensibilidad cubre SF7-SF12 y el nodo
        // elige "the smallest SF required to successfully transmit".
        sfMax = 12;
        pueyoFloraLikeRx = true;
        enableSfScanRx = false;
        enableNs3EnergyFramework = true; // KPIs: FND y T_50 requieren modelo de energia
        if (allowPaperLikeSfRangeVariant)
        {
            sfMin = cliSfMin;
            sfMax = cliSfMax;
        }
        if (allowBeaconOverride)
        {
            beaconIntervalWarmSec = cliBeaconWarmSec;
            beaconIntervalStableSec = cliBeaconStableSec;
        }
        // §DC-on-override: para el escenario de evaluación principal (DC on 1%).
        if (allowDutyOverride)
        {
            cfg.enableDutyCycle = true;
            cfg.dutyLimit = dutyOverridePct / 100.0;
        }
        NS_LOG_WARN("Aplicando profile=pueyo2024_paper_like: base Pueyo + SF7-8 + FLoRa-like RX + "
                    "energy framework + duty="
                    << (allowDutyOverride ? "ON(1%)" : "off") << ".");
    }
    else if (profileLower == "pueyo2024_paper_like_csmacad")
    {
        // Mirror of paper_like but with CSMA/CAD+backoff MAC enabled; duty-cycle stays
        // disabled (100% availability) so the isolated effect of the MAC can be measured.
        applyPueyoComparableBase();
        cfg.enableCsma = true;
        routeMetricMode = "toa_only";
        useBeaconBattery = false; // §BatBeacon: TOA ignores SoC
        socHysteresisPercent = 0;
        sfLinkMode = "deterministic_sensitivity";
        sfMin = 7;
        // SF7-12, el rango de la referencia. Estuvo en 7-8 hasta 2026-07-28 y
        // eso truncaba la red: SF7 alcanza 251 m y SF8 350 m, asi que en
        // rejilla a 178 m los nodos a dos celdas (356 m) y las esquinas
        // opuestas (503 m) quedaban fuera del alcance de TODO SF permitido, y
        // el PDR salia entre un 19% y un 24% por debajo. Pueyo-Centelles usa
        // el rango completo: su tabla de sensibilidad cubre SF7-SF12 y el nodo
        // elige "the smallest SF required to successfully transmit".
        sfMax = 12;
        pueyoFloraLikeRx = true;
        enableSfScanRx = false;
        if (allowPaperLikeSfRangeVariant)
        {
            sfMin = cliSfMin;
            sfMax = cliSfMax;
        }
        if (allowBeaconOverride)
        {
            beaconIntervalWarmSec = cliBeaconWarmSec;
            beaconIntervalStableSec = cliBeaconStableSec;
        }
        // §DC-on-override: para el escenario de evaluación principal (DC on 1%).
        if (allowDutyOverride)
        {
            cfg.enableDutyCycle = true;
            cfg.dutyLimit = dutyOverridePct / 100.0;
        }
        NS_LOG_WARN("Aplicando profile=pueyo2024_paper_like_csmacad: base Pueyo + SF7-8 + "
                    "FLoRa-like RX + CSMA/CAD + duty="
                    << (allowDutyOverride ? "ON(1%)" : "off") << ".");
    }
    else if (profileLower == "csmacad_free_backoff")
    {
        // Identical to pueyo2024_paper_like_csmacad EXCEPT cbf/dbf are NOT clobbered.
        // Purpose: study sensitivity of PDR to (controlBackoffFactor, dataBackoffFactor)
        // without invalidating the Pueyo profile. Results from this profile are NOT
        // comparable to Pueyo 2024 — different name, different validator, different intent.
        // See FIXES_AND_METHODOLOGY.md §8.7 item 5.

        // Capture CLI-provided cbf/dbf BEFORE applyPueyoComparableBase clobbers them to
        // (1.0, 10.0).
        const double cliCbf = controlBackoffFactor;
        const double cliDbf = dataBackoffFactor;
        applyPueyoComparableBase();
        cfg.enableCsma = true;
        routeMetricMode = "toa_only";
        useBeaconBattery = false; // §BatBeacon: TOA ignores SoC
        socHysteresisPercent = 0;
        sfLinkMode = "deterministic_sensitivity";
        sfMin = 7;
        // SF7-12, el rango de la referencia. Estuvo en 7-8 hasta 2026-07-28 y
        // eso truncaba la red: SF7 alcanza 251 m y SF8 350 m, asi que en
        // rejilla a 178 m los nodos a dos celdas (356 m) y las esquinas
        // opuestas (503 m) quedaban fuera del alcance de TODO SF permitido, y
        // el PDR salia entre un 19% y un 24% por debajo. Pueyo-Centelles usa
        // el rango completo: su tabla de sensibilidad cubre SF7-SF12 y el nodo
        // elige "the smallest SF required to successfully transmit".
        sfMax = 12;
        pueyoFloraLikeRx = true;
        enableSfScanRx = false;
        if (allowPaperLikeSfRangeVariant)
        {
            sfMin = cliSfMin;
            sfMax = cliSfMax;
        }
        if (allowBeaconOverride)
        {
            beaconIntervalWarmSec = cliBeaconWarmSec;
            beaconIntervalStableSec = cliBeaconStableSec;
        }
        // Restore the CLI cbf/dbf — this is the only divergence from pueyo2024_paper_like_csmacad.
        controlBackoffFactor = cliCbf;
        dataBackoffFactor = cliDbf;
        NS_LOG_WARN(
            "Aplicando profile=csmacad_free_backoff: idéntico a pueyo2024_paper_like_csmacad "
            "pero cbf/dbf libres desde CLI (cbf="
            << cliCbf << " dbf=" << cliDbf << "). NO comparable con Pueyo 2024.");
    }
    else if (profileLower == "proposal_pueyo_like")
    {
        applyPueyoComparableBase();
        cfg.enableCsma = true;
        cfg.enableDutyCycle = true;
        cfg.dutyLimit = 0.01;
        dutyWindowSec = 3600.0;
        prioritizeBeacons = false;
        pueyoStrictQueueScheduler = true;
        routeMetricMode = "composite_score";
        useBeaconBattery = true;  // §BatBeacon: real neighbor SoC for Psi(b_j)
        socHysteresisPercent = 5; // 5% threshold reduces routing churn
        sfLinkMode = "deterministic_sensitivity";
        pueyoFloraLikeRx = true;
        enableSfScanRx = false;
        NS_LOG_WARN(
            "Aplicando profile=proposal_pueyo_like: base Pueyo + composite_score + CSMA + duty 1% "
            "+ strict queue + receive-start FLoRa-like + no beacon prioritization.");
    }
    else if (profileLower == "proposal_pueyo_like_observed")
    {
        applyPueyoComparableBase();
        cfg.enableCsma = true;
        cfg.enableDutyCycle = true;
        cfg.dutyLimit = 0.01;
        dutyWindowSec = 3600.0;
        prioritizeBeacons = true;
        pueyoStrictQueueScheduler = false;
        routeMetricMode = "composite_score";
        useBeaconBattery = true; // §BatBeacon
        socHysteresisPercent = 5;
        sfLinkMode = "observed_rxsf";
        // Idem pueyo2024: la combinacion por defecto (sin escaneo y sin
        // recepcion estilo FLoRa) deja al receptor sin forma de engancharse.
        pueyoFloraLikeRx = true;
        NS_LOG_WARN("Aplicando profile=proposal_pueyo_like_observed: base Pueyo + composite_score "
                    "+ CSMA + duty 1% + observed_rxsf.");
    }
    else if (profileLower == "proposal_pueyo_like_aloha")
    {
        // ABLACION CMP-ALOHA: identico a pueyo2024_paper_like en TODO excepto:
        //   - routeMetricMode = composite_score  (unico cambio intencional)
        //   - enableNs3EnergyFramework = true    (necesario para Psi(b_j) y KPIs FND/T_50)
        // MAC: ALOHA (enableCsma=false), duty: off, SF7-8, deterministic_sensitivity,
        // pueyoFloraLikeRx=true -- identico al baseline ToA para comparacion limpia.
        applyPueyoComparableBase();
        routeMetricMode = "composite_score";
        useBeaconBattery = true;  // §BatBeacon: real neighbor SoC for Psi(b_j)
        socHysteresisPercent = 5; // 5% threshold reduces routing churn
        sfLinkMode = "deterministic_sensitivity";
        sfMin = 7;
        // SF7-12, el rango de la referencia. Estuvo en 7-8 hasta 2026-07-28 y
        // eso truncaba la red: SF7 alcanza 251 m y SF8 350 m, asi que en
        // rejilla a 178 m los nodos a dos celdas (356 m) y las esquinas
        // opuestas (503 m) quedaban fuera del alcance de TODO SF permitido, y
        // el PDR salia entre un 19% y un 24% por debajo. Pueyo-Centelles usa
        // el rango completo: su tabla de sensibilidad cubre SF7-SF12 y el nodo
        // elige "the smallest SF required to successfully transmit".
        sfMax = 12;
        pueyoFloraLikeRx = true;
        enableSfScanRx = false;
        enableNs3EnergyFramework = true; // Psi(b_j) en routing + KPIs FND/T_50
        if (allowPaperLikeSfRangeVariant)
        {
            sfMin = cliSfMin;
            sfMax = cliSfMax;
        }
        if (allowBeaconOverride)
        {
            beaconIntervalWarmSec = cliBeaconWarmSec;
            beaconIntervalStableSec = cliBeaconStableSec;
        }
        // §DC-on-override: para el escenario de evaluación principal (DC on 1%).
        if (allowDutyOverride)
        {
            cfg.enableDutyCycle = true;
            cfg.dutyLimit = dutyOverridePct / 100.0;
        }
        NS_LOG_WARN("Aplicando profile=proposal_pueyo_like_aloha: ABLACION CMP-ALOHA -- "
                    "unico cambio vs pueyo2024_paper_like: composite_score + energy framework. "
                    "MAC=ALOHA, duty="
                    << (allowDutyOverride ? "ON(1%)" : "off")
                    << ", SF7-8, deterministic. Comparacion metodologicamente limpia.");
    }
    else if (profileLower == "proposal_pueyo_like_csmacad")
    {
        // ABLACION CMP-CSMA/CAD: identico a pueyo2024_paper_like_csmacad en TODO excepto:
        //   - routeMetricMode = composite_score  (unico cambio intencional)
        //   - enableNs3EnergyFramework = true    (necesario para Psi(b_j) y KPIs FND/T_50)
        applyPueyoComparableBase();
        cfg.enableCsma = true;
        routeMetricMode = "composite_score";
        useBeaconBattery = true;  // §BatBeacon: real neighbor SoC for Psi(b_j)
        socHysteresisPercent = 5; // 5% threshold reduces routing churn
        sfLinkMode = "deterministic_sensitivity";
        sfMin = 7;
        // SF7-12, el rango de la referencia. Estuvo en 7-8 hasta 2026-07-28 y
        // eso truncaba la red: SF7 alcanza 251 m y SF8 350 m, asi que en
        // rejilla a 178 m los nodos a dos celdas (356 m) y las esquinas
        // opuestas (503 m) quedaban fuera del alcance de TODO SF permitido, y
        // el PDR salia entre un 19% y un 24% por debajo. Pueyo-Centelles usa
        // el rango completo: su tabla de sensibilidad cubre SF7-SF12 y el nodo
        // elige "the smallest SF required to successfully transmit".
        sfMax = 12;
        pueyoFloraLikeRx = true;
        enableSfScanRx = false;
        enableNs3EnergyFramework = true;
        if (allowPaperLikeSfRangeVariant)
        {
            sfMin = cliSfMin;
            sfMax = cliSfMax;
        }
        if (allowBeaconOverride)
        {
            beaconIntervalWarmSec = cliBeaconWarmSec;
            beaconIntervalStableSec = cliBeaconStableSec;
        }
        // §DC-on-override: para el escenario de evaluación principal (DC on 1%).
        if (allowDutyOverride)
        {
            cfg.enableDutyCycle = true;
            cfg.dutyLimit = dutyOverridePct / 100.0;
        }
        NS_LOG_WARN(
            "Aplicando profile=proposal_pueyo_like_csmacad: ABLACION CMP-CSMA/CAD -- "
            "unico cambio vs pueyo2024_paper_like_csmacad: composite_score + energy framework. "
            "MAC=CSMA/CAD, duty="
            << (allowDutyOverride ? "ON(1%)" : "off")
            << ", SF7-8, deterministic. Comparacion metodologicamente limpia.");
    }

    if (allowTemporalDesyncVariant &&
        (profileLower == "pueyo2024" || profileLower == "pueyo2024_paper_like" ||
         profileLower == "pueyo2024_paper_like_csmacad" || profileLower == "csmacad_free_backoff" ||
         profileLower == "proposal_pueyo_like" || profileLower == "proposal_pueyo_like_aloha" ||
         profileLower == "proposal_pueyo_like_csmacad" ||
         profileLower == "proposal_pueyo_like_observed"))
    {
        dataPeriodJitterMaxSec = cliDataPeriodJitterMaxSec;
        dataPeriodJitterSymmetric = cliDataPeriodJitterSymmetric;
        dataStartPhaseMaxSec = cliDataStartPhaseMaxSec;
        enableDataSlots = cliEnableDataSlots;
        dataSlotPeriodSec = cliDataSlotPeriodSec;
        dataSlotJitterSec = cliDataSlotJitterSec;
        dataStartPhaseOnly = cliDataStartPhaseOnly;
        dataFixedPhaseCadence = cliDataFixedPhaseCadence;
    }

    // §Robustness-override (round-6): tras aplicar el perfil (que fija estos
    // valores) y antes de validar, restaurar el valor CLI cuando el flag esta
    // activo. Los NS_ABORT correspondientes se relajan con el mismo flag.
    // Sin flag (default) no se toca nada => identico al binario congelado.
    if (allowShadowOverride)
    {
        shadowingSigmaDb = cliShadowingSigmaDb;
    }
    if (allowPayloadOverride)
    {
        dataPayloadSizeBytes = cliDataPayloadSizeBytes;
    }
    if (allowDvPayloadOverride)
    {
        dvPayloadMaxBytes = cliDvPayloadMaxBytes;
    }
    if (allowSfMarginOverride)
    {
        sfLinkMarginDb = cliSfLinkMarginDb;
    }
    if (allowFlatBeaconBillingOverride)
    {
        pueyoFlatBeaconBytes = cliPueyoFlatBeaconBytes;
    }
    if (allowInterferenceModelOverride)
    {
        interferenceModel = cliInterferenceModel;
    }
    if (allowPacketsPerPairOverride)
    {
        pueyoPacketsPerPair = cliPueyoPacketsPerPair;
    }
    if (allowMetricModeOverride)
    {
        routeMetricMode = cliRouteMetricMode;
    }

    // Nueve de los parametros que fija el perfil tienen una via legitima para
    // variarlos; los otros 31 no. Nombrar el override en el propio error ahorra
    // ir a leer el codigo para averiguar cual es.
    static const std::map<std::string, std::string> kOverrideFor = {
        {"beaconIntervalStableSec", "allowBeaconOverride"},
        {"beaconIntervalWarmSec", "allowBeaconOverride"},
        {"dataPayloadSizeBytes", "allowPayloadOverride"},
        {"dvPayloadMaxBytes", "allowDvPayloadOverride"},
        {"sfLinkMarginDb", "allowSfMarginOverride"},
        {"pueyoFlatBeaconBytes", "allowFlatBeaconBillingOverride"},
        {"enableNs3EnergyFramework", "allowEnergyFwOverride"},
        {"interferenceModel", "allowInterferenceModelOverride"},
        {"pueyoPacketsPerPair", "allowPacketsPerPairOverride"},
        {"routeMetricMode", "allowMetricModeOverride"},
        {"sfMax", "allowPaperLikeSfRangeVariant"},
        {"sfMin", "allowPaperLikeSfRangeVariant"},
        {"shadowingSigmaDb", "allowShadowOverride"},
    };

    NS_ABORT_MSG_IF(shadowingModel != "static" && shadowingModel != "per_packet" &&
                        shadowingModel != "none",
                    "Error: shadowingModel debe ser static, per_packet o none (recibido '"
                        << shadowingModel << "')");

    // Que toco el perfil ENCIMA de la base. El sfMax=8 de los perfiles estuvo
    // ahi desde f30a63359 sin que nadie lo eligiera: truncaba la red a 350 m de
    // alcance y costaba entre un 19% y un 24% de PDR, y no se vio en tres meses
    // porque el valor solo aparecia enterrado en el JSON de cada corrida. Un
    // perfil PUEDE apartarse de la base -- para eso existe -- pero apartarse en
    // silencio es lo que hace que una campaña diga medir una cosa y mida otra.
    if (!baseSnapshot.empty())
    {
        const std::map<std::string, std::string> tras = snapshotProfileForced();
        std::ostringstream cambios;
        std::size_t nCambios = 0;
        for (const auto& [clave, valorBase] : baseSnapshot)
        {
            const auto it = tras.find(clave);
            if (it != tras.end() && it->second != valorBase)
            {
                cambios << (nCambios++ ? ", " : "") << clave << ": " << valorBase << " -> "
                        << it->second;
            }
        }
        std::cerr << "[perfil] '" << profileLower << "' modifica " << nCambios
                  << " valor(es) de la base comparable"
                  << (nCambios ? ": " + cambios.str() : "") << std::endl;
    }

    // Contraste: lo pedido por linea de comandos frente a lo que quedo tras
    // aplicar el perfil. Un flag aceptado y descartado en silencio hace que la
    // corrida diga medir una cosa y mida otra.
    {
        const std::map<std::string, std::string> effective = snapshotProfileForced();
        std::ostringstream discarded;
        std::size_t n = 0;
        for (const auto& [flag, requested] : requestedValues)
        {
            if (!requestedFlags.count(flag))
            {
                continue;
            }
            const auto it = effective.find(flag);
            if (it == effective.end() || it->second == requested)
            {
                continue;
            }
            discarded << (n++ ? "; " : "") << "--" << flag << " (pedido " << requested
                      << ", aplicado " << it->second << ")";
            const auto ov = kOverrideFor.find(flag);
            discarded << (ov != kOverrideFor.end()
                              ? " -> usa --" + ov->second + "=true"
                              : std::string(" -> este parametro no admite override"));
        }
        NS_ABORT_MSG_IF(n > 0,
                        "El perfil '" << profileLower << "' descarta " << n
                                      << " flag(s) que pediste: " << discarded.str() << ".");
    }

    auto validatePueyoComparableBase = [&](const std::string& profileName) {
        NS_ABORT_MSG_IF(wireFormat != "pueyo7b",
                        "Error: profile=" << profileName << " requiere wireFormat=pueyo7b");
        NS_ABORT_MSG_IF(txPowerDbm != 20.0,
                        "Error: profile=" << profileName << " requiere txPowerDbm=20");
        NS_ABORT_MSG_IF(preambleSymbols != 16,
                        "Error: profile=" << profileName << " requiere preambleSymbols=16");
        NS_ABORT_MSG_IF(trafficMode != "pueyo_all_to_all",
                        "Error: profile=" << profileName
                                          << " requiere trafficMode=pueyo_all_to_all");
        NS_ABORT_MSG_IF(pueyoPacketsPerPair != 100 && !allowPacketsPerPairOverride,
                        "Error: profile=" << profileName
                                          << " requiere pueyoPacketsPerPair=100 (usar "
                                             "allowPacketsPerPairOverride para sostener trafico)");
        NS_ABORT_MSG_IF(enableDataRandomDest,
                        "Error: profile=" << profileName << " requiere enableDataRandomDest=false");
        NS_ABORT_MSG_IF((beaconIntervalWarmSec != 60.0 || beaconIntervalStableSec != 60.0) &&
                            !allowBeaconOverride,
                        "Error: profile=" << profileName
                                          << " requiere beaconIntervalWarm/Stable=60s (usar "
                                             "allowBeaconOverride para barrer)");
        NS_ABORT_MSG_IF(routeTimeoutFactor != 5.0,
                        "Error: profile=" << profileName << " requiere routeTimeoutFactor=5");
        NS_ABORT_MSG_IF(routeAdvertPolicy != "cost_weighted",
                        "Error: profile=" << profileName
                                          << " requiere routeAdvertPolicy=cost_weighted");
        NS_ABORT_MSG_IF(dvPayloadMaxBytes != 251 && !allowDvPayloadOverride,
                        "Error: profile=" << profileName << " requiere dvPayloadMaxBytes=251");
        NS_ABORT_MSG_IF(maxRoutesPerDestination != 2 || maxTotalRoutes != 1024,
                        "Error: profile=" << profileName << " requiere tabla DV 2/1024");
        NS_ABORT_MSG_IF(costEncoding != "cost255",
                        "Error: profile=" << profileName << " requiere costEncoding=cost255");
        NS_ABORT_MSG_IF(interferenceModel != "goursaud" && !allowInterferenceModelOverride,
                        "Error: profile=" << profileName
                                          << " requiere interferenceModel=goursaud (base); use "
                                             "--allowInterferenceModelOverride para variar");
        NS_ABORT_MSG_IF(dataPeriodJitterMaxSec != 0.0 && !allowTemporalDesyncVariant,
                        "Error: profile=" << profileName << " requiere dataPeriodJitterMaxSec=0");
        NS_ABORT_MSG_IF(dataStartPhaseMaxSec != kPueyoDefaultDataStartPhaseMaxSec &&
                            !allowTemporalDesyncVariant,
                        "Error: profile=" << profileName << " requiere dataStartPhaseMaxSec="
                                          << kPueyoDefaultDataStartPhaseMaxSec);
        NS_ABORT_MSG_IF(dataPayloadSizeBytes != 20 && !allowPayloadOverride,
                        "Error: profile=" << profileName << " requiere dataPayloadSizeBytes=20");
        NS_ABORT_MSG_IF(sfScanEdThresholdDbm != -120.0 || !sfScanResetOnNewSignal,
                        "Error: profile="
                            << profileName
                            << " requiere sfScanEdThresholdDbm=-120 y SfScanResetOnNewSignal=true");
        NS_ABORT_MSG_IF(std::fabs(shadowingSigmaDb - 3.57) > 1e-9 && !allowShadowOverride,
                        "Error: profile=" << profileName << " requiere shadowingSigmaDb=3.57");
    };

    if (profileLower == "pueyo2024")
    {
        validatePueyoComparableBase(profileLower);
        NS_ABORT_MSG_IF(cfg.enableCsma, "Error: profile=pueyo2024 requiere enableCsma=false");
        NS_ABORT_MSG_IF(!pueyoFloraLikeRx,
                        "Error: profile=pueyo2024 requiere pueyoFloraLikeRx=true");
        NS_ABORT_MSG_IF(cfg.enableDutyCycle || cfg.dutyLimit != 1.0,
                        "Error: profile=pueyo2024 requiere duty disabled y dutyLimit=1.0");
        NS_ABORT_MSG_IF(routeMetricMode != "toa_only",
                        "Error: profile=pueyo2024 requiere routeMetricMode=toa_only");
        NS_ABORT_MSG_IF(sfLinkMode != "deterministic_sensitivity",
                        "Error: profile=pueyo2024 requiere sfLinkMode=deterministic_sensitivity");
    }
    else if (profileLower == "pueyo2024_paper_like")
    {
        validatePueyoComparableBase(profileLower);
        NS_ABORT_MSG_IF(cfg.enableCsma,
                        "Error: profile=pueyo2024_paper_like requiere enableCsma=false");
        if (!allowDutyOverride)
        {
            NS_ABORT_MSG_IF(cfg.enableDutyCycle || cfg.dutyLimit != 1.0,
                            "Error: profile=pueyo2024_paper_like requiere duty disabled y "
                            "dutyLimit=1.0 (sin --allowDutyOverride)");
        }
        else
        {
            NS_ABORT_MSG_IF(!cfg.enableDutyCycle,
                            "Error: profile=pueyo2024_paper_like+allowDutyOverride requiere "
                            "duty=on y dutyLimit=0.01");
        }
        NS_ABORT_MSG_IF(routeMetricMode != "toa_only",
                        "Error: profile=pueyo2024_paper_like requiere routeMetricMode=toa_only");
        NS_ABORT_MSG_IF(
            sfLinkMode != "deterministic_sensitivity",
            "Error: profile=pueyo2024_paper_like requiere sfLinkMode=deterministic_sensitivity");
        if (!allowPaperLikeSfRangeVariant)
        {
            NS_ABORT_MSG_IF(sfMin != 7 || sfMax != 12,
                            "Error: profile=pueyo2024_paper_like requiere sfMin=7 y sfMax=12");
        }
        else
        {
            NS_ABORT_MSG_IF(sfMin != 7 || sfMax < 8 || sfMax > 12,
                            "Error: profile=pueyo2024_paper_like con "
                            "allowPaperLikeSfRangeVariant=true requiere sfMin=7 y sfMax en [8,12]");
        }
        NS_ABORT_MSG_IF(!pueyoFloraLikeRx,
                        "Error: profile=pueyo2024_paper_like requiere pueyoFloraLikeRx=true");
        NS_ABORT_MSG_IF(enableSfScanRx,
                        "Error: profile=pueyo2024_paper_like requiere EnableSfScanRx=false");
    }
    else if (profileLower == "pueyo2024_paper_like_csmacad")
    {
        validatePueyoComparableBase(profileLower);
        NS_ABORT_MSG_IF(!cfg.enableCsma,
                        "Error: profile=pueyo2024_paper_like_csmacad requiere enableCsma=true");
        if (!allowDutyOverride)
        {
            NS_ABORT_MSG_IF(cfg.enableDutyCycle || cfg.dutyLimit != 1.0,
                            "Error: profile=pueyo2024_paper_like_csmacad requiere duty disabled y "
                            "dutyLimit=1.0 (sin --allowDutyOverride)");
        }
        else
        {
            NS_ABORT_MSG_IF(!cfg.enableDutyCycle,
                            "Error: profile=pueyo2024_paper_like_csmacad+allowDutyOverride "
                            "requiere duty=on y dutyLimit=0.01");
        }
        NS_ABORT_MSG_IF(
            routeMetricMode != "toa_only",
            "Error: profile=pueyo2024_paper_like_csmacad requiere routeMetricMode=toa_only");
        NS_ABORT_MSG_IF(sfLinkMode != "deterministic_sensitivity",
                        "Error: profile=pueyo2024_paper_like_csmacad requiere "
                        "sfLinkMode=deterministic_sensitivity");
        if (!allowPaperLikeSfRangeVariant)
        {
            NS_ABORT_MSG_IF(
                sfMin != 7 || sfMax != 12,
                "Error: profile=pueyo2024_paper_like_csmacad requiere sfMin=7 y sfMax=12");
        }
        else
        {
            NS_ABORT_MSG_IF(sfMin != 7 || sfMax < 8 || sfMax > 12,
                            "Error: profile=pueyo2024_paper_like_csmacad con "
                            "allowPaperLikeSfRangeVariant=true requiere sfMin=7 y sfMax en [8,12]");
        }
        NS_ABORT_MSG_IF(
            !pueyoFloraLikeRx,
            "Error: profile=pueyo2024_paper_like_csmacad requiere pueyoFloraLikeRx=true");
        NS_ABORT_MSG_IF(
            enableSfScanRx,
            "Error: profile=pueyo2024_paper_like_csmacad requiere EnableSfScanRx=false");
    }
    else if (profileLower == "csmacad_free_backoff")
    {
        // Validator parallels pueyo2024_paper_like_csmacad EXCEPT no check on cbf/dbf.
        // That's the entire reason this profile exists.
        validatePueyoComparableBase(profileLower);
        NS_ABORT_MSG_IF(!cfg.enableCsma,
                        "Error: profile=csmacad_free_backoff requiere enableCsma=true");
        NS_ABORT_MSG_IF(
            cfg.enableDutyCycle || cfg.dutyLimit != 1.0,
            "Error: profile=csmacad_free_backoff requiere duty disabled y dutyLimit=1.0");
        NS_ABORT_MSG_IF(routeMetricMode != "toa_only",
                        "Error: profile=csmacad_free_backoff requiere routeMetricMode=toa_only");
        NS_ABORT_MSG_IF(
            sfLinkMode != "deterministic_sensitivity",
            "Error: profile=csmacad_free_backoff requiere sfLinkMode=deterministic_sensitivity");
        if (!allowPaperLikeSfRangeVariant)
        {
            NS_ABORT_MSG_IF(sfMin != 7 || sfMax != 12,
                            "Error: profile=csmacad_free_backoff requiere sfMin=7 y sfMax=12");
        }
        else
        {
            NS_ABORT_MSG_IF(sfMin != 7 || sfMax < 8 || sfMax > 12,
                            "Error: profile=csmacad_free_backoff con "
                            "allowPaperLikeSfRangeVariant=true requiere sfMin=7 y sfMax en [8,12]");
        }
        NS_ABORT_MSG_IF(!pueyoFloraLikeRx,
                        "Error: profile=csmacad_free_backoff requiere pueyoFloraLikeRx=true");
        NS_ABORT_MSG_IF(enableSfScanRx,
                        "Error: profile=csmacad_free_backoff requiere EnableSfScanRx=false");
        // Note: cbf/dbf are intentionally NOT validated here; sweeping them is the
        // whole purpose of this profile.
    }
    else if (profileLower == "proposal_pueyo_like" ||
             profileLower == "proposal_pueyo_like_observed")
    {
        validatePueyoComparableBase(profileLower);
        NS_ABORT_MSG_IF(!cfg.enableCsma,
                        "Error: profile=" << profileLower << " requiere enableCsma=true");
        NS_ABORT_MSG_IF(!cfg.enableDutyCycle || cfg.dutyLimit != 0.01 || dutyWindowSec != 3600.0,
                        "Error: profile=" << profileLower << " requiere duty 1% con ventana 3600s");
        NS_ABORT_MSG_IF(routeMetricMode != "composite_score" && !allowMetricModeOverride,
                        "Error: profile=" << profileLower
                                          << " requiere routeMetricMode=composite_score");
        NS_ABORT_MSG_IF(beaconLatestOnly,
                        "Error: profile=" << profileLower << " requiere BeaconLatestOnly=false");
        NS_ABORT_MSG_IF(controlBackoffFactor != 1.0 || dataBackoffFactor != 10.0,
                        "Error: profile=" << profileLower
                                          << " requiere backoff factors control=1.0 data=10.0");
        if (profileLower == "proposal_pueyo_like")
        {
            NS_ABORT_MSG_IF(prioritizeBeacons,
                            "Error: profile=proposal_pueyo_like requiere PrioritizeBeacons=false");
            NS_ABORT_MSG_IF(
                !pueyoStrictQueueScheduler,
                "Error: profile=proposal_pueyo_like requiere PueyoStrictQueueScheduler=true");
            NS_ABORT_MSG_IF(
                sfLinkMode != "deterministic_sensitivity",
                "Error: profile=proposal_pueyo_like requiere sfLinkMode=deterministic_sensitivity");
            NS_ABORT_MSG_IF(!pueyoFloraLikeRx,
                            "Error: profile=proposal_pueyo_like requiere pueyoFloraLikeRx=true");
            NS_ABORT_MSG_IF(enableSfScanRx,
                            "Error: profile=proposal_pueyo_like requiere EnableSfScanRx=false");
        }
        else
        {
            NS_ABORT_MSG_IF(
                !prioritizeBeacons,
                "Error: profile=proposal_pueyo_like_observed requiere PrioritizeBeacons=true");
        NS_ABORT_MSG_IF(!pueyoFloraLikeRx,
                        "Error: profile=proposal_pueyo_like_observed requiere "
                        "pueyoFloraLikeRx=true");
            NS_ABORT_MSG_IF(pueyoStrictQueueScheduler,
                            "Error: profile=proposal_pueyo_like_observed requiere "
                            "PueyoStrictQueueScheduler=false");
            NS_ABORT_MSG_IF(
                sfLinkMode != "observed_rxsf",
                "Error: profile=proposal_pueyo_like_observed requiere sfLinkMode=observed_rxsf");
        }
    }
    else if (profileLower == "proposal_pueyo_like_aloha")
    {
        validatePueyoComparableBase(profileLower);
        NS_ABORT_MSG_IF(
            cfg.enableCsma,
            "Error: profile=proposal_pueyo_like_aloha requiere enableCsma=false (MAC ALOHA)");
        if (!allowDutyOverride)
        {
            NS_ABORT_MSG_IF(cfg.enableDutyCycle || cfg.dutyLimit != 1.0,
                            "Error: profile=proposal_pueyo_like_aloha requiere duty disabled y "
                            "dutyLimit=1.0 (sin --allowDutyOverride)");
        }
        else
        {
            NS_ABORT_MSG_IF(!cfg.enableDutyCycle,
                            "Error: profile=proposal_pueyo_like_aloha+allowDutyOverride requiere "
                            "duty=on y dutyLimit=0.01");
        }
        NS_ABORT_MSG_IF(
            routeMetricMode != "composite_score" && !allowMetricModeOverride,
            "Error: profile=proposal_pueyo_like_aloha requiere routeMetricMode=composite_score");
        NS_ABORT_MSG_IF(sfLinkMode != "deterministic_sensitivity",
                        "Error: profile=proposal_pueyo_like_aloha requiere "
                        "sfLinkMode=deterministic_sensitivity");
        if (!allowPaperLikeSfRangeVariant)
        {
            NS_ABORT_MSG_IF(sfMin != 7 || sfMax != 12,
                            "Error: profile=proposal_pueyo_like_aloha requiere sfMin=7 y sfMax=12");
        }
        else
        {
            NS_ABORT_MSG_IF(sfMin != 7 || sfMax < 8 || sfMax > 12,
                            "Error: proposal_pueyo_like_aloha+allowPaperLikeSfRangeVariant "
                            "requiere sfMin=7 sfMax en [8,12]");
        }
        NS_ABORT_MSG_IF(!pueyoFloraLikeRx,
                        "Error: profile=proposal_pueyo_like_aloha requiere pueyoFloraLikeRx=true");
        NS_ABORT_MSG_IF(enableSfScanRx,
                        "Error: profile=proposal_pueyo_like_aloha requiere enableSfScanRx=false");
        NS_ABORT_MSG_IF(
            !enableNs3EnergyFramework && !allowEnergyFwOverride,
            "Error: profile=proposal_pueyo_like_aloha requiere enableNs3EnergyFramework=true (usar "
            "allowEnergyFwOverride para deshabilitar)");
    }
    else if (profileLower == "proposal_pueyo_like_csmacad")
    {
        validatePueyoComparableBase(profileLower);
        NS_ABORT_MSG_IF(!cfg.enableCsma,
                        "Error: profile=proposal_pueyo_like_csmacad requiere enableCsma=true");
        // §DC-on-override: con allowDutyOverride, el profile activa DC=on (1%) y la
        // validación de duty=off se omite (escenario de evaluación principal).
        if (!allowDutyOverride)
        {
            NS_ABORT_MSG_IF(cfg.enableDutyCycle || cfg.dutyLimit != 1.0,
                            "Error: profile=proposal_pueyo_like_csmacad requiere duty disabled y "
                            "dutyLimit=1.0 (sin --allowDutyOverride)");
        }
        else
        {
            NS_ABORT_MSG_IF(!cfg.enableDutyCycle,
                            "Error: profile=proposal_pueyo_like_csmacad+allowDutyOverride requiere "
                            "duty=on y dutyLimit=0.01");
        }
        NS_ABORT_MSG_IF(
            routeMetricMode != "composite_score" && !allowMetricModeOverride,
            "Error: profile=proposal_pueyo_like_csmacad requiere routeMetricMode=composite_score");
        NS_ABORT_MSG_IF(sfLinkMode != "deterministic_sensitivity",
                        "Error: profile=proposal_pueyo_like_csmacad requiere "
                        "sfLinkMode=deterministic_sensitivity");
        if (!allowPaperLikeSfRangeVariant)
        {
            NS_ABORT_MSG_IF(
                sfMin != 7 || sfMax != 12,
                "Error: profile=proposal_pueyo_like_csmacad requiere sfMin=7 y sfMax=12");
        }
        else
        {
            NS_ABORT_MSG_IF(sfMin != 7 || sfMax < 8 || sfMax > 12,
                            "Error: proposal_pueyo_like_csmacad+allowPaperLikeSfRangeVariant "
                            "requiere sfMin=7 sfMax en [8,12]");
        }
        NS_ABORT_MSG_IF(
            !pueyoFloraLikeRx,
            "Error: profile=proposal_pueyo_like_csmacad requiere pueyoFloraLikeRx=true");
        NS_ABORT_MSG_IF(enableSfScanRx,
                        "Error: profile=proposal_pueyo_like_csmacad requiere enableSfScanRx=false");
        NS_ABORT_MSG_IF(
            !enableNs3EnergyFramework && !allowEnergyFwOverride,
            "Error: profile=proposal_pueyo_like_csmacad requiere enableNs3EnergyFramework=true "
            "(usar allowEnergyFwOverride para deshabilitar)");
    }

    if (pueyoFloraLikeRx)
    {
        enableSfScanRx = false;
        NS_LOG_WARN("Aplicando pueyoFloraLikeRx=true: misma semantica single-channel/single-demod "
                    "y mismo modelo de colision; solo se desactiva pending scan/lock para usar "
                    "lock inmediato viable.");
    }

    // FIX C3: Validación de parámetros CLI
    NS_ABORT_MSG_IF(cfg.nEd < 1, "Error: nEd debe ser >= 1 (necesitas al menos 2 nodos para mesh)");
    NS_ABORT_MSG_IF(cfg.dutyLimit < 0.0 || cfg.dutyLimit > 1.0,
                    "Error: dutyLimit debe estar en [0.0, 1.0], valor actual: " << cfg.dutyLimit);
    // [C2-B] Option B: auto-scale dataStartSec by grid convergence time.
    // Only applies to pueyo_grid topology; no-op otherwise.
    if (autoDataStartSec)
    {
        if (nodePlacementMode == "pueyo_grid" || nodePlacementMode == "pueyo_random_equiv")
        {
            const uint32_t sideB =
                (pueyoGridSide > 0)
                    ? pueyoGridSide
                    : static_cast<uint32_t>(std::llround(std::sqrt(static_cast<double>(cfg.nEd))));
            const uint32_t diameter = (sideB > 1u) ? 2u * (sideB - 1u) : 0u;
            const double warmupSec = 60.0; // warmupTime is const Seconds(60)
            const double warmupHops = std::floor(warmupSec / beaconIntervalWarmSec);
            const double remHops = std::max(0.0, static_cast<double>(diameter) - warmupHops);
            const double convSec = warmupSec + remHops * beaconIntervalStableSec;
            const double newDataStart = std::ceil(convSec * 1.3);
            NS_LOG_INFO("[C2-autoDataStart] N="
                        << cfg.nEd << " side=" << sideB << " diam=" << diameter << " warmupHops="
                        << warmupHops << " remHops=" << remHops << " conv=" << convSec << "s"
                        << " => dataStartSec=" << newDataStart << "s"
                        << " (was " << dataStartSec << "s)");
            dataStartSec = newDataStart;
        }
        else
        {
            NS_LOG_WARN("[C2-autoDataStart] nodePlacementMode='"
                        << nodePlacementMode
                        << "' is not a grid — autoDataStartSec ignored, "
                           "keeping dataStartSec="
                        << dataStartSec << "s");
        }
    }

    NS_ABORT_MSG_IF(dataStartSec < 0.0,
                    "Error: dataStartSec debe ser >= 0, valor actual: " << dataStartSec);
    NS_ABORT_MSG_IF(cfg.simTimeSec <= 0.0,
                    "Error: simTimeSec debe ser > 0, valor actual: " << cfg.simTimeSec);
    NS_ABORT_MSG_IF(dataStartSec >= cfg.simTimeSec,
                    "Error: dataStartSec (" << dataStartSec << ") debe ser < simTimeSec ("
                                            << cfg.simTimeSec << ")");
    NS_ABORT_MSG_IF(dataStopSec >= 0.0 && dataStopSec <= dataStartSec,
                    "Error: dataStopSec (" << dataStopSec << ") debe ser > dataStartSec ("
                                           << dataStartSec << ") o -1 para deshabilitar");
    NS_ABORT_MSG_IF(dataStopSec >= cfg.simTimeSec,
                    "Error: dataStopSec (" << dataStopSec << ") debe ser < simTimeSec ("
                                           << cfg.simTimeSec << ")");
    NS_ABORT_MSG_IF(puelloCaptureThresholdDb < 0.0,
                    "Error: puelloCaptureThresholdDb debe ser >= 0");
    NS_ABORT_MSG_IF(puelloAssumedBandwidthHz <= 0.0,
                    "Error: puelloAssumedBandwidthHz debe ser > 0");
    NS_ABORT_MSG_IF(puelloPreambleSymbols < 0.0, "Error: puelloPreambleSymbols debe ser >= 0");
    NS_ABORT_MSG_IF(
        preambleSymbols < 6 || preambleSymbols > 64,
        "Error: preambleSymbols debe estar en [6,64], valor actual: " << preambleSymbols);
    NS_ABORT_MSG_IF(wireFormat != "pueyo7b",
                    "Error: el modulo implementa un unico wire (pueyo7b), "
                    "valor actual: "
                        << wireFormat);
    NS_ABORT_MSG_IF(sfControl < 7 || sfControl > 12,
                    "Error: sfControl debe estar en [7,12], valor actual: " << sfControl);
    NS_ABORT_MSG_IF(sfMin < 7 || sfMin > 12 || sfMax < 7 || sfMax > 12,
                    "Error: sfMin/sfMax deben estar en [7,12], valores actuales: sfMin="
                        << sfMin << " sfMax=" << sfMax);
    NS_ABORT_MSG_IF(
        sfMin > sfMax,
        "Error: sfMin debe ser <= sfMax, valores actuales: sfMin=" << sfMin << " sfMax=" << sfMax);
    NS_ABORT_MSG_IF(
        initTtl > 63,
        "Error: initTtl debe estar en [0,63] para wire v2/pueyo7b, valor actual: " << initTtl);
    NS_ABORT_MSG_IF(
        sfScanEdThresholdDbm < -160.0 || sfScanEdThresholdDbm > -60.0,
        "Error: sfScanEdThresholdDbm fuera de rango razonable [-160,-60], valor actual: "
            << sfScanEdThresholdDbm);
    NS_ABORT_MSG_IF(
        trafficMode != "periodic_any_to_any" && trafficMode != "pueyo_all_to_all",
        "Error: trafficMode debe ser 'periodic_any_to_any' o 'pueyo_all_to_all', valor actual: "
            << trafficMode);
    NS_ABORT_MSG_IF(onlyGenerateFromNodeId < -1 ||
                        onlyGenerateFromNodeId >= static_cast<int32_t>(cfg.nEd),
                    "Error: onlyGenerateFromNodeId debe estar en [-1, nEd-1], valor actual: "
                        << onlyGenerateFromNodeId << " nEd=" << cfg.nEd);
    NS_ABORT_MSG_IF(forcedDataDestinationId < -1 ||
                        forcedDataDestinationId >= static_cast<int32_t>(cfg.nEd),
                    "Error: forcedDataDestinationId debe estar en [-1, nEd-1], valor actual: "
                        << forcedDataDestinationId << " nEd=" << cfg.nEd);
    NS_ABORT_MSG_IF(
        onlyGenerateFromNodeId >= 0 && forcedDataDestinationId >= 0 &&
            onlyGenerateFromNodeId == forcedDataDestinationId,
        "Error: onlyGenerateFromNodeId y forcedDataDestinationId no pueden ser iguales");
    NS_ABORT_MSG_IF(dataPayloadSizeBytes < 1 || dataPayloadSizeBytes > 250,
                    "Error: dataPayloadSizeBytes debe estar en [1,250], valor actual: "
                        << dataPayloadSizeBytes);
    NS_ABORT_MSG_IF(
        routeAdvertPolicy != "top_score" && routeAdvertPolicy != "uniform" &&
            routeAdvertPolicy != "cost_weighted",
        "Error: routeAdvertPolicy debe ser top_score|uniform|cost_weighted, valor actual: "
            << routeAdvertPolicy);
    NS_ABORT_MSG_IF(routeMetricMode != "composite_score" && routeMetricMode != "toa_only" &&
                        routeMetricMode != "rssi",
                    "Error: routeMetricMode debe ser composite_score|toa_only, valor actual: "
                        << routeMetricMode);
    NS_ABORT_MSG_IF(
        costEncoding != "score100" && costEncoding != "cost255" && costEncoding != "score255",
        "Error: costEncoding debe ser score100|cost255|score255, valor actual: " << costEncoding);
    NS_ABORT_MSG_IF(
        sfLinkMode != "observed_rxsf" && sfLinkMode != "deterministic_sensitivity",
        "Error: sfLinkMode debe ser observed_rxsf|deterministic_sensitivity, valor actual: "
            << sfLinkMode);
    NS_ABORT_MSG_IF(
        sfLinkMarginDb < -10.0 || sfLinkMarginDb > 20.0,
        "Error: sfLinkMarginDb fuera de rango [-10,20], valor actual: " << sfLinkMarginDb);
    NS_ABORT_MSG_IF(maxRoutesPerDestination < 1 || maxRoutesPerDestination > 2,
                    "Error: maxRoutesPerDestination debe estar en [1,2], valor actual: "
                        << maxRoutesPerDestination);
    NS_ABORT_MSG_IF(maxTotalRoutes < 1,
                    "Error: maxTotalRoutes debe ser >=1, valor actual: " << maxTotalRoutes);
    // La facturacion plana solo tiene sentido entre la cabecera de baliza (6 B,
    // por debajo no cabe ni el encabezado) y la MTU LoRa. Y se avisa por stderr
    // cada vez que se activa: es un artefacto deliberado y no puede pasar
    // inadvertido en un log de campaña.
    NS_ABORT_MSG_IF(pueyoFlatBeaconBytes > 0 && (pueyoFlatBeaconBytes < 6 ||
                                                 pueyoFlatBeaconBytes > 255),
                    "Error: pueyoFlatBeaconBytes debe ser 0 (desactivado) o estar en [6,255], "
                    "valor actual: "
                        << pueyoFlatBeaconBytes);
    if (pueyoFlatBeaconBytes > 0)
    {
        std::cerr << "[ABLACION] facturacion plana de balizas ACTIVADA a " << pueyoFlatBeaconBytes
                  << " B: las rutas llegan completas y el aire se cobra a tamaño fijo. "
                     "Emulacion del setByteLength(12) de FLoRaMesh. Estos numeros NO son de "
                     "nuestro protocolo -- no reportar sin declararlo."
                  << std::endl;
    }
    NS_ABORT_MSG_IF(nodePlacementMode != "line" && nodePlacementMode != "random" &&
                        nodePlacementMode != "pueyo_grid" &&
                        nodePlacementMode != "pueyo_random_equiv",
                    "Error: nodePlacementMode debe ser line|random|pueyo_grid|pueyo_random_equiv, "
                    "valor actual: "
                        << nodePlacementMode);
    if (nodePlacementMode == "pueyo_grid" || nodePlacementMode == "pueyo_random_equiv")
    {
        const uint32_t sideAuto =
            static_cast<uint32_t>(std::llround(std::sqrt(static_cast<double>(cfg.nEd))));
        const uint32_t side = (pueyoGridSide > 0) ? pueyoGridSide : sideAuto;
        NS_ABORT_MSG_IF(side == 0 || side * side != cfg.nEd,
                        "Error: en modo pueyo_* nEd debe ser cuadrado perfecto (o definir "
                        "pueyoGridSide válido). nEd="
                            << cfg.nEd << " side=" << side);
        NS_ABORT_MSG_IF(pueyoGridSpacingM <= 0.0,
                        "Error: pueyoGridSpacingM debe ser >0 en modo pueyo_*");
        areaWidth = (side > 1) ? (side - 1) * pueyoGridSpacingM : pueyoGridSpacingM;
        areaHeight = (side > 1) ? (side - 1) * pueyoGridSpacingM : pueyoGridSpacingM;
        pueyoGridSide = side;
    }

    // La matriz es un miembro estatico leido por el constructor de cada
    // LoraInterferenceHelper, de modo que debe fijarse antes de crear los PHY.
    if (collisionMatrix == "aloha")
    {
        lorawan::LoraInterferenceHelper::collisionMatrix = lorawan::LoraInterferenceHelper::ALOHA;
    }
    else if (collisionMatrix == "goursaud")
    {
        lorawan::LoraInterferenceHelper::collisionMatrix =
            lorawan::LoraInterferenceHelper::GOURSAUD;
    }
    else
    {
        NS_ABORT_MSG("Error: collisionMatrix debe ser 'goursaud' o 'aloha', valor actual: "
                     << collisionMatrix);
    }

    lorawan::LoraInterferenceHelper::InterferenceModel interferenceModelEnum =
        lorawan::LoraInterferenceHelper::PUEYO_FIXED_CAPTURE;
    if (interferenceModel == "goursaud")
    {
        interferenceModelEnum = lorawan::LoraInterferenceHelper::GOURSAUD_PROBABILISTIC;
    }
    else if (interferenceModel == "puello" || interferenceModel == "pueyo" ||
             interferenceModel == "pueyo_fixed_capture")
    {
        interferenceModelEnum = lorawan::LoraInterferenceHelper::PUEYO_FIXED_CAPTURE;
    }
    else
    {
        NS_ABORT_MSG("Error: interferenceModel debe ser 'goursaud', 'puello', 'pueyo' o "
                     "'pueyo_fixed_capture', valor actual: "
                     << interferenceModel);
    }

    if (verboseLogs)
    {
        LogComponentEnable("MeshDvBaseline", LOG_LEVEL_INFO);
        LogComponentEnable("DvClApp", LOG_LEVEL_INFO);
        LogComponentEnable("DvClCsmaCadMac", LOG_LEVEL_INFO);
    }

    NS_LOG_INFO("=== Mesh DV Baseline - Peer-to-Peer Network ===");
    NS_LOG_INFO("Total nodes: " << (cfg.nEd + 1) << " (all identical mesh nodes)");
    NS_LOG_INFO("Duration: " << cfg.simTimeSec << " s");
    NS_LOG_INFO("NodePlacement=" << nodePlacementMode << " area=" << areaWidth << "x" << areaHeight
                                 << " rngRun=" << rngRun);
    const Time dataStartTime = Seconds(dataStartSec);
    NS_LOG_INFO("Warm-up DV only until " << warmupTime.GetSeconds() << "s, data apps start at "
                                         << dataStartTime.GetSeconds() << "s");
    NS_LOG_INFO("Backoff parameters: minBackoffSlots=" << minBackoffSlots
                                                       << ", backoffStep=" << backoffStep);

    // Crear recolector ANTES de la simulación
    static Ptr<MetricsCollector> s_collectorKeepAlive;
    s_collectorKeepAlive = CreateObject<MetricsCollector>();
    g_metricsCollector = PeekPointer(s_collectorKeepAlive);
    g_metricsCollector->SetSimulationStopSec(cfg.simTimeSec);
    if (pdrEndWindowSec <= 0.0 && dataStopSec >= 0.0 && dataStopSec < cfg.simTimeSec)
    {
        pdrEndWindowSec = cfg.simTimeSec - dataStopSec;
    }
    dedupWindowSec = (dedupWindowSec > 0.0) ? dedupWindowSec : std::max(60.0, pdrEndWindowSec);
    g_metricsCollector->SetEndWindowSec(std::max(0.0, pdrEndWindowSec));
    g_metricsCollector->SetEssentialMetricsOnly(enableMetricsEssentialOnly);
    g_metricsCollector->SetStopOnFullDepletion(stopOnFullDepletion);
    const uint32_t dataHeaderBytes = DvClDataHeader::kSerializedSize;
    const uint32_t beaconHeaderBytes = DvClBeaconHeader::kSerializedSize;
    const uint32_t dvEntryBytes = DvClDvEntry::kEntrySize;
    g_metricsCollector->SetWireFormatMetadata(wireFormat,
                                              dataHeaderBytes,
                                              beaconHeaderBytes,
                                              dvEntryBytes);
    auto trafficIntervalFromLoad = [&trafficLoad]() {
        std::string load = trafficLoad;
        for (char& ch : load)
        {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        if (load == "low")
        {
            return 100.0;
        }
        if (load == "high")
        {
            return 1.0;
        }
        if (load == "saturation")
        {
            return 0.1;
        }
        return 10.0; // medium default
    };
    const auto resolveGridSideForMetadata = [&]() -> uint32_t {
        if (pueyoGridSide > 0)
        {
            return pueyoGridSide;
        }
        const double sideF = std::sqrt(static_cast<double>(cfg.nEd));
        const uint32_t side = static_cast<uint32_t>(std::llround(sideF));
        return side;
    };
    const uint32_t resolvedGridSide =
        (nodePlacementMode == "pueyo_grid" || nodePlacementMode == "pueyo_random_equiv")
            ? resolveGridSideForMetadata()
            : pueyoGridSide;
    const double resolvedAreaWidthM =
        (nodePlacementMode == "pueyo_grid" || nodePlacementMode == "pueyo_random_equiv")
            ? ((resolvedGridSide > 1) ? (resolvedGridSide - 1) * pueyoGridSpacingM : 0.0)
            : areaWidth;
    const double resolvedAreaHeightM =
        (nodePlacementMode == "pueyo_grid" || nodePlacementMode == "pueyo_random_equiv")
            ? ((resolvedGridSide > 1) ? (resolvedGridSide - 1) * pueyoGridSpacingM : 0.0)
            : areaHeight;
    const std::string gitCommitShort = getGitCommitShort();
    MetricsCollector::RunConfigMetadata runMeta;
    runMeta.gitCommit = gitCommitShort;
    runMeta.simVersion = "sim_v2.5_" + gitCommitShort + "_" + profileLower + "_" + wireFormat;
    runMeta.nNodes = cfg.nEd;
    runMeta.topology = nodePlacementMode;
    runMeta.topologyPreset =
        (nodePlacementMode == "pueyo_grid" || nodePlacementMode == "pueyo_random_equiv")
            ? ("spacing_" + std::to_string(static_cast<int>(pueyoGridSpacingM)) + "m")
            : "";
    runMeta.gridSide = resolvedGridSide;
    runMeta.gridSpacingXM = pueyoGridSpacingM;
    runMeta.gridSpacingYM = pueyoGridSpacingM;
    runMeta.areaWidthM = resolvedAreaWidthM;
    runMeta.areaHeightM = resolvedAreaHeightM;
    runMeta.rngRun = rngRun;
    runMeta.enableCsma = cfg.enableCsma;
    runMeta.enableDuty = cfg.enableDutyCycle;
    runMeta.dutyLimit = cfg.dutyLimit;
    runMeta.dutyWindowSec = dutyWindowSec;
    runMeta.dataStartSec = dataStartSec;
    runMeta.dataStopSec = dataStopSec;
    runMeta.stopSec = cfg.simTimeSec;
    runMeta.pdrEndWindowSec = pdrEndWindowSec;
    runMeta.trafficLoad = trafficLoad;
    runMeta.trafficIntervalS = trafficIntervalFromLoad();
    runMeta.trafficMode = trafficMode;
    runMeta.enableDataRandomDest = enableDataRandomDest;
    runMeta.onlyGenerateFromNodeId = onlyGenerateFromNodeId;
    runMeta.forcedDataDestinationId = forcedDataDestinationId;
    runMeta.payloadBytes = dataPayloadSizeBytes;
    runMeta.dedupWindowSec = dedupWindowSec;
    runMeta.beaconIntervalWarmS = beaconIntervalWarmSec;
    runMeta.beaconIntervalStableS = beaconIntervalStableSec;
    runMeta.routeTimeoutFactor = routeTimeoutFactor;
    runMeta.routeTimeoutSec = beaconIntervalStableSec * routeTimeoutFactor;
    runMeta.interferenceModel = interferenceModel;
    runMeta.propModel = "log_distance";
    runMeta.frequencyHz = 868000000;
    runMeta.bandwidthHz = 125000;
    runMeta.codingRate = "4/5";
    runMeta.sfMin = sfMin;
    runMeta.sfMax = sfMax;

    // Configuracion EFECTIVA en un fichero propio, tras aplicar el perfil y
    // todos los overrides.
    //
    // El sfMax=8 de los perfiles sobrevivio tres meses de campañas porque su
    // valor solo aparecia en el JSON de cada corrida, que los runners borraban
    // al reducir la corrida a una fila. El rastro existia y nadie lo leyo. Con
    // un fichero aparte, corto y estable, los runners pueden (a) guardarlo
    // junto a cada fila de resultados y (b) COMPROBAR que coincide con lo que
    // la campaña dice estar midiendo, que es lo que convierte un defecto
    // silencioso en un aborto en la primera celda.
    //
    // Dos lineas: cabecera y valores, para que un runner pueda pegar la segunda
    // a su fila y verificar la primera contra lo que espera.
    {
        std::ofstream cfgFile("mesh_dv_effective_config.csv");
        const std::vector<std::pair<std::string, std::string>> efectiva = {
            {"git", gitCommitShort},
            {"profile", profileLower},
            {"metric", routeMetricMode},
            {"alpha", std::to_string(compositeWToa)},
            {"beta", std::to_string(compositeWHop)},
            {"delta", std::to_string(compositeWEnergy)},
            {"wsum", std::to_string(compositeWToa + compositeWHop + compositeWEnergy)},
            {"coststep", std::to_string(compositeCostStep)},
            {"costenc", costEncoding},
            {"sfmin", std::to_string(sfMin)},
            {"sfmax", std::to_string(sfMax)},
            {"sflinkmode", sfLinkMode},
            {"shadow", shadowingModel},
            {"sigma", std::to_string(shadowingSigmaDb)},
            {"pathloss", std::to_string(pathLossExponent)},
            {"periodsec", std::to_string(dataPeriodSec)},
            {"trafficload", trafficLoad},
            {"trafficmode", trafficMode},
            {"placement", nodePlacementMode},
            {"spacing", std::to_string(pueyoGridSpacingM)},
            {"pktsperpair", std::to_string(pueyoPacketsPerPair)},
            {"duty", cfg.enableDutyCycle ? std::to_string(cfg.dutyLimit) : std::string("off")},
            {"hyst", routeSwitchHysteresis ? "1" : "0"},
            {"hystdelta", std::to_string(routeSwitchMinDeltaX100)},
            {"socmin", std::to_string(socInitMin)},
            {"socmax", std::to_string(socInitMax)},
            {"socbimodal", socInitBimodal ? "1" : "0"},
            {"wire", wireFormat},
            {"mac", cfg.enableCsma ? "csmacad" : "aloha"},
            {"flooding", floodingMode ? "1" : "0"},
            // Añadidos el 2026-08-07. El DoE §10 promete que cualquier figura se
            // puede rastrear hasta la configuracion que la produjo, y estos
            // cuatro NO estaban: son justo los que hemos barrido en E20-E26 y
            // los que causaron tres de los once defectos. Sin ellos aqui, la
            // unica prueba de que una campaña NO llevaba facturacion plana era
            // leer su runner -- que es exactamente la clase de verificacion
            // circular que nos ha costado ya un retracto.
            {"sfmargin", std::to_string(sfLinkMarginDb)},
            {"interfmodel", interferenceModel},
            {"dvpayloadmax", std::to_string(dvPayloadMaxBytes)},
            {"flatbeaconbytes", std::to_string(pueyoFlatBeaconBytes)},
            {"beaconwarm", std::to_string(beaconIntervalWarmSec)},
            {"beaconstable", std::to_string(beaconIntervalStableSec)},
        };
        for (std::size_t i = 0; i < efectiva.size(); ++i)
        {
            cfgFile << (i ? "," : "") << efectiva[i].first;
        }
        cfgFile << std::endl;
        for (std::size_t i = 0; i < efectiva.size(); ++i)
        {
            cfgFile << (i ? "," : "") << efectiva[i].second;
        }
        cfgFile << std::endl;
    }
    runMeta.txPowerDbm = txPowerDbm;
    runMeta.profile = profileLower;
    runMeta.preambleSymbols = preambleSymbols;
    runMeta.packetsPerPair = (trafficMode == "pueyo_all_to_all") ? pueyoPacketsPerPair : 0;
    runMeta.routeAdvertPolicy = routeAdvertPolicy;
    runMeta.routeMetricMode = routeMetricMode;
    runMeta.costEncoding = costEncoding;
    runMeta.sfLinkMode = sfLinkMode;
    runMeta.sfLinkMarginDb = sfLinkMarginDb;
    runMeta.compositeWToa = compositeWToa;
    runMeta.compositeWHop = compositeWHop;
    runMeta.compositeWEnergy = compositeWEnergy;
    runMeta.compositeCostStep = compositeCostStep;
    runMeta.energyLo = energyLo;
    runMeta.energyHi = energyHi;
    runMeta.energyPow = energyPow;
    runMeta.energyMaxPenalty = energyMaxPenalty;
    runMeta.maxRoutesPerDestination = maxRoutesPerDestination;
    runMeta.maxTotalRoutes = maxTotalRoutes;
    runMeta.dvPayloadMaxBytes = dvPayloadMaxBytes;
    runMeta.beaconLatestOnly = beaconLatestOnly;
    runMeta.prioritizeBeacons = prioritizeBeacons;
    runMeta.pueyoStrictQueueScheduler = pueyoStrictQueueScheduler;
    runMeta.controlBackoffFactor = controlBackoffFactor;
    runMeta.dataBackoffFactor = dataBackoffFactor;
    runMeta.sfScanEdThresholdDbm = sfScanEdThresholdDbm;
    runMeta.sfScanResetOnNewSignal = sfScanResetOnNewSignal;
    runMeta.enableSfScanRx = enableSfScanRx;
    runMeta.pueyoFloraLikeRx = pueyoFloraLikeRx;
    // §energyfw-override: aplicar override antes de propagar a runMeta y al simulador
    if (allowEnergyFwOverride)
    {
        enableNs3EnergyFramework = cliEnergyFw;
    }
    runMeta.enableNs3EnergyFramework = enableNs3EnergyFramework;
    runMeta.batteryFullCapacityJ = batteryFullCapacityJ;
    runMeta.shadowingSigmaDb = shadowingSigmaDb;
    runMeta.channelCount = 1;
    runMeta.receptionPaths = 1;
    g_metricsCollector->SetRunConfigMetadata(runMeta);
    if (enableMetricsPeriodicFlush)
    {
        NS_ABORT_MSG_IF(
            metricsFlushIntervalSec <= 0.0,
            "Error: metricsFlushIntervalSec debe ser >0 si enableMetricsPeriodicFlush=true");
        g_metricsCollector->StartPeriodicFlush(Seconds(metricsFlushIntervalSec), "mesh_dv_metrics");
    }

    RngSeedManager::SetRun(rngRun);

    NodeContainer nodes;
    nodes.Create(cfg.nEd);

    cfg.nodePlacementMode = nodePlacementMode;
    cfg.areaWidth = resolvedAreaWidthM;
    cfg.areaHeight = resolvedAreaHeightM;
    cfg.gridSpacingX = pueyoGridSpacingM;
    cfg.gridSpacingY = pueyoGridSpacingM;
    cfg.gridSide = resolvedGridSide;
    cfg.pathLossExponent = pathLossExponent;
    cfg.referenceDistance = referenceDistance;
    cfg.referenceLossDb = referenceLossDb;
    cfg.shadowingSigmaDb = shadowingSigmaDb;
    cfg.shadowingModel = shadowingModel;
    cfg.initTtl = static_cast<uint8_t>(std::min<uint32_t>(initTtl, 63));

    Ptr<DvClHelper> helper = CreateObject<DvClHelper>();
    helper->SetConfig(cfg);

    Config::SetDefaultFailSafe("ns3::dvcl::DvClCsmaCadMac::MinBackoffSlots",
                               UintegerValue(minBackoffSlots));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClCsmaCadMac::BackoffStep",
                               UintegerValue(backoffStep));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClCsmaCadMac::DutyCycleLimit",
                               DoubleValue(cfg.dutyLimit));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClCsmaCadMac::DutyCycleWindow",
                               TimeValue(Seconds(dutyWindowSec)));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClCsmaCadMac::DutyCycleEnabled",
                               BooleanValue(cfg.enableDutyCycle));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClCsmaCadMac::CadSenseMarginDb",
                               DoubleValue(cadSenseMarginDb));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::CsmaMaxRetries", UintegerValue(csmaMaxRetries));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::CsmaTxQueueMax", UintegerValue(csmaTxQueueMax));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataStartTimeSec", DoubleValue(dataStartSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataStopTimeSec", DoubleValue(dataStopSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::TrafficLoad", StringValue(trafficLoad));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::TrafficMode", StringValue(trafficMode));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::OnlyGenerateFromNodeId",
                               IntegerValue(onlyGenerateFromNodeId));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::ForcedDataDestinationId",
                               IntegerValue(forcedDataDestinationId));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::PueyoPacketsPerPair",
                               UintegerValue(pueyoPacketsPerPair));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataPayloadSizeBytes",
                               UintegerValue(dataPayloadSizeBytes));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::EnableDataRandomDest",
                               BooleanValue(enableDataRandomDest));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::BeaconIntervalWarm",
                               TimeValue(Seconds(beaconIntervalWarmSec)));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::BeaconIntervalStable",
                               TimeValue(Seconds(beaconIntervalStableSec)));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::PurePueyoBaselineMode",
                               BooleanValue(profileLower == "pueyo2024" ||
                                            profileLower == "pueyo2024_paper_like" ||
                                            profileLower == "pueyo2024_paper_like_csmacad"));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::PueyoValidationTrace",
                               BooleanValue(pueyoValidationTrace));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::EnableGapAuditTrace",
                               BooleanValue(enableGapAuditTrace));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::PueyoSyntheticEntriesNodeId",
                               IntegerValue(pueyoSyntheticEntriesNodeId));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::PueyoSyntheticEntries",
                               StringValue(pueyoSyntheticEntries));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DvBeaconMaxRoutes",
                               UintegerValue(dvBeaconMaxRoutes));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DvPayloadMaxBytes",
                               UintegerValue(dvPayloadMaxBytes));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::RouteAdvertPolicy",
                               StringValue(routeAdvertPolicy));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::MacCacheWindow",
                               TimeValue(Seconds(linkAddrCacheWindowSec)));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::LinkAddrCacheWindow",
                               TimeValue(Seconds(linkAddrCacheWindowSec)));
    double neighborLinkTimeoutFactor = 1.0;
    if (neighborLinkTimeoutSec > 0.0)
    {
        neighborLinkTimeoutFactor =
            std::max(0.1, neighborLinkTimeoutSec / std::max(0.001, beaconIntervalStableSec));
    }
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::AutoTimeoutsFromBeacon", BooleanValue(true));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::NeighborLinkTimeoutFactor",
                               DoubleValue(neighborLinkTimeoutFactor));
    if (neighborLinkTimeoutSec > 0.0)
    {
        Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::NeighborLinkTimeout",
                                   TimeValue(Seconds(neighborLinkTimeoutSec)));
    }
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::AllowStaleMacForUnicastData",
                               BooleanValue(allowStaleLinkAddrForUnicastData));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::AllowStaleLinkAddrForUnicastData",
                               BooleanValue(allowStaleLinkAddrForUnicastData));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::EmpiricalSfMinSamples",
                               UintegerValue(empiricalSfMinSamples));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::EmpiricalSfSelectMode",
                               StringValue(empiricalSfSelectMode));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::UseProbabilisticSfForBeacons",
                               BooleanValue(useProbabilisticSfForBeacons));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::SfControl", UintegerValue(sfControl));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::SfMin", UintegerValue(sfMin));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::SfMax", UintegerValue(sfMax));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::PreambleSymbols",
                               UintegerValue(preambleSymbols));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::RouteSwitchMinDeltaX100",
                               UintegerValue(routeSwitchMinDeltaX100));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::AvoidImmediateBacktrack",
                               BooleanValue(avoidImmediateBacktrack));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataPeriodJitterMax",
                               DoubleValue(dataPeriodJitterMaxSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataPeriodSec",
                               DoubleValue(dataPeriodSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::RouteSwitchHysteresis",
                               BooleanValue(routeSwitchHysteresis));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataPeriodJitterSymmetric",
                               BooleanValue(dataPeriodJitterSymmetric));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataStartPhaseMaxSec",
                               DoubleValue(dataStartPhaseMaxSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::EnableDataSlots",
                               BooleanValue(enableDataSlots));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataSlotPeriodSec",
                               DoubleValue(dataSlotPeriodSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataSlotJitterSec",
                               DoubleValue(dataSlotJitterSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataStartPhaseOnly",
                               BooleanValue(dataStartPhaseOnly));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataFixedPhaseCadence",
                               BooleanValue(dataFixedPhaseCadence));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::ExtraDvBeaconMaxPerWindow",
                               UintegerValue(extraDvBeaconMaxPerWindow));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::ExtraDvBeaconMinGap",
                               TimeValue(Seconds(extraDvBeaconMinGapSec)));
    Config::SetDefaultFailSafe("ns3::LoraInterferenceHelper::EnableProbabilisticCapture",
                               BooleanValue(enableProbabilisticCapture));
    Config::SetDefaultFailSafe("ns3::LoraInterferenceHelper::CaptureSlope",
                               DoubleValue(captureSlope));
    Config::SetDefaultFailSafe("ns3::LoraInterferenceHelper::CaptureMinProb",
                               DoubleValue(captureMinProb));
    Config::SetDefaultFailSafe("ns3::LoraInterferenceHelper::CaptureMaxProb",
                               DoubleValue(captureMaxProb));
    Config::SetDefaultFailSafe("ns3::LoraInterferenceHelper::InterferenceModel",
                               EnumValue(interferenceModelEnum));
    Config::SetDefaultFailSafe("ns3::LoraInterferenceHelper::PuelloCaptureThresholdDb",
                               DoubleValue(puelloCaptureThresholdDb));
    Config::SetDefaultFailSafe("ns3::LoraInterferenceHelper::PuelloAssumedBandwidthHz",
                               DoubleValue(puelloAssumedBandwidthHz));
    Config::SetDefaultFailSafe("ns3::LoraInterferenceHelper::PuelloPreambleSymbols",
                               DoubleValue(puelloPreambleSymbols));
    Config::SetDefaultFailSafe("ns3::SimpleGatewayLoraPhy::SfScanEdThresholdDbm",
                               DoubleValue(sfScanEdThresholdDbm));
    Config::SetDefaultFailSafe("ns3::SimpleGatewayLoraPhy::SfScanResetOnNewSignal",
                               BooleanValue(sfScanResetOnNewSignal));
    Config::SetDefaultFailSafe("ns3::SimpleGatewayLoraPhy::EnableSfScanRx",
                               BooleanValue(enableSfScanRx));
    lorawan::LoraInterferenceHelper::SetProbabilisticCaptureDefaults(enableProbabilisticCapture,
                                                                     captureSlope,
                                                                     captureMinProb,
                                                                     captureMaxProb);
    lorawan::LoraInterferenceHelper::SetInterferenceModelDefaults(interferenceModelEnum,
                                                                  puelloCaptureThresholdDb,
                                                                  puelloAssumedBandwidthHz,
                                                                  puelloPreambleSymbols);
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::PrioritizeBeacons",
                               BooleanValue(prioritizeBeacons));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::BeaconLatestOnly",
                               BooleanValue(beaconLatestOnly));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::PueyoStrictQueueScheduler",
                               BooleanValue(pueyoStrictQueueScheduler));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::ControlBackoffFactor",
                               DoubleValue(controlBackoffFactor));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DataBackoffFactor",
                               DoubleValue(dataBackoffFactor));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::EnableControlGuard",
                               BooleanValue(enableControlGuard));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::ControlGuardSec", DoubleValue(controlGuardSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::StudyForwardSpreadEnable",
                               BooleanValue(studyForwardSpreadEnable));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::StudyForwardBaseDelayMs",
                               DoubleValue(studyForwardBaseDelayMs));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::StudyForwardSpreadMs",
                               DoubleValue(studyForwardSpreadMs));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::StudySuperframeEnable",
                               BooleanValue(studySuperframeEnable));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::StudySuperframePeriodSec",
                               DoubleValue(studySuperframePeriodSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::StudySuperframeCtrlWindowSec",
                               DoubleValue(studySuperframeCtrlWindowSec));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DisableExtraAfterWarmup",
                               BooleanValue(disableExtraAfterWarmup));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::BatteryFullCapacityJ",
                               DoubleValue(batteryFullCapacityJ));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::RouteTimeoutFactor",
                               DoubleValue(routeTimeoutFactor));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::DedupWindowSec",
                               TimeValue(Seconds(dedupWindowSec)));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::LinkWeight", DoubleValue(dvLinkWeight));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::PathWeight", DoubleValue(dvPathWeight));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::PathHopWeight",
                               DoubleValue(dvPathHopWeight));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::MetricMode", StringValue(routeMetricMode));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::CostEncoding", StringValue(costEncoding));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::CompositeWToa", DoubleValue(compositeWToa));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::CompositeWHop", DoubleValue(compositeWHop));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::CompositeWEnergy",
                               DoubleValue(compositeWEnergy));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::CompositeCostStep",
                               DoubleValue(compositeCostStep));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::EnergyLo", DoubleValue(energyLo));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::EnergyHi", DoubleValue(energyHi));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::EnergyPow", DoubleValue(energyPow));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::EnergyMaxPenalty",
                               DoubleValue(energyMaxPenalty));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::SfLinkMode", StringValue(sfLinkMode));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::SfLinkMarginDb", DoubleValue(sfLinkMarginDb));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClApp::PueyoFlatBeaconBytes",
                               UintegerValue(pueyoFlatBeaconBytes));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::MaxRoutesPerDestination",
                               UintegerValue(maxRoutesPerDestination));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::MaxTotalRoutes",
                               UintegerValue(maxTotalRoutes));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::PueyoValidationTrace",
                               BooleanValue(pueyoValidationTrace));
    // §BatBeacon runtime attributes
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::UseBeaconSoC",
                               BooleanValue(useBeaconBattery));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::SoCHysteresisPercent",
                               UintegerValue(socHysteresisPercent));
    // §DC-aware runtime attributes
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::UseDcAwareRouting",
                               BooleanValue(useDcAwareRouting));
    Config::SetDefaultFailSafe("ns3::dvcl::DvClRouting::DcFeasibilityThreshold",
                               UintegerValue(dcFeasibilityThreshold));
    // Debe definirse antes de Install(), porque SetDefault sólo aplica a objetos futuros.
    bool txPowerApplied = Config::SetDefaultFailSafe("ns3::dvcl::DvClLoraNetDevice::TxPowerDbm",
                                                     DoubleValue(txPowerDbm));
    bool preambleApplied =
        Config::SetDefaultFailSafe("ns3::dvcl::DvClLoraNetDevice::PreambleSymbols",
                                   UintegerValue(preambleSymbols));
    if (!txPowerApplied)
    {
        NS_LOG_WARN("No se pudo aplicar Config::SetDefault para DvClLoraNetDevice::TxPowerDbm; se "
                    "usarán los valores por defecto del dispositivo");
    }
    if (!preambleApplied)
    {
        NS_LOG_WARN(
            "No se pudo aplicar Config::SetDefault para DvClLoraNetDevice::PreambleSymbols");
    }
    NS_LOG_INFO("DataStartTimeSec=" << dataStartSec << "s");
    NS_LOG_INFO("DataStopTimeSec=" << dataStopSec << "s");
    NS_LOG_INFO(
        "Profile=" << profileLower << " TrafficMode=" << trafficMode
                   << " TrafficLoad=" << trafficLoad
                   << " EnableDataRandomDest=" << (enableDataRandomDest ? "true" : "false")
                   << " PueyoPacketsPerPair=" << pueyoPacketsPerPair << " DataPayloadBytes="
                   << dataPayloadSizeBytes << " BeaconWarm=" << beaconIntervalWarmSec << "s"
                   << " BeaconStable=" << beaconIntervalStableSec << "s"
                   << " DvBeaconMaxRoutes=" << dvBeaconMaxRoutes << " DvPayloadMaxBytes="
                   << dvPayloadMaxBytes << " RouteAdvertPolicy=" << routeAdvertPolicy
                   << " LinkAddrCacheWindow=" << linkAddrCacheWindowSec << "s"
                   << " NeighborLinkTimeoutSec="
                   << ((neighborLinkTimeoutSec > 0.0) ? std::to_string(neighborLinkTimeoutSec)
                                                      : std::string("auto"))
                   << " NeighborLinkTimeoutFactor=" << neighborLinkTimeoutFactor
                   << " AllowStaleLinkAddr="
                   << (allowStaleLinkAddrForUnicastData ? "true" : "false") << " EmpiricalSfMode="
                   << empiricalSfSelectMode << " EmpiricalSfMinSamples=" << empiricalSfMinSamples
                   << " RouteSwitchMinDelta=" << routeSwitchMinDeltaX100
                   << " AvoidBacktrack=" << (avoidImmediateBacktrack ? "true" : "false")
                   << " PdrEndWindow=" << pdrEndWindowSec << "s"
                   << " DedupWindow=" << dedupWindowSec << "s"
                   << " WireFormat=" << wireFormat << " RouteMetricMode=" << routeMetricMode
                   << " CostEncoding=" << costEncoding << " CompositeWToa=" << compositeWToa
                   << " CompositeWHop=" << compositeWHop << " CompositeWEnergy=" << compositeWEnergy
                   << " CompositeCostStep=" << compositeCostStep
                   << " DataJitterMax=" << dataPeriodJitterMaxSec << "s"
                   << " DataJitterSymmetric=" << (dataPeriodJitterSymmetric ? "on" : "off")
                   << " DataStartPhaseMax=" << dataStartPhaseMaxSec << "s"
                   << " DataSlots=" << (enableDataSlots ? "on" : "off")
                   << " SlotPeriod=" << dataSlotPeriodSec << "s"
                   << " SlotJitter=" << dataSlotJitterSec << "s"
                   << " ExtraDvMax=" << extraDvBeaconMaxPerWindow
                   << " ExtraDvMinGap=" << extraDvBeaconMinGapSec << "s"
                   << " DvWeights(legacy/no-op)=" << dvLinkWeight << "," << dvPathWeight << ","
                   << dvPathHopWeight << " InterferenceModel=" << interferenceModel
                   << " Puello(threshold=" << puelloCaptureThresholdDb << "dB,bw="
                   << puelloAssumedBandwidthHz << "Hz,preamble=" << puelloPreambleSymbols << ")"
                   << " BeaconLatestOnly=" << (beaconLatestOnly ? "true" : "false"));
    if (enablePcap)
    {
        helper->EnablePcap("mesh_dv_node");
    }
    helper->Install(nodes);
    for (uint32_t wi = 0; wi < nodes.GetN(); ++wi)
    {
        for (uint32_t wa = 0; wa < nodes.Get(wi)->GetNApplications(); ++wa)
        {
            if (auto wapp = DynamicCast<DvClApp>(nodes.Get(wi)->GetApplication(wa)))
            {
                wapp->SetStatsSink(s_collectorKeepAlive);
            }
        }
    }

    // §DoE E6: inundacion gestionada como plano de datos (linea de referencia).
    if (floodingMode)
    {
        uint32_t configured = 0;
        for (uint32_t i = 0; i < nodes.GetN(); ++i)
        {
            for (uint32_t a = 0; a < nodes.Get(i)->GetNApplications(); ++a)
            {
                if (auto capp = DynamicCast<DvClApp>(nodes.Get(i)->GetApplication(a)))
                {
                    capp->SetAttribute("FloodingMode", BooleanValue(true));
                    capp->SetAttribute("FloodJitterMs", UintegerValue(floodJitterMs));
                    ++configured;
                }
            }
        }
        NS_LOG_INFO("Flooding habilitado en " << configured << " nodos (jitter " << floodJitterMs
                                              << " ms)");
    }

    // ========================================================================
    // ENERGY: seed the heterogeneous initial state of charge.
    //
    // The module ships a single energy model (DvClLoraEnergyModel), created and
    // owned per node by the application, which charges itself event by event
    // and answers the SoC the composite metric reads. There is nothing to
    // install here -- only the initial charge to seed. `enableNs3EnergyFramework`
    // is kept as the gate the profiles already set for the lifetime KPIs, but it
    // no longer installs a second, parallel energy model: that model's depletion
    // notification was wired to nothing and it double-booked the battery
    // (VALIDATION.md, 2026-07-22).
    if (enableNs3EnergyFramework)
    {
        Ptr<UniformRandomVariable> socRng = CreateObject<UniformRandomVariable>();
        // [B4] Stream determinista singleton para SOC inicial reproducible al variar N.
        socRng->SetStream(10);
        socRng->SetAttribute("Min", DoubleValue(socInitMin)); // §SoCInit override (default 0.60)
        socRng->SetAttribute("Max", DoubleValue(socInitMax)); // §SoCInit override (default 1.00)

        for (uint32_t i = 0; i < nodes.GetN(); ++i)
        {
            // Bimodal: nodos pares al minimo, impares al maximo. Determinista a
            // proposito, para que el contraste no dependa de la semilla.
            const double initialSoc = socInitBimodal ? ((i % 2 == 0) ? socInitMin : socInitMax)
                                                      : socRng->GetValue();
            for (uint32_t a = 0; a < nodes.Get(i)->GetNApplications(); ++a)
            {
                if (auto capp = DynamicCast<DvClApp>(nodes.Get(i)->GetApplication(a)))
                {
                    capp->SetAttribute("InitialSocFraction", DoubleValue(initialSoc));
                }
            }
            NS_LOG_INFO("Node " << i << " initial SOC: " << std::fixed << std::setprecision(1)
                                << (initialSoc * 100) << "%");
        }
    }
    else
    {
        NS_LOG_INFO("Heterogeneous SoC seeding disabled; every node starts full.");
    }

    // Ajustar inicio/fin de las apps de datos: DV sigue activo desde t=0
    for (uint32_t i = 0; i < nodes.GetN(); ++i)
    {
        Ptr<Node> n = nodes.Get(i);
        for (uint32_t a = 0; a < n->GetNApplications(); ++a)
        {
            Ptr<DvClApp> app = DynamicCast<DvClApp>(n->GetApplication(a));
            if (app)
            {
                Time jitter = Seconds(static_cast<double>(i) * 0.2);
                app->SetStartTime(jitter);
                app->SetStopTime(Seconds(cfg.simTimeSec - 0.1));
            }
        }
    }

    // ======================================================================
    // multisink: K sinks chosen explicitly (--sinkNodeIds=a,b,..) or auto-placed
    // (--numSinks=K, K spread targets over the node bounding box). Each non-sink
    // node is assigned to its NEAREST sink by actual node position
    // (MobilityModel) => works for both pueyo_grid and random. Empty/0 =>
    // single-sink default (bit-for-bit with frozen, regression-verified). A sink
    // sends to itself => generates no data. Routing/forwarding unchanged.
    // ======================================================================
    {
        const uint32_t M = nodes.GetN();
        std::vector<double> px(M, 0.0), py(M, 0.0);
        for (uint32_t i = 0; i < M; ++i)
        {
            Ptr<MobilityModel> mm = nodes.Get(i)->GetObject<MobilityModel>();
            if (mm)
            {
                Vector p = mm->GetPosition();
                px[i] = p.x;
                py[i] = p.y;
            }
        }
        std::vector<uint32_t> sinks;
        if (!sinkNodeIdsCsv.empty())
        {
            std::string cur;
            for (char ch : sinkNodeIdsCsv)
            {
                if (ch == ',')
                {
                    if (!cur.empty())
                    {
                        sinks.push_back(static_cast<uint32_t>(std::stoul(cur)));
                        cur.clear();
                    }
                }
                else if (ch != ' ')
                {
                    cur += ch;
                }
            }
            if (!cur.empty())
            {
                sinks.push_back(static_cast<uint32_t>(std::stoul(cur)));
            }
        }
        else if (numSinks > 0 && M > 0)
        {
            double minx = px[0], maxx = px[0], miny = py[0], maxy = py[0];
            for (uint32_t i = 1; i < M; ++i)
            {
                if (px[i] < minx)
                {
                    minx = px[i];
                }
                if (px[i] > maxx)
                {
                    maxx = px[i];
                }
                if (py[i] < miny)
                {
                    miny = py[i];
                }
                if (py[i] > maxy)
                {
                    maxy = py[i];
                }
            }
            std::vector<double> tx, ty;
            if (numSinks == 1)
            {
                tx.push_back((minx + maxx) / 2);
                ty.push_back((miny + maxy) / 2);
            }
            else if (numSinks == 2)
            {
                tx.push_back(minx + (maxx - minx) * 0.25);
                ty.push_back((miny + maxy) / 2);
                tx.push_back(minx + (maxx - minx) * 0.75);
                ty.push_back((miny + maxy) / 2);
            }
            else
            {
                const double fx[2] = {0.25, 0.75};
                const double fy[2] = {0.25, 0.75};
                for (int a = 0; a < 2; ++a)
                {
                    for (int b = 0; b < 2; ++b)
                    {
                        tx.push_back(minx + (maxx - minx) * fx[a]);
                        ty.push_back(miny + (maxy - miny) * fy[b]);
                    }
                }
            }
            for (std::size_t t = 0; t < tx.size(); ++t)
            {
                long best = -1;
                double bestd = 1e36;
                for (uint32_t i = 0; i < M; ++i)
                {
                    bool taken = false;
                    for (uint32_t s : sinks)
                    {
                        if (s == i)
                        {
                            taken = true;
                            break;
                        }
                    }
                    if (taken)
                    {
                        continue;
                    }
                    const double dx = px[i] - tx[t], dy = py[i] - ty[t], d = dx * dx + dy * dy;
                    if (d < bestd)
                    {
                        bestd = d;
                        best = static_cast<long>(i);
                    }
                }
                if (best >= 0)
                {
                    sinks.push_back(static_cast<uint32_t>(best));
                }
            }
        }
        if (!sinks.empty())
        {
            std::string sl;
            for (uint32_t i = 0; i < M; ++i)
            {
                bool iIsSink = false;
                for (uint32_t s : sinks)
                {
                    if (s == i)
                    {
                        iIsSink = true;
                        break;
                    }
                }
                uint32_t dst = i; // sink: send to self => no data generation
                if (!iIsSink)
                {
                    double bestd = 1e36;
                    uint32_t best = sinks[0];
                    for (uint32_t s : sinks)
                    {
                        const double dx = px[i] - px[s], dy = py[i] - py[s], d = dx * dx + dy * dy;
                        if (d < bestd || (d == bestd && s < best))
                        {
                            bestd = d;
                            best = s;
                        }
                    }
                    dst = best;
                }
                Ptr<Node> n = nodes.Get(i);
                for (uint32_t a = 0; a < n->GetNApplications(); ++a)
                {
                    Ptr<DvClApp> app = DynamicCast<DvClApp>(n->GetApplication(a));
                    if (app)
                    {
                        app->SetAttribute("ForcedDataDestinationId",
                                          IntegerValue(static_cast<int32_t>(dst)));
                        app->SetCollectorNodeId(dst);
                    }
                }
                NS_LOG_WARN("multisink node=" << i << " -> sink=" << dst
                                              << (iIsSink ? " (SINK)" : ""));
            }
            for (uint32_t s : sinks)
            {
                sl += std::to_string(s) + " ";
            }
            NS_LOG_WARN("multisink: " << sinks.size() << " sinks (" << sl
                                      << ") placement=" << nodePlacementMode);
        }
    }

    NS_LOG_INFO("Starting simulation for " << cfg.simTimeSec << " seconds...");

    // ========================================================================
    // THESIS METRICS T50/FND: Configure MetricsCollector
    // ========================================================================
    if (g_metricsCollector)
    {
        g_metricsCollector->SetTotalNodes(nodes.GetN());
        // Collector node: aligned with DvClApp's m_collectorNodeId
        // In LoRaMESH all nodes are equal peers (no gateway).
        // SimpleGatewayLoraPhy is used only for multi-SF RX capability.
        // The collector is just the data sink for metric computation.
        uint32_t sinkNodeId = (nodes.GetN() > 0) ? (nodes.GetN() - 1) : 0; // Data collector node
        g_metricsCollector->SetSinkNodeId(sinkNodeId);
        NS_LOG_INFO("T50/FND metrics configured: totalNodes=" << nodes.GetN()
                                                              << " sinkNodeId=" << sinkNodeId);
    }

    Simulator::Stop(Seconds(cfg.simTimeSec));

    Simulator::Run();
    NS_LOG_INFO("=== Simulación completada ===");

    // Desglose del gasto por actividad, leído del registro -- el libro que
    // gobierna el SoC anunciado, la métrica y la muerte del nodo. Sin este
    // desglose no se puede saber qué fracción del consumo es transmisión
    // propia frente a escucha, y por tanto qué fracción puede siquiera
    // redistribuir una decisión de encaminamiento.
    {
        std::ofstream st("mesh_dv_metrics_energy_breakdown.csv");
        st << "nodeId,txMah,rxMah,cadMah,idleMah\n";
        for (uint32_t i = 0; i < nodes.GetN(); ++i)
        {
            Ptr<Node> n = nodes.Get(i);
            const uint32_t id = n->GetId();
            for (uint32_t d = 0; d < n->GetNDevices(); ++d)
            {
                auto dev = DynamicCast<DvClLoraNetDevice>(n->GetDevice(d));
                if (!dev || !dev->GetEnergyModel())
                {
                    continue;
                }
                auto em = dev->GetEnergyModel();
                st << id << "," << em->GetTxMah() << "," << em->GetRxMah() << ","
                   << em->GetCadMah() << "," << em->GetIdleMah() << "\n";
                break;
            }
        }
    }
    // §LossFine: flush final per-app stats even if the sim stopped via the death hook
    for (uint32_t li = 0; li < nodes.GetN(); ++li)
    {
        Ptr<Node> ln = nodes.Get(li);
        for (uint32_t la = 0; la < ln->GetNApplications(); ++la)
        {
            Ptr<DvClApp> lapp = DynamicCast<DvClApp>(ln->GetApplication(la));
            if (lapp)
            {
                lapp->ForceFinalFlush();
            }
        }
    }

    // ========================================================================
    // EXPORTAR MÉTRICAS
    // ========================================================================
    if (g_metricsCollector)
    {
        if (enableMetricsPeriodicFlush)
        {
            g_metricsCollector->StopPeriodicFlush();
        }
        std::cerr << "\n>>> Exportando métricas...\n";
        if (!enableMetricsPeriodicFlush)
        {
            g_metricsCollector->PrintStatistics();
        }
        g_metricsCollector->ExportToCSV("mesh_dv_metrics");
        g_metricsCollector->ExportToJson("mesh_dv_summary");
        std::cerr << ">>> Archivos CSV exportados:\n";
        std::cerr << "    - mesh_dv_metrics_tx.csv\n";
        std::cerr << "    - mesh_dv_metrics_rx.csv\n";
        std::cerr << "    - mesh_dv_metrics_routes.csv\n";
        // The collector is an ns-3 Object owned by s_collectorKeepAlive (Ptr);
        // clear the raw alias without deleting (the Ptr frees it).
        g_metricsCollector = nullptr;
    }

    // Close the unified PCAP file
    DvClLoraNetDevice::CloseGlobalPcap();

    Simulator::Destroy();
    return 0;
}
