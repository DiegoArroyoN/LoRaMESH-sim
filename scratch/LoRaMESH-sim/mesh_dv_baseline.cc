#include "lora-device-energy-model-helper.h"
#include "lora-device-energy-model.h"
#include "mesh_dv_app.h"
#include "mesh_lora_net_device.h"
#include "metrics_collector.h"

#include "ns3/core-module.h"
#include "ns3/energy-module.h"
#include "ns3/lora-interference-helper.h"
#include "ns3/loramesh-helper.h"
#include "ns3/network-module.h"
#include "ns3/rng-seed-manager.h"

#include <cctype>
#include <cmath>
#include <cstdio>

using namespace ns3;

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

    loramesh::LoraMeshConfig cfg;
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
    double dataStopSec = -1.0;
    std::string trafficLoad = "medium";
    bool enableDataRandomDest = true; // Always random destinations in mesh
    int32_t onlyGenerateFromNodeId = -1;
    int32_t forcedDataDestinationId = -1;
    bool verboseLogs = true;
    uint32_t minBackoffSlots = 4;
    uint32_t backoffStep = 2;
    double beaconIntervalWarmSec = 10.0;
    double beaconIntervalStableSec = 90.0;
    std::string profile =
        "pueyo2024_paper_like"; // default operativo; extended queda como legacy
    std::string wireFormat = "pueyo7b"; // default operativo; v2 queda como legacy
    std::string trafficMode = "periodic_any_to_any"; // periodic_any_to_any | pueyo_all_to_all
    uint32_t pueyoPacketsPerPair = 100;
    uint32_t dataPayloadSizeBytes = 20;
    uint32_t dvBeaconMaxRoutes = 0;
    uint32_t dvPayloadMaxBytes = 0; // 0: MTU-derived
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
    bool avoidImmediateBacktrack = true;
    double dataPeriodJitterMaxSec = 3.0;
    bool enableDataSlots = false;
    double dataSlotPeriodSec = 0.0;
    double dataSlotJitterSec = 0.0;
    uint32_t extraDvBeaconMaxPerWindow = 1;
    double extraDvBeaconMinGapSec = 0.5;
    bool enableProbabilisticCapture = true; // Croce et al.: cross-SF quasi-orthogonality
    double captureSlope = 0.7;
    double captureMinProb = 0.05;
    double captureMaxProb = 0.95;
    std::string interferenceModel = "puello"; // goursaud | puello | pueyo | pueyo_fixed_capture
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
    bool disableExtraAfterWarmup = true;
    double batteryFullCapacityJ = 38880.0; // 10.8 Wh Li-Ion 18650 nominal full capacity
    double routeTimeoutFactor = 6.0;
    double pdrEndWindowSec = 0.0;
    double dedupWindowSec = 600.0;
    double dvLinkWeight = 0.70;
    double dvPathWeight = 0.25;
    double dvPathHopWeight = 0.05;
    std::string routeMetricMode = "composite_score"; // composite_score | toa_only
    std::string costEncoding = "cost255";           // score100 | cost255 | score255
    std::string sfLinkMode = "observed_rxsf";        // observed_rxsf | deterministic_sensitivity
    double sfLinkMarginDb = 0.0;
    double compositeWToa = 1.0;
    double compositeWHop = 1.0;
    double compositeWEnergy = 2.0;
    double compositeCostStep = 1.0;
    double energyLo = 0.20;
    double energyHi = 0.50;
    double energyPow = 3.0;
    double energyMaxPenalty = 50.0;
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
    bool enableSfScanRx = true;
    bool pueyoFloraLikeRx = false;
    bool enableNs3EnergyFramework = false;
    bool enableMetricsPeriodicFlush = false;
    double metricsFlushIntervalSec = 3600.0;
    bool enableMetricsEssentialOnly = false;
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
    cmd.AddValue("dataStopSec",
                 "Stop time for data generation (seconds). -1 disables stop window",
                 dataStopSec);
    cmd.AddValue("trafficLoad", "low/medium/high/saturation", trafficLoad);
    cmd.AddValue("profile",
                 "Execution profile: pueyo2024_paper_like (default) | pueyo2024 | "
                 "proposal_pueyo_like | proposal_pueyo_like_observed | extended (legacy)",
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
    cmd.AddValue("pueyoPacketsPerPair",
                 "Packets generated per source-destination pair in pueyo_all_to_all mode",
                 pueyoPacketsPerPair);
    cmd.AddValue("dataPayloadSizeBytes",
                 "Application payload size [bytes] for generated unicast data packets",
                 dataPayloadSizeBytes);
    cmd.AddValue("verboseLogs",
                 "Enable ns-3 component INFO logs for MeshDvBaseline/MeshDvApp/CsmaCadMac",
                 verboseLogs);
    cmd.AddValue("enableMetricsPeriodicFlush",
                 "Periodically flush detailed metrics CSVs to disk to bound memory usage",
                 enableMetricsPeriodicFlush);
    cmd.AddValue("metricsFlushIntervalSec",
                 "Simulation-time interval in seconds for periodic metrics flush",
                 metricsFlushIntervalSec);
    cmd.AddValue("enableMetricsEssentialOnly",
                 "Keep only essential metrics detail (TX+delay+summary) and skip heavy RX/route/overhead traces",
                 enableMetricsEssentialOnly);
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
                 "Route selection policy when beacon payload truncates entries: top_score | uniform | cost_weighted",
                 routeAdvertPolicy);
    cmd.AddValue("macCacheWindowSec",
                 "Legacy alias for linkAddr cache window for nextHop resolution [s]",
                 linkAddrCacheWindowSec);
    cmd.AddValue("linkAddrCacheWindowSec",
                 "Link-layer address cache window for nextHop resolution [s]",
                 linkAddrCacheWindowSec);
    cmd.AddValue("neighborLinkTimeoutSec",
                 "Empirical per-SF neighbor history validity window [s] (<=0 auto-correlates to beacon interval)",
                 neighborLinkTimeoutSec);
    cmd.AddValue("allowStaleMacForUnicastData",
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
    cmd.AddValue("sfMin",
                 "Minimum SF for probabilistic beacon selection [7..12]",
                 sfMin);
    cmd.AddValue("sfMax",
                 "Maximum SF for probabilistic beacon selection [7..12]",
                 sfMax);
    cmd.AddValue("initTtl",
                 "Initial data TTL (encoded on 6 bits for v2/pueyo7b wire formats)",
                 initTtl);
    cmd.AddValue("routeSwitchMinDeltaX100",
                 "Minimum score delta to switch next-hop in DV updates",
                 routeSwitchMinDeltaX100);
    cmd.AddValue("avoidImmediateBacktrack",
                 "Drop forwarding decisions that immediately backtrack to prevHop",
                 avoidImmediateBacktrack);
    cmd.AddValue("dataPeriodJitterMaxSec", "Max data period jitter [s]", dataPeriodJitterMaxSec);
    cmd.AddValue("enableDataSlots", "Enable local micro-slots for data TX", enableDataSlots);
    cmd.AddValue("dataSlotPeriodSec",
                 "Data slot period [s] (EnableDataSlots=true)",
                 dataSlotPeriodSec);
    cmd.AddValue("dataSlotJitterSec",
                 "Data slot jitter [s] (EnableDataSlots=true)",
                 dataSlotJitterSec);
    cmd.AddValue("extraDvBeaconMaxPerWindow",
                 "Max extra DV beacons per window (0=disable)",
                 extraDvBeaconMaxPerWindow);
    cmd.AddValue("extraDvBeaconMinGapSec",
                 "Min gap between extra DV beacons [s]",
                 extraDvBeaconMinGapSec);
    cmd.AddValue("batteryFullCapacityJ",
                 "Nominal full battery capacity [J] used for SOC tracking",
                 batteryFullCapacityJ);
    cmd.AddValue("enableNs3EnergyFramework",
                 "Enable ns-3 BasicEnergySource + LoRaDeviceEnergyModel battery depletion framework",
                 enableNs3EnergyFramework);
    cmd.AddValue("routeTimeoutFactor",
                 "Route timeout multiplier based on beacon interval",
                 routeTimeoutFactor);
    cmd.AddValue("pdrEndWindowSec",
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
                 "Routing metric mode: composite_score | toa_only",
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
    cmd.AddValue("sfLinkMarginDb",
                 "Margin [dB] added to sensitivity threshold when sfLinkMode=deterministic_sensitivity",
                 sfLinkMarginDb);
    cmd.AddValue("maxRoutesPerDestination",
                 "Max route candidates stored per destination in DV table",
                 maxRoutesPerDestination);
    cmd.AddValue("maxTotalRoutes",
                 "Max total route entries in DV table (primary+backup)",
                 maxTotalRoutes);
    cmd.AddValue("nodePlacementMode", "line | random | pueyo_grid | pueyo_random_equiv", nodePlacementMode);
    cmd.AddValue("areaWidth", "Random placement width [m] (mode=random)", areaWidth);
    cmd.AddValue("areaHeight", "Random placement height [m] (mode=random)", areaHeight);
    cmd.AddValue("pueyoGridSpacingM",
                 "Grid spacing [m] for pueyo_grid / pueyo_random_equiv modes",
                 pueyoGridSpacingM);
    cmd.AddValue("pueyoGridSide",
                 "Grid side for pueyo modes (0=auto from nEd)",
                 pueyoGridSide);
    cmd.AddValue("rngRun", "RNG run number for reproducible placement", rngRun);
    cmd.AddValue("pathLossExponent", "Log-distance path loss exponent", pathLossExponent);
    cmd.AddValue("referenceDistance", "Path loss reference distance [m]", referenceDistance);
    cmd.AddValue("referenceLossDb", "Path loss at reference distance [dB]", referenceLossDb);
    cmd.AddValue("shadowingSigmaDb",
                 "Log-normal shadowing sigma [dB] in propagation model (0 disables shadowing)",
                 shadowingSigmaDb);
    cmd.AddValue("enableSfScanRx",
                 "Enable RX scan/lock pending-signal acquisition in SimpleGatewayLoraPhy",
                 enableSfScanRx);
    cmd.AddValue("pueyoFloraLikeRx",
                 "Minimal FLoRa-like receive-start ablation: disable pending scan/lock while preserving single-channel and single-demod behavior",
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
    cmd.AddValue("interferenceModel",
                 "PHY interference model: goursaud | puello | pueyo | pueyo_fixed_capture",
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
    cmd.AddValue("beaconLatestOnly",
                 "Keep only latest pending beacon in queue",
                 beaconLatestOnly);
    cmd.AddValue("pueyoStrictQueueScheduler",
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
    cmd.AddValue("disableExtraAfterWarmup",
                 "Disable extra DV beacons after warmup period",
                 disableExtraAfterWarmup);
    cmd.Parse(argc, argv);

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
                        profileLower != "proposal_pueyo_like" &&
                        profileLower != "proposal_pueyo_like_observed",
                    "Error: profile debe ser 'pueyo2024_paper_like', 'pueyo2024', "
                    "'proposal_pueyo_like', 'proposal_pueyo_like_observed' o 'extended' "
                    "(legacy), valor actual: "
                        << profile);

    auto applyPueyoComparableBase = [&]() {
        cfg.enableCsma = false;
        cfg.enableDutyCycle = false; // Equivalent to 100% duty availability (no duty gate).
        cfg.dutyLimit = 1.0;
        dutyWindowSec = 3600.0;
        wireFormat = "pueyo7b";
        txPowerDbm = 20.0;
        preambleSymbols = 16;
        puelloPreambleSymbols = static_cast<double>(preambleSymbols);
        interferenceModel = "pueyo_fixed_capture";
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
        controlBackoffFactor = 1.0;
        dataBackoffFactor = 10.0;
        routeAdvertPolicy = "cost_weighted";
        costEncoding = "cost255";
        sfLinkMarginDb = 0.0;
        maxRoutesPerDestination = 2;
        maxTotalRoutes = 1024;
        dvPayloadMaxBytes = 251;
        sfScanEdThresholdDbm = -120.0;
        sfScanResetOnNewSignal = true;
        shadowingSigmaDb = 3.57;
        dataPayloadSizeBytes = 20;
        enableNs3EnergyFramework = false;
    };

    if (profileLower == "pueyo2024")
    {
        applyPueyoComparableBase();
        routeMetricMode = "toa_only";
        sfLinkMode = "deterministic_sensitivity";
        NS_LOG_WARN("Aplicando profile=pueyo2024: defaults estrictos para comparabilidad.");
    }
    else if (profileLower == "pueyo2024_paper_like")
    {
        applyPueyoComparableBase();
        routeMetricMode = "toa_only";
        sfLinkMode = "deterministic_sensitivity";
        sfMin = 7;
        sfMax = 8;
        pueyoFloraLikeRx = true;
        enableSfScanRx = false;
        NS_LOG_WARN("Aplicando profile=pueyo2024_paper_like: base Pueyo + SF7-8 + receive-start FLoRa-like.");
    }
    else if (profileLower == "proposal_pueyo_like")
    {
        applyPueyoComparableBase();
        cfg.enableCsma = true;
        cfg.enableDutyCycle = true;
        cfg.dutyLimit = 0.01;
        dutyWindowSec = 3600.0;
        prioritizeBeacons = true;
        pueyoStrictQueueScheduler = false;
        routeMetricMode = "composite_score";
        sfLinkMode = "deterministic_sensitivity";
        NS_LOG_WARN("Aplicando profile=proposal_pueyo_like: base Pueyo + composite_score + CSMA + duty 1%.");
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
        sfLinkMode = "observed_rxsf";
        NS_LOG_WARN("Aplicando profile=proposal_pueyo_like_observed: base Pueyo + composite_score + CSMA + duty 1% + observed_rxsf.");
    }

    auto validatePueyoComparableBase = [&](const std::string& profileName) {
        NS_ABORT_MSG_IF(wireFormat != "pueyo7b",
                        "Error: profile=" << profileName << " requiere wireFormat=pueyo7b");
        NS_ABORT_MSG_IF(txPowerDbm != 20.0,
                        "Error: profile=" << profileName << " requiere txPowerDbm=20");
        NS_ABORT_MSG_IF(preambleSymbols != 16,
                        "Error: profile=" << profileName << " requiere preambleSymbols=16");
        NS_ABORT_MSG_IF(trafficMode != "pueyo_all_to_all",
                        "Error: profile=" << profileName << " requiere trafficMode=pueyo_all_to_all");
        NS_ABORT_MSG_IF(pueyoPacketsPerPair != 100,
                        "Error: profile=" << profileName << " requiere pueyoPacketsPerPair=100");
        NS_ABORT_MSG_IF(enableDataRandomDest,
                        "Error: profile=" << profileName << " requiere enableDataRandomDest=false");
        NS_ABORT_MSG_IF(beaconIntervalWarmSec != 60.0 || beaconIntervalStableSec != 60.0,
                        "Error: profile=" << profileName << " requiere beaconIntervalWarm/Stable=60s");
        NS_ABORT_MSG_IF(routeTimeoutFactor != 5.0,
                        "Error: profile=" << profileName << " requiere routeTimeoutFactor=5");
        NS_ABORT_MSG_IF(routeAdvertPolicy != "cost_weighted",
                        "Error: profile=" << profileName << " requiere routeAdvertPolicy=cost_weighted");
        NS_ABORT_MSG_IF(dvPayloadMaxBytes != 251,
                        "Error: profile=" << profileName << " requiere dvPayloadMaxBytes=251");
        NS_ABORT_MSG_IF(maxRoutesPerDestination != 2 || maxTotalRoutes != 1024,
                        "Error: profile=" << profileName << " requiere tabla DV 2/1024");
        NS_ABORT_MSG_IF(costEncoding != "cost255",
                        "Error: profile=" << profileName << " requiere costEncoding=cost255");
        NS_ABORT_MSG_IF(interferenceModel != "pueyo_fixed_capture" && interferenceModel != "puello" &&
                            interferenceModel != "pueyo",
                        "Error: profile=" << profileName
                                           << " requiere interferenceModel=pueyo_fixed_capture");
        NS_ABORT_MSG_IF(dataPeriodJitterMaxSec != 0.0,
                        "Error: profile=" << profileName << " requiere dataPeriodJitterMaxSec=0");
        NS_ABORT_MSG_IF(dataPayloadSizeBytes != 20,
                        "Error: profile=" << profileName << " requiere dataPayloadSizeBytes=20");
        NS_ABORT_MSG_IF(sfScanEdThresholdDbm != -120.0 || !sfScanResetOnNewSignal,
                        "Error: profile=" << profileName
                                           << " requiere sfScanEdThresholdDbm=-120 y SfScanResetOnNewSignal=true");
        NS_ABORT_MSG_IF(std::fabs(shadowingSigmaDb - 3.57) > 1e-9,
                        "Error: profile=" << profileName << " requiere shadowingSigmaDb=3.57");
    };

    if (profileLower == "pueyo2024")
    {
        validatePueyoComparableBase(profileLower);
        NS_ABORT_MSG_IF(cfg.enableCsma, "Error: profile=pueyo2024 requiere enableCsma=false");
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
        NS_ABORT_MSG_IF(cfg.enableDutyCycle || cfg.dutyLimit != 1.0,
                        "Error: profile=pueyo2024_paper_like requiere duty disabled y dutyLimit=1.0");
        NS_ABORT_MSG_IF(routeMetricMode != "toa_only",
                        "Error: profile=pueyo2024_paper_like requiere routeMetricMode=toa_only");
        NS_ABORT_MSG_IF(sfLinkMode != "deterministic_sensitivity",
                        "Error: profile=pueyo2024_paper_like requiere sfLinkMode=deterministic_sensitivity");
        NS_ABORT_MSG_IF(sfMin != 7 || sfMax != 8,
                        "Error: profile=pueyo2024_paper_like requiere sfMin=7 y sfMax=8");
        NS_ABORT_MSG_IF(!pueyoFloraLikeRx,
                        "Error: profile=pueyo2024_paper_like requiere pueyoFloraLikeRx=true");
        NS_ABORT_MSG_IF(enableSfScanRx,
                        "Error: profile=pueyo2024_paper_like requiere EnableSfScanRx=false");
    }
    else if (profileLower == "proposal_pueyo_like" ||
             profileLower == "proposal_pueyo_like_observed")
    {
        validatePueyoComparableBase(profileLower);
        NS_ABORT_MSG_IF(!cfg.enableCsma,
                        "Error: profile=" << profileLower << " requiere enableCsma=true");
        NS_ABORT_MSG_IF(!cfg.enableDutyCycle || cfg.dutyLimit != 0.01 || dutyWindowSec != 3600.0,
                        "Error: profile=" << profileLower
                                           << " requiere duty 1% con ventana 3600s");
        NS_ABORT_MSG_IF(routeMetricMode != "composite_score",
                        "Error: profile=" << profileLower
                                           << " requiere routeMetricMode=composite_score");
        NS_ABORT_MSG_IF(beaconLatestOnly,
                        "Error: profile=" << profileLower << " requiere BeaconLatestOnly=false");
        NS_ABORT_MSG_IF(!prioritizeBeacons,
                        "Error: profile=" << profileLower << " requiere PrioritizeBeacons=true");
        NS_ABORT_MSG_IF(pueyoStrictQueueScheduler,
                        "Error: profile=" << profileLower
                                           << " requiere PueyoStrictQueueScheduler=false con CSMA");
        NS_ABORT_MSG_IF(controlBackoffFactor != 1.0 || dataBackoffFactor != 10.0,
                        "Error: profile=" << profileLower
                                           << " requiere backoff factors control=1.0 data=10.0");
        if (profileLower == "proposal_pueyo_like")
        {
            NS_ABORT_MSG_IF(sfLinkMode != "deterministic_sensitivity",
                            "Error: profile=proposal_pueyo_like requiere sfLinkMode=deterministic_sensitivity");
        }
        else
        {
            NS_ABORT_MSG_IF(sfLinkMode != "observed_rxsf",
                            "Error: profile=proposal_pueyo_like_observed requiere sfLinkMode=observed_rxsf");
        }
    }

    if (pueyoFloraLikeRx)
    {
        enableSfScanRx = false;
        NS_LOG_WARN("Aplicando pueyoFloraLikeRx=true: misma semantica single-channel/single-demod y mismo modelo de colision; solo se desactiva pending scan/lock para usar lock inmediato viable.");
    }

    // FIX C3: Validación de parámetros CLI
    NS_ABORT_MSG_IF(cfg.nEd < 1, "Error: nEd debe ser >= 1 (necesitas al menos 2 nodos para mesh)");
    NS_ABORT_MSG_IF(cfg.dutyLimit < 0.0 || cfg.dutyLimit > 1.0,
                    "Error: dutyLimit debe estar en [0.0, 1.0], valor actual: " << cfg.dutyLimit);
    NS_ABORT_MSG_IF(dataStartSec < 0.0,
                    "Error: dataStartSec debe ser >= 0, valor actual: " << dataStartSec);
    NS_ABORT_MSG_IF(cfg.simTimeSec <= 0.0,
                    "Error: simTimeSec debe ser > 0, valor actual: " << cfg.simTimeSec);
    NS_ABORT_MSG_IF(dataStartSec >= cfg.simTimeSec,
                    "Error: dataStartSec (" << dataStartSec << ") debe ser < simTimeSec ("
                                            << cfg.simTimeSec << ")");
    NS_ABORT_MSG_IF(dataStopSec >= 0.0 && dataStopSec <= dataStartSec,
                    "Error: dataStopSec (" << dataStopSec
                                            << ") debe ser > dataStartSec (" << dataStartSec
                                            << ") o -1 para deshabilitar");
    NS_ABORT_MSG_IF(dataStopSec >= cfg.simTimeSec,
                    "Error: dataStopSec (" << dataStopSec << ") debe ser < simTimeSec ("
                                            << cfg.simTimeSec << ")");
    NS_ABORT_MSG_IF(puelloCaptureThresholdDb < 0.0,
                    "Error: puelloCaptureThresholdDb debe ser >= 0");
    NS_ABORT_MSG_IF(puelloAssumedBandwidthHz <= 0.0,
                    "Error: puelloAssumedBandwidthHz debe ser > 0");
    NS_ABORT_MSG_IF(puelloPreambleSymbols < 0.0,
                    "Error: puelloPreambleSymbols debe ser >= 0");
    NS_ABORT_MSG_IF(preambleSymbols < 6 || preambleSymbols > 64,
                    "Error: preambleSymbols debe estar en [6,64], valor actual: "
                        << preambleSymbols);
    NS_ABORT_MSG_IF(wireFormat != "v1" && wireFormat != "v2" && wireFormat != "pueyo7b",
                    "Error: wireFormat debe ser 'pueyo7b', 'v2' (legacy) o 'v1' (legacy), "
                    "valor actual: "
                        << wireFormat);
    NS_ABORT_MSG_IF(sfControl < 7 || sfControl > 12,
                    "Error: sfControl debe estar en [7,12], valor actual: " << sfControl);
    NS_ABORT_MSG_IF(sfMin < 7 || sfMin > 12 || sfMax < 7 || sfMax > 12,
                    "Error: sfMin/sfMax deben estar en [7,12], valores actuales: sfMin="
                        << sfMin << " sfMax=" << sfMax);
    NS_ABORT_MSG_IF(sfMin > sfMax,
                    "Error: sfMin debe ser <= sfMax, valores actuales: sfMin=" << sfMin
                                                                               << " sfMax=" << sfMax);
    NS_ABORT_MSG_IF(initTtl > 63,
                    "Error: initTtl debe estar en [0,63] para wire v2/pueyo7b, valor actual: "
                        << initTtl);
    NS_ABORT_MSG_IF(sfScanEdThresholdDbm < -160.0 || sfScanEdThresholdDbm > -60.0,
                    "Error: sfScanEdThresholdDbm fuera de rango razonable [-160,-60], valor actual: "
                        << sfScanEdThresholdDbm);
    NS_ABORT_MSG_IF(trafficMode != "periodic_any_to_any" && trafficMode != "pueyo_all_to_all",
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
    NS_ABORT_MSG_IF(onlyGenerateFromNodeId >= 0 && forcedDataDestinationId >= 0 &&
                        onlyGenerateFromNodeId == forcedDataDestinationId,
                    "Error: onlyGenerateFromNodeId y forcedDataDestinationId no pueden ser iguales");
    NS_ABORT_MSG_IF(dataPayloadSizeBytes < 1 || dataPayloadSizeBytes > 250,
                    "Error: dataPayloadSizeBytes debe estar en [1,250], valor actual: "
                        << dataPayloadSizeBytes);
    NS_ABORT_MSG_IF(routeAdvertPolicy != "top_score" && routeAdvertPolicy != "uniform" &&
                        routeAdvertPolicy != "cost_weighted",
                    "Error: routeAdvertPolicy debe ser top_score|uniform|cost_weighted, valor actual: "
                        << routeAdvertPolicy);
    NS_ABORT_MSG_IF(routeMetricMode != "composite_score" && routeMetricMode != "toa_only",
                    "Error: routeMetricMode debe ser composite_score|toa_only, valor actual: "
                        << routeMetricMode);
    NS_ABORT_MSG_IF(costEncoding != "score100" && costEncoding != "cost255" &&
                        costEncoding != "score255",
                    "Error: costEncoding debe ser score100|cost255|score255, valor actual: "
                        << costEncoding);
    NS_ABORT_MSG_IF(sfLinkMode != "observed_rxsf" && sfLinkMode != "deterministic_sensitivity",
                    "Error: sfLinkMode debe ser observed_rxsf|deterministic_sensitivity, valor actual: "
                        << sfLinkMode);
    NS_ABORT_MSG_IF(sfLinkMarginDb < -10.0 || sfLinkMarginDb > 20.0,
                    "Error: sfLinkMarginDb fuera de rango [-10,20], valor actual: "
                        << sfLinkMarginDb);
    NS_ABORT_MSG_IF(maxRoutesPerDestination < 1 || maxRoutesPerDestination > 2,
                    "Error: maxRoutesPerDestination debe estar en [1,2], valor actual: "
                        << maxRoutesPerDestination);
    NS_ABORT_MSG_IF(maxTotalRoutes < 1,
                    "Error: maxTotalRoutes debe ser >=1, valor actual: " << maxTotalRoutes);
    NS_ABORT_MSG_IF(nodePlacementMode != "line" && nodePlacementMode != "random" &&
                        nodePlacementMode != "pueyo_grid" && nodePlacementMode != "pueyo_random_equiv",
                    "Error: nodePlacementMode debe ser line|random|pueyo_grid|pueyo_random_equiv, valor actual: "
                        << nodePlacementMode);
    if (nodePlacementMode == "pueyo_grid" || nodePlacementMode == "pueyo_random_equiv")
    {
        const uint32_t sideAuto = static_cast<uint32_t>(std::llround(std::sqrt(static_cast<double>(cfg.nEd))));
        const uint32_t side = (pueyoGridSide > 0) ? pueyoGridSide : sideAuto;
        NS_ABORT_MSG_IF(side == 0 || side * side != cfg.nEd,
                        "Error: en modo pueyo_* nEd debe ser cuadrado perfecto (o definir pueyoGridSide válido). nEd="
                            << cfg.nEd << " side=" << side);
        NS_ABORT_MSG_IF(pueyoGridSpacingM <= 0.0,
                        "Error: pueyoGridSpacingM debe ser >0 en modo pueyo_*");
        areaWidth = (side > 1) ? (side - 1) * pueyoGridSpacingM : pueyoGridSpacingM;
        areaHeight = (side > 1) ? (side - 1) * pueyoGridSpacingM : pueyoGridSpacingM;
        pueyoGridSide = side;
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
        LogComponentEnable("MeshDvApp", LOG_LEVEL_INFO);
        LogComponentEnable("CsmaCadMac", LOG_LEVEL_INFO);
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
    g_metricsCollector = new MetricsCollector();
    g_metricsCollector->SetSimulationStopSec(cfg.simTimeSec);
    if (pdrEndWindowSec <= 0.0 && dataStopSec >= 0.0 && dataStopSec < cfg.simTimeSec)
    {
        pdrEndWindowSec = cfg.simTimeSec - dataStopSec;
    }
    dedupWindowSec = (dedupWindowSec > 0.0) ? dedupWindowSec : std::max(60.0, pdrEndWindowSec);
    g_metricsCollector->SetEndWindowSec(std::max(0.0, pdrEndWindowSec));
    g_metricsCollector->SetEssentialMetricsOnly(enableMetricsEssentialOnly);
    const uint32_t dataHeaderBytes = (wireFormat == "v1")
                                         ? 12U
                                         : ((wireFormat == "pueyo7b")
                                                ? DataWireHeaderPueyo7b::kSerializedSize
                                                : DataWireHeaderV2::kSerializedSize);
    const uint32_t beaconHeaderBytes = (wireFormat == "v1") ? 12U : BeaconWireHeaderV2::kSerializedSize;
    const uint32_t dvEntryBytes = (wireFormat == "v1") ? 6U : BeaconWireHeaderV2::kEntrySize;
    g_metricsCollector->SetWireFormatMetadata(
        wireFormat, dataHeaderBytes, beaconHeaderBytes, dvEntryBytes);
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
    runMeta.topologyPreset = (nodePlacementMode == "pueyo_grid" || nodePlacementMode == "pueyo_random_equiv")
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
    runMeta.enableNs3EnergyFramework = enableNs3EnergyFramework;
    runMeta.shadowingSigmaDb = shadowingSigmaDb;
    runMeta.channelCount = 1;
    runMeta.receptionPaths = 1;
    g_metricsCollector->SetRunConfigMetadata(runMeta);
    if (enableMetricsPeriodicFlush)
    {
        NS_ABORT_MSG_IF(metricsFlushIntervalSec <= 0.0,
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
    cfg.initTtl = static_cast<uint8_t>(std::min<uint32_t>(initTtl, 63));

    Ptr<loramesh::LoraMeshHelper> helper = CreateObject<loramesh::LoraMeshHelper>();
    helper->SetConfig(cfg);

    Config::SetDefaultFailSafe("ns3::loramesh::CsmaCadMac::MinBackoffSlots",
                               UintegerValue(minBackoffSlots));
    Config::SetDefaultFailSafe("ns3::loramesh::CsmaCadMac::BackoffStep",
                               UintegerValue(backoffStep));
    Config::SetDefaultFailSafe("ns3::loramesh::CsmaCadMac::DutyCycleLimit",
                               DoubleValue(cfg.dutyLimit));
    Config::SetDefaultFailSafe("ns3::loramesh::CsmaCadMac::DutyCycleWindow",
                               TimeValue(Seconds(dutyWindowSec)));
    Config::SetDefaultFailSafe("ns3::loramesh::CsmaCadMac::DutyCycleEnabled",
                               BooleanValue(cfg.enableDutyCycle));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DataStartTimeSec", DoubleValue(dataStartSec));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DataStopTimeSec", DoubleValue(dataStopSec));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::TrafficLoad", StringValue(trafficLoad));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::TrafficMode", StringValue(trafficMode));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::OnlyGenerateFromNodeId",
                               IntegerValue(onlyGenerateFromNodeId));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::ForcedDataDestinationId",
                               IntegerValue(forcedDataDestinationId));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::PueyoPacketsPerPair",
                               UintegerValue(pueyoPacketsPerPair));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DataPayloadSizeBytes",
                               UintegerValue(dataPayloadSizeBytes));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::EnableDataRandomDest",
                               BooleanValue(enableDataRandomDest));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::BeaconIntervalWarm",
                               TimeValue(Seconds(beaconIntervalWarmSec)));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::BeaconIntervalStable",
                               TimeValue(Seconds(beaconIntervalStableSec)));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::WireFormat", StringValue(wireFormat));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::PurePueyoBaselineMode",
                               BooleanValue(profileLower == "pueyo2024" ||
                                            profileLower == "pueyo2024_paper_like"));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::PueyoValidationTrace",
                               BooleanValue(pueyoValidationTrace));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::EnableGapAuditTrace",
                               BooleanValue(enableGapAuditTrace));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::PueyoSyntheticEntriesNodeId",
                               IntegerValue(pueyoSyntheticEntriesNodeId));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::PueyoSyntheticEntries",
                               StringValue(pueyoSyntheticEntries));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DvBeaconMaxRoutes",
                               UintegerValue(dvBeaconMaxRoutes));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DvPayloadMaxBytes",
                               UintegerValue(dvPayloadMaxBytes));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::RouteAdvertPolicy",
                               StringValue(routeAdvertPolicy));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::MacCacheWindow",
                               TimeValue(Seconds(linkAddrCacheWindowSec)));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::LinkAddrCacheWindow",
                               TimeValue(Seconds(linkAddrCacheWindowSec)));
    double neighborLinkTimeoutFactor = 1.0;
    if (neighborLinkTimeoutSec > 0.0)
    {
        neighborLinkTimeoutFactor =
            std::max(0.1, neighborLinkTimeoutSec / std::max(0.001, beaconIntervalStableSec));
    }
    Config::SetDefaultFailSafe("ns3::MeshDvApp::AutoTimeoutsFromBeacon", BooleanValue(true));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::NeighborLinkTimeoutFactor",
                               DoubleValue(neighborLinkTimeoutFactor));
    if (neighborLinkTimeoutSec > 0.0)
    {
        Config::SetDefaultFailSafe("ns3::MeshDvApp::NeighborLinkTimeout",
                                   TimeValue(Seconds(neighborLinkTimeoutSec)));
    }
    Config::SetDefaultFailSafe("ns3::MeshDvApp::AllowStaleMacForUnicastData",
                               BooleanValue(allowStaleLinkAddrForUnicastData));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::AllowStaleLinkAddrForUnicastData",
                               BooleanValue(allowStaleLinkAddrForUnicastData));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::EmpiricalSfMinSamples",
                               UintegerValue(empiricalSfMinSamples));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::EmpiricalSfSelectMode",
                               StringValue(empiricalSfSelectMode));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::UseProbabilisticSfForBeacons",
                               BooleanValue(useProbabilisticSfForBeacons));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::SfControl", UintegerValue(sfControl));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::SfMin", UintegerValue(sfMin));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::SfMax", UintegerValue(sfMax));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::PreambleSymbols",
                               UintegerValue(preambleSymbols));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::RouteSwitchMinDeltaX100",
                               UintegerValue(routeSwitchMinDeltaX100));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::AvoidImmediateBacktrack",
                               BooleanValue(avoidImmediateBacktrack));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DataPeriodJitterMax",
                               DoubleValue(dataPeriodJitterMaxSec));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::EnableDataSlots", BooleanValue(enableDataSlots));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DataSlotPeriodSec", DoubleValue(dataSlotPeriodSec));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DataSlotJitterSec", DoubleValue(dataSlotJitterSec));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::ExtraDvBeaconMaxPerWindow",
                               UintegerValue(extraDvBeaconMaxPerWindow));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::ExtraDvBeaconMinGap",
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
    Config::SetDefaultFailSafe("ns3::MeshDvApp::PrioritizeBeacons",
                               BooleanValue(prioritizeBeacons));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::BeaconLatestOnly",
                               BooleanValue(beaconLatestOnly));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::PueyoStrictQueueScheduler",
                               BooleanValue(pueyoStrictQueueScheduler));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::ControlBackoffFactor",
                               DoubleValue(controlBackoffFactor));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DataBackoffFactor", DoubleValue(dataBackoffFactor));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::EnableControlGuard",
                               BooleanValue(enableControlGuard));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::ControlGuardSec", DoubleValue(controlGuardSec));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DisableExtraAfterWarmup",
                               BooleanValue(disableExtraAfterWarmup));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::BatteryFullCapacityJ",
                               DoubleValue(batteryFullCapacityJ));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::RouteTimeoutFactor", DoubleValue(routeTimeoutFactor));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DedupWindowSec",
                               TimeValue(Seconds(dedupWindowSec)));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::LinkWeight", DoubleValue(dvLinkWeight));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::PathWeight", DoubleValue(dvPathWeight));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::PathHopWeight",
                               DoubleValue(dvPathHopWeight));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::MetricMode",
                               StringValue(routeMetricMode));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::CostEncoding",
                               StringValue(costEncoding));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::CompositeWToa",
                               DoubleValue(compositeWToa));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::CompositeWHop",
                               DoubleValue(compositeWHop));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::CompositeWEnergy",
                               DoubleValue(compositeWEnergy));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::CompositeCostStep",
                               DoubleValue(compositeCostStep));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::EnergyLo",
                               DoubleValue(energyLo));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::EnergyHi",
                               DoubleValue(energyHi));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::EnergyPow",
                               DoubleValue(energyPow));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::EnergyMaxPenalty",
                               DoubleValue(energyMaxPenalty));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::SfLinkMode",
                               StringValue(sfLinkMode));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::SfLinkMarginDb",
                               DoubleValue(sfLinkMarginDb));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::MaxRoutesPerDestination",
                               UintegerValue(maxRoutesPerDestination));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::MaxTotalRoutes",
                               UintegerValue(maxTotalRoutes));
    Config::SetDefaultFailSafe("ns3::loramesh::RoutingDv::PueyoValidationTrace",
                               BooleanValue(pueyoValidationTrace));
    // Debe definirse antes de Install(), porque SetDefault sólo aplica a objetos futuros.
    bool txPowerApplied = Config::SetDefaultFailSafe("ns3::lorawan::MeshLoraNetDevice::TxPowerDbm",
                                                     DoubleValue(txPowerDbm));
    bool preambleApplied =
        Config::SetDefaultFailSafe("ns3::lorawan::MeshLoraNetDevice::PreambleSymbols",
                                   UintegerValue(preambleSymbols));
    Config::SetDefaultFailSafe("ns3::lorawan::MeshLoraNetDevice::WireFormat",
                               StringValue(wireFormat));
    if (!txPowerApplied)
    {
        NS_LOG_WARN("No se pudo aplicar Config::SetDefault para MeshLoraNetDevice::TxPowerDbm; se "
                    "usarán los valores por defecto del dispositivo");
    }
    if (!preambleApplied)
    {
        NS_LOG_WARN("No se pudo aplicar Config::SetDefault para MeshLoraNetDevice::PreambleSymbols");
    }
    NS_LOG_INFO("DataStartTimeSec=" << dataStartSec << "s");
    NS_LOG_INFO("DataStopTimeSec=" << dataStopSec << "s");
    NS_LOG_INFO("Profile=" << profileLower
                << " TrafficMode="
                << trafficMode
                << " TrafficLoad="
                << trafficLoad
                << " EnableDataRandomDest=" << (enableDataRandomDest ? "true" : "false")
                << " PueyoPacketsPerPair=" << pueyoPacketsPerPair
                << " DataPayloadBytes=" << dataPayloadSizeBytes
                << " BeaconWarm=" << beaconIntervalWarmSec << "s"
                << " BeaconStable=" << beaconIntervalStableSec << "s"
                << " DvBeaconMaxRoutes=" << dvBeaconMaxRoutes
                << " DvPayloadMaxBytes=" << dvPayloadMaxBytes
                << " RouteAdvertPolicy=" << routeAdvertPolicy
                << " LinkAddrCacheWindow=" << linkAddrCacheWindowSec << "s"
                << " NeighborLinkTimeoutSec="
                << ((neighborLinkTimeoutSec > 0.0) ? std::to_string(neighborLinkTimeoutSec) : std::string("auto"))
                << " NeighborLinkTimeoutFactor=" << neighborLinkTimeoutFactor
                << " AllowStaleLinkAddr=" << (allowStaleLinkAddrForUnicastData ? "true" : "false")
                << " EmpiricalSfMode=" << empiricalSfSelectMode
                << " EmpiricalSfMinSamples=" << empiricalSfMinSamples
                << " RouteSwitchMinDelta=" << routeSwitchMinDeltaX100
                << " AvoidBacktrack=" << (avoidImmediateBacktrack ? "true" : "false")
                << " PdrEndWindow=" << pdrEndWindowSec << "s"
                << " DedupWindow=" << dedupWindowSec << "s"
                << " WireFormat=" << wireFormat
                << " RouteMetricMode=" << routeMetricMode
                << " CostEncoding=" << costEncoding
                << " CompositeWToa=" << compositeWToa
                << " CompositeWHop=" << compositeWHop
                << " CompositeWEnergy=" << compositeWEnergy
                << " CompositeCostStep=" << compositeCostStep
                << " DataJitterMax=" << dataPeriodJitterMaxSec << "s"
                << " DataSlots=" << (enableDataSlots ? "on" : "off")
                << " SlotPeriod=" << dataSlotPeriodSec << "s"
                << " SlotJitter=" << dataSlotJitterSec << "s"
                << " ExtraDvMax=" << extraDvBeaconMaxPerWindow
                << " ExtraDvMinGap=" << extraDvBeaconMinGapSec << "s"
                << " DvWeights(legacy/no-op)=" << dvLinkWeight << "," << dvPathWeight << ","
                << dvPathHopWeight
                << " InterferenceModel=" << interferenceModel
                << " Puello(threshold="
                << puelloCaptureThresholdDb << "dB,bw=" << puelloAssumedBandwidthHz
                << "Hz,preamble=" << puelloPreambleSymbols << ")"
                << " BeaconLatestOnly=" << (beaconLatestOnly ? "true" : "false"));
    if (enablePcap)
    {
        helper->EnablePcap("mesh_dv_node");
    }
    helper->Install(nodes);

    // ========================================================================
    // ENERGY FRAMEWORK SETUP
    if (enableNs3EnergyFramework)
    {
        // ========================================================================
        NS_LOG_INFO("Setting up ns-3 Energy Framework with random SOC U[60%,100%]...");

        // 1. Create BasicEnergySource for each node with random initial SOC
        //    Using BasicEnergySource instead of GenericBatteryModel to allow SetInitialEnergy()
        //    Li-Ion 18650: ~10.8 Wh = 38880 J at 3.6V nominal
        const double fullCapacityJ = batteryFullCapacityJ;

        Ptr<UniformRandomVariable> socRng = CreateObject<UniformRandomVariable>();
        socRng->SetAttribute("Min", DoubleValue(0.60)); // 60% minimum SOC
        socRng->SetAttribute("Max", DoubleValue(1.00)); // 100% maximum SOC

        energy::EnergySourceContainer batteries;
        for (uint32_t i = 0; i < nodes.GetN(); ++i)
        {
            double initialSoc = socRng->GetValue();
            double initialEnergyJ = fullCapacityJ * initialSoc;

            BasicEnergySourceHelper batteryHelper;
            batteryHelper.Set("BasicEnergySourceInitialEnergyJ", DoubleValue(initialEnergyJ));
            batteryHelper.Set("BasicEnergySupplyVoltageV", DoubleValue(3.6)); // Li-Ion nominal

            energy::EnergySourceContainer nodeBattery = batteryHelper.Install(nodes.Get(i));
            batteries.Add(nodeBattery);

            NS_LOG_INFO("Node " << i << " initial SOC: " << std::fixed << std::setprecision(1)
                                << (initialSoc * 100) << "% (" << initialEnergyJ << "J / "
                                << fullCapacityJ << "J)");
        }

        // 2. Get NetDevices for energy model installation
        NetDeviceContainer loraDevices;
        for (uint32_t i = 0; i < nodes.GetN(); ++i)
        {
            Ptr<Node> n = nodes.Get(i);
            for (uint32_t d = 0; d < n->GetNDevices(); ++d)
            {
                Ptr<NetDevice> dev = n->GetDevice(d);
                if (dev->GetInstanceTypeId().GetName().find("LoRa") != std::string::npos ||
                    dev->GetInstanceTypeId().GetName().find("Mesh") != std::string::npos)
                {
                    loraDevices.Add(dev);
                    break; // One LoRa device per node
                }
            }
        }

        // 3. Install LoRaDeviceEnergyModel on each device
        LoRaDeviceEnergyModelHelper loraEnergyHelper;
        loraEnergyHelper.Set("AutoTxCurrentFromPower", BooleanValue(true));
        loraEnergyHelper.Set("TxCurrentAt14dBmA", DoubleValue(0.100));
        loraEnergyHelper.Set("TxCurrentAt20dBmA", DoubleValue(0.120));
        loraEnergyHelper.Set("TxCurrentA",
                             DoubleValue(0.100)); // Initial TX current (auto-updated per txPowerDbm)
        loraEnergyHelper.Set("RxCurrentA", DoubleValue(0.011)); // 11 mA
        loraEnergyHelper.Set("CadCurrentA", DoubleValue(0.011)); // 11 mA (CAD)
        loraEnergyHelper.Set("IdleCurrentA", DoubleValue(0.001)); // 1 mA
        loraEnergyHelper.Set("SleepCurrentA", DoubleValue(0.0000002)); // 0.2 µA

        energy::DeviceEnergyModelContainer deviceEnergyModels =
            loraEnergyHelper.Install(loraDevices, batteries);

        // 5. Connect LoRaDeviceEnergyModel to each MeshLoraNetDevice
        for (uint32_t i = 0; i < loraDevices.GetN(); ++i)
        {
            Ptr<lorawan::MeshLoraNetDevice> meshDev =
                DynamicCast<lorawan::MeshLoraNetDevice>(loraDevices.Get(i));
            Ptr<LoRaDeviceEnergyModel> energyModel =
                DynamicCast<LoRaDeviceEnergyModel>(deviceEnergyModels.Get(i));

            if (meshDev && energyModel)
            {
                meshDev->SetLoRaEnergyModel(energyModel);
                NS_LOG_DEBUG("Node " << meshDev->GetNode()->GetId()
                                     << " LoRaDeviceEnergyModel connected to MeshLoraNetDevice");
            }
        }

        NS_LOG_INFO("Energy framework: " << batteries.GetN() << " batteries (SOC U[50%,100%]), "
                                         << deviceEnergyModels.GetN()
                                         << " device models installed");
    }
    else
    {
        NS_LOG_WARN("ns-3 energy framework disabled for this run; no BasicEnergySource or "
                    "LoRaDeviceEnergyModel will be installed");
    }

    // Ajustar inicio/fin de las apps de datos: DV sigue activo desde t=0
    for (uint32_t i = 0; i < nodes.GetN(); ++i)
    {
        Ptr<Node> n = nodes.Get(i);
        for (uint32_t a = 0; a < n->GetNApplications(); ++a)
        {
            Ptr<MeshDvApp> app = DynamicCast<MeshDvApp>(n->GetApplication(a));
            if (app)
            {
                Time jitter = Seconds(static_cast<double>(i) * 0.2);
                app->SetStartTime(jitter);
                app->SetStopTime(Seconds(cfg.simTimeSec - 0.1));
            }
        }
    }

    NS_LOG_INFO("Starting simulation for " << cfg.simTimeSec << " seconds...");

    // ========================================================================
    // THESIS METRICS T50/FND: Configure MetricsCollector
    // ========================================================================
    if (g_metricsCollector)
    {
        g_metricsCollector->SetTotalNodes(nodes.GetN());
        // Collector node: aligned with MeshDvApp's m_collectorNodeId
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
        delete g_metricsCollector;
        g_metricsCollector = nullptr;
    }

    // Close the unified PCAP file
    lorawan::MeshLoraNetDevice::CloseGlobalPcap();

    Simulator::Destroy();
    return 0;
}
