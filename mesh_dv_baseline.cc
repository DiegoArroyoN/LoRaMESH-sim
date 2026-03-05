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

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("MeshDvBaseline");

int
main(int argc, char* argv[])
{
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
    bool verboseLogs = true;
    uint32_t minBackoffSlots = 4;
    uint32_t backoffStep = 2;
    double beaconIntervalWarmSec = 10.0;
    double beaconIntervalStableSec = 90.0;
    std::string wireFormat = "v2";
    uint32_t dvBeaconMaxRoutes = 0;
    double linkAddrCacheWindowSec = 300.0;
    double neighborLinkTimeoutSec = -1.0; // <=0 => auto (1x beacon interval)
    bool allowStaleLinkAddrForUnicastData = true;
    uint32_t empiricalSfMinSamples = 2;
    std::string empiricalSfSelectMode = "robust_min";
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
    std::string interferenceModel = "puello"; // goursaud | puello
    double puelloCaptureThresholdDb = 6.0;
    double puelloAssumedBandwidthHz = 125000.0;
    double puelloPreambleSymbols = 8.0;
    double txPowerDbm = 20.0;
    bool prioritizeBeacons = true;
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
    std::string nodePlacementMode = "random";
    double areaWidth = 1000.0;
    double areaHeight = 1000.0;
    uint32_t rngRun = 1;
    // Thesis specification: FLoRa urban environment model
    double pathLossExponent = 2.08;  // FLoRa urban (thesis: n=2.08)
    double referenceDistance = 40.0; // FLoRa reference (thesis: d₀=40m)
    double referenceLossDb = 127.41; // FLoRa urban L(d₀=40m) = 127.41 dB (thesis specification)

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
    cmd.AddValue("verboseLogs",
                 "Enable ns-3 component INFO logs for MeshDvBaseline/MeshDvApp/CsmaCadMac",
                 verboseLogs);
    cmd.AddValue("beaconIntervalWarmSec",
                 "DV beacon interval during warmup [s]",
                 beaconIntervalWarmSec);
    cmd.AddValue("beaconIntervalStableSec",
                 "DV beacon interval during stable phase [s]",
                 beaconIntervalStableSec);
    cmd.AddValue("wireFormat", "Packet wire format: v1 | v2", wireFormat);
    cmd.AddValue("dvBeaconMaxRoutes",
                 "Max routes per DV beacon (0 = MTU-derived)",
                 dvBeaconMaxRoutes);
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
    cmd.AddValue("nodePlacementMode", "line or random", nodePlacementMode);
    cmd.AddValue("areaWidth", "Random placement width [m] (mode=random)", areaWidth);
    cmd.AddValue("areaHeight", "Random placement height [m] (mode=random)", areaHeight);
    cmd.AddValue("rngRun", "RNG run number for reproducible placement", rngRun);
    cmd.AddValue("pathLossExponent", "Log-distance path loss exponent", pathLossExponent);
    cmd.AddValue("referenceDistance", "Path loss reference distance [m]", referenceDistance);
    cmd.AddValue("referenceLossDb", "Path loss at reference distance [dB]", referenceLossDb);
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
                 "PHY interference model: goursaud | puello",
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
    cmd.AddValue("txPowerDbm", "Node TX power [dBm]", txPowerDbm);
    cmd.AddValue("prioritizeBeacons",
                 "Prioritize DV beacons ahead of data in CSMA queue",
                 prioritizeBeacons);
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
    NS_ABORT_MSG_IF(wireFormat != "v1" && wireFormat != "v2",
                    "Error: wireFormat debe ser 'v1' o 'v2', valor actual: " << wireFormat);

    lorawan::LoraInterferenceHelper::InterferenceModel interferenceModelEnum =
        lorawan::LoraInterferenceHelper::PUEYO_FIXED_CAPTURE;
    if (interferenceModel == "goursaud")
    {
        interferenceModelEnum = lorawan::LoraInterferenceHelper::GOURSAUD_PROBABILISTIC;
    }
    else if (interferenceModel == "puello" || interferenceModel == "pueyo")
    {
        interferenceModelEnum = lorawan::LoraInterferenceHelper::PUEYO_FIXED_CAPTURE;
    }
    else
    {
        NS_ABORT_MSG("Error: interferenceModel debe ser 'goursaud' o 'puello', valor actual: "
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
    const uint32_t dataHeaderBytes =
        (wireFormat == "v2") ? DataWireHeaderV2::kSerializedSize : 12U;
    const uint32_t beaconHeaderBytes =
        (wireFormat == "v2") ? BeaconWireHeaderV2::kSerializedSize : 12U;
    const uint32_t dvEntryBytes = (wireFormat == "v2") ? BeaconWireHeaderV2::kEntrySize : 6U;
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
    MetricsCollector::RunConfigMetadata runMeta;
    runMeta.simVersion = "wire_v2.1_rpfix";
    runMeta.nNodes = cfg.nEd;
    runMeta.topology = nodePlacementMode;
    runMeta.areaWidthM = areaWidth;
    runMeta.areaHeightM = areaHeight;
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
    runMeta.payloadBytes = 20;
    runMeta.dedupWindowSec = dedupWindowSec;
    runMeta.beaconIntervalWarmS = beaconIntervalWarmSec;
    runMeta.beaconIntervalStableS = beaconIntervalStableSec;
    runMeta.routeTimeoutFactor = routeTimeoutFactor;
    runMeta.routeTimeoutSec = beaconIntervalStableSec * routeTimeoutFactor;
    runMeta.interferenceModel = interferenceModel;
    runMeta.propModel = "log_distance";
    runMeta.txPowerDbm = txPowerDbm;
    runMeta.channelCount = 1;
    g_metricsCollector->SetRunConfigMetadata(runMeta);

    RngSeedManager::SetRun(rngRun);

    NodeContainer nodes;
    nodes.Create(cfg.nEd);

    cfg.nodePlacementMode = nodePlacementMode;
    cfg.areaWidth = areaWidth;
    cfg.areaHeight = areaHeight;
    cfg.pathLossExponent = pathLossExponent;
    cfg.referenceDistance = referenceDistance;
    cfg.referenceLossDb = referenceLossDb;

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
    Config::SetDefaultFailSafe("ns3::MeshDvApp::EnableDataRandomDest",
                               BooleanValue(enableDataRandomDest));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::BeaconIntervalWarm",
                               TimeValue(Seconds(beaconIntervalWarmSec)));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::BeaconIntervalStable",
                               TimeValue(Seconds(beaconIntervalStableSec)));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::WireFormat", StringValue(wireFormat));
    Config::SetDefaultFailSafe("ns3::MeshDvApp::DvBeaconMaxRoutes",
                               UintegerValue(dvBeaconMaxRoutes));
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
    // Debe definirse antes de Install(), porque SetDefault sólo aplica a objetos futuros.
    bool txPowerApplied = Config::SetDefaultFailSafe("ns3::lorawan::MeshLoraNetDevice::TxPowerDbm",
                                                     DoubleValue(txPowerDbm));
    Config::SetDefaultFailSafe("ns3::lorawan::MeshLoraNetDevice::WireFormat",
                               StringValue(wireFormat));
    if (!txPowerApplied)
    {
        NS_LOG_WARN("No se pudo aplicar Config::SetDefault para MeshLoraNetDevice::TxPowerDbm; se "
                    "usarán los valores por defecto del dispositivo");
    }
    NS_LOG_INFO("DataStartTimeSec=" << dataStartSec << "s");
    NS_LOG_INFO("DataStopTimeSec=" << dataStopSec << "s");
    NS_LOG_INFO("TrafficMode=any-to-any (mesh) TrafficLoad="
                << trafficLoad
                << " EnableDataRandomDest=" << (enableDataRandomDest ? "true" : "false")
                << " BeaconWarm=" << beaconIntervalWarmSec << "s"
                << " BeaconStable=" << beaconIntervalStableSec << "s"
                << " DvBeaconMaxRoutes=" << dvBeaconMaxRoutes
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
                << "Hz,preamble=" << puelloPreambleSymbols << ")");
    if (enablePcap)
    {
        helper->EnablePcap("mesh_dv_node");
    }
    helper->Install(nodes);

    // ========================================================================
    // ENERGY FRAMEWORK SETUP
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
    loraEnergyHelper.Set("TxCurrentA", DoubleValue(0.120));   // 120 mA @ 20dBm (SX1276 high-power TX)
    loraEnergyHelper.Set("RxCurrentA", DoubleValue(0.011));   // 11 mA
    loraEnergyHelper.Set("CadCurrentA", DoubleValue(0.011));  // 11 mA (CAD)
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
                                     << deviceEnergyModels.GetN() << " device models installed");

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
        std::cerr << "\n>>> Exportando métricas...\n";
        g_metricsCollector->PrintStatistics();
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
