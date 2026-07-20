/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-helper.h"

#include "ns3/dv-cl-app.h"
#include "ns3/dv-cl-lora-net-device.h"
#include "ns3/lora-channel.h"
#include "ns3/mac48-address.h"
#include "ns3/mobility-helper.h"
#include "ns3/pcap-file-wrapper.h"
#include "ns3/pointer.h"
#include "ns3/position-allocator.h"
#include "ns3/propagation-delay-model.h"
#include "ns3/propagation-loss-model.h"
#include "ns3/random-variable-stream.h"
#include "ns3/simple-end-device-lora-phy.h"
#include "ns3/simple-gateway-lora-phy.h"
#include "ns3/simulator.h"

#include <algorithm>
#include <cmath>

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("DvClHelper");

namespace dvcl
{

NS_OBJECT_ENSURE_REGISTERED(DvClHelper);

TypeId
DvClHelper::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClHelper").SetParent<Object>().AddConstructor<DvClHelper>();
    return tid;
}

DvClHelper::DvClHelper() = default;

void
DvClHelper::SetConfig(const DvClMeshConfig& cfg)
{
    m_cfg = cfg;
}

void
DvClHelper::EnablePcap(const std::string& prefix)
{
    m_enablePcap = true;
    if (!prefix.empty())
    {
        m_pcapPrefix = prefix;
    }
}

void
DvClHelper::Install(NodeContainer& nodes)
{
    ConfigureMobility(nodes);
    InstallDevices(nodes);
    InstallApplications(nodes);
    ForceStandbyMode(nodes);
}

void
DvClHelper::ConfigureMobility(NodeContainer& nodes)
{
    MobilityHelper mobility;
    Ptr<ListPositionAllocator> alloc = CreateObject<ListPositionAllocator>();

    const uint32_t totalNodes = nodes.GetN();
    const uint32_t gwIndex = (totalNodes > 0) ? (totalNodes - 1) : 0;

    auto installRandom = [&](double width, double height, const std::string& label) -> bool {
        if (width <= 0.0 || height <= 0.0)
        {
            return false;
        }
        Ptr<UniformRandomVariable> rng = CreateObject<UniformRandomVariable>();
        for (uint32_t i = 0; i < totalNodes; ++i)
        {
            const double x = rng->GetValue(0.0, width);
            const double y = rng->GetValue(0.0, height);
            const double z = (i == gwIndex) ? m_cfg.gwHeight : 0.0;
            alloc->Add(Vector(x, y, z));
        }
        mobility.SetPositionAllocator(alloc);
        mobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
        mobility.Install(nodes);
        NS_LOG_INFO("TOPOLOGY: " << label << " " << width << "x" << height << " m");
        return true;
    };

    auto installGrid = [&](uint32_t side, double dx, double dy, const std::string& label) -> bool {
        if (side == 0 || dx <= 0.0 || dy <= 0.0 || side * side != totalNodes)
        {
            return false;
        }
        for (uint32_t idx = 0; idx < totalNodes; ++idx)
        {
            const uint32_t row = idx / side;
            const uint32_t col = idx % side;
            const double x = static_cast<double>(col) * dx;
            const double y = static_cast<double>(row) * dy;
            const double z = (idx == gwIndex) ? m_cfg.gwHeight : 0.0;
            alloc->Add(Vector(x, y, z));
        }
        mobility.SetPositionAllocator(alloc);
        mobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
        mobility.Install(nodes);
        NS_LOG_INFO("TOPOLOGY: " << label << " side=" << side << " spacing=(" << dx << "," << dy
                                 << ") area=" << ((side > 1) ? (side - 1) * dx : 0.0) << "x"
                                 << ((side > 1) ? (side - 1) * dy : 0.0) << " m");
        return true;
    };

    auto resolveGridSide = [&]() -> uint32_t {
        if (m_cfg.gridSide > 0)
        {
            return m_cfg.gridSide;
        }
        const double sideF = std::sqrt(static_cast<double>(totalNodes));
        const uint32_t side = static_cast<uint32_t>(std::llround(sideF));
        return side;
    };

    const std::string placementMode = m_cfg.nodePlacementMode;
    if (placementMode == "pueyo_grid" || placementMode == "pueyo_random_equiv")
    {
        const uint32_t side = resolveGridSide();
        const double spacingX = (m_cfg.gridSpacingX > 0.0) ? m_cfg.gridSpacingX : m_cfg.spacing;
        const double spacingY = (m_cfg.gridSpacingY > 0.0) ? m_cfg.gridSpacingY : m_cfg.spacing;
        if (side == 0 || side * side != totalNodes)
        {
            NS_LOG_WARN("Invalid pueyo_* config (nEd is not a perfect square). Falling back to "
                        "line placement");
        }
        else if (placementMode == "pueyo_grid")
        {
            if (installGrid(side, spacingX, spacingY, "Pueyo grid"))
            {
                return;
            }
        }
        else
        {
            const double width = (side > 1) ? (side - 1) * spacingX : spacingX;
            const double height = (side > 1) ? (side - 1) * spacingY : spacingY;
            if (installRandom(width, height, "Pueyo random equivalent area"))
            {
                return;
            }
        }
    }

    if (placementMode == "random")
    {
        const double width = m_cfg.areaWidth;
        const double height = m_cfg.areaHeight;
        if (width <= 0.0 || height <= 0.0)
        {
            NS_LOG_WARN("Invalid random area size; falling back to line placement");
        }
        else
        {
            if (installRandom(width, height, "Random uniform"))
            {
                return;
            }
        }
    }

    double currentX = 0.0;
    for (uint32_t i = 0; i < totalNodes; ++i)
    {
        double z = (i == gwIndex) ? m_cfg.gwHeight : 0.0;
        alloc->Add(Vector(currentX, 0.0, z));
        if (i == gwIndex)
        {
            NS_LOG_INFO("Node " << i << " (GW/Dest) pos=(" << currentX << ", 0, " << m_cfg.gwHeight
                                << ")");
        }
        else
        {
            NS_LOG_INFO("Node " << i << " (ED) pos=(" << currentX << ", 0, 0)");
        }
        currentX += m_cfg.spacing;
    }

    mobility.SetPositionAllocator(alloc);
    mobility.SetMobilityModel("ns3::ConstantPositionMobilityModel");
    mobility.Install(nodes);

    NS_LOG_INFO("TOPOLOGY: Linear with spacing=" << m_cfg.spacing << "m, total length="
                                                 << currentX - m_cfg.spacing << "m");
}

void
DvClHelper::InstallDevices(NodeContainer& nodes)
{
    Ptr<LogDistancePropagationLossModel> loss = CreateObject<LogDistancePropagationLossModel>();
    loss->SetPathLossExponent(m_cfg.pathLossExponent);
    loss->SetReference(m_cfg.referenceDistance, m_cfg.referenceLossDb);

    if (m_cfg.shadowingSigmaDb > 0.0)
    {
        Ptr<RandomPropagationLossModel> shadowing = CreateObject<RandomPropagationLossModel>();
        Ptr<NormalRandomVariable> shadowingVar = CreateObject<NormalRandomVariable>();
        shadowingVar->SetAttribute("Mean", DoubleValue(0.0));
        shadowingVar->SetAttribute("Variance",
                                   DoubleValue(m_cfg.shadowingSigmaDb * m_cfg.shadowingSigmaDb));
        shadowing->SetAttribute("Variable", PointerValue(shadowingVar));
        loss->SetNext(shadowing); // Chain: LogDistance → Shadowing
    }

    Ptr<PropagationDelayModel> delay = CreateObject<ConstantSpeedPropagationDelayModel>();
    Ptr<lorawan::LoraChannel> channel = CreateObject<lorawan::LoraChannel>(loss, delay);

    NS_LOG_INFO("✓ Canal LoRa creado con LogDistancePropagationLossModel (ref="
                << m_cfg.referenceLossDb << "dB@" << m_cfg.referenceDistance
                << "m, exp=" << m_cfg.pathLossExponent << ")"
                << (m_cfg.shadowingSigmaDb > 0.0
                        ? (" + Shadowing σ=" + std::to_string(m_cfg.shadowingSigmaDb) + "dB")
                        : " + No shadowing"));

    // ========================================================================
    // Initialize global unified PCAP file for all nodes
    // ========================================================================
    if (m_enablePcap)
    {
        std::string pcapName = m_pcapPrefix + "_all.pcap";
        DvClLoraNetDevice::InitGlobalPcap(pcapName);
    }

    for (uint32_t i = 0; i < nodes.GetN(); ++i)
    {
        Ptr<Node> node = nodes.Get(i);

        Ptr<lorawan::SimpleGatewayLoraPhy> phy = CreateObject<lorawan::SimpleGatewayLoraPhy>();
        phy->SetMobility(node->GetObject<MobilityModel>());
        phy->SetChannel(channel);
        phy->AddFrequency(868000000); // EU868 band as per thesis specification
        // Single-demod configuration: one reception path per node (no concurrent multi-demod RX).
        for (int p = 0; p < 1; ++p)
        {
            phy->AddReceptionPath();
        }

        Ptr<DvClLoraNetDevice> dev = CreateObject<DvClLoraNetDevice>();
        dev->SetNode(node);
        dev->SetAddress(Mac48Address::Allocate());
        dev->SetMtu(255);
        dev->SetPhy(phy);

        channel->Add(phy);

        node->AddDevice(dev);

        NS_LOG_INFO("Node " << i << " device installed (SimpleGatewayLoraPhy), freq=868MHz");
    }

    NS_LOG_INFO("Total PHYs registered: " << channel->GetNDevices());
}

void
DvClHelper::InstallApplications(NodeContainer& nodes)
{
    const double stopTime = m_cfg.simTimeSec;
    const uint32_t collectorNodeId = (nodes.GetN() > 0) ? (nodes.GetN() - 1) : 0;
    for (uint32_t i = 0; i < nodes.GetN(); ++i)
    {
        Ptr<DvClApp> app = CreateObject<DvClApp>();
        app->SetPeriod(Seconds(60.0));
        app->SetInitTtl(std::min<uint8_t>(63, m_cfg.initTtl));
        app->SetInitScoreX100(100);
        app->SetCsmaEnabled(m_cfg.enableCsma);
        app->SetCollectorNodeId(collectorNodeId);

        nodes.Get(i)->AddApplication(app);
        app->SetStartTime(Seconds(1.0 + i * 0.5));
        app->SetStopTime(Seconds(stopTime));

        NS_LOG_INFO(">>> DvClApp installed on node " << i << " (collectorNodeId=" << collectorNodeId
                                                     << ")");
    }

    NS_LOG_INFO("DvClApp configured on ALL " << nodes.GetN() << " nodes");
}

void
DvClHelper::ForceStandbyMode(NodeContainer& nodes)
{
    NS_LOG_INFO("Verificando que todos los nodos estén listos para RX...");

    for (uint32_t i = 0; i < nodes.GetN(); ++i)
    {
        Ptr<Node> node = nodes.Get(i);
        Ptr<NetDevice> netDev = node->GetDevice(0);
        Ptr<DvClLoraNetDevice> meshDev = DynamicCast<DvClLoraNetDevice>(netDev);

        if (meshDev)
        {
            Ptr<lorawan::LoraPhy> basePhy = meshDev->GetPhy();
            Ptr<lorawan::SimpleGatewayLoraPhy> phy =
                DynamicCast<lorawan::SimpleGatewayLoraPhy>(basePhy);

            if (phy)
            {
                // SimpleGatewayLoraPhy remains in RX when not transmitting; no standby switch
                // needed.
                NS_LOG_INFO("✓ Node " << i << " PHY (single-demod) ready for RX");
            }
        }
    }

    NS_LOG_INFO("=== TODOS LOS NODOS LISTOS PARA RX ===");
}

} // namespace dvcl
} // namespace ns3
