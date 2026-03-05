/*
 * LoRa Device Energy Model Helper Implementation
 */

#include "lora-device-energy-model-helper.h"

#include "lora-device-energy-model.h"

#include "ns3/log.h"
#include "ns3/names.h"

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("LoRaDeviceEnergyModelHelper");

LoRaDeviceEnergyModelHelper::LoRaDeviceEnergyModelHelper()
{
    m_loraDeviceEnergyModel.SetTypeId("ns3::LoRaDeviceEnergyModel");
}

void
LoRaDeviceEnergyModelHelper::Set(std::string name, const AttributeValue& v)
{
    m_loraDeviceEnergyModel.Set(name, v);
}

Ptr<energy::DeviceEnergyModel>
LoRaDeviceEnergyModelHelper::Install(Ptr<NetDevice> device, Ptr<energy::EnergySource> source) const
{
    NS_ASSERT(device);
    NS_ASSERT(source);

    Ptr<Node> node = device->GetNode();
    NS_ASSERT(node);

    // Create the device energy model
    Ptr<LoRaDeviceEnergyModel> model = m_loraDeviceEnergyModel.Create<LoRaDeviceEnergyModel>();

    NS_ASSERT(model);

    // Set node reference
    model->SetNode(node);

    // Connect to energy source
    model->SetEnergySource(source);
    source->AppendDeviceEnergyModel(model);

    NS_LOG_DEBUG("LoRaDeviceEnergyModel installed on node " << node->GetId());

    return model;
}

energy::DeviceEnergyModelContainer
LoRaDeviceEnergyModelHelper::Install(NetDeviceContainer devices,
                                     energy::EnergySourceContainer sources) const
{
    energy::DeviceEnergyModelContainer container;

    // Ensure we have matching sizes
    NS_ASSERT(devices.GetN() == sources.GetN());

    for (uint32_t i = 0; i < devices.GetN(); ++i)
    {
        Ptr<energy::DeviceEnergyModel> model = Install(devices.Get(i), sources.Get(i));
        container.Add(model);
    }

    return container;
}

} // namespace ns3
