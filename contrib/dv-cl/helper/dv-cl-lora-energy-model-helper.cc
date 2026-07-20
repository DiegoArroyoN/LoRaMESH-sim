/* SPDX-License-Identifier: GPL-2.0-only */

/*
 * LoRa Device Energy Model Helper Implementation
 */

#include "dv-cl-lora-energy-model-helper.h"

#include "ns3/dv-cl-lora-energy-model.h"

#include "ns3/log.h"
#include "ns3/names.h"

namespace ns3
{
namespace dvcl
{

NS_LOG_COMPONENT_DEFINE("DvClLoraEnergyModelHelper");

DvClLoraEnergyModelHelper::DvClLoraEnergyModelHelper()
{
    m_loraDeviceEnergyModel.SetTypeId("ns3::dvcl::DvClLoraEnergyModel");
}

void
DvClLoraEnergyModelHelper::Set(std::string name, const AttributeValue& v)
{
    m_loraDeviceEnergyModel.Set(name, v);
}

Ptr<energy::DeviceEnergyModel>
DvClLoraEnergyModelHelper::Install(Ptr<NetDevice> device, Ptr<energy::EnergySource> source) const
{
    NS_ASSERT(device);
    NS_ASSERT(source);

    Ptr<Node> node = device->GetNode();
    NS_ASSERT(node);

    // Create the device energy model
    Ptr<DvClLoraEnergyModel> model = m_loraDeviceEnergyModel.Create<DvClLoraEnergyModel>();

    NS_ASSERT(model);

    // Set node reference
    model->SetNode(node);

    // Connect to energy source
    model->SetEnergySource(source);
    source->AppendDeviceEnergyModel(model);

    NS_LOG_DEBUG("DvClLoraEnergyModel installed on node " << node->GetId());

    return model;
}

energy::DeviceEnergyModelContainer
DvClLoraEnergyModelHelper::Install(NetDeviceContainer devices,
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

} // namespace dvcl
} // namespace ns3
