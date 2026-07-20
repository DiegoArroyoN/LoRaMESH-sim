/* SPDX-License-Identifier: GPL-2.0-only */

/*
 * LoRa Device Energy Model Helper
 *
 * Helper class to install DvClLoraEnergyModel on devices.
 */

#ifndef DV_CL_LORA_ENERGY_MODEL_HELPER_H
#define DV_CL_LORA_ENERGY_MODEL_HELPER_H

#include "ns3/device-energy-model-container.h"
#include "ns3/energy-source-container.h"
#include "ns3/net-device-container.h"
#include "ns3/node-container.h"
#include "ns3/object-factory.h"

namespace ns3
{
namespace dvcl
{

/**
 * \brief Helper to install DvClLoraEnergyModel on devices
 */
class DvClLoraEnergyModelHelper
{
  public:
    DvClLoraEnergyModelHelper();
    ~DvClLoraEnergyModelHelper() = default;

    /**
     * \param name Name of attribute to set
     * \param v Value of the attribute
     *
     * Sets an attribute of the underlying DvClLoraEnergyModel
     */
    void Set(std::string name, const AttributeValue& v);

    /**
     * \param device Pointer to the NetDevice to install DeviceEnergyModel
     * \param source Pointer to EnergySource to use
     * \returns Pointer to the installed DeviceEnergyModel
     *
     * Installs a DvClLoraEnergyModel on the given device using the given source.
     */
    Ptr<energy::DeviceEnergyModel> Install(Ptr<NetDevice> device,
                                           Ptr<energy::EnergySource> source) const;

    /**
     * \param devices Container of NetDevices
     * \param sources Container of EnergySources
     * \returns Container of installed DeviceEnergyModels
     *
     * Installs LoRaDeviceEnergyModels on devices using corresponding sources.
     */
    energy::DeviceEnergyModelContainer Install(NetDeviceContainer devices,
                                               energy::EnergySourceContainer sources) const;

  private:
    ObjectFactory m_loraDeviceEnergyModel;
};

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_LORA_ENERGY_MODEL_HELPER_H */
