/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_HELPER_H
#define DV_CL_HELPER_H

#include "ns3/log.h"
#include "ns3/node-container.h"
#include "ns3/object.h"

#include <cstdint>
#include <string>

namespace ns3
{

namespace dvcl
{

struct DvClMeshConfig
{
    uint32_t nEd{3};
    double simTimeSec{150.0};
    bool enableDutyCycle{true};
    double dutyLimit{0.10}; // 10%
    double spacing{30.0};
    double gwHeight{12.0};
    bool enableCsma{true};
    std::string nodePlacementMode{"line"};
    double areaWidth{2000.0};
    double areaHeight{2000.0};
    uint32_t gridSide{0};
    double gridSpacingX{30.0};
    double gridSpacingY{30.0};
    double pathLossExponent{2.7};
    double referenceDistance{1.0};
    double referenceLossDb{7.7};
    double shadowingSigmaDb{3.57};
    uint8_t initTtl{10};
};

class DvClHelper : public Object
{
  public:
    static TypeId GetTypeId();

    DvClHelper();
    ~DvClHelper() override = default;

    void SetConfig(const DvClMeshConfig& cfg);

    // Crea nodos, instala NetDevices, aplica movilidad y aplicaciones DV.
    void Install(NodeContainer& nodes);

    // Habilita PCAP con un prefijo opcional.
    void EnablePcap(const std::string& prefix);

  private:
    void ConfigureMobility(NodeContainer& nodes);
    void InstallDevices(NodeContainer& nodes);
    void InstallApplications(NodeContainer& nodes);
    void ForceStandbyMode(NodeContainer& nodes);

    DvClMeshConfig m_cfg;
    bool m_enablePcap{false};
    std::string m_pcapPrefix{"mesh_dv_node"};
};

} // namespace dvcl
} // namespace ns3

#endif /* DV_CL_HELPER_H */
