#ifndef LORAMESH_HELPER_H
#define LORAMESH_HELPER_H

#include "ns3/object.h"
#include "ns3/node-container.h"
#include "ns3/log.h"
#include <string>

namespace ns3
{

namespace loramesh
{

struct LoraMeshConfig
{
  uint32_t nEd {3};
  double simTimeSec {150.0};
  bool enableAdr {true};
  bool enableDutyCycle {true};
  double dutyLimit {0.10};  // 10%
  double spacing {30.0};
  double gwHeight {12.0};
};

class LoraMeshHelper : public Object
{
public:
  static TypeId GetTypeId ();

  LoraMeshHelper ();
  ~LoraMeshHelper () override = default;

  void SetConfig (const LoraMeshConfig& cfg);

  // Crea nodos, instala NetDevices, aplica movilidad y aplicaciones DV.
  void Install (NodeContainer& nodes);

  // Habilita PCAP con un prefijo opcional.
  void EnablePcap (const std::string& prefix);

private:
  void ConfigureMobility (NodeContainer& nodes);
  void InstallDevices (NodeContainer& nodes);
  void InstallApplications (NodeContainer& nodes);
  void ForceStandbyMode (NodeContainer& nodes);

  LoraMeshConfig m_cfg;
  bool m_enablePcap {false};
  std::string m_pcapPrefix {"mesh_dv_node"};
};

} // namespace loramesh
} // namespace ns3

#endif /* LORAMESH_HELPER_H */

