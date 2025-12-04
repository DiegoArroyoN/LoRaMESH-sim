#ifndef MESH_LORA_NET_DEVICE_H
#define MESH_LORA_NET_DEVICE_H

#include "ns3/net-device.h"
#include "ns3/lora-phy.h"
#include "ns3/traced-callback.h"
#include "ns3/mac48-address.h"
#include "ns3/ptr.h"
#include "ns3/pcap-file-wrapper.h"

namespace ns3 {
namespace lorawan {

/**
 * \brief NetDevice LoRa minimalista para mesh peer-to-peer
 * 
 * No tiene MAC LoRaWAN. Permite broadcast directo usando LoraPhy.
 */
class MeshLoraNetDevice : public NetDevice
{
public:
  static TypeId GetTypeId ();
  
  MeshLoraNetDevice ();
  ~MeshLoraNetDevice () override;
  
  // Setters
  void SetPhy (Ptr<LoraPhy> phy);
  Ptr<LoraPhy> GetPhy () const;
  // Habilita volcados pcap (TX/RX) desde Send/Receive.
  void SetPcap (Ptr<PcapFileWrapper> tx, Ptr<PcapFileWrapper> rx)
  {
    m_pcapTx = tx;
    m_pcapRx = rx;
  }
  
  // NetDevice interface
  bool Send (Ptr<Packet> packet, const Address& dest, uint16_t protocolNumber) override;
  bool SendFrom (Ptr<Packet> packet, const Address& source, const Address& dest, uint16_t protocolNumber) override;
  void SetIfIndex (const uint32_t index) override;
  uint32_t GetIfIndex () const override;
  Ptr<Channel> GetChannel () const override;
  void SetAddress (Address address) override;
  Address GetAddress () const override;
  bool SetMtu (const uint16_t mtu) override;
  uint16_t GetMtu () const override;
  bool IsLinkUp () const override;
  void AddLinkChangeCallback (Callback<void> callback) override;
  bool IsBroadcast () const override;
  Address GetBroadcast () const override;
  bool IsMulticast () const override;
  Address GetMulticast (Ipv4Address multicastGroup) const override;
  Address GetMulticast (Ipv6Address addr) const override;
  bool IsPointToPoint () const override;
  bool IsBridge () const override;
  void SetNode (Ptr<Node> node) override;
  Ptr<Node> GetNode () const override;
  bool NeedsArp () const override;
  void SetReceiveCallback (NetDevice::ReceiveCallback cb) override;
  void SetPromiscReceiveCallback (NetDevice::PromiscReceiveCallback cb) override;
  bool SupportsSendFrom () const override;
  //Get the last received RSSI value \return RSSI in dBm 
  double GetLastRxRssi () const;
  void SetLastRxRssi (double rssi);
  
  // Callback desde PHY cuando llega un paquete
  void Receive (Ptr<const Packet> packet);

private:
  Ptr<LoraPhy> m_phy;
  Ptr<Node> m_node;
  Ptr<PcapFileWrapper> m_pcapTx {nullptr};
  Ptr<PcapFileWrapper> m_pcapRx {nullptr};
  double m_lastRxRssi; ///< Last received RSSI in dBm
  uint32_t m_ifIndex;
  Mac48Address m_address;
  uint16_t m_mtu;
  NetDevice::ReceiveCallback m_rxCallback;
  NetDevice::PromiscReceiveCallback m_promiscRxCallback;
};

} // namespace lorawan
} // namespace ns3

#endif
