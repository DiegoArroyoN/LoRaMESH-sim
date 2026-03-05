#ifndef MESH_LORA_NET_DEVICE_H
#define MESH_LORA_NET_DEVICE_H

#include "lora-device-energy-model.h"

#include "ns3/lora-phy.h"
#include "ns3/loramesh-energy-model.h"
#include "ns3/loramesh-mac-csma-cad.h"
#include "ns3/mac48-address.h"
#include "ns3/net-device.h"
#include "ns3/pcap-file-wrapper.h"
#include "ns3/ptr.h"
#include "ns3/traced-callback.h"

#include <string>

namespace ns3
{
namespace lorawan
{

/**
 * \brief NetDevice LoRa minimalista para mesh peer-to-peer
 *
 * No tiene MAC LoRaWAN. Permite broadcast directo usando LoraPhy.
 */
class MeshLoraNetDevice : public NetDevice
{
  public:
    static TypeId GetTypeId();

    MeshLoraNetDevice();
    ~MeshLoraNetDevice() override;

    // Setters
    void SetPhy(Ptr<LoraPhy> phy);
    Ptr<LoraPhy> GetPhy() const;

    void SetMac(Ptr<loramesh::CsmaCadMac> mac)
    {
        m_mac = mac;
        if (m_mac && m_phy)
        {
            m_mac->SetPhy(m_phy);
        }
    }

    Ptr<loramesh::CsmaCadMac> GetMac() const
    {
        return m_mac;
    }

    void SetEnergyModel(Ptr<loramesh::EnergyModel> energy)
    {
        m_energyModel = energy;
    }

    Ptr<loramesh::EnergyModel> GetEnergyModel() const
    {
        return m_energyModel;
    }

    double GetTxPowerDbm() const
    {
        return m_txPowerDbm;
    }

    void SetWireFormat(const std::string& format);
    std::string GetWireFormat() const;

    // Habilita volcados pcap (TX/RX) desde Send/Receive.
    void SetPcap(Ptr<PcapFileWrapper> tx, Ptr<PcapFileWrapper> rx)
    {
        m_pcapTx = tx;
        m_pcapRx = rx;
    }

    // ========================================================================
    // Static global PCAP for unified trace of all nodes
    // ========================================================================
    static void InitGlobalPcap(const std::string& filename);
    static void WriteGlobalPcap(uint32_t nodeId, bool isTx, Ptr<const Packet> packet);
    static void CloseGlobalPcap();

  private:
    static Ptr<PcapFileWrapper> s_globalPcap;
    static bool s_globalPcapInitialized;

  public:
    // NetDevice interface
    bool Send(Ptr<Packet> packet, const Address& dest, uint16_t protocolNumber) override;
    bool SendFrom(Ptr<Packet> packet,
                  const Address& source,
                  const Address& dest,
                  uint16_t protocolNumber) override;
    void SetIfIndex(const uint32_t index) override;
    uint32_t GetIfIndex() const override;
    Ptr<Channel> GetChannel() const override;
    void SetAddress(Address address) override;
    Address GetAddress() const override;
    bool SetMtu(const uint16_t mtu) override;
    uint16_t GetMtu() const override;
    bool IsLinkUp() const override;
    void AddLinkChangeCallback(Callback<void> callback) override;
    bool IsBroadcast() const override;
    Address GetBroadcast() const override;
    bool IsMulticast() const override;
    Address GetMulticast(Ipv4Address multicastGroup) const override;
    Address GetMulticast(Ipv6Address addr) const override;
    bool IsPointToPoint() const override;
    bool IsBridge() const override;
    void SetNode(Ptr<Node> node) override;
    Ptr<Node> GetNode() const override;
    bool NeedsArp() const override;
    void SetReceiveCallback(NetDevice::ReceiveCallback cb) override;
    void SetPromiscReceiveCallback(NetDevice::PromiscReceiveCallback cb) override;
    bool SupportsSendFrom() const override;
    // Get the last received RSSI value \return RSSI in dBm
    double GetLastRxRssi() const;
    void SetLastRxRssi(double rssi);

    // Callback desde PHY cuando llega un paquete
    void Receive(Ptr<const Packet> packet);

    // ========================================================================
    // NEW: ns-3 Energy Framework integration
    // ========================================================================
    void SetLoRaEnergyModel(Ptr<LoRaDeviceEnergyModel> model)
    {
        m_loraEnergyModel = model;
    }

    Ptr<LoRaDeviceEnergyModel> GetLoRaEnergyModel() const
    {
        return m_loraEnergyModel;
    }

    /**
     * Notify the energy model of a radio state change.
     * Called internally by PHY/MAC operations.
     * States: 0=TX, 1=RX, 2=CAD, 3=IDLE, 4=SLEEP
     */
    void NotifyRadioStateChange(int newState);

    Ptr<LoraPhy> m_phy;
    Ptr<Node> m_node;
    Ptr<loramesh::CsmaCadMac> m_mac;
    Ptr<loramesh::EnergyModel> m_energyModel;     ///< Legacy energy model (deprecated)
    Ptr<LoRaDeviceEnergyModel> m_loraEnergyModel; ///< ns-3 energy framework model
    double m_txPowerDbm{20.0};                    // Unified TX power for data/control.
    Ptr<PcapFileWrapper> m_pcapTx{nullptr};
    Ptr<PcapFileWrapper> m_pcapRx{nullptr};
    double m_lastRxRssi; ///< Last received RSSI in dBm
    uint32_t m_ifIndex;
    Mac48Address m_address;
    uint16_t m_mtu;
    std::string m_wireFormat{"v2"};
    NetDevice::ReceiveCallback m_rxCallback;
    NetDevice::PromiscReceiveCallback m_promiscRxCallback;
};

} // namespace lorawan
} // namespace ns3

#endif
