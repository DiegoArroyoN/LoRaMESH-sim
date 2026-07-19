/* SPDX-License-Identifier: GPL-2.0-only */

#ifndef DV_CL_LORA_NET_DEVICE_H
#define DV_CL_LORA_NET_DEVICE_H

#include "dv-cl-energy-registry.h"
#include "dv-cl-lora-energy-model.h"

#include "ns3/lora-phy.h"
#include "dv-cl-mac-csma-cad.h"
#include "ns3/mac48-address.h"
#include "ns3/net-device.h"
#include "ns3/pcap-file-wrapper.h"
#include "ns3/ptr.h"
#include "ns3/traced-callback.h"

#include <string>

namespace ns3
{
namespace dvcl
{

/**
 * \brief NetDevice LoRa minimalista para mesh peer-to-peer
 *
 * No tiene MAC LoRaWAN. Permite broadcast directo usando lorawan::LoraPhy.
 */
class DvClLoraNetDevice : public NetDevice
{
  public:
    static TypeId GetTypeId();

    DvClLoraNetDevice();
    ~DvClLoraNetDevice() override;

    // Setters
    void SetPhy(Ptr<lorawan::LoraPhy> phy);
    Ptr<lorawan::LoraPhy> GetPhy() const;

    void SetMac(Ptr<DvClCsmaCadMac> mac)
    {
        m_mac = mac;
        if (m_mac && m_phy)
        {
            m_mac->SetPhy(m_phy);
        }
    }

    Ptr<DvClCsmaCadMac> GetMac() const
    {
        return m_mac;
    }

    void SetEnergyModel(Ptr<DvClEnergyRegistry> energy)
    {
        m_energyModel = energy;
    }

    Ptr<DvClEnergyRegistry> GetEnergyModel() const
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
    void SetLoRaEnergyModel(Ptr<DvClLoraEnergyModel> model)
    {
        m_loraEnergyModel = model;
    }

    Ptr<DvClLoraEnergyModel> GetLoRaEnergyModel() const
    {
        return m_loraEnergyModel;
    }

    /**
     * Notify the energy model of a radio state change.
     * Called internally by PHY/MAC operations.
     * States: 0=TX, 1=RX, 2=CAD, 3=IDLE, 4=SLEEP
     */
    void NotifyRadioStateChange(int newState);

    Ptr<lorawan::LoraPhy> m_phy;
    Ptr<Node> m_node;
    Ptr<DvClCsmaCadMac> m_mac;
    Ptr<DvClEnergyRegistry> m_energyModel; //!< campaign energy engine (registry)
    Ptr<DvClLoraEnergyModel> m_loraEnergyModel; ///< ns-3 energy framework model
    double m_txPowerDbm{14.0};                    // Unified TX power for data/control.
    uint8_t m_preambleSymbols{8};                 // LoRa preamble symbols used on-air.
    Ptr<PcapFileWrapper> m_pcapTx{nullptr};
    Ptr<PcapFileWrapper> m_pcapRx{nullptr};
    double m_lastRxRssi; ///< Last received RSSI in dBm
    uint32_t m_ifIndex;
    Mac48Address m_address;
    uint16_t m_mtu;
    std::string m_wireFormat{"pueyo7b"};
    NetDevice::ReceiveCallback m_rxCallback;
    NetDevice::PromiscReceiveCallback m_promiscRxCallback;
};

} // namespace dvcl
} // namespace ns3

#endif
