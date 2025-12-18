#include "mesh_lora_net_device.h"
#include "ns3/log.h"
#include "ns3/node.h"
#include "ns3/packet.h"
#include "ns3/simulator.h"
#include "ns3/lora-phy.h"
#include "ns3/end-device-lora-phy.h"
#include "ns3/lora-channel.h"
#include "ns3/simple-end-device-lora-phy.h"
#include "ns3/lora-tag.h"
#include "mesh_metric_tag.h"
#include "mesh_mac_header.h"
#include "ns3/pcap-file-wrapper.h"


namespace ns3 {
namespace lorawan {

NS_LOG_COMPONENT_DEFINE ("MeshLoraNetDevice");
NS_OBJECT_ENSURE_REGISTERED (MeshLoraNetDevice);

TypeId
MeshLoraNetDevice::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::lorawan::MeshLoraNetDevice")
    .SetParent<NetDevice> ()
    .SetGroupName ("lorawan")
    .AddConstructor<MeshLoraNetDevice> ();
  return tid;
}

MeshLoraNetDevice::MeshLoraNetDevice ()
  : m_phy (nullptr),
    m_node (nullptr),
    m_lastRxRssi (-120.0),
    m_ifIndex (0),
    m_mtu (255),
    m_address (Mac48Address::Allocate ())  // Asignar dirección MAC aleatoria
{
  NS_LOG_FUNCTION (this);
}



MeshLoraNetDevice::~MeshLoraNetDevice ()
{
}

void
MeshLoraNetDevice::SetPhy (Ptr<LoraPhy> phy)
{
  NS_LOG_FUNCTION (this << phy);
  m_phy = phy;
  
  NS_LOG_INFO ("Configurando PHY callbacks para MeshLoraNetDevice en node " << (m_node ? m_node->GetId() : -1));
  
  // ========================================================================
  // MÉTODO 1: SetReceiveOkCallback (directo, preferido)
  // ========================================================================
  Ptr<ns3::lorawan::LoraPhy> basePhy = DynamicCast<ns3::lorawan::LoraPhy> (phy);
  if (basePhy)
  {
    basePhy->SetReceiveOkCallback (MakeCallback (&MeshLoraNetDevice::Receive, this));
    NS_LOG_INFO ("✓ SetReceiveOkCallback registrado exitosamente");
  }
  else
  {
    NS_LOG_WARN ("✗ No se pudo hacer DynamicCast a LoraPhy");
  }
  
  // ========================================================================
  // MÉTODO 2: TracedCallback (backup)
  // ========================================================================
  if (basePhy)
  {
    basePhy->TraceConnectWithoutContext ("m_phyRxEndTrace",
                                         MakeCallback (&MeshLoraNetDevice::Receive, this));
    NS_LOG_INFO ("✓ m_phyRxEndTrace conectado (backup)");
  }
  
  phy->SetDevice (this);
  NS_LOG_INFO ("PHY completamente configurado con NetDevice=" << this);
}







Ptr<LoraPhy> 
MeshLoraNetDevice::GetPhy () const 
{ 
  return m_phy; 
}

// Transmite un paquete desde la capa de red hacia el PHY LoRa.
bool
MeshLoraNetDevice::Send (Ptr<Packet> packet, const Address& dest, uint16_t protocolNumber)
{
  NS_LOG_FUNCTION (this << packet << dest << protocolNumber);
  
  if (!m_phy)
  {
    NS_LOG_WARN ("PHY no configurado");
    return false;
  }
  
  // Verificar que el PHY tenga canal
  Ptr<LoraChannel> channel = DynamicCast<LoraChannel> (m_phy->GetChannel ());
  if (!channel)
  {
    NS_LOG_ERROR ("PHY no tiene canal configurado!");
    return false;
  }

  // Volcado pcap de TX
  if (m_pcapTx)
  {
    m_pcapTx->Write (Simulator::Now (), packet);
  }
  
  // Parámetros TX LoRa
  LoraTxParameters txParams;
  txParams.sf = 10;
  txParams.headerDisabled = false;
  txParams.codingRate = 1;
  txParams.bandwidthHz = 125000;
  txParams.nPreamble = 8;
  txParams.crcEnabled = true;
  txParams.lowDataRateOptimizationEnabled = false;

  NS_LOG_UNCOND ("MeshLoraNetDevice::Send params sf=" << unsigned(txParams.sf)
                  << " bw=" << txParams.bandwidthHz
                  << " cr=" << unsigned(txParams.codingRate)
                  << " pktSize=" << packet->GetSize ());

  MeshMetricTag meshTag;
  // Potencia base para datos; DV ya se eleva más abajo
  double txPowerDbm = 20.0;
  if (packet->PeekPacketTag (meshTag))
  {
    uint8_t desiredSf = meshTag.GetSf ();
    if (desiredSf >= 7 && desiredSf <= 12)
    {
      txParams.sf = desiredSf;
    }
    // Plano de control: beacons/dv usan SF12 y alta potencia
    if (meshTag.GetDst () == 0xFFFF)
    {
      txPowerDbm = 20.0;
    }
  }
  else
  {
    // Fallback: usar SF por defecto del nodo si no viene tag
    txParams.sf = 9;
  }

  NS_LOG_INFO ("Node " << GetNode()->GetId() << " SENDING to " << dest 
               << ": pkt_size=" << packet->GetSize() 
               << " freq=915MHz, power=" << txPowerDbm << "dBm, SF" << unsigned (txParams.sf));
  
  // Enviar paquete por PHY
  m_phy->Send (packet, txParams, 915000000, txPowerDbm);
  NS_LOG_UNCOND("MeshLoraNetDevice::Send EJECUTADO en node "
              << GetNode()->GetId() << " size=" << packet->GetSize());

  // ========================================================================
  // CRÍTICO: Calcular duración TX y programar vuelta a STANDBY
  // ========================================================================
  Ptr<SimpleEndDeviceLoraPhy> edPhy = DynamicCast<SimpleEndDeviceLoraPhy> (m_phy);
  if (edPhy)
  {
    Time txDuration = edPhy->GetOnAirTime (packet, txParams);
    if (m_mac)
    {
      m_mac->NotifyTxStart (txDuration.GetSeconds ());
    }
    if (m_energyModel && m_node)
    {
      m_energyModel->UpdateEnergy (m_node->GetId (), loramesh::EnergyModel::kDefaultTxCurrentMa,
                                   txDuration.GetSeconds ());
    }
    NS_LOG_UNCOND("Node " << GetNode()->GetId() 
                  << " duración TX: " << txDuration.GetMilliSeconds() << "ms"
                  << " (" << txDuration.GetSeconds() << "s)");
    
    // Programar vuelta a STANDBY después de TX
    Simulator::Schedule (txDuration + MilliSeconds(10),
                         &SimpleEndDeviceLoraPhy::SwitchToStandby,
                         edPhy);
    NS_LOG_INFO("Node " << GetNode()->GetId() << " programado retorno a STANDBY en " 
                << txDuration.GetMilliSeconds() << "ms");
  }
  else
  {
    NS_LOG_WARN ("Node " << GetNode()->GetId() 
                 << " PHY is not SimpleEndDeviceLoraPhy, cannot auto-switch to STANDBY");
  }
  
  return true;
}



// Callback invocado cuando SimpleEndDeviceLoraPhy entrega un paquete recibido.
void
MeshLoraNetDevice::Receive (Ptr<const Packet> packet)
{
  NS_LOG_UNCOND(">>>>>>> MeshLoraNetDevice::Receive LLAMADO en node "
                << (m_node ? m_node->GetId() : -1) << " size=" << packet->GetSize());
  
  NS_LOG_FUNCTION (this << packet << packet->GetSize());

  // Volcado pcap de RX
  if (m_pcapRx)
  {
    Ptr<Packet> copy = packet->Copy ();
    m_pcapRx->Write (Simulator::Now (), copy);
  }
  
  NS_LOG_INFO ("**** RECEIVE() CALLED ON NETDEVICE **** Node " << (m_node ? m_node->GetId() : -1) 
             << " size=" << packet->GetSize());

  
  // ========================================================================
  // Capturar RSSI REAL
  // ========================================================================
  LoraTag loraTag;
  bool hasTag = packet->PeekPacketTag (loraTag);
  
  if (hasTag)
  {
    double rxPower = loraTag.GetReceivePower ();
    m_lastRxRssi = rxPower;
    NS_LOG_INFO ("Node " << (m_node ? m_node->GetId() : -1) 
                 << " Captured RX RSSI: " << rxPower << " dBm");
  }
  else
  {
    NS_LOG_WARN ("LoraTag NOT found, RSSI=default -120dBm");
    m_lastRxRssi = -120.0;
  }
  
  Ptr<Packet> pktCopy = packet->Copy ();
  // Extraer MAC de origen si el header está presente para pasarlo al callback
  Address srcAddr = m_address;
  MeshMacHeader macHdr;
  if (pktCopy->PeekHeader (macHdr))
    {
      srcAddr = macHdr.GetSrc ();
    }
  MeshMetricTag rxTag;
  if (pktCopy->PeekPacketTag (rxTag))
  {
    double duration = rxTag.GetToaUs () / 1e6;
    if (m_mac)
    {
      m_mac->NotifyRxStart (duration);
    }
    if (m_energyModel && m_node)
    {
      m_energyModel->UpdateRxEnergy (m_node->GetId (), duration);
    }
  }
  
  if (!m_rxCallback.IsNull ())
  {
    m_rxCallback (this, pktCopy, 0x88B5, srcAddr);
  }
  else
  {
    NS_LOG_WARN ("RxCallback es NULL!");
  }
}



// ======== Resto de interfaz NetDevice (boilerplate) ========

bool 
MeshLoraNetDevice::SendFrom (Ptr<Packet> p, const Address& src, const Address& dest, uint16_t proto)
{
  return Send (p, dest, proto);
}

void 
MeshLoraNetDevice::SetIfIndex (const uint32_t index) 
{ 
  m_ifIndex = index; 
}

uint32_t 
MeshLoraNetDevice::GetIfIndex () const 
{ 
  return m_ifIndex; 
}

Ptr<Channel> 
MeshLoraNetDevice::GetChannel () const
{
  return m_phy ? m_phy->GetChannel () : nullptr;
}

void 
MeshLoraNetDevice::SetAddress (Address address) 
{ 
  m_address = Mac48Address::ConvertFrom (address); 
}

Address 
MeshLoraNetDevice::GetAddress () const 
{ 
  return m_address; 
}

bool 
MeshLoraNetDevice::SetMtu (const uint16_t mtu) 
{ 
  m_mtu = mtu; 
  return true; 
}

uint16_t 
MeshLoraNetDevice::GetMtu () const 
{ 
  return m_mtu; 
}

bool 
MeshLoraNetDevice::IsLinkUp () const 
{ 
  return true; 
}

void 
MeshLoraNetDevice::AddLinkChangeCallback (Callback<void> cb) 
{
}

bool 
MeshLoraNetDevice::IsBroadcast () const 
{ 
  return true; 
}

Address 
MeshLoraNetDevice::GetBroadcast () const 
{ 
  return Mac48Address ("ff:ff:ff:ff:ff:ff"); 
}

bool 
MeshLoraNetDevice::IsMulticast () const 
{ 
  return false; 
}

Address 
MeshLoraNetDevice::GetMulticast (Ipv4Address multicastGroup) const 
{ 
  return GetBroadcast (); 
}

Address 
MeshLoraNetDevice::GetMulticast (Ipv6Address addr) const 
{ 
  return GetBroadcast (); 
}

bool 
MeshLoraNetDevice::IsPointToPoint () const 
{ 
  return false; 
}

bool 
MeshLoraNetDevice::IsBridge () const 
{ 
  return false; 
}

void 
MeshLoraNetDevice::SetNode (Ptr<Node> node) 
{ 
  m_node = node; 
}

Ptr<Node> 
MeshLoraNetDevice::GetNode () const 
{ 
  return m_node; 
}

bool 
MeshLoraNetDevice::NeedsArp () const 
{ 
  return false; 
}

void 
MeshLoraNetDevice::SetReceiveCallback (NetDevice::ReceiveCallback cb) 
{ 
  m_rxCallback = cb; 
}

void 
MeshLoraNetDevice::SetPromiscReceiveCallback (NetDevice::PromiscReceiveCallback cb) 
{ 
  m_promiscRxCallback = cb; 
}

bool 
MeshLoraNetDevice::SupportsSendFrom () const 
{ 
  return true; 
}

double
MeshLoraNetDevice::GetLastRxRssi () const
{
  return m_lastRxRssi;
}

void
MeshLoraNetDevice::SetLastRxRssi (double rssi)
{
  m_lastRxRssi = rssi;
}


} // namespace lorawan
} // namespace ns3
