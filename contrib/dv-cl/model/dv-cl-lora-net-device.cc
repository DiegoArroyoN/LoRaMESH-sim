/* SPDX-License-Identifier: GPL-2.0-only */

#include "dv-cl-lora-net-device.h"

#include "dv-cl-lora-energy-model.h"
#include "dv-cl-mac-header.h"
#include "dv-cl-metric-tag.h"
#include "dv-cl-toa.h"
#include "dv-cl-wire.h"

#include "ns3/double.h"
#include "ns3/end-device-lora-phy.h"
#include "ns3/gateway-lora-phy.h"
#include "ns3/log.h"
#include "ns3/lora-channel.h"
#include "ns3/lora-phy.h"
#include "ns3/lora-tag.h"
#include "ns3/node.h"
#include "ns3/packet.h"
#include "ns3/pcap-file-wrapper.h"
#include "ns3/simple-end-device-lora-phy.h"
#include "ns3/simple-gateway-lora-phy.h"
#include "ns3/simulator.h"
#include "ns3/string.h"

#include <cctype>
#include <iomanip>
#include <sstream>

namespace ns3
{
namespace dvcl
{

/**
 * Minimum received size that triggers the beacon source-address probe.
 *
 * A beacon is the 6 B header plus its DV entries, so 6 B is the smallest one
 * that can arrive: a node advertising no routes yet. The campaign guarded this
 * probe with 7 B — the size of the retired experimental header — and therefore
 * never recovered the logical source of an empty beacon, never learning a
 * nodeId<->MAC mapping from it. That cost it routes, and with them relays and
 * delivered packets. Guarding at the real header size is the correction.
 */
static constexpr uint32_t kBeaconProbeMinSize = DvClBeaconHeader::kSerializedSize;
using namespace ns3::lorawan;

namespace
{

Mac48Address
LogicalIdToPseudoMac48(uint16_t logicalId)
{
    std::ostringstream oss;
    oss << "02:00:00:00:" << std::hex << std::setfill('0') << std::setw(2)
        << ((logicalId >> 8) & 0xFF) << ":" << std::setw(2) << (logicalId & 0xFF);
    return Mac48Address(oss.str().c_str());
}
} // namespace

NS_LOG_COMPONENT_DEFINE("DvClLoraNetDevice");
NS_OBJECT_ENSURE_REGISTERED(DvClLoraNetDevice);

TypeId
DvClLoraNetDevice::GetTypeId()
{
    static TypeId tid =
        TypeId("ns3::dvcl::DvClLoraNetDevice")
            .SetParent<NetDevice>()
            .SetGroupName("DvCl")
            .AddConstructor<DvClLoraNetDevice>()
            .AddAttribute("TxPowerDbm",
                          "Unified TX power in dBm for data and control (DV).",
                          DoubleValue(14.0),
                          MakeDoubleAccessor(&DvClLoraNetDevice::m_txPowerDbm),
                          MakeDoubleChecker<double>())
            .AddAttribute("PreambleSymbols",
                          "LoRa preamble symbols used to build PHY TX parameters.",
                          UintegerValue(8),
                          MakeUintegerAccessor(&DvClLoraNetDevice::m_preambleSymbols),
                          MakeUintegerChecker<uint8_t>(6, 64));
    return tid;
}

// ============================================================================
// Static global PCAP implementation
// ============================================================================
Ptr<PcapFileWrapper> DvClLoraNetDevice::s_globalPcap = nullptr;
bool DvClLoraNetDevice::s_globalPcapInitialized = false;

void
DvClLoraNetDevice::InitGlobalPcap(const std::string& filename)
{
    if (s_globalPcapInitialized)
    {
        return; // Already initialized
    }
    s_globalPcap = CreateObject<PcapFileWrapper>();
    s_globalPcap->Open(filename, std::ios::out | std::ios::binary);
    s_globalPcap->Init(0); // Link-type 0 = NULL/Loopback
    s_globalPcapInitialized = true;
    NS_LOG_INFO("✓ PCAP unificado global inicializado: " << filename);
}

void
DvClLoraNetDevice::WriteGlobalPcap(uint32_t nodeId, bool isTx, Ptr<const Packet> packet)
{
    if (!s_globalPcapInitialized || !s_globalPcap)
    {
        return;
    }

    // Extract DvClMetricTag info if present
    DvClMetricTag tag;
    bool hasTag = packet->PeekPacketTag(tag);

    // Create enhanced header (24 bytes) with all metadata
    // Format:
    //   [0-1]   Magic: 0x4D53 ("MS" for MeSH)
    //   [2-3]   NodeId (observer)
    //   [4]     Direction: 0x54='T'(TX), 0x52='R'(RX)
    //   [5]     Type: 0x42='B'(Broadcast), 0x55='U'(Unicast)
    //   [6-7]   Source
    //   [8-9]   Destination (0xFFFF = broadcast)
    //   [10-13] Sequence
    //   [14]    TTL
    //   [15]    Hops
    //   [16]    SF
    //   [17-18] RSSI (signed, dBm)
    //   [19-20] Battery (mV)
    //   [21-22] Score (x100)
    //   [23]    Reserved

    uint8_t header[24];
    memset(header, 0, sizeof(header));

    // Magic bytes "MS"
    header[0] = 0x4D;
    header[1] = 0x53;

    // Node ID (observer)
    header[2] = static_cast<uint8_t>(nodeId & 0xFF);
    header[3] = static_cast<uint8_t>((nodeId >> 8) & 0xFF);

    // Direction: 'T' or 'R'
    header[4] = isTx ? 0x54 : 0x52;

    if (hasTag)
    {
        uint16_t dst = tag.GetDst();
        bool isBroadcast = (dst == 0xFFFF);

        // Type: 'B' = Broadcast, 'U' = Unicast
        header[5] = isBroadcast ? 0x42 : 0x55;

        // Source
        uint16_t src = tag.GetSrc();
        header[6] = static_cast<uint8_t>(src & 0xFF);
        header[7] = static_cast<uint8_t>((src >> 8) & 0xFF);

        // Destination
        header[8] = static_cast<uint8_t>(dst & 0xFF);
        header[9] = static_cast<uint8_t>((dst >> 8) & 0xFF);

        // Sequence
        uint32_t seq = tag.GetSeq();
        header[10] = static_cast<uint8_t>(seq & 0xFF);
        header[11] = static_cast<uint8_t>((seq >> 8) & 0xFF);
        header[12] = static_cast<uint8_t>((seq >> 16) & 0xFF);
        header[13] = static_cast<uint8_t>((seq >> 24) & 0xFF);

        // TTL, Hops, SF
        header[14] = tag.GetTtl();
        header[15] = tag.GetHops();
        header[16] = tag.GetSf();

        // RSSI (signed)
        int16_t rssi = 0; // RSSI removed from DvClMetricTag
        header[17] = static_cast<uint8_t>(rssi & 0xFF);
        header[18] = static_cast<uint8_t>((rssi >> 8) & 0xFF);

        // Battery (mV)
        uint16_t batt = tag.GetBatt_mV();
        header[19] = static_cast<uint8_t>(batt & 0xFF);
        header[20] = static_cast<uint8_t>((batt >> 8) & 0xFF);

        // Score (x100)
        uint16_t score = tag.GetScoreX100();
        header[21] = static_cast<uint8_t>(score & 0xFF);
        header[22] = static_cast<uint8_t>((score >> 8) & 0xFF);
    }
    else
    {
        header[5] = 0x3F; // '?' = Unknown type
    }

    header[23] = 0x00; // Reserved

    // Create packet with header prepended
    Ptr<Packet> pktWithHeader = Create<Packet>(header, 24);
    pktWithHeader->AddAtEnd(packet->Copy());

    s_globalPcap->Write(Simulator::Now(), pktWithHeader);
}

void
DvClLoraNetDevice::CloseGlobalPcap()
{
    if (s_globalPcapInitialized && s_globalPcap)
    {
        s_globalPcap = nullptr;
        s_globalPcapInitialized = false;
    }
}

DvClLoraNetDevice::DvClLoraNetDevice()
    : m_phy(nullptr),
      m_node(nullptr),
      m_lastRxRssi(-120.0),
      m_ifIndex(0),
      m_address(Mac48Address::Allocate()), // Asignar dirección MAC aleatoria
      m_mtu(255)
{
    NS_LOG_FUNCTION(this);
}

DvClLoraNetDevice::~DvClLoraNetDevice()
{
}

void
DvClLoraNetDevice::SetPhy(Ptr<LoraPhy> phy)
{
    NS_LOG_FUNCTION(this << phy);
    m_phy = phy;
    if (m_mac)
    {
        m_mac->SetPhy(phy);
    }

    NS_LOG_INFO("Configurando PHY callbacks para DvClLoraNetDevice en node "
                << (m_node ? m_node->GetId() : -1));

    // ========================================================================
    // MÉTODO 1: SetReceiveOkCallback (directo, preferido)
    // ========================================================================
    Ptr<ns3::lorawan::LoraPhy> basePhy = DynamicCast<ns3::lorawan::LoraPhy>(phy);
    if (basePhy)
    {
        basePhy->SetReceiveOkCallback(MakeCallback(&DvClLoraNetDevice::Receive, this));
        NS_LOG_INFO("✓ SetReceiveOkCallback registrado exitosamente");
    }
    else
    {
        NS_LOG_WARN("✗ No se pudo hacer DynamicCast a LoraPhy");
    }

    // ========================================================================
    // MÉTODO 2: TracedCallback (backup)
    // ========================================================================
    if (basePhy)
    {
        basePhy->TraceConnectWithoutContext("m_phyRxEndTrace",
                                            MakeCallback(&DvClLoraNetDevice::Receive, this));
        NS_LOG_INFO("✓ m_phyRxEndTrace conectado (backup)");
    }

    phy->SetDevice(this);
    NS_LOG_INFO("PHY completamente configurado con NetDevice=" << this);
}

Ptr<LoraPhy>
DvClLoraNetDevice::GetPhy() const
{
    return m_phy;
}

// Transmite un paquete desde la capa de red hacia el PHY LoRa.
bool
DvClLoraNetDevice::Send(Ptr<Packet> packet, const Address& dest, uint16_t protocolNumber)
{
    NS_LOG_FUNCTION(this << packet << dest << protocolNumber);

    if (!m_phy)
    {
        NS_LOG_WARN("PHY no configurado");
        return false;
    }

    // Verificar que el PHY tenga canal
    Ptr<LoraChannel> channel = DynamicCast<LoraChannel>(m_phy->GetChannel());
    if (!channel)
    {
        NS_LOG_ERROR("PHY no tiene canal configurado!");
        return false;
    }

    Ptr<Packet> txPacket = packet;

    // Volcado pcap de TX
    if (m_pcapTx)
    {
        m_pcapTx->Write(Simulator::Now(), txPacket);
    }
    // Global unified PCAP
    WriteGlobalPcap(m_node ? m_node->GetId() : 0, true, txPacket);

    // Parámetros TX LoRa
    LoraTxParameters txParams;
    txParams = BuildTxParams(10);

    NS_LOG_INFO("DvClLoraNetDevice::Send params sf="
                << unsigned(txParams.sf) << " bw=" << txParams.bandwidthHz
                << " cr=" << unsigned(txParams.codingRate) << " pktSize=" << packet->GetSize());

    DvClMetricTag meshTag;
    // TX unificado para datos y beacons para alinear DV con el comportamiento real de datos.
    double txPowerDbm = m_txPowerDbm;
    if (txPacket->PeekPacketTag(meshTag))
    {
        uint8_t desiredSf = meshTag.GetSf();
        if (desiredSf >= 7 && desiredSf <= 12)
        {
            txParams.sf = desiredSf;
        }
    }
    else
    {
        // Fallback: usar SF por defecto del nodo si no viene tag
        txParams.sf = 9;
    }

    NS_LOG_INFO("Node " << GetNode()->GetId() << " SENDING to " << dest << ": pkt_size="
                        << txPacket->GetSize() << " freq=868MHz, power=" << txPowerDbm << "dBm, SF"
                        << unsigned(txParams.sf));

    // ========================================================================
    // ENERGY: switch instantaneous state to TX
    // ========================================================================
    NotifyRadioStateChange(0); // 0 = TX

    // Enviar paquete por PHY (EU868 = 868 MHz)
    const uint32_t carrierHz = m_region ? m_region->GetFrequencyHz() : 868000000;
    m_phy->Send(txPacket, txParams, carrierHz, txPowerDbm);
    NS_LOG_INFO("DvClLoraNetDevice::Send EJECUTADO en node " << GetNode()->GetId()
                                                             << " size=" << txPacket->GetSize());

    // ========================================================================
    // Calcular duración TX usando LoraPhy::GetOnAirTime (static, works with any PHY)
    // ========================================================================
    Time txDuration = LoraPhy::GetOnAirTime(txPacket, txParams);
    if (m_mac)
    {
        m_mac->NotifyTxStart(txDuration.GetSeconds());
    }
    if (m_energyModel && m_node)
    {
        m_energyModel->ChargeTx(txDuration.GetSeconds());
    }
    NS_LOG_INFO("Node " << GetNode()->GetId() << " duración TX: " << txDuration.GetMilliSeconds()
                        << "ms"
                        << " (" << txDuration.GetSeconds() << "s)");

    // SimpleGatewayLoraPhy (used for multi-SF RX, NOT as LoRaWAN gateway)
    // handles TX→RX transition internally via TxFinished callback.
    // Schedule state change to IDLE after TX completes for energy tracking.
    Simulator::Schedule(txDuration + MilliSeconds(10),
                        &DvClLoraNetDevice::NotifyRadioStateChange,
                        this,
                        3); // 3 = IDLE

    NS_LOG_INFO("Node " << GetNode()->GetId() << " TX duration " << txDuration.GetMilliSeconds()
                        << "ms, multi-SF PHY auto-returns to RX");

    return true;
}

// Callback invocado cuando SimpleEndDeviceLoraPhy entrega un paquete recibido.
void
DvClLoraNetDevice::Receive(Ptr<const Packet> packet)
{
    // ========================================================================
    // ENERGY: Notify state change to RX (receiving a packet)
    // ========================================================================
    NotifyRadioStateChange(1); // 1 = RX

    NS_LOG_INFO(">>>>>>> DvClLoraNetDevice::Receive LLAMADO en node "
                << (m_node ? m_node->GetId() : -1) << " size=" << packet->GetSize());

    NS_LOG_FUNCTION(this << packet << packet->GetSize());

    // Volcado pcap de RX
    if (m_pcapRx)
    {
        Ptr<Packet> copy = packet->Copy();
        m_pcapRx->Write(Simulator::Now(), copy);
    }
    // Global unified PCAP
    WriteGlobalPcap(m_node ? m_node->GetId() : 0, false, packet);

    NS_LOG_INFO("**** RECEIVE() CALLED ON NETDEVICE **** Node " << (m_node ? m_node->GetId() : -1)
                                                                << " size=" << packet->GetSize());

    // ========================================================================
    // Capturar RSSI REAL
    // ========================================================================
    LoraTag loraTag;
    bool hasTag = packet->PeekPacketTag(loraTag);

    if (hasTag)
    {
        double rxPower = loraTag.GetReceivePower();
        m_lastRxRssi = rxPower;
        NS_LOG_INFO("Node " << (m_node ? m_node->GetId() : -1) << " Captured RX RSSI: " << rxPower
                            << " dBm");
    }
    else
    {
        NS_LOG_WARN("LoraTag NOT found, RSSI=default -120dBm");
        m_lastRxRssi = -120.0;
    }

    Ptr<Packet> pktCopy = packet->Copy();
    Ptr<Packet> pktForUpper = pktCopy->Copy();

    // Extraer (y remover) cabecera L2 antes de entregar a capas superiores.
    // Esto evita que el parser DV interprete bytes de header como payload de rutas.
    Address srcAddr = Address();
    DvClMacHeader macHdr;

    if (pktForUpper->GetSize() >= kBeaconProbeMinSize)
    {
        // Recover the logical src from the on-air beacon header and expose it
        // as the callback "from" address so the upper layer can learn the
        // nodeId<->MAC cache. Module wire contract: single DvClBeaconHeader.
        Ptr<Packet> hdrProbe = pktForUpper->Copy();
        DvClBeaconHeader beaconHdr;
        const uint32_t removed = hdrProbe->RemoveHeader(beaconHdr);
        if (removed == DvClBeaconHeader::kSerializedSize &&
            UnpackType(beaconHdr.GetFlagsTtl()) == DvClPacketType::BEACON)
        {
            srcAddr = Address(LogicalIdToPseudoMac48(beaconHdr.GetSrc()));
        }
    }
    DvClMetricTag rxTag;
    if (pktCopy->PeekPacketTag(rxTag))
    {
        // DEBUG: Log para TODOS los paquetes (beacons y datos)
        NS_LOG_INFO("NETDEV_RX_TAG node=" << (m_node ? m_node->GetId() : -1)
                                          << " src=" << rxTag.GetSrc() << " dst=" << rxTag.GetDst()
                                          << " seq=" << rxTag.GetSeq()
                                          << " size=" << pktCopy->GetSize() << " hasTag=true");
        if (rxTag.GetDst() == 0xFFFF)
        {
            NS_LOG_INFO("DVTRACE_RX_PHY time=" << Simulator::Now().GetSeconds()
                                               << " node=" << (m_node ? m_node->GetId() : -1)
                                               << " src=" << rxTag.GetSrc() << " seq="
                                               << rxTag.GetSeq() << " rssi=" << m_lastRxRssi
                                               << " size=" << pktCopy->GetSize());
        }
        else
        {
            // v1: filtro por expectedNextHop desde DvClMetricTag.
        }
        // Compute RX duration: use ToA from tag if available, else compute from packet
        double duration = rxTag.GetToaUs() / 1e6;
        if (duration <= 0.0)
        {
            // Fallback: compute from packet size and SF using LoraPhy::GetOnAirTime
            lorawan::LoraTxParameters rxParams;
            rxParams.sf = rxTag.GetSf();
            rxParams.bandwidthHz = 125000;
            rxParams.codingRate = 1;
            rxParams.crcEnabled = true;
            rxParams.headerDisabled = false;
            rxParams.nPreamble = m_preambleSymbols;
            Time airTime = lorawan::LoraPhy::GetOnAirTime(pktCopy, rxParams);
            duration = airTime.GetSeconds();
        }
        if (m_mac)
        {
            m_mac->NotifyRxStart(duration);
        }
        if (m_energyModel && m_node)
        {
            // Charge the reception for the frame's own duration. This callback
            // fires when the frame is already demodulated, so the debit is
            // shifted one frame late -- the honest approximation available at a
            // reception-complete hook, and the reason charge is event-driven
            // rather than a state machine that would leave the radio stuck in
            // RX (VALIDATION.md, 2026-07-22).
            m_energyModel->ChargeRx(duration);
        }
        Simulator::Schedule(Seconds(duration),
                            &DvClLoraNetDevice::NotifyRadioStateChange,
                            this,
                            3); // 3 = IDLE
    }

    if (!m_rxCallback.IsNull())
    {
        m_rxCallback(this, pktForUpper, 0x88B5, srcAddr);
    }
    else
    {
        NS_LOG_WARN("RxCallback es NULL!");
    }
}

// ======== Resto de interfaz NetDevice (boilerplate) ========

bool
DvClLoraNetDevice::SendFrom(Ptr<Packet> p,
                            [[maybe_unused]] const Address& src,
                            const Address& dest,
                            uint16_t proto)
{
    return Send(p, dest, proto);
}

void
DvClLoraNetDevice::SetIfIndex(const uint32_t index)
{
    m_ifIndex = index;
}

uint32_t
DvClLoraNetDevice::GetIfIndex() const
{
    return m_ifIndex;
}

Ptr<Channel>
DvClLoraNetDevice::GetChannel() const
{
    return m_phy ? m_phy->GetChannel() : nullptr;
}

void
DvClLoraNetDevice::SetAddress(Address address)
{
    m_address = Mac48Address::ConvertFrom(address);
}

Address
DvClLoraNetDevice::GetAddress() const
{
    return m_address;
}

bool
DvClLoraNetDevice::SetMtu(const uint16_t mtu)
{
    m_mtu = mtu;
    return true;
}

uint16_t
DvClLoraNetDevice::GetMtu() const
{
    // MTU dinámico según SF (EU868 DR limits)
    // Ref: LoRaWAN Regional Parameters EU868 Table 2
    //
    // SF12: 51 bytes max payload  (DR0)
    // SF11: 51 bytes              (DR1)
    // SF10: 51 bytes              (DR2)
    // SF9:  115 bytes             (DR3)
    // SF8:  222 bytes             (DR4)
    // SF7:  222 bytes             (DR5)

    // Tabla de MTU por SF (índice = SF - 7)
    static constexpr uint16_t kMtuBySf[] = {
        222, // SF7
        222, // SF8
        115, // SF9
        51,  // SF10
        51,  // SF11
        51   // SF12
    };

    // Intentar obtener SF del PHY
    if (m_phy)
    {
        // Usamos SimpleGatewayLoraPhy que no tiene GetSpreadingFactor() directo
        // pero podemos usar el SF del último paquete recibido via LoraTag
        // Por ahora, usamos un enfoque conservador: si tenemos PHY, usar SF mínimo seguro

        // Alternativa: Obtener del EndDeviceLoraPhy si está disponible
        // Try EndDeviceLoraPhy first (has GetSpreadingFactor)
        Ptr<EndDeviceLoraPhy> edPhy = DynamicCast<EndDeviceLoraPhy>(m_phy);
        if (edPhy)
        {
            uint8_t sf = edPhy->GetSpreadingFactor();
            if (sf >= 7 && sf <= 12)
            {
                return kMtuBySf[sf - 7];
            }
        }
        // GatewayLoraPhy doesn't have GetSpreadingFactor — use conservative default
        // (SF12 = 51 bytes) to avoid oversized packets
    }

    // Fallback: usar MTU configurado por usuario (conservador)
    return m_mtu;
}

bool
DvClLoraNetDevice::IsLinkUp() const
{
    return true;
}

void
DvClLoraNetDevice::AddLinkChangeCallback([[maybe_unused]] Callback<void> cb)
{
}

bool
DvClLoraNetDevice::IsBroadcast() const
{
    return true;
}

Address
DvClLoraNetDevice::GetBroadcast() const
{
    return Mac48Address("ff:ff:ff:ff:ff:ff");
}

bool
DvClLoraNetDevice::IsMulticast() const
{
    return false;
}

Address
DvClLoraNetDevice::GetMulticast([[maybe_unused]] Ipv4Address multicastGroup) const
{
    return GetBroadcast();
}

Address
DvClLoraNetDevice::GetMulticast([[maybe_unused]] Ipv6Address addr) const
{
    return GetBroadcast();
}

bool
DvClLoraNetDevice::IsPointToPoint() const
{
    return false;
}

bool
DvClLoraNetDevice::IsBridge() const
{
    return false;
}

void
DvClLoraNetDevice::SetNode(Ptr<Node> node)
{
    m_node = node;
}

Ptr<Node>
DvClLoraNetDevice::GetNode() const
{
    return m_node;
}

bool
DvClLoraNetDevice::NeedsArp() const
{
    return false;
}

void
DvClLoraNetDevice::SetReceiveCallback(NetDevice::ReceiveCallback cb)
{
    m_rxCallback = cb;
}

void
DvClLoraNetDevice::SetPromiscReceiveCallback(NetDevice::PromiscReceiveCallback cb)
{
    m_promiscRxCallback = cb;
}

bool
DvClLoraNetDevice::SupportsSendFrom() const
{
    return true;
}

double
DvClLoraNetDevice::GetLastRxRssi() const
{
    return m_lastRxRssi;
}

void
DvClLoraNetDevice::SetLastRxRssi(double rssi)
{
    m_lastRxRssi = rssi;
}

// ============================================================================
// NEW: ns-3 Energy Framework integration - NotifyRadioStateChange
// ============================================================================
lorawan::LoraTxParameters
DvClLoraNetDevice::BuildTxParams(uint8_t sf) const
{
    lorawan::LoraTxParameters p;
    p.sf = sf;
    p.headerDisabled = false;
    p.codingRate = m_region ? m_region->GetCodingRate() : 1;
    p.bandwidthHz = m_region ? m_region->GetBandwidthHz() : 125000;
    p.nPreamble = m_preambleSymbols;
    p.crcEnabled = true;
    // The specification mandates the optimisation once the symbol time passes
    // 16 ms, which at 125 kHz is SF11 and SF12. Leaving it off there put frames
    // on the air shorter than any real radio would send them.
    p.lowDataRateOptimizationEnabled = LowDataRateOptimizationRequired(sf, p.bandwidthHz);
    return p;
}

Time
DvClLoraNetDevice::GetOnAirTimeFor(Ptr<const Packet> packet, uint8_t sf) const
{
    if (!packet)
    {
        return Seconds(0);
    }
    const uint8_t sfUsed = (sf >= 7 && sf <= 12) ? sf : 9;
    return lorawan::LoraPhy::GetOnAirTime(packet->Copy(), BuildTxParams(sfUsed));
}

void
DvClLoraNetDevice::NotifyRadioStateChange(int newState)
{
    if (m_energyModel)
    {
        m_energyModel->ChangeState(newState);
    }
}

} // namespace dvcl
} // namespace ns3
