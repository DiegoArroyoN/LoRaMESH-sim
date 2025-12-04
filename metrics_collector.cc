#include "metrics_collector.h"
#include "ns3/log.h"
#include <iomanip>

NS_LOG_COMPONENT_DEFINE ("MetricsCollector");

namespace ns3 {

MetricsCollector::MetricsCollector ()
{
  NS_LOG_FUNCTION (this);
}

MetricsCollector::~MetricsCollector ()
{
  NS_LOG_FUNCTION (this);
}

void
MetricsCollector::RecordTx (uint32_t nodeId, uint32_t seq, uint32_t dst, uint8_t ttl,
                            uint8_t hops, int16_t rssi, uint16_t battery, uint16_t score, bool ok)
{
  TxEvent event;
  event.timestamp = Simulator::Now ();
  event.nodeId = nodeId;
  event.seq = seq;
  event.dst = dst;
  event.ttl = ttl;
  event.hops = hops;
  event.rssi = rssi;
  event.battery = battery;
  event.score = score;
  event.ok = ok;
  
  m_txEvents.push_back (event);
}

void
MetricsCollector::RecordRx (uint32_t nodeId, uint32_t src, uint32_t dst, uint32_t seq,
                            uint8_t ttl, uint8_t hops, int16_t rssi, uint16_t battery,
                            uint16_t score, bool isForwarded)
{
  RxEvent event;
  event.timestamp = Simulator::Now ();
  event.nodeId = nodeId;
  event.src = src;
  event.dst = dst;
  event.seq = seq;
  event.ttl = ttl;
  event.hops = hops;
  event.rssi = rssi;
  event.battery = battery;
  event.score = score;
  event.isForwarded = isForwarded;
  
  m_rxEvents.push_back (event);
}

void
MetricsCollector::RecordRoute (uint32_t nodeId, uint32_t destination, uint32_t nextHop,
                               uint8_t hops, uint16_t score, uint32_t seq, std::string action)
{
  RouteEvent event;
  event.timestamp = Simulator::Now ();
  event.nodeId = nodeId;
  event.destination = destination;
  event.nextHop = nextHop;
  event.hops = hops;
  event.score = score;
  event.seq = seq;
  event.action = action;
  
  m_routeEvents.push_back (event);
}

void
MetricsCollector::RecordE2eDelay (uint32_t src, uint32_t dst, uint32_t seq, uint8_t hops,
                                  double delaySec, uint32_t bytes, uint8_t sf, bool delivered)
{
  DelayEvent ev;
  ev.timestamp = Simulator::Now ();
  ev.src = src;
  ev.dst = dst;
  ev.seq = seq;
  ev.hops = hops;
  ev.delaySec = delaySec;
  ev.bytes = bytes;
  ev.sf = sf;
  ev.delivered = delivered;
  m_delayEvents.push_back (ev);
}

void
MetricsCollector::RecordOverhead (uint32_t nodeId, const std::string& kind, uint32_t bytes,
                                  uint32_t src, uint32_t dst, uint32_t seq, uint8_t hops, uint8_t sf)
{
  OverheadEvent ev;
  ev.timestamp = Simulator::Now ();
  ev.nodeId = nodeId;
  ev.kind = kind;
  ev.bytes = bytes;
  ev.src = src;
  ev.dst = dst;
  ev.seq = seq;
  ev.hops = hops;
  ev.sf = sf;
  m_overheadEvents.push_back (ev);
}

void
MetricsCollector::ExportTxCSV (std::string filename)
{
  std::ofstream file (filename);
  file << "timestamp(s),nodeId,seq,dst,ttl,hops,rssi(dBm),battery(mV),score,ok\n";
  
  for (const auto& event : m_txEvents)
  {
    file << std::fixed << std::setprecision(9) << event.timestamp.GetSeconds()
          << "," << event.nodeId
          << "," << event.seq
          << "," << event.dst
          << "," << (int)event.ttl
          << "," << (int)event.hops
          << "," << event.rssi
          << "," << event.battery
          << "," << event.score
          << "," << (event.ok ? 1 : 0) << "\n";
  }
  
  file.close ();
  NS_LOG_INFO ("TX CSV exportado a: " << filename);
}

void
MetricsCollector::ExportRxCSV (std::string filename)
{
  std::ofstream file (filename);
  file << "timestamp(s),nodeId,src,dst,seq,ttl,hops,rssi(dBm),battery(mV),score,forwarded\n";
  
  for (const auto& event : m_rxEvents)
  {
    file << std::fixed << std::setprecision(9) << event.timestamp.GetSeconds()
          << "," << event.nodeId
          << "," << event.src
          << "," << event.dst
          << "," << event.seq
          << "," << (int)event.ttl
          << "," << (int)event.hops
          << "," << event.rssi
          << "," << event.battery
          << "," << event.score
          << "," << (event.isForwarded ? 1 : 0) << "\n";
  }
  
  file.close ();
  NS_LOG_INFO ("RX CSV exportado a: " << filename);
}

void
MetricsCollector::ExportRouteCSV (std::string filename)
{
  std::ofstream file (filename);
  file << "timestamp(s),nodeId,destination,nextHop,hops,score,seq,action\n";
  
  for (const auto& event : m_routeEvents)
  {
    file << std::fixed << std::setprecision(9) << event.timestamp.GetSeconds()
          << "," << event.nodeId
          << "," << event.destination
          << "," << event.nextHop
          << "," << (int)event.hops
          << "," << event.score
          << "," << event.seq
          << "," << event.action << "\n";
  }
  
  file.close ();
  NS_LOG_INFO ("Route CSV exportado a: " << filename);
}

void
MetricsCollector::ExportDelayCSV (std::string filename)
{
  std::ofstream file (filename);
  file << "timestamp(s),src,dst,seq,hops,delay(s),bytes,sf,delivered\n";
  for (const auto& e : m_delayEvents)
  {
    file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds()
         << "," << e.src
         << "," << e.dst
         << "," << e.seq
         << "," << (int)e.hops
         << "," << e.delaySec
         << "," << e.bytes
         << "," << (int)e.sf
         << "," << (e.delivered ? 1 : 0) << "\n";
  }
  file.close ();
  NS_LOG_INFO ("Delay CSV exportado a: " << filename);
}

void
MetricsCollector::ExportOverheadCSV (std::string filename)
{
  std::ofstream file (filename);
  file << "timestamp(s),nodeId,kind,bytes,src,dst,seq,hops,sf\n";
  for (const auto& e : m_overheadEvents)
  {
    file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds()
         << "," << e.nodeId
         << "," << e.kind
         << "," << e.bytes
         << "," << e.src
         << "," << e.dst
         << "," << e.seq
         << "," << (int)e.hops
         << "," << (int)e.sf << "\n";
  }
  file.close ();
  NS_LOG_INFO ("Overhead CSV exportado a: " << filename);
}

void
MetricsCollector::ExportToCSV (std::string prefix)
{
  ExportTxCSV (prefix + "_tx.csv");
  ExportRxCSV (prefix + "_rx.csv");
  ExportRouteCSV (prefix + "_routes.csv");
  ExportDelayCSV (prefix + "_delay.csv");
  ExportOverheadCSV (prefix + "_overhead.csv");
  
  NS_LOG_INFO ("=== MÉTRICAS EXPORTADAS ===");
  NS_LOG_INFO ("TX events: " << m_txEvents.size ());
  NS_LOG_INFO ("RX events: " << m_rxEvents.size ());
  NS_LOG_INFO ("Route events: " << m_routeEvents.size ());
}

void
MetricsCollector::PrintStatistics ()
{
  NS_LOG_INFO ("=== ESTADÍSTICAS FINALES ===");
  NS_LOG_INFO ("Total TX: " << m_txEvents.size ());
  NS_LOG_INFO ("Total RX: " << m_rxEvents.size ());
  
  uint32_t txOk = 0;
  for (const auto& e : m_txEvents) if (e.ok) txOk++;
  double txRate = (m_txEvents.size() > 0) ? (100.0 * txOk / m_txEvents.size()) : 0.0;
  NS_LOG_INFO ("TX Success Rate: " << txRate << "%");
  
  uint32_t fwd = 0;
  for (const auto& e : m_rxEvents) if (e.isForwarded) fwd++;
  NS_LOG_INFO ("Forwarded packets: " << fwd << "/" << m_rxEvents.size());
  
  NS_LOG_INFO ("Route updates: " << m_routeEvents.size ());
  if (!m_delayEvents.empty ())
  {
    double acc = 0.0;
    for (const auto& e : m_delayEvents) acc += e.delaySec;
    NS_LOG_INFO ("Avg E2E delay: " << (acc / m_delayEvents.size ()) << " s");
  }
  NS_LOG_INFO ("Overhead events: " << m_overheadEvents.size ());
}

} // namespace ns3
