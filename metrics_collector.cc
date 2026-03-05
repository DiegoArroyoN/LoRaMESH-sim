#include "metrics_collector.h"

#include "ns3/log.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <numeric>
#include <tuple>

NS_LOG_COMPONENT_DEFINE("MetricsCollector");

namespace ns3
{

MetricsCollector* g_metricsCollector = nullptr;

namespace
{
double
Percentile95(std::vector<uint32_t> values)
{
    if (values.empty())
    {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const std::size_t idx = static_cast<std::size_t>(std::ceil(0.95 * values.size())) - 1;
    return static_cast<double>(values[std::min(idx, values.size() - 1)]);
}

double
Percentile95Double(std::vector<double> values)
{
    if (values.empty())
    {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const std::size_t idx = static_cast<std::size_t>(std::ceil(0.95 * values.size())) - 1;
    return values[std::min(idx, values.size() - 1)];
}

double
Percentile50Double(std::vector<double> values)
{
    if (values.empty())
    {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const std::size_t idx = static_cast<std::size_t>(std::ceil(0.50 * values.size())) - 1;
    return values[std::min(idx, values.size() - 1)];
}
} // namespace

MetricsCollector::MetricsCollector()
{
    NS_LOG_FUNCTION(this);
}

MetricsCollector::~MetricsCollector()
{
    NS_LOG_FUNCTION(this);
}

void
MetricsCollector::RecordTx(uint32_t nodeId,
                           uint32_t seq,
                           uint32_t dst,
                           uint8_t ttl,
                           uint8_t hops,
                           int16_t rssi,
                           uint16_t battery,
                           uint16_t score,
                           uint8_t sf,
                           double energyJ,
                           double energyFrac,
                           bool ok)
{
    TxEvent event;
    event.timestamp = Simulator::Now();
    event.nodeId = nodeId;
    event.seq = seq;
    event.dst = dst;
    event.ttl = ttl;
    event.hops = hops;
    event.rssi = rssi;
    event.battery = battery;
    event.score = score;
    event.sf = sf;
    event.energyJ = energyJ;
    event.energyFrac = energyFrac;
    event.ok = ok;

    m_txEvents.push_back(event);

    if (dst != 0xFFFF)
    {
        std::tuple<uint32_t, uint32_t, uint32_t> key{nodeId, dst, seq};
        if (m_firstTxTime.find(key) == m_firstTxTime.end())
        {
            m_firstTxTime[key] = event.timestamp.GetSeconds();
        }
    }
}

void
MetricsCollector::RecordRx(uint32_t nodeId,
                           uint32_t src,
                           uint32_t dst,
                           uint32_t seq,
                           uint8_t ttl,
                           uint8_t hops,
                           int16_t rssi,
                           uint16_t battery,
                           uint16_t score,
                           uint8_t sf,
                           double energyJ,
                           double energyFrac,
                           bool isForwarded)
{
    RxEvent event;
    event.timestamp = Simulator::Now();
    event.nodeId = nodeId;
    event.src = src;
    event.dst = dst;
    event.seq = seq;
    event.ttl = ttl;
    event.hops = hops;
    event.rssi = rssi;
    event.battery = battery;
    event.score = score;
    event.sf = sf;
    event.energyJ = energyJ;
    event.energyFrac = energyFrac;
    event.isForwarded = isForwarded;

    m_rxEvents.push_back(event);
}

void
MetricsCollector::RecordRoute(uint32_t nodeId,
                              uint32_t destination,
                              uint32_t nextHop,
                              uint8_t hops,
                              uint16_t score,
                              uint32_t seq,
                              std::string action)
{
    RouteEvent event;
    event.timestamp = Simulator::Now();
    event.nodeId = nodeId;
    event.destination = destination;
    event.nextHop = nextHop;
    event.hops = hops;
    event.score = score;
    event.seq = seq;
    event.action = action;

    m_routeEvents.push_back(event);

    if (action == "NEW")
    {
        m_routeNewEvents++;
    }
    else if (action == "UPDATE")
    {
        m_routeUpdateEvents++;
    }
    else if (action == "POISON")
    {
        m_routePoisonEvents++;
    }
    else if (action == "EXPIRE")
    {
        m_routeExpireEvents++;
    }
    else if (action == "PURGE")
    {
        m_routePurgeEvents++;
    }
}

void
MetricsCollector::RecordRouteUsed(uint32_t nodeId,
                                  uint32_t destination,
                                  uint32_t nextHop,
                                  uint8_t hops,
                                  uint16_t score,
                                  uint32_t seq)
{
    RouteEvent event;
    event.timestamp = Simulator::Now();
    event.nodeId = nodeId;
    event.destination = destination;
    event.nextHop = nextHop;
    event.hops = hops;
    event.score = score;
    event.seq = seq;
    event.action = "USED";
    m_routeUsedEvents.push_back(event);
    m_routeUsedEventsCount++;
}

void
MetricsCollector::RecordDataGenerated(uint32_t src, uint32_t dst, uint32_t seq)
{
    const auto key = std::make_tuple(src, dst, seq);
    m_generatedDataKeys.emplace(key);
    if (m_generatedDataTime.find(key) == m_generatedDataTime.end())
    {
        m_generatedDataTime[key] = Simulator::Now().GetSeconds();
    }
}

void
MetricsCollector::RecordDataPacket(uint32_t src, uint32_t seq, uint8_t hops, bool delivered)
{
    NS_LOG_DEBUG("RecordDataPacket (deprecated): src="
                 << src << " seq=" << seq << " hops=" << unsigned(hops)
                 << " delivered=" << (delivered ? 1 : 0));
}

void
MetricsCollector::RecordE2eDelay(uint32_t src,
                                 uint32_t dst,
                                 uint32_t seq,
                                 uint8_t hops,
                                 double delaySec,
                                 uint32_t bytes,
                                 uint8_t sf,
                                 bool delivered)
{
    DelayEvent ev;
    ev.timestamp = Simulator::Now();
    ev.src = src;
    ev.dst = dst;
    ev.seq = seq;
    ev.hops = hops;
    ev.delaySec = delaySec;
    ev.bytes = bytes;
    ev.sf = sf;
    ev.delivered = delivered;
    m_delayEvents.push_back(ev);
}

void
MetricsCollector::RecordOverhead(uint32_t nodeId,
                                 const std::string& kind,
                                 uint32_t bytes,
                                 uint32_t src,
                                 uint32_t dst,
                                 uint32_t seq,
                                 uint8_t hops,
                                 uint8_t sf)
{
    OverheadEvent ev;
    ev.timestamp = Simulator::Now();
    ev.nodeId = nodeId;
    ev.kind = kind;
    ev.bytes = bytes;
    ev.src = src;
    ev.dst = dst;
    ev.seq = seq;
    ev.hops = hops;
    ev.sf = sf;
    m_overheadEvents.push_back(ev);
}

void
MetricsCollector::RecordDuty(uint32_t nodeId,
                             double dutyUsed,
                             uint32_t txCount,
                             uint32_t backoffCount)
{
    DutyEvent ev;
    ev.nodeId = nodeId;
    ev.dutyUsed = dutyUsed;
    ev.txCount = txCount;
    ev.backoffCount = backoffCount;
    m_dutyEvents.push_back(ev);
}

void
MetricsCollector::RecordEnergySnapshot(uint32_t nodeId, double energyJ, double energyFrac)
{
    EnergySummary& s = m_energySummary[nodeId];
    if (s.initialJ < 0.0 && energyJ >= 0.0)
    {
        s.initialJ = energyJ;
    }
    if (energyJ >= 0.0)
    {
        s.remainingJ = energyJ;
    }
    if (energyFrac >= 0.0)
    {
        s.frac = energyFrac;
    }

    // Fallback robusto: si alguna instantánea ya reporta energía agotada,
    // registrar muerte de nodo (deduplicado internamente).
    const bool depletedByJ = (energyJ >= 0.0 && energyJ <= 1e-9);
    const bool depletedByFrac = (energyFrac >= 0.0 && energyFrac <= 1e-9);
    if (depletedByJ || depletedByFrac)
    {
        RecordNodeDeath(nodeId, energyFrac, "energy_snapshot_depleted");
    }
}

double
MetricsCollector::GetFirstTxTime(uint32_t src, uint32_t dst, uint32_t seq) const
{
    std::tuple<uint32_t, uint32_t, uint32_t> key{src, dst, seq};
    auto it = m_firstTxTime.find(key);
    if (it == m_firstTxTime.end())
    {
        return -1.0;
    }
    return it->second;
}

void
MetricsCollector::ExportTxCSV(std::string filename)
{
    // FIX: Usar append mode si ya se hizo flush periódico
    std::ios_base::openmode mode = m_appendMode ? std::ios::app : std::ios::out;
    std::ofstream file(filename, mode);

    if (!m_appendMode)
    {
        std::string header = "timestamp(s),nodeId,seq,dst,ttl,hops,rssi(dBm),battery(mV),score,sf,"
                             "energyJ,energyFrac,ok\n";
        file << header;
    }
    file << std::fixed << std::setprecision(9);

    for (const auto& event : m_txEvents)
    {
        file << event.timestamp.GetSeconds() << "," << event.nodeId << "," << event.seq << ","
             << event.dst << "," << (int)event.ttl << "," << (int)event.hops << "," << event.rssi
             << "," << event.battery << "," << event.score << "," << (int)event.sf << ","
             << event.energyJ << "," << event.energyFrac << "," << (event.ok ? 1 : 0) << "\n";
    }

    file.close();
    NS_LOG_INFO("TX CSV exportado a: " << filename);
}

void
MetricsCollector::ExportRxCSV(std::string filename)
{
    // FIX: Usar append mode si ya se hizo flush periódico
    std::ios_base::openmode mode = m_appendMode ? std::ios::app : std::ios::out;
    std::ofstream file(filename, mode);

    // Solo escribir header si es archivo nuevo (no append)
    if (!m_appendMode)
    {
        std::string header = "timestamp(s),nodeId,src,dst,seq,ttl,hops,rssi(dBm),battery(mV),score,"
                             "sf,energyJ,energyFrac,forwarded\n";
        file << header;
    }
    file << std::fixed << std::setprecision(9);

    for (const auto& event : m_rxEvents)
    {
        file << event.timestamp.GetSeconds() << "," << event.nodeId << "," << event.src << ","
             << event.dst << "," << event.seq << "," << (int)event.ttl << "," << (int)event.hops
             << "," << event.rssi << "," << event.battery << "," << event.score << ","
             << (int)event.sf << "," << event.energyJ << "," << event.energyFrac << ","
             << (event.isForwarded ? 1 : 0) << "\n";
    }

    file.close();
    NS_LOG_INFO("RX CSV exportado a: " << filename);
}

void
MetricsCollector::ExportRouteCSV(std::string filename)
{
    std::ofstream file(filename);
    file << "timestamp(s),nodeId,destination,nextHop,hops,score,seq,action\n";

    for (const auto& event : m_routeEvents)
    {
        file << std::fixed << std::setprecision(9) << event.timestamp.GetSeconds() << ","
             << event.nodeId << "," << event.destination << "," << event.nextHop << ","
             << (int)event.hops << "," << event.score << "," << event.seq << "," << event.action
             << "\n";
    }

    file.close();
    NS_LOG_INFO("Route CSV exportado a: " << filename);
}

void
MetricsCollector::ExportRouteUsedCSV(std::string filename)
{
    std::ofstream file(filename);
    file << "timestamp(s),nodeId,destination,nextHop,hops,score,seq,action\n";

    for (const auto& event : m_routeUsedEvents)
    {
        file << std::fixed << std::setprecision(9) << event.timestamp.GetSeconds() << ","
             << event.nodeId << "," << event.destination << "," << event.nextHop << ","
             << (int)event.hops << "," << event.score << "," << event.seq << "," << event.action
             << "\n";
    }

    file.close();
    NS_LOG_INFO("Route USED CSV exportado a: " << filename);
}

void
MetricsCollector::ExportDelayCSV(std::string filename)
{
    std::ofstream file(filename);
    file << "timestamp(s),src,dst,seq,hops,delay(s),bytes,sf,delivered\n";
    for (const auto& e : m_delayEvents)
    {
        file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds() << "," << e.src
             << "," << e.dst << "," << e.seq << "," << (int)e.hops << "," << e.delaySec << ","
             << e.bytes << "," << (int)e.sf << "," << (e.delivered ? 1 : 0) << "\n";
    }
    file.close();
    NS_LOG_INFO("Delay CSV exportado a: " << filename);
}

void
MetricsCollector::ExportOverheadCSV(std::string filename)
{
    std::ofstream file(filename);
    file << "timestamp(s),nodeId,kind,bytes,src,dst,seq,hops,sf\n";
    for (const auto& e : m_overheadEvents)
    {
        file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds() << "," << e.nodeId
             << "," << e.kind << "," << e.bytes << "," << e.src << "," << e.dst << "," << e.seq
             << "," << (int)e.hops << "," << (int)e.sf << "\n";
    }
    file.close();
    NS_LOG_INFO("Overhead CSV exportado a: " << filename);
}

void
MetricsCollector::ExportDutyCSV(std::string filename)
{
    std::ofstream file(filename);
    file << "nodeId,dutyUsed,txCount,backoffCount\n";
    for (const auto& e : m_dutyEvents)
    {
        file << e.nodeId << "," << std::fixed << std::setprecision(6) << e.dutyUsed << ","
             << e.txCount << "," << e.backoffCount << "\n";
    }
    file.close();
    NS_LOG_INFO("Duty CSV exportado a: " << filename);
}

void
MetricsCollector::ExportEnergyCSV(std::string filename)
{
    std::ofstream file(filename);
    file << "nodeId,energyInitialJ,energyConsumedJ,energyRemainingJ,energyFrac\n";
    for (const auto& kv : m_energySummary)
    {
        uint32_t node = kv.first;
        const EnergySummary& s = kv.second;
        double consumed =
            (s.initialJ >= 0.0 && s.remainingJ >= 0.0) ? (s.initialJ - s.remainingJ) : -1.0;
        file << node << "," << s.initialJ << "," << consumed << "," << s.remainingJ << "," << s.frac
             << "\n";
    }
    file.close();
    NS_LOG_INFO("Energy CSV exportado a: " << filename);
}

void
MetricsCollector::ExportToCSV(std::string prefix)
{
    ExportTxCSV(prefix + "_tx.csv");
    ExportRxCSV(prefix + "_rx.csv");
    ExportRouteCSV(prefix + "_routes.csv");
    ExportRouteUsedCSV(prefix + "_routes_used.csv");
    ExportDelayCSV(prefix + "_delay.csv");
    ExportOverheadCSV(prefix + "_overhead.csv");
    ExportDutyCSV(prefix + "_duty.csv");
    ExportEnergyCSV(prefix + "_energy.csv");
    ExportLifetimeCSV(prefix + "_lifetime.csv");

    NS_LOG_INFO("=== MÉTRICAS EXPORTADAS ===");
    NS_LOG_INFO("TX events: " << m_txEvents.size());
    NS_LOG_INFO("RX events: " << m_rxEvents.size());
    NS_LOG_INFO("Route events: " << m_routeEvents.size());
}

void
MetricsCollector::PrintStatistics()
{
    NS_LOG_INFO("=== ESTADÍSTICAS FINALES ===");
    NS_LOG_INFO("Total TX: " << m_txEvents.size());
    NS_LOG_INFO("Total RX: " << m_rxEvents.size());

    uint32_t txOk = 0;
    for (const auto& e : m_txEvents)
    {
        if (e.ok)
        {
            txOk++;
        }
    }
    double txRate = (m_txEvents.size() > 0) ? (100.0 * txOk / m_txEvents.size()) : 0.0;
    NS_LOG_INFO("TX Success Rate: " << txRate << "%");

    uint32_t fwd = 0;
    for (const auto& e : m_rxEvents)
    {
        if (e.isForwarded)
        {
            fwd++;
        }
    }
    NS_LOG_INFO("Forwarded packets: " << fwd << "/" << m_rxEvents.size());

    NS_LOG_INFO("Route updates: " << m_routeEvents.size());
    if (!m_delayEvents.empty())
    {
        double acc = 0.0;
        for (const auto& e : m_delayEvents)
        {
            acc += e.delaySec;
        }
        NS_LOG_INFO("Avg E2E delay: " << (acc / m_delayEvents.size()) << " s");
    }
    NS_LOG_INFO("Overhead events: " << m_overheadEvents.size());
}

// FIX D1: Métodos para flush periódico

void
MetricsCollector::StartPeriodicFlush(Time interval, std::string prefix)
{
    m_flushInterval = interval;
    m_csvPrefix = prefix;
    m_flushCount = 0;
    m_appendMode = false;

    NS_LOG_INFO("MetricsCollector: Starting periodic flush every " << interval.GetSeconds()
                                                                   << "s, prefix=" << prefix);

    ScheduleNextFlush();
}

void
MetricsCollector::StopPeriodicFlush()
{
    if (m_flushEvent.IsPending())
    {
        Simulator::Cancel(m_flushEvent);
    }
    NS_LOG_INFO("MetricsCollector: Stopped periodic flush. Total flushes: " << m_flushCount);
}

void
MetricsCollector::ScheduleNextFlush()
{
    m_flushEvent = Simulator::Schedule(m_flushInterval, &MetricsCollector::FlushToDisk, this);
}

void
MetricsCollector::FlushToDisk()
{
    m_flushCount++;

    // Log tamaños antes del flush
    NS_LOG_INFO("MetricsCollector: Flush #"
                << m_flushCount << " at t=" << Simulator::Now().GetSeconds() << "s"
                << " | TX=" << m_txEvents.size() << " RX=" << m_rxEvents.size()
                << " Routes=" << m_routeEvents.size() << " Delay=" << m_delayEvents.size()
                << " Overhead=" << m_overheadEvents.size());

    // Modo append para CSVs (después del primer flush)
    auto openMode = m_appendMode ? std::ios::app : std::ios::out;

    // Exportar TX
    {
        std::ofstream file(m_csvPrefix + "_tx.csv", openMode);
        if (!m_appendMode)
        {
            file << "timestamp(s),nodeId,seq,dst,ttl,hops,rssi(dBm),battery(mV),score,sf,energyJ,"
                    "energyFrac,ok\n";
        }
        file << std::fixed << std::setprecision(9);
        for (const auto& e : m_txEvents)
        {
            file << e.timestamp.GetSeconds() << "," << e.nodeId << "," << e.seq << "," << e.dst
                 << "," << (int)e.ttl << "," << (int)e.hops << "," << e.rssi << "," << e.battery
                 << "," << e.score << "," << (int)e.sf << "," << e.energyJ << "," << e.energyFrac
                 << "," << (e.ok ? 1 : 0) << "\n";
        }
    }

    // Exportar RX
    {
        std::ofstream file(m_csvPrefix + "_rx.csv", openMode);
        if (!m_appendMode)
        {
            file << "timestamp(s),nodeId,src,dst,seq,ttl,hops,rssi(dBm),battery(mV),score,sf,"
                    "energyJ,energyFrac,forwarded\n";
        }
        file << std::fixed << std::setprecision(9);
        for (const auto& e : m_rxEvents)
        {
            file << e.timestamp.GetSeconds() << "," << e.nodeId << "," << e.src << "," << e.dst
                 << "," << e.seq << "," << (int)e.ttl << "," << (int)e.hops << "," << e.rssi << ","
                 << e.battery << "," << e.score << "," << (int)e.sf << "," << e.energyJ << ","
                 << e.energyFrac << "," << (e.isForwarded ? 1 : 0) << "\n";
        }
    }

    // Exportar Routes
    {
        std::ofstream file(m_csvPrefix + "_routes.csv", openMode);
        if (!m_appendMode)
        {
            file << "timestamp(s),nodeId,destination,nextHop,hops,score,seq,action\n";
        }
        for (const auto& e : m_routeEvents)
        {
            file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds() << ","
                 << e.nodeId << "," << e.destination << "," << e.nextHop << "," << (int)e.hops
                 << "," << e.score << "," << e.seq << "," << e.action << "\n";
        }
    }

    // Exportar Routes Used
    {
        std::ofstream file(m_csvPrefix + "_routes_used.csv", openMode);
        if (!m_appendMode)
        {
            file << "timestamp(s),nodeId,destination,nextHop,hops,score,seq,action\n";
        }
        for (const auto& e : m_routeUsedEvents)
        {
            file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds() << ","
                 << e.nodeId << "," << e.destination << "," << e.nextHop << "," << (int)e.hops
                 << "," << e.score << "," << e.seq << "," << e.action << "\n";
        }
    }

    // Exportar Delay
    {
        std::ofstream file(m_csvPrefix + "_delay.csv", openMode);
        if (!m_appendMode)
        {
            file << "timestamp(s),src,dst,seq,hops,delay(s),bytes,sf,delivered\n";
        }
        for (const auto& e : m_delayEvents)
        {
            file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds() << "," << e.src
                 << "," << e.dst << "," << e.seq << "," << (int)e.hops << "," << e.delaySec << ","
                 << e.bytes << "," << (int)e.sf << "," << (e.delivered ? 1 : 0) << "\n";
        }
    }

    // Exportar Overhead
    {
        std::ofstream file(m_csvPrefix + "_overhead.csv", openMode);
        if (!m_appendMode)
        {
            file << "timestamp(s),nodeId,kind,bytes,src,dst,seq,hops,sf\n";
        }
        for (const auto& e : m_overheadEvents)
        {
            file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds() << ","
                 << e.nodeId << "," << e.kind << "," << e.bytes << "," << e.src << "," << e.dst
                 << "," << e.seq << "," << (int)e.hops << "," << (int)e.sf << "\n";
        }
    }

    // Limpiar vectores para liberar memoria
    m_txEvents.clear();
    m_rxEvents.clear();
    m_routeEvents.clear();
    m_routeUsedEvents.clear();
    m_delayEvents.clear();
    m_overheadEvents.clear();
    // Nota: m_dutyEvents y m_energySummary NO se limpian (son acumulativos por nodo)
    // Nota: m_firstTxTime NO se limpia (necesario para cálculo de delay E2E)

    // Después del primer flush, usar append mode
    m_appendMode = true;

    // Programar siguiente flush
    ScheduleNextFlush();
}

// ============================================================================
// THESIS METRICS: T50 and FND Implementation
// ============================================================================

void
MetricsCollector::RecordNodeDeath(uint32_t nodeId, double energyFrac, const std::string& reason)
{
    // Evitar duplicar eventos para el mismo nodo.
    if (m_deadNodes.find(nodeId) != m_deadNodes.end())
    {
        return;
    }

    NodeDeathEvent event;
    event.timestamp = Simulator::Now();
    event.nodeId = nodeId;
    event.energyFrac = energyFrac;
    event.reason = reason;
    m_nodeDeathEvents.push_back(event);
    m_deadNodes.insert(nodeId);

    // Update FND cache if this is the first death
    if (m_fndCached < 0.0)
    {
        m_fndCached = event.timestamp.GetSeconds();
        NS_LOG_INFO("FND (First Node Death): Node " << nodeId << " at t=" << m_fndCached
                                                    << "s, reason=" << reason);
    }

    // T50 energético: primer instante cuando nodos muertos >= ceil(0.5 * N)
    if (m_t50Cached < 0.0 && m_totalNodes > 0)
    {
        const uint32_t threshold =
            static_cast<uint32_t>(std::ceil(0.5 * static_cast<double>(m_totalNodes)));
        if (m_deadNodes.size() >= threshold)
        {
            m_t50Cached = event.timestamp.GetSeconds();
            NS_LOG_INFO("T50 reached (energy): " << m_deadNodes.size() << "/" << m_totalNodes
                                                 << " dead nodes at t=" << m_t50Cached << "s");
        }
    }
}

void
MetricsCollector::RecordConnectivity(uint32_t nodeId, uint32_t destination, bool hasRoute)
{
    ConnectivityEvent event;
    event.timestamp = Simulator::Now();
    event.nodeId = nodeId;
    event.destination = destination;
    event.hasRoute = hasRoute;
    m_connectivityEvents.push_back(event);
}

double
MetricsCollector::GetT50() const
{
    return m_t50Cached;
}

double
MetricsCollector::GetFND() const
{
    return m_fndCached;
}

void
MetricsCollector::ExportLifetimeCSV(std::string filename)
{
    std::ofstream file(filename);
    file << "metric,value_s\n";
    file << "fnd_s," << m_fndCached << "\n";
    file << "t50_s," << m_t50Cached << "\n";
    file << "dead_nodes," << m_deadNodes.size() << "\n";
    file << "\n";
    file << "timestamp(s),nodeId,energyFrac,reason\n";
    for (const auto& ev : m_nodeDeathEvents)
    {
        file << std::fixed << std::setprecision(9) << ev.timestamp.GetSeconds() << "," << ev.nodeId
             << "," << ev.energyFrac << "," << ev.reason << "\n";
    }
    file.close();
    NS_LOG_INFO("Lifetime CSV exportado a: " << filename);
}

void
MetricsCollector::ExportToJson(std::string prefix)
{
    std::string filename;
    if (prefix.size() >= 8 && prefix.compare(prefix.size() - 8, 8, "_summary") == 0)
    {
        filename = prefix + ".json";
    }
    else
    {
        filename = prefix + "_summary.json";
    }
    std::ofstream file(filename);

    // Calculate summary statistics
    const uint32_t totalTx = m_txEvents.size();
    uint32_t totalDataTxLegacy = 0;
    for (const auto& ev : m_txEvents)
    {
        if (ev.dst != 0xFFFF)
        {
            totalDataTxLegacy++;
        }
    }
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>> deliveredKeys;
    double totalDelay = 0.0;
    double minDelay = 1e9;
    double maxDelay = 0.0;
    std::vector<double> deliveredDelaySamples;

    for (const auto& e : m_delayEvents)
    {
        if (e.delivered)
        {
            deliveredKeys.emplace(e.src, e.dst, e.seq);
            totalDelay += e.delaySec;
            minDelay = std::min(minDelay, e.delaySec);
            maxDelay = std::max(maxDelay, e.delaySec);
            deliveredDelaySamples.push_back(e.delaySec);
        }
    }

    const uint32_t deliveredPackets = deliveredKeys.size();
    const uint32_t totalDataGenerated = m_generatedDataKeys.size();
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>> endWindowKeys;
    if (m_simulationStopSec > 0.0 && m_endWindowSec > 0.0)
    {
        const double cutoff = m_simulationStopSec - m_endWindowSec;
        for (const auto& kv : m_generatedDataTime)
        {
            if (kv.second >= cutoff)
            {
                endWindowKeys.insert(kv.first);
            }
        }
    }
    const uint32_t endWindowGenerated = static_cast<uint32_t>(endWindowKeys.size());
    const uint32_t totalDataGeneratedEligible =
        (totalDataGenerated >= endWindowGenerated) ? (totalDataGenerated - endWindowGenerated) : 0;
    uint32_t deliveredEligible = 0;
    for (const auto& key : deliveredKeys)
    {
        if (endWindowKeys.find(key) == endWindowKeys.end())
        {
            deliveredEligible++;
        }
    }
    const double avgDelay = deliveredPackets > 0 ? totalDelay / deliveredPackets : 0.0;
    const double p50Delay = Percentile50Double(deliveredDelaySamples);
    const double p95Delay = Percentile95Double(deliveredDelaySamples);
    const double pdr = totalDataGenerated > 0 ? static_cast<double>(deliveredPackets) / totalDataGenerated
                                              : 0.0;
    const double pdrEligible = totalDataGeneratedEligible > 0
                                   ? static_cast<double>(deliveredEligible) / totalDataGeneratedEligible
                                   : 0.0;
    const double legacyPdrTxBased =
        totalDataTxLegacy > 0 ? static_cast<double>(deliveredPackets) / totalDataTxLegacy : 0.0;

    std::map<uint32_t, uint32_t> generatedBySrc;
    for (const auto& k : m_generatedDataKeys)
    {
        generatedBySrc[std::get<0>(k)]++;
    }
    std::map<uint32_t, uint32_t> deliveredBySrc;
    std::map<uint32_t, uint32_t> deliveredByDst;
    for (const auto& k : deliveredKeys)
    {
        deliveredBySrc[std::get<0>(k)]++;
        deliveredByDst[std::get<1>(k)]++;
    }

    // Calculate energy stats
    double totalEnergyUsed = 0.0;
    double minEnergyFrac = 1.0;
    double maxEnergyFrac = 0.0;
    for (const auto& kv : m_energySummary)
    {
        if (kv.second.frac >= 0.0)
        {
            minEnergyFrac = std::min(minEnergyFrac, kv.second.frac);
            maxEnergyFrac = std::max(maxEnergyFrac, kv.second.frac);
            if (kv.second.initialJ > 0 && kv.second.remainingJ >= 0)
            {
                totalEnergyUsed += (kv.second.initialJ - kv.second.remainingJ);
            }
        }
    }

    // Count overhead
    uint32_t beaconBytes = 0;
    uint32_t dataBytes = 0;
    for (const auto& e : m_overheadEvents)
    {
        if (e.kind == "beacon")
        {
            beaconBytes += e.bytes;
        }
        else
        {
            dataBytes += e.bytes;
        }
    }

    // Aggregate runtime queue/CAD/duty/drop stats
    uint64_t txQueueLenEndTotal = 0;
    uint64_t queuedPacketsEnd = 0;
    uint64_t cadBusyEvents = 0;
    uint64_t dutyBlockedEvents = 0;
    double totalWaitTimeDueToDutyS = 0.0;
    uint64_t dropNoRoute = 0;
    uint64_t dropTtlExpired = 0;
    uint64_t dropQueueOverflow = 0;
    uint64_t dropBacktrack = 0;
    uint64_t dropOther = 0;
    uint64_t beaconScheduled = 0;
    uint64_t beaconTxSent = 0;
    uint64_t beaconBlockedByDuty = 0;
    uint64_t rpGapLargeEvents = 0;
    std::vector<uint32_t> queueLensPerNode;
    queueLensPerNode.reserve(m_runtimeStatsByNode.size());
    for (const auto& kv : m_runtimeStatsByNode)
    {
        const RuntimeNodeStats& s = kv.second;
        txQueueLenEndTotal += s.txQueueLenEnd;
        queuedPacketsEnd += s.queuedPacketsEnd;
        cadBusyEvents += s.cadBusyEvents;
        dutyBlockedEvents += s.dutyBlockedEvents;
        totalWaitTimeDueToDutyS += s.totalWaitTimeDueToDutyS;
        dropNoRoute += s.dropNoRoute;
        dropTtlExpired += s.dropTtlExpired;
        dropQueueOverflow += s.dropQueueOverflow;
        dropBacktrack += s.dropBacktrack;
        dropOther += s.dropOther;
        beaconScheduled += s.beaconScheduled;
        beaconTxSent += s.beaconTxSent;
        beaconBlockedByDuty += s.beaconBlockedByDuty;
        rpGapLargeEvents += s.rpGapLargeEvents;
        queueLensPerNode.push_back(s.txQueueLenEnd);
    }
    const double txQueueLenEndAvgNode =
        queueLensPerNode.empty()
            ? 0.0
            : static_cast<double>(txQueueLenEndTotal) / static_cast<double>(queueLensPerNode.size());
    const double txQueueLenEndP95Node = Percentile95(queueLensPerNode);
    uint32_t txQueueLenEndMaxNode = 0;
    for (uint32_t q : queueLensPerNode)
    {
        txQueueLenEndMaxNode = std::max(txQueueLenEndMaxNode, q);
    }
    const double beaconDelayMean =
        m_beaconDelaySamples.empty()
            ? 0.0
            : std::accumulate(m_beaconDelaySamples.begin(), m_beaconDelaySamples.end(), 0.0) /
                  static_cast<double>(m_beaconDelaySamples.size());
    const double beaconDelayP95 = Percentile95Double(m_beaconDelaySamples);

    uint32_t sourceFirstTxCount = 0;
    for (const auto& key : m_generatedDataKeys)
    {
        if (m_firstTxTime.find(key) != m_firstTxTime.end())
        {
            sourceFirstTxCount++;
        }
    }
    const double txAttemptsPerGenerated =
        totalDataGenerated > 0 ? static_cast<double>(totalDataTxLegacy) / totalDataGenerated : 0.0;
    const double sourceFirstTxRatio =
        totalDataGenerated > 0 ? static_cast<double>(sourceFirstTxCount) / totalDataGenerated : 0.0;
    const double deliveredPerTxAttempt =
        totalDataTxLegacy > 0 ? static_cast<double>(deliveredPackets) / totalDataTxLegacy : 0.0;
    const double deliveryRatio =
        totalDataGenerated > 0 ? static_cast<double>(deliveredPackets) / totalDataGenerated : 0.0;

    // Write JSON
    file << "{\n";
    file << "  \"simulation\": {\n";
    file << "    \"prefix\": \"" << prefix << "\",\n";
    file << "    \"timestamp\": \"" << Simulator::Now().GetSeconds() << "s\",\n";
    if (m_hasRunConfig)
    {
        file << "    \"n_nodes\": " << m_runConfig.nNodes << ",\n";
        file << "    \"sim_version\": \"" << m_runConfig.simVersion << "\",\n";
        file << "    \"topology\": \"" << m_runConfig.topology << "\",\n";
        file << "    \"area_w_m\": " << m_runConfig.areaWidthM << ",\n";
        file << "    \"area_h_m\": " << m_runConfig.areaHeightM << ",\n";
        file << "    \"rng_run\": " << m_runConfig.rngRun << ",\n";
        file << "    \"enable_csma\": " << (m_runConfig.enableCsma ? "true" : "false") << ",\n";
        file << "    \"enable_duty\": " << (m_runConfig.enableDuty ? "true" : "false") << ",\n";
        file << "    \"duty_limit\": " << m_runConfig.dutyLimit << ",\n";
        file << "    \"duty_window_sec\": " << m_runConfig.dutyWindowSec << ",\n";
        file << "    \"data_start_sec\": " << m_runConfig.dataStartSec << ",\n";
        file << "    \"data_stop_sec\": " << m_runConfig.dataStopSec << ",\n";
        file << "    \"stop_sec\": " << m_runConfig.stopSec << ",\n";
        file << "    \"pdr_end_window_sec\": " << m_runConfig.pdrEndWindowSec << ",\n";
        file << "    \"traffic_load\": \"" << m_runConfig.trafficLoad << "\",\n";
        file << "    \"traffic_interval_s\": " << m_runConfig.trafficIntervalS << ",\n";
        file << "    \"payload_bytes\": " << m_runConfig.payloadBytes << ",\n";
        file << "    \"dedup_window_sec\": " << m_runConfig.dedupWindowSec << ",\n";
        file << "    \"beacon_interval_warm_s\": " << m_runConfig.beaconIntervalWarmS << ",\n";
        file << "    \"beacon_interval_stable_s\": " << m_runConfig.beaconIntervalStableS << ",\n";
        file << "    \"route_timeout_factor\": " << m_runConfig.routeTimeoutFactor << ",\n";
        file << "    \"route_timeout_sec\": " << m_runConfig.routeTimeoutSec << ",\n";
        file << "    \"interference_model\": \"" << m_runConfig.interferenceModel << "\",\n";
        file << "    \"prop_model\": \"" << m_runConfig.propModel << "\",\n";
        file << "    \"tx_power_dbm\": " << m_runConfig.txPowerDbm << ",\n";
        file << "    \"channel_count\": " << m_runConfig.channelCount << ",\n";
    }
    file << "    \"wire_format\": \"" << m_wireFormat << "\",\n";
    file << "    \"data_header_bytes\": " << m_dataHeaderBytes << ",\n";
    file << "    \"beacon_header_bytes\": " << m_beaconHeaderBytes << ",\n";
    file << "    \"dv_entry_bytes\": " << m_dvEntryBytes << "\n";
    file << "  },\n";
    file << "  \"pdr\": {\n";
    file << "    \"definition\": \"e2e_src_to_final_dst\",\n";
    file << "    \"total_tx\": " << totalTx << ",\n";
    file << "    \"total_data_generated\": " << totalDataGenerated << ",\n";
    file << "    \"total_data_tx\": " << totalDataGenerated << ",\n";
    file << "    \"delivered\": " << deliveredPackets << ",\n";
    file << "    \"pdr\": " << std::fixed << std::setprecision(4) << pdr << ",\n";
    file << "    \"delivery_ratio\": " << std::fixed << std::setprecision(4) << deliveryRatio << ",\n";
    file << "    \"total_data_generated_eligible\": " << totalDataGeneratedEligible << ",\n";
    file << "    \"delivered_eligible\": " << deliveredEligible << ",\n";
    file << "    \"pdr_e2e_generated_eligible\": " << std::fixed << std::setprecision(4)
         << pdrEligible << ",\n";
    file << "    \"end_window_generated\": " << endWindowGenerated << ",\n";
    file << "    \"end_window_sec\": " << std::fixed << std::setprecision(3) << m_endWindowSec
         << ",\n";
    file << "    \"tx_attempts_per_generated\": " << std::fixed << std::setprecision(4)
         << txAttemptsPerGenerated << ",\n";
    file << "    \"admission_ratio\": " << std::fixed << std::setprecision(4)
         << txAttemptsPerGenerated << ",\n";
    file << "    \"source_first_tx_count\": " << sourceFirstTxCount << ",\n";
    file << "    \"source_first_tx_ratio\": " << std::fixed << std::setprecision(4)
         << sourceFirstTxRatio << ",\n";
    file << "    \"source_admission_ratio\": " << std::fixed << std::setprecision(4)
         << sourceFirstTxRatio << ",\n";
    file << "    \"delivered_per_tx_attempt\": " << std::fixed << std::setprecision(4)
         << deliveredPerTxAttempt << ",\n";
    file << "    \"legacy_total_data_tx_attempts\": " << totalDataTxLegacy << ",\n";
    file << "    \"legacy_pdr_tx_based\": " << std::fixed << std::setprecision(4) << legacyPdrTxBased
         << "\n";
    file << "  },\n";
    file << "  \"pdr_by_source\": [\n";
    bool firstSrc = true;
    for (const auto& kv : generatedBySrc)
    {
        const uint32_t src = kv.first;
        const uint32_t generated = kv.second;
        const uint32_t delivered = deliveredBySrc[src];
        const double srcPdr = generated > 0 ? static_cast<double>(delivered) / generated : 0.0;
        file << (firstSrc ? "" : ",\n");
        file << "    {\"src\": " << src << ", \"generated\": " << generated
             << ", \"delivered\": " << delivered << ", \"pdr\": " << std::fixed
             << std::setprecision(4) << srcPdr << "}";
        firstSrc = false;
    }
    if (!generatedBySrc.empty())
    {
        file << "\n";
    }
    file << "  ],\n";
    file << "  \"delivery_by_destination\": [\n";
    bool firstDst = true;
    for (const auto& kv : deliveredByDst)
    {
        file << (firstDst ? "" : ",\n");
        file << "    {\"dst\": " << kv.first << ", \"delivered\": " << kv.second << "}";
        firstDst = false;
    }
    if (!deliveredByDst.empty())
    {
        file << "\n";
    }
    file << "  ],\n";
    file << "  \"tx_attempts\": {\n";
    file << "    \"total_data_tx_attempts\": " << totalDataTxLegacy << ",\n";
    file << "    \"attempts_per_generated\": " << txAttemptsPerGenerated << ",\n";
    file << "    \"tx_attempts_per_generated\": " << txAttemptsPerGenerated << ",\n";
    file << "    \"admission_ratio\": " << txAttemptsPerGenerated << ",\n";
    file << "    \"source_first_tx_count\": " << sourceFirstTxCount << ",\n";
    file << "    \"source_first_tx_ratio\": " << sourceFirstTxRatio << ",\n";
    file << "    \"source_admission_ratio\": " << sourceFirstTxRatio << ",\n";
    file << "    \"delivered_per_tx_attempt\": " << deliveredPerTxAttempt << "\n";
    file << "  },\n";
    file << "  \"delay\": {\n";
    file << "    \"avg_s\": " << std::fixed << std::setprecision(6) << avgDelay << ",\n";
    file << "    \"p50_s\": " << std::fixed << std::setprecision(6) << p50Delay << ",\n";
    file << "    \"p95_s\": " << std::fixed << std::setprecision(6) << p95Delay << ",\n";
    file << "    \"min_s\": " << (minDelay < 1e9 ? minDelay : 0.0) << ",\n";
    file << "    \"max_s\": " << maxDelay << "\n";
    file << "  },\n";
    file << "  \"energy\": {\n";
    file << "    \"total_used_j\": " << std::fixed << std::setprecision(4) << totalEnergyUsed
         << ",\n";
    file << "    \"min_remaining_frac\": " << minEnergyFrac << ",\n";
    file << "    \"max_remaining_frac\": " << maxEnergyFrac << "\n";
    file << "  },\n";
    file << "  \"overhead\": {\n";
    file << "    \"beacon_bytes\": " << beaconBytes << ",\n";
    file << "    \"data_bytes\": " << dataBytes << ",\n";
    file << "    \"ratio\": " << (dataBytes > 0 ? (double)beaconBytes / dataBytes : 0.0) << "\n";
    file << "  },\n";
    file << "  \"routes\": {\n";
    file << "    \"total_events\": " << (m_routeNewEvents + m_routeUpdateEvents + m_routePoisonEvents +
                                         m_routeExpireEvents + m_routePurgeEvents)
         << ",\n";
    file << "    \"new_events\": " << m_routeNewEvents << ",\n";
    file << "    \"update_events\": " << m_routeUpdateEvents << ",\n";
    file << "    \"dv_route_poison_events\": " << m_routePoisonEvents << ",\n";
    file << "    \"dv_route_expire_events\": " << m_routeExpireEvents << ",\n";
    file << "    \"dv_route_purge_events\": " << m_routePurgeEvents << ",\n";
    file << "    \"used_events\": " << m_routeUsedEventsCount << "\n";
    file << "  },\n";
    file << "  \"control_plane\": {\n";
    file << "    \"beacon_scheduled\": " << beaconScheduled << ",\n";
    file << "    \"beacon_tx_sent\": " << beaconTxSent << ",\n";
    file << "    \"beacon_blocked_by_duty\": " << beaconBlockedByDuty << ",\n";
    file << "    \"rp_gap_large_events\": " << rpGapLargeEvents << ",\n";
    file << "    \"beacon_delay_s_mean\": " << std::fixed << std::setprecision(6)
         << beaconDelayMean << ",\n";
    file << "    \"beacon_delay_s_p95\": " << std::fixed << std::setprecision(6) << beaconDelayP95
         << ",\n";
    file << "    \"beacon_delay_samples\": " << m_beaconDelaySamples.size() << "\n";
    file << "  },\n";
    file << "  \"queue_backlog\": {\n";
    file << "    \"txQueue_len_end_total\": " << txQueueLenEndTotal << ",\n";
    file << "    \"queued_packets_end\": " << queuedPacketsEnd << ",\n";
    file << "    \"txQueue_len_end_avg_node\": " << std::fixed << std::setprecision(4)
         << txQueueLenEndAvgNode << ",\n";
    file << "    \"txQueue_len_end_p95_node\": " << std::fixed << std::setprecision(4)
         << txQueueLenEndP95Node << ",\n";
    file << "    \"txQueue_len_end_max_node\": " << txQueueLenEndMaxNode << ",\n";
    file << "    \"cad_busy_events\": " << cadBusyEvents << ",\n";
    file << "    \"duty_blocked_events\": " << dutyBlockedEvents << ",\n";
    file << "    \"total_wait_time_due_to_duty_s\": " << std::fixed << std::setprecision(6)
         << totalWaitTimeDueToDutyS << "\n";
    file << "  },\n";
    file << "  \"drops\": {\n";
    file << "    \"drop_no_route\": " << dropNoRoute << ",\n";
    file << "    \"drop_ttl_expired\": " << dropTtlExpired << ",\n";
    file << "    \"drop_queue_overflow\": " << dropQueueOverflow << ",\n";
    file << "    \"drop_backtrack\": " << dropBacktrack << ",\n";
    file << "    \"drop_other\": " << dropOther << "\n";
    file << "  },\n";
    file << "  \"thesis_metrics\": {\n";
    file << "    \"t50_s\": " << m_t50Cached << ",\n";
    file << "    \"fnd_s\": " << m_fndCached << "\n";
    file << "  }\n";
    file << "}\n";

    file.close();
    NS_LOG_INFO("JSON summary exported to: " << filename);
}

} // namespace ns3
