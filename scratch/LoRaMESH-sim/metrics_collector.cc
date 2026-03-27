#include "metrics_collector.h"

#include "ns3/log.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
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
                           uint32_t src,
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
    event.src = src;
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

    if (!m_essentialMetricsOnly)
    {
        m_rxEvents.push_back(event);
    }
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

    if (!m_essentialMetricsOnly)
    {
        m_routeEvents.push_back(event);
    }

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
    if (!m_essentialMetricsOnly)
    {
        m_routeUsedEvents.push_back(event);
    }
    m_routeUsedEventsCount++;
}

void
MetricsCollector::RecordQuantizationSample(uint32_t nodeId,
                                           uint32_t destination,
                                           uint32_t nextHop,
                                           bool isBackup,
                                           double rawMetric,
                                           uint16_t scoreQuantized)
{
    QuantizationSample sample;
    sample.nodeId = nodeId;
    sample.destination = destination;
    sample.nextHop = nextHop;
    sample.isBackup = isBackup;
    sample.rawMetric = rawMetric;
    sample.rawMetricMilli =
        static_cast<uint64_t>(std::llround(std::max(0.0, rawMetric) * 1000.0));
    sample.scoreQuantized = scoreQuantized;
    if (!m_essentialMetricsOnly)
    {
        m_quantizationSamples.push_back(sample);
    }
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
                                 double delayFirstTxSec,
                                 uint32_t bytes,
                                 uint8_t sf,
                                 bool delivered)
{
    const double nowSec = Simulator::Now().GetSeconds();
    const auto key = std::make_tuple(src, dst, seq);
    double delayGenSec = -1.0;
    auto itGen = m_generatedDataTime.find(key);
    if (itGen != m_generatedDataTime.end())
    {
        delayGenSec = nowSec - itGen->second;
    }

    DelayEvent ev;
    ev.timestamp = Simulator::Now();
    ev.src = src;
    ev.dst = dst;
    ev.seq = seq;
    ev.hops = hops;
    ev.delayFirstTxSec = delayFirstTxSec;
    ev.delayGenSec = delayGenSec;
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
    if (!m_essentialMetricsOnly)
    {
        m_overheadEvents.push_back(ev);
    }
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
        std::string header = "timestamp(s),nodeId,src,seq,dst,ttl,hops,rssi(dBm),battery(mV),score,sf,"
                             "energyJ,energyFrac,forwarded,ok\n";
        file << header;
    }
    file << std::fixed << std::setprecision(9);

    for (const auto& event : m_txEvents)
    {
        file << event.timestamp.GetSeconds() << "," << event.nodeId << "," << event.src << ","
             << event.seq << "," << event.dst << "," << (int)event.ttl << "," << (int)event.hops
             << "," << event.rssi << "," << event.battery << "," << event.score << ","
             << (int)event.sf << "," << event.energyJ << "," << event.energyFrac << ","
             << ((event.nodeId != event.src) ? 1 : 0) << "," << (event.ok ? 1 : 0) << "\n";
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
    file << "timestamp(s),src,dst,seq,hops,delay_gen_to_rx(s),delay_first_tx_to_rx(s),bytes,sf,delivered\n";
    for (const auto& e : m_delayEvents)
    {
        file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds() << "," << e.src
             << "," << e.dst << "," << e.seq << "," << (int)e.hops << "," << e.delayGenSec << ","
             << e.delayFirstTxSec << "," << e.bytes << "," << (int)e.sf << ","
             << (e.delivered ? 1 : 0) << "\n";
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
    ExportDelayCSV(prefix + "_delay.csv");
    if (!m_essentialMetricsOnly)
    {
        ExportRxCSV(prefix + "_rx.csv");
        ExportRouteCSV(prefix + "_routes.csv");
        ExportRouteUsedCSV(prefix + "_routes_used.csv");
        ExportOverheadCSV(prefix + "_overhead.csv");
    }
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
        uint32_t n = 0;
        for (const auto& e : m_delayEvents)
        {
            if (e.delayGenSec >= 0.0)
            {
                acc += e.delayGenSec;
                n++;
            }
        }
        NS_LOG_INFO("Avg E2E delay (gen->rx): " << (n > 0 ? (acc / n) : 0.0) << " s");
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
            file << "timestamp(s),nodeId,src,seq,dst,ttl,hops,rssi(dBm),battery(mV),score,sf,"
                    "energyJ,energyFrac,ok\n";
        }
        file << std::fixed << std::setprecision(9);
        for (const auto& e : m_txEvents)
        {
            file << e.timestamp.GetSeconds() << "," << e.nodeId << "," << e.src << "," << e.seq
                 << "," << e.dst << "," << (int)e.ttl << "," << (int)e.hops << "," << e.rssi
                 << "," << e.battery << "," << e.score << "," << (int)e.sf << "," << e.energyJ
                 << "," << e.energyFrac << "," << (e.ok ? 1 : 0) << "\n";
        }
    }

    // Exportar RX
    if (!m_essentialMetricsOnly)
    {
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
                     << "," << e.seq << "," << (int)e.ttl << "," << (int)e.hops << "," << e.rssi
                     << "," << e.battery << "," << e.score << "," << (int)e.sf << "," << e.energyJ
                     << "," << e.energyFrac << "," << (e.isForwarded ? 1 : 0) << "\n";
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
    }

    // Exportar Delay
    {
        std::ofstream file(m_csvPrefix + "_delay.csv", openMode);
        if (!m_appendMode)
        {
            file << "timestamp(s),src,dst,seq,hops,delay_gen_to_rx(s),delay_first_tx_to_rx(s),bytes,sf,delivered\n";
        }
        for (const auto& e : m_delayEvents)
        {
            file << std::fixed << std::setprecision(9) << e.timestamp.GetSeconds() << "," << e.src
                 << "," << e.dst << "," << e.seq << "," << (int)e.hops << "," << e.delayGenSec << ","
                 << e.delayFirstTxSec << "," << e.bytes << "," << (int)e.sf << ","
                 << (e.delivered ? 1 : 0) << "\n";
        }
    }

    // Exportar Overhead
    if (!m_essentialMetricsOnly)
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
    uint32_t forwardTxSentTotal = 0;
    uint32_t sourceTxSentTotal = 0;
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>> forwardedUniqueKeys;
    for (const auto& ev : m_txEvents)
    {
        if (ev.dst != 0xFFFF)
        {
            totalDataTxLegacy++;
            if (ev.nodeId != ev.src)
            {
                forwardTxSentTotal++;
                forwardedUniqueKeys.emplace(ev.src, ev.dst, ev.seq);
            }
            else
            {
                sourceTxSentTotal++;
            }
        }
    }
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>> deliveredKeys;
    double totalDelayGen = 0.0;
    double minDelayGen = 1e9;
    double maxDelayGen = 0.0;
    std::vector<double> deliveredDelayGenSamples;
    std::vector<double> deliveredDelayFirstTxSamples;

    for (const auto& e : m_delayEvents)
    {
        if (e.delivered)
        {
            deliveredKeys.emplace(e.src, e.dst, e.seq);
            if (e.delayGenSec >= 0.0)
            {
                totalDelayGen += e.delayGenSec;
                minDelayGen = std::min(minDelayGen, e.delayGenSec);
                maxDelayGen = std::max(maxDelayGen, e.delayGenSec);
                deliveredDelayGenSamples.push_back(e.delayGenSec);
            }
            if (e.delayFirstTxSec >= 0.0)
            {
                deliveredDelayFirstTxSamples.push_back(e.delayFirstTxSec);
            }
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
    const double avgDelayGen =
        deliveredDelayGenSamples.empty()
            ? 0.0
            : totalDelayGen / static_cast<double>(deliveredDelayGenSamples.size());
    const double p50DelayGen = Percentile50Double(deliveredDelayGenSamples);
    const double p95DelayGen = Percentile95Double(deliveredDelayGenSamples);
    std::vector<double> deliveredHopSamples;
    deliveredHopSamples.reserve(deliveredKeys.size());
    for (const auto& e : m_delayEvents)
    {
        if (e.delivered)
        {
            deliveredHopSamples.push_back(static_cast<double>(e.hops));
        }
    }
    const double avgHopsDelivered =
        deliveredHopSamples.empty()
            ? 0.0
            : std::accumulate(deliveredHopSamples.begin(), deliveredHopSamples.end(), 0.0) /
                  static_cast<double>(deliveredHopSamples.size());
    const double p95HopsDelivered = Percentile95Double(deliveredHopSamples);
    const double avgDelayFirstTx =
        deliveredDelayFirstTxSamples.empty()
            ? 0.0
            : std::accumulate(deliveredDelayFirstTxSamples.begin(),
                              deliveredDelayFirstTxSamples.end(),
                              0.0) /
                  static_cast<double>(deliveredDelayFirstTxSamples.size());
    const double p50DelayFirstTx = Percentile50Double(deliveredDelayFirstTxSamples);
    const double p95DelayFirstTx = Percentile95Double(deliveredDelayFirstTxSamples);
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
    uint64_t dutyBlockedControl = 0;
    uint64_t dutyBlockedData = 0;
    double totalWaitTimeDueToDutyS = 0.0;
    uint64_t dropNoRoute = 0;
    uint64_t dropNoRouteSrc = 0;
    uint64_t dropNoRouteRelay = 0;
    uint64_t dropTtlExpired = 0;
    uint64_t dropQueueOverflow = 0;
    uint64_t dropBacktrack = 0;
    uint64_t dropOther = 0;
    uint64_t beaconScheduled = 0;
    uint64_t beaconTxSent = 0;
    uint64_t dataTxSent = 0;
    uint64_t beaconBlockedByDuty = 0;
    uint64_t beaconSupersededLatestOnly = 0;
    uint64_t rpGapLargeEvents = 0;
    uint64_t cadBusyEventsLocalPower = 0;
    uint64_t cadBusyEventsOracle = 0;
    uint64_t rxScanAttempts = 0;
    uint64_t rxScanLocks = 0;
    uint64_t rxScanMissBeforeLock = 0;
    uint64_t rxPostLockInterferenceFail = 0;
    uint64_t rxNoMoreDemodDrops = 0;
    uint64_t pueyoSameSfOverlapEvents = 0;
    uint64_t pueyoDestructiveOverlapDrops = 0;
    uint64_t pueyoCaptureOrTimingSurvivals = 0;
    uint64_t pueyoCrossSfIgnoredOverlaps = 0;
    uint64_t goursaudDeterministicDrops = 0;
    uint64_t goursaudCrossSfCaptureSuccesses = 0;
    uint64_t goursaudCrossSfCaptureFails = 0;
    uint64_t beaconRxOk = 0;
    double rxScanTimeTotalS = 0.0;
    uint64_t sfLinkSamples = 0;
    uint64_t sfLinkObservedMismatch = 0;
    uint64_t routesTotal = 0;
    uint64_t routesPrimaryTotal = 0;
    uint64_t routesBackupTotal = 0;
    uint64_t destinationsWithBackupTotal = 0;
    uint64_t routeEvictionsTotal = 0;
    uint64_t backupPromotionsTotal = 0;
    std::vector<double> rxAcquisitionDelayMeanSamples;
    std::vector<double> rxAcquisitionDelayP95Samples;
    std::vector<double> firstUsableRouteTimeSamples;
    std::vector<double> coverage80RouteTimeSamples;
    std::map<uint32_t, double> firstUsableRouteByNode;
    uint64_t routesAtDataStart = 0;
    uint64_t beaconTxAtDataStart = 0;
    uint64_t beaconRxAtDataStart = 0;
    uint64_t routesAtMidpoint = 0;
    uint64_t beaconTxAtMidpoint = 0;
    uint64_t beaconRxAtMidpoint = 0;
    uint64_t routesAtDataStop = 0;
    uint64_t beaconTxAtDataStop = 0;
    uint64_t beaconRxAtDataStop = 0;
    uint64_t queuedPacketsAtDataStop = 0;
    std::vector<uint32_t> queueLensPerNode;
    queueLensPerNode.reserve(m_runtimeStatsByNode.size());
    for (const auto& kv : m_runtimeStatsByNode)
    {
        const RuntimeNodeStats& s = kv.second;
        txQueueLenEndTotal += s.txQueueLenEnd;
        queuedPacketsEnd += s.queuedPacketsEnd;
        cadBusyEvents += s.cadBusyEvents;
        dutyBlockedEvents += s.dutyBlockedEvents;
        dutyBlockedControl += s.dutyBlockedControl;
        dutyBlockedData += s.dutyBlockedData;
        totalWaitTimeDueToDutyS += s.totalWaitTimeDueToDutyS;
        dropNoRoute += s.dropNoRoute;
        dropNoRouteSrc += s.dropNoRouteSrc;
        dropNoRouteRelay += s.dropNoRouteRelay;
        dropTtlExpired += s.dropTtlExpired;
        dropQueueOverflow += s.dropQueueOverflow;
        dropBacktrack += s.dropBacktrack;
        dropOther += s.dropOther;
        beaconScheduled += s.beaconScheduled;
        beaconTxSent += s.beaconTxSent;
        dataTxSent += s.dataTxSent;
        beaconBlockedByDuty += s.beaconBlockedByDuty;
        beaconSupersededLatestOnly += s.beaconSupersededLatestOnly;
        rpGapLargeEvents += s.rpGapLargeEvents;
        cadBusyEventsLocalPower += s.cadBusyEventsLocalPower;
        cadBusyEventsOracle += s.cadBusyEventsOracle;
        rxScanAttempts += s.rxScanAttempts;
        rxScanLocks += s.rxScanLocks;
        rxScanMissBeforeLock += s.rxScanMissBeforeLock;
        rxPostLockInterferenceFail += s.rxPostLockInterferenceFail;
        rxNoMoreDemodDrops += s.rxNoMoreDemodDrops;
        pueyoSameSfOverlapEvents += s.pueyoSameSfOverlapEvents;
        pueyoDestructiveOverlapDrops += s.pueyoDestructiveOverlapDrops;
        pueyoCaptureOrTimingSurvivals += s.pueyoCaptureOrTimingSurvivals;
        pueyoCrossSfIgnoredOverlaps += s.pueyoCrossSfIgnoredOverlaps;
        goursaudDeterministicDrops += s.goursaudDeterministicDrops;
        goursaudCrossSfCaptureSuccesses += s.goursaudCrossSfCaptureSuccesses;
        goursaudCrossSfCaptureFails += s.goursaudCrossSfCaptureFails;
        beaconRxOk += s.beaconRxOk;
        rxScanTimeTotalS += s.rxScanTimeTotalS;
        sfLinkSamples += s.sfLinkSamples;
        sfLinkObservedMismatch += s.sfLinkObservedMismatch;
        routesTotal += s.routesTotal;
        routesPrimaryTotal += s.routesPrimaryTotal;
        routesBackupTotal += s.routesBackupTotal;
        destinationsWithBackupTotal += s.destinationsWithBackupTotal;
        routeEvictionsTotal += s.routeEvictionsTotal;
        backupPromotionsTotal += s.backupPromotionsTotal;
        rxAcquisitionDelayMeanSamples.push_back(s.rxAcquisitionDelayMeanS);
        rxAcquisitionDelayP95Samples.push_back(s.rxAcquisitionDelayP95S);
        if (s.firstUsableRouteTimeSec >= 0.0)
        {
            firstUsableRouteTimeSamples.push_back(s.firstUsableRouteTimeSec);
            firstUsableRouteByNode[s.nodeId] = s.firstUsableRouteTimeSec;
        }
        if (s.coverage80RouteTimeSec >= 0.0)
        {
            coverage80RouteTimeSamples.push_back(s.coverage80RouteTimeSec);
        }
        routesAtDataStart += s.routesAtDataStart;
        beaconTxAtDataStart += s.beaconTxAtDataStart;
        beaconRxAtDataStart += s.beaconRxAtDataStart;
        routesAtMidpoint += s.routesAtMidpoint;
        beaconTxAtMidpoint += s.beaconTxAtMidpoint;
        beaconRxAtMidpoint += s.beaconRxAtMidpoint;
        routesAtDataStop += s.routesAtDataStop;
        beaconTxAtDataStop += s.beaconTxAtDataStop;
        beaconRxAtDataStop += s.beaconRxAtDataStop;
        queuedPacketsAtDataStop += s.queuedPacketsAtDataStop;
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
    const double rxAcquisitionDelayMean =
        rxAcquisitionDelayMeanSamples.empty()
            ? 0.0
            : std::accumulate(rxAcquisitionDelayMeanSamples.begin(),
                              rxAcquisitionDelayMeanSamples.end(),
                              0.0) /
                  static_cast<double>(rxAcquisitionDelayMeanSamples.size());
    const double rxAcquisitionDelayP95 = Percentile95Double(rxAcquisitionDelayP95Samples);
    const double firstUsableRouteTimeMean =
        firstUsableRouteTimeSamples.empty()
            ? -1.0
            : std::accumulate(firstUsableRouteTimeSamples.begin(),
                              firstUsableRouteTimeSamples.end(),
                              0.0) /
                  static_cast<double>(firstUsableRouteTimeSamples.size());
    const double coverage80RouteTimeMean =
        coverage80RouteTimeSamples.empty()
            ? -1.0
            : std::accumulate(coverage80RouteTimeSamples.begin(),
                              coverage80RouteTimeSamples.end(),
                              0.0) /
                  static_cast<double>(coverage80RouteTimeSamples.size());
    const double sfLinkObservedMismatchRatio =
        sfLinkSamples > 0 ? static_cast<double>(sfLinkObservedMismatch) / sfLinkSamples : 0.0;
    const double dstWithBackupRatio =
        routesPrimaryTotal > 0 ? static_cast<double>(destinationsWithBackupTotal) / routesPrimaryTotal
                               : 0.0;
    const double controlToDataTxRatio =
        dataTxSent > 0 ? static_cast<double>(beaconTxSent) / dataTxSent : 0.0;
    const double dataStopSec = m_hasRunConfig ? m_runConfig.dataStopSec : -1.0;
    const double dataStartSec = m_hasRunConfig ? m_runConfig.dataStartSec : 0.0;
    uint64_t generatedBeforeFirstRoute = 0;
    uint64_t generatedAfterFirstRoute = 0;
    uint64_t generatedWithoutRouteEver = 0;
    uint64_t generatedNoFirstTxByEnd = 0;
    uint64_t generatedNoFirstTxLateWindow = 0;
    uint64_t generatedNoDrainEligible = 0;
    uint64_t lateGeneratedNearDataStop = 0;
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>> deliveredNoDrainKeys;
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>> deliveredPostConvergenceKeys;
    const double lateDataCutoff =
        (dataStopSec > dataStartSec && m_endWindowSec > 0.0)
            ? std::max(dataStartSec, dataStopSec - m_endWindowSec)
            : std::numeric_limits<double>::infinity();
    for (const auto& kv : m_generatedDataTime)
    {
        const auto& key = kv.first;
        const uint32_t src = std::get<0>(key);
        const double genSec = kv.second;
        auto itRoute = firstUsableRouteByNode.find(src);
        if (itRoute == firstUsableRouteByNode.end())
        {
            generatedWithoutRouteEver++;
        }
        else if (genSec < itRoute->second)
        {
            generatedBeforeFirstRoute++;
        }
        else
        {
            generatedAfterFirstRoute++;
        }

        if (m_firstTxTime.find(key) == m_firstTxTime.end())
        {
            generatedNoFirstTxByEnd++;
            if (genSec >= lateDataCutoff)
            {
                generatedNoFirstTxLateWindow++;
            }
        }
        if (dataStopSec > 0.0 && genSec <= dataStopSec)
        {
            generatedNoDrainEligible++;
            if (genSec >= lateDataCutoff)
            {
                lateGeneratedNearDataStop++;
            }
        }
    }
    for (const auto& e : m_delayEvents)
    {
        if (!e.delivered)
        {
            continue;
        }
        const auto key = std::make_tuple(e.src, e.dst, e.seq);
        auto itGen = m_generatedDataTime.find(key);
        if (itGen == m_generatedDataTime.end())
        {
            continue;
        }
        const double genSec = itGen->second;
        if (dataStopSec > 0.0 && genSec <= dataStopSec && e.timestamp.GetSeconds() <= dataStopSec)
        {
            deliveredNoDrainKeys.insert(key);
        }
        auto itRoute = firstUsableRouteByNode.find(e.src);
        if (itRoute != firstUsableRouteByNode.end() && genSec >= itRoute->second)
        {
            deliveredPostConvergenceKeys.insert(key);
        }
    }
    const double pdrPostConvergence =
        generatedAfterFirstRoute > 0
            ? static_cast<double>(deliveredPostConvergenceKeys.size()) / generatedAfterFirstRoute
            : 0.0;
    const double pdrNoDrain =
        generatedNoDrainEligible > 0
            ? static_cast<double>(deliveredNoDrainKeys.size()) / generatedNoDrainEligible
            : 0.0;
    const uint64_t beaconTxDuringDataPhase =
        (beaconTxAtDataStop >= beaconTxAtDataStart) ? (beaconTxAtDataStop - beaconTxAtDataStart) : 0;
    const uint64_t beaconRxDuringDataPhase =
        (beaconRxAtDataStop >= beaconRxAtDataStart) ? (beaconRxAtDataStop - beaconRxAtDataStart) : 0;

    std::vector<double> quantRaw;
    std::vector<double> quantScore;
    quantRaw.reserve(m_quantizationSamples.size());
    quantScore.reserve(m_quantizationSamples.size());
    std::map<uint16_t, std::set<uint64_t>> scoreToRawSet;
    for (const auto& s : m_quantizationSamples)
    {
        if (s.rawMetric <= 0.0)
        {
            continue;
        }
        quantRaw.push_back(s.rawMetric);
        quantScore.push_back(static_cast<double>(s.scoreQuantized));
        scoreToRawSet[s.scoreQuantized].insert(s.rawMetricMilli);
    }
    const auto calcMean = [](const std::vector<double>& vals) -> double {
        return vals.empty()
                   ? 0.0
                   : std::accumulate(vals.begin(), vals.end(), 0.0) /
                         static_cast<double>(vals.size());
    };
    const auto minRaw = quantRaw.empty() ? 0.0 : *std::min_element(quantRaw.begin(), quantRaw.end());
    const auto maxRaw = quantRaw.empty() ? 0.0 : *std::max_element(quantRaw.begin(), quantRaw.end());
    const auto minScore =
        quantScore.empty() ? 0.0 : *std::min_element(quantScore.begin(), quantScore.end());
    const auto maxScore =
        quantScore.empty() ? 0.0 : *std::max_element(quantScore.begin(), quantScore.end());
    uint64_t quantizationCollisions = 0;
    uint64_t quantizationSaturationCount = 0;
    for (const auto& kv : scoreToRawSet)
    {
        if (kv.second.size() > 1)
        {
            quantizationCollisions += static_cast<uint64_t>(kv.second.size() - 1);
        }
    }
    for (const auto& s : m_quantizationSamples)
    {
        const uint32_t maxMetricValue =
            (m_hasRunConfig &&
             (m_runConfig.costEncoding == "cost255" || m_runConfig.costEncoding == "score255"))
                ? 255u
                : 100u;
        const double compositeStep =
            (m_hasRunConfig && m_runConfig.compositeCostStep > 0.0) ? m_runConfig.compositeCostStep
                                                                    : 1.0;
        if (s.rawMetric > 0.0 &&
            std::isfinite(s.rawMetric) &&
            (s.rawMetric / compositeStep) >= static_cast<double>(maxMetricValue))
        {
            quantizationSaturationCount++;
        }
    }
    const double quantizationCollisionRatio =
        quantRaw.empty() ? 0.0
                         : static_cast<double>(quantizationCollisions) / static_cast<double>(quantRaw.size());
    const double quantizationSaturationRatio =
        quantRaw.empty() ? 0.0
                         : static_cast<double>(quantizationSaturationCount) /
                               static_cast<double>(quantRaw.size());
    const uint64_t uniqueScores = static_cast<uint64_t>(scoreToRawSet.size());
    const uint64_t totalScores = static_cast<uint64_t>(quantScore.size());

    std::vector<QuantizationSample> quantSampleTop = m_quantizationSamples;
    std::sort(quantSampleTop.begin(), quantSampleTop.end(), [](const QuantizationSample& a, const QuantizationSample& b) {
        if (a.rawMetric != b.rawMetric)
        {
            return a.rawMetric < b.rawMetric;
        }
        if (a.scoreQuantized != b.scoreQuantized)
        {
            return a.scoreQuantized > b.scoreQuantized;
        }
        if (a.nodeId != b.nodeId)
        {
            return a.nodeId < b.nodeId;
        }
        return a.destination < b.destination;
    });
    if (quantSampleTop.size() > 50)
    {
        quantSampleTop.resize(50);
    }

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
    const double payloadBits = static_cast<double>((m_hasRunConfig ? m_runConfig.payloadBytes : 0U) * 8U);
    double activeTrafficSec = 0.0;
    if (m_hasRunConfig)
    {
        if (m_runConfig.dataStopSec > m_runConfig.dataStartSec)
        {
            activeTrafficSec = m_runConfig.dataStopSec - m_runConfig.dataStartSec;
        }
        else if (m_runConfig.stopSec > m_runConfig.dataStartSec)
        {
            activeTrafficSec = m_runConfig.stopSec - m_runConfig.dataStartSec;
        }
    }
    const double throughputBps =
        activeTrafficSec > 0.0 ? (static_cast<double>(totalDataTxLegacy) * payloadBits) / activeTrafficSec
                               : 0.0;
    const double goodputBps =
        activeTrafficSec > 0.0 ? (static_cast<double>(deliveredPackets) * payloadBits) / activeTrafficSec
                               : 0.0;

    // Write JSON
    file << "{\n";
    file << "  \"simulation\": {\n";
    file << "    \"prefix\": \"" << prefix << "\",\n";
    file << "    \"timestamp\": \"" << Simulator::Now().GetSeconds() << "s\",\n";
    if (m_hasRunConfig)
    {
        file << "    \"n_nodes\": " << m_runConfig.nNodes << ",\n";
        file << "    \"sim_version\": \"" << m_runConfig.simVersion << "\",\n";
        file << "    \"git_commit\": \"" << m_runConfig.gitCommit << "\",\n";
        file << "    \"topology\": \"" << m_runConfig.topology << "\",\n";
        file << "    \"topology_preset\": \"" << m_runConfig.topologyPreset << "\",\n";
        file << "    \"grid_side\": " << m_runConfig.gridSide << ",\n";
        file << "    \"grid_spacing_x_m\": " << m_runConfig.gridSpacingXM << ",\n";
        file << "    \"grid_spacing_y_m\": " << m_runConfig.gridSpacingYM << ",\n";
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
        file << "    \"profile\": \"" << m_runConfig.profile << "\",\n";
        file << "    \"traffic_load\": \"" << m_runConfig.trafficLoad << "\",\n";
        file << "    \"traffic_mode\": \"" << m_runConfig.trafficMode << "\",\n";
        file << "    \"traffic_interval_s\": " << m_runConfig.trafficIntervalS << ",\n";
        file << "    \"enable_data_random_dest\": "
             << (m_runConfig.enableDataRandomDest ? "true" : "false") << ",\n";
        file << "    \"only_generate_from_node_id\": " << m_runConfig.onlyGenerateFromNodeId
             << ",\n";
        file << "    \"forced_data_destination_id\": " << m_runConfig.forcedDataDestinationId
             << ",\n";
        file << "    \"packets_per_pair\": " << m_runConfig.packetsPerPair << ",\n";
        file << "    \"payload_bytes\": " << m_runConfig.payloadBytes << ",\n";
        file << "    \"dedup_window_sec\": " << m_runConfig.dedupWindowSec << ",\n";
        file << "    \"frequency_hz\": " << m_runConfig.frequencyHz << ",\n";
        file << "    \"bandwidth_hz\": " << m_runConfig.bandwidthHz << ",\n";
        file << "    \"coding_rate\": \"" << m_runConfig.codingRate << "\",\n";
        file << "    \"sf_min\": " << m_runConfig.sfMin << ",\n";
        file << "    \"sf_max\": " << m_runConfig.sfMax << ",\n";
        file << "    \"preamble_symbols\": " << m_runConfig.preambleSymbols << ",\n";
        file << "    \"beacon_interval_warm_s\": " << m_runConfig.beaconIntervalWarmS << ",\n";
        file << "    \"beacon_interval_stable_s\": " << m_runConfig.beaconIntervalStableS << ",\n";
        file << "    \"route_timeout_factor\": " << m_runConfig.routeTimeoutFactor << ",\n";
        file << "    \"route_timeout_sec\": " << m_runConfig.routeTimeoutSec << ",\n";
        file << "    \"route_advert_policy\": \"" << m_runConfig.routeAdvertPolicy << "\",\n";
        file << "    \"route_metric_mode\": \"" << m_runConfig.routeMetricMode << "\",\n";
        file << "    \"cost_encoding\": \"" << m_runConfig.costEncoding << "\",\n";
        file << "    \"sf_link_mode\": \"" << m_runConfig.sfLinkMode << "\",\n";
        file << "    \"sf_link_margin_db\": " << m_runConfig.sfLinkMarginDb << ",\n";
        file << "    \"composite_w_toa\": " << m_runConfig.compositeWToa << ",\n";
        file << "    \"composite_w_hop\": " << m_runConfig.compositeWHop << ",\n";
        file << "    \"composite_w_energy\": " << m_runConfig.compositeWEnergy << ",\n";
        file << "    \"composite_cost_step\": " << m_runConfig.compositeCostStep << ",\n";
        file << "    \"energy_lo\": " << m_runConfig.energyLo << ",\n";
        file << "    \"energy_hi\": " << m_runConfig.energyHi << ",\n";
        file << "    \"energy_pow\": " << m_runConfig.energyPow << ",\n";
        file << "    \"energy_max_penalty\": " << m_runConfig.energyMaxPenalty << ",\n";
        file << "    \"max_routes_per_destination\": " << m_runConfig.maxRoutesPerDestination
             << ",\n";
        file << "    \"max_total_routes\": " << m_runConfig.maxTotalRoutes << ",\n";
        file << "    \"dv_payload_max_bytes\": " << m_runConfig.dvPayloadMaxBytes << ",\n";
        file << "    \"beacon_latest_only\": "
             << (m_runConfig.beaconLatestOnly ? "true" : "false") << ",\n";
        file << "    \"prioritize_beacons\": "
             << (m_runConfig.prioritizeBeacons ? "true" : "false") << ",\n";
        file << "    \"pueyo_strict_queue_scheduler\": "
             << (m_runConfig.pueyoStrictQueueScheduler ? "true" : "false") << ",\n";
        file << "    \"control_backoff_factor\": " << m_runConfig.controlBackoffFactor << ",\n";
        file << "    \"data_backoff_factor\": " << m_runConfig.dataBackoffFactor << ",\n";
        file << "    \"sf_scan_ed_threshold_dbm\": " << m_runConfig.sfScanEdThresholdDbm << ",\n";
        file << "    \"sf_scan_reset_on_new_signal\": "
             << (m_runConfig.sfScanResetOnNewSignal ? "true" : "false") << ",\n";
        file << "    \"enable_sf_scan_rx\": "
             << (m_runConfig.enableSfScanRx ? "true" : "false") << ",\n";
        file << "    \"pueyo_flora_like_rx\": "
             << (m_runConfig.pueyoFloraLikeRx ? "true" : "false") << ",\n";
        file << "    \"enable_ns3_energy_framework\": "
             << (m_runConfig.enableNs3EnergyFramework ? "true" : "false") << ",\n";
        file << "    \"shadowing_sigma_db\": " << m_runConfig.shadowingSigmaDb << ",\n";
        file << "    \"interference_model\": \"" << m_runConfig.interferenceModel << "\",\n";
        file << "    \"prop_model\": \"" << m_runConfig.propModel << "\",\n";
        file << "    \"tx_power_dbm\": " << m_runConfig.txPowerDbm << ",\n";
        file << "    \"channel_count\": " << m_runConfig.channelCount << ",\n";
        file << "    \"reception_paths\": " << m_runConfig.receptionPaths << ",\n";
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
    file << "    \"pdr_post_convergence\": " << std::fixed << std::setprecision(4)
         << pdrPostConvergence << ",\n";
    file << "    \"pdr_no_drain\": " << std::fixed << std::setprecision(4) << pdrNoDrain
         << ",\n";
    file << "    \"generated_before_first_route\": " << generatedBeforeFirstRoute << ",\n";
    file << "    \"generated_after_first_route\": " << generatedAfterFirstRoute << ",\n";
    file << "    \"generated_without_route_ever\": " << generatedWithoutRouteEver << ",\n";
    file << "    \"generated_no_first_tx_by_end\": " << generatedNoFirstTxByEnd << ",\n";
    file << "    \"late_generated_near_data_stop\": " << lateGeneratedNearDataStop << ",\n";
    file << "    \"generated_no_first_tx_late_window\": " << generatedNoFirstTxLateWindow << ",\n";
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
    file << "    \"source_tx_sent_total\": " << sourceTxSentTotal << ",\n";
    file << "    \"forward_tx_sent_total\": " << forwardTxSentTotal << ",\n";
    file << "    \"attempts_per_generated\": " << txAttemptsPerGenerated << ",\n";
    file << "    \"tx_attempts_per_generated\": " << txAttemptsPerGenerated << ",\n";
    file << "    \"admission_ratio\": " << txAttemptsPerGenerated << ",\n";
    file << "    \"source_first_tx_count\": " << sourceFirstTxCount << ",\n";
    file << "    \"source_first_tx_ratio\": " << sourceFirstTxRatio << ",\n";
    file << "    \"source_admission_ratio\": " << sourceFirstTxRatio << ",\n";
    file << "    \"delivered_per_tx_attempt\": " << deliveredPerTxAttempt << "\n";
    file << "  },\n";
    file << "  \"forwarding\": {\n";
    file << "    \"forward_tx_sent_total\": " << forwardTxSentTotal << ",\n";
    file << "    \"forwarded_unique_count\": " << forwardedUniqueKeys.size() << ",\n";
    file << "    \"avg_hops_delivered\": " << std::fixed << std::setprecision(6)
         << avgHopsDelivered << ",\n";
    file << "    \"p95_hops_delivered\": " << std::fixed << std::setprecision(6)
         << p95HopsDelivered << "\n";
    file << "  },\n";
    file << "  \"throughput\": {\n";
    file << "    \"active_traffic_sec\": " << std::fixed << std::setprecision(3) << activeTrafficSec
         << ",\n";
    file << "    \"throughput_bps\": " << std::fixed << std::setprecision(3) << throughputBps
         << ",\n";
    file << "    \"goodput_bps\": " << std::fixed << std::setprecision(3) << goodputBps << "\n";
    file << "  },\n";
    file << "  \"delay\": {\n";
    file << "    \"definition\": \"gen_to_final_rx\",\n";
    file << "    \"avg_s\": " << std::fixed << std::setprecision(6) << avgDelayGen << ",\n";
    file << "    \"p50_s\": " << std::fixed << std::setprecision(6) << p50DelayGen << ",\n";
    file << "    \"p95_s\": " << std::fixed << std::setprecision(6) << p95DelayGen << ",\n";
    file << "    \"min_s\": " << (minDelayGen < 1e9 ? minDelayGen : 0.0) << ",\n";
    file << "    \"max_s\": " << maxDelayGen << ",\n";
    file << "    \"delivered_count\": " << deliveredPackets << ",\n";
    file << "    \"samples_gen_to_rx\": " << deliveredDelayGenSamples.size() << ",\n";
    file << "    \"first_tx_to_rx_avg_s\": " << std::fixed << std::setprecision(6)
         << avgDelayFirstTx << ",\n";
    file << "    \"first_tx_to_rx_p50_s\": " << std::fixed << std::setprecision(6)
         << p50DelayFirstTx << ",\n";
    file << "    \"first_tx_to_rx_p95_s\": " << std::fixed << std::setprecision(6)
         << p95DelayFirstTx << ",\n";
    file << "    \"samples_first_tx_to_rx\": " << deliveredDelayFirstTxSamples.size() << "\n";
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
    file << "    \"used_events\": " << m_routeUsedEventsCount << ",\n";
    file << "    \"routes_total\": " << routesTotal << ",\n";
    file << "    \"routes_primary_total\": " << routesPrimaryTotal << ",\n";
    file << "    \"routes_backup_total\": " << routesBackupTotal << ",\n";
    file << "    \"dst_with_backup_total\": " << destinationsWithBackupTotal << ",\n";
    file << "    \"dst_with_backup_ratio\": " << std::fixed << std::setprecision(6)
         << dstWithBackupRatio << ",\n";
    file << "    \"evictions_total\": " << routeEvictionsTotal << ",\n";
    file << "    \"backup_promotions_total\": " << backupPromotionsTotal << "\n";
    file << "  },\n";
    file << "  \"control_plane\": {\n";
    file << "    \"beacon_scheduled\": " << beaconScheduled << ",\n";
    file << "    \"beacon_tx_sent\": " << beaconTxSent << ",\n";
    file << "    \"control_tx_sent\": " << beaconTxSent << ",\n";
    file << "    \"data_tx_sent\": " << dataTxSent << ",\n";
    file << "    \"control_to_data_tx_ratio\": " << std::fixed << std::setprecision(6)
         << controlToDataTxRatio << ",\n";
    file << "    \"duty_blocked_control\": " << dutyBlockedControl << ",\n";
    file << "    \"duty_blocked_data\": " << dutyBlockedData << ",\n";
    file << "    \"beacon_blocked_by_duty\": " << beaconBlockedByDuty << ",\n";
    file << "    \"cad_busy_events\": " << cadBusyEvents << ",\n";
    file << "    \"cad_busy_events_local_power\": " << cadBusyEventsLocalPower << ",\n";
    file << "    \"cad_busy_events_oracle\": " << cadBusyEventsOracle << ",\n";
    file << "    \"beacon_superseded_latest_only\": " << beaconSupersededLatestOnly << ",\n";
    file << "    \"rp_gap_large_events\": " << rpGapLargeEvents << ",\n";
    file << "    \"rx_scan_attempts\": " << rxScanAttempts << ",\n";
    file << "    \"rx_scan_locks\": " << rxScanLocks << ",\n";
    file << "    \"rx_scan_miss_before_lock\": " << rxScanMissBeforeLock << ",\n";
    file << "    \"rx_post_lock_interference_fail\": " << rxPostLockInterferenceFail << ",\n";
    file << "    \"rx_no_more_demodulators\": " << rxNoMoreDemodDrops << ",\n";
    file << "    \"pueyo_same_sf_overlap_events\": " << pueyoSameSfOverlapEvents << ",\n";
    file << "    \"pueyo_destructive_overlap_drops\": " << pueyoDestructiveOverlapDrops << ",\n";
    file << "    \"pueyo_capture_or_timing_survivals\": " << pueyoCaptureOrTimingSurvivals
         << ",\n";
    file << "    \"pueyo_cross_sf_ignored_overlaps\": " << pueyoCrossSfIgnoredOverlaps
         << ",\n";
    file << "    \"goursaud_deterministic_drops\": " << goursaudDeterministicDrops << ",\n";
    file << "    \"goursaud_cross_sf_capture_successes\": "
         << goursaudCrossSfCaptureSuccesses << ",\n";
    file << "    \"goursaud_cross_sf_capture_fails\": " << goursaudCrossSfCaptureFails
         << ",\n";
    file << "    \"beacon_rx_ok\": " << beaconRxOk << ",\n";
    file << "    \"rx_scan_time_total_s\": " << std::fixed << std::setprecision(6)
         << rxScanTimeTotalS << ",\n";
    file << "    \"rx_acquisition_delay_s_mean\": " << std::fixed << std::setprecision(6)
         << rxAcquisitionDelayMean << ",\n";
    file << "    \"rx_acquisition_delay_s_p95\": " << std::fixed << std::setprecision(6)
         << rxAcquisitionDelayP95 << ",\n";
    file << "    \"first_usable_route_time_s_mean\": " << std::fixed << std::setprecision(6)
         << firstUsableRouteTimeMean << ",\n";
    file << "    \"coverage80_route_time_s_mean\": " << std::fixed << std::setprecision(6)
         << coverage80RouteTimeMean << ",\n";
    file << "    \"routes_at_data_start\": " << routesAtDataStart << ",\n";
    file << "    \"beacon_tx_at_data_start\": " << beaconTxAtDataStart << ",\n";
    file << "    \"beacon_rx_at_data_start\": " << beaconRxAtDataStart << ",\n";
    file << "    \"routes_at_midpoint\": " << routesAtMidpoint << ",\n";
    file << "    \"beacon_tx_at_midpoint\": " << beaconTxAtMidpoint << ",\n";
    file << "    \"beacon_rx_at_midpoint\": " << beaconRxAtMidpoint << ",\n";
    file << "    \"routes_at_data_stop\": " << routesAtDataStop << ",\n";
    file << "    \"beacon_tx_at_data_stop\": " << beaconTxAtDataStop << ",\n";
    file << "    \"beacon_rx_at_data_stop\": " << beaconRxAtDataStop << ",\n";
    file << "    \"beacon_tx_before_data_start\": " << beaconTxAtDataStart << ",\n";
    file << "    \"beacon_rx_before_data_start\": " << beaconRxAtDataStart << ",\n";
    file << "    \"beacon_tx_during_data_phase\": " << beaconTxDuringDataPhase << ",\n";
    file << "    \"beacon_rx_during_data_phase\": " << beaconRxDuringDataPhase << ",\n";
    file << "    \"beacon_delay_s_mean\": " << std::fixed << std::setprecision(6)
         << beaconDelayMean << ",\n";
    file << "    \"beacon_delay_s_p95\": " << std::fixed << std::setprecision(6) << beaconDelayP95
         << ",\n";
    file << "    \"beacon_delay_samples\": " << m_beaconDelaySamples.size() << ",\n";
    file << "    \"sf_link_samples\": " << sfLinkSamples << ",\n";
    file << "    \"sf_link_observed_mismatch\": " << sfLinkObservedMismatch << ",\n";
    file << "    \"sf_link_observed_mismatch_ratio\": " << std::fixed << std::setprecision(6)
         << sfLinkObservedMismatchRatio << "\n";
    file << "  },\n";
    file << "  \"quantization\": {\n";
    file << "    \"samples\": " << quantRaw.size() << ",\n";
    file << "    \"metric_raw_min\": " << std::fixed << std::setprecision(6) << minRaw << ",\n";
    file << "    \"metric_raw_mean\": " << std::fixed << std::setprecision(6) << calcMean(quantRaw)
         << ",\n";
    file << "    \"metric_raw_p50\": " << std::fixed << std::setprecision(6)
         << Percentile50Double(quantRaw) << ",\n";
    file << "    \"metric_raw_p95\": " << std::fixed << std::setprecision(6)
         << Percentile95Double(quantRaw) << ",\n";
    file << "    \"metric_raw_max\": " << std::fixed << std::setprecision(6) << maxRaw << ",\n";
    file << "    \"toa_cost_raw_min\": " << std::fixed << std::setprecision(6) << minRaw << ",\n";
    file << "    \"toa_cost_raw_mean\": " << std::fixed << std::setprecision(6) << calcMean(quantRaw)
         << ",\n";
    file << "    \"toa_cost_raw_p50\": " << std::fixed << std::setprecision(6)
         << Percentile50Double(quantRaw) << ",\n";
    file << "    \"toa_cost_raw_p95\": " << std::fixed << std::setprecision(6)
         << Percentile95Double(quantRaw) << ",\n";
    file << "    \"toa_cost_raw_max\": " << std::fixed << std::setprecision(6) << maxRaw << ",\n";
    file << "    \"score_quantized_min\": " << std::fixed << std::setprecision(6) << minScore
         << ",\n";
    file << "    \"score_quantized_mean\": " << std::fixed << std::setprecision(6)
         << calcMean(quantScore) << ",\n";
    file << "    \"score_quantized_p50\": " << std::fixed << std::setprecision(6)
         << Percentile50Double(quantScore) << ",\n";
    file << "    \"score_quantized_p95\": " << std::fixed << std::setprecision(6)
         << Percentile95Double(quantScore) << ",\n";
    file << "    \"score_quantized_max\": " << std::fixed << std::setprecision(6) << maxScore
         << ",\n";
    file << "    \"quantization_collisions\": " << quantizationCollisions << ",\n";
    file << "    \"quantization_collision_ratio\": " << std::fixed << std::setprecision(6)
         << quantizationCollisionRatio << ",\n";
    file << "    \"saturation_count\": " << quantizationSaturationCount << ",\n";
    file << "    \"saturation_ratio\": " << std::fixed << std::setprecision(6)
         << quantizationSaturationRatio << ",\n";
    file << "    \"unique_scores\": " << uniqueScores << ",\n";
    file << "    \"total_scores\": " << totalScores << ",\n";
    file << "    \"max_toa_cost_raw\": " << std::fixed << std::setprecision(6) << maxRaw << ",\n";
    file << "    \"sample_top_raw\": [\n";
    for (std::size_t i = 0; i < quantSampleTop.size(); ++i)
    {
        const auto& s = quantSampleTop[i];
        file << "      {\"node\": " << s.nodeId << ", \"dst\": " << s.destination
             << ", \"nextHop\": " << s.nextHop << ", \"is_backup\": "
             << (s.isBackup ? "true" : "false") << ", \"metric_raw\": " << s.rawMetric
             << ", \"toa_cost_raw\": " << s.rawMetric
             << ", \"score_quantized\": " << s.scoreQuantized << "}";
        if (i + 1 < quantSampleTop.size())
        {
            file << ",";
        }
        file << "\n";
    }
    file << "    ]\n";
    file << "  },\n";
    file << "  \"queue_backlog\": {\n";
    file << "    \"txQueue_len_end_total\": " << txQueueLenEndTotal << ",\n";
    file << "    \"queued_packets_end\": " << queuedPacketsEnd << ",\n";
    file << "    \"txQueue_len_end_avg_node\": " << std::fixed << std::setprecision(4)
         << txQueueLenEndAvgNode << ",\n";
    file << "    \"txQueue_len_end_p95_node\": " << std::fixed << std::setprecision(4)
         << txQueueLenEndP95Node << ",\n";
    file << "    \"txQueue_len_end_max_node\": " << txQueueLenEndMaxNode << ",\n";
    file << "    \"queued_packets_at_data_stop\": " << queuedPacketsAtDataStop << ",\n";
    file << "    \"cad_busy_events\": " << cadBusyEvents << ",\n";
    file << "    \"cad_busy_events_local_power\": " << cadBusyEventsLocalPower << ",\n";
    file << "    \"cad_busy_events_oracle\": " << cadBusyEventsOracle << ",\n";
    file << "    \"duty_blocked_events\": " << dutyBlockedEvents << ",\n";
    file << "    \"total_wait_time_due_to_duty_s\": " << std::fixed << std::setprecision(6)
         << totalWaitTimeDueToDutyS << "\n";
    file << "  },\n";
    file << "  \"drops\": {\n";
    file << "    \"drop_no_route\": " << dropNoRoute << ",\n";
    file << "    \"drop_no_route_src\": " << dropNoRouteSrc << ",\n";
    file << "    \"drop_no_route_relay\": " << dropNoRouteRelay << ",\n";
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
