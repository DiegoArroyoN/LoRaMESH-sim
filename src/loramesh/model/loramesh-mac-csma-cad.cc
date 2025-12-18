#include "loramesh-mac-csma-cad.h"

#include "ns3/log.h"
#include "ns3/simulator.h"

#include <algorithm>

namespace ns3
{
namespace loramesh
{

NS_LOG_COMPONENT_DEFINE ("CsmaCadMac");
NS_OBJECT_ENSURE_REGISTERED (CsmaCadMac);

TypeId
CsmaCadMac::GetTypeId ()
{
  static TypeId tid = TypeId ("ns3::loramesh::CsmaCadMac")
    .SetParent<Object> ()
    .AddConstructor<CsmaCadMac> ();
  return tid;
}

CsmaCadMac::CsmaCadMac ()
  : m_rng (CreateObject<UniformRandomVariable> ()),
    m_dutyCycleWindow (Hours (1)),
    m_dutyCycleLimit (0.10),
    m_cadDuration (MilliSeconds (5.5)),
    m_difsCadCount (3),
    m_backoffWindow (8),
    m_cadBusyProbability (0.2)
{
}

bool
CsmaCadMac::CanTransmitNow ()
{
  return CanTransmitNow (0.0);
}

bool
CsmaCadMac::CanTransmitNow (double toaSeconds)
{
  CleanOldTxHistory ();
  const double dutyCycle = GetDutyCycleUsed ();
  double projected = dutyCycle;
  if (toaSeconds > 0.0 && m_dutyCycleWindow.GetSeconds () > 0.0)
    {
      projected += toaSeconds / m_dutyCycleWindow.GetSeconds ();
    }

  const bool allowed = projected <= m_dutyCycleLimit;
  if (!allowed)
    {
      NS_LOG_WARN ("CsmaCadMac: DUTY limit exceeded current="
                   << dutyCycle * 100.0 << "% projected=" << projected * 100.0
                   << "% limit=" << m_dutyCycleLimit * 100.0 << "%");
    }
  return allowed;
}

void
CsmaCadMac::NotifyTxStart (double toaSeconds)
{
  CleanOldTxHistory ();
  const Time now = Simulator::Now ();
  const Time duration = Seconds (toaSeconds);
  m_txHistory.emplace_back (now, duration);
  NS_LOG_DEBUG ("CsmaCadMac: TX recorded duration=" << duration.GetSeconds ()
                                                    << "s dc=" << GetDutyCycleUsed () * 100.0 << "%");
}

void
CsmaCadMac::NotifyRxStart (double durationSeconds)
{
  NS_LOG_DEBUG ("CsmaCadMac: RX duration=" << durationSeconds << "s");
  (void) durationSeconds;
}

uint32_t
CsmaCadMac::GetBackoffSlots ()
{
  const uint32_t maxSlots = (1u << m_backoffWindow) - 1;
  return m_rng->GetInteger (0, maxSlots);
}

double
CsmaCadMac::GetDutyCycleUsed ()
{
  CleanOldTxHistory ();
  if (m_txHistory.empty () || m_dutyCycleWindow.IsZero ())
    {
      return 0.0;
    }

  Time total { Seconds (0) };
  for (const auto& record : m_txHistory)
    {
      total += record.second;
    }
  return total.GetSeconds () / m_dutyCycleWindow.GetSeconds ();
}

double
CsmaCadMac::GetDutyCycleLimit () const
{
  return m_dutyCycleLimit;
}

void
CsmaCadMac::SetDutyCycleLimit (double limit)
{
  m_dutyCycleLimit = std::clamp (limit, 0.0, 1.0);
}

void
CsmaCadMac::SetDutyCycleWindow (Time window)
{
  m_dutyCycleWindow = window;
}

void
CsmaCadMac::SetCadDuration (Time duration)
{
  m_cadDuration = duration;
}

void
CsmaCadMac::SetDifsCadCount (uint8_t count)
{
  m_difsCadCount = count;
}

void
CsmaCadMac::SetBackoffWindow (uint8_t window)
{
  m_backoffWindow = window;
}

void
CsmaCadMac::SetRandomStream (Ptr<UniformRandomVariable> rng)
{
  m_rng = rng ? rng : CreateObject<UniformRandomVariable> ();
}

void
CsmaCadMac::SetCadBusyProbability (double probability)
{
  m_cadBusyProbability = std::clamp (probability, 0.0, 1.0);
}

Time
CsmaCadMac::GetCadDuration () const
{
  return m_cadDuration;
}

uint8_t
CsmaCadMac::GetDifsCadCount () const
{
  return m_difsCadCount;
}

bool
CsmaCadMac::PerformChannelAssessment ()
{
  for (uint8_t i = 0; i < m_difsCadCount; ++i)
    {
      if (PerformCadOnce ())
        {
          NS_LOG_INFO ("CsmaCadMac: CAD busy at attempt=" << unsigned (i));
          return true;
        }
    }
  return false;
}

bool
CsmaCadMac::PerformCadOnce () const
{
  const double val = m_rng->GetValue (0.0, 1.0);
  const bool busy = (val < m_cadBusyProbability);
  NS_LOG_DEBUG ("CsmaCadMac: CAD result busy=" << busy);
  return busy;
}

void
CsmaCadMac::CleanOldTxHistory ()
{
  if (m_txHistory.empty ())
    {
      return;
    }

  const Time threshold = Simulator::Now () - m_dutyCycleWindow;
  while (!m_txHistory.empty () && m_txHistory.front ().first < threshold)
    {
      m_txHistory.pop_front ();
    }
}

} // namespace loramesh
} // namespace ns3
