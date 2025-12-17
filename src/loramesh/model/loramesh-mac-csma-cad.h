#ifndef LORAMESH_MAC_CSMA_CAD_H
#define LORAMESH_MAC_CSMA_CAD_H

#include "ns3/object.h"
#include "ns3/nstime.h"
#include "ns3/ptr.h"
#include "ns3/random-variable-stream.h"

#include <deque>

namespace ns3
{
namespace loramesh
{

/**
 * \brief Minimal CSMA/CAD MAC helper handling channel sampling, backoff and duty cycle.
 */
class CsmaCadMac : public Object
{
public:
  static TypeId GetTypeId ();
  CsmaCadMac ();
  ~CsmaCadMac () override = default;

  bool CanTransmitNow ();
  bool CanTransmitNow (double toaSeconds);
  void NotifyTxStart (double toaSeconds);
  void NotifyRxStart (double durationSeconds);
  uint32_t GetBackoffSlots ();
  double GetDutyCycleUsed ();
  void SetDutyCycleLimit (double limit);
  void SetDutyCycleWindow (Time window);
  void SetCadDuration (Time duration);
  void SetDifsCadCount (uint8_t count);
  void SetBackoffWindow (uint8_t window);
  void SetRandomStream (Ptr<UniformRandomVariable> rng);
  void SetCadBusyProbability (double probability);

  Time GetCadDuration () const;
  uint8_t GetDifsCadCount () const;
  bool PerformChannelAssessment ();

private:
  bool PerformCadOnce () const;
  void CleanOldTxHistory ();

  Ptr<UniformRandomVariable> m_rng;
  Time m_dutyCycleWindow;
  double m_dutyCycleLimit;
  Time m_cadDuration;
  uint8_t m_difsCadCount;
  uint8_t m_backoffWindow;
  double m_cadBusyProbability;
  std::deque<std::pair<Time, Time>> m_txHistory;
};

} // namespace loramesh
} // namespace ns3

#endif /* LORAMESH_MAC_CSMA_CAD_H */
