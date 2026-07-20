/* SPDX-License-Identifier: GPL-2.0-only */

#include "ns3/basic-energy-source.h"
#include "ns3/double.h"
#include "ns3/dv-cl-lora-energy-model.h"
#include "ns3/simulator.h"
#include "ns3/test.h"

namespace ns3
{
namespace dvcl
{
namespace test
{

/**
 * \ingroup dv-cl
 * F1.3(b) closure at module level: the per-state time ledger commits
 * exactly at state transitions, and the accumulated consumption equals
 * E = sum(I_s * V * t_s) to the microjoule on a synthetic sequence
 * (TX 2 s, SLEEP 3 s at datasheet currents, V = 3.0).
 */
class DvClEnergyLedgerTestCase : public TestCase
{
  public:
    DvClEnergyLedgerTestCase()
        : TestCase("dv-cl energy per-state ledger and E = sum(I*V*t) closure")
    {
    }

  private:
    Ptr<DvClLoraEnergyModel> m_model;

    void ToSleep()
    {
        m_model->ChangeState(static_cast<int>(DvClRadioState::SLEEP));
    }

    void ToIdleAndCheck()
    {
        m_model->ChangeState(static_cast<int>(DvClRadioState::IDLE));
        const double sTx = m_model->GetTimeInState(DvClRadioState::TX).GetSeconds();
        const double sSleep = m_model->GetTimeInState(DvClRadioState::SLEEP).GetSeconds();
        const double sIdle = m_model->GetTimeInState(DvClRadioState::IDLE).GetSeconds();
        NS_TEST_ASSERT_MSG_EQ_TOL(sTx, 2.0, 1e-9, "TX time committed at transition");
        NS_TEST_ASSERT_MSG_EQ_TOL(sSleep, 3.0, 1e-9, "SLEEP time committed at transition");
        NS_TEST_ASSERT_MSG_EQ_TOL(sIdle, 0.0, 1e-12, "IDLE accrues only at the next commit");
        // E = V * (I_tx*2 + I_sleep*3) with datasheet defaults and V=3.0.
        const double expected = 3.0 * (0.120 * 2.0 + 0.0000002 * 3.0);
        NS_TEST_ASSERT_MSG_EQ_TOL(m_model->GetTotalEnergyConsumption(),
                                  expected,
                                  1e-6,
                                  "energy closure E = sum(I_s*V*t_s)");
    }

    void DoRun() override
    {
        auto src = CreateObject<energy::BasicEnergySource>();
        src->SetAttribute("BasicEnergySourceInitialEnergyJ", DoubleValue(1000.0));
        src->SetAttribute("BasicEnergySupplyVoltageV", DoubleValue(3.0));
        m_model = CreateObject<DvClLoraEnergyModel>();
        m_model->SetEnergySource(src);

        m_model->ChangeState(static_cast<int>(DvClRadioState::TX)); // t = 0
        Simulator::Schedule(Seconds(2), &DvClEnergyLedgerTestCase::ToSleep, this);
        Simulator::Schedule(Seconds(5), &DvClEnergyLedgerTestCase::ToIdleAndCheck, this);
        Simulator::Stop(Seconds(6)); // BasicEnergySource reschedules forever
        Simulator::Run();
        Simulator::Destroy();
        m_model = nullptr;
    }
};

/**
 * \ingroup dv-cl
 * The EnergyDepleted TraceSource (the module replacement for the
 * campaign-tree global collector hook) is registered on the TypeId.
 */
class DvClEnergyTraceSourceTestCase : public TestCase
{
  public:
    DvClEnergyTraceSourceTestCase()
        : TestCase("dv-cl energy EnergyDepleted TraceSource is registered")
    {
    }

  private:
    void DoRun() override
    {
        TypeId tid = DvClLoraEnergyModel::GetTypeId();
        auto accessor = tid.LookupTraceSourceByName("EnergyDepleted");
        NS_TEST_ASSERT_MSG_EQ((accessor != nullptr),
                              true,
                              "EnergyDepleted trace source registered");
    }
};

/**
 * \ingroup dv-cl
 * The dv-cl-energy test suite.
 */
class DvClEnergyTestSuite : public TestSuite
{
  public:
    DvClEnergyTestSuite()
        : TestSuite("dv-cl-energy", Type::UNIT)
    {
        AddTestCase(new DvClEnergyLedgerTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClEnergyTraceSourceTestCase, TestCase::Duration::QUICK);
    }
};

static DvClEnergyTestSuite g_dvClEnergyTestSuite; //!< static suite registration

} // namespace test
} // namespace dvcl
} // namespace ns3
