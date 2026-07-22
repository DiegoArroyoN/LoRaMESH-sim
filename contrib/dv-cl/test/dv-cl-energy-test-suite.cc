/* SPDX-License-Identifier: GPL-2.0-only */

#include "ns3/basic-energy-source.h"
#include "ns3/double.h"
#include "ns3/dv-cl-energy-registry.h"
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
/**
 * \ingroup dv-cl
 * \brief F1.3: el libro del registro cierra, y una bateria vacia no entrega
 *        carga.
 *
 * El registro es la fuente de verdad del estado de carga: alimenta el byte SoC
 * de la baliza, el argumento de Psi en la metrica y la deteccion de muerte del
 * nodo. Debe cumplir dos cosas. Que lo gastado por categoria mas lo que queda
 * sume la capacidad inicial -- si no, el desglose por actividad no significa
 * nada. Y que deje de cobrar al llegar a cero: cobrar el nominal aunque no
 * quede carga hacia crecer los contadores de por vida despues de la muerte del
 * nodo, inflando la parte de idle en toda corrida donde alguien muere pronto.
 */
class DvClEnergyRegistryClosureTestCase : public TestCase
{
  public:
    DvClEnergyRegistryClosureTestCase()
        : TestCase("dv-cl registry ledger closes and a flat battery draws nothing")
    {
    }

  private:
    void DoRun() override
    {
        auto reg = CreateObject<DvClEnergyRegistry>();
        const double capacityMah = 1.0; // pequena a proposito: se agota dentro del test
        reg->SetCapacityMah(capacityMah);
        reg->RegisterNode(0);
        reg->SetRemainingFraction(0, 1.0);

        // Un poco de cada actividad, separadas en el tiempo para que el idle
        // diferido tambien entre en juego.
        Simulator::Schedule(Seconds(10), [reg]() { reg->UpdateEnergy(0, 120.0, 1.0); });
        Simulator::Schedule(Seconds(20), [reg]() { reg->UpdateRxEnergy(0, 2.0); });
        Simulator::Schedule(Seconds(30), [reg]() { reg->UpdateCadEnergy(0, 0.5); });

        Simulator::Schedule(Seconds(40), [this, reg, capacityMah]() {
            const double spent = reg->GetTxMah(0) + reg->GetRxMah(0) + reg->GetCadMah(0) +
                                 reg->GetIdleMah(0);
            const double remaining = reg->GetEnergyFraction(0) * capacityMah;
            NS_TEST_ASSERT_MSG_EQ_TOL(spent + remaining,
                                      capacityMah,
                                      1e-9,
                                      "gastado por categoria + remanente = capacidad inicial");
        });

        // Agotar la bateria y seguir pidiendo consumo: nada mas debe cobrarse.
        Simulator::Schedule(Seconds(50), [reg]() { reg->UpdateEnergy(0, 120.0, 3600.0); });
        Simulator::Schedule(Seconds(60), [this, reg, capacityMah]() {
            NS_TEST_ASSERT_MSG_EQ_TOL(reg->GetEnergyFraction(0), 0.0, 1e-12, "bateria agotada");
            const double spentAtDeath = reg->GetTxMah(0) + reg->GetRxMah(0) + reg->GetCadMah(0) +
                                        reg->GetIdleMah(0);
            NS_TEST_ASSERT_MSG_EQ_TOL(spentAtDeath,
                                      capacityMah,
                                      1e-9,
                                      "no se cobra mas que la capacidad que habia");
            m_spentAtDeath = spentAtDeath;
        });

        // Mucho despues de la muerte los contadores no pueden haber crecido.
        Simulator::Schedule(Seconds(100000), [this, reg]() {
            reg->UpdateRxEnergy(0, 10.0);
            const double spentLater = reg->GetTxMah(0) + reg->GetRxMah(0) + reg->GetCadMah(0) +
                                      reg->GetIdleMah(0);
            NS_TEST_ASSERT_MSG_EQ_TOL(spentLater,
                                      m_spentAtDeath,
                                      1e-9,
                                      "un nodo muerto no sigue consumiendo");
        });

        Simulator::Stop(Seconds(100001));
        Simulator::Run();
        Simulator::Destroy();
    }

    double m_spentAtDeath{0.0};
};

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
        AddTestCase(new DvClEnergyRegistryClosureTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClEnergyTraceSourceTestCase, TestCase::Duration::QUICK);
    }
};

static DvClEnergyTestSuite g_dvClEnergyTestSuite; //!< static suite registration

} // namespace test
} // namespace dvcl
} // namespace ns3
