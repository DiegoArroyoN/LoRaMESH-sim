/* SPDX-License-Identifier: GPL-2.0-only */

#include "ns3/double.h"
#include "ns3/dv-cl-static-shadowing.h"
#include "ns3/simulator.h"
#include "ns3/test.h"

#include <cmath>
#include <set>

namespace ns3
{
namespace dvcl
{
namespace test
{

/**
 * \ingroup dv-cl
 * The shadowing of a link is drawn once and then held.
 *
 * This is the property the model exists for. With a fresh draw per call the
 * beacon a node hears says nothing about the data that follows, the spreading
 * factor derived from it is systematically too low -- only the favourable draws
 * clear the sensitivity threshold and get measured -- and routes end up
 * installed over links that cannot carry traffic.
 */
class DvClShadowingIsStablePerLinkTestCase : public TestCase
{
  public:
    DvClShadowingIsStablePerLinkTestCase()
        : TestCase("dv-cl shadowing is drawn once per link and held")
    {
    }

  private:
    void DoRun() override
    {
        auto m = CreateObject<DvClStaticShadowingPropagationLossModel>();
        m->SetAttribute("SigmaDb", DoubleValue(3.57));

        const double first = m->GetLinkShadowingDb(3, 7);
        for (int i = 0; i < 20; ++i)
        {
            NS_TEST_ASSERT_MSG_EQ_TOL(m->GetLinkShadowingDb(3, 7),
                                      first,
                                      1e-12,
                                      "el sombreado del enlace 3-7 cambio entre consultas");
        }
        Simulator::Destroy();
    }
};

/**
 * \ingroup dv-cl
 * Shadowing is reciprocal: A->B and B->A are the same link.
 *
 * Drawing twice would make a link look usable in one direction and not the
 * other, so a route could be installed on a beacon that arrived while the
 * reverse path silently dropped every packet.
 */
class DvClShadowingIsReciprocalTestCase : public TestCase
{
  public:
    DvClShadowingIsReciprocalTestCase()
        : TestCase("dv-cl shadowing is the same in both directions of a link")
    {
    }

  private:
    void DoRun() override
    {
        auto m = CreateObject<DvClStaticShadowingPropagationLossModel>();
        m->SetAttribute("SigmaDb", DoubleValue(3.57));
        for (uint32_t a = 0; a < 5; ++a)
        {
            for (uint32_t b = a + 1; b < 6; ++b)
            {
                NS_TEST_ASSERT_MSG_EQ_TOL(m->GetLinkShadowingDb(a, b),
                                          m->GetLinkShadowingDb(b, a),
                                          1e-12,
                                          "el enlace " << a << "-" << b
                                                       << " no es reciproco");
            }
        }
        Simulator::Destroy();
    }
};

/**
 * \ingroup dv-cl
 * Different links get different draws, and sigma=0 disables the model.
 *
 * Without the first check a model that returned a constant would pass the two
 * tests above while modelling nothing at all.
 */
class DvClShadowingVariesAcrossLinksTestCase : public TestCase
{
  public:
    DvClShadowingVariesAcrossLinksTestCase()
        : TestCase("dv-cl shadowing varies across links and vanishes at sigma=0")
    {
    }

  private:
    void DoRun() override
    {
        auto m = CreateObject<DvClStaticShadowingPropagationLossModel>();
        m->SetAttribute("SigmaDb", DoubleValue(3.57));
        std::set<double> seen;
        for (uint32_t b = 1; b < 25; ++b)
        {
            seen.insert(m->GetLinkShadowingDb(0, b));
        }
        NS_TEST_ASSERT_MSG_GT(seen.size(),
                              20u,
                              "los enlaces distintos comparten valor: el sorteo no varia");

        auto off = CreateObject<DvClStaticShadowingPropagationLossModel>();
        off->SetAttribute("SigmaDb", DoubleValue(0.0));
        for (uint32_t b = 1; b < 5; ++b)
        {
            NS_TEST_ASSERT_MSG_EQ_TOL(off->GetLinkShadowingDb(0, b),
                                      0.0,
                                      1e-12,
                                      "con sigma=0 no debe haber sombreado");
        }
        Simulator::Destroy();
    }
};

/**
 * \ingroup dv-cl
 * The sample spread matches the configured sigma.
 *
 * Guards against the variance/standard-deviation mix-up: ns-3's normal variable
 * takes a variance, so passing sigma straight through would silently produce a
 * channel with sqrt(sigma) dB of spread.
 */
class DvClShadowingHonoursSigmaTestCase : public TestCase
{
  public:
    DvClShadowingHonoursSigmaTestCase()
        : TestCase("dv-cl shadowing spread matches the configured sigma")
    {
    }

  private:
    void DoRun() override
    {
        const double sigma = 3.57;
        auto m = CreateObject<DvClStaticShadowingPropagationLossModel>();
        m->SetAttribute("SigmaDb", DoubleValue(sigma));

        const uint32_t n = 4000;
        double sum = 0.0;
        double sumSq = 0.0;
        for (uint32_t i = 0; i < n; ++i)
        {
            const double v = m->GetLinkShadowingDb(i, i + 100000);
            sum += v;
            sumSq += v * v;
        }
        const double mean = sum / n;
        const double sd = std::sqrt(sumSq / n - mean * mean);
        NS_TEST_ASSERT_MSG_EQ_TOL(mean, 0.0, 0.35, "la media del sombreado no es cero");
        NS_TEST_ASSERT_MSG_EQ_TOL(sd,
                                  sigma,
                                  0.35,
                                  "la desviacion tipica no coincide con sigma "
                                  "(confusion varianza/sigma?)");
        Simulator::Destroy();
    }
};

class DvClShadowingTestSuite : public TestSuite
{
  public:
    DvClShadowingTestSuite()
        : TestSuite("dv-cl-shadowing", Type::UNIT)
    {
        AddTestCase(new DvClShadowingIsStablePerLinkTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClShadowingIsReciprocalTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClShadowingVariesAcrossLinksTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClShadowingHonoursSigmaTestCase, TestCase::Duration::QUICK);
    }
};

static DvClShadowingTestSuite g_dvClShadowingTestSuite;

} // namespace test
} // namespace dvcl
} // namespace ns3
