/* SPDX-License-Identifier: GPL-2.0-only */

#include "ns3/dv-cl-wire.h"
#include "ns3/packet.h"
#include "ns3/test.h"

namespace ns3
{
namespace dvcl
{
namespace test
{

/**
 * \ingroup dv-cl
 * Golden-byte test: locks the on-air layout of both headers so any
 * change to the wire contract fails loudly (the campaign-tree lesson:
 * a silent 2-byte shift collapsed the control plane).
 */
class DvClWireGoldenBytesTestCase : public TestCase
{
  public:
    DvClWireGoldenBytesTestCase()
        : TestCase("dv-cl wire golden bytes (beacon 6B, data 7B)")
    {
    }

  private:
    void DoRun() override
    {
        // Beacon: src=0x1234 dst=0xFFFF type=BEACON ttl=5 soc=87
        {
            DvClBeaconHeader h;
            h.SetSrc(0x1234);
            h.SetDst(0xFFFF);
            h.SetFlagsTtl(PackFlagsTtl(DvClPacketType::BEACON, 5));
            h.SetSoc(87);
            Ptr<Packet> p = Create<Packet>();
            p->AddHeader(h);
            NS_TEST_ASSERT_MSG_EQ(p->GetSize(), 6, "beacon header must be 6 bytes");
            uint8_t buf[6];
            p->CopyData(buf, 6);
            const uint8_t golden[6] = {0x34, 0x12, 0xFF, 0xFF, 0x45, 0x57};
            for (uint32_t i = 0; i < 6; ++i)
            {
                NS_TEST_ASSERT_MSG_EQ(buf[i], golden[i], "beacon byte " << i);
            }
        }
        // Data: src=1 dst=2 via=3 type=DATA ttl=63
        {
            DvClDataHeader h;
            h.SetSrc(1);
            h.SetDst(2);
            h.SetVia(3);
            h.SetFlagsTtl(PackFlagsTtl(DvClPacketType::DATA, 63));
            Ptr<Packet> p = Create<Packet>();
            p->AddHeader(h);
            NS_TEST_ASSERT_MSG_EQ(p->GetSize(), 7, "data header must be 7 bytes");
            uint8_t buf[7];
            p->CopyData(buf, 7);
            const uint8_t golden[7] = {0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x3F};
            for (uint32_t i = 0; i < 7; ++i)
            {
                NS_TEST_ASSERT_MSG_EQ(buf[i], golden[i], "data byte " << i);
            }
        }
    }
};

/**
 * \ingroup dv-cl
 * Round-trips and helper semantics: header field recovery through
 * Packet Add/Remove, TTL capping, type extraction, DV entry pack/unpack
 * including poison (score 0) and truncation.
 */
class DvClWireRoundTripTestCase : public TestCase
{
  public:
    DvClWireRoundTripTestCase()
        : TestCase("dv-cl wire round-trips, flags helpers, DV entries")
    {
    }

  private:
    void DoRun() override
    {
        // Beacon round-trip.
        {
            DvClBeaconHeader tx;
            tx.SetSrc(41);
            tx.SetDst(0xFFFF);
            tx.SetFlagsTtl(PackFlagsTtl(DvClPacketType::BEACON, 12));
            tx.SetSoc(63);
            Ptr<Packet> p = Create<Packet>();
            p->AddHeader(tx);
            DvClBeaconHeader rx;
            p->RemoveHeader(rx);
            NS_TEST_ASSERT_MSG_EQ(rx.GetSrc(), 41, "beacon src");
            NS_TEST_ASSERT_MSG_EQ(rx.GetDst(), 0xFFFF, "beacon dst");
            NS_TEST_ASSERT_MSG_EQ(unsigned(UnpackTtl(rx.GetFlagsTtl())), 12u, "beacon ttl");
            NS_TEST_ASSERT_MSG_EQ((UnpackType(rx.GetFlagsTtl()) == DvClPacketType::BEACON),
                                  true,
                                  "beacon type");
            NS_TEST_ASSERT_MSG_EQ(unsigned(rx.GetSoc()), 63u, "beacon soc");
        }
        // Data round-trip.
        {
            DvClDataHeader tx;
            tx.SetSrc(7);
            tx.SetDst(48);
            tx.SetVia(23);
            tx.SetFlagsTtl(PackFlagsTtl(DvClPacketType::DATA, 9));
            Ptr<Packet> p = Create<Packet>();
            p->AddHeader(tx);
            DvClDataHeader rx;
            p->RemoveHeader(rx);
            NS_TEST_ASSERT_MSG_EQ(rx.GetSrc(), 7, "data src");
            NS_TEST_ASSERT_MSG_EQ(rx.GetDst(), 48, "data dst");
            NS_TEST_ASSERT_MSG_EQ(rx.GetVia(), 23, "data via");
            NS_TEST_ASSERT_MSG_EQ(unsigned(UnpackTtl(rx.GetFlagsTtl())), 9u, "data ttl");
        }
        // TTL cap at 63.
        NS_TEST_ASSERT_MSG_EQ(unsigned(UnpackTtl(PackFlagsTtl(DvClPacketType::DATA, 200))),
                              63u,
                              "ttl capped at 63");
        // DV entries: pack/unpack incl. poison, LE destination.
        {
            std::vector<DvClDvEntry> in = {{0x0102, 90}, {0xBEEF, 0}, {7, 1}};
            uint8_t buf[9];
            const uint32_t n = SerializeDvEntries(in, buf, sizeof(buf));
            NS_TEST_ASSERT_MSG_EQ(n, 9u, "3 entries = 9 bytes");
            NS_TEST_ASSERT_MSG_EQ(buf[0], 0x02, "dest LE low byte");
            NS_TEST_ASSERT_MSG_EQ(buf[1], 0x01, "dest LE high byte");
            NS_TEST_ASSERT_MSG_EQ(buf[5], 0, "poison score preserved");
            std::vector<DvClDvEntry> out;
            DeserializeDvEntries(buf, n, out);
            NS_TEST_ASSERT_MSG_EQ(out.size(), in.size(), "entry count");
            for (size_t i = 0; i < in.size(); ++i)
            {
                NS_TEST_ASSERT_MSG_EQ(out[i].destination, in[i].destination, "entry dest " << i);
                NS_TEST_ASSERT_MSG_EQ(unsigned(out[i].score),
                                      unsigned(in[i].score),
                                      "entry score " << i);
            }
        }
        // Truncation at maxLen and floor(len/3) on partial tails.
        {
            std::vector<DvClDvEntry> in = {{1, 10}, {2, 20}, {3, 30}};
            uint8_t buf[7]; // room for only 2 entries
            const uint32_t n = SerializeDvEntries(in, buf, sizeof(buf));
            NS_TEST_ASSERT_MSG_EQ(n, 6u, "truncated to 2 entries");
            std::vector<DvClDvEntry> out;
            DeserializeDvEntries(buf, 7, out); // 7 bytes -> floor = 2 entries
            NS_TEST_ASSERT_MSG_EQ(out.size(), 2u, "partial tail ignored");
        }
    }
};

/**
 * \ingroup dv-cl
 * The dv-cl-wire test suite.
 */
class DvClWireTestSuite : public TestSuite
{
  public:
    DvClWireTestSuite()
        : TestSuite("dv-cl-wire", Type::UNIT)
    {
        AddTestCase(new DvClWireGoldenBytesTestCase, TestCase::Duration::QUICK);
        AddTestCase(new DvClWireRoundTripTestCase, TestCase::Duration::QUICK);
    }
};

static DvClWireTestSuite g_dvClWireTestSuite; //!< static suite registration

} // namespace test
} // namespace dvcl
} // namespace ns3
