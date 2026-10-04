package net.rudp.test;

import java.util.Arrays;

import net.rudp.impl.ACKSegment;
import net.rudp.impl.DATSegment;
import net.rudp.impl.EAKSegment;
import net.rudp.impl.FINSegment;
import net.rudp.impl.NULSegment;
import net.rudp.impl.RSTSegment;
import net.rudp.impl.SYNSegment;
import net.rudp.impl.Segment;

/**
 * Tests for the segment wire format: every segment type has to survive a
 * serialize/parse round trip unchanged, and a malformed one has to be
 * rejected as a protocol error.
 * <p>
 * These need no sockets and no threads, which is what makes them worth having
 * first: the parse path is reached with whatever bytes a datagram happened to
 * carry, so a bug there surfaces either as a crash in the socket reader thread
 * or, worse, as a segment that decodes into the wrong type and quietly
 * corrupts the stream.
 */
public class SegmentParseTest
{
    private static final int SYN_LEN = Segment.RUDP_HEADER_LEN + 16;

    public static void main(String[] args)
    {
        Assert.suite("Segment.parse");

        testHeaderLayout();
        testRoundTrips();
        testTypeSelection();
        testOffsetParsing();
        testMalformedInput();
        testBookkeeping();

        System.exit(Assert.report());
    }

    private static void testHeaderLayout()
    {
        Assert.test("writes the header into the documented byte positions", () -> {
            byte[] bytes = new ACKSegment(0x21, 0x37).getBytes();

            Assert.equals("length", Segment.RUDP_HEADER_LEN, bytes.length);
            Assert.equals("flags", 0x40, bytes[0] & 0xFF);
            Assert.equals("header length", 6, bytes[1] & 0xFF);
            Assert.equals("sequence number", 0x21, bytes[2] & 0xFF);
            Assert.equals("ack number", 0x37, bytes[3] & 0xFF);
            Assert.equals("checksum high", 0, bytes[4] & 0xFF);
            Assert.equals("checksum low", 0, bytes[5] & 0xFF);
        });

        Assert.test("writeTo honours the offset and reports what it wrote", () -> {
            byte[] buffer = new byte[Segment.RUDP_HEADER_LEN + 4];
            Arrays.fill(buffer, (byte) 0x5A);

            int written = new ACKSegment(1, 2).writeTo(buffer, 4);

            Assert.equals("written", Segment.RUDP_HEADER_LEN, written);
            Assert.equals("flags", 0x40, buffer[4] & 0xFF);
            Assert.equals("sequence number", 1, buffer[6] & 0xFF);
            for (int i = 0; i < 4; i++) {
                Assert.equals("untouched prefix byte " + i, 0x5A, buffer[i] & 0xFF);
            }
        });

        Assert.test("getBytes and writeTo agree for every segment type", () -> {
            for (Segment s : allTypes()) {
                byte[] viaGetBytes = s.getBytes();
                byte[] viaWriteTo = new byte[viaGetBytes.length];
                int written = s.writeTo(viaWriteTo, 0);

                Assert.equals(s.type() + ": writeTo length", viaGetBytes.length, written);
                Assert.arrayEquals(s.type() + ": bytes", viaGetBytes, viaWriteTo);
            }
        });

        Assert.test("writeTo into a dirty buffer matches a fresh serialization", () -> {
            // The send path reuses one buffer for every packet, so a segment
            // must fully overwrite the bytes it occupies and must not depend on
            // what the previous packet left there.
            for (Segment s : allTypes()) {
                byte[] buffer = new byte[64];
                Arrays.fill(buffer, (byte) 0xFF);

                int written = s.writeTo(buffer, 0);

                Assert.equals(s.type() + ": length", s.length(), written);
                Assert.arrayEquals(s.type() + ": bytes", s.getBytes(),
                        Arrays.copyOf(buffer, written));
            }
        });
    }

    private static void testRoundTrips()
    {
        Assert.test("round-trips every segment type", () -> {
            for (Segment s : allTypes()) {
                Segment parsed = Segment.parse(s.getBytes());

                Assert.equals(s.type() + ": type", s.type(), parsed.type());
                Assert.equals(s.type() + ": class", s.getClass(), parsed.getClass());
                Assert.equals(s.type() + ": sequence number", s.seq(), parsed.seq());
                Assert.equals(s.type() + ": ack number", s.getAck(), parsed.getAck());
                Assert.equals(s.type() + ": length", s.length(), parsed.length());
            }
        });

        Assert.test("round-trips a DAT payload", () -> {
            byte[] payload = new byte[256];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) i;
            }

            Segment parsed = Segment.parse(
                    new DATSegment(5, 6, payload, 0, payload.length).getBytes());

            Assert.equals("length", Segment.RUDP_HEADER_LEN + payload.length, parsed.length());
            Assert.arrayEquals("payload", payload, ((DATSegment) parsed).getData());
        });

        Assert.test("a DAT copies only the requested slice of its source", () -> {
            byte[] source = new byte[] { 9, 9, 1, 2, 3, 4, 9, 9 };
            DATSegment dat = new DATSegment(1, 2, source, 2, 4);
            byte[] expected = new byte[] { 1, 2, 3, 4 };

            Assert.arrayEquals("slice", expected, dat.getData());
            Assert.arrayEquals("slice survives the round trip", expected,
                    ((DATSegment) Segment.parse(dat.getBytes())).getData());
        });

        Assert.test("round-trips an EAK out-of-sequence list", () -> {
            int[] acks = new int[] { 1, 2, 3, 128, 254 };
            Segment parsed = Segment.parse(new EAKSegment(200, 201, acks).getBytes());

            Assert.equals("length", Segment.RUDP_HEADER_LEN + acks.length, parsed.length());
            Assert.arrayEquals("out of sequence acks", acks, ((EAKSegment) parsed).getACKs());
        });

        Assert.test("round-trips the negotiated parameters of a SYN", () -> {
            SYNSegment syn = new SYNSegment(42, 64, 1200, 200, 300, 2000, 3, 4, 5, 6);
            SYNSegment parsed = (SYNSegment) Segment.parse(syn.getBytes());

            Assert.equals("version", Segment.RUDP_VERSION, parsed.getVersion());
            Assert.equals("max outstanding segments", 64, parsed.getMaxOutstandingSegments());
            Assert.equals("option flags", 0x01, parsed.getOptionFlags());
            Assert.equals("max segment size", 1200, parsed.getMaxSegmentSize());
            Assert.equals("retransmission timeout", 200, parsed.getRetransmissionTimeout());
            Assert.equals("cumulative ack timeout", 300, parsed.getCummulativeAckTimeout());
            Assert.equals("null segment timeout", 2000, parsed.getNulSegmentTimeout());
            Assert.equals("max retransmissions", 3, parsed.getMaxRetransmissions());
            Assert.equals("max cumulative acks", 4, parsed.getMaxCumulativeAcks());
            Assert.equals("max out of sequence", 5, parsed.getMaxOutOfSequence());
            Assert.equals("max auto reset", 6, parsed.getMaxAutoReset());
        });

        Assert.test("round-trips the extremes of the 8-bit sequence space", () -> {
            roundTrip(new ACKSegment(0, 0));
            roundTrip(new ACKSegment(255, 255));
            roundTrip(new NULSegment(128));
            roundTrip(new EAKSegment(255, 254, new int[] { 0, 128, 255 }));
            roundTrip(new SYNSegment(200, 64, 1200, 200, 300, 2000, 3, 3, 3, 3));
        });
    }

    private static void testTypeSelection()
    {
        Assert.test("picks the segment type from the flag bits", () -> {
            assertType("a bare SYN", "SYN", Segment.SYN_FLAG | Segment.ACK_FLAG, SYN_LEN);
            assertType("a bare NUL", "NUL", Segment.NUL_FLAG, Segment.RUDP_HEADER_LEN);
            assertType("a bare RST", "RST", Segment.RST_FLAG | Segment.ACK_FLAG,
                    Segment.RUDP_HEADER_LEN);
            assertType("a bare FIN", "FIN", Segment.FIN_FLAG | Segment.ACK_FLAG,
                    Segment.RUDP_HEADER_LEN);
            assertType("a bare ACK", "ACK", Segment.ACK_FLAG, Segment.RUDP_HEADER_LEN);
            assertType("an ACK with a payload", "DAT", Segment.ACK_FLAG,
                    Segment.RUDP_HEADER_LEN + 1);
            assertType("an EAK", "EAK", Segment.EAK_FLAG, Segment.RUDP_HEADER_LEN + 1);
        });

        Assert.test("resolves conflicting flags in a fixed order", () -> {
            // No peer sets two of these at once, but the parser still has to
            // pick one deterministically instead of falling through to a
            // payload-length guess. The order is SYN, NUL, EAK, RST, FIN, ACK.
            assertType("SYN outranks everything", "SYN", 0xFF, SYN_LEN);
            assertType("NUL outranks RST", "NUL", Segment.NUL_FLAG | Segment.RST_FLAG,
                    Segment.RUDP_HEADER_LEN);
            assertType("EAK outranks RST", "EAK", Segment.EAK_FLAG | Segment.RST_FLAG,
                    Segment.RUDP_HEADER_LEN + 1);
            assertType("RST outranks FIN", "RST", Segment.RST_FLAG | Segment.FIN_FLAG,
                    Segment.RUDP_HEADER_LEN);
            assertType("FIN outranks ACK", "FIN", Segment.FIN_FLAG | Segment.ACK_FLAG,
                    Segment.RUDP_HEADER_LEN);
        });

        Assert.test("an ACK flag alone means DAT as soon as a payload follows", () -> {
            // ACK and DAT share ACK_FLAG and are told apart by length only.
            byte[] ack = new ACKSegment(1, 2).getBytes();
            byte[] dat = new DATSegment(1, 2, new byte[] { 42 }, 0, 1).getBytes();

            Assert.equals("6 bytes", "ACK", Segment.parse(ack).type());
            Assert.equals("7 bytes", "DAT", Segment.parse(dat).type());
        });

        Assert.test("treats sequence and ack numbers as unsigned bytes", () -> {
            // Sequence numbers live in an 8-bit space, so 200 is an ordinary
            // sequence number and must not come back as -56.
            Segment parsed = Segment.parse(new ACKSegment(200, 254).getBytes());

            Assert.equals("sequence number", 200, parsed.seq());
            Assert.equals("ack number", 254, parsed.getAck());
        });
    }

    private static void testOffsetParsing()
    {
        Assert.test("parses a segment embedded at an offset", () -> {
            byte[] dat = new DATSegment(3, 4, new byte[] { 10, 20, 30 }, 0, 3).getBytes();
            byte[] padded = new byte[7 + dat.length + 5];
            System.arraycopy(dat, 0, padded, 7, dat.length);

            Segment parsed = Segment.parse(padded, 7, dat.length);

            Assert.equals("type", "DAT", parsed.type());
            Assert.arrayEquals("payload", new byte[] { 10, 20, 30 },
                    ((DATSegment) parsed).getData());
        });

        Assert.test("ignores bytes past the declared length", () -> {
            byte[] dat = new DATSegment(3, 4, new byte[] { 10, 20, 30 }, 0, 3).getBytes();
            byte[] withTrailer = new byte[dat.length + 4];
            System.arraycopy(dat, 0, withTrailer, 0, dat.length);

            Segment parsed = Segment.parse(withTrailer, 0, dat.length);

            Assert.equals("length", dat.length, parsed.length());
            Assert.arrayEquals("payload", new byte[] { 10, 20, 30 },
                    ((DATSegment) parsed).getData());
        });
    }

    private static void testMalformedInput()
    {
        Assert.test("rejects anything shorter than a header", () -> {
            for (int len = 0; len < Segment.RUDP_HEADER_LEN; len++) {
                byte[] bytes = new byte[len];
                if (len > 0) {
                    bytes[0] = Segment.ACK_FLAG;
                }

                Assert.throwsExactly("length " + len, IllegalArgumentException.class,
                        () -> Segment.parse(bytes));
            }
        });

        Assert.test("rejects a declared length that does not fit the buffer", () -> {
            byte[] dat = new DATSegment(3, 4, new byte[] { 10, 20, 30 }, 0, 3).getBytes();

            Assert.throwsExactly("length past the end", IllegalArgumentException.class,
                    () -> Segment.parse(dat, 0, dat.length + 1));
            Assert.throwsExactly("offset past the end", IllegalArgumentException.class,
                    () -> Segment.parse(dat, dat.length - 2, Segment.RUDP_HEADER_LEN));
            Assert.throwsExactly("negative offset", IllegalArgumentException.class,
                    () -> Segment.parse(dat, -1, Segment.RUDP_HEADER_LEN));
            Assert.throwsExactly("negative length", IllegalArgumentException.class,
                    () -> Segment.parse(dat, 0, -1));
        });

        Assert.test("rejects a datagram with no recognized flag", () -> {
            Assert.throwsExactly("all flags clear", IllegalArgumentException.class,
                    () -> Segment.parse(raw(0x00, Segment.RUDP_HEADER_LEN)));
            Assert.throwsExactly("only the checksum flag", IllegalArgumentException.class,
                    () -> Segment.parse(raw(Segment.CHK_FLAG, Segment.RUDP_HEADER_LEN)));
            Assert.throwsExactly("only the spare bit", IllegalArgumentException.class,
                    () -> Segment.parse(raw(0x01, Segment.RUDP_HEADER_LEN)));
        });

        Assert.test("rejects an EAK that carries no ack numbers", () -> {
            // An EAK with an empty list claims to be missing nothing, which
            // contradicts its own type. It used to reach the ack array loop and
            // fail with an index error, which is indistinguishable from memory
            // corruption in a log.
            byte[] bytes = new EAKSegment(1, 2, new int[0]).getBytes();

            Assert.isTrue("the segment still claims the EAK flag",
                    (bytes[0] & Segment.EAK_FLAG) != 0);
            Assert.throwsExactly("an empty EAK", IllegalArgumentException.class,
                    () -> Segment.parse(bytes));
        });

        Assert.test("rejects a SYN too short to hold its parameters", () -> {
            byte[] full = new SYNSegment(1, 64, 1200, 200, 300, 2000, 3, 3, 3, 3).getBytes();
            byte[] truncated = Arrays.copyOf(full, Segment.RUDP_HEADER_LEN + 4);

            Assert.throwsExactly("a short SYN", IllegalArgumentException.class,
                    () -> Segment.parse(truncated));
        });

        Assert.test("rejects a SYN from an unsupported protocol version", () -> {
            byte[] bytes = new SYNSegment(1, 64, 1200, 200, 300, 2000, 3, 3, 3, 3).getBytes();
            bytes[4] = (byte) ((9 << 4) & 0xFF);

            Assert.throwsExactly("a foreign version", IllegalArgumentException.class,
                    () -> Segment.parse(bytes));
        });
    }

    private static void testBookkeeping()
    {
        Assert.test("setAck raises the ACK flag without disturbing the others", () -> {
            NULSegment nul = new NULSegment(1);

            Assert.equals("ack before setAck", -1, nul.getAck());
            Assert.equals("ACK flag before setAck", 0, nul.flags() & Segment.ACK_FLAG);

            nul.setAck(1234);

            Assert.equals("ack after setAck", 1234, nul.getAck());
            Assert.equals("ACK flag after setAck", Segment.ACK_FLAG,
                    nul.flags() & Segment.ACK_FLAG);
            Assert.equals("NUL flag kept", Segment.NUL_FLAG, nul.flags() & Segment.NUL_FLAG);
        });

        Assert.test("a fresh control segment has no ack number", () -> {
            Assert.equals("NUL", -1, new NULSegment(1).getAck());
            Assert.equals("FIN", -1, new FINSegment(1).getAck());
            Assert.equals("RST", -1, new RSTSegment(1).getAck());
        });

        Assert.test("stamps a deadline relative to the instant of the send", () -> {
            DATSegment dat = new DATSegment(1, 2, new byte[] { 1 }, 0, 1);

            Assert.equals("deadline before the first send", Long.MAX_VALUE, dat.deadline());

            dat.markSent(1000000L, 200);

            Assert.equals("sent time", 1000000L, dat.sentTime());
            Assert.equals("deadline", 1000200L, dat.deadline());
        });

        Assert.test("accumulates at most the permitted backoff doublings", () -> {
            // The cap is the point of the doubling: a segment nobody ever
            // acknowledges has to stop asking to be retried.
            DATSegment dat = new DATSegment(1, 2, new byte[] { 1 }, 0, 1);
            Assert.equals("initial shift", 0, dat.rtoShift());

            for (int i = 1; i <= 10; i++) {
                dat.backOffRto(4);
                Assert.equals("shift after " + i + " timeouts", Math.min(i, 4), dat.rtoShift());
            }

            dat.clearBackOff();
            Assert.equals("shift after clearBackOff", 0, dat.rtoShift());
        });

        Assert.test("a shift of zero leaves the backoff alone", () -> {
            // This is how a fast retransmission asks for no backoff at all.
            DATSegment dat = new DATSegment(1, 2, new byte[] { 1 }, 0, 1);
            dat.backOffRto(0);

            Assert.equals("shift", 0, dat.rtoShift());
        });

        Assert.test("remembers that a segment was already retransmitted", () -> {
            DATSegment dat = new DATSegment(1, 2, new byte[] { 1 }, 0, 1);

            Assert.isFalse("before", dat.wasRetransmitted());
            Assert.isFalse("acked before", dat.isAcked());

            dat.markRetransmitted();
            dat.markAcked();

            Assert.isTrue("after", dat.wasRetransmitted());
            Assert.isTrue("acked after", dat.isAcked());
        });

        Assert.test("counts retransmissions", () -> {
            DATSegment dat = new DATSegment(1, 2, new byte[] { 1 }, 0, 1);

            Assert.equals("initial count", 0, dat.getRetxCounter());

            dat.setRetxCounter(3);
            Assert.equals("count", 3, dat.getRetxCounter());
        });
    }

    private static Segment[] allTypes()
    {
        return new Segment[] {
            new ACKSegment(1, 2),
            new DATSegment(3, 4, new byte[] { 1, 2, 3, 4, 5 }, 0, 5),
            new EAKSegment(5, 6, new int[] { 7, 8 }),
            new SYNSegment(9, 64, 1200, 200, 300, 2000, 3, 3, 3, 3),
            new NULSegment(10),
            new FINSegment(11),
            new RSTSegment(12),
        };
    }

    private static void roundTrip(Segment s)
    {
        Segment parsed = Segment.parse(s.getBytes());

        Assert.equals(s.type() + ": type", s.type(), parsed.type());
        Assert.equals(s.type() + ": sequence number", s.seq(), parsed.seq());
        Assert.equals(s.type() + ": ack number", s.getAck(), parsed.getAck());
    }

    private static void assertType(String what, String expectedType, int flags, int len)
    {
        Assert.equals(what + " (flags 0x" + Integer.toHexString(flags) + ", length " + len + ")",
                expectedType, Segment.parse(raw(flags, len)).type());
    }

    /**
     * Builds a datagram of the given length that carries nothing but the given
     * flag bits and a non-zero filler, i.e. a packet a peer could plausibly
     * put on the wire but that this library never builds itself.
     */
    private static byte[] raw(int flags, int len)
    {
        byte[] bytes = new byte[len];
        Arrays.fill(bytes, (byte) 0x01);
        bytes[0] = (byte) flags;
        bytes[1] = (byte) len;

        if ((flags & Segment.SYN_FLAG) != 0) {
            // A SYN is the one type whose body has to be meaningful to decode.
            bytes[4] = (byte) (Segment.RUDP_VERSION << 4);
        }

        return bytes;
    }
}
