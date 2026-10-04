package net.rudp.test;

import net.rudp.impl.DATSegment;
import net.rudp.impl.RtoEstimator;
import net.rudp.impl.Segment;

/**
 * Tests for the retransmission timeout policy: how a round trip sample becomes
 * an RTO (RFC 6298) and how a segment that keeps timing out has its timeout
 * doubled.
 * <p>
 * This is pure arithmetic with a fake instant passed in, so every expected
 * value here is exact. It exists because the bug this schedule once carried -
 * a backoff multiplier that both callers computed and both callers threw away -
 * is invisible from the outside: the retransmissions still happen, only at the
 * wrong instants.
 */
public class RtoEstimatorTest
{
    public static void main(String[] args)
    {
        Assert.suite("RTO estimator");

        testInitialValue();
        testSampling();
        testSamplingBounds();
        testBackoff();
        testSegmentSchedule();
        testRejectsNonsense();

        System.exit(Assert.report());
    }

    private static void testInitialValue()
    {
        Assert.test("starts out at the profile timeout with no sample", () -> {
            RtoEstimator rto = new RtoEstimator(200);

            Assert.equals("rto", 200, rto.rto());
            Assert.isFalse("hasSample", rto.hasSample());
            Assert.equals("srtt", -1, rto.srtt());
        });
    }

    private static void testSampling()
    {
        Assert.test("the first sample seeds both estimators (RFC 6298 2.2)", () -> {
            RtoEstimator rto = new RtoEstimator(200);
            rto.updateSample(100);

            Assert.isTrue("hasSample", rto.hasSample());
            Assert.equals("srtt", 100, rto.srtt());
            Assert.equals("rttvar", 50, rto.rttvar());
            Assert.equals("rto", 300, rto.rto());
        });

        Assert.test("a later sample is smoothed into the previous ones", () -> {
            // rttvar = (3*50 + |100-200|) / 4 = 62
            // srtt  = (7*100 + 200) / 8       = 112
            // rto   = 112 + 4*62               = 360
            RtoEstimator rto = new RtoEstimator(200);
            rto.updateSample(100);
            rto.updateSample(200);

            Assert.equals("srtt", 112, rto.srtt());
            Assert.equals("rttvar", 62, rto.rttvar());
            Assert.equals("rto", 360, rto.rto());
        });

        Assert.test("a constant round trip time settles on that round trip time", () -> {
            RtoEstimator rto = new RtoEstimator(200);
            for (int i = 0; i < 64; i++) {
                rto.updateSample(100);
            }

            Assert.equals("srtt", 100, rto.srtt());
            Assert.equals("rttvar", 0, rto.rttvar());
            Assert.equals("rto", 100, rto.rto());
        });

        Assert.test("a growing round trip time pulls the estimate up", () -> {
            RtoEstimator steady = new RtoEstimator(200);
            RtoEstimator growing = new RtoEstimator(200);
            for (int i = 0; i < 8; i++) {
                steady.updateSample(100);
                growing.updateSample(100 + i * 100);
            }

            Assert.isTrue("growing srtt above steady srtt", growing.srtt() > steady.srtt());
            Assert.isTrue("growing rto above steady rto", growing.rto() > steady.rto());
        });
    }

    private static void testSamplingBounds()
    {
        Assert.test("never reports an RTO below the floor", () -> {
            RtoEstimator rto = new RtoEstimator(200);
            rto.updateSample(1);

            Assert.equals("srtt", 1, rto.srtt());
            Assert.equals("rto", RtoEstimator.MIN_RTO, rto.rto());
        });

        Assert.test("treats a zero or negative sample as one millisecond", () -> {
            RtoEstimator zero = new RtoEstimator(200);
            zero.updateSample(0);

            RtoEstimator negative = new RtoEstimator(200);
            negative.updateSample(-500);

            Assert.equals("zero srtt", 1, zero.srtt());
            Assert.equals("negative srtt", 1, negative.srtt());
            Assert.equals("zero rto", zero.rto(), negative.rto());
        });

        Assert.test("never reports an RTO above the ceiling", () -> {
            RtoEstimator rto = new RtoEstimator(200);
            rto.updateSample(100000);

            Assert.equals("rto", RtoEstimator.MAX_RTO, rto.rto());
        });
    }

    private static void testBackoff()
    {
        Assert.test("doubles the timeout once per accumulated shift", () -> {
            RtoEstimator rto = new RtoEstimator(1000);

            Assert.equals("shift 0", 1000, rto.rtoFor(0));
            Assert.equals("shift 1", 2000, rto.rtoFor(1));
            Assert.equals("shift 2", 4000, rto.rtoFor(2));
        });

        Assert.test("stops doubling at the ceiling", () -> {
            RtoEstimator rto = new RtoEstimator(1000);

            Assert.equals("shift 3", RtoEstimator.MAX_RTO, rto.rtoFor(3));
            Assert.equals("shift 4", RtoEstimator.MAX_RTO, rto.rtoFor(4));
            Assert.equals("shift 40", RtoEstimator.MAX_RTO, rto.rtoFor(40));
        });

        Assert.test("a timeout-retry shift is what a timed out segment asks for", () -> {
            // The socket caps the shift at MAX_BACKOFF_SHIFT; beyond that the
            // doubling is the caller's business, not the estimator's.
            Assert.equals("cap", 4, RtoEstimator.MAX_BACKOFF_SHIFT);
        });
    }

    private static void testSegmentSchedule()
    {
        Assert.test("the shift a segment accumulates reaches its deadline", () -> {
            /* Walk one segment through four consecutive timeouts and stamp the
               deadline the way the socket does. Every deadline has to be later
               than the one before it: this is exactly what regressed when
               backOffRto() returned a multiplier both callers discarded, and
               the symptom then was a retry at the very same interval as the
               first attempt. */
            Segment segment = new DATSegment(1, 2, new byte[] { 1 }, 0, 1);
            RtoEstimator rto = new RtoEstimator(200);

            long now = 1000;
            long previousDeadline = 0;
            int iterations = RtoEstimator.MAX_BACKOFF_SHIFT + 2;

            for (int timeout = 1; timeout <= iterations; timeout++) {
                int expectedShift = Math.min(timeout - 1, RtoEstimator.MAX_BACKOFF_SHIFT);
                int timeoutMillis = rto.rtoFor(segment.rtoShift());
                segment.markSent(now, timeoutMillis);

                Assert.equals("timeout " + timeout + ": shift", expectedShift,
                        segment.rtoShift());
                Assert.equals("timeout " + timeout + ": timeout", 200 << expectedShift,
                        timeoutMillis);
                Assert.isTrue("timeout " + timeout + ": timeout within the ceiling",
                        timeoutMillis <= RtoEstimator.MAX_RTO);

                if (timeout > 1) {
                    Assert.isTrue("timeout " + timeout + ": deadline moved out",
                            segment.deadline() > previousDeadline);
                }
                Assert.equals("timeout " + timeout + ": deadline", now + timeoutMillis,
                        segment.deadline());

                previousDeadline = segment.deadline();
                segment.backOffRto(RtoEstimator.MAX_BACKOFF_SHIFT);
                now = segment.deadline();
            }
        });

        Assert.test("a fast retransmit asks for no backoff at all", () -> {
            Segment fast = new DATSegment(1, 2, new byte[] { 1 }, 0, 1);
            Segment timedOut = new DATSegment(1, 2, new byte[] { 1 }, 0, 1);

            fast.backOffRto(0);
            timedOut.backOffRto(RtoEstimator.MAX_BACKOFF_SHIFT);

            Assert.equals("fast shift", 0, fast.rtoShift());
            Assert.equals("timed out shift", 1, timedOut.rtoShift());
        });

        Assert.test("acknowledging a segment clears the backoff it had earned", () -> {
            Segment segment = new DATSegment(1, 2, new byte[] { 1 }, 0, 1);
            segment.backOffRto(RtoEstimator.MAX_BACKOFF_SHIFT);
            segment.backOffRto(RtoEstimator.MAX_BACKOFF_SHIFT);

            Assert.equals("shift", 2, segment.rtoShift());

            segment.clearBackOff();

            Assert.equals("shift after the ack", 0, segment.rtoShift());
        });
    }

    private static void testRejectsNonsense()
    {
        Assert.test("rejects a non-positive initial timeout", () -> {
            Assert.throwsExactly("zero", IllegalArgumentException.class,
                    () -> new RtoEstimator(0));
            Assert.throwsExactly("negative", IllegalArgumentException.class,
                    () -> new RtoEstimator(-1));
        });

        Assert.test("rejects a negative backoff shift", () -> {
            RtoEstimator rto = new RtoEstimator(200);

            Assert.throwsExactly("shift -1", IllegalArgumentException.class,
                    () -> rto.rtoFor(-1));
        });
    }
}
