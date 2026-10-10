/*
 * Simple Reliable UDP (rudp)
 * Copyright (c) 2026, Nikolay Borodin (monsterovich@gmail.com)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package net.rudp.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.rudp.impl.Timer;

/**
 * Tests the scheduling primitive every socket timer is built on: lazy start,
 * one-shot versus periodic, and the cancel/reset/destroy transitions.
 * <p>
 * The lazy start is the one with a reason behind it: a ReliableSocket owns
 * four timers, and starting all four at construction would make a socket that
 * exists only to answer a SYN - and never completes the handshake - pay for
 * four native threads, which is what a SYN flood turns into a thread
 * exhaustion. So the test pins both that the thread is not born until the
 * first schedule() and that it still runs once it is.
 */
public class TimerTest
{
    /** Generous enough for a heavily loaded machine, short enough to bound a run. */
    private static final long AWAIT_MS = 5000;

    private static final long GRACE_MS = 200;

    public static void main(String[] args)
    {
        Assert.suite("timer");

        testLazyStart();
        testRunsOnce();
        testRunsPeriodically();
        testScheduleTwiceRejected();
        testIsScheduledReporting();
        testCancelStopsPeriodic();
        testDestroyStops();
        testResetSkipsAPendingTask();

        System.exit(Assert.report());
    }

    private static void testLazyStart()
    {
        Assert.test("the thread is not born until the first schedule", () -> {
            Timer timer = new Timer("test-lazy", () -> { });

            Assert.equals("state before schedule", Thread.State.NEW, timer.getState());
            Assert.isFalse("alive before schedule", timer.isAlive());

            try {
                timer.schedule(0);

                /*
                 * The start is synchronous with schedule(), so the thread
                 * exists by the time it returns; only its first instruction
                 * races us.
                 */
                long deadline = System.currentTimeMillis() + AWAIT_MS;
                while (!timer.isAlive() && System.currentTimeMillis() < deadline) {
                    pause(1);
                }

                Assert.isTrue("alive after schedule", timer.isAlive());
            }
            finally {
                timer.destroy();
            }
        });
    }

    private static void testRunsOnce()
    {
        Assert.test("a schedule without a period runs its task exactly once", () -> {
            AtomicInteger runs = new AtomicInteger();
            CountDownLatch first = new CountDownLatch(1);
            Timer timer = new Timer("test-once", () -> {
                runs.incrementAndGet();
                first.countDown();
            });

            try {
                timer.schedule(0);
                Assert.isTrue("the task ran", first.await(AWAIT_MS, TimeUnit.MILLISECONDS));

                pause(GRACE_MS);

                Assert.equals("runs", 1, runs.get());
            }
            finally {
                timer.destroy();
            }
        });
    }

    private static void testRunsPeriodically()
    {
        Assert.test("a period keeps re-arming the task", () -> {
            AtomicInteger runs = new AtomicInteger();
            CountDownLatch three = new CountDownLatch(3);
            Timer timer = new Timer("test-periodic", () -> {
                runs.incrementAndGet();
                three.countDown();
            });

            try {
                timer.schedule(0, 20);
                Assert.isTrue("the task ran at least three times",
                        three.await(AWAIT_MS, TimeUnit.MILLISECONDS));

                Assert.isTrue("runs >= 3 (" + runs.get() + ")", runs.get() >= 3);
            }
            finally {
                timer.destroy();
            }
        });
    }

    private static void testScheduleTwiceRejected()
    {
        Assert.test("arming an already armed timer is a programming error", () -> {
            /*
             * A single Timer cannot be two timers. The socket arms each of its
             * four once and lets the task re-arm it, so a second arm is a bug
             * in the caller and is reported rather than silently replacing the
             * first schedule.
             */
            Timer timer = new Timer("test-twice", () -> { });

            try {
                timer.schedule(AWAIT_MS, AWAIT_MS);

                Assert.throwsExactly("second schedule", IllegalStateException.class,
                        () -> timer.schedule(1));
            }
            finally {
                timer.destroy();
            }
        });
    }

    private static void testIsScheduledReporting()
    {
        Assert.test("reports whether it is armed and idle", () -> {
            Timer timer = new Timer("test-scheduled", () -> { });

            try {
                Assert.isFalse("scheduled before", timer.isScheduled());
                Assert.isTrue("idle before", timer.isIdle());

                timer.schedule(AWAIT_MS, AWAIT_MS);

                Assert.isTrue("scheduled after", timer.isScheduled());
                Assert.isFalse("idle after", timer.isIdle());

                timer.cancel();

                Assert.isFalse("scheduled after cancel", timer.isScheduled());
                Assert.isTrue("idle after cancel", timer.isIdle());
            }
            finally {
                timer.destroy();
            }
        });
    }

    private static void testCancelStopsPeriodic()
    {
        Assert.test("cancel stops a periodic task", () -> {
            AtomicInteger runs = new AtomicInteger();
            CountDownLatch started = new CountDownLatch(1);
            Timer timer = new Timer("test-cancel", () -> {
                runs.incrementAndGet();
                started.countDown();
            });

            try {
                timer.schedule(0, 20);
                Assert.isTrue("the task started",
                        started.await(AWAIT_MS, TimeUnit.MILLISECONDS));

                timer.cancel();

                /*
                 * The task may be mid-run when cancel() lands, so the count is
                 * read after cancel rather than before and observed not to
                 * grow.
                 */
                pause(GRACE_MS);
                int settled = runs.get();

                pause(GRACE_MS);

                Assert.equals("runs after cancel", settled, runs.get());
            }
            finally {
                timer.destroy();
            }
        });
    }

    private static void testDestroyStops()
    {
        Assert.test("destroy stops the task and the thread", () -> {
            AtomicInteger runs = new AtomicInteger();
            CountDownLatch started = new CountDownLatch(1);
            Timer timer = new Timer("test-destroy", () -> {
                runs.incrementAndGet();
                started.countDown();
            });

            timer.schedule(0, 20);

            Assert.isTrue("the task started", started.await(AWAIT_MS, TimeUnit.MILLISECONDS));

            timer.destroy();

            pause(GRACE_MS);
            int settled = runs.get();

            pause(GRACE_MS);

            Assert.equals("runs after destroy", settled, runs.get());
        });
    }

    private static void testResetSkipsAPendingTask()
    {
        Assert.test("a reset before a pending deadline skips that run", () -> {
            /*
             * reset() is what a keep-alive timer uses to push its deadline out
             * again on every segment it sees: the task must not fire for the
             * delay that was cancelled. The delay is long enough that the
             * reset lands well inside it, so this does not depend on how the
             * thread is scheduled against the test.
             */
            AtomicInteger runs = new AtomicInteger();
            Timer timer = new Timer("test-reset", runs::incrementAndGet);

            try {
                timer.schedule(400);
                pause(50);
                timer.reset();

                pause(600);

                Assert.equals("runs", 0, runs.get());
            }
            finally {
                timer.destroy();
            }
        });
    }

    private static void pause(long millis)
    {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException xcp) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting");
        }
    }
}
