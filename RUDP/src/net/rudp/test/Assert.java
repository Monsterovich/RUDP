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
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package net.rudp.test;

import java.util.ArrayList;
import java.util.List;

/**
 * Minimal assertion harness for the RUDP tests.
 *
 * The project's tests are plain main() classes, so there is no JUnit on the
 * classpath and nothing collects results for us: a check either prints or it
 * is invisible. This helper keeps every check counted and every failure
 * reported with the name of the case it belongs to, and turns the tally into
 * the process exit code, so run_tests.sh fails on a red test the same way it
 * fails on a stack trace.
 */
final class Assert
{
    /** A test body that is allowed to throw anything. */
    interface Body
    {
        void run() throws Throwable;
    }

    static void suite(String name)
    {
        _suite = name;
        System.out.println("=== " + name + " ===");
    }

    static void test(String name, Body body)
    {

        try {
            body.run();
            _checks++;
            System.out.println("  [ok]   " + name);
        }
        catch (Throwable t) {
            _failures.add(name + ": " + t);
            System.out.println("  [FAIL] " + name + " -> " + t);
            for (StackTraceElement e : t.getStackTrace()) {
                if (e.getClassName().startsWith("net.rudp.")) {
                    System.out.println("           at " + e);
                }
            }
        }
    }

    static void isTrue(String what, boolean condition)
    {
        if (!condition) {
            throw new AssertionError(what + " is false");
        }
    }

    static void isFalse(String what, boolean condition)
    {
        isTrue(what, !condition);
    }

    static void notNull(String what, Object value)
    {
        if (value == null) {
            throw new AssertionError(what + " is null");
        }
    }

    static void equals(String what, long expected, long actual)
    {
        if (expected != actual) {
            throw new AssertionError(what + ": expected " + expected + ", got " + actual);
        }
    }

    static void equals(String what, Object expected, Object actual)
    {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(what + ": expected " + expected + ", got " + actual);
        }
    }

    static void arrayEquals(String what, byte[] expected, byte[] actual)
    {
        notNull(what, actual);

        if (expected.length != actual.length) {
            throw new AssertionError(what + ": expected " + expected.length +
                    " bytes, got " + actual.length);
        }

        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                throw new AssertionError(what + ": byte " + i + " expected " +
                        (expected[i] & 0xFF) + ", got " + (actual[i] & 0xFF));
            }
        }
    }

    static void arrayEquals(String what, int[] expected, int[] actual)
    {
        notNull(what, actual);

        if (expected.length != actual.length) {
            throw new AssertionError(what + ": expected " + expected.length +
                    " values, got " + actual.length);
        }

        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                throw new AssertionError(what + ": value " + i + " expected " +
                        expected[i] + ", got " + actual[i]);
            }
        }
    }

    /**
     * Runs the body and requires it to throw something of the given type.
     * Subclasses count, so a more specific protocol error still satisfies the
     * check; what must not slip through is a different failure - a truncated
     * or malformed segment has to be rejected with IllegalArgumentException,
     * not with the ArrayIndexOutOfBoundsException that an unchecked buffer
     * access produces.
     */
    static void throwsExactly(String what, Class<? extends Throwable> expected, Body body)
    {
        try {
            body.run();
        }
        catch (Throwable t) {
            if (expected.isInstance(t)) {
                return;
            }

            throw new AssertionError(what + ": expected " + expected.getSimpleName() +
                    ", got " + t.getClass().getName() + " (" + t.getMessage() + ")");
        }

        throw new AssertionError(what + ": expected " + expected.getSimpleName() +
                ", nothing was thrown");
    }

    static int report()
    {
        System.out.println("");

        if (_failures.isEmpty()) {
            System.out.println(_suite + ": " + _checks + " checks passed");
            return 0;
        }

        System.out.println(_suite + ": " + _failures.size() + " of " +
                (_checks + _failures.size()) + " checks FAILED");
        for (String f : _failures) {
            System.out.println("  - " + f);
        }
        return 1;
    }

    private static String _suite = "tests";
    private static int _checks;
    private static final List<String> _failures = new ArrayList<String>();

    private Assert()
    {
    }
}
