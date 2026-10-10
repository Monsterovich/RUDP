#!/bin/bash
#
# Compiles RUDP and runs the integration tests.
#
# Usage: ./run_tests.sh [--extra]
#   --extra  also run the additional suites (server/API contracts, connection
#            lifecycle, reordering, multiplexing, malformed input). Some of
#            them deliberately provoke the library's error logging, so they are
#            opt-in.
#
# The library is compiled with javac rather than ant: RUDP/build.xml sets
# basedir="..", which resolves srcdir to a non-existent <repo>/src, so
# "ant clean build" always fails. It used to fail silently here, because the
# script had no "set -e", and the tests then ran against whatever
# rudp-SNAPSHOT.jar happened to be lying around from an earlier build, with
# the jar ahead of src/ on the classpath so its classes won. A failing build
# therefore still ended in "All tests completed!".
#
# Everything now lands in one output directory that is wiped first, so a
# stale class cannot shadow a source file that moved or was renamed, and the
# run fails loudly instead of reporting a green result it did not earn.
#
# Each test also runs under a timeout. A test that hangs used to take the whole
# run with it - "set -e" has nothing to say about a test that never returns -
# and Ctrl-C did not help either, because ReliableSocket's shutdown hook joins
# threads that may themselves be stuck, so the JVM never got to exit. The kill
# escalates to SIGKILL after a grace period for exactly that reason.

set -e

# Only --extra is understood; anything else is a mistake worth failing on
# before a compile is spent on it.
EXTRA=0
for arg in "$@"; do
    case "$arg" in
        --extra) EXTRA=1 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

cd "$(dirname "$0")/.."

OUT=build_out/classes

rm -rf "$OUT"
mkdir -p "$OUT"

echo "Compiling RUDP library and tests into $OUT..."
javac -nowarn -d "$OUT" $(find RUDP/src -name '*.java')

# The first three are deterministic and need no clock: they either check pure
# functions or drive a connection against a clock they advance by hand, so a
# regression in the segment wire format or in the retransmission schedule is
# reported instead of being absorbed into the timing of a transfer test. They
# run first because they are fast and localise a failure to a single method.
# ConnectTest goes with them: it runs a real connection over loopback, but
# without a peer that can be slow to answer, so it is fast and deterministic.
# CongestionControlTest goes with those two: a real connection over loopback
# whose peer is the test itself, and whose clock is frozen so that what the
# window does is read from the wire rather than raced against a real timer.
TESTS="SegmentParseTest RtoEstimatorTest RetransmissionTest ConnectTest \
CongestionControlTest SimpleClientServerTest MultiplexedClientServerTest DataTransferTest"

# Extra suites, run only with --extra. They are layered on top of a live
# loopback connection: the server and API contracts, the connection state
# machine, the reordering and multiplexing paths, and finally the
# malformed-input cases. They are kept out of the default run because a few of
# them deliberately provoke the library's error logging (a caught exception
# printed with a stack trace), which is part of what they test but would make
# the default output dirty.
EXTRA_TESTS="ProfileTest TimerTest SocketApiTest ServerSocketTest \
ConnectionLifecycleTest ReceiveOrderingTest MultiplexingTest RobustnessTest"

if [ $EXTRA -eq 1 ]; then
    echo "Running the extra suites as well (--extra)..."
    TESTS="$TESTS $EXTRA_TESTS"
fi

# Generous enough for the transfer tests, short enough to not be mistaken for a
# run that is still working.
TEST_TIMEOUT_S=180

first=1
for test in $TESTS; do
    if [ $first -eq 0 ]; then
        echo ""
        echo "=========================================="
    fi
    first=0

    echo ""
    echo "Running $test..."
    timeout --kill-after=10 "$TEST_TIMEOUT_S" \
        java -cp "$OUT" "net.rudp.test.$test"
done

echo ""
echo "All tests completed!"
