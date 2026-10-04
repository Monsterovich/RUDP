#!/bin/bash
#
# Compiles RUDP and runs the integration tests.
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
TESTS="SegmentParseTest RtoEstimatorTest RetransmissionTest ConnectTest \
SimpleClientServerTest MultiplexedClientServerTest DataTransferTest"

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
