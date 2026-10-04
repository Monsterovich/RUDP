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

set -e

cd "$(dirname "$0")/.."

OUT=build_out/classes

rm -rf "$OUT"
mkdir -p "$OUT"

echo "Compiling RUDP library and tests into $OUT..."
javac -nowarn -d "$OUT" $(find RUDP/src -name '*.java')

TESTS="SimpleClientServerTest MultiplexedClientServerTest DataTransferTest"

first=1
for test in $TESTS; do
    if [ $first -eq 0 ]; then
        echo ""
        echo "=========================================="
    fi
    first=0

    echo ""
    echo "Running $test..."
    java -cp "$OUT" "net.rudp.test.$test"
done

echo ""
echo "All tests completed!"
