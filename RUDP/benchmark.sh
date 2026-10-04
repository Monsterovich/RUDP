#!/bin/bash
#
# Runs net.rudp.test.Benchmark against a freshly compiled library.
#
# This is deliberately not part of run_tests.sh: the benchmark reports numbers
# that depend on the machine and the run takes minutes, so it is a separate
# command rather than something to sit in the middle of the test pass.
#
# Arguments are passed straight through, e.g.
#   RUDP/benchmark.sh
#   RUDP/benchmark.sh --loss 0.05 --iterations 3
#   RUDP/benchmark.sh --throughput-N 8388608 --latency-N 500 --connect-N 50
#
# Note that RUDP/build.xml sets basedir="..", which resolves srcdir to a
# non-existent <repo>/src, so ant cannot build this project. The library is
# compiled with javac directly here, the way run_tests.sh compiles the tests.

set -e

cd "$(dirname "$0")/.."

OUT=build_out/classes

# Wiped first so that a class left behind by an earlier build cannot shadow a
# renamed or moved source file, which is how run_tests.sh ended up running a
# stale jar once already.
rm -rf "$OUT"
mkdir -p "$OUT"

echo "Compiling RUDP library into $OUT..."
javac -nowarn -d "$OUT" $(find RUDP/src -name '*.java')

echo ""
if [ $# -eq 0 ]; then
    echo "Running Benchmark with default settings..."
else
    echo "Running Benchmark $*..."
fi

# Benchmark formats its numbers with the default locale, so a locale with a
# comma decimal separator would print "178,3" and make the output ambiguous.
exec java -Duser.language=en -Duser.country=US -cp "$OUT" net.rudp.test.Benchmark "$@"
