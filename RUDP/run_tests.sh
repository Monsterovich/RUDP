#!/bin/bash

cd "$(dirname "$0")/.."

echo "Compiling RUDP library..."
ant -f RUDP/build.xml clean build

cd RUDP

echo ""
echo "Compiling test examples..."
javac -cp "rudp-SNAPSHOT.jar" src/net/rudp/test/*.java

echo ""
echo "Running SimpleClientServerTest..."
java -cp "rudp-SNAPSHOT.jar:src" net.rudp.test.SimpleClientServerTest

echo ""
echo "=========================================="
echo ""
echo "Running MultiplexedClientServerTest..."
java -cp "rudp-SNAPSHOT.jar:src" net.rudp.test.MultiplexedClientServerTest

echo ""
echo "=========================================="
echo ""
echo "Running DataTransferTest..."
java -cp "rudp-SNAPSHOT.jar:src" net.rudp.test.DataTransferTest

echo ""
echo "All tests completed!"
