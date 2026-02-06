#!/bin/bash

cd /Users/jose/ambient_labs/underleaf/mycelium_mesh_agent

echo "Starting MMA server..."
go run ./cmd/serve/main.go > /tmp/mma_test.log 2>&1 &
SERVER_PID=$!

echo "Server PID: $SERVER_PID"
sleep 3

echo ""
echo "Testing /health endpoint:"
curl -s http://localhost:8080/health
echo ""

echo ""
echo "Testing /metrics endpoint:"
curl -s http://localhost:8080/metrics | head -20
echo ""

echo ""
echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Done!"
