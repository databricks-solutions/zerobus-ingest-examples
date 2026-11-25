#!/usr/bin/env bash
# Regenerate the gRPC stubs in src/zerobus_mock_server/pb from proto/zerobus_service.proto.
# Needs grpcio-tools. Run from the project root:  scripts/gen_proto.sh
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=src/zerobus_mock_server/pb
python -m grpc_tools.protoc -I proto --python_out="$OUT" --grpc_python_out="$OUT" proto/zerobus_service.proto
# grpc_tools writes absolute imports; make them relative so the package imports cleanly.
sed -i 's/^import zerobus_service_pb2 as/from . import zerobus_service_pb2 as/' "$OUT/zerobus_service_pb2_grpc.py"
touch "$OUT/__init__.py"
echo "generated $OUT"
