#!/usr/bin/env bash
set -e

# protoc.exe --csharp_out=. ./greet.proto
py -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. ./greet.proto
cp greet_pb2.py ../../conduit_lib/greet_pb2.py
cp greet_pb2_grpc.py ../../conduit_lib/greet_pb2_grpc.py
