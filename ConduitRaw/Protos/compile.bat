REM protoc --csharp_out=. .\greet.proto
py -m grpc_tools.protoc --proto_path=. --python_out=. --mypy_out=. --grpc_python_out=. .\conduit_raw.proto
copy .\conduit_raw.proto ..\..\conduit_lib\.\conduit_raw.proto
copy conduit_raw_pb2.py ..\..\conduit_lib\conduit_raw_pb2.py
copy conduit_raw_pb2_grpc.py ..\..\conduit_lib\conduit_raw_pb2_grpc.py
copy conduit_raw_pb2.pyi ..\..\conduit_lib\conduit_raw_pb2.pyi 
del conduit_raw_pb2.py
del conduit_raw_pb2_grpc.py
del conduit_raw_pb2.pyi
