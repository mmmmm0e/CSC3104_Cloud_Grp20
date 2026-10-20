"""Run from the repository root. Never hand-edit generated modules."""
from grpc_tools import protoc

if __name__ == "__main__":
    raise SystemExit(protoc.main([
        "grpc_tools.protoc", "-Iproto", "--python_out=.",
        "--grpc_python_out=.", "proto/cranelock/protocol.proto",
    ]))
