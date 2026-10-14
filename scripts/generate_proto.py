"""Generate and package bindings; run from the repository root."""
from pathlib import Path
from grpc_tools import protoc

output = Path("cranelock_gate/generated")
output.mkdir(exist_ok=True)
(output / "__init__.py").touch()
result = protoc.main(["protoc", "-Iproto/cranelock_gate/v1",
                      "--python_out=" + str(output), "--grpc_python_out=" + str(output),
                      "proto/cranelock_gate/v1/gate.proto"])
if result:
    raise SystemExit(result)
grpc_file = output / "gate_pb2_grpc.py"
grpc_file.write_text(grpc_file.read_text().replace("import gate_pb2 as gate__pb2",
                                                "from . import gate_pb2 as gate__pb2"))
print("Generated protobuf and gRPC bindings")
