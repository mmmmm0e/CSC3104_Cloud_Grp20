"""Regenerate both protocols. Never hand-edit generated modules."""
from pathlib import Path

from grpc_tools import protoc


ROOT = Path(__file__).resolve().parents[1]


def main():
    result = protoc.main([
        "grpc_tools.protoc", f"-I{ROOT / 'proto'}",
        f"--python_out={ROOT}", f"--grpc_python_out={ROOT}",
        str(ROOT / "proto/cranelock/protocol.proto"),
    ])
    if result:
        return result

    output = ROOT / "cranelock_gate/generated"
    output.mkdir(parents=True, exist_ok=True)
    (output / "__init__.py").touch()
    result = protoc.main([
        "grpc_tools.protoc", f"-I{ROOT / 'proto/cranelock_gate/v1'}",
        f"--python_out={output}", f"--grpc_python_out={output}",
        str(ROOT / "proto/cranelock_gate/v1/gate.proto"),
    ])
    if result:
        return result
    grpc_file = output / "gate_pb2_grpc.py"
    grpc_file.write_text(grpc_file.read_text(encoding="utf-8").replace(
        "import gate_pb2 as gate__pb2", "from . import gate_pb2 as gate__pb2"
    ), encoding="utf-8")
    print("Generated crane agent and gate protobuf and gRPC bindings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
