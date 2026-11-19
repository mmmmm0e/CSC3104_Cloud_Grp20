"""Short, read-only view of who holds the token (for demos).

Asks the Gate (the official record) and each crane agent what they believe,
and prints one line each. It changes nothing.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.integrated import gate_client, peer_channel  # noqa: E402


def main():
    from cranelock import protocol_pb2 as pb
    from cranelock import protocol_pb2_grpc as rpc

    channel, client = gate_client()
    try:
        gate = client.state()
    finally:
        channel.close()
    print(f"GATE (official record): token holder = {gate['holder']}, mode = {gate['mode']}, "
          f"epoch = {gate['epoch']}, revision = {gate['revision']}, lane = {gate['lane_state']}")
    print()
    for crane in ("crane-1", "crane-2", "crane-3"):
        try:
            with peer_channel(crane) as agent_channel:
                state = rpc.CraneAgentStub(agent_channel).GetAgentState(pb.Empty(), timeout=3)
        except Exception:
            print(f"  {crane}:  NOT REACHABLE (stopped or crashed)")
            continue
        marker = "  <== HAS THE TOKEN" if state.has_token else ""
        print(f"  {crane}:  mode = {pb.Mode.Name(state.mode):<9} epoch = {state.epoch:<4} "
              f"thinks holder is {state.token_holder}{marker}")


if __name__ == "__main__":
    main()
