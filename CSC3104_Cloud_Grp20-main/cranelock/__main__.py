import argparse
import asyncio
import json
import grpc
from google.protobuf.json_format import MessageToDict
from .agent import Agent, Mode
from . import protocol_pb2 as pb
from . import protocol_pb2_grpc as rpc


async def main():
    parser = argparse.ArgumentParser(description="CraneLock normal-operation demo")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--id", required=True)
    run.add_argument("--listen", required=True)
    run.add_argument("--peer", action="append", default=[], metavar="ID=HOST:PORT")
    run.add_argument("--holder", required=True)
    run.add_argument("--epoch", type=int, default=1)
    run.add_argument("--mode", choices=["CENTRAL", "LOCAL"], default="LOCAL")
    run.add_argument("--interval", type=float, default=0.5)
    run.add_argument("--timeout", type=float, default=2)
    run.add_argument("--state-file", required=True)
    run.add_argument("--request-after", type=float)
    state = commands.add_parser("state")
    state.add_argument("address")
    args = parser.parse_args()
    if args.command == "state":
        async with grpc.aio.insecure_channel(args.address) as channel:
            result = await rpc.CraneAgentStub(channel).GetAgentState(pb.Empty(), timeout=3)
            print(json.dumps(MessageToDict(result, preserving_proto_field_name=True,
                                          always_print_fields_with_no_presence=True), indent=2))
        return
    peers = dict(p.split("=", 1) for p in args.peer)
    if len(peers) != len(args.peer):
        parser.error("Duplicate peer IDs")
    agent = Agent(args.id, peers, args.holder, epoch=args.epoch, mode=Mode[args.mode],
                  interval=args.interval, timeout=args.timeout, state_file=args.state_file)
    try:
        await agent.start(args.listen)
        if args.request_after is not None:
            await asyncio.sleep(args.request_after)
            reply = await agent.request_token()
            agent.emit("TOKEN_REQUEST_RESULT", accepted=reply.accepted, reason=reply.reason)
        await asyncio.Event().wait()
    finally:
        await agent.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
