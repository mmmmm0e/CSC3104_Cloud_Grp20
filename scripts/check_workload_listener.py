"""Check workload mTLS and service isolation without requesting a usable grant."""
import argparse
import json
from pathlib import Path

import grpc

from integrated import BASE, peer_channel
from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from scripts.workload_credentials import CRANES


def expect_status(call, expected):
    try:
        call()
    except grpc.RpcError as error:
        if error.code() != expected:
            raise ValueError(f'Expected {expected.name}, got {error.code().name}') from None
    else:
        raise ValueError(f'Expected {expected.name}, but the RPC succeeded')


def check(config, negative, *, check_docker_peer=False):
    options = json.loads(config.read_text())
    if set(options['agents']) != set(CRANES):
        raise ValueError('Expected one workload endpoint per crane')
    ca, cert, key = (Path(options[field]).read_bytes() for field in ('ca', 'cert', 'key'))
    approved = grpc.ssl_channel_credentials(ca, key, cert)
    unapproved = grpc.ssl_channel_credentials(ca, (negative / 'client-key.pem').read_bytes(),
                                              (negative / 'client.pem').read_bytes())
    anonymous = grpc.ssl_channel_credentials(ca)
    # The server rejects this deliberately invalid UUID before persisting a binding
    # or making Gate calls. A real grant is exercised by external_workload.py.
    request = pb.WorkloadGrantRequest(request_id='listener-auth-probe')
    for crane, address in options['agents'].items():
        with grpc.secure_channel(address, approved) as channel:
            reply = rpc.CraneWorkloadStub(channel).RequestGrant(request, timeout=5)
            if reply.allowed or reply.reason != 'INVALID_REQUEST_ID':
                raise ValueError(f'{crane}: approved identity did not reach workload validation')
            expect_status(lambda: rpc.CraneAgentStub(channel).GetAgentState(pb.Empty(), timeout=3),
                          grpc.StatusCode.UNIMPLEMENTED)
        for credentials, status in ((unapproved, grpc.StatusCode.PERMISSION_DENIED),
                                    (anonymous, grpc.StatusCode.UNAVAILABLE)):
            with grpc.secure_channel(address, credentials) as channel:
                expect_status(lambda: rpc.CraneWorkloadStub(channel).RequestGrant(request, timeout=3), status)
        if check_docker_peer:
            with peer_channel(crane) as channel:
                peer = rpc.CraneAgentStub(channel).GetAgentState(pb.Empty(), timeout=3)
                if peer.agent_id != crane:
                    raise ValueError(f'{crane}: peer listener returned an unexpected agent')
                expect_status(lambda: rpc.CraneWorkloadStub(channel).RequestGrant(request, timeout=3),
                              grpc.StatusCode.UNIMPLEMENTED)
        print(f'PASS {crane}: approved identity, wrong identity, missing certificate and RPC separation')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workload-config', type=Path, default=BASE / 'workload/config.json')
    parser.add_argument('--negative-credentials', type=Path, default=BASE / 'workload/negative-test')
    parser.add_argument('--docker-peer', action='store_true', help='Also check published Compose peer ports')
    args = parser.parse_args()
    try:
        check(args.workload_config, args.negative_credentials, check_docker_peer=args.docker_peer)
    except (OSError, ValueError, grpc.RpcError) as error:
        parser.exit(1, f'{error}\n')
