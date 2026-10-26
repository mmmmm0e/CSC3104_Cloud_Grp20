import asyncio
import json

import grpc
import pytest

from cranelock import protocol_pb2 as pb
from cranelock_sim.agents import AgentCluster, run_agent_demo


# models a lost acknowledgement after the receiver accepts
@pytest.mark.parametrize("code", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED])
def test_handoff_retries_same_transfer_without_duplicate_ownership(tmp_path, monkeypatch, code):
    start = AgentCluster.start
    calls = []

    async def intercept(cluster):
        await start(cluster)
        stub = cluster.agents["crane-1"].stubs["crane-2"]

        class LostReply:
            def __getattr__(self, name):
                return getattr(stub, name)

            async def TokenTransfer(self, request, **options):
                calls.append(request.transfer_id)
                result = await stub.TokenTransfer(request, **options)
                if len(calls) == 1:
                    assert result.accepted
                    raise grpc.aio.AioRpcError(code, None, None, "injected lost reply")
                return result

        cluster.agents["crane-1"].stubs["crane-2"] = LostReply()

    monkeypatch.setattr(AgentCluster, "start", intercept)
    output = tmp_path / "retry"
    summary = asyncio.run(run_agent_demo(output, rounds=1, crossing_time=0.01))
    assert summary["completed"] == 3 and summary["safety_violations"] == 0
    assert len(calls) == 2 and calls[0] == calls[1]
    events = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
    assert sum(event["event"] == "token_transfer_retry" for event in events) == 1
    received = [event["agent_event"] for event in events if event["event"] == "agent_event"
                and event["agent_event"]["event"] == "TOKEN_TRANSFER_RECEIVED"]
    assert sum(event["agent_id"] == "crane-2" for event in received) == 1


# preserves definitive rejection as an experiment failure
def test_handoff_rejection_is_reported_without_retry(tmp_path, monkeypatch):
    start = AgentCluster.start

    async def intercept(cluster):
        await start(cluster)
        stub = cluster.agents["crane-1"].stubs["crane-2"]

        class Reject:
            def __getattr__(self, name):
                return getattr(stub, name)

            async def TokenTransfer(self, request, **options):
                return pb.Reply(accepted=False, reason="injected rejection", epoch=0)

        cluster.agents["crane-1"].stubs["crane-2"] = Reject()

    monkeypatch.setattr(AgentCluster, "start", intercept)
    output = tmp_path / "rejected"
    with pytest.raises(RuntimeError, match="REJECTED: injected rejection"):
        asyncio.run(run_agent_demo(output, rounds=1, crossing_time=0.01))
    events = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
    assert not any(event["event"] == "token_transfer_retry" for event in events)
    assert not (output / "summary.json").exists()
