"""Telemetry must preserve protocol state and expose failures without granting authority."""
import asyncio
import json
from urllib.request import urlopen

from prometheus_client import generate_latest
from cranelock.agent import Agent
from cranelock.telemetry import AgentMetrics


def test_metrics_observe_without_mutating_saved_authority(tmp_path):
    async def run():
        observed=[]
        path=tmp_path/'agent.json'
        agent=Agent('a',{'b':'127.0.0.1:1'},'a',state_file=path,event_sink=observed.append)
        before=path.read_bytes()
        metrics=AgentMetrics(agent)
        metrics.start(0,'127.0.0.1')
        try:
            await asyncio.sleep(.02)
            agent.emit('CONTROLLER_SUSPECTED',detection_latency_seconds=2)
            agent.emit('RECOVERY_AUTHORITY_INSTALL_RESULT',accepted=True,reason='OK')
            port=metrics.http.server_address[1]
            body=await asyncio.to_thread(lambda: urlopen(f'http://127.0.0.1:{port}/metrics').read().decode())
            assert 'cranelock_agent_has_token{agent_id="a"} 1.0' in body
            assert 'cranelock_agent_events_total{agent_id="a",event="RECOVERY_AUTHORITY_INSTALL_RESULT",outcome="True"} 1.0' in body
            assert path.read_bytes()==before
            assert [event['event'] for event in observed]==['CONTROLLER_SUSPECTED','RECOVERY_AUTHORITY_INSTALL_RESULT']
        finally:
            await metrics.close()
            await agent.close()
    asyncio.run(run())
