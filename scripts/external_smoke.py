"""One external synthetic crossing through deployed agents/gate; no in-process services."""
import argparse
import json
import time
import uuid

import grpc
from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock_sim.lane import Lane
from cranelock_sim.workload_client import GrantAttempt, channel as workload_channel
from pathlib import Path
from integrated import ROOT, simulator_gate_client, validation_lock


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--new-empty-simulation',action='store_true',required=True,
                        help='Explicitly start a new empty physical model; no other workload runner may be active')
    parser.add_argument('--workload-config', required=True)
    args = parser.parse_args()
    channel,sim=simulator_gate_client()
    events=[]
    def emit(event,**values): events.append(dict(event=event,timestamp=time.time(),monotonic=time.monotonic(),**values))
    out=ROOT/'run/platform-validation';out.mkdir(parents=True,exist_ok=True)
    result={'passed':False,'scope':'One fresh synthetic lane crossing; real external container RPCs, not a general workload benchmark'}
    try:
        state=sim.state()
        if state['mode']!='CENTRAL' or state['lane_state']!='UNKNOWN' or state['grant'] is not None:
            raise RuntimeError('Requires CENTRAL, UNKNOWN and no grant; refusing to reset existing lane state')
        lane=Lane(sim,time.time,emit)
        if not lane.observe()['allowed']: raise RuntimeError('New model observation rejected')
        old=state['holder']
        receiver=next(identity for identity in sorted(sim.config.members) if identity!=old)
        with grpc.insecure_channel('127.0.0.1:'+str(50100+int(old.rsplit('-',1)[1]))) as peer_channel:
            reply=rpc.CraneAgentStub(peer_channel).TokenRequest(pb.TokenRequestMessage(
                agent_id=receiver,epoch=state['epoch'],revision=state['revision'],request_id=str(uuid.uuid4())),timeout=3)
        if not reply.accepted: raise RuntimeError('Agent refused token request: '+reply.reason)
        emit('external_token_request',old_holder=old,requester=receiver)
        end=time.monotonic()+15
        while time.monotonic()<end:
            authority=sim.state()
            with grpc.insecure_channel('127.0.0.1:'+str(50100+int(receiver.rsplit('-',1)[1]))) as peer_channel:
                agent=rpc.CraneAgentStub(peer_channel).GetAgentState(pb.Empty(),timeout=2)
            if authority['holder']==receiver and agent.has_token and agent.epoch==authority['epoch'] and agent.revision==authority['revision']: break
            # Fresh physical samples while the new model remains empty.
            lane.observe()
            time.sleep(.2)
        else: raise RuntimeError('Token handoff did not settle')
        emit('external_handoff_verified',holder=receiver,epoch=authority['epoch'],revision=authority['revision'])
        if not lane.observe()['allowed']: raise RuntimeError('Fresh pre-entry evidence rejected')
        options=json.loads(Path(args.workload_config).read_text(encoding='utf-8'))
        credentials=[Path(options[name]).read_bytes() for name in ('ca','cert','key')]
        authority=sim.state()
        workload_connection, stub=workload_channel(options['agents'][receiver], *credentials)
        try:
            grant=GrantAttempt(authority['epoch'], authority['revision']).call(stub)
        finally:
            workload_connection.close()
        if not grant['allowed'] or grant['replayed'] or not grant['action_applied']:
            raise RuntimeError('Grant denied or uncertain: '+grant['reason'])
        request_id=str(uuid.uuid4())
        if not lane.enter(receiver,request_id,grant['grant'],authority): raise RuntimeError('Entry not accepted')
        time.sleep(.05)
        lane.exit(receiver,request_id)
        # Omit subsequent updates so this model leaves no reusable clearance.
        time.sleep(sim.config.observation_timeout_seconds+.5)
        final=sim.state()
        if final['lane_state']!='UNKNOWN' or final['grant'] is not None: raise RuntimeError('Old synthetic clearance remained reusable')
        result.update(passed=True,old_holder=old,new_holder=receiver,epoch=final['epoch'],revision=final['revision'],
                      completed_crossings=1,physical_entries=sum(e['event']=='physical_entry' for e in events),
                      physical_exits=sum(e['event']=='physical_exit' for e in events),final_lane_state=final['lane_state'])
    except Exception as error:
        result['error']=str(error)
        raise
    finally:
        channel.close()
        result['events']=events
        path=out/f'external-smoke-{time.time_ns()}.json';path.write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps({k:v for k,v in result.items() if k!='events'},indent=2));print('Evidence:',path)

if __name__=='__main__':
    with validation_lock(): main()
