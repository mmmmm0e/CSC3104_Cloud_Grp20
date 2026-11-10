"""One external synthetic crossing through deployed agents/gate; no in-process services."""
import argparse
import json
import time
import uuid

import grpc
from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock_gate.client import Client
from cranelock_gate.demo import load_keys
from cranelock_sim.lane import Lane
from integrated import BASE, ROOT, gate_client, validation_lock


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--new-empty-simulation',action='store_true',required=True,
                        help='Explicitly start a new empty physical model; no other workload runner may be active')
    parser.parse_args()
    channel,controller_client=gate_client()
    events=[]
    def emit(event,**values): events.append(dict(event=event,timestamp=time.time(),monotonic=time.monotonic(),**values))
    out=ROOT/'run/platform-validation';out.mkdir(parents=True,exist_ok=True)
    result={'passed':False,'scope':'One fresh synthetic lane crossing; real external container RPCs, not a general workload benchmark'}
    try:
        state=controller_client.state()
        if state['mode']!='CENTRAL' or state['lane_state']!='UNKNOWN' or state['grant'] is not None:
            raise RuntimeError('Requires CENTRAL, UNKNOWN and no grant; refusing to reset existing lane state')
        sim=Client(channel,controller_client.config,load_keys(BASE/'signing/simulator.json'))
        lane=Lane(sim,time.time,emit)
        if not lane.observe()['allowed']: raise RuntimeError('New model observation rejected')
        old=state['holder']
        receiver=next(identity for identity in sorted(controller_client.config.members) if identity!=old)
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
        crane=Client(channel,controller_client.config,load_keys(BASE/f'signing/{receiver}.json'))
        authority=crane.state()
        grant=crane.call(receiver,'RequestGrant',epoch=authority['epoch'],revision=authority['revision'])
        if not grant['allowed']: raise RuntimeError('Grant denied: '+grant['reason'])
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
