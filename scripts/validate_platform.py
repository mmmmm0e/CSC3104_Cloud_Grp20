"""Exercise actual integrated containers; writes bounded authority/fault evidence."""
import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import grpc
from cranelock import protocol_pb2 as pb, protocol_pb2_grpc as rpc
from cranelock_gate.demo import load_keys
from integrated import ROOT, BASE, gate_client, record_fault, check, validation_lock, peer_channel
import faults


def wait(predicate, label, timeout=40):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            value=predicate()
            if value: return value
        except (grpc.RpcError,RuntimeError,OSError): pass
        time.sleep(.4)
    raise RuntimeError('Timed out: '+label)


def agents():
    values={}
    for i in (1,2,3):
        with peer_channel(f'crane-{i}') as channel:
            s=rpc.CraneAgentStub(channel).GetAgentState(pb.Empty(),timeout=2)
        values[s.agent_id]=s
    return values


def converge(client, mode):
    def predicate():
        s=client.state()
        if s['mode']!=mode: return False
        a=agents()
        if all(v.epoch==s['epoch'] and v.revision==s['revision'] and v.mode==getattr(pb,mode)
               and v.token_holder==s['holder'] and v.has_token==(name==s['holder'])
               for name,v in a.items()): return s
        return False
    return wait(predicate,'agent/gate convergence in '+mode)


def query(expression):
    url='http://127.0.0.1:9092/api/v1/query?'+urllib.parse.urlencode({'query':expression})
    data=json.load(urllib.request.urlopen(url,timeout=4))
    return data['data']['result']


def monitoring():
    def targets_ready():
        targets=json.load(urllib.request.urlopen('http://127.0.0.1:9092/api/v1/targets',timeout=4))['data']['activeTargets']
        return len(targets)==5 and all(t['health']=='up' for t in targets)
    # Allow the next scrape after preceding restart scenarios.
    wait(targets_ready,'all five metric targets UP',timeout=20)
    password=(BASE/'grafana-password').read_text().strip()
    token=base64.b64encode(('admin:'+password).encode()).decode()
    request=urllib.request.Request('http://127.0.0.1:3001/api/dashboards/uid/cranelock-integrated',headers={'Authorization':'Basic '+token})
    dashboard=json.load(urllib.request.urlopen(request,timeout=4))
    # Count panels inside folded rows too; the main view is deliberately short.
    panels=[p for top in dashboard['dashboard']['panels'] for p in [top,*top.get('panels',[])] if p.get('type')!='row']
    if len(panels)<10 or not dashboard['meta']['provisioned']:
        raise RuntimeError('Dashboard not fully provisioned')
    record_fault('stop','controller')
    try:
        wait(lambda: query('up{job="controller"}')[0]['value'][1]=='0','controller target DOWN',timeout=20)
    finally: record_fault('start','controller')
    return dict(targets=5,dashboard_provisioned=True,controller_down_observed=True)


def controller_cycle(client):
    before=converge(client,'CENTRAL')
    record_fault('stop','controller')
    try:
        local=converge(client,'LOCAL')
        if local['holder']!=before['holder'] or local['epoch']<=before['epoch']:
            raise RuntimeError('Fallback did not preserve holder with a newer epoch')
    finally: record_fault('start','controller')
    central=converge(client,'CENTRAL')
    time.sleep(5)
    final=converge(client,'CENTRAL')
    if (final['epoch'],final['revision'])!=(central['epoch'],central['revision']):
        raise RuntimeError('Authority churn after return')
    return dict(before_epoch=before['epoch'],local_epoch=local['epoch'],final_epoch=final['epoch'],final_revision=final['revision'],stable_observation_seconds=5)


def holder_cycle(client):
    record_fault('stop','controller')
    old=None
    try:
        old=converge(client,'LOCAL')
        holder=old['holder']
        record_fault('kill',holder)
        new=wait(lambda: (s if s['epoch']>old['epoch'] and s['holder']!=holder else False)
                 if (s:=client.state()) else False,'majority replacement')
        client.keys=load_keys(BASE/f'signing/{holder}.json')
        stale=client.call(holder,'RequestGrant',epoch=old['epoch'],revision=old['revision'])
        if stale['allowed'] or stale['reason']!='STALE_EPOCH': raise RuntimeError('Old holder was not fenced')
        record_fault('start',holder)
        converge(client,'LOCAL')
        if agents()[holder].has_token: raise RuntimeError('Restarted old holder regained stale token')
        return dict(old_holder=holder,replacement=new['holder'],old_epoch=old['epoch'],new_epoch=new['epoch'],stale_reason=stale['reason'],restarted_holder_blocked=True)
    finally:
        if old:
            record_fault('start',old['holder'])
        record_fault('start','controller')
        converge(client,'CENTRAL')


def gate_cycle(client):
    before=client.state()
    record_fault('stop','gate')
    try:
        try: client.state()
        except grpc.RpcError: pass
        else: raise RuntimeError('Stopped gate still reachable')
    finally: record_fault('start','gate')
    after=wait(client.state,'gate restart')
    # Live agents may legitimately approve a newer epoch during gate downtime.
    # Exact persisted tuple retention is tested separately under K8s maintenance.
    if after['epoch']<before['epoch'] or (after['epoch']==before['epoch'] and after['revision']<before['revision']) or after['holder']!=before['holder'] or after['session_id']==before['session_id'] or after['grant'] is not None or after['lane_state']!='UNKNOWN':
        raise RuntimeError('Gate restart lost authority or reused physical clearance')
    converge(client,'CENTRAL')
    return dict(epoch=after['epoch'],holder=after['holder'],session_changed=True,lane='UNKNOWN',grant_invalidated=True)


def network(client):
    record_fault('stop','controller')
    try:
        old=converge(client,'LOCAL')
        process=subprocess.Popen([sys.executable,str(ROOT/'scripts/faults.py'),'partition',old['holder'],'--seconds','10'])
        try:
            new=wait(lambda: (s if s['epoch']>old['epoch'] and s['holder']!=old['holder'] else False)
                     if (s:=client.state()) else False,'majority recovery across partition',timeout=30)
        finally:
            if process.poll() is None: process.wait(timeout=30)
        if process.returncode: raise RuntimeError('Partition helper failed')
        converge(client,'LOCAL')
        faults.delay('crane-3',200,20,25,0,5)
        return dict(partition_replacement=new['holder'],partition_epoch=new['epoch'],packet_delay_ms=200,packet_reorder_percent=25,cleanup_completed=True)
    finally:
        record_fault('start','controller')
        converge(client,'CENTRAL')


def no_quorum(client):
    record_fault('stop','controller')
    try:
        converge(client,'LOCAL')
        observed={}
        def inspect():
            # Rule installation is not atomic across namespaces. Drain proposals
            # formed during setup before measuring a steady all-isolated window.
            time.sleep(3)
            before=client.state()
            states=agents()
            if any(len(a.peers)!=2 or any(p.status!='SUSPECTED_UNAVAILABLE' for p in a.peers) for a in states.values()):
                raise RuntimeError('All-isolated peer suspicion not established')
            time.sleep(5)
            after=client.state()
            if (before['epoch'],before['revision'])!=(after['epoch'],after['revision']):
                raise RuntimeError('Authority changed during drained all-isolated window')
            observed.update(epoch=after['epoch'],revision=after['revision'],observation_seconds=5,no_replacement_authority=True)
        faults.partition('all',12,observe=inspect)
        return observed
    finally:
        record_fault('start','controller')
        converge(client,'CENTRAL')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('scenario',choices=['controller','holder','gate','monitoring','network','no-quorum','reordered-command','lost-updates','all'])
    p.add_argument('--new-empty-simulation',action='store_true',help='Required for lost-updates/all; no other physical runner may be active')
    args=p.parse_args()
    if args.scenario in {'all','lost-updates'} and not args.new_empty_simulation:
        p.error('Explicit --new-empty-simulation required for occupancy test')
    out=ROOT/'run/platform-validation';out.mkdir(parents=True,exist_ok=True)
    result=dict(started_at=time.time(),scope='Local Docker containers; authority/transport and synthetic missing occupancy evidence, no physical crossing workload',results={})
    channel,client=gate_client()
    try:
        check()
        scenarios=['controller','holder','gate','monitoring','network','no-quorum','reordered-command','lost-updates'] if args.scenario=='all' else [args.scenario]
        for name in scenarios:
            started=time.monotonic()
            if name=='lost-updates':
                faults.lost_updates(args.new_empty_simulation);value={'missing_updates_blocked_grant':True}
            elif name=='reordered-command':
                faults.reordered_command();value={'delayed_signed_request_fenced':True}
            else: value={'controller':controller_cycle,'holder':holder_cycle,'gate':gate_cycle,'monitoring':lambda _:monitoring(),'network':network,'no-quorum':no_quorum}[name](client)
            result['results'][name]=dict(passed=True,elapsed_seconds=time.monotonic()-started,**value)
            print('PASS',name,flush=True)
        result['passed']=True
    except Exception as error:
        result['passed']=False;result['error']=str(error)
        raise
    finally:
        channel.close()
        result['finished_at']=time.time()
        path=out/f'container-validation-{time.time_ns()}.json'
        path.write_text(json.dumps(result,indent=2)+'\n')
        print('Evidence:',path,flush=True)

if __name__=='__main__':
    with validation_lock():
        main()
