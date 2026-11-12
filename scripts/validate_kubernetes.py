"""Validate Kubernetes PVC retention, pod replacement and monitoring in the local cluster."""
import json
import time

from integrated import ROOT
from kubernetes import kube


def pods(app):
    return json.loads(kube('get','pods','-l','app='+app,'-o','json',capture=True).stdout)['items']


def rpc(identity):
    return json.loads(kube('exec',identity+'-0','--','python','-m','cranelock','state','127.0.0.1:50051',capture=True).stdout)


def gate_state():
    name=pods('gate')[0]['metadata']['name']
    result=kube('exec',name,'--','python','-m','cranelock_gate.demo','state','--directory','/config',
                '--target','localhost:50051','--tls-ca','/tls/ca.pem','--tls-cert','/tls/probe.pem','--tls-key','/tls/probe-key.pem',capture=True)
    return json.loads(result.stdout)


def wait(predicate,label):
    end=time.monotonic()+60
    while time.monotonic()<end:
        try:
            result=predicate()
            if result: return result
        except Exception: pass
        time.sleep(1)
    raise RuntimeError('Timed out: '+label)


def converged():
    s=gate_state()
    if s['mode']!='CENTRAL': return False
    for identity in ('crane-1','crane-2','crane-3'):
        a=rpc(identity)
        if int(a['epoch'])!=s['epoch'] or int(a['revision'])!=s['revision'] or a['mode']!='CENTRAL' or a.get('has_token',False)!=(identity==s['holder']): return False
    return s


def main():
    result={'scope':'Single local kind node; planned gate maintenance and agent pod replacement, not node-loss resilience', 'checks':[]}
    before=wait(converged,'initial convergence')
    try:
        old=pods('crane-2')[0]['metadata']['uid']
        pvc=json.loads(kube('get','pvc','data-crane-2-0','-o','json',capture=True).stdout)
        claim=pvc['metadata']['uid']
        kube('rollout','restart','statefulset/crane-2')
        kube('rollout','status','statefulset/crane-2','--timeout=120s')
        wait(converged,'agent rejoin')
        if pods('crane-2')[0]['metadata']['uid']==old: raise RuntimeError('Agent pod was not replaced')
        if json.loads(kube('get','pvc','data-crane-2-0','-o','json',capture=True).stdout)['metadata']['uid']!=claim: raise RuntimeError('PVC changed')
        log=kube('logs','crane-2-0',capture=True).stdout
        if '"mode": "REJOINING"' not in log: raise RuntimeError('Restart did not expose REJOINING')
        result['checks'].append('agent_pod_replaced_same_pvc_and_reconciled')
        # Planned maintenance isolates persistence from simultaneous majority fallback.
        for kind,name in [('deployment','controller'),('statefulset','crane-1'),('statefulset','crane-2'),('statefulset','crane-3')]:
            kube('scale',kind+'/'+name,'--replicas=0')
        for app in ('controller','crane-1','crane-2','crane-3'):
            wait(lambda app=app:not pods(app),'stop '+app)
        before=gate_state()
        kube('rollout','restart','deployment/gate')
        kube('rollout','status','deployment/gate','--timeout=120s')
        after=gate_state()
        if any(before[k]!=after[k] for k in ('epoch','revision','holder')) or before['session_id']==after['session_id'] or after['grant'] is not None or after['lane_state']!='UNKNOWN':
            raise RuntimeError('Gate persisted-authority/restart check failed')
        result['checks'].append('gate_pod_restart_preserved_authority_changed_session_invalidated_clearance')
    finally:
        kube('scale','deployment/controller','--replicas=1')
        for i in (1,2,3): kube('scale',f'statefulset/crane-{i}','--replicas=1')
    final=wait(converged,'restore services')
    controller=pods('controller')[0]['metadata']['name']
    code="import json,urllib.request; print(json.dumps({'targets':json.load(urllib.request.urlopen('http://prometheus:9090/api/v1/targets'))['data']['activeTargets'],'grafana':json.load(urllib.request.urlopen('http://grafana:3000/api/health'))['database']}))"
    def monitoring():
        d=json.loads(kube('exec',controller,'--','python','-c',code,capture=True).stdout)
        return d if len(d['targets'])==5 and all(t['health']=='up' for t in d['targets']) and d['grafana']=='ok' else False
    wait(monitoring,'monitoring targets')
    result['checks'].append('five_kubernetes_prometheus_targets_up_and_grafana_healthy')
    result.update(passed=True,final_epoch=final['epoch'],final_revision=final['revision'],final_holder=final['holder'])
    out=ROOT/'run/platform-validation';out.mkdir(parents=True,exist_ok=True)
    path=out/f'kubernetes-validation-{time.time_ns()}.json';path.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2));print('Evidence:',path)

if __name__=='__main__': main()
