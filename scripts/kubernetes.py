"""Dedicated local kind/Kustomize deployment; never uses the user's default context."""
import argparse
import base64
import json
from pathlib import Path
import subprocess
import time

from integrated import ROOT, BASE, COMPOSE, monitoring_secret, require_peer_tls, state_command
from scripts.workload_credentials import CLIENT_FILES, CRANES, PORT, require_credentials, runner_config

KUBE = ['kubectl', '--kubeconfig', str(ROOT / 'demo-data/kubeconfig'), '--context', 'kind-cranelock', '-n', 'cranelock']
OUT = ROOT / 'deploy/kubernetes/integrated'


def kube(*args, data=None, capture=False):
    return subprocess.run(KUBE + list(args), input=json.dumps(data) if data is not None else None,
                          text=True, capture_output=capture, check=True, cwd=ROOT)


def obj(kind, name, spec=None, **extra):
    value = dict(apiVersion='v1', kind=kind, metadata=dict(name=name, namespace='cranelock'), **extra)
    if spec is not None:
        value['spec'] = spec
    return value


def secret_files(name, directory, filenames):
    return obj('Secret', name, type='Opaque', data={
        filename: base64.b64encode((directory / filename).read_bytes()).decode('ascii')
        for filename in filenames})


def apply_input(value):
    if value['kind'] != 'Secret':
        kube('apply', '-f', '-', data=value)
        return
    try:
        # Avoid storing a second client-side copy in a last-applied annotation or
        # echoing a server error that may contain parts of the submitted Secret.
        kube('apply', '--server-side', '--field-manager=cranelock-platform', '-f', '-',
             data=value, capture=True)
    except subprocess.CalledProcessError:
        raise ValueError(f"Could not apply Secret {value['metadata']['name']} to kind-cranelock") from None


def render(*, apply_inputs=True):
    workload_files = require_credentials(BASE)
    require_peer_tls(BASE)
    monitoring_secret()
    config = json.loads(subprocess.run(COMPOSE + ['config', '--format', 'json'], capture_output=True, text=True, check=True).stdout)
    resources, inputs = [], []
    inputs.append(obj('Namespace', 'cranelock'))
    inputs[-1]['metadata'].pop('namespace')
    inputs.append(obj('ConfigMap', 'cranelock-public', data={'config.json': (BASE / 'config.json').read_text()}))
    inputs.append(secret_files('workload-runner-tls', workload_files / 'runner', CLIENT_FILES))
    runner = runner_config(Path('/workload-client'), {crane: f'{crane}:{PORT}' for crane in CRANES})
    inputs.append(obj('ConfigMap', 'workload-runner-config', data={'config.json': json.dumps(runner)}))
    for name, service in config['services'].items():
        labels = {'app': name, 'part-of': 'cranelock'}
        mounts, volumes, secret, public, has_data = [], [], {}, {}, False
        # Filter before numbering so the upgrade preserves existing Secret keys.
        ordinary_volumes = [volume for volume in service.get('volumes', [])
                            if not (name in CRANES and volume['target'].startswith('/workload/'))]
        for i, volume in enumerate(ordinary_volumes):
            target = volume['target']
            if volume['type'] == 'volume':
                has_data = True
                mounts.append(dict(name='data', mountPath=target))
                continue
            source = Path(volume['source'])
            if source == BASE / 'config.json':
                volumes.append(dict(name=f'file-{i}', configMap={'name': 'cranelock-public'}))
                mounts.append(dict(name=f'file-{i}', mountPath=target, subPath='config.json', readOnly=True))
            else:
                key = f'file-{i}'
                private = BASE in source.parents
                if private:
                    secret[key] = base64.b64encode(source.read_bytes()).decode()
                else:
                    public[key] = source.read_text()
                volumes.append(dict(name=key, **({'secret': {'secretName': name+'-files'}} if private else {'configMap': {'name': name+'-public'}})))
                mounts.append(dict(name=key, mountPath=target, subPath=key, readOnly=True))
        if secret:
            inputs.append(obj('Secret', name+'-files', type='Opaque', data=secret))
        if public:
            inputs.append(obj('ConfigMap', name+'-public', data=public))
        if name in CRANES:
            workload_secret = name + '-workload-tls'
            inputs.append(secret_files(workload_secret, workload_files / name,
                                       ('ca.pem', 'server.pem', 'server-key.pem', 'allowlist.json')))
            volumes.append(dict(name='workload-tls', secret=dict(secretName=workload_secret, defaultMode=0o440)))
            mounts.append(dict(name='workload-tls', mountPath='/workload', readOnly=True))
        uid = 472 if name == 'grafana' else 65534 if name == 'prometheus' else 10001
        container = dict(name=name, image=service['image'], imagePullPolicy='IfNotPresent',
                         volumeMounts=mounts, securityContext=dict(allowPrivilegeEscalation=False, capabilities={'drop': ['ALL']}),
                         resources=dict(requests=dict(cpu='50m', memory='96Mi'), limits=dict(cpu='1', memory='384Mi')))
        if service.get('entrypoint'):
            container['command'] = service['entrypoint']
        if service.get('command'):
            container['args'] = service['command']
        if service.get('environment'):
            container['env'] = [dict(name=k, value=str(v)) for k,v in service['environment'].items()]
        port = 50051 if name == 'gate' or name.startswith('crane-') else 50052 if name == 'controller' else 9090 if name == 'prometheus' else 3000
        container['ports'] = [dict(name='service', containerPort=port)]
        if name in CRANES:
            container['ports'].append(dict(name='workload', containerPort=PORT))
        if name in {'gate','controller'} or name.startswith('crane-'):
            container['ports'].append(dict(name='metrics', containerPort=9090 if name=='gate' else 9101))
        if name == 'gate':
            probe = dict(exec={'command': service['healthcheck']['test'][1:]}, periodSeconds=5, timeoutSeconds=3, failureThreshold=12)
        elif name.startswith('crane-'):
            probe = dict(exec={'command': state_command(name)}, periodSeconds=5, timeoutSeconds=3, failureThreshold=12)
        elif name == 'controller':
            probe = dict(tcpSocket={'port': port}, periodSeconds=5, timeoutSeconds=3, failureThreshold=12)
        else:
            probe = dict(httpGet={'port': port,'path': '/-/ready' if name=='prometheus' else '/api/health'}, periodSeconds=5, failureThreshold=12)
        container['readinessProbe'] = probe
        container['startupProbe'] = {**probe, 'failureThreshold': 30}
        container['livenessProbe'] = {**probe, 'failureThreshold': 6, 'periodSeconds': 10}
        pod = dict(metadata=dict(labels=labels), spec=dict(securityContext=dict(runAsNonRoot=True, runAsUser=uid, runAsGroup=uid, fsGroup=uid),
                   terminationGracePeriodSeconds=15, containers=[container], volumes=volumes))
        if name.startswith('crane-'):
            # Three one-replica sets allow distinct signing Secrets for fixed identities.
            container['command'] = ['python','-m','cranelock']
            spec = dict(serviceName=name, replicas=1, selector={'matchLabels':labels}, template=pod,
                        volumeClaimTemplates=[dict(metadata={'name':'data'},spec=dict(accessModes=['ReadWriteOnce'],resources={'requests':{'storage':'256Mi'}}))])
            workload = dict(apiVersion='apps/v1', kind='StatefulSet',metadata=dict(name=name,namespace='cranelock'),spec=spec)
        else:
            if has_data:
                claim = name+'-data'
                resources.append(obj('PersistentVolumeClaim',claim,dict(accessModes=['ReadWriteOnce'],resources={'requests':{'storage':'1Gi'}})))
                volumes.append(dict(name='data',persistentVolumeClaim={'claimName':claim}))
            workload=dict(apiVersion='apps/v1',kind='Deployment',metadata=dict(name=name,namespace='cranelock'),
                          spec=dict(replicas=1,strategy={'type':'Recreate'},selector={'matchLabels':labels},template=pod))
        resources.append(workload)
        service_ports=[dict(name='service',port=port,targetPort=port)]
        if name in CRANES:
            service_ports.append(dict(name='workload',port=PORT,targetPort='workload'))
        if name=='gate': service_ports.append(dict(name='metrics',port=9090,targetPort=9090))
        if name=='controller' or name.startswith('crane-'): service_ports.append(dict(name='metrics',port=9101,targetPort=9101))
        spec=dict(selector=labels,ports=service_ports)
        if name.startswith('crane-'): spec.update(clusterIP='None',publishNotReadyAddresses=True)
        resources.append(obj('Service',name,spec))
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT/'resources.yaml').write_text('\n---\n'.join(json.dumps(r,indent=2) for r in resources)+'\n')
    (OUT/'kustomization.yaml').write_text('apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: cranelock\nresources:\n  - resources.yaml\n')
    # Secret contents only travel over stdin, never to the tracked manifest or argv.
    if apply_inputs:
        for value in inputs:
            apply_input(value)
    return resources


def job(name,pod):
    pod=json.loads(json.dumps(pod))
    pod['metadata']={'labels':{'app':name}}
    pod['spec']['restartPolicy']='Never'
    return dict(apiVersion='batch/v1',kind='Job',metadata=dict(name=name,namespace='cranelock'),spec=dict(backoffLimit=0,template=pod))


def initialize(resources):
    # Initialization is explicit and refuses an existing named Job/database.
    if kube('get','jobs','-o','json',capture=True).stdout.find('cranelock-init') >= 0:
        raise ValueError('Initialization job already exists; use up, never reset existing state.')
    zero=json.loads(json.dumps(resources))
    for r in zero:
        if r['kind'] in {'Deployment','StatefulSet'}: r['spec']['replicas']=0
    kube('apply','-f','-',data=dict(apiVersion='v1',kind='List',items=zero))
    gate=next(r for r in resources if r['kind']=='Deployment' and r['metadata']['name']=='gate')
    pod=json.loads(json.dumps(gate['spec']['template']))
    c=pod['spec']['containers'][0]
    c['args']=['init','--config','/config/config.json','--database','/data/gate.sqlite']
    for key in ('readinessProbe','startupProbe','livenessProbe'): c.pop(key,None)
    kube('create','-f','-',data=job('cranelock-init',pod))
    kube('wait','--for=condition=complete','job/cranelock-init','--timeout=120s')
    kube('scale','deployment/gate','--replicas=1')
    kube('rollout','status','deployment/gate','--timeout=120s')
    controller=next(r for r in resources if r['kind']=='Deployment' and r['metadata']['name']=='controller')
    pod=json.loads(json.dumps(controller['spec']['template']))
    c=pod['spec']['containers'][0]
    code="""from pathlib import Path
import grpc
from cranelock_gate.config import Config
from cranelock_gate.client import Client, certificate
from cranelock_gate.demo import load_keys
cfg=Config.load('/config/config.json')
keys=load_keys('/signing/controller.json')
creds=grpc.ssl_channel_credentials(Path('/gate-tls/ca.pem').read_bytes(),Path('/gate-tls/client-key.pem').read_bytes(),Path('/gate-tls/client.pem').read_bytes())
with grpc.secure_channel('gate:50051',creds) as channel:
 c=Client(channel,cfg,keys); s=c.state()
 if s['epoch']!=-1: raise SystemExit('Refusing to bootstrap existing authority')
 cert=certificate(cfg,keys,s,'crane-1',0,mode='CENTRAL',signers=[cfg.controller_id])
 r=c.call(cfg.controller_id,'InstallAuthority',certificate=cert,expected_epoch=s['epoch'],expected_revision=s['revision'])
 if not r['allowed']: raise SystemExit(r['reason'])
 print('CENTRAL epoch 0 installed; lane remains UNKNOWN')
"""
    c['command']=['python','-c',code];c.pop('args',None)
    for key in ('readinessProbe','startupProbe','livenessProbe'): c.pop(key,None)
    kube('create','-f','-',data=job('cranelock-bootstrap',pod))
    kube('wait','--for=condition=complete','job/cranelock-bootstrap','--timeout=120s')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['cluster-create','cluster-start','cluster-stop','load-images','prepare','init','up','status','check','stop','logs'])
    args=parser.parse_args()
    try:
        if args.action=='cluster-create':
            subprocess.run(['kind','create','cluster','--name','cranelock','--kubeconfig',str(ROOT/'demo-data/kubeconfig'),'--wait','120s'],check=True)
        elif args.action=='load-images':
            architecture=subprocess.run(['docker','info','--format','{{.Architecture}}'],capture_output=True,text=True,check=True).stdout.strip()
            architecture='arm64' if architecture in {'arm64','aarch64'} else 'amd64'
            archive=ROOT/'demo-data/kubernetes-images.tar'
            subprocess.run(['docker','image','save','--platform','linux/'+architecture,'-o',str(archive),
                            'cranelock-gate:integrated','cranelock-agent:integrated','prom/prometheus:v3.5.0','grafana/grafana:12.2.0'],check=True)
            subprocess.run(['kind','load','image-archive',str(archive),'--name','cranelock'],check=True)
        elif args.action=='cluster-stop':
            subprocess.run(['docker','stop','cranelock-control-plane'],check=True)
            print('Dedicated local node stopped; node filesystem and PVCs retained. Do not delete the cluster.')
        elif args.action=='cluster-start':
            subprocess.run(['docker','start','cranelock-control-plane'],check=True)
            kube('wait','--for=condition=Ready','node/cranelock-control-plane','--timeout=120s')
        elif args.action in {'prepare','init','up'}:
            resources=render()
            if args.action=='init': initialize(resources)
            if args.action=='up':
                kube('apply','-k',str(OUT))
                for r in resources:
                    if r['kind'] in {'Deployment','StatefulSet'}:
                        kube('rollout','status',r['kind'].lower()+'/'+r['metadata']['name'],'--timeout=180s')
        elif args.action=='status': kube('get','pods,pvc,services')
        elif args.action=='check':
            for i in (1,2,3): kube('exec',f'crane-{i}-0','--',*state_command(f'crane-{i}'))
        elif args.action=='stop':
            kube('scale','deployment','--all','--replicas=0');kube('scale','statefulset','--all','--replicas=0')
            print('Namespace workloads stopped; PVCs retained. Kind control plane still running.')
        elif args.action=='logs':
            out=ROOT/'run/platform-validation';out.mkdir(parents=True,exist_ok=True)
            pods=json.loads(kube('get','pods','-o','json',capture=True).stdout)
            path=out/f'kubernetes-{time.time_ns()}.log'
            with path.open('w') as stream:
                for pod in pods['items']:
                    name=pod['metadata']['name'];stream.write(f'POD {name}\n')
                    result=kube('logs',name,'--all-containers',capture=True);stream.write(result.stdout)
            print(path)
    except (ValueError,OSError,subprocess.CalledProcessError) as error:
        parser.exit(1,str(error)+'\n')

if __name__=='__main__': main()
