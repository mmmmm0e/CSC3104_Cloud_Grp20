"""Bounded faults restricted to the local cranelock-integrated containers."""
import argparse
import json
import subprocess
import time

from integrated import ROOT, BASE, SERVICES, gate_client

IMAGE = 'cranelock-chaos:local'


def run(*args, check=True):
    return subprocess.run(list(args), check=check, capture_output=True, text=True)


def container(service):
    name = f'cranelock-integrated-{service}-1'
    data = json.loads(run('docker', 'inspect', name).stdout)[0]
    if data['Config']['Labels'].get('com.docker.compose.project') != 'cranelock-integrated':
        raise ValueError('Refusing a container outside the integrated project')
    if not data['State']['Running']:
        raise ValueError(f'{service} is not running')
    return name, data


def tool(service, *args, check=True):
    name, _ = container(service)
    return run('docker', 'run', '--rm', '--network', 'container:'+name,
               '--cap-add', 'NET_ADMIN', IMAGE, *args, check=check)


def record(event, **fields):
    out = ROOT / 'run/platform-validation'
    out.mkdir(parents=True, exist_ok=True)
    with (out / 'fault-events.jsonl').open('a') as stream:
        stream.write(json.dumps(dict(event=event, timestamp=time.time(),
                                     monotonic=time.monotonic(), **fields))+'\n')


def partition(service, seconds, observe=None):
    targets = list(SERVICES[2:]) if service == 'all' else [service]
    applied = []
    record('partition_started', targets=targets, duration_seconds=seconds)
    try:
        for target in targets:
            for peer in SERVICES[2:]:
                if peer == target: continue
                _, data = container(peer)
                ip = data['NetworkSettings']['Networks']['cranelock-integrated_default']['IPAddress']
                for chain, direction in [('INPUT','-s'),('OUTPUT','-d')]:
                    rule = (chain, direction, ip, '-m', 'comment', '--comment', 'cranelock-platform', '-j', 'DROP')
                    if tool(target, 'iptables', '-C', *rule, check=False).returncode == 0:
                        raise ValueError('Existing platform partition found; restore it before another test')
                    tool(target, 'iptables', '-I', *rule)
                    applied.append((target, rule))
        record('partition_applied', targets=targets)
        deadline=time.monotonic()+seconds
        if observe:
            observe()
        time.sleep(max(0,deadline-time.monotonic()))
    finally:
        failures=[]
        for target, rule in reversed(applied):
            try: tool(target, 'iptables', '-D', *rule)
            except Exception as error: failures.append(str(error))
        record('partition_cleanup', success=not failures, errors=failures)
        if failures: raise RuntimeError('Partition cleanup failed; inspect fault log before continuing')


def delay(service, ms, jitter, reorder, loss, seconds):
    # TCP preserves byte order; this reorders packets, not delivered gRPC messages.
    parameters = ['delay', f'{ms}ms', f'{jitter}ms']
    if reorder: parameters += ['reorder', f'{reorder}%']
    if loss: parameters += ['loss', f'{loss}%']
    record('netem_started', service=service, delay_ms=ms, jitter_ms=jitter,
           reorder_percent=reorder, loss_percent=loss, duration_seconds=seconds)
    applied=False
    try:
        tool(service, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', *parameters)
        applied=True
        record('netem_applied', service=service)
        time.sleep(seconds)
    finally:
        if applied:
            tool(service, 'tc', 'qdisc', 'del', 'dev', 'eth0', 'root')
            record('netem_removed', service=service)


def lost_updates(new_empty_simulation):
    if not new_empty_simulation:
        raise ValueError('Requires --new-empty-simulation; never use alongside another workload runner')
    from cranelock_gate.demo import load_keys
    channel, client = gate_client()
    try:
        state=client.state()
        if state['grant'] is not None or state['lane_state'] != 'UNKNOWN':
            raise ValueError('Requires UNKNOWN occupancy and no active grant; refusing to clear existing state')
        client.keys=load_keys(BASE/'signing/simulator.json')
        record('new_empty_model_observation')
        result=client.call(client.config.simulator_id,'ReportLaneState',
            session_id=state['session_id'], observation_revision=state['observation_revision'],
            sequence=state['observation_sequence']+1, observed_at_ms=round(time.time()*1000),lane_state='CLEAR')
        if not result['allowed']: raise ValueError(result['reason'])
        reported_lane=client.state()['lane_state']
        record('occupancy_updates_suppressed', timeout_seconds=client.config.observation_timeout_seconds)
        time.sleep(client.config.observation_timeout_seconds+.5)
        state=client.state()
        client.keys=load_keys(BASE/f"signing/{state['holder']}.json")
        result=client.call(state['holder'],'RequestGrant',epoch=state['epoch'],revision=state['revision'])
        if result['allowed'] or result['reason'] != 'LANE_UNKNOWN':
            raise ValueError('Lost occupancy did not block new grant')
        record('lost_updates_verified', reason=result['reason'])
        print(f"PASS: expired occupancy evidence blocks grants; no physical entry attempted "
              f"(lane status: {reported_lane} -> {state['lane_state']}; {state['holder']} refused: {result['reason']})")
    finally:
        channel.close()


def reordered_command(seconds=1):
    """Hold an already signed old grant request until newer authority is installed."""
    from cranelock_gate.client import command, certificate
    from cranelock_gate.crypto import COMMAND_DOMAIN, canonical
    from cranelock_gate.demo import load_keys
    from cranelock_gate.generated.gate_pb2 import SignedCommand
    channel, client = gate_client()
    try:
        state=client.state()
        if state['mode']!='CENTRAL' or state['grant'] is not None:
            raise ValueError('Requires CENTRAL authority and no active grant; do not run alongside a workload')
        holder=state['holder']
        key=load_keys(BASE/f'signing/{holder}.json')[holder]
        payload=canonical(command(client.config,holder,'RequestGrant',epoch=state['epoch'],revision=state['revision']))
        held=SignedCommand(payload_json=payload,signature=key.sign(COMMAND_DOMAIN+payload))
        record('signed_grant_held',epoch=state['epoch'],holder=holder)
        cert=certificate(client.config,client.keys,state,holder,state['epoch'],
                         mode='CENTRAL',signers=[client.config.controller_id])
        result=client.call(client.config.controller_id,'InstallAuthority',certificate=cert,
                           expected_epoch=state['epoch'],expected_revision=state['revision'])
        if not result['allowed']: raise ValueError(result['reason'])
        time.sleep(seconds)
        response=client.stub.RequestGrant(held,timeout=3)
        if response.allowed or response.reason!='STALE_REVISION':
            raise ValueError('Delayed old request was not fenced')
        record('signed_grant_released_after_authority',accepted=False,reason=response.reason)
        print('PASS: delayed signed grant delivered after newer authority was rejected')
    finally:
        channel.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='action',required=True)
    sub.add_parser('build')
    p=sub.add_parser('partition');p.add_argument('service',choices=[*SERVICES[2:],'all']);p.add_argument('--seconds',type=float,default=10)
    p=sub.add_parser('delay');p.add_argument('service',choices=SERVICES);p.add_argument('--seconds',type=float,default=10)
    p.add_argument('--ms',type=int,default=200);p.add_argument('--jitter',type=int,default=20)
    p.add_argument('--reorder',type=float,default=0);p.add_argument('--loss',type=float,default=0)
    p=sub.add_parser('lost-updates');p.add_argument('--new-empty-simulation',action='store_true')
    sub.add_parser('reordered-command')
    args=parser.parse_args()
    if hasattr(args,'seconds') and not 0 < args.seconds <= 60:
        parser.error('Fault duration must be >0 and <=60 seconds')
    if args.action=='delay' and (not 1<=args.ms<=10000 or not 0<=args.jitter<=args.ms or not 0<=args.reorder<=100 or not 0<=args.loss<=100):
        parser.error('Require 1..10000 ms, jitter <= delay, and percentages 0..100')
    try:
        if args.action=='build':
            subprocess.run(['docker','build','-f',str(ROOT/'deploy/Dockerfile.chaos'),'-t',IMAGE,str(ROOT)],check=True)
        elif args.action=='partition': partition(args.service,args.seconds)
        elif args.action=='delay': delay(args.service,args.ms,args.jitter,args.reorder,args.loss,args.seconds)
        elif args.action=='reordered-command': reordered_command()
        else: lost_updates(args.new_empty_simulation)
    except (ValueError,OSError,subprocess.CalledProcessError) as error:
        message=error.stderr if isinstance(error,subprocess.CalledProcessError) else str(error)
        parser.exit(1,message+'\n')

if __name__=='__main__': main()
