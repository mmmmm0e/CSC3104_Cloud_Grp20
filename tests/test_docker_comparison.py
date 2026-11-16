"""Isolation and pairing checks; real Docker matrix is an explicit experiment."""
from pathlib import Path
import json

import pytest

from cranelock_sim.benefits import paired
from cranelock_sim.controller import CONTROLLER_IMPLEMENTATION, CONTROLLER_SOURCE
from cranelock_sim.docker_comparison import DockerCluster
from cranelock_sim.docker_comparison import audit_matrix
from cranelock_sim.comparison import MODELS, workload_hash, write_csv


def test_container_addresses_reach_future_peers_and_use_container_paths():
    cluster = DockerCluster.__new__(DockerCluster)
    cluster.output = Path('C:/test evidence/case')
    cluster.addresses = {'127.0.0.1:50101':'crane-1:50101',
                         '127.0.0.1:50102':'crane-2:50102', '127.0.0.1:50103':'crane-3:50103'}
    assert cluster.translate(['--peer','crane-3=127.0.0.1:50103','--state-file',
        cluster.output/'state'/'crane-1'/'state.json'], 'crane-1') == [
            '--peer','crane-3=crane-3:50103','--state-file','/state/state.json']


def test_credentials_are_scoped_and_never_in_shared_mounts(tmp_path):
    from cranelock_sim.evaluation_provision import provision
    provision(tmp_path)
    cluster = DockerCluster.__new__(DockerCluster)
    cluster.output = tmp_path
    cluster.addresses = {}
    for identity in ('crane-1', 'crane-2', 'crane-3'):
        credentials = tmp_path/'.credentials'/identity
        assert (credentials/f'{identity}.key.json').is_file()
        assert {p.name for p in credentials.glob('*.key.json')} == {f'{identity}.key.json'}
        assert not (credentials/'workload-tls'/'client-key.pem').exists()
        mounts = cluster.mounts(identity)
        assert str(credentials) in ' '.join(mounts)
        assert f'source={tmp_path},target=' not in ' '.join(mounts)
        assert 'target=/credentials,readonly' in ' '.join(mounts)
        assert str(tmp_path/'.credentials'/'driver') not in ' '.join(mounts)
        for other in {'crane-1', 'crane-2', 'crane-3', 'controller'} - {identity}:
            assert str(tmp_path/'.credentials'/other) not in ' '.join(mounts)
    driver = tmp_path/'.credentials'/'driver'
    assert {p.name for p in driver.glob('*.key.json')} == {'simulator.key.json'}
    assert not list(driver.rglob('server-key.pem'))
    assert not list(driver.rglob('crane-*.key.json'))
    assert not list(driver.rglob('controller.key.json'))


def test_docker_pairing_requires_matching_image_resources_and_execution():
    base = dict(seed=7,requested=18,workload_sha256='same',duration_seconds=30,
        crossing_time_seconds=.1,detection_timeout_seconds=2,evaluation='benefits-docker-v1',
        scenario='controller-outage',controller_implementation=CONTROLLER_IMPLEMENTATION,
        controller_source_commit=CONTROLLER_SOURCE,recovery_driver='agent_workers',
        execution_mode='docker_containers',passed=True,post_fault_new_entries=0,
        docker=dict(image_id='sha256:example',cpus_per_service=1,memory_bytes_per_service=268435456))
    central = dict(base,model='central-lock')
    crane = dict(base,model='cranelock',post_fault_new_entries=12)
    metric = lambda rows: paired(rows,'controller-outage','central-lock','post_fault_new_entries')['median']
    assert metric([central,crane]) == 12
    for changes in [dict(passed=False),dict(execution_mode='independent_processes'),
                    dict(docker={}),dict(docker=dict(base['docker'],cpus_per_service=2)),
                    dict(docker=dict(base['docker'],image_id='different'))]:
        assert metric([central,dict(crane,**changes)]) is None
    assert metric([central,central,crane]) is None


def test_saved_matrix_audit_rejects_changed_csv_and_overlapping_entries(tmp_path):
    workload = [dict(request_id='r1',crane='crane-2',requested_at=0)]
    for model in MODELS:
        path = tmp_path/'seed-7'/'normal'/model
        path.mkdir(parents=True)
        manifest = dict(seed=7,scenario='normal',model=model,
            container_ids={str(i):f'{model}-{i}' for i in range(5)})
        summary = dict(**manifest,passed=True,execution_mode='docker_containers',
            requested=1,completed=1,accepted=1,total_completion_seconds=.3,safety_violations=0,
            workload_sha256=workload_hash(workload),duration_seconds=None,crossing_time_seconds=.1,
            detection_timeout_seconds=2,heartbeat_interval_seconds=.5,host_rpc_timeout_seconds=2,
            docker=dict(image_id='same',cpus_per_service=1,memory_bytes_per_service=268435456))
        for name,data in [('experiment.json',manifest),('summary.json',summary),
                          ('workload.json',workload),('cleanup.json',dict(succeeded=True))]:
            (path/name).write_text(json.dumps(data))
        write_csv(path/'requests.csv',[dict(request_id='r1',entered_at=.1,exited_at=.2)])
        (path/'events.jsonl').write_text('\n'.join(json.dumps(e) for e in [
            dict(event='physical_entry',request_id='r1',time=.1),
            dict(event='physical_exit',request_id='r1',time=.2)]))
    assert audit_matrix(tmp_path,seeds=[7],scenarios=['normal'])['completed'] == 3
    path = tmp_path/'seed-7'/'normal'/'cranelock'
    original = (path/'requests.csv').read_text()
    (path/'requests.csv').write_text(original.replace('0.2','0.4'))
    with pytest.raises(ValueError,match='CSV totals differ'):
        audit_matrix(tmp_path,seeds=[7],scenarios=['normal'])
    (path/'requests.csv').write_text(original)
    events = (path/'events.jsonl').read_text()
    (path/'events.jsonl').write_text(json.dumps(dict(event='physical_entry',request_id='r2',time=.05))+'\n'+events)
    with pytest.raises(ValueError,match='Overlapping physical entries'):
        audit_matrix(tmp_path,seeds=[7],scenarios=['normal'])
