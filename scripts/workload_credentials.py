"""Issue development-only TLS credentials for the integrated workload listeners."""
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import tempfile

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from cranelock_gate.config import Config
from cranelock_sim.workload_tls import IDENTITY

CRANES = ('crane-1', 'crane-2', 'crane-3')
PORT = 50053
DENIED_IDENTITY = 'urn:cranelock:workload:unapproved-test-runner'
CLIENT_FILES = ('ca.pem', 'client.pem', 'client-key.pem')


def host_endpoint(crane):
    return f'localhost:{50201 + CRANES.index(crane)}'


def runner_config(directory, agents):
    return dict(zip(('ca', 'cert', 'key'), (str(directory / name) for name in CLIENT_FILES)),
                agents=agents)


def require_credentials(base):
    directory = base / 'workload'
    paths = [directory / 'config.json']
    paths.extend(directory / 'runner' / name for name in CLIENT_FILES)
    paths.extend(directory / crane / name for crane in CRANES
                 for name in ('ca.pem', 'server.pem', 'server-key.pem', 'allowlist.json'))
    if not all(path.is_file() for path in paths):
        raise ValueError('Workload credentials missing: run scripts/integrated.py provision-workload')
    return directory


def provision_workload(base, *, include_test_identity=False):
    base = Path(base).resolve()
    target = base / 'workload'
    if target.exists() or target.is_symlink():
        raise ValueError('Refusing to overwrite existing workload credentials')
    config = Config.load(base / 'config.json')
    if set(config.members) != set(CRANES):
        raise ValueError('Workload provisioning requires crane-1, crane-2 and crane-3')

    def subject(value):
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, value)])

    now = datetime.now(timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    issuer = subject('CraneLock integrated workload development CA')

    def issue(name, key, usage=None, sans=()):
        builder = (x509.CertificateBuilder().subject_name(name).issuer_name(issuer)
                   .public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(now - timedelta(minutes=5)).not_valid_after(now + timedelta(days=30))
                   .add_extension(x509.BasicConstraints(ca=usage is None,
                                                        path_length=0 if usage is None else None), critical=True))
        if usage is not None:
            builder = (builder.add_extension(x509.SubjectAlternativeName(sans), critical=False)
                       .add_extension(x509.ExtendedKeyUsage([usage]), critical=True))
        return builder.sign(ca_key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM)

    ca = issue(issuer, ca_key)

    def write_file(path, data, mode):
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode), 'wb') as stream:
            stream.write(data)

    def write_role(root, name, role, usage, sans, *, bind_mount=False):
        directory = root / name
        directory.mkdir(mode=0o700)
        key = ec.generate_private_key(ec.SECP256R1())
        write_file(directory / 'ca.pem', ca, 0o644)
        write_file(directory / f'{role}.pem', issue(subject(name), key, usage, sans), 0o644)
        # The host directory is owner-only. Docker mounts only this crane's individual
        # files, so UID 10001 can read its own bind-mounted key without a host chown.
        key_file = directory / f'{role}-key.pem'
        write_file(key_file, key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption()), 0o600)
        if bind_mount:
            key_file.chmod(0o644)
        return directory

    # Reserve the final path without replacing anything. Clean up only our staging
    # directory on failure; an existing credential bundle is never modified.
    with tempfile.TemporaryDirectory(prefix='.workload-', dir=base) as staging:
        root = Path(staging) / 'material'
        root.mkdir(mode=0o700)
        for crane in CRANES:
            names = [crane, 'localhost', f'{crane}.cranelock', f'{crane}.cranelock.svc',
                     f'{crane}.cranelock.svc.cluster.local']
            sans = [x509.DNSName(name) for name in names]
            sans.append(x509.IPAddress(ipaddress.ip_address('127.0.0.1')))
            directory = write_role(root, crane, 'server', ExtendedKeyUsageOID.SERVER_AUTH, sans, bind_mount=True)
            allowlist = dict(crane_id=crane, lane_id=config.lane_id, allowed_workload_uri_sans=[IDENTITY])
            write_file(directory / 'allowlist.json', (json.dumps(allowlist) + '\n').encode(), 0o644)
        write_role(root, 'runner', 'client', ExtendedKeyUsageOID.CLIENT_AUTH,
                   [x509.UniformResourceIdentifier(IDENTITY)])
        if include_test_identity:
            write_role(root, 'negative-test', 'client', ExtendedKeyUsageOID.CLIENT_AUTH,
                       [x509.UniformResourceIdentifier(DENIED_IDENTITY)])
        options = runner_config(target / 'runner', {crane: host_endpoint(crane) for crane in CRANES})
        write_file(root / 'config.json', (json.dumps(options, indent=2) + '\n').encode(), 0o600)
        target.mkdir(mode=0o700, exist_ok=False)
        for entry in root.iterdir():
            entry.rename(target / entry.name)
    return target / 'config.json'
