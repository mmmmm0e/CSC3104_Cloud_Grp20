"""Ephemeral workload certificates for isolated local evaluation only."""
import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

IDENTITY = "urn:cranelock:workload:deployed-runner"


def write(directory, extra_identities=()):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "CraneLock evaluation CA")])

    def issue(name, key, ca=False, sans=(), usage=None):
        builder = (x509.CertificateBuilder().subject_name(name).issuer_name(ca_name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now-datetime.timedelta(minutes=5))
            .not_valid_after(now+datetime.timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True))
        if sans:
            builder = builder.add_extension(x509.SubjectAlternativeName(list(sans)), critical=False)
        if usage:
            builder = builder.add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        return builder.sign(ca_key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM)

    (directory/"ca.pem").write_bytes(issue(ca_name, ca_key, ca=True))
    roles = [
        ("server", [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))], ExtendedKeyUsageOID.SERVER_AUTH),
        ("client", [x509.UniformResourceIdentifier(IDENTITY)], ExtendedKeyUsageOID.CLIENT_AUTH)]
    roles.extend((f"extra-{index}", [x509.UniformResourceIdentifier(identity)], ExtendedKeyUsageOID.CLIENT_AUTH)
                 for index, identity in enumerate(extra_identities))
    for role, sans, usage in roles:
        key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "evaluation-"+role)])
        (directory/f"{role}.pem").write_bytes(issue(name, key, sans=sans, usage=usage))
        (directory/f"{role}-key.pem").write_bytes(key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
