"""Optional mutual TLS for agent-to-agent RPCs, with identity binding.

Each agent certificate carries exactly one URI SAN, ``urn:cranelock:agent:<id>``,
plus a DNS SAN equal to the agent ID. Clients pin the expected peer by
overriding the TLS target name to that ID, so a channel to crane-2 only accepts
crane-2's certificate. Servers compare the client certificate's URI SAN with the
identity claimed inside each request, so a CA-signed peer cannot impersonate
another member in heartbeats, queue requests, transfers or recovery votes.
"""
from dataclasses import dataclass
from pathlib import Path

import grpc
from cryptography import x509

AGENT_URI_PREFIX = "urn:cranelock:agent:"


def agent_uri(agent_id):
    return AGENT_URI_PREFIX + agent_id


def certificate_uri(pem):
    """The single URI SAN of a PEM certificate, or None."""
    try:
        certificate = x509.load_pem_x509_certificate(pem)
        sans = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        identities = sans.get_values_for_type(x509.UniformResourceIdentifier)
    except (ValueError, x509.ExtensionNotFound):
        return None
    return identities[0] if len(identities) == 1 else None


def context_uri(context):
    """The verified client certificate URI SAN of an mTLS gRPC call, or None."""
    try:
        auth = context.auth_context()
    except AttributeError:
        return None
    if auth.get("transport_security_type") != [b"ssl"]:
        return None
    certificates = auth.get("x509_pem_cert", [])
    return certificate_uri(certificates[0]) if len(certificates) == 1 else None


@dataclass(frozen=True)
class PeerTLS:
    ca: bytes
    cert: bytes
    key: bytes

    @classmethod
    def load(cls, ca, cert, key):
        return cls(Path(ca).read_bytes(), Path(cert).read_bytes(), Path(key).read_bytes())

    def validate_for(self, agent_id):
        if certificate_uri(self.cert) != agent_uri(agent_id):
            raise ValueError("Peer TLS certificate does not identify this agent")

    def server_credentials(self):
        return grpc.ssl_server_credentials([(self.key, self.cert)], root_certificates=self.ca,
                                           require_client_auth=True)

    def channel(self, peer_id, address):
        credentials = grpc.ssl_channel_credentials(self.ca, self.key, self.cert)
        # Pin the server identity to the expected member, not to the dialled host.
        return grpc.aio.secure_channel(address, credentials,
                                       options=(("grpc.ssl_target_name_override", peer_id),))
