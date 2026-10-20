"""Compose health check: occupancy UNKNOWN still means a healthy gate."""
import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc


def main():
    try:
        with grpc.insecure_channel("127.0.0.1:50051") as channel:
            response = health_pb2_grpc.HealthStub(channel).Check(
                health_pb2.HealthCheckRequest(service="cranelock.gate.v1.TrustedAccessGate"), timeout=2)
            raise SystemExit(0 if response.status == health_pb2.HealthCheckResponse.SERVING else 1)
    except grpc.RpcError:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
