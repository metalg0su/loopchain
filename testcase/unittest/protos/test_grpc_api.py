import grpc
import pytest

from loopchain.peer import PeerOuterService
from loopchain.protos import loopchain_pb2_grpc, loopchain_pb2, message_code
from loopchain.tools.grpc_helper import GRPCHelper

PEER_PORT = 7100


class Test:
    @pytest.fixture(autouse=True)
    def setup(self):
        outer_service = PeerOuterService()
        p2p_outer_server = GRPCHelper().start_outer_server(f"{PEER_PORT}")
        loopchain_pb2_grpc.add_PeerServiceServicer_to_server(outer_service, p2p_outer_server)

        yield

        p2p_outer_server.stop(None)

    def test(self):
        request = loopchain_pb2.Message(code=message_code.Request.status)
        with grpc.insecure_channel(f"localhost:{PEER_PORT}") as channel:
            stub = loopchain_pb2_grpc.PeerServiceStub(channel)
            response = stub.Request(request)

        print("RRR: ", response)
