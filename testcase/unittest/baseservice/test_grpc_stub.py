import datetime
import multiprocessing
import time
from contextlib import contextmanager
from typing import Callable, Optional

import pytest
from freezegun import freeze_time

from loopchain import configure as conf
from loopchain.baseservice.stub_manager import StubManager
from loopchain.peer.peer_outer_service import PeerOuterService
from loopchain.protos import loopchain_pb2_grpc, loopchain_pb2, message_code
from loopchain.tools.grpc_helper import GRPCHelper

PORT = 8080
ECHO_MESSAGE = "Echo this message plz"


@contextmanager
def _run_server(port):
    def __run_server(_port):
        print(f">> Run server at: 127.0.0.1:{_port}")
        grpc_server = GRPCHelper().start_outer_server(str(_port))
        loopchain_pb2_grpc.add_PeerServiceServicer_to_server(PeerOuterService(), grpc_server)
        grpc_server.wait_for_termination(2)

    p = multiprocessing.Process(target=__run_server, args=(port,))
    p.start()
    time.sleep(1)  # Warming Up

    yield

    p.join()

    print(">> Close Server")


@pytest.fixture
def create_grpc_stub() -> Callable[..., StubManager]:
    def _create_grpc_stub(port) -> StubManager:
        stub_manager = StubManager(
            target=f"127.0.0.1:{port}",
            stub_type=loopchain_pb2_grpc.PeerServiceStub
        )

        return stub_manager

    return _create_grpc_stub


@pytest.fixture
def client_response(create_grpc_stub, request) -> Optional[loopchain_pb2.CommonReply]:
    """Create stub with given port and send Echo request."""
    # Given I create gRPC stub with same target
    stub: StubManager = create_grpc_stub(request.param)
    print(">> Client Stub port: ", request.param)

    # AND I send Echo request
    res: loopchain_pb2.CommonReply = stub.call_in_times(
        method_name="Echo",
        message=loopchain_pb2.CommonRequest(request=ECHO_MESSAGE),
        retry_times=2
    )
    print(">>Response from Server", res)

    return res


class TestStubManager:
    def test_is_stub_reused(self, create_grpc_stub):
        # Given I create gRPC StubManager
        stub_manager = StubManager(
            target=f"127.0.0.1:{PORT}",
            stub_type=loopchain_pb2_grpc.PeerServiceStub
        )

        # When I get stub
        stub0 = stub_manager.stub
        # And I get stub again
        stub0_reused = stub_manager.stub

        # Then stubs must be reused (same)
        assert id(stub0) == id(stub0_reused)

        # And it has proper stub class
        assert isinstance(stub0, loopchain_pb2_grpc.PeerServiceStub)
        assert isinstance(stub0_reused, loopchain_pb2_grpc.PeerServiceStub)

        # Given the enough time has been passed
        with freeze_time(datetime.datetime.now() + datetime.timedelta(minutes=conf.STUB_REUSE_TIMEOUT, seconds=1)):
            # When I get stub
            stub1 = stub_manager.stub
            # When I get stub again
            stub1_reused = stub_manager.stub

            # Then it must be differ with the former one (re-created)
            assert id(stub0) != id(stub1)

            # And it has proper stub class
            assert isinstance(stub1, loopchain_pb2_grpc.PeerServiceStub)
            assert isinstance(stub1_reused, loopchain_pb2_grpc.PeerServiceStub)

    @pytest.mark.parametrize("client_port", range(9000, 9600, 100))
    def test_stub_target(self, create_grpc_stub, client_port):
        # When I create gRPC stub by StubManager
        stub: StubManager = create_grpc_stub(client_port)

        # Then expected target should be stub's target.
        for _ in range(5):
            assert stub.target == f"127.0.0.1:{client_port}"

    def test_make_stub_delays_if_no_response(self, create_grpc_stub):
        for _ in range(5):
            stub: StubManager = create_grpc_stub(9000)
            stub.bc_run()


@pytest.mark.slow
class TestRunServerEverytime:
    @pytest.fixture
    def run_server(self, request):
        with _run_server(request.param):
            yield

    @pytest.mark.parametrize("run_server, client_response", [(9000, 9000)], indirect=True)
    def test_success(self, run_server, client_response):
        # When the client target matches server target

        # Then receive successful response
        assert client_response.response_code == message_code.Response.success
        assert client_response.message == ECHO_MESSAGE

    @pytest.mark.parametrize("run_server, client_response", [
        (8080, 9000),
        (9000, 8080),
    ], indirect=True)
    def test_failure(self, run_server, client_response):
        """Suppose that the port between client and server does not match."""
        # When the client and server target do not match for each other

        # Then the return value should be nothing
        assert not client_response


class TestRunServerAndReuse:
    @pytest.fixture(scope="class", autouse=True)
    def run_server(self):
        with _run_server(PORT):
            yield

    @pytest.mark.parametrize("client_response", [PORT]*5, ids=range(5), indirect=True)
    def test_multiple_call_with_same_stub(self, client_response):
        # When I send request to server with recreated stub (See fixture)

        # Then its result should be same
        assert client_response.response_code == message_code.Response.success
        assert client_response.message == ECHO_MESSAGE
