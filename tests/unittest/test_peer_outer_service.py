import multiprocessing
import time

import grpc
import pytest

from loopchain.peer import PeerOuterService, PeerService
from loopchain.protos import loopchain_pb2


# def mock_peer_outer_service(mocker):
#     return_get_status_data = {
#         "block_height": 1,
#         "total_tx": 2,
#         "unconfirmed_block_height": 3,
#         "leader_complaint": 1, "peer_id": "peer_id!"
#     }
#
#     mocker.patch("loopchain.peer.PeerOuterService._PeerOuterService__get_status_data", return_value=return_get_status_data)
#     outer_server = PeerOuterService()
#
#     print("========", outer_server, dir(outer_server))
#     return outer_server


def run_server():
    peer_service = PeerService(radio_station_target="")
    peer_service._PeerService__peer_port = 9000  # listen at '[::]:port'

    return_get_status_data = {
        "block_height": 1,
        "total_tx": 2,
        "unconfirmed_block_height": 3,
        "leader_complaint": 1, "peer_id": "peer_id!"
    }

    outer_server = None

    def mock_service(mocker):
        mocker.patch("loopchain.peer.PeerOuterService._PeerOuterService__get_status_data", return_value=return_get_status_data)
        outer_server = PeerOuterService()
        return outer_server

    print("========", outer_server, dir(outer_server))

    peer_service._PeerService__outer_service = outer_server
    peer_service.run_p2p_server()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt as e:
        print("Interrupted!! :", e)
        peer_service.p2p_server_stop()


@pytest.fixture
def stub():
    channel = grpc.insecure_channel('localhost:9000')
    stub = loopchain_pb2.PeerServiceStub(channel)

    return stub


# @pytest.mark.skip
def test_echo(stub):
    expected_message = "hello, server!"
    req = loopchain_pb2.CommonRequest(request=expected_message)
    result = stub.Echo(req)

    assert result.message == expected_message


@pytest.mark.skip
def test_get_status(stub):
    req = loopchain_pb2.StatusRequest(request="", channel="channel")
    result = stub.GetStatus(req)
    print(result)
    assert 0


# proc = subprocess.Popen(["python", "server.py"])
proc = multiprocessing.Process(target=run_server)
proc.start()
time.sleep(2)
