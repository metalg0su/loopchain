import multiprocessing
import time

from loopchain.peer import PeerService, PeerOuterService


# def mock_peer_outer_service():
#     def _mockreturn_get_status_data():
#         return {
#             "block_height": 1,
#             "total_tx": 2,
#             "unconfirmed_block_height": 3,
#             "leader_complaint": 1, "peer_id": "peer_id!"
#         }
#
#     from _pytest.monkeypatch import MonkeyPatch
#     monkeypatch = MonkeyPatch()
#     outer_server = PeerOuterService()
#     monkeypatch.setattr(outer_server,
#                         '_PeerOuterService__get_status_data', _mockreturn_get_status_data)
#
#     print("========", outer_server, dir(outer_server))
#     return outer_server


def mock_peer_outer_service(mocker):
    return_get_status_data = {
        "block_height": 1,
        "total_tx": 2,
        "unconfirmed_block_height": 3,
        "leader_complaint": 1, "peer_id": "peer_id!"
    }

    # mocker.patch("loopchain.peer.PeerOuterService._PeerOuterService__get_status_data", return_value=return_get_status_data)
    mocker.patch("loopchain.peer.PeerOuterService._PeerOuterService__get_status_data", return_value=return_get_status_data)
    outer_server = PeerOuterService()

    print("========", outer_server, dir(outer_server))
    return outer_server


# def run_server():
#     def _run_server():
#         peer_service = PeerService(radio_station_target="")
#         peer_service._PeerService__peer_port = 9000  # listen at '[::]:port'
#
#         outer_server = mock_peer_outer_service()
#
#         peer_service._PeerService__outer_service = outer_server
#         peer_service.run_p2p_server()
#
#         try:
#             while True:
#                 time.sleep(1)
#         except KeyboardInterrupt as e:
#             print("Interrupted!! :", e)
#             peer_service.p2p_server_stop()
#
#     server = multiprocessing.Process(target=_run_server())
#     server.daemon = True
#     server.start()


def run_server():
    peer_service = PeerService(radio_station_target="")
    peer_service._PeerService__peer_port = 9000  # listen at '[::]:port'

    outer_server = mock_peer_outer_service()

    peer_service._PeerService__outer_service = outer_server
    peer_service.run_p2p_server()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt as e:
        print("Interrupted!! :", e)
        peer_service.p2p_server_stop()

# run_server()
