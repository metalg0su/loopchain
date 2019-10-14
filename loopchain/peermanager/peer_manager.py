# Copyright 2019 ICON Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A module for managing peer list"""

import json
import logging
import math
import threading
from typing import Optional, Union

import loopchain.utils as util
from loopchain import configure as conf
from loopchain.baseservice import ObjectManager, StubManager
from loopchain.blockchain.blocks import BlockProverType
from loopchain.blockchain.blocks.v0_3 import BlockProver
from loopchain.blockchain.types import ExternalAddress, Hash32
from loopchain.peermanager import Peer, PeerLoader, PeerListData
from loopchain.protos import loopchain_pb2


class PeerManager:
    def __init__(self):
        """Manage peer list in operation."""

        self._peer_list_data = PeerListData()

        # lock object for if add new peer don't have order that must locking
        self.__add_peer_lock: threading.Lock = threading.Lock()

        # reps_hash, reps for reset_all_peers
        self._reps_reset_data: Optional[tuple] = None

        self._prepared_reps_hash = None

    @property
    def peer_list(self) -> dict:
        """return peer_list of peer_list_data

        :return:
        """
        return self._peer_list_data.peer_list

    @property
    def prepared_reps_hash(self):
        return self._prepared_reps_hash

    def reps_hash(self) -> Hash32:
        """return reps root hash.

        :return:
        """
        block_prover = BlockProver((ExternalAddress.fromhex_address(peer.peer_id).extend()
                                    for peer in self._peer_list_data.peer_list.values()),
                                   BlockProverType.Rep)
        return block_prover.get_proof_root()

    def serialize_as_preps(self) -> list:
        return [{'id': peer_id, 'p2pEndpoint': peer.target}
                for peer_id, peer in self._peer_list_data.peer_list.items()]

    def load_peers(self) -> None:
        PeerLoader.load(peer_manager=self)
        blockchain = ObjectManager().channel_service.block_manager.blockchain

        reps_hash = self.reps_hash()
        reps_in_db = blockchain.find_preps_by_roothash(reps_hash)

        if not reps_in_db:
            preps = self.serialize_as_preps()
            util.logger.spam(f"in _load_peers serialize_as_preps({preps})")
            blockchain.write_preps(reps_hash, preps)

    def get_quorum(self):
        peer_count = self.get_peer_count()
        quorum = math.floor(peer_count * conf.VOTING_RATIO) + 1
        complain_quorum = math.floor(peer_count * (1-conf.VOTING_RATIO)) + 1

        return quorum, complain_quorum

    def get_reps(self):
        return [{"id": peer.peer_id, "target": peer.target} for peer in self.peer_list.values()]

    def get_peer_by_target(self, peer_target):
        return next((peer for peer in self.peer_list.values() if peer.target == peer_target), None)

    def add_peer(self, peer: Union[Peer, dict]):
        """add_peer to peer_manager

        :param peer: Peer, dict
        :return: create_peer_order
        """

        if isinstance(peer, dict):
            peer = Peer(peer["id"], peer["peer_target"], order=peer["order"])

        util.logger.debug(f"add peer id: {peer.peer_id}")

        # add_peer logic must be atomic
        with self.__add_peer_lock:
            last_peer = self._peer_list_data.last_peer()
            if last_peer and peer.order <= last_peer.order:
                util.logger.warning(f"Fail add peer order({peer.order}), last peer order({last_peer.order})"
                                    f"\npeers({self._peer_list_data.peer_list})")
                return None

            self.peer_list[peer.peer_id] = peer
            self._prepared_reps_hash = self.reps_hash()

        return peer.order

    def remove_peer(self, peer_id):
        logging.debug(f"remove peer : {peer_id}")
        removed_peer = self._peer_list_data.peer_list.pop(peer_id, None)
        if removed_peer:
            util.logger.spam(f"peer_manager:remove_peer try remove audience in sub processes")
            self._prepared_reps_hash = self.reps_hash()
            return True

        return False

    def get_peer_stub_manager(self, peer) -> Optional[StubManager]:
        logging.debug(f"get_peer_stub_manager peer_id : {peer.peer_id}")

        try:
            return self.peer_list[peer.peer_id].stub_manager
        except Exception as e:
            logging.debug("try get peer stub except: " + str(e))
            return None

    def __find_highest_peer(self) -> Peer:
        # 강제로 list 를 적용하여 값을 복사한 다음 사용한다. (중간에 값이 변경될 때 발생하는 오류를 방지하기 위해서)
        most_height = 0
        most_height_peer = None
        for peer_id in list(self.peer_list):
            peer_each = self.peer_list[peer_id]
            stub_manager = peer_each.stub_manager
            try:
                response = stub_manager.call("GetStatus",
                                             loopchain_pb2.StatusRequest(request="find highest peer"),
                                             is_stub_reuse=True)

                peer_status = json.loads(response.status)
                if int(peer_status["block_height"]) >= most_height:
                    most_height = int(peer_status["block_height"])
                    most_height_peer = peer_each
            except Exception as e:
                logging.warning("gRPC Exception: " + str(e))

        return most_height_peer

    def reset_all_peers(self, reps_hash, reps, update_now=True):
        util.logger.debug(
            f"reset_all_peers."
            f"\nresult roothash({reps_hash})"
            f"\npeer_list roothash({self.reps_hash().hex()})"
            f"\nupdate now({update_now})")

        if not update_now:
            self._reps_reset_data = (reps_hash, reps)
            return

        blockchain = ObjectManager().channel_service.block_manager.blockchain

        if reps_hash == self.reps_hash().hex():
            util.logger.debug(f"There is no change in load_peers_from_iiss.")
            return

        for peer_id in list(self.peer_list):
            self.remove_peer(peer_id)

        for order, rep_info in enumerate(reps, 1):
            peer = Peer(rep_info["id"], rep_info["p2pEndpoint"], order=order)
            self.add_peer(peer)

        blockchain.reset_leader_made_block_count()

    def update_all_peers(self):
        if self._reps_reset_data:
            self.reset_all_peers(*self._reps_reset_data, update_now=True)
            self._reps_reset_data = None

    def get_peer_count(self):
        count = 0
        try:
            count = len(self.peer_list)
        except KeyError:
            logging.debug("no peer list")

        return count
