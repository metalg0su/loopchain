"""PeerListData Loader for PeerManager"""

import os

from loopchain import configure as conf
from loopchain import utils
from loopchain.baseservice import ObjectManager, RestMethod
from loopchain.blockchain.blocks import BlockProverType
from loopchain.blockchain.blocks.v0_3 import BlockProver
from loopchain.blockchain.types import ExternalAddress, Hash32
from loopchain.channel.channel_property import ChannelProperty


class Prep:
    def __init__(self, address: ExternalAddress, endpoint: str):
        self._address: ExternalAddress = address
        self._endpoint: str = endpoint

    @property
    def address(self) -> ExternalAddress:
        return self._address

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @classmethod
    def from_(cls, data: dict):
        address = data.get("id") or data.get("address")
        address = ExternalAddress.fromhex_address(address)

        endpoint = data.get("p2pEndpoint") or data.get("peer_target")

        return cls(address=address, endpoint=endpoint)

    def serialize(self) -> dict:
        return {
            "id": self._address.hex_hx(),
            "p2pEndpoint": self._endpoint
        }


class PeerLoader:
    def __init__(self):
        pass

    @staticmethod
    def load():
        crep_root_hash: str = conf.CHANNEL_OPTION[ChannelProperty().name].get('crep_root_hash')
        peers = PeerLoader._load_peers_from_db(crep_root_hash)
        if peers:
            utils.logger.info("Reps data loaded from DB")
        elif os.path.exists(conf.CHANNEL_MANAGE_DATA_PATH):
            utils.logger.info(f"Try to load reps data from {conf.CHANNEL_MANAGE_DATA_PATH}")
            peers = PeerLoader._load_peers_from_file()
        else:
            utils.logger.info("Try to load reps data from other reps")
            peers = PeerLoader._load_peers_from_rest_call(crep_root_hash)

        peer_root_hash = PeerLoader._get_peer_root_hash(peers)

        return peer_root_hash, peers

    @staticmethod
    def _get_peer_root_hash(peers: list):
        block_prover = BlockProver((peer.address.extend() for peer in peers), BlockProverType.Rep)

        return block_prover.get_proof_root()

    @staticmethod
    def _load_peers_from_db(reps_hash: str) -> list:
        blockchain = ObjectManager().channel_service.block_manager.blockchain
        last_block = blockchain.last_block
        rep_root_hash = (last_block.header.reps_hash if last_block else Hash32.fromhex(reps_hash))
        reps = blockchain.find_preps_by_roothash(rep_root_hash)

        return [Prep.from_(rep) for rep in reps]

    @staticmethod
    def _load_peers_from_file():
        channel_info = utils.load_json_data(conf.CHANNEL_MANAGE_DATA_PATH)
        reps: list = channel_info[ChannelProperty().name].get("peers")
        return [Prep.from_(rep) for rep in reps]

    @staticmethod
    def _load_peers_from_rest_call(crep_root_hash: str):
        rs_client = ObjectManager().channel_service.rs_client
        reps = rs_client.call(
            RestMethod.GetReps,
            RestMethod.GetReps.value.params(crep_root_hash)
        )
        return [Prep.from_(rep) for rep in reps]
