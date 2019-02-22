# Copyright 2018 ICON Foundation
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
"""It manages the information needed during consensus to store one block height.
Candidate Blocks, Quorum, Votes and Leader Complaints.
"""
import logging
import traceback

import loopchain.utils as util
from loopchain import configure as conf
from loopchain.baseservice import ObjectManager
from loopchain.blockchain import Vote, BlockBuilder, Transaction, TransactionStatusInQueue, TransactionVerifier


class Epoch:
    COMPLAIN_VOTE_HASH = "complain_vote_hash_for_reuse_Vote_class"

    def __init__(self, block_manager, leader_id=None):
        if block_manager.get_blockchain().last_block:
            self.height = block_manager.get_blockchain().last_block.header.height + 1
        else:
            self.height = 1
        self.leader_id = leader_id
        self.prev_leader_id = None
        self.__block_manager = block_manager
        self.__blockchain = self.__block_manager.get_blockchain()
        util.logger.debug(f"New Epoch Start height({self.height }) leader_id({leader_id})")

        # TODO using Epoch in BlockManager instead using candidate_blocks directly.
        # But now! only collect leader complain votes.
        self.__candidate_blocks = None
        self.__complain_vote = Vote(Epoch.COMPLAIN_VOTE_HASH, ObjectManager().channel_service.peer_manager)

    @staticmethod
    def new_epoch(leader_id=None):
        block_manager = ObjectManager().channel_service.block_manager
        leader_id = leader_id or ObjectManager().channel_service.block_manager.epoch.leader_id
        return Epoch(block_manager, leader_id)

    def set_epoch_leader(self, leader_id):
        util.logger.debug(f"Set Epoch leader height({self.height}) leader_id({leader_id})")
        self.leader_id = leader_id
        self.__complain_vote = Vote(Epoch.COMPLAIN_VOTE_HASH, ObjectManager().channel_service.peer_manager)

    def add_complain(self, complained_leader_id, new_leader_id, block_height, peer_id, group_id):
        util.logger.debug(f"add_complain complain_leader_id({complained_leader_id}), "
                          f"new_leader_id({new_leader_id}), "
                          f"block_height({block_height}), "
                          f"peer_id({peer_id})")
        self.__complain_vote.add_vote(group_id, peer_id, new_leader_id)

    def complain_result(self) -> str or None:
        """return new leader id when complete complain leader.

        :return: new leader id or None
        """
        vote_result = self.__complain_vote.get_result(Epoch.COMPLAIN_VOTE_HASH, conf.LEADER_COMPLAIN_RATIO)
        util.logger.debug(f"complain_result vote_result({vote_result})")

        return vote_result

    def _check_unconfirmed_block(self):
        blockchain = self.__block_manager.get_blockchain()
        # util.logger.debug(f"-------------------_check_unconfirmed_block, "
        #                    f"candidate_blocks({len(self._block_manager.candidate_blocks.blocks)})")
        if blockchain.last_unconfirmed_block:
            vote = self.__block_manager.candidate_blocks.get_vote(blockchain.last_unconfirmed_block.header.hash)
            # util.logger.debug(f"-------------------_check_unconfirmed_block, "
            #                    f"last_unconfirmed_block({self._blockchain.last_unconfirmed_block.header.hash}), "
            #                    f"vote({vote.votes})")
            vote_result = vote.get_result(blockchain.last_unconfirmed_block.header.hash.hex(), conf.VOTING_RATIO)
            if not vote_result:
                util.logger.debug(f"last_unconfirmed_block({blockchain.last_unconfirmed_block.header.hash}), "
                                  f"vote result({vote_result})")

    def __add_tx_to_block(self, block_builder):
        tx_queue = self.__block_manager.get_tx_queue()

        block_tx_size = 0
        tx_versioner = self.__blockchain.tx_versioner
        while tx_queue:
            if block_tx_size >= conf.MAX_TX_SIZE_IN_BLOCK:
                logging.debug(f"consensus_base total size({block_builder.size()}) "
                              f"count({len(block_builder.transactions)}) "
                              f"_txQueue size ({len(tx_queue)})")
                break

            tx: 'Transaction' = tx_queue.get_item_in_status(
                TransactionStatusInQueue.normal,
                TransactionStatusInQueue.added_to_block
            )
            if tx is None:
                break

            tv = TransactionVerifier.new(tx.version, tx_versioner)

            try:
                tv.verify(tx, self.__blockchain)
            except Exception as e:
                logging.warning(f"tx hash invalid.\n"
                                f"tx: {tx}\n"
                                f"exception: {e}")
                traceback.print_exc()
            else:
                block_builder.transactions[tx.hash] = tx
                block_tx_size += tx.size(tx_versioner)

    def makeup_block(self):
        # self._check_unconfirmed_block(
        block_height = self.__blockchain.last_block.header.height + 1
        block_version = self.__blockchain.block_versioner.get_version(block_height)
        block_builder = BlockBuilder.new(block_version, self.__blockchain.tx_versioner)
        if self.complain_result():
            block_builder.complained = self.complain_result()
        else:
            block_builder.complained = False
            self.__add_tx_to_block(block_builder)

        return block_builder
