from loopchain.blockchain import CandidateBlocks, BlockChain


class Syncer:
    def __init__(self, candidate_blocks: CandidateBlocks, blockchain: BlockChain):
        """

        :param candidate_blocks:
        :param blockchain: needed to access DB
        """
        self._candidate_blocks: CandidateBlocks = candidate_blocks
        self._blockchain: BlockChain = blockchain

    def sync(self):
        pass

    def _request_block(self):
        pass

    def _write_block(self):
        pass

    def _get_network_max_height(self):
        pass

    def _get_peer_stub_list(self):
        pass
