from typing import TYPE_CHECKING, List

from lft.consensus.messages.data import DataVerifier

from loopchain import configure_default as conf
from loopchain import utils
from loopchain.blockchain.blocks.v1_0 import BlockBuilder
from loopchain.blockchain.exception import BlockVersionNotMatch, TransactionOutOfTimeBound, BlockHeightMismatch
from loopchain.blockchain.invoke_result import InvokePool, InvokeRequest, InvokeData
from loopchain.blockchain.transactions import TransactionVerifier, TransactionVersioner
from loopchain.blockchain.types import ExternalAddress, Hash32
from loopchain.crypto.signature import SignVerifier

if TYPE_CHECKING:
    from loopchain.blockchain.blocks.v1_0.block import Block, BlockHeader


class BlockVerifier(DataVerifier):
    version = "1.0"

    def __init__(self, tx_versioner: TransactionVersioner, invoke_pool: InvokePool):
        self._invoke_pool: InvokePool = invoke_pool
        self._tx_versioner: TransactionVersioner = tx_versioner

    async def verify(self, prev_data: 'Block', data: 'Block'):
        self._verify_transactions(data)
        self._verify_current_block(data)
        self._verify_prev_block(prev_data, data)
        # self._verify_prev_votes(prev_data, data)
        self._verify_invoke(data)

    def _verify_transactions(self, block: 'Block'):
        for tx in block.body.transactions.values():
            if not utils.is_in_time_boundary(tx.timestamp, conf.TIMESTAMP_BOUNDARY_SECOND, block.header.timestamp):
                raise TransactionOutOfTimeBound(tx, block.header.timestamp)

            tv = TransactionVerifier.new(tx.version, tx.type(), self._tx_versioner)
            tv.verify(tx)

    def _verify_current_block(self, block: 'Block'):
        header: 'BlockHeader' = block.header
        if header.timestamp is None:
            raise RuntimeError(f"Block({header.height}, {header.hash.hex()} does not have timestamp.")
        if header.height > 0 and header.prev_hash is None:
            raise RuntimeError(f"Block({header.height}, {header.hash.hex()} does not have prev_hash.")

        if header.version != self.version:
            raise BlockVersionNotMatch(header.version, self.version, f"The block version is incorrect. Block({header})")

        # FIXME: There's no peer id in BlockHeader 1.0
        sign_verifier = SignVerifier.from_address(block.header.peer_id.hex_xx())
        try:
            sign_verifier.verify_hash(block.header.hash, block.header.signature)
        except Exception as e:
            raise RuntimeError(f"Block({block.header.height}, {block.header.hash.hex()}, Invalid Signature {e}")

    def _verify_prev_block(self, prev_block, block):
        if block.header.height != prev_block.header.height + 1:
            raise BlockHeightMismatch(
                f"Block({block.header.height}, {block.header.hash.hex()}, "
                f"Height({block.header.height}), "
                f"Expected({prev_block.header.height + 1})."
            )

        if block.header.prev_hash != prev_block.header.hash:
            raise RuntimeError(
                f"Block({block.header.height}, {block.header.hash.hex()}, "
                f"PrevHash({block.header.prev_hash.hex()}), "
                f"Expected({prev_block.header.hash.hex()})."
            )

        valid_max_timestamp = utils.get_time_stamp() + conf.TIMESTAMP_BUFFER_IN_VERIFIER
        if prev_block and not (prev_block.header.timestamp < block.header.timestamp < valid_max_timestamp):
            raise RuntimeError(
                f"Block({block.header.height}, {block.header.hash.hex()},"
                f"timestamp({block.header.timestamp} is invalid. "
                f"prev_block timestamp({prev_block.header.timestamp}), "
                f"current timestamp({utils.get_now_time_stamp()}"
            )

        expected_current_validators_hash = prev_block.header.next_validators_hash
        current_validators_hash = block.header.validators_hash
        if current_validators_hash != expected_current_validators_hash:
            raise RuntimeError(
                f"Block({block.header.height}, {block.header.hash.hex()}, "
                f"ValidatorsHash({current_validators_hash}), "
                f"Expected({expected_current_validators_hash})."
            )

        # FIXME
        prev_votes = block.body.prev_votes
        prev_validators: List[ExternalAddress] = [prev_vote.voter_id for prev_vote in prev_votes]

        temp_builder = BlockBuilder(self._tx_versioner)
        temp_builder.validators = prev_validators
        prev_validators_hash = temp_builder.build_validators_hash()

        if prev_block.header.validators_hash != prev_validators_hash:
            raise RuntimeError(
                f"Block({block.header.height}, {block.header.hash.hex()}, "
                f"PreviousValidatorsHashByVotes({prev_validators_hash}), "
                f"Expected({prev_block.header.validators_hash}). "
            )

    def _verify_invoke(self, block):
        builder = BlockBuilder.from_new(block, self._tx_versioner)
        builder.reset_cache()
        builder.peer_id = block.header.peer_id
        builder.signature = block.header.signature

        invoke_result = self._do_invoke(block)
        # TODO: Check CandidateID === (StateHash, ReceiptHash, BlockHash)

        self._verify_next_validators(builder, block, invoke_result)
        self._verify_logs_bloom(builder, block, invoke_result)
        # TODO: CONSENSUS ID?

        self._verify_transactions_hash(builder, block)
        # FIXME: Why verify here?
        # builder.build_validators_hash()
        # if block.header.validators_hash != builder.validators_hash:
        #     raise RuntimeError(
        #         f"Block({header.height}, {header.hash.hex()}, "
        #         f"RepRootHash({header.validators_hash.hex()}), "
        #         f"Expected({builder.validators_hash.hex()})."
        #     )

        # TODO: MOVE TO Verify VOtes?
        self._verify_prev_votes_hash(builder, block)
        self._verify_block_hash(builder, block)

    def _do_invoke(self, block) -> InvokeData:
        invoke_request = InvokeRequest.from_block(block=block)
        invoke_result = self._invoke_pool.invoke(
            epoch_num=block.header.epoch,
            round_num=block.header.round,
            invoke_request=invoke_request
        )

        return invoke_result

    def _verify_next_validators(self, builder, block, invoke_result):
        next_validators = invoke_result.next_validators
        if next_validators:
            next_validators = [ExternalAddress.fromhex(next_validator["id"], ignore_prefix=True)
                               for next_validator in next_validators]
            builder.next_validators = next_validators
        else:
            builder.next_validators_hash = block.header.validators_hash  # FIXME: Hash32.empty()? build from prev votes?
        builder.build_next_validators_hash()
        if block.header.next_validators_hash != invoke_result.next_validators_hash:
            raise RuntimeError(
                f"Block({block.header.height}, {block.header.hash.hex()}, "
                f"NextRepsHash({block.header.next_validators_hash}), "
                f"Expected({invoke_result.next_validators_hash}). "
            )

    def _verify_logs_bloom(self, builder, block, invoke_result):
        # TODO: WHAT TYPE prev_receipts expects?
        builder.prev_receipts = invoke_result.receipts
        builder.build_logs_bloom()
        if block.header.logs_bloom != builder.logs_bloom:
            raise RuntimeError(
                f"Block({block.header.height}, {block.header.hash.hex()}, "
                f"LogsBloom({block.header.logs_bloom.hex()}), "
                f"Expected({builder.logs_bloom.hex()})."
            )

    def _verify_transactions_hash(self, builder, block):
        builder.build_transactions_hash()
        if block.header.transactions_hash != builder.transactions_hash:
            raise RuntimeError(
                f"Block({block.header.height}, {block.header.hash.hex()}, "
                f"TransactionsRootHash({block.header.transactions_hash.hex()}), "
                f"Expected({builder.transactions_hash.hex()})."
            )

    def _verify_prev_votes_hash(self, builder, block):
        builder.build_prev_votes_hash()
        if block.header.prev_votes_hash != builder.prev_votes_hash:
            raise RuntimeError(
                f"Block({block.header.height}, {block.header.hash.hex()}, "
                f"PrevVoteRootHash({block.header.prev_votes_hash.hex()}), "
                f"Expected({block.builder.prev_votes_hash.hex()})."
            )

    def _verify_block_hash(self, builder, block):
        builder.build_hash()
        if block.header.hash != builder.hash:
            raise RuntimeError(
                f"Block({block.header.height}, {block.header.hash.hex()}, "
                f"Hash({block.header.hash.hex()}, "
                f"Expected({builder.hash.hex()}), "
                f"header({block.header}), "
                f"builder({builder.build_block_header_data()})."
            )

