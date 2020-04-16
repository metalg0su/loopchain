import datetime
import os
from collections import OrderedDict
from typing import List, Callable

import pytest
from freezegun import freeze_time
from lft.consensus.epoch import EpochPool
from mock import MagicMock

from loopchain import configure_default as conf
from loopchain import utils
from loopchain.baseservice.aging_cache import AgingCache
from loopchain.blockchain import TransactionOutOfTimeBound, BlockVersionNotMatch, TransactionInvalidError, \
    BlockHeightMismatch
from loopchain.blockchain.blocks import v1_0
from loopchain.blockchain.blocks.v1_0 import BlockBuilder, BlockFactory, Block, BlockVerifier
from loopchain.blockchain.invoke_result import InvokePool, InvokeData
from loopchain.blockchain.transactions import TransactionVersioner
from loopchain.blockchain.transactions import v3
from loopchain.blockchain.types import Hash32, ExternalAddress, Signature
from loopchain.blockchain.votes.v1_0 import BlockVote, BlockVoteFactory
from loopchain.blockchain.votes.v1_0.vote_factory import get_signature
from loopchain.crypto.signature import Signer
from loopchain.store.key_value_store import KeyValueStore

epoch_num = 1
round_num = 1

data_id = Hash32(os.urandom(Hash32.size))
commit_id = Hash32(os.urandom(Hash32.size))

signers = [Signer.from_prikey(os.urandom(32)) for _ in range(4)]
validators = [ExternalAddress.fromhex_address(signer.address) for signer in signers]
builder = BlockBuilder(tx_versioner=TransactionVersioner())
builder.validators = validators
validators_hash = builder.build_validators_hash()
next_validators_hash = validators_hash  # Validators not changed

receipt_hash = Hash32(os.urandom(Hash32.size))
state_hash = Hash32(os.urandom(Hash32.size))


@pytest.fixture
def invoke_data(icon_query: dict, icon_invoke: dict) -> InvokeData:
    next_validators = []
    for validator in validators:
        prep = {
            "id": validator.hex(),
            "p2pEndPoint": "123.45.67.89:7100"
        }
        next_validators.append(prep)

    icon_query["prep"]["nextReps"] = next_validators
    icon_query["prep"]["rootHash"] = next_validators_hash.hex()
    result: InvokeData = InvokeData.from_dict(
        epoch_num=epoch_num,
        round_num=round_num,
        query_result=icon_query
    )
    result.add_invoke_result(invoke_result=icon_invoke)
    print("Receipts: ", result.receipts)

    return result


@pytest.fixture
def invoke_pool(mocker, invoke_data: InvokeData):
    invoke_pool: InvokePool = mocker.MagicMock(InvokePool)
    invoke_pool.prepare_invoke.return_value = invoke_data
    invoke_pool.invoke.return_value = invoke_data

    return invoke_pool


@pytest.fixture
def block_factory(mocker, invoke_pool) -> BlockFactory:
    # TODO: Temporary mocking...
    tx_queue: AgingCache = mocker.MagicMock(AgingCache)
    db: KeyValueStore = mocker.MagicMock(KeyValueStore)
    tx_versioner = TransactionVersioner()

    signer: Signer = Signer.new()
    epoch_pool = EpochPool()

    return BlockFactory(epoch_pool, tx_queue, db, tx_versioner, invoke_pool, signer)


@pytest.fixture
def build_vote() -> Callable[..., BlockVote]:
    def _(**kwargs):
        _signer = kwargs.get("signer", signers[0])
        voter_id = kwargs.get("voter_id", ExternalAddress.fromhex_address(_signer.address))
        _commit_id = kwargs.get("commit_id", Hash32(commit_id))
        _data_id = kwargs.get("data_id", Hash32(data_id))
        _epoch_num = kwargs.get("epoch_num", epoch_num)
        _round_num = kwargs.get("round_num", round_num)
        _state_hash = kwargs.get("state_hash", state_hash)
        _receipt_hash = kwargs.get("receipt_hash", receipt_hash)
        timestamp = kwargs.get("timestamp", utils.get_time_stamp())

        signature = get_signature(
            signer=_signer,
            voter_id=voter_id,
            commit_id=_commit_id,
            data_id=_data_id,
            epoch_num=_epoch_num,
            round_num=_round_num,
            state_hash=_state_hash,
            receipt_hash=_receipt_hash,
            timestamp=timestamp
        )

        return BlockVote(
            data_id=_data_id,
            commit_id=_commit_id,
            voter_id=voter_id,
            epoch_num=_epoch_num,
            round_num=_round_num,
            state_hash=_state_hash,
            receipt_hash=_receipt_hash,
            timestamp=timestamp,
            signature=kwargs.get("signature", signature),
        )

    return _


@pytest.fixture
def build_votes(build_vote) -> Callable[..., List[BlockVote]]:
    def _(_signers=signers):
        votes = []

        for signer in _signers:
            votes.append(build_vote(signer=signer))

        return votes

    return _


@pytest.fixture
def build_block(txs, build_votes) -> Callable[..., Block]:
    def _(**kwargs):
        block_builder = BlockBuilder.new("1.0", TransactionVersioner())

        # Required in Interface
        block_builder.height = kwargs.get("height", 1)
        block_builder.prev_hash = kwargs.get("prev_hash", Hash32.empty())
        block_builder.signer = kwargs.get("signer", signers[0])
        block_builder.transactions = kwargs.get("transactions", txs)

        # Required in 1.0
        block_builder.validators = kwargs.get("validators", validators)
        block_builder.next_validators = kwargs.get("next_validators", validators)
        block_builder.prev_votes = kwargs.get("prev_votes", build_votes())
        block_builder.epoch = kwargs.get("epoch_num", epoch_num)
        block_builder.round = kwargs.get("round_num", round_num)

        # Optional
        block_builder.fixed_timestamp = kwargs.get("timestamp", utils.get_time_stamp())
        block_builder.validators_hash = kwargs.get("validators_hash")
        block_builder.next_validators_hash = kwargs.get("next_validators_hash")

        return block_builder.build()

    return _


@pytest.fixture
def txs(tx_factory) -> OrderedDict:
    transactions = OrderedDict()
    for _ in range(5):
        tx = tx_factory(v3.version)
        transactions[tx.hash] = tx

    return transactions


@pytest.mark.asyncio
class TestVerifyTransactions:
    async def test_verify_pass(self, block_factory: BlockFactory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # AND I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash
        )
        assert block.body.transactions

        # WHEN I try to verify
        with pytest.raises(Exception) as excinfo:
            await verifier.verify(prev_block, block)
        # THEN Exception raises but it is not related to Transactions
        assert excinfo.type not in [TransactionOutOfTimeBound, TransactionInvalidError]

    @pytest.mark.parametrize("time_delta", [
        +datetime.timedelta(seconds=conf.TIMESTAMP_BOUNDARY_SECOND+1),
        -datetime.timedelta(seconds=conf.TIMESTAMP_BOUNDARY_SECOND+1)
    ], ids=["FutureTxs", "PastTxs"])
    async def test_invalid_tx_timestamp(self, block_factory: BlockFactory, tx_factory, build_block, time_delta):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # WHEN I have Txs whose timestamp is invalid
        with freeze_time(datetime.datetime.utcnow() + time_delta):
            future_txs = OrderedDict()
            for _ in range(5):
                tx = tx_factory(v3.version)
                future_txs[tx.hash] = tx
        # AND the block which contains those txs is created just now
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash,
            transactions=future_txs
        )

        # THEN Verification fails
        with pytest.raises(TransactionOutOfTimeBound):
            await verifier.verify(prev_block, block)

    async def test_invalid_tx(self, block_factory: BlockFactory, tx_factory, build_block, monkeypatch):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # WHEN I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash
        )
        assert block.body.transactions

        # AND Suppose that contained txs have certain problems, by mocking TransactionVerifier
        from loopchain.blockchain.transactions import TransactionVerifier
        tx_verifier = MagicMock(TransactionVerifier)
        tx_verifier.verify.side_effect = TransactionInvalidError("invalid_tx", "this is invalid tx!")
        tx_verifier.new.return_value = tx_verifier

        monkeypatch.setattr(v1_0.block_verifier, "TransactionVerifier", tx_verifier)

        # THEN Verification fails
        with pytest.raises(TransactionInvalidError):
            await verifier.verify(prev_block, block)


@pytest.mark.asyncio
class TestVerifyCurrentBlock:
    async def test_verify_pass(self, block_factory: BlockFactory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # AND I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash,
            validators_hash=prev_block.header.next_validators_hash
        )

        # WHEN I try to verify
        with pytest.raises(Exception) as excinfo:
            await verifier.verify(prev_block, block)

        # THEN Exception raises but it is not related to target exceptions
        assert excinfo.type != BlockVersionNotMatch

        # AND Its message is not related to target messages either.
        error_messages = [
            " does not have timestamp",
            " does not have prev_hash",
            " block version is incorrect"
            " Invalid Signature",
        ]
        for msg in error_messages:
            assert msg not in str(excinfo.value)

    @pytest.mark.skip(reason="Cannot make block without timestamp.")
    async def test_block_has_no_timestamp(self, block_factory: BlockFactory, build_block):
        pass

    @pytest.mark.skip(reason="Cannot make block without prev_hash if its height is greater than 0.")
    async def test_block_has_no_prev_hash(self, block_factory: BlockFactory, build_block):
        pass

    async def test_mismatch_version(self, block_factory: BlockFactory, tx_factory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # AND I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash
        )
        assert block.header.version == "1.0"

        # AND Suppose that verifier version is not matched to block version
        verifier.version = "0.5"

        # WHEN I try to verify block, THEN It raises
        with pytest.raises(BlockVersionNotMatch, match=".* block version.* incorrect"):
            await verifier.verify(prev_block, block)

    async def test_invalid_sign(self, block_factory: BlockFactory, tx_factory, build_block, monkeypatch):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # WHEN I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash
        )

        # AND Suppose that its signature has problems, by mocking its verifier
        from loopchain.crypto.signature import SignVerifier
        sign_verifier: SignVerifier = MagicMock(SignVerifier)
        sign_verifier.verify_hash.side_effect = ValueError("signature verification fail!")
        sign_verifier.from_address.return_value = sign_verifier  # BlockVerifier initializes SignVerifier using this

        monkeypatch.setattr(v1_0.block_verifier, "SignVerifier", sign_verifier)

        # THEN Verification fails
        with pytest.raises(RuntimeError, match=".* Invalid Signature.*"):
            await verifier.verify(prev_block, block)


@pytest.mark.asyncio
class TestVerifyPrevBlock:
    async def test_verify_pass(self, block_factory: BlockFactory, build_block, build_votes):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # AND I have block
        block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash
        )

        # WHEN I try to verify
        with pytest.raises(Exception) as excinfo:
            await verifier.verify(prev_block, block)

        # THEN Exception raises but it is not related to target exceptions
        assert excinfo.type != BlockVersionNotMatch

        # AND Its message is not related to target messages either.
        error_messages = [
            " Height",
            " PrevHash",
            " prev_block timestamp"
            " ValidatorsHash",
            " PreviousValidatorsHashByVotes"
        ]
        for msg in error_messages:
            assert msg not in str(excinfo.value)

    async def test_mismatch_curr_prev_height(self, block_factory: BlockFactory, tx_factory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # WHEN I have block
        block: Block = build_block(
            height=prev_block.header.height + 2,
            prev_hash=prev_block.header.hash
        )
        # AND Heights are not linked
        assert block.header.height != prev_block.header.height + 1

        # THEN Verification fails
        with pytest.raises(BlockHeightMismatch, match=".*Height.*"):
            await verifier.verify(prev_block, block)

    async def test_mismatch_curr_prev_hash(self, block_factory: BlockFactory, tx_factory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # WHEN I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=Hash32.empty()
        )
        # AND Hashes are not linked
        assert block.header.prev_hash != prev_block.header.hash

        # THEN Verification fails
        with pytest.raises(RuntimeError, match=".*PrevHash.*"):
            await verifier.verify(prev_block, block)

    async def test_prev_block_ahead_of_curr_block(self, block_factory: BlockFactory, tx_factory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block_timestamp = utils.get_time_stamp()
        prev_block = build_block(
            timestamp=prev_block_timestamp
        )

        # WHEN I have block, created exactly same moment as the prev block is created
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash,
            timestamp=prev_block_timestamp
        )
        assert not prev_block.header.timestamp < block.header.timestamp

        # THEN Verification fails
        with pytest.raises(RuntimeError, match=".*timestamp.*"):
            await verifier.verify(prev_block, block)

    async def test_curr_block_created_too_late(self, block_factory: BlockFactory, tx_factory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # WHEN I have block AND It is created far future
        with freeze_time(datetime.datetime.utcnow() + datetime.timedelta(seconds=1)):
            block: Block = build_block(
                height=prev_block.header.height + 1,
                prev_hash=prev_block.header.hash
            )

        # THEN Verification fails
        with pytest.raises(RuntimeError, match=".*timestamp.*"):
            await verifier.verify(prev_block, block)

    async def test_mismatch_curr_next_validators_hash(self, block_factory: BlockFactory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have a prev block
        expected_next_validators = validators
        prev_block = build_block(
            validators=validators,
            next_validators=expected_next_validators
        )

        # WHEN I got a block with wrong validators
        weird_validators = reversed(validators)
        block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash,
            validators=weird_validators,
            next_validators=expected_next_validators
        )
        assert expected_next_validators != weird_validators

        # THEN Verification fails
        with pytest.raises(RuntimeError, match=".* ValidatorsHash.*"):
            await verifier.verify(prev_block, block)

    async def test_fail_to_rebuild_prev_validators_hash(self, block_factory: BlockFactory, build_block, build_votes):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have a prev block
        prev_block = build_block(
        )

        # WHEN I got a block with wrong validators
        reversed_signers = reversed(signers)
        wrong_prev_votes = build_votes(_signers=reversed_signers)
        block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash,
            prev_votes=wrong_prev_votes
        )

        # THEN Verification fails
        with pytest.raises(RuntimeError, match=".* PreviousValidatorsHashByVotes.*"):
            await verifier.verify(prev_block, block)


@pytest.mark.asyncio
class TestVerifyPrevVotes:
    async def test_vote_quorum(self, block_factory: BlockFactory, build_block):
        pytest.fail("verify here? or In Library?")


@pytest.mark.asyncio
class TestVerifyInvoke:
    async def test_verify_pass(self, block_factory: BlockFactory, build_block):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # AND I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash
        )
        assert block.body.transactions

        # WHEN I try to verify
        await verifier.verify(prev_block, block)

    async def test_consensus_id(self, block_factory: BlockFactory, build_block):
        pytest.fail("Implement ConsensusID first!")

    async def test_next_validators_not_changed(self, block_factory: BlockFactory, build_block):
        pytest.fail("not yet")

    async def test_next_validators_changed(self, block_factory: BlockFactory, build_block):
        pytest.fail("not yet")

    async def test_mismatch_prev_receipts_hash(self, block_factory: BlockFactory, build_block):
        pytest.fail("not yet")

    async def test_mismatch_logs_bloom(self, block_factory: BlockFactory, build_block):
        pytest.fail("not yet")

    async def test_mismatch_tx_hash(self, block_factory: BlockFactory, build_block, monkeypatch):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # WHEN I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash
        )

        # AND Suppose that something is wrong in building transactions hash
        monkeypatch.setattr(v1_0.block_verifier.BlockBuilder, "_build_transactions_hash", lambda x: Hash32.empty())

        # THEN Verification fails
        with pytest.raises(RuntimeError, match="TransactionsRootHash"):
            await verifier.verify(prev_block, block)

    async def test_mismatch_validators_hash(self, block_factory: BlockFactory, build_block, monkeypatch):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # WHEN I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash
        )

        # AND Suppose that something is wrong in building validators hash
        monkeypatch.setattr(v1_0.block_verifier.BlockBuilder, "_build_validators_hash", lambda x: Hash32.empty())

        # THEN Verification fails
        with pytest.raises(RuntimeError, match="RepRootHash"):
            await verifier.verify(prev_block, block)

    async def test_mismatch_prev_votes_hash(self, block_factory: BlockFactory, build_block, monkeypatch):
        # GIVEN I have a BlockVerifier
        verifier: BlockVerifier = await block_factory.create_data_verifier()

        # AND I have prev block
        prev_block = build_block()

        # WHEN I have block
        block: Block = build_block(
            height=prev_block.header.height + 1,
            prev_hash=prev_block.header.hash
        )

        # AND Suppose that something is wrong in building transactions_hash
        monkeypatch.setattr(v1_0.block_verifier.BlockBuilder, "_build_prev_votes_hash", lambda x: Hash32.empty())

        # THEN Verification fails
        with pytest.raises(RuntimeError, match="RepRootHash"):
            await verifier.verify(prev_block, block)

    async def test_mismatch_block_hash(self, block_factory: BlockFactory, build_block, monkeypatch):
        pytest.fail("not yet")
