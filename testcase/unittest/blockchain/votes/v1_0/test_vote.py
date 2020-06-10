import pytest

from loopchain.blockchain import Hash32
from loopchain.blockchain.votes.v1_0.vote import BlockVote


class TestVote_v1_0:
    @pytest.fixture
    def dumped_vote(self) -> dict:
        return {
            "!type": "loopchain.blockchain.votes.v1_0.vote.BlockVote",
            "!data": {
                "validator": "hx9f049228bade72bc0a3490061b824f16bbb74589",
                "timestamp": "0x58b01eba4c3fe",
                "blockHeight": "0x16",
                "blockHash": "0x0399e62d77438f940dd207a2ba4593d2b231214606140c0ee6fa8f4fa7ff1d3c",
                "commitHash": "0x0399e62d77438f940dd207a2ba4593d2b231214606140c0ee6fa8f4fa7ff1d3d",
                "stateHash": "0x0399e62d77438f940dd207a2ba4593d2b231214606140c0ee6fa8f4fa7ff1d3e",
                "receiptHash": "0x0399e62d77438f940dd207a2ba4593d2b231214606140c0ee6fa8f4fa7ff1d3f",
                "nextValidatorsHash": "0x0399e62d77438f940dd207a2ba4593d2b231214606140c0ee6fa8f4fa7ff1d3a",
                "epoch": "0x2",
                "round": "0x1",
                "signature": "aC8qGOAO5Fz/lNVZW5nHdR8MiNj5WaDr+2IimKiYJ9dAXLQoaolOU/"
                             "Zmefp9L1lTxAAvbkmWCZVtQpj1lMHClQE="
            }
        }

    @pytest.fixture
    def vote(self, dumped_vote) -> BlockVote:
        return BlockVote.deserialize(dumped_vote)

    def test_vote_deserialized(self, dumped_vote):
        """Test deserialize."""

        # GIVEN I got serialized BlockVote
        # WHEN BlockVote is deserialized
        vote: BlockVote = BlockVote.deserialize(dumped_vote)

        # THEN all properties must be identical
        data = dumped_vote["!data"]
        assert vote.voter_id.hex_hx() == data["validator"]
        assert hex(vote._timestamp) == data["timestamp"]
        assert vote.commit_id.hex_0x() == data["commitHash"]
        assert vote.state_hash.hex_0x() == data["stateHash"]
        assert vote.receipt_hash.hex_0x() == data["receiptHash"]
        assert hex(vote.epoch_num) == data["epoch"]
        assert hex(vote.round_num) == data["round"]
        assert vote.signature.to_base64str() == data["signature"]

        # AND custom properties also should be identical
        assert vote.id == vote.hash

    def test_consensus_hash(self, vote: BlockVote):
        expected = Hash32.fromhex("0x0000000000000000000000000000000000000000000000000000000000000007")

        assert isinstance(vote.consensus_id, Hash32)
        assert vote.consensus_id == expected
        assert vote.data_id ^ vote.receipt_hash == vote.receipt_hash ^ vote.data_id

    @pytest.mark.parametrize("tag", range(3), ids=["current", "generate", "add_bytes"])
    def test_compare_xor(self, benchmark, tag):
        me = Hash32.fromhex("0x1111111111111111111111111111111111111111111111111111111111111111")
        you = Hash32.fromhex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

        def func0(ha0: Hash32, ha1: Hash32):
            pivot = bytearray(ha0)
            for idx, byte in enumerate(ha1):
                pivot[idx] ^= byte

            return Hash32(pivot)

        def func1(ha0: Hash32, ha1: Hash32):
            xored = bytes(h0 ^ h1 for h0, h1 in zip(ha0, ha1))

            return Hash32(xored)

        def func2(ha0: Hash32, ha1: Hash32):
            pivot = b""
            for h0, h1 in zip(ha0, ha1):
                pivot += bytes([h0 ^ h1])

            return Hash32(pivot)

        funcs = {
            0: func0,
            1: func1,
            2: func2
        }

        benchmark(funcs[tag], me, you)
