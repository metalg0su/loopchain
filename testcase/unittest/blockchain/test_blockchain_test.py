#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
"""Test block chain class"""

import json
from typing import List

import pytest

from loopchain import configure as conf
from loopchain.blockchain.blockchain import BlockChain
from loopchain.blockchain.blocks import BlockBuilder
from loopchain.blockchain.transactions import TransactionVersioner
from loopchain.blockchain.types import Hash32, ExternalAddress
from loopchain.store.key_value_store import KeyValueStore

channel_name = "test_channel"
store_id = "blockchain_store_db"
MOCK_CHANNEL_CONFIG = {
    # LOOPCHAIN_DEFAULT_CHANNEL: {
    channel_name: {
        "block_versions": {
            "0.1a": 0
        },
        "hash_versions": {
            "genesis": 1,
            "0x2": 1,
            "0x3": 1
        },
        "load_cert": False,
        "consensus_cert_use": False,
        "tx_cert_use": False,
        # "key_load_type": KeyLoadType.FILE_LOAD,
        "role_switch_block_height": -1,
    }
}

genesis_data = {
    "transaction_data": {
        "nid": "0x3",
        "accounts": [
            {
                "name": "god",
                "address": "hx5a05b58a25a1e5ea0f1d5715e1f655dffc1fb30a",
                "balance": "0x2961fd42d71041700b90000"
            },
            {
                "name": "treasury",
                "address": "hxd5775948cb745525d28ec8c1f0c84d73b38c78d4",
                "balance": "0x0"
            },
            {
                "name": "test1",
                "address": "hx86aba2210918a9b116973f3c4b27c41a54d5dafe",
                "balance": "0x56bc75e2d63100000"
            }
        ],
        "message": "A rHizomE has no beGInning Or enD; it is alWays IN the miDDle, between tHings, interbeing, intermeZzO. ThE tree is fiLiatioN, but the rhizome is alliance, uniquelY alliance. The tree imposes the verb \"to be\" but the fabric of the rhizome is the conJUNction, \"AnD ... and ...and...\"THis conJunction carriEs enouGh force to shaKe and uproot the verb \"to be.\" Where are You goIng? Where are you coMing from? What are you heading for? These are totally useless questions.\n\n- Mille Plateaux, Gilles Deleuze & Felix Guattari\n\n\"Hyperconnect the world\""
    }
}

@pytest.fixture
def generate_genesis_data_path(tmpdir_factory) -> str:
    genesis_data_dumped = json.dumps(genesis_data)

    genesis_data_path: str = str(tmpdir_factory.mktemp("tempdir").join("init_genesis_test.json"))
    with open(genesis_data_path, "w") as f:
        f.write(genesis_data_dumped)

    MOCK_CHANNEL_CONFIG[channel_name]["genesis_data_path"] = genesis_data_path

    assert "genesis_data_path" in MOCK_CHANNEL_CONFIG[channel_name]
    return genesis_data_path


@pytest.fixture
def blockchain_store_path(tmpdir) -> str:
    db_path = tmpdir.mkdir(store_id)

    return db_path


@pytest.fixture
def blockchain(monkeypatch, generate_genesis_data_path, blockchain_store_path) -> BlockChain:
    monkeypatch.setattr(conf, "DEFAULT_STORAGE_PATH", value=blockchain_store_path)
    monkeypatch.setattr(conf, "CHANNEL_OPTION", value=MOCK_CHANNEL_CONFIG)
    block_chain = BlockChain(channel_name=channel_name, store_id=store_id)

    print(block_chain)
    print(dir(block_chain))

    return block_chain


# @pytest.mark.skip(reason="passed")
class TestBlockchainBasic:
    @pytest.mark.parametrize("nid, expected", [
        ("test_nid", None),
        (None, None)
    ])
    def test_put_nid(self, blockchain, nid, expected):
        """TODO: Why use this method?...
        It always returns None!"""
        result = blockchain.put_nid(nid)

        assert expected == result

    def test_rebuild_transaction_count(self, blockchain):
        pass
        blockchain.rebuild_transaction_count()


@pytest.mark.skip(reason="Too many dependencies!!!")
class TestBlockChainGenesis:
    def test_generate_genesis_block(self, blockchain: BlockChain):
        reps: List[ExternalAddress] = []
        blockchain.generate_genesis_block(reps)


# @pytest.mark.skip(reason="passed")
class TestBlockChainStore:
    def store_key(self, tx_hash_key, blockchain):
        store: KeyValueStore = blockchain.get_blockchain_store()
        store_key = tx_hash_key.encode(encoding=conf.HASH_KEY_ENCODING)
        store_value = json.dumps(genesis_data["transaction_data"]).encode()
        store.put(store_key, store_value)

        return blockchain

    def test_close_blockchain_store(self, blockchain):
        assert blockchain.get_blockchain_store()
        blockchain.close_blockchain_store()
        assert not blockchain.get_blockchain_store()

    def test_find_tx_info(self, blockchain):
        tx_hash_key: str = Hash32.new().hex()
        self.store_key(tx_hash_key, blockchain)
        tx_info_json = blockchain.find_tx_info(tx_hash_key)
        print("tx_info_json", tx_info_json)

        assert tx_info_json

    @pytest.mark.skip
    def test_find_tx_by_key(self, blockchain):
        tx_hash_key: str = Hash32.new().hex()
        self.store_key(tx_hash_key, blockchain)

        blockchain.find_tx_by_key(tx_hash_key)

