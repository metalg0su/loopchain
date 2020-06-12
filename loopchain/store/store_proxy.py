import json
import logging
import pickle
from typing import Union, Optional

from loopchain import configure_default as conf
from loopchain import utils
from loopchain.blockchain import Votes
from loopchain.blockchain.blocks import BlockSerializer
from loopchain.blockchain.blocks import BlockVersioner
from loopchain.blockchain.transactions import TransactionVersioner, TransactionSerializer
from loopchain.blockchain.types import Hash32
from .key_value_store import KeyValueStore


class DbKey:
    NID_KEY = b'NID_KEY'
    PRECOMMIT_BLOCK_KEY = b'PRECOMMIT_BLOCK'
    TRANSACTION_COUNT_KEY = b'TRANSACTION_COUNT'
    LAST_BLOCK_KEY = b'last_block_key'
    BLOCK_HEIGHT_KEY = b'block_height_key'

    # Additional information of the block is generated when the add_block phase of the consensus is reached.
    CONFIRM_INFO_KEY = b'confirm_info_key'
    PREPS_KEY = b'preps_key'
    INVOKE_RESULT_BLOCK_HEIGHT_KEY = b'invoke_result_block_height_key'


class DbStore:
    def __init__(self, store: KeyValueStore, tx_versioner: TransactionVersioner, block_versioner: BlockVersioner):
        self._db: KeyValueStore = store
        self._tx_versioner: TransactionVersioner = tx_versioner
        self._block_versioner: BlockVersioner = block_versioner

    def close(self):
        self._db.close()

    def get(self, key: bytes, *, default=None, **kwargs) -> bytes:
        return self._db.get(key, default=default, **kwargs)

    def put(self, key: bytes, value: bytes, *, sync=False, **kwargs):
        return self._db.put(key, value, sync=sync, **kwargs)

    def delete(self, key: bytes, *, sync=False, **kwargs):
        return self._db.delete(key, sync=sync, **kwargs)

    def destroy_store(self):
        self._db.destroy_store()

    def WriteBatch(self, sync=False) -> 'KeyValueStoreWriteBatch':
        return self._db.WriteBatch(sync=sync)

    def CancelableWriteBatch(self, sync=False) -> 'KeyValueStoreCancelableWriteBatch':
        return self._db.CancelableWriteBatch(sync=sync)

    def find_preps_by_roothash(self, roothash: Hash32) -> list:
        try:
            preps_dumped = bytes(self._db.get(DbKey.PREPS_KEY + roothash))
        except (KeyError, TypeError):
            return []
        else:
            return json.loads(preps_dumped)

    def find_block_by_key(self, key):
        try:
            block_bytes = self._db.get(key)
            block_dumped = json.loads(block_bytes)
            block_height = self._block_versioner.get_height(block_dumped)
            block_version = self._block_versioner.get_version(block_height)
            return BlockSerializer.new(block_version, self._tx_versioner).deserialize(block_dumped)
        except KeyError as e:
            utils.logger.debug(f"__find_block_by_key::KeyError block_hash({key}) error({e})")

        return None

    def find_block_by_hash(self, block_hash: Union[str, Hash32]):
        """find block in DB by block hash.

        :param block_hash: plain string,
        key 로 사용되기전에 함수내에서 encoding 되므로 미리 encoding 된 key를 parameter 로 사용해선 안된다.
        :return: None or Block
        """
        if isinstance(block_hash, Hash32):
            block_hash: str = block_hash.hex()
        return self.find_block_by_key(block_hash.encode(encoding='UTF-8'))

    def find_block_by_height(self, block_height: int):
        return self._db.get(
            DbKey.BLOCK_HEIGHT_KEY + block_height.to_bytes(conf.BLOCK_HEIGHT_BYTES_LEN, byteorder='big')
        )

    def find_confirm_info_by_hash(self, block_hash: Union[str, Hash32]) -> bytes:
        if isinstance(block_hash, Hash32):
            block_hash = block_hash.hex()

        hash_encoded = block_hash.encode('UTF-8')
        return self._db.get(DbKey.CONFIRM_INFO_KEY + hash_encoded)

    # ----- Find Transactions
    def find_tx_info(self, tx_hash_key: Union[str, Hash32]) -> Optional[dict]:
        try:
            tx_info = self._find_tx_info(tx_hash_key)
            tx_info_json = json.loads(tx_info, encoding=conf.PEER_DATA_ENCODING)
        except UnicodeDecodeError as e:
            logging.warning("blockchain::find_tx_info: UnicodeDecodeError: " + str(e))
            return None

        return tx_info_json

    def _find_tx_info(self, tx_hash_key):
        if isinstance(tx_hash_key, Hash32):
            tx_hash_key = tx_hash_key.hex()

        return self._db.get(
            tx_hash_key.encode(encoding=conf.HASH_KEY_ENCODING)
        )

    def find_tx_by_key(self, tx_hash_key):
        """find tx by hash

        :param tx_hash_key: tx hash
        :return None: There is no tx by hash or transaction object.
        """

        try:
            tx_info_json = self.find_tx_info(tx_hash_key)
        except KeyError as e:
            return None
        if tx_info_json is None:
            logging.warning(f"tx not found. tx_hash ({tx_hash_key})")
            return None

        tx_data = tx_info_json["transaction"]
        tx_version, tx_type = self._tx_versioner.get_version(tx_data)
        tx_serializer = TransactionSerializer.new(tx_version, tx_type, self._tx_versioner)
        return tx_serializer.from_(tx_data)

    def find_nid(self):
        try:
            nid = self._db.get(DbKey.NID_KEY)
            nid = nid.decode(conf.HASH_KEY_ENCODING)
        except KeyError as e:
            logging.debug(f"get_nid::There is no NID.")
            nid = None

        return nid

    def put_nid(self, nid: str):
        """
        write nid to DB
        :param nid: Network ID
        :return:
        """
        if nid is None:
            return

        results = self._db.put(DbKey.NID_KEY, nid.encode(encoding=conf.HASH_KEY_ENCODING))
        utils.logger.spam(f"result of to write to db ({results})")

        return results

    def write_preps(self, roothash: Hash32, preps: list, batch: 'KeyValueStoreWriteBatch' = None):
        write_target = batch or self

        write_target.put(
            DbKey.PREPS_KEY + roothash,
            json.dumps(preps).encode(encoding=conf.PEER_DATA_ENCODING)
        )

    def write_tx(self, block, receipts, tx_queue, batch=None):
        """save additional information of transactions to efficient searching and support user APIs.

        :param block:
        :param tx_queue:
        :param receipts: invoke result of transaction
        :param batch:
        :return:
        """
        write_target = batch or self

        # loop all tx in block
        logging.debug("try add all tx in block to block db, block hash: " + block.header.hash.hex())

        for index, tx in enumerate(block.body.transactions.values()):
            tx_hash = tx.hash.hex()
            receipt = receipts[tx_hash]

            tx_serializer = TransactionSerializer.new(tx.version, tx.type(), self._tx_versioner)
            tx_info = {
                'block_hash': block.header.hash.hex(),
                'block_height': block.header.height,
                'tx_index': hex(index),
                'transaction': tx_serializer.to_db_data(tx),
                'result': receipt
            }

            write_target.put(
                tx_hash.encode(encoding=conf.HASH_KEY_ENCODING),
                json.dumps(tx_info).encode(encoding=conf.PEER_DATA_ENCODING))

            tx_queue.pop(tx_hash, None)

            if block.header.height > 0:
                self._write_tx_by_address(tx, batch)

        # save_invoke_result_block_height
        bit_length = block.header.height.bit_length()
        byte_length = (bit_length + 7) // 8
        block_height_bytes = block.header.height.to_bytes(byte_length, byteorder='big')
        write_target.put(
            DbKey.INVOKE_RESULT_BLOCK_HEIGHT_KEY,
            block_height_bytes
        )

    def _get_tx_list_key(self, address, index):
        return conf.TX_LIST_ADDRESS_PREFIX + (address + str(index)).encode(encoding=conf.HASH_KEY_ENCODING)

    def get_tx_list_by_address(self, address, index=0):
        list_key = self._get_tx_list_key(address, index)

        try:
            tx_list = pickle.loads(self.get(list_key))
            next_index = tx_list[-1]
        except KeyError:
            tx_list = [0]  # 0 means there is no more list after this.
            next_index = 0

        return tx_list, next_index

    def _add_tx_to_list_by_address(self, address, tx_hash, batch=None):
        write_target = batch or self
        current_list, current_index = self.get_tx_list_by_address(address, 0)

        if len(current_list) > conf.MAX_TX_LIST_SIZE_BY_ADDRESS:
            new_index = current_index + 1
            new_list_key = self._get_tx_list_key(address, new_index)
            self.put(new_list_key, pickle.dumps(current_list))
            current_list = [new_index]

        current_list.insert(0, tx_hash)
        list_key = self._get_tx_list_key(address, 0)
        write_target.put(list_key, pickle.dumps(current_list))

        return True

    def _write_tx_by_address(self, tx: 'Transaction', batch):
        if tx.type() == "base":
            return
        address = tx.from_address.hex_hx()
        return self._add_tx_to_list_by_address(address, tx.hash.hex(), batch)

    def write_block_data(self, block: 'Block', confirm_info, receipts, next_prep,
                         next_total_tx, tx_queue, last_block=None):
        # a condition for the exception case of genesis block.
        if block.header.height > 0:
            next_total_tx += len(block.body.transactions)

        bit_length = next_total_tx.bit_length()
        byte_length = (bit_length + 7) // 8
        next_total_tx_bytes = next_total_tx.to_bytes(byte_length, byteorder='big')

        block_serializer = BlockSerializer.new(block.header.version, self._tx_versioner)
        block_serialized = json.dumps(block_serializer.serialize(block))
        block_hash_encoded = block.header.hash.hex().encode(encoding='UTF-8')

        batch = self.WriteBatch()
        batch.put(block_hash_encoded, block_serialized.encode("utf-8"))
        batch.put(DbKey.LAST_BLOCK_KEY, block_hash_encoded)
        batch.put(DbKey.TRANSACTION_COUNT_KEY, next_total_tx_bytes)
        batch.put(
            DbKey.BLOCK_HEIGHT_KEY +
            block.header.height.to_bytes(conf.BLOCK_HEIGHT_BYTES_LEN, byteorder='big'),
            block_hash_encoded)

        if receipts:
            self.write_tx(block, receipts, tx_queue, batch)

        if next_prep:
            utils.logger.spam(
                f"store next_prep in __write_block_data\nprep_hash({next_prep['rootHash']})"
                f"\npreps({next_prep['preps']})")
            self.write_preps(Hash32.fromhex(next_prep['rootHash'], ignore_prefix=True), next_prep['preps'], batch)

        if confirm_info:
            if isinstance(confirm_info, list):
                votes_class = Votes.get_block_votes_class(block.header.version)
                confirm_info = json.dumps(votes_class.serialize_votes(confirm_info))
            if isinstance(confirm_info, str):
                confirm_info = confirm_info.encode('utf-8')
            batch.put(
                DbKey.CONFIRM_INFO_KEY + block_hash_encoded,
                confirm_info
            )
        else:
            utils.logger.debug(f"This block({block.header.hash}) is trying to add without confirm_info.")

        if last_block and last_block.header.prev_hash:
            # Delete confirm info to avoid data duplication.
            block_hash_encoded = last_block.header.prev_hash.hex().encode("utf-8")
            block_confirm_info_key = DbKey.CONFIRM_INFO_KEY + block_hash_encoded
            batch.delete(block_confirm_info_key)

        batch.write()

        return next_total_tx
