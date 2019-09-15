import json
import subprocess
import time
from typing import Optional
from typing import Union

import requests
from iconsdk.builder.transaction_builder import TransactionBuilder
from iconsdk.exception import JSONRPCException, DataTypeException
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.signed_transaction import SignedTransaction
from iconsdk.wallet.wallet import KeyWallet

from loopchain.blockchain.blocks import Block, BlockSerializer
from loopchain.blockchain.blocks import v0_3
from loopchain.blockchain.transactions import TransactionVersioner
from testcase.integration.configure.config_generator import PeerConfig

MAX_REQUEST_RETRY = 60


def _get_payload(block_height):
    payload = {
        "jsonrpc": "2.0",
        "method": "icx_getBlock",
        "id": 1234,
    }
    if not block_height == "latest":
        payload["params"] = {
            "height": hex(block_height)
        }
    print("REQ payload: ", payload)

    return payload


def _convert_raw_block(raw_block: dict, block_version: str) -> Block:
    block_serializer = BlockSerializer.new(block_version, TransactionVersioner())
    block: Block = block_serializer.deserialize(block_dumped=raw_block)
    print("RES block: ", block)

    return block


def _request(endpoint, payload) -> dict:
    print("Req endpoint: ", endpoint)
    response = requests.post(endpoint, json=payload)
    print("RES: ", response)

    response_as_dict: dict = response.json()
    assert "error" not in response_as_dict

    return response_as_dict


def get_block(endpoint, nth_block: Union[int, str] = "latest", block_version=v0_3.version) -> Block:
    payload = _get_payload(block_height=nth_block)
    raw_block = _request(endpoint, payload)

    raw_block: dict = raw_block["result"]
    if nth_block == 0:
        raw_block["commit_state"] = None

    block = _convert_raw_block(raw_block, block_version)
    return block


def _request_service_available(endpoint, timeout=1) -> Optional[dict]:
    exc = None
    try:
        response = requests.get(endpoint, timeout=timeout)
        response = response.json()
    except (requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
            json.JSONDecodeError) as e:
        exc = e
        response = None

    print(f"\n\n\n\nTEST>> {exc or response}")
    return response


def ensure_run(endpoint: str, max_retry=MAX_REQUEST_RETRY):
    interval_sleep_sec = 1
    retry_count = 0
    is_success = False

    while not is_success:
        if retry_count >= max_retry:
            break
        time.sleep(interval_sleep_sec)
        retry_count += 1

        response = _request_service_available(endpoint, timeout=interval_sleep_sec)
        if not response:
            continue

        if response["service_available"]:
            is_success = True

    print("is_success?: ", is_success)
    return is_success


def send_tx(endpoint, wallet: KeyWallet, from_addr=None, to_addr=None) -> str:
    print("REQ endpoint: ", endpoint)
    icon_service = IconService(HTTPProvider(endpoint))

    # Build transaction and sign it with wallet
    transaction = TransactionBuilder()\
        .from_(from_addr or wallet.address)\
        .to(to_addr or wallet.address)\
        .value(10)\
        .step_limit(100000000)\
        .nid(3)\
        .nonce(100)\
        .build()
    signed_transaction = SignedTransaction(transaction, wallet)
    tx_hash = icon_service.send_transaction(signed_transaction=signed_transaction)
    print("Tx hash: ", tx_hash)

    return tx_hash


def get_tx_by_hash(endpoint, tx_hash, max_retry=MAX_REQUEST_RETRY):
    icon_service = IconService(HTTPProvider(endpoint))

    is_consensus_completed = False

    interval_sleep_sec = 1
    retry_count = 0
    tx_result = None

    while not is_consensus_completed:
        if retry_count >= max_retry:
            raise RuntimeError(f"Consensus failed!"
                               f"tx hash: {tx_hash}"
                               f"endpoint: {endpoint}")
        try:
            tx_result = icon_service.get_transaction(tx_hash)
        except (JSONRPCException, DataTypeException) as e:
            print(">> Exc: ", e)
            time.sleep(interval_sleep_sec)
            retry_count += 1
        else:
            assert tx_result
            is_consensus_completed = True

    return tx_result


def loopchain(peer_config: PeerConfig) -> subprocess.Popen:
    """Run single loopchain node by given args."""
    cmd = ["loop", "-d", "-o", peer_config.path]
    print("Run loopchain cmd: ", cmd)
    proc = subprocess.Popen(cmd)

    endpoint = f"http://localhost:{peer_config.rest_port}/api/v1/avail/peer"
    assert ensure_run(endpoint=endpoint)

    return proc
