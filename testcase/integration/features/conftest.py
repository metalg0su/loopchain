import time
from pathlib import Path
from typing import Dict

import pytest
from iconsdk.builder.transaction_builder import MessageTransactionBuilder
from iconsdk.exception import IconServiceBaseException
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.signed_transaction import SignedTransaction
from pytest_bdd import given, when, then, parsers

from loopchain.blockchain.types import VarBytes
from ..helper.config import FilePath
from ..helper.runner import LoopchainRunner, get_status


def _create_tx(wallet) -> SignedTransaction:
    byte_msg = f"test_msg on 1".encode("utf-8")
    msg = VarBytes(byte_msg).hex_0x()

    # Address
    from_to_address = wallet.get_address()

    # Build transaction and sign it with wallet
    transaction_data = {
        "from": from_to_address,
        "to": from_to_address,
        "step_limit": 100000000,
        "nid": 3,
        "nonce": 100,
        "data": msg,
    }
    transaction = MessageTransactionBuilder().from_dict(transaction_data).build()
    return SignedTransaction(transaction, wallet)


class ContextKey:
    tx = "tx"
    tx_hash = "tx_hash"
    blocks = "block"
    timestamp = "timestamp"


@pytest.fixture
def context() -> dict:
    return {
        ContextKey.blocks: {}
    }


# Steps ------
@pytest.mark.asyncio
@given("The network is running")
async def run_loopchain(loopchain_runner, config_path):
    assert await loopchain_runner.run(config_path)


@given("DB does not exist")
def remove_db_storage(config_path):
    def _remove_dir(directory):
        super_dir = Path(directory)
        for sub_dir in super_dir.iterdir():
            if sub_dir.is_dir():
                _remove_dir(sub_dir)
            else:
                sub_dir.unlink()
        directory.rmdir()

    db_path: Path = config_path.db_path
    if db_path.exists():
        _remove_dir(db_path)

    assert not db_path.exists()


@given(parsers.re(r"I kill Node(?P<order>\d+)"), converters={"order": int})
def kill_node(loopchain_runner: "LoopchainRunner", order: int):
    print("Try to kill node: ", order, type(order), bool(order))
    loopchain_runner.kill(order)
    print("Try to kill node-suc: ", order)


@given("I have enough balance")
def check_balance(wallet):
    url = "http://localhost:9000/api/v3"
    icon_service = IconService(HTTPProvider(url))
    balance = icon_service.get_balance(wallet.address)
    assert balance > 10000


def _wait_block_height(height):
    url = "http://localhost:9000/api/v3"
    icon_service = IconService(HTTPProvider(url))

    max_retry = 60
    count = 0
    while count < max_retry:
        print("count curr/max: ", count, max_retry)
        try:
            time.sleep(1)
            block: dict = icon_service.get_block(height)
        except IconServiceBaseException as e:
            print(e)
            count += 1
        else:
            print("Wait complete: ", block)
            actual_height = block.get("height")
            assert height == actual_height
            break


@given(parsers.re(r"I wait until block (?P<height>\d+) is available"), converters={"height": int})
def wait_block_height(height):
    _wait_block_height(height)


@when(parsers.re(r"I wait until block (?P<height>\d+) is available"), converters={"height": int})
def wait_block_height_(height):
    _wait_block_height(height)


@given("I create tx")
def create_tx(wallet, context):
    context[ContextKey.tx] = _create_tx(wallet)


@when("I send tx")
def send_tx(context):
    url = "http://localhost:9000/api/v3"
    icon_service = IconService(HTTPProvider(url))
    tx = context[ContextKey.tx]

    tx_hash = icon_service.send_transaction(tx)
    context[ContextKey.tx_hash] = tx_hash


@when(parsers.re(r"I get block \[(?P<start>\d+)-(?P<end>\d+)\]"), converters={"start": int, "end": int})
async def store_blocks(loopchain_runner: LoopchainRunner, context, start, end):
    node_count = len(loopchain_runner.proc_list)
    channel_name = "icon_dex"
    url: str = ""

    for idx in range(node_count):
        port = 9000 + (100 * idx)
        endpoint = f"http://localhost:{port}/api/v3"
        is_available = await get_status(endpoint=endpoint, channel_name=channel_name, timeout=0.1)

        if is_available:
            break

    if not url:
        pytest.fail(msg="No available Nodes!")

    icon_service = IconService(HTTPProvider(url))

    for height in range(start, end+1):
        block: dict = icon_service.get_block(height)
        context[ContextKey.blocks][height] = block


@when("I store timestamp")
def store_timestamp(context):
    context[ContextKey.timestamp] = time.time()


@then("Tx Receipt should be valid")
def check_tx_hash_form(context):
    tx_hash = context[ContextKey.tx_hash]
    print("Check tx hash: ", tx_hash, type(tx_hash))

    assert tx_hash.startswith("0x")
    # TODO


@then("I can retrieve tx info")
def get_tx_info(context):
    tx_hash = context[ContextKey.tx_hash]
    url = "http://localhost:9000/api/v3"
    icon_service = IconService(HTTPProvider(url))

    max_retry = 60
    count = 0
    while count < max_retry:
        print("COUNT: ", count)
        try:
            time.sleep(1)
            tx = icon_service.get_transaction(tx_hash)
            print("RESULT!!!!: ", tx, type(tx))
            if tx.get("txHash"):
                break
            else:
                count += 1
        except IconServiceBaseException as e:
            print(e)
            count += 1
    else:
        pytest.fail(msg="Cannot find Tx hash!")


@then(
    parsers.re(r"block \[(?P<start>\d+)-(?P<end>\d+)\] (?P<attr>.*) should be same"),
    converters={"start": int, "end": int, "attr": str}
)
def assert_true_block_attr(context, start, end, attr):
    blocks: Dict[int, dict] = context[ContextKey.blocks]
    target_attrs = [block.get(attr) for height, block in blocks.items()
                    if start <= block["height"] <= end]

    print("True?: ", start, end, target_attrs)

    assert len(set(target_attrs)) == 1


@then(
    parsers.re(r"block \[(?P<start>\d+)-(?P<end>\d+)\] (?P<attr>.*) should be different"),
    converters={"start": int, "end": int, "attr": str}
)
def assert_false_block_attr(context, start, end, attr):
    blocks: Dict[int, dict] = context[ContextKey.blocks]
    target_attrs = [block.get(attr) for height, block in blocks.items()
                    if start <= block["height"] <= end]

    print("False?: ", start, end, target_attrs)

    assert not len(set(target_attrs)) == 1


@then(
    parsers.re(r"Time diff (?P<operator>(greater than|less than|equal)) (?P<sec>\d+\.\d+) sec"),
    converters={"operator": str, "sec": float}
)
def compare_timestamp(context, sec, operator):
    target_timestamp: float = context[ContextKey.timestamp]

    print("Stored: ", target_timestamp)
    print("Elapsed: ", sec)
    print("Operator: ", operator)

    diff = time.time() - target_timestamp
    if operator == "greater than":
        assert sec < diff
    elif operator == "less than":
        assert diff < sec
    elif operator == "equal":  # ...;
        assert diff == sec
    else:
        pytest.fail(msg=f"Unknown operator!: {operator}")


@then(parsers.re("Clear context: '(?P<attr>.*)'"), converters={"attr": str})
def clear_context(context, attr):
    key = getattr(ContextKey, attr)
    context[key] = {}


@when("w")
def wwww():
    print("When!")
    time.sleep(10)
    print("When-END!")


@then("t")
def tttt():
    print("Then!")
