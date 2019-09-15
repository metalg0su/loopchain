import pytest
from iconsdk.wallet.wallet import KeyWallet

from loopchain import configure as conf
from loopchain import utils
from loopchain.blockchain.blocks import v0_1a
from loopchain.blockchain.blocks import v0_3
from loopchain.blockchain.blocks.block_verifier import BlockVerifier
from loopchain.blockchain.transactions import TransactionVersioner
from testcase.integration import conftest
from testcase.integration.configure.config_generator import ConfigInterface
from testcase.integration.helper import get_block, send_tx, get_tx_by_hash


class TestSendTx:
    config: ConfigInterface = None
    tx_hash_by_channel = {}

    @pytest.fixture(scope="class", autouse=True)
    def run_and_wait(self, request, config_factory_class_scoped, run_loopchain):
        """Run loopchain nodes then wait until the test ends."""
        channel_count = int(request.config.getoption("--channel-count"))
        peer_count = int(request.config.getoption("--peer-count"))

        config: ConfigInterface = config_factory_class_scoped()
        config.generate_config(channel_count=channel_count, peer_count=peer_count)
        config.write()

        TestSendTx.config = config

        assert run_loopchain(config=config)

    @pytest.mark.parametrize("channel_name", conftest.channel_list)
    @pytest.mark.parametrize("port", conftest.port_list)
    def test_check_genesis_block(self, port, channel_name):
        """Check Genesis Block before test starts."""
        endpoint = utils.normalize_request_url(str(port), conf.ApiVersion.v3, channel_name)
        block = get_block(endpoint=endpoint, nth_block=0, block_version=v0_1a.version)

        genesis_tx = list(block.body.transactions.values())[0]
        genesis_data = TestSendTx.config.genesis_data.get_config()
        expected_data = genesis_data["transaction_data"]

        assert expected_data["accounts"] == genesis_tx.raw_data["accounts"]
        assert expected_data["message"] == genesis_tx.raw_data["message"]
        assert expected_data["nid"] == genesis_tx.raw_data["nid"]

    @pytest.mark.parametrize("channel_name", conftest.channel_list)
    def test_send_tx_icx(self, channel_name):
        """Test for `icx_sendTransaction`."""
        config = TestSendTx.config
        first_account = config.genesis_data.accounts[0]
        wallet: KeyWallet = first_account.wallet

        url = utils.normalize_request_url("9000", conf.ApiVersion.v3, channel_name)
        tx_hash = send_tx(endpoint=url, wallet=wallet)

        assert tx_hash.startswith("0x")
        TestSendTx.tx_hash_by_channel[channel_name] = tx_hash

    @pytest.mark.parametrize("channel_name", conftest.channel_list)
    @pytest.mark.parametrize("port", conftest.port_list)
    def test_tx_reached_consensus(self, port, channel_name):
        """Find tx_hash from given endpoint."""
        endpoint = utils.normalize_request_url(str(port), conf.ApiVersion.v3, channel_name)
        expected_tx = TestSendTx.tx_hash_by_channel[channel_name]
        tx_result = get_tx_by_hash(endpoint=endpoint, tx_hash=expected_tx)

        assert tx_result

    @pytest.mark.parametrize("channel_name", conftest.channel_list)
    @pytest.mark.parametrize("port", conftest.port_list)
    def test_verify_block_with_latest_block(self, port, channel_name):
        """Verify lastest block."""
        block_version = v0_3.version
        endpoint = utils.normalize_request_url("9000", conf.ApiVersion.v3, channel_name)

        block = get_block(endpoint=endpoint, block_version=block_version)

        block_verifier = BlockVerifier.new(block_version, TransactionVersioner())
        block_verifier.verify_transactions(block)

        assert True
