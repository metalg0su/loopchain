import json
import os

import pytest

from testcase.integration.configure.config_generator import (
    Account, ChannelConfigKey, ChannelConfig, PeerConfigKey, PeerConfig, ChannelManageData, GenesisData, ConfigInterface
)
from testcase.integration.configure.exceptions import (
    NotEnoughAccounts, EmptyAccounts, NotFoundChannelConfig, DuplicatedChannelConfig
)


class _Params:
    CHANNEL_NAME_LIST = [
        ["test_one_channel"],
        ["ICON_DEX", "DEX_ICON"],
        ["ICON_DEX", "NOCI", "THIRD_cHaNnEl"],
    ]
    CHANNEL_PEER_COUNT = [
        (2, 7), (1, 1), (4, 9)
    ]
    NUM_LIST = [1, 4, 8]
    ACCOUNT_COUNT = [4, 9, 20]


class TestAccount:
    def test_has_valid_name(self, account_factory):
        key_name = "test_key"

        account = account_factory(name=key_name)
        assert account.name == key_name

    def test_has_valid_address(self, account_factory):
        """Check that wallet generator has no issues in making accounts."""
        account = account_factory()

        assert account.address.startswith("hx")

    def test_has_correct_balance(self, account_factory):
        show_me_the_money = "0x99999999999999999999999999999999"
        account: Account = account_factory(balance=show_me_the_money)

        assert account.balance == show_me_the_money

    def test_has_valid_key_path(self, account_factory):
        account = account_factory()

        assert os.path.exists(account.path)

    def test_has_valid_password(self, account_factory):
        password = "password"

        account = account_factory(password=password)
        assert account.password == password

    def test_get_config_has_valid_form(self, account_factory):
        account: Account = account_factory()
        result = account.get_config()

        assert isinstance(result, dict)
        for key in ["name", "address", "balance"]:
            assert key in result


class TestGenesisData:
    @pytest.fixture
    def genesis_data(self, tmp_path):
        return GenesisData(root_path=tmp_path)

    def test_add_account(self, genesis_data, account_factory):
        expected_list = []
        account_num = 3

        for i in range(account_num):
            expected_account = account_factory(name=f"atheist_{i}")
            expected_list.append(expected_account)

            genesis_data.add_account(expected_account)

        assert len(genesis_data.accounts) == account_num

        for i, expected_account in enumerate(expected_list):
            assert expected_account == genesis_data.accounts[i]

    def test_get_config_has_valid_form(self, genesis_data):
        result: dict = genesis_data.get_config()

        assert isinstance(result, dict)
        assert "transaction_data" in result
        assert "accounts" in result["transaction_data"]

    def test_write_file(self, genesis_data):
        assert not os.path.exists(genesis_data.path)

        genesis_data.write()
        assert os.path.exists(genesis_data.path)

        with open(genesis_data.path) as f:
            d = json.load(f)
            print(d)


class TestChannelConfig:

    def test_channel_has_valid_name(self, channel_config_factory):
        channel_name = "icon_test_dex"
        channel_config = channel_config_factory(channel_name)
        assert channel_config.name == channel_name

    def test_get_config_has_valid_form(self, channel_config_factory):
        channel_config = channel_config_factory()
        config_dict = channel_config.get_config()
        assert isinstance(config_dict, dict)

        print("CONFIG DICT: ", config_dict)

        channel_option_keys = [
            "block_versions",
            "hash_versions",
            "load_cert",
            "consensus_cert_use",
            "tx_cert_use",
            "key_load_type"
        ]
        for key in channel_option_keys:
            assert key in config_dict.keys()

    def test_set_genesis_data_path_for_leader_peer_config(self, channel_config_factory):
        channel_config: ChannelConfig = channel_config_factory()
        assert not channel_config.get_config().get("genesis_data_path")

        expected_path = "/my/path/is/just/created/for/test.json"
        channel_config.genesis_data_path = expected_path
        assert channel_config.get_config().get("genesis_data_path") == expected_path


class TestPeerConfig:
    @pytest.mark.parametrize("channel_name_list", _Params.CHANNEL_NAME_LIST)
    def test_add_multiple_channels(self, peer_config_factory, channel_name_list):
        peer_config: PeerConfig = peer_config_factory()
        assert not peer_config.channel_names

        for channel_name in channel_name_list:
            channel_config = ChannelConfig(name=channel_name)
            peer_config.add_channel(channel_config)

        assert len(peer_config.channel_names) == len(channel_name_list)

        for channel in peer_config._channel_config_list:
            assert isinstance(channel, ChannelConfig)

    def test_add_duplicated_channel_config(self, peer_config_factory):
        peer_config: PeerConfig = peer_config_factory()

        channel_config = ChannelConfig("icon_dex")
        peer_config.add_channel(channel_config)

        with pytest.raises(DuplicatedChannelConfig):
            peer_config.add_channel(channel_config)

    @pytest.mark.parametrize("order, expected_grpc_port, expected_rest_port", [
        (0, 7100, 9000), (1, 7200, 9100), (4, 7500, 9400)
    ])
    def test_has_expected_port(self, peer_config_factory, order, expected_grpc_port, expected_rest_port):
        peer_config: PeerConfig = peer_config_factory(order=order)

        assert peer_config.grpc_port == expected_grpc_port
        assert peer_config.rest_port == expected_rest_port

    @pytest.fixture
    def setup_peer_config(self, peer_config_factory) -> PeerConfig:
        peer_config: PeerConfig = peer_config_factory()
        channel_config = ChannelConfig()
        peer_config.add_channel(channel_config)

        peer_config.channel_manage_data_path = "dummy"

        assert not os.path.exists(peer_config.path)
        return peer_config

    def test_get_config_has_valid_form(self, setup_peer_config):
        """Check all keys in PeerConfig."""
        peer_config = setup_peer_config
        config_dict = peer_config.get_config()

        for key in PeerConfigKey.iterator():
            assert key in config_dict.keys()

    def test_write_config(self, setup_peer_config):
        peer_config = setup_peer_config
        peer_config.write()

        assert os.path.exists(peer_config.path)


class TestChannelManageData:
    @pytest.fixture
    def channel_manage_data(self, tmp_path):
        channel_manage_data = ChannelManageData(tmp_path)
        return channel_manage_data

    @pytest.mark.parametrize("number_of_peers", _Params.NUM_LIST)
    def test_add_peer_config(self, channel_manage_data, peer_config_factory, number_of_peers):
        assert not channel_manage_data.peer_config_list

        for peer_order in range(number_of_peers):
            peer_config = peer_config_factory(order=peer_order)
            channel_manage_data.add_peer(peer_config)

        assert len(channel_manage_data.peer_config_list) == number_of_peers

    @pytest.mark.parametrize("channel_name_list", _Params.CHANNEL_NAME_LIST)
    def test_write(self, channel_manage_data, channel_config_factory, peer_config_factory, channel_name_list):
        assert not os.path.exists(channel_manage_data.path)

        peer_config: PeerConfig = peer_config_factory()

        for channel_name in channel_name_list:
            channel_config = channel_config_factory(channel_name)
            peer_config.add_channel(channel_config)

        channel_manage_data.add_peer(peer_config)
        channel_manage_data.write()
        assert os.path.exists(channel_manage_data.path)

        with open(channel_manage_data.path) as f:
            json_config: dict = json.load(f)

        for channel_name in channel_name_list:
            assert channel_name in json_config

        assert len(json_config.keys()) == len(channel_name_list)


class TestConfigInterface:
    @pytest.mark.parametrize("account_count", _Params.ACCOUNT_COUNT)
    def test_generate_accounts(self, config_factory, account_count):
        config: ConfigInterface = config_factory()
        assert not config.wallet_list

        config._generate_accounts(how_many=account_count)
        assert len(config.wallet_list) == account_count

    @pytest.mark.parametrize("channel_count", _Params.NUM_LIST)
    def test_generate_channel_configs(self, config_factory, channel_count):
        config: ConfigInterface = config_factory()
        assert not config._channel_config_list

        config._generate_channel_configs(how_many=channel_count)
        assert config.channel_count == channel_count

    @pytest.mark.parametrize("peer_count", _Params.NUM_LIST)
    def test_generate_peer_configs(self, config_factory, peer_count):
        config: ConfigInterface = config_factory()
        assert not config.peer_config_list

        config._generate_accounts(how_many=peer_count)
        config._generate_channel_configs(how_many=2)
        config._generate_peer_configs(how_many=peer_count)
        assert config.peer_count == peer_count

    def test_generate_peer_configs_with_no_channel_configs(self, config_factory):
        config: ConfigInterface = config_factory()
        assert not config.peer_config_list

        gen_count = 4
        config._generate_accounts(how_many=gen_count)

        with pytest.raises(NotFoundChannelConfig, match="ChannelConfig"):
            config._generate_peer_configs(how_many=gen_count)

    def test_generate_peer_configs_with_not_enough_wallet_count(self, config_factory):
        wallet_count = 4
        peer_count = 7

        config: ConfigInterface = config_factory()
        config._generate_accounts(how_many=wallet_count)
        config._generate_channel_configs(how_many=2)

        with pytest.raises(NotEnoughAccounts):
            config._generate_peer_configs(how_many=peer_count)

    def test_generate_genesis_data(self, config_factory):
        additional_account_num = 4  # Will be created

        config: ConfigInterface = config_factory()
        assert not config.genesis_data.accounts

        config._generate_accounts(how_many=additional_account_num)
        config._generate_genesis_data()
        print("genesis_data: ", config.genesis_data)

        assert len(config.genesis_data.accounts) == additional_account_num

    def test_generate_genesis_data_with_no_wallets_generated(self, config_factory):
        config: ConfigInterface = config_factory()

        with pytest.raises(EmptyAccounts):
            config._generate_genesis_data()

    def test_generate_all(self, config_factory):
        channel_count = 2
        peer_count = 4

        config: ConfigInterface = config_factory()
        config.generate_config(channel_count=channel_count, peer_count=peer_count)

        assert config.peer_count == peer_count
        assert config.channel_count == channel_count

        assert len(config.genesis_data.accounts) == peer_count
        assert len(config.channel_manage_data.peer_config_list) == peer_count

    @pytest.fixture
    def setup_config_interface(self, config_factory) -> ConfigInterface:
        channel_count = 2
        peer_count = 4

        config: ConfigInterface = config_factory()
        config.generate_config(channel_count=channel_count, peer_count=peer_count)

        config.write()

        return config

    def test_write_and_only_first_peer_config_has_genesis_data_path(self, setup_config_interface):
        peer_config_list = setup_config_interface.peer_config_list
        for peer_order, peer_config in enumerate(peer_config_list):
            for channel_config in peer_config.channel_config_list:
                if peer_order == 0:
                    assert channel_config.is_leader_config
                else:
                    assert not channel_config.is_leader_config

    def test_write_and_channel_manage_data_is_not_null(self, setup_config_interface):
        peer_config_list = setup_config_interface.peer_config_list
        for peer_config in peer_config_list:
            with open(peer_config.path) as f:
                dict_config: dict = json.load(f)

            assert dict_config.get(PeerConfigKey.CHANNEL_MANAGE_DATA_PATH) == setup_config_interface.channel_manage_data.path
