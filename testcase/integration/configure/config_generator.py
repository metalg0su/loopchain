import copy
import json
import os
from typing import List

from iconsdk.wallet.wallet import KeyWallet

from loopchain.blockchain.blocks import v0_1a
from loopchain.blockchain.blocks import v0_3
from testcase.integration.configure.exceptions import (
    NotEnoughAccounts, EmptyAccounts, NotFoundChannelConfig, DuplicatedChannelConfig, NotFoundChannelManageDataPath
)


class Account:
    def __init__(self, root_path, name, password="password", balance="0x2961fd42d71041700b90000"):
        self._name = name
        self._password = password
        self._balance = balance

        self._path = os.path.join(root_path, f"keystore_{self._name}.json")
        KeyWallet.create().store(self._path, self._password)
        self._wallet: KeyWallet = KeyWallet.load(self._path, self._password)

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._name

    @property
    def password(self):
        return self._password

    @property
    def balance(self):
        return self._balance

    @property
    def wallet(self) -> KeyWallet:
        return self._wallet

    @property
    def address(self):
        return self._wallet.address

    def get_config(self) -> dict:
        return {
            "name": self._name,
            "address": self.address,
            "balance": self._balance
        }

    def remove(self):
        os.remove(self._path)


class GenesisData:
    GENESIS_FILE_NAME = "genesis_test.json"

    def __init__(self, root_path, accounts=None, nid="0x3"):
        self._data = {}
        self._path = os.path.join(root_path, GenesisData.GENESIS_FILE_NAME)
        self._nid = nid
        self._accounts: List[Account] = accounts or []

    @property
    def path(self):
        return self._path

    @property
    def accounts(self) -> List[Account]:
        return self._accounts

    def add_account(self, account: Account):
        self._accounts.append(account)

    def get_config(self) -> dict:
        return {
            "transaction_data": {
                "nid": self._nid,
                "accounts": [account.get_config() for account in self._accounts],
                "message":
                    "A rHizomE has no beGInning Or enD; "
                    "it is alWays IN the miDDle, between tHings, interbeing, intermeZzO. ThE tree is fiLiatioN, "
                    "but the rhizome is alliance, uniquelY alliance. "
                    "The tree imposes the verb \"to be\" but the fabric of the rhizome is the conJUNction, "
                    "\"AnD ... and ...and...\""
                    "THis conJunction carriEs enouGh force to shaKe and uproot the verb \"to be.\" "
                    "Where are You goIng? Where are you coMing from? What are you heading for? "
                    "These are totally useless questions.\n\n- 'Mille Plateaux', Gilles Deleuze & Felix Guattari\n\n\""
                    "Hyperconnect the world\""
            }
        }

    def write(self):
        with open(self._path, "w") as f:
            json.dump(self.get_config(), f)


class ChannelConfigKey:
    BLOCK_VERSIONS = "block_versions"
    HASH_VERSIONS = "hash_versions"
    LOAD_CERT = "load_cert"
    CONSENSUS_CERT_USE = "consensus_cert_use"
    TX_CERT_USE = "tx_cert_use"
    KEY_LOAD_TYPE = "key_load_type"
    GENESIS_DATA_PATH = "genesis_data_path"


class ChannelConfig:
    def __init__(self, name="icon_dex"):
        self._name = name
        self._genesis_data_path = None

    @property
    def name(self):
        return self._name

    @property
    def genesis_data_path(self) -> str:
        return self._genesis_data_path

    @genesis_data_path.setter
    def genesis_data_path(self, path):
        self._genesis_data_path = path

    @property
    def is_leader_config(self):
        return bool(self._genesis_data_path)

    def get_config(self) -> dict:
        channel_config: dict = {
            ChannelConfigKey.BLOCK_VERSIONS: {
                v0_1a.version: 0,
                v0_3.version: 1
            },
            ChannelConfigKey.HASH_VERSIONS: {
                "genesis": 1,
                "0x2": 1,
                "0x3": 1
            },
            ChannelConfigKey.LOAD_CERT: False,
            ChannelConfigKey.CONSENSUS_CERT_USE: False,
            ChannelConfigKey.TX_CERT_USE: False,
            ChannelConfigKey.KEY_LOAD_TYPE: 0,
        }
        if self.is_leader_config:
            channel_config[ChannelConfigKey.GENESIS_DATA_PATH] = self._genesis_data_path

        return channel_config


class PeerConfigKey:
    LOOPCHAIN_DEFAULT_CHANNEL = "LOOPCHAIN_DEFAULT_CHANNEL"
    CHANNEL_OPTION = "CHANNEL_OPTION"
    PRIVATE_PATH = "PRIVATE_PATH"
    PRIVATE_PASSWORD = "PRIVATE_PASSWORD"
    RUN_ICON_IN_LAUNCHER = "RUN_ICON_IN_LAUNCHER"
    ALLOW_MAKE_EMPTY_BLOCK = "ALLOW_MAKE_EMPTY_BLOCK"
    PORT_PEER = "PORT_PEER"
    PEER_ORDER = "PEER_ORDER"
    PEER_ID = "PEER_ID"
    LOOPCHAIN_DEVELOP_LOG_LEVEL = "LOOPCHAIN_DEVELOP_LOG_LEVEL"
    DEFAULT_STORAGE_PATH = "DEFAULT_STORAGE_PATH"
    CHANNEL_MANAGE_DATA_PATH = "CHANNEL_MANAGE_DATA_PATH"

    @staticmethod
    def iterator():
        return [var for var in dir(PeerConfigKey) if var[0].isupper()]


class PeerConfig:
    STORAGE_FOLDER_NAME = ".storage"
    PORT_DIFF_GRPC_REST = 1900

    def __init__(self, root_path, peer_order: int, account: Account):
        self._account: Account = account
        self._peer_order: int = peer_order
        self._port_peer = 7100 + (self._peer_order * 100)
        self._channel_config_list: List[ChannelConfig] = []
        self._path = os.path.join(root_path, f"test_{self._peer_order}_conf.json")

        self._channel_manage_data_path = None
        self._default_storage_path = os.path.join(root_path, PeerConfig.STORAGE_FOLDER_NAME)

        self._run_icon_in_launcher = True
        self._allow_make_empty_block = True
        self._loopchain_develop_log_level = "INFO"

    @property
    def path(self):
        return self._path

    @property
    def grpc_port(self):
        return self._port_peer

    @property
    def rest_port(self):
        return self.grpc_port + PeerConfig.PORT_DIFF_GRPC_REST

    @property
    def peer_id(self):
        return self._account.address

    @property
    def channel_manage_data_path(self):
        return self._channel_manage_data_path

    @channel_manage_data_path.setter
    def channel_manage_data_path(self, path):
        self._channel_manage_data_path = path

    @property
    def channel_names(self) -> List[str]:
        if self._channel_config_list:
            channel_names = [channel_config.name for channel_config in self._channel_config_list]
        else:
            channel_names = []
        return channel_names

    @property
    def channel_config_list(self) -> List[ChannelConfig]:
        return self._channel_config_list

    def set_leader_peer(self, genesis_path):
        """Set genesis path to all ChannelConfigs in this peer."""
        for channel_config in self._channel_config_list:
            channel_config.genesis_data_path = genesis_path

    def _get_config_of_channels(self):
        return {channel_config.name: channel_config.get_config()
                for channel_config in self._channel_config_list}

    def get_config(self) -> dict:
        if not self._channel_manage_data_path:
            raise NotFoundChannelManageDataPath()

        return {
            PeerConfigKey.LOOPCHAIN_DEFAULT_CHANNEL: self.channel_names[0],
            PeerConfigKey.CHANNEL_OPTION: self._get_config_of_channels(),
            PeerConfigKey.PRIVATE_PATH: self._account.path,
            PeerConfigKey.PRIVATE_PASSWORD: self._account.password,
            PeerConfigKey.RUN_ICON_IN_LAUNCHER: self._run_icon_in_launcher,
            PeerConfigKey.ALLOW_MAKE_EMPTY_BLOCK: self._allow_make_empty_block,
            PeerConfigKey.PORT_PEER: self._port_peer,
            PeerConfigKey.PEER_ORDER: self._peer_order,
            PeerConfigKey.PEER_ID: self.peer_id,
            PeerConfigKey.LOOPCHAIN_DEVELOP_LOG_LEVEL: self._loopchain_develop_log_level,
            PeerConfigKey.DEFAULT_STORAGE_PATH: self._default_storage_path,
            PeerConfigKey.CHANNEL_MANAGE_DATA_PATH: self._channel_manage_data_path
        }

    def add_channel(self, channel_config: ChannelConfig):
        if channel_config.name not in self.channel_names:
            self._channel_config_list.append(channel_config)
        else:
            raise DuplicatedChannelConfig("Duplicated Channel Name in Peer!")

    def write(self):
        with open(self._path, "w") as f:
            json.dump(self.get_config(), f)


class ChannelManageData:
    CHANNEL_MANAGE_DATA_FILE_NAME = "channel_manage_data.json"

    def __init__(self, root_path, peer_config_list: List[PeerConfig] = None):
        self._path = os.path.join(root_path, ChannelManageData.CHANNEL_MANAGE_DATA_FILE_NAME)
        self._data = {}
        self._peer_config_list: List[PeerConfig] = peer_config_list or []

    @property
    def path(self):
        return self._path

    @property
    def peer_config_list(self):
        return self._peer_config_list

    @property
    def rest_port_list(self):
        return [peer_config.rest_port for peer_config in self.peer_config_list]

    @property
    def channel_name_list(self):
        return self.peer_config_list[0].channel_names

    def add_peer(self, peer_config: PeerConfig):
        self._peer_config_list.append(peer_config)

    def write(self):
        if not self._data:
            self._write()

        with open(self._path, "w") as f:
            json.dump(self._data, f)

    def _write(self):
        peers = []
        for peer_order, peer_config in enumerate(self._peer_config_list, start=1):
            peer_config.channel_manage_data_path = self._path
            each_peer = {
                "id": peer_config.peer_id,
                "peer_target": f"[local_ip]:{peer_config.grpc_port}",
                "order": peer_order
            }
            peers.append(each_peer)

        for channel_name in self.channel_name_list:
            self._data[channel_name] = {"peers": peers}


class ConfigInterface:
    def __init__(self, root_path):
        self._root_path = root_path

        self._account_list: List[Account] = []
        self._peer_config_list: List[PeerConfig] = []
        self._channel_config_list: List[ChannelConfig] = []
        self._genesis_data: GenesisData = GenesisData(self._root_path)
        self._channel_manage_data: ChannelManageData = ChannelManageData(self._root_path)

    @property
    def wallet_list(self):
        return self._account_list

    @property
    def channel_count(self):
        return len(self._channel_config_list)

    @property
    def peer_count(self):
        return len(self._peer_config_list)

    @property
    def channel_names(self) -> list:
        return self._channel_manage_data.channel_name_list

    @property
    def rest_port_list(self):
        return self._channel_manage_data.rest_port_list

    @property
    def genesis_data(self) -> GenesisData:
        return self._genesis_data

    @property
    def channel_manage_data(self):
        return self._channel_manage_data

    @property
    def peer_config_list(self):
        return self._peer_config_list

    def generate_config(self, channel_count: int, peer_count: int, wallet_count: int = None):
        self._clean_accounts()
        self._generate_accounts(how_many=wallet_count or peer_count)
        self._generate_channel_configs(how_many=channel_count)
        self._generate_peer_configs(how_many=peer_count)
        self._generate_channel_manage_data()
        self._generate_genesis_data()

    def write(self):
        self._genesis_data.write()
        self._channel_manage_data.write()

        for peer_config in self.peer_config_list:
            peer_config.write()

    def _clean_accounts(self):
        for account in self._account_list:
            account.remove()

    def _generate_accounts(self, how_many: int):
        self._account_list = []

        for num in range(how_many):
            account = Account(root_path=self._root_path, name=f"atheist_{num}")
            self._account_list.append(account)

    def _generate_channel_configs(self, how_many: int):
        self._channel_config_list = []

        for channel_num in range(how_many):
            channel_config: ChannelConfig = ChannelConfig(f"channel_{channel_num}")
            self._channel_config_list.append(channel_config)

    def _check_before_generate_peer_configs(self, how_many: int):
        if how_many > len(self._account_list):
            raise NotEnoughAccounts(f"Number of accounts must be greater than Peer count. Generate more Wallets!\n"
                                    f"> Account: {len(self._account_list)}\n"
                                    f"> Peer: {how_many}\n")

        if not self._channel_config_list:
            raise NotFoundChannelConfig("There's no ChannelConfig. Generate First!")

    def _generate_peer_configs(self, how_many: int):
        self._check_before_generate_peer_configs(how_many=how_many)
        self._peer_config_list = []

        for peer_order in range(how_many):
            wallet = self._account_list[peer_order]
            peer_config: PeerConfig = PeerConfig(root_path=self._root_path, peer_order=peer_order, account=wallet)

            channel_configs: List[ChannelConfig] = copy.deepcopy(self._channel_config_list)
            if peer_order == 0:
                for channel_config in channel_configs:
                    channel_config.genesis_data_path = self._genesis_data.path

            for channel_config in channel_configs:
                peer_config.add_channel(channel_config)

            self._peer_config_list.append(peer_config)

    def _generate_channel_manage_data(self):
        self._channel_manage_data = ChannelManageData(self._root_path, peer_config_list=self.peer_config_list)

    def _generate_genesis_data(self):
        if not self._account_list:
            raise EmptyAccounts("There's no accounts. Generate account first!")

        self._genesis_data = GenesisData(self._root_path, self._account_list)
