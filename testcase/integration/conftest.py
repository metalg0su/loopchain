import functools
import subprocess
import time
from typing import List

import pytest

from testcase.integration.configure.config_generator import (
    Account, ChannelConfig, PeerConfig, GenesisData, ConfigInterface
)
from testcase.integration.helper import loopchain

port_list = []
channel_list = []


def pytest_addoption(parser):
    """Set args for tests."""
    parser.addoption("--peer-count", action="store", default=4, help="Number of peer to be tested")
    parser.addoption("--channel-count", action="store", default=2, help="Number of channel to set in each peer.\n"
                                                                        "Each will be named as 'channel_[num]'.\n"
                                                                        "Use this option to test multi-channel.\n")


def pytest_configure(config):
    """Make port and channel list to parameterize in intergration tests."""
    global port_list
    peer_count = int(config.getoption("--peer-count"))
    for peer_order in range(peer_count):
        port = 9000 + (100 * peer_order)
        port_list.append(port)

    global channel_list
    channel_count = int(config.getoption("--channel-count"))
    for channel_num in range(channel_count):
        channel_list.append(f"channel_{channel_num}")


@pytest.fixture
def account_factory(tmp_path):
    def _account(root_path, name="test_key", password="password", balance="0x9999999"):
        account = Account(root_path=root_path, name=name, password=password, balance=balance)

        return account

    return functools.partial(_account, root_path=tmp_path)


@pytest.fixture
def channel_config_factory():
    def _channel_config(channel_name="icon_dex"):
        return ChannelConfig(channel_name)

    return _channel_config


@pytest.fixture
def peer_config_factory(tmp_path, account_factory):
    def _peer_config(root_path, account, order: int = 0):
        peer_config = PeerConfig(root_path,
                                 peer_order=order,
                                 account=account)

        return peer_config

    return functools.partial(_peer_config, root_path=tmp_path, account=account_factory())


@pytest.fixture
def genesis_data_factory(tmp_path):
    def _genesis_data(root_path):
        genesis_data = GenesisData(root_path=root_path)

        accounts = [
            Account(root_path=root_path, name="god", balance="0x2961ffa20dd47f5c4700000"),
            Account(root_path=root_path, name="treasury", balance="0x0")
        ]
        for account in accounts:
            genesis_data.add_account(account)

        return genesis_data

    return functools.partial(_genesis_data, root_path=tmp_path)


@pytest.fixture
def config_factory(tmp_path):
    def _config_factory(root_path) -> ConfigInterface:
        return ConfigInterface(root_path=root_path)

    return functools.partial(_config_factory, root_path=tmp_path)


@pytest.fixture(scope="class")
def config_factory_class_scoped(tmp_path_factory):
    def _config_factory(root_path) -> ConfigInterface:
        return ConfigInterface(root_path=root_path)

    return functools.partial(_config_factory, root_path=tmp_path_factory.getbasetemp())


@pytest.fixture(scope="class")
def run_loopchain():
    proc_list: List[subprocess.Popen] = []

    def _loopchain(config: ConfigInterface):
        nonlocal proc_list

        for k, peer_config in enumerate(config.peer_config_list, start=1):
            print(f"==========PEER_{k}/{config.peer_count} READY TO START ==========")
            loopchain_proc = loopchain(peer_config=peer_config)
            proc_list.append(loopchain_proc)
        print(f"==========ALL GREEN ==========")
        time.sleep(3)  # WarmUp before test starts

        return proc_list

    yield _loopchain

    # tear down
    for proc in proc_list:
        proc.terminate()

    time.sleep(1)  # Give CoolDown for additional tests
