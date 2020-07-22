import json
import time
from pathlib import Path

import pytest
from iconsdk.wallet.wallet import KeyWallet

from loopchain.crypto.cert_serializers import DerSerializer
from .helper import LoopchainRunner, FilePath


@pytest.fixture(scope="class")
def config_path() -> FilePath:
    import loopchain
    root_path = Path(loopchain.__file__).parents[1]
    return FilePath(root_path)


@pytest.fixture
def wallet(config_path) -> KeyWallet:
    configs = list(config_path.peer_configs)
    first_node_config: str = configs[0]
    with open(first_node_config) as f:
        config = json.load(f)

    loopchain_root = Path(__file__).parents[3]
    key_path = loopchain_root / config["PRIVATE_PATH"]
    key_path: Path = key_path.resolve()

    password = config["PRIVATE_PASSWORD"]

    prikey = DerSerializer.deserialize_private_key_file(str(key_path), password.encode())
    return KeyWallet.load(prikey)


@pytest.fixture(scope="class")
def loopchain_runner() -> LoopchainRunner:
    _loopchain = LoopchainRunner()

    yield _loopchain

    _loopchain.kill()
    time.sleep(3)  # Give CoolDown for additional tests
