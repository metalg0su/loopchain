import time
from pathlib import Path

import psutil
import pytest

from testcase.integration.helper.configg import FilePath
from .helper.executor import LoopchainRunner


@pytest.fixture(scope="class")
def config_path() -> FilePath:
    import loopchain
    root_path = Path(loopchain.__file__).parents[1]
    return FilePath(root_path)


@pytest.fixture(scope="class")
def loopchain_runner():
    _loopchain = LoopchainRunner()

    yield _loopchain

    # tear down
    print("Kill Peer Processes!", _loopchain.proc_list)
    for proc in _loopchain.proc_list:
        proc.send_signal(15)
        proc.wait()

    print("Kill Channel Processes!")
    for proc in psutil.process_iter(["name"]):
        if "python" in proc.info["name"]:
            info = proc.cmdline()[0]
            if "loopchain channel" in info:
                try:
                    proc.send_signal(2)
                except Exception:
                    pass
                finally:
                    print("Killed!: ", info)

    time.sleep(3)  # Give CoolDown for additional tests


@pytest.fixture(scope="class")
async def run_loopchain(loopchain_runner, config_path):
    assert await loopchain_runner.run(config_path)
