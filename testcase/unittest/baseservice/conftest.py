import pytest

from loopchain.baseservice.timer_service import TimerService
from loopchain.blockchain.transactions import Transaction


@pytest.fixture
def timer_service():
    ts = TimerService()
    ts.start()

    yield ts

    ts.stop()


@pytest.fixture
def tx(mocker) -> Transaction:
    tx = mocker.MagicMock(spec=Transaction)
    tx.version = "0x3"
    tx.raw_data = {
        "key": "value"
    }

    return tx
