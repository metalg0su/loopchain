import functools

import pytest
from pytest_bdd import scenario

scenario = functools.partial(scenario, "loopchain.feature")


@pytest.mark.asyncio
@scenario("Send Tx")
async def test_send_tx():
    pass


@pytest.mark.asyncio
@scenario("Leader Rotation")
async def test_leader_rotation():
    pass


@pytest.mark.asyncio
@scenario("Generate block interval 2 sec")
async def test_block_interval():
    pass


@pytest.mark.asyncio
@scenario("Leader Complained")
async def test_leader_complained():
    pass
