import pytest

from .runner import ensure_run


class TestConfig:
    def test_config_path_api(self, config_path):
        assert config_path.channel_manage_data

        peer_configs = config_path.peer_configs
        for i in peer_configs:
            print(i)

    def test_wallet(self, wallet):
        print(wallet.address)


class TestRunLoopchain:
    @pytest.mark.asyncio
    async def test_ensure_run(self, loopchain_runner, config_path):
        assert await loopchain_runner.run(config_path)
        assert await ensure_run([9000, 9100, 9200, 9300])
