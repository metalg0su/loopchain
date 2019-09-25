import pytest
from aiohttp import ClientSession
from loopchain import utils

from typing import List
from loopchain.baseservice import RestClient


@pytest.mark.asyncio
class TestRestClient:
    async def test_select_fastest_enpoint(self, mocker):
        endpoints: List[str] = ["https://test.loopchain"]
        rest_client = RestClient()

        with mocker.patch.object(utils, "normalize_request_url", ):
            await rest_client.init(endpoints)

    @pytest.mark.parametrize("request_uri, expected_endpoint", [
        ("https://127.0.0.1:9000/", "https://127.0.0.1:9000"),
    ])
    async def test_fetch_status_check_endpoint(rest_client: RestClient, request_uri, expected_endpoint, mocker, monkeypatch):
        # with mocker.patch.object(ClientSession, "get", return_value=mock_get_return) as mock_get:
        monkeypatch.setattr(ClientSession, "get", mock_get_return)
        session = ClientSession()
        result = await rest_client._fetch_status(session=session, request_uri=request_uri)

        assert result["target"] == expected_endpoint


