import pytest

from loopchain.utils import normalize_request_url
from loopchain.configure_default import ApiVersion
from loopchain import configure_default as conf


class TestNormalizeRequestURL:
    def test_get_local_endpoint_if_url_input_not_exist(self):
        url_input = None
        expected_result = f"http://127.0.0.1:{conf.PORT_PEER_FOR_REST}/api/v3/{conf.LOOPCHAIN_DEFAULT_CHANNEL}"

        assert expected_result == normalize_request_url(url_input)

    @pytest.mark.parametrize("url", [
        "http://test.com",
        "http://test.com:80",
        "https://test.com",
        "https://test.com:443",
        "https://test.com:9000",
        "http://127.0.0.1:9000",
        "https://127.0.0.1:9000",
        "test.com",
    ])
    def test_various_url_input(self, url):
        res = normalize_request_url(url)

        print(url, "====", res)
        assert "http://" in res or "https://" in res

    @pytest.mark.parametrize("version", [ApiVersion.node, ApiVersion.v1, ApiVersion.v2, ApiVersion.v3])
    @pytest.mark.parametrize("channel", ["aa", "a1", "1a"])
    def test_various_input(self, url, version, channel):
        res = normalize_request_url(url, version=version, channel=channel)

        print(url, version, channel, "====", res)
        assert "http://" in res or "https://" in res

    def test_scheme_http(self, url):
        res = normalize_request_url(url)

        assert res == "http://test.com:443/api/v3/icon_dex"

    @pytest.mark.parametrize("url", [
        "https://test.com",
        "test.com"
    ])
    def test_scheme_http(self, url):
        res = normalize_request_url(url)
        print()
        print(res)

    @pytest.mark.parametrize("version", [ApiVersion.node, ApiVersion.v1, ApiVersion.v2, ApiVersion.v3])
    def test_with_channel(self, version):
        url = "https://test.com"
        res = normalize_request_url(url, version)

        assert res == f"https://test.com:443/api/{version.name}/icon_dex"

    def test_with_channel(self):
        pass

    def test_all(self):
        url = "test.com"
        version = ApiVersion.v1
