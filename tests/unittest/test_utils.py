#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2018 ICON Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Test Utils Util"""

import pytest

import loopchain.utils as util
from loopchain import configure as conf
from loopchain.utils import loggers

loggers.set_preset_type(loggers.PresetType.develop)
loggers.update_preset()


skip_test = pytest.mark.skip


class TestUtils:

    @skip_test(reason="Not Used?")
    def test_long_to_bytes(self):
        pass

    @skip_test(reason="Later")
    def test_exit_and_msg(self):
        msg = "Test"
        assert util.exit_and_msg(msg)

    @skip_test(reason="Not Used?")
    def test_load_user_score(self):
        pass

    @skip_test(reason="Later")
    def test_get_stub_to_server(self):
        pass

    @skip_test(reason="Later")
    def test_request_server_in_time(self):
        pass

    @skip_test(reason="Not Used?")
    def test_request_server_wait_response(self):
        pass

    @skip_test(reason="Feels bad.. Need further investigating")
    def test_normalize_request_url(self):
        pass

    @pytest.mark.parametrize("ip, dns, port, version, use_https, channel, expected",
                             [(None, None, None, None, False, None,
                               f"http://{conf.IP_LOCAL}:{conf.PORT_PEER_FOR_REST}/api/v{conf.ApiVersion.v3}/{conf.LOOPCHAIN_DEFAULT_CHANNEL}"),
                              ("127.0.0.1", None, 1234, conf.ApiVersion.v3, True, "icon_dex",
                               f"https://127.0.0.1:1234/api/v{conf.ApiVersion.v3}/icon_dex"),
                              ("255.255.255.255", "overwrite-as-dns.org", "1234", conf.ApiVersion.v3, False, "not_default_channeltest_",
                               f"http://overwrite-as-dns.org:443/api/v{conf.ApiVersion.v3}/not_default_channeltest_"),
                              ])
    def test_generate_url_from_params(self, ip, dns, port, version, use_https, channel, expected):
        """ Test for generate url from params
        Test supposed to 'ENABLE_MULTI_CHANNEL_REQUEST = True'
        """

        assert util.generate_url_from_params(
            ip=ip, dns=dns, port=port, version=version, use_https=use_https, channel=channel) == expected

    def test_get_private_ip(self):
        """Test for get_private_ip

        Need more test cases"""
        result = util.get_private_ip().split(".")
        assert len(result) == 4

        for ip_cls in result:
            assert 0 <= int(ip_cls) <= 255

    def test_load_json_data(self, tmp_path):
        """TODO: NEED REFACTORING!!"""

        local_ip = util.get_private_ip()

        channel_manage_data_content = """
        {
          "loopchain_channel": {
            "score_package": "score/icx",
            "peers": [
              {
                "id": "test1",
                "peer_target": "[local_ip]:7100",
                "order": 1
              },
              {
                "id": "test2",
                "peer_target": "[local_ip]:7200",
                "order": 2
              },
              {
                "id": "test3",
                "peer_target": "[local_ip]:7300",
                "order": 3
              },
              {
                "id": "test4",
                "peer_target": "[local_ip]:7400",
                "order": 4
              }
            ]
          }
        }

        """
        channel_manage_data_content_answer = {
            "loopchain_channel": {
                "score_package": "score/icx",
                "peers": [
                    {
                        "id": "test1",
                        "peer_target": f"{local_ip}:7100",
                        "order": 1
                    },
                    {
                        "id": "test2",
                        "peer_target": f"{local_ip}:7200",
                        "order": 2
                    },
                    {
                        "id": "test3",
                        "peer_target": f"{local_ip}:7300",
                        "order": 3
                    },
                    {
                        "id": "test4",
                        "peer_target": f"{local_ip}:7400",
                        "order": 4
                    }
                ]
            }
        }

        file_path = tmp_path / "channel_manage_data.json"
        file_path.write_text(channel_manage_data_content)

        json_data = util.load_json_data(file_path)
        assert channel_manage_data_content_answer == json_data

    @skip_test(reason="Not Used?")
    def test_dict_to_binary(self):
        pass

    @skip_test(reason="Self-Explanatory function")
    def test_get_time_stamp(self):
        result = util.get_time_stamp()
        assert result == 0

    @skip_test(reason="Self-Explanatory function")
    def test_diff_in_seconds(self):
        result = util.diff_in_seconds(1)
        assert result == 0

    @skip_test(reason="Not Used?")
    def test_get_timestamp_seconds(self):
        result = util.get_timestamp_seconds(1)
        assert result == 0

    @skip_test(reason="Out of Dependency")
    def test_get_valid_filename(self):
        assert 0

    @skip_test(reason="Out of Dependency")
    def test_is_protected_type(self):
        assert 0

    @skip_test(reason="Out of Dependency")
    def test_force_text(self):
        assert 0

    @skip_test(reason="Later")
    def test_check_port_using(self):
        from http.server import HTTPServer
        assert 0

    @skip_test(reason="Self-Explanatory function")
    def test_datetime_diff_in_mins(self):
        assert 0

    @skip_test(reason="Self-Explanatory function")
    def test_pretty_json(self):
        assert 0

    @skip_test(reason="Not Used?")
    def test_parse_target_list(self):
        pass

    def test_init_level_db(self, tmp_path):
        """Test for init_level_db

        Cannot monkey patch the method due to tight coupling
        So, try to mimic the method

        """

        from leveldb import LevelDB

        db_root = tmp_path / "db_root"
        db_file = "this_is.database"

        db_root.mkdir()
        db, full_path = util.init_level_db(level_db_identity=db_file, allow_rename_path=False, db_root=db_root)

        print(db, full_path)

        assert isinstance(db, LevelDB)

    @skip_test(reason="Not Used?")
    def test_no_send_apm_event(self):
        assert 0

    @skip_test(reason="Not Used?")
    def test_send_apm_event(self):
        assert 0

