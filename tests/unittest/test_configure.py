"""Unittest related to loopchain configure

This test is inspired from 'testcase/unittest/test_configure.py
Also this shows minor efforts to increase test coverage, checking the usage of its original source code

"""

import json
import logging

import pytest

from loopchain import configure as conf
from loopchain import configure_default as conf_default
from loopchain.utils import loggers

# Logger
loggers.set_preset_type(loggers.PresetType.develop)
loggers.update_preset()


class TestConfigure:
    """Test Class related to configure"""

    def test_configure_type(self):
        """Test from legacy"""
        logging.debug(f"conf.IP_LOCAL: {conf.IP_LOCAL}")
        assert conf.IP_LOCAL == conf_default.IP_LOCAL

        logging.debug(f"conf.GRPC_TIMEOUT: {conf.GRPC_TIMEOUT}")
        assert isinstance(conf.GRPC_TIMEOUT, int)

        logging.debug(f"conf.LEVEL_DB_KEY_FOR_PEER_LIST: {conf.LEVEL_DB_KEY_FOR_PEER_LIST}")
        assert conf.LEVEL_DB_KEY_FOR_PEER_LIST == conf_default.LEVEL_DB_KEY_FOR_PEER_LIST

        logging.debug(f"conf.TOKEN_TYPE_TOKEN: {conf.TOKEN_TYPE_TOKEN}")
        assert isinstance(conf.TOKEN_TYPE_TOKEN, str)

    @pytest.fixture
    def test_load_configure_json(self, tmp_path):
        """Test for load_configure_json method

        Test that the load_configure_json method can read json file and apply configure properly"""

        ip_local_before_load_json = conf.IP_LOCAL
        token_type_token_before_load_json = conf_default.TOKEN_TYPE_TOKEN
        logging.debug(f"before json file load, conf.IP_LOCAL({ip_local_before_load_json})"
                      f", conf.TOKEN_TYPE_TOKEN({token_type_token_before_load_json})")

        json_content = """
        {
            "IP_LOCAL": "999.000.111.222",
            "TOKEN_TYPE_TOKEN": "04"
        }
        """
        file_path = tmp_path / "configure_json_for_test.json"
        file_path.write_text(json_content)

        with open(file_path) as file:
            content = json.load(file)
            logging.debug(f"VALID_JSON: {content}")
            assert content == json.loads(json_content)

        conf.Configure().load_configure_json(file_path)
        logging.debug(f"after json file load, conf.IP_LOCAL({conf.IP_LOCAL})"
                      f", conf.TOKEN_TYPE_TOKEN({conf.TOKEN_TYPE_TOKEN})")

        assert not ip_local_before_load_json == conf.IP_LOCAL
        assert not token_type_token_before_load_json == conf.TOKEN_TYPE_TOKEN


    @pytest.mark.skip
    @pytest.fixture
    def test_load_configure_json_invalid_config_key(self, tmp_path):
        """Test for Invalid configure

        But Exception in load_configure_json is so weird...

        """

        json_content = """
        {
            "IP_LOCAL": 99,
            "TOKEN_TYPE_TOKEN": "44",
            "INVALID_CONFIG_KEY" : "INVALID_CONFIG_VALUE"
        }
        """
        file_path = tmp_path / "invalid_configure_json_for_test.json"
        file_path.write_text(json_content)

        with open(file_path) as file:
            content = json.load(file)
            logging.debug(f"INVALID_JSON: {content}")
            assert content == json.loads(json_content)

        with pytest.raises(Exception):  # TODO: Too broad Exception!
            conf.Configure().load_configure_json(file_path)
            # logging.debug(f"configure: {conf.Configure().configure_info_list}")

    @pytest.mark.parametrize("node_type, node_function", [
        (conf.NodeType.CommunityNode, conf.NodeFunction.Vote),
        (conf.NodeType.CommunityNode, conf.NodeFunction.Block),
        (conf.NodeType.CitizenNode, conf.NodeFunction.Block),
    ])
    def test_this_node_can_do_those_function(self, node_type, node_function):
        """Test that a certain type of node can carry out a node_function

        Community Node can do: Vote, Block
        Citizen Node can do: Block

        """

        logging.debug(f"CAN DO---\n node_type: {node_type}, node_function: {node_function}")
        assert conf.NodeType.is_support_node_function(node_function=node_function, node_type=node_type)

    @pytest.mark.parametrize("node_type, node_function", [
        (conf.NodeType.CitizenNode, conf.NodeFunction.Vote),
    ])
    def test_this_node_cannot_do_those_function(self, node_type, node_function):
        """Anti-test against 'test_this_node_can_do_those_function'

        Check that a certain node MUST NOT do a node_function
        """

        logging.debug(f"CANNOT---\n node_type: {node_type}, node_function: {node_function}")
        assert not conf.NodeType.is_support_node_function(node_function=node_function, node_type=node_type)

