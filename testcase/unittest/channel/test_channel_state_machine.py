import enum
from typing import cast
from unittest.mock import MagicMock

import pytest

from loopchain import ChannelService
from loopchain import configure_default as conf
from loopchain.channel.channel_property import ChannelProperty
from loopchain.channel.channel_statemachine import ChannelStateMachine
from loopchain.configure_default import NodeType
from loopchain.protos import loopchain_pb2


class States(enum.Enum):
    InitComponents = enum.auto()
    EvaluateNetwork = enum.auto()
    BlockGenerate = enum.auto()
    Vote = enum.auto()
    Watch = enum.auto()
    SubscribeNetwork = enum.auto()


@pytest.fixture
def channel_service() -> ChannelService:
    service = cast(ChannelService, MagicMock(ChannelService))
    service.is_support_node_function = lambda node_function: conf.NodeType.is_support_node_function(node_function, ChannelProperty().node_type)

    return service


class TestChannelStateMachine:
    @pytest.fixture
    def channel_state_machine(self, channel_service) -> ChannelStateMachine:
        return ChannelStateMachine(channel_service)

    def test_init_channel_state(self, channel_state_machine):
        # GIVEN
        assert channel_state_machine.state == States.InitComponents.name

        # WHEN
        channel_state_machine.complete_init_components()

        # THEN
        assert channel_state_machine.state == "EvaluateNetwork"

    @pytest.fixture(autouse=True)
    def channel_property(self):
        yield

        ChannelProperty().name = None
        ChannelProperty().peer_target = None
        ChannelProperty().rest_target = None
        ChannelProperty().rs_target = None
        ChannelProperty().amqp_target = None
        ChannelProperty().peer_port = None
        ChannelProperty().peer_id = None
        ChannelProperty().peer_address = None
        ChannelProperty().peer_auth = None
        ChannelProperty().node_type = None
        ChannelProperty().nid = None
        ChannelProperty().crep_root_hash = None

    @pytest.mark.parametrize("peer_type, node_type, expected_state", [
        (loopchain_pb2.BLOCK_GENERATOR, NodeType.CommunityNode, States.BlockGenerate.name),
        (loopchain_pb2.PEER, NodeType.CommunityNode, States.Vote.name),
        (loopchain_pb2.PEER, NodeType.CitizenNode, States.Watch.name),
    ])
    def test_change_state_by_condition(self, channel_service, peer_type, node_type, expected_state):
        # GIVEN
        channel_service.peer_type = peer_type
        ChannelProperty().node_type = node_type
        channel_state_machine = ChannelStateMachine(channel_service)

        # WHEN
        channel_state_machine.complete_init_components()
        channel_state_machine.block_sync()
        channel_state_machine.complete_sync()
        channel_state_machine.complete_subscribe()

        # THEN
        assert channel_state_machine.state == expected_state

    def test_change_state_by_multiple_condition(self, channel_service):
        # GIVEN
        channel_service.peer_type = loopchain_pb2.PEER
        ChannelProperty().node_type = NodeType.CitizenNode
        channel_state_machine = ChannelStateMachine(channel_service)

        # AND
        channel_state_machine.complete_init_components()
        channel_state_machine.block_sync()
        channel_state_machine.complete_sync()
        assert channel_state_machine.state == States.SubscribeNetwork.name

        # WHEN
        channel_state_machine.complete_subscribe()

        # THEN
        assert channel_state_machine.state == States.Watch.name

    def test_change_state_from_same_state(self, channel_service):
        # GIVEN
        channel_service.peer_type = loopchain_pb2.BLOCK_GENERATOR
        ChannelProperty().node_type = NodeType.CommunityNode
        channel_state_machine = ChannelStateMachine(channel_service)

        channel_state_machine.complete_init_components()
        channel_state_machine.block_sync()
        channel_state_machine.complete_sync()
        channel_state_machine.complete_subscribe()

        assert channel_state_machine.state == States.BlockGenerate.name

        # WHEN
        channel_state_machine.turn_to_leader()
        assert channel_service.block_manager.start_block_generate_timer.called == 1

        channel_state_machine.turn_to_leader()
        assert channel_state_machine.state == States.BlockGenerate.name

        # THEN
        assert channel_service.block_manager.start_block_generate_timer.called == 1
