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

import loopchain.utils as util
from pytest_bdd import scenario, given, when, then, parsers
from loopchain.channel.channel_statemachine import ChannelStateMachine
from loopchain.protos import loopchain_pb2
from typing import Optional


class MockBlockManager:
    peer_type = loopchain_pb2.BLOCK_GENERATOR

    def __init__(self):
        self.timer_called = 0
        self.peer_type = loopchain_pb2.BLOCK_GENERATOR

    def start_block_generate_timer(self):
        if self.timer_called == 0:
            self.timer_called += 1

    def stop_block_generate_timer(self):
        self.timer_called -= 1

    def block_height_sync(self):
        pass

    def stop_block_height_sync_timer(self):
        pass

    def update_service_status(self, status):
        pass

    def start_epoch(self):
        pass


class MockBlockManagerCitizen:
    peer_type = loopchain_pb2.PEER

    def start_block_generate_timer(self):
        pass

    def block_height_sync(self):
        pass

    def stop_block_height_sync_timer(self):
        pass

    def update_service_status(self, status):
        pass

    def start_epoch(self):
        pass


class MockChannelService:
    def __init__(self):
        self.block_manager = MockBlockManager()

    async def evaluate_network(self):
        pass

    async def subscribe_network(self):
        pass

    def update_sub_services_properties(self):
        pass

    def start_subscribe_timer(self):
        pass

    def start_shutdown_timer(self):
        pass

    def stop_subscribe_timer(self):
        pass

    def stop_shutdown_timer(self):
        pass


class MockChannelServiceCitizen:
    async def evaluate_network(self):
        pass

    async def subscribe_network(self):
        pass

    def update_sub_services_properties(self):
        pass

    def start_subscribe_timer(self):
        pass

    def start_shutdown_timer(self):
        pass

    def stop_subscribe_timer(self):
        pass

    def stop_shutdown_timer(self):
        pass

    def is_support_node_function(self, node_function):
        return False

    @property
    def block_manager(self):
        return MockBlockManagerCitizen()


class States:
    init_components = "InitComponents"
    evaluate_network = "EvaluateNetwork"
    subscribe_network = "SubscribeNetwork"
    block_generate = "BlockGenerate"
    watch = "Watch"


feature_file_path = "channel_fsm.feature"
@scenario(feature_file_path, "init channel state")
def test_init_channel_state_machine():
    pass


@scenario(feature_file_path, "init community node")
def test_init_citizen_node():
    pass


@scenario(feature_file_path, "init citizen node")
def test_init_community_node():
    pass


@scenario(feature_file_path, "trigger turn_to_peer twice and timer called only once")
def test_turn_to_peer_twice_and_timer_called_only_once():
    pass


@given(parsers.parse("{node_type} node is initialized"))
def channel_state_machine(node_type) -> ChannelStateMachine:
    channel_service = None
    if node_type == "community":
        channel_service = MockChannelService()
    elif node_type == "citizen":
        channel_service = MockChannelServiceCitizen()

    fsm = ChannelStateMachine(channel_service)
    assert fsm.state == States.init_components

    return fsm


@given("trigger complete_init_components")
def complete_init_components(channel_state_machine: ChannelStateMachine):
    channel_state_machine.complete_init_components()


@when("trigger complete_init_components")
def when_complete_init_components(channel_state_machine: ChannelStateMachine):
    channel_state_machine.complete_init_components()


@given("trigger block_sync")
def trigger_block_sync(channel_state_machine: ChannelStateMachine):
    channel_state_machine.block_sync()


@given("trigger complete_sync")
def trigger_complete_sync(channel_state_machine: ChannelStateMachine):
    channel_state_machine.complete_sync()
    assert channel_state_machine.state == States.subscribe_network


@given("trigger complete_subscribe")
def trigger_complete_subscribe(channel_state_machine: ChannelStateMachine):
    channel_state_machine.complete_subscribe()


@when("trigger complete_subscribe")
def when_trigger_complete_subscribe(channel_state_machine: ChannelStateMachine):
    channel_state_machine.complete_subscribe()


@then("the state should be BlockGenerate")
def state_should_be_block_generate(channel_state_machine: ChannelStateMachine):
    assert channel_state_machine.state == States.block_generate


@then("the state should be EvaluateNetwork")
def state_should_be_evaluate_network(channel_state_machine: ChannelStateMachine):
    assert channel_state_machine.state == States.evaluate_network


@then("the state should be Watch")
def state_should_be_watch(channel_state_machine: ChannelStateMachine):
    assert channel_state_machine.state == States.watch


@when("trigger turn_to_leader")
def trigger_turn_to_leader(channel_state_machine: ChannelStateMachine):
    channel_state_machine.turn_to_leader()


@then("timer_should_be_called_only_at_once")
def timer_should_be_called_only_at_once(channel_state_machine: ChannelStateMachine):
    channel_service = channel_state_machine._ChannelStateMachine__channel_service

    assert channel_service.block_manager.timer_called == 1


