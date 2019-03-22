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
"""State Machine for Channel Service"""
import asyncio

from earlgrey import MessageQueueService
from transitions import State

import loopchain.utils as util
from loopchain import configure as conf
from loopchain.peer import status_code
from loopchain.protos import loopchain_pb2
from loopchain.statemachine import statemachine
from loopchain.utils import loggers


# ChannelStateMachine은 StateMachine을 받아오는 것 같군.
@statemachine.StateMachine("Channel State Machine")
class ChannelStateMachine(object):
    print("채널 스테이트 머신 첫줄.")
    states = ['InitComponents',
              State(name='Consensus', ignore_invalid_triggers=True,
                    on_enter='_consensus_on_enter'),
              State(name='BlockHeightSync', ignore_invalid_triggers=True,
                    on_enter='_blockheightsync_on_enter'),
              'EvaluateNetwork',
              State(name='BlockSync', ignore_invalid_triggers=True,
                    on_enter='_blocksync_on_enter', on_exit='_blocksync_on_exit'),
              State(name='SubscribeNetwork', ignore_invalid_triggers=True,
                    on_enter='_subscribe_network_on_enter', on_exit='_subscribe_network_on_exit'),
              'Watch',
              State(name='Vote', ignore_invalid_triggers=True,
                    on_enter='_vote_on_enter', on_exit='_vote_on_exit'),
              State(name='BlockGenerate', ignore_invalid_triggers=True,
                    on_enter='_blockgenerate_on_enter', on_exit='_blockgenerate_on_exit'),
              State(name='LeaderComplain', ignore_invalid_triggers=True,
                    on_enter='_leadercomplain_on_enter', on_exit='_leadercomplain_on_exit'),
              'GracefulShutdown']
    init_state = 'InitComponents'
    state = init_state
    print("채널 스테이트 머신 글로벌 끝")

    def __init__(self, channel_service):
        print("채널 스테이트 머신 이닛")
        self.__channel_service = channel_service

        self.machine.add_transition('complete_sync', 'BlockSync', 'SubscribeNetwork', after='_do_subscribe_network')
        # (trigger, from, to [, condition]) 식
        self.machine.add_transition('complete_subscribe', 'SubscribeNetwork', 'BlockGenerate', conditions=['_is_leader'])
        print("채널 스테이트 머신 애드 트랜지션: 조건1")
        self.machine.add_transition('complete_subscribe', 'SubscribeNetwork', 'Watch', conditions=['_has_no_vote_function'])
        print("채널 스테이트 머신 애드 트랜지션: 조건2")
        self.machine.add_transition('complete_subscribe', 'SubscribeNetwork', 'Vote')
        print("채널 스테이트 머신 애드 트랜지션: 조건3")

    # 음... 이 아래에 있는 것들은 info_dict에 들어가는 것 같긴 한데.. 어떨 때 변경되는거지
    @statemachine.transition(source='InitComponents', dest='Consensus')
    def complete_init_components(self):
        pass

    @statemachine.transition(source='Consensus', dest='BlockHeightSync')
    def block_height_sync(self): # 자동이네
        pass

    @statemachine.transition(source='BlockHeightSync',
                             dest='EvaluateNetwork',
                             after='_do_evaluate_network')
    def evaluate_network(self):
        pass

    @statemachine.transition(source=('EvaluateNetwork', 'Vote', 'BlockSync', 'BlockGenerate'),
                             dest='BlockSync',
                             after='_do_block_sync')
    def block_sync(self):
        pass

    @statemachine.transition(source='Watch', dest='SubscribeNetwork', after='_do_subscribe_network')
    def subscribe_network(self):
        pass

    @statemachine.transition(source=('Vote', 'LeaderComplain'), dest='Vote', after='_do_vote')
    def vote(self):
        pass

    def complete_subscribe(self):
        pass

    def complete_sync(self):
        pass

    @statemachine.transition(source=('BlockGenerate', 'Vote', 'LeaderComplain'), dest='Vote')
    def turn_to_peer(self):
        pass

    @statemachine.transition(source=('Vote', 'BlockGenerate', 'LeaderComplain'), dest='BlockGenerate')
    def turn_to_leader(self):
        pass

    @statemachine.transition(source=('Vote', 'LeaderComplain'), dest='LeaderComplain')
    def leader_complain(self):
        pass

    def _is_leader(self):
        return self.__channel_service.block_manager.peer_type == loopchain_pb2.BLOCK_GENERATOR

    def _has_no_vote_function(self):
        return not self.__channel_service.is_support_node_function(conf.NodeFunction.Vote)

    def _do_block_sync(self):
        self.__channel_service.block_manager.block_height_sync()

    def _do_evaluate_network(self):
        loop = MessageQueueService.loop
        asyncio.run_coroutine_threadsafe(self.__channel_service.evaluate_network(), loop)

    def _do_subscribe_network(self):
        loop = MessageQueueService.loop
        asyncio.run_coroutine_threadsafe(self.__channel_service.subscribe_network(), loop)

    def _do_vote(self):
        self.__channel_service.block_manager.vote_as_peer()

    def _consensus_on_enter(self):
        self.block_height_sync()

    def _blockheightsync_on_enter(self):
        self.evaluate_network()

    def _blocksync_on_enter(self):
        self.__channel_service.block_manager.update_service_status(status_code.Service.block_height_sync)

    def _blocksync_on_exit(self):
        self.__channel_service.block_manager.stop_block_height_sync_timer()
        self.__channel_service.block_manager.update_service_status(status_code.Service.online)

    def _subscribe_network_on_enter(self):
        self.__channel_service.start_subscribe_timer()
        self.__channel_service.start_shutdown_timer()

    def _subscribe_network_on_exit(self):
        self.__channel_service.stop_subscribe_timer()
        self.__channel_service.stop_shutdown_timer()

    def _vote_on_enter(self):
        loggers.get_preset().is_leader = False
        loggers.get_preset().update_logger()

    def _vote_on_exit(self):
        # util.logger.debug(f"_vote_on_exit")
        pass

    def _blockgenerate_on_enter(self):
        loggers.get_preset().is_leader = True
        loggers.get_preset().update_logger()
        self.__channel_service.block_manager.start_block_generate_timer()

    def _blockgenerate_on_exit(self):
        self.__channel_service.block_manager.stop_block_generate_timer()

    def _leadercomplain_on_enter(self):
        util.logger.debug(f"_leadercomplain_on_enter")
        self.__channel_service.block_manager.leader_complain()

    def _leadercomplain_on_exit(self):
        util.logger.debug(f"_leadercomplain_on_exit")
