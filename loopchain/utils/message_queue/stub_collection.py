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

import logging

from typing import Dict, TYPE_CHECKING
from loopchain.components import SingletonMetaClass

if TYPE_CHECKING:
    from loopchain.peer import PeerInnerStub
    from loopchain.channel.channel_inner_service import ChannelInnerStub, ChannelTxReceiverInnerStub
    from loopchain.scoreservice import ScoreInnerStub, IconScoreInnerStub


class StubCollection(metaclass=SingletonMetaClass):
    def __init__(self):
        self.amqp_target = None
        self.amqp_key = None

        self.peer_stub: PeerInnerStub = None
        self.channel_stubs: Dict[str, ChannelInnerStub] = {}
        self.channel_tx_receiver_stubs: Dict[str, ChannelTxReceiverInnerStub] = {}
        self.score_stubs: Dict[str, ScoreInnerStub] = {}
        self.icon_score_stubs: Dict[str, IconScoreInnerStub] = {}

    async def create_peer_stub(self):
        # 싱글톤으로 사용하기 위해서 이걸 이용해 이너스텁을 만든 거였구나.. 언제든 조회가 되겠어....
        print("\n\n\n피어 스텁 만들기!!!!!!!!")
        # todo: 이것의 역할은?...
        from loopchain import configure as conf
        from loopchain.peer import PeerInnerStub

        queue_name = conf.PEER_QUEUE_NAME_FORMAT.format(amqp_key=self.amqp_key)
        self.peer_stub = PeerInnerStub(self.amqp_target, queue_name, conf.AMQP_USERNAME, conf.AMQP_PASSWORD) # todo : 이 부분이 이해가 안감...
        await self.peer_stub.connect(conf.AMQP_CONNECTION_ATTEMPS, conf.AMQP_RETRY_DELAY)
        # todo: InnerService에서 들을 준비를 하고, 이제 보낼 준비를 하는 것 같은데.아무런 정보도 안받고 채널은 peerInnerStub을 만드나?
        return self.peer_stub

    async def create_channel_stub(self, channel_name):
        from loopchain import configure as conf
        from loopchain.channel.channel_inner_service import ChannelInnerStub

        queue_name = conf.CHANNEL_QUEUE_NAME_FORMAT.format(
            channel_name=channel_name, amqp_key=self.amqp_key)
        stub = ChannelInnerStub(self.amqp_target, queue_name, conf.AMQP_USERNAME, conf.AMQP_PASSWORD)
        await stub.connect(conf.AMQP_CONNECTION_ATTEMPS, conf.AMQP_RETRY_DELAY)
        self.channel_stubs[channel_name] = stub

        print("\n\n\n채널 스텁 만들었다. -0 왜 여러개가 뜨지? ")

        logging.debug(f"ChannelTasks : {channel_name}, Queue : {queue_name}")
        return stub

    async def create_channel_tx_receiver_stub(self, channel_name):
        from loopchain import configure as conf
        from loopchain.channel.channel_inner_service import ChannelTxReceiverInnerStub

        queue_name = conf.CHANNEL_TX_RECEIVER_QUEUE_NAME_FORMAT.format(
            channel_name=channel_name, amqp_key=self.amqp_key)
        # 채널마다 각기 다른 스텁을 만들어서 혼선이 없도록 했군..
        stub = ChannelTxReceiverInnerStub(self.amqp_target, queue_name, conf.AMQP_USERNAME, conf.AMQP_PASSWORD)
        await stub.connect(conf.AMQP_CONNECTION_ATTEMPS, conf.AMQP_RETRY_DELAY)
        self.channel_tx_receiver_stubs[channel_name] = stub # 다른 메서드도 그렇지만, 만들었으면 멤버 객체에 집어넣어서 언제든 싱글턴의 역할을 수행할 수 있도록 한다.

        print("\n\n\n채널 티엑쓰 리씨버 만들었다. -0 왜 여러개가 뜨지? ")
        logging.debug(f"ChannelTxReceiverTasks : {channel_name}, Queue : {queue_name}")
        return stub

    async def create_score_stub(self, channel_name, score_package_name):
        from loopchain import configure as conf
        from loopchain.scoreservice import ScoreInnerStub

        queue_name = conf.SCORE_QUEUE_NAME_FORMAT.format(
            score_package_name=score_package_name, channel_name=channel_name, amqp_key=self.amqp_key)
        stub = ScoreInnerStub(self.amqp_target, queue_name, conf.AMQP_USERNAME, conf.AMQP_PASSWORD)
        await stub.connect(conf.AMQP_CONNECTION_ATTEMPS, conf.AMQP_RETRY_DELAY)
        self.score_stubs[channel_name] = stub
        return stub

    async def create_icon_score_stub(self, channel_name):
        from loopchain import configure as conf
        from loopchain.scoreservice import IconScoreInnerStub

        print("\n\n\n아이콘 스코어 스텁 만들었따!!!! - 얘는 채널도 만들기도 하고, 스코어가 만들기도 하네?.. .")
        queue_name = conf.ICON_SCORE_QUEUE_NAME_FORMAT.format(
            channel_name=channel_name, amqp_key=self.amqp_key
        )
        stub = IconScoreInnerStub(self.amqp_target, queue_name, conf.AMQP_USERNAME, conf.AMQP_PASSWORD)
        await stub.connect(conf.AMQP_CONNECTION_ATTEMPS, conf.AMQP_RETRY_DELAY)
        self.icon_score_stubs[channel_name] = stub
        return stub
