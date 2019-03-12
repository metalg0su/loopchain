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
"""loopchain main peer service.
It has secure outer service for p2p consensus and status monitoring.
And also has insecure inner service for inner process modules."""

import multiprocessing
import signal
import timeit
from functools import partial

from loopchain.baseservice import CommonSubprocess
from loopchain.baseservice import StubManager, Monitor, ObjectManager, RestStubManager
from loopchain.blockchain import *
from loopchain.container import RestService, CommonService
from loopchain.crypto.signature import Signer
from loopchain.peer import PeerInnerService, PeerOuterService
from loopchain.protos import loopchain_pb2, loopchain_pb2_grpc
from loopchain.utils import loggers, command_arguments
from loopchain.utils.message_queue import StubCollection


class PeerService:
    """Peer Service 의 main Class
    outer 와 inner gRPC 인터페이스를 가진다.
    서비스 루프 및 공통 요소는 commonservice 를 통해서 처리한다.
    channel 관련 instance 는 channel manager 를 통해서 관리한다.
    """

    def __init__(self, group_id=None, radio_station_target=None, node_type=None):
        """Peer는 Radio Station 에 접속하여 leader 및 다른 Peer에 대한 접속 정보를 전달 받는다.

        :param group_id: Peer Group 을 구분하기 위한 ID, None 이면 Single Peer Group 이 된다. (peer_id is group_id)
        conf.PEER_GROUP_ID 를 사용하면 configure 파일에 저장된 값을 group_id 로 사용하게 된다.
        :param radio_station_ip: RS IP
        :param radio_station_port: RS Port
        :return:
        """
        node_type = node_type or conf.NodeType.CommunityNode

        # TODO: 이게 뭐지?
        self.is_support_node_function = \
            partial(conf.NodeType.is_support_node_function, node_type=node_type)

        util.logger.spam(f"Your Peer Service runs on debugging MODE!")
        util.logger.spam(f"You can see many terrible garbage logs just for debugging, DO U Really want it?")

        # process monitor must start before any subprocess
        if conf.ENABLE_PROCESS_MONITORING:
            Monitor().start()

        # todo: 이게 여기 있는 이유는..? 가독성 떄문인가
        self.__node_type = node_type

        self.__radio_station_target = radio_station_target
        logging.info("Set Radio Station target is " + self.__radio_station_target)

        # radio_station stub을 이용해서 radio_station과 상호작용을 할 것으로 기대되는군
        self.__radio_station_stub = None

        self.__level_db = None
        self.__level_db_path = ""

        self.__peer_id = None
        self.__group_id = group_id
        if self.__group_id is None and conf.PEER_GROUP_ID != "":
            self.__group_id = conf.PEER_GROUP_ID

        # common_service와 channel_infos가 몰려있구나
        self.__common_service = None
        self.__channel_infos = None

        # rest도 보이고
        self.__rest_service = None
        self.__rest_proxy_server = None

        # peer status cache for channel
        # todo: 무엇?
        self.status_cache = {}  # {channel:status}

        self.__score = None
        self.__peer_target = None
        self.__rest_target = None
        self.__inner_target = None
        self.__peer_port = 0

        # gRPC service for Peer
        self.__inner_service: PeerInnerService = None
        self.__outer_service: PeerOuterService = None
        self.__channel_services = {}

        self.__reset_voter_in_progress = False
        self.__json_conf_path = None

        self.__node_keys: dict = {}

        ObjectManager().peer_service = self

    @property
    def common_service(self):
        return self.__common_service

    @property
    def inner_service(self):
        return self.__inner_service

    @property
    def outer_service(self):
        return self.__outer_service

    @property
    def peer_target(self):
        return self.__peer_target

    @property
    def rest_target(self):
        return self.__rest_target

    @property
    def json_conf_path(self):
        return self.__json_conf_path

    @property
    def channel_infos(self):
        return self.__channel_infos

    @property
    def node_type(self):
        return self.__node_type

    @property
    def radio_station_target(self):
        return self.__radio_station_target

    @property
    def stub_to_radiostation(self):
        # 일단 지금은 rs stub이라고 알아두자
        if self.__radio_station_stub is None:
            if self.is_support_node_function(conf.NodeFunction.Vote):

                if conf.ENABLE_REP_RADIO_STATION:
                    self.__radio_station_stub = StubManager.get_stub_manager_to_server(
                        self.__radio_station_target,
                        loopchain_pb2_grpc.RadioStationStub,
                        conf.CONNECTION_RETRY_TIMEOUT_TO_RS,
                        ssl_auth_type=conf.GRPC_SSL_TYPE)
                else:
                    self.__radio_station_stub = None
            else:
                self.__radio_station_stub = RestStubManager(self.__radio_station_target)

        return self.__radio_station_stub

    @property
    def peer_port(self):
        return self.__peer_port

    @property
    def peer_id(self):
        return self.__peer_id

    @property
    def group_id(self):
        if self.__group_id is None:
            self.__group_id = self.__peer_id
        return self.__group_id

    @property
    def node_keys(self):
        return self.__node_keys

    def service_stop(self):
        self.__common_service.stop()

    def __get_channel_infos(self):
        print("\n\n\n겟 채널 인포쓰으 -- \n\n\n")
        # 잘은 모르겠지만 rs에게 channel info를 물어봐서 가져오는 듯 하군
        # util.logger.spam(f"__get_channel_infos:node_type::{self.__node_type}")
        if self.is_support_node_function(conf.NodeFunction.Vote):
            # todo: 아래 옵션은 앵간하면 False네. REP 라디오 스테이션은 대체 무엇을 의미하는 것인가?..
            if conf.ENABLE_REP_RADIO_STATION:
                response = self.stub_to_radiostation.call_in_times(
                    method_name="GetChannelInfos",
                    message=loopchain_pb2.GetChannelInfosRequest(
                        peer_id=self.__peer_id,
                        peer_target=self.__peer_target,
                        group_id=self.group_id),
                    retry_times=conf.CONNECTION_RETRY_TIMES_TO_RS,
                    is_stub_reuse=False,
                    timeout=conf.CONNECTION_TIMEOUT_TO_RS
                )
                # util.logger.spam(f"__get_channel_infos:response::{response}")

                if not response:
                    return None
                logging.info(f"Connect to channels({util.pretty_json(response.channel_infos)})")
                channels = json.loads(response.channel_infos)
            else:
                # 여기서 채널 매니지 테이터를 읽어오는군.
                channels = util.load_json_data(conf.CHANNEL_MANAGE_DATA_PATH)

                if conf.ENABLE_CHANNEL_AUTH:
                    filtered_channels = {channel: channels[channel] for channel in channels
                                         for peer in channels[channel]['peers']
                                         if self.__peer_id == peer['id']}
                    channels = filtered_channels
        else:
            response = self.stub_to_radiostation.call_in_times(method_name="GetChannelInfos")
            channels = {channel: value for channel, value in response["channel_infos"].items()}

        return channels

    def __init_port(self, port):
        # service 초기화 작업
        target_ip = util.get_private_ip()
        self.__peer_target = util.get_private_ip() + ":" + str(port)
        self.__peer_port = int(port)

        rest_port = int(port) + conf.PORT_DIFF_REST_SERVICE_CONTAINER
        self.__rest_target = f"{target_ip}:{rest_port}"

        logging.info("Start Peer Service at port: " + str(port))

    def __init_level_db(self):
        # level db for peer service not a channel, It store unique peer info like peer_id
        self.__level_db, self.__level_db_path = util.init_level_db(
            level_db_identity=self.__peer_target,
            allow_rename_path=False
        )

    def __run_rest_services(self, port):
        if conf.ENABLE_REST_SERVICE and conf.RUN_ICON_IN_LAUNCHER:
            logging.debug(f'Launch Sanic RESTful server. '
                          f'Port = {int(port) + conf.PORT_DIFF_REST_SERVICE_CONTAINER}')
            self.__rest_service = RestService(int(port))

    def __init_key_by_channel(self):
        for channel in conf.CHANNEL_OPTION:
            signer = Signer.from_channel(channel)
            if channel == conf.LOOPCHAIN_DEFAULT_CHANNEL:
                self.__make_peer_id(signer.address)
            self.__node_keys[channel] = signer.private_key.private_key

    def __make_peer_id(self, address):
        self.__peer_id = address

        logger_preset = loggers.get_preset()
        logger_preset.peer_id = self.peer_id
        logger_preset.update_logger()

        logging.info(f"run peer_id : {self.__peer_id}")

    def timer_test_callback_function(self, message):
        logging.debug(f'timer test callback function :: ({message})')

    @staticmethod
    def __get_use_kms():
        if conf.GRPC_SSL_KEY_LOAD_TYPE == conf.KeyLoadType.KMS_LOAD:
            return True
        for value in conf.CHANNEL_OPTION.values():
            if value["key_load_type"] == conf.KeyLoadType.KMS_LOAD:
                return True
        return False

    def __init_kms_helper(self, agent_pin):
        if self.__get_use_kms():
            from loopchain.tools.kms_helper import KmsHelper
            KmsHelper().set_agent_pin(agent_pin)

    def __close_kms_helper(self):
        if self.__get_use_kms():
            from loopchain.tools.kms_helper import KmsHelper
            KmsHelper().remove_agent_pin()

    def run_common_service(self):
        print("\n\n\n 피어 써비스의 런 커먼 써어비쓰 시자아아악 !!!\n\n")
        inner_service_port = conf.PORT_INNER_SERVICE or (self.__peer_port + conf.PORT_DIFF_INNER_SERVICE)
        self.__inner_target = conf.IP_LOCAL + ":" + str(inner_service_port)

        print("\n\n\n 커먼 써어비쓰  정의!!!\n\n")
        self.__common_service = CommonService(loopchain_pb2, inner_service_port)
        self.__common_service.start(str(self.__peer_port), self.__peer_id, self.__group_id)

        # 이 시점에서 outer service가 기동되는군
        print("\n\n\n피어써어비쓰 투 써버 (아우터 써어비쓰만 등록)")
        loopchain_pb2_grpc.add_PeerServiceServicer_to_server(self.__outer_service, self.__common_service.outer_server)
        # 아.. 마치 래빗앰큐처럼 grpc도 데몬같은 게 있고, 거기에 스텁이나 스켈레톤 같은걸 등록시킨다음에, 데몬끼리 통신해주는건가?..
        # 그리고 그 역할을 여기서는 커먼 서비스라고 묶은 것 같음. 'listen이 가능한 스켈을 지알피씨에 등록하는 과정' 까지인 느낌이 든다. 잘 모르겠음 ㅡㅡ;;;;

    def serve(self,
              port,
              agent_pin: str=None,
              amqp_target: str=None,
              amqp_key: str=None,
              event_for_init: multiprocessing.Event=None):
        """start func of Peer Service ===================================================================

        :param port:
        :param agent_pin: kms agent pin
        :param amqp_target: rabbitmq host target
        :param amqp_key: sharing queue key
        :param event_for_init: set when peer initiates
        """

        amqp_target = amqp_target or conf.AMQP_TARGET
        amqp_key = amqp_key or conf.AMQP_KEY

        stopwatch_start = timeit.default_timer()

        self.__init_kms_helper(agent_pin)
        self.__init_port(port)
        self.__init_level_db()
        self.__init_key_by_channel()

        # todo: 이 지점이 싱글턴 객체의 최초 사용 지점인가보다.. 최초의 amqp 관련 설정을 하는 모습같다.
        StubCollection().amqp_target = amqp_target
        StubCollection().amqp_key = amqp_key

        peer_queue_name = conf.PEER_QUEUE_NAME_FORMAT.format(amqp_key=amqp_key)
        print("\n\n\n피어 아우터 써어비쓰 - 선언 정도만 ")
        self.__outer_service = PeerOuterService()
        print("\n\n\n피어 이너 써어비쓰 - 선언 정도만 ")
        self.__inner_service = PeerInnerService(
            amqp_target, peer_queue_name, conf.AMQP_USERNAME, conf.AMQP_PASSWORD, peer_service=self)

        # todo: rs 없이 돌아가는건 어떤 경로를 통해 그런건지 나중에 조사
        self.__channel_infos = self.__get_channel_infos()
        if not self.__channel_infos:
            # 채널 정보가 없으면 쫒아내버리는군
            util.exit_and_msg("There is no peer_list, initial network is not allowed without RS!")

        # rest가 이 시점에서 시작된다.
        print("\n\n\nreeeest 호출 시작!!!!!========================================================================================!!!!!\n\n\n")
        self.__run_rest_services(port)
        print("\n\n\nrestttt 호출 끝!!!!!=======================================. 아마도 이제 Rest 프로세스도 로그를 남기기 시작해서 중첩될듯 !!!!\n\n\n")
        print("\n\n\n피어 써어비쓰가 호출하는 커먼 써어비쓰 시작================================================!!!\n\n\n")
        self.run_common_service()
        print("\n\n\n커먼서비스으으으ㅡ응 끝========================================================================!!!!\n\n\n")

        self.__close_kms_helper()

        stopwatch_duration = timeit.default_timer() - stopwatch_start
        logging.info(f"Start Peer Service at port: {port} start duration({stopwatch_duration})")

        print("\n\n\n피어 써비스 어싱크의 경계선!!!!!======================================================= ")

        async def _serve():
            # 아래의 ready_tasks에서 채널 태스크를 띄우는 건가봄!
            print("\n\n\n레디 테스크 시작!!!!!")
            await self.ready_tasks()
            print("\n\n\n~~~~~~이너써어비스 - 커넥트 시작! - 이건 rabbitMQ로 하는듯.") # 결국은 피어와 [채널, 채널tx리씨버, 스코어] 와는 래빗앰큐로 통신.
            await self.__inner_service.connect(conf.AMQP_CONNECTION_ATTEMPS, conf.AMQP_RETRY_DELAY, exclusive=True) # 들을 준비가 여기서 되는 것 같군.
            print("\n\n\n~~~~~~이너써어비스 - 커넥트 끝!!")

            # 여기서부터는 채널을 띄우는 것 같아. 아얘 프로세스로.  이 지점은 수신점이 되는건가
            if conf.CHANNEL_BUILTIN:
                print("\n\n\n써어브 채널 시작===========================. 채널 하나를 띄우기 떄문에 꽤 길어진다. 옵션은 빌트인인거가틈.")
                await self.serve_channels()

            print("\n\n\n써어브 채널 끝.?")
            if event_for_init is not None:
                print("\n\n\n 이벤트 포 이닛이 넌이 아님")
                event_for_init.set()

            logging.info(f'peer_service: init complete peer: {self.peer_id}')
            print("\n\n\n끝: 피어 써비스 어씽크의 경계선!!! 탈출 성공!!!!!!!! ============================================================================\n\n\n")
            # nㅜㅜ 탈출 성공

        loop = self.__inner_service.loop
        # todo: 왜 여러개를 만드는거야 - 여러개가 아니라, 프로세스가 이미 이전에 여러개가 뜬 거 같음. 그래서 로그 보면 pid가 다른 것 같음.
        loop.create_task(_serve())
        loop.add_signal_handler(signal.SIGINT, self.close)
        loop.add_signal_handler(signal.SIGTERM, self.close)

        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

        self.__common_service.wait()

        # process monitor must stop monitoring before any subprocess stop
        # Monitor().stop()

        logging.info("Peer Service Ended.")
        if self.__rest_service is not None:
            self.__rest_service.stop()

        if self.__rest_proxy_server is not None:
            self.__rest_proxy_server.stop()

    def close(self):
        async def _close():
            for channel_stub in StubCollection().channel_stubs.values():
                await channel_stub.async_task().stop("Close")

            self.service_stop()
            loop.stop()

        loop = self.__inner_service.loop
        loop.create_task(_close())

    async def serve_channels(self):
        """ 아.. 이 시점에서 새로운 프로세스를 또 띄우시는군요... channel 이라고 하는 """

        for i, channel_name in enumerate(self.__channel_infos.keys()): # 채널 갯수만큼 주르륵 띄운다.
            # 채널명은 channel_infos에서 가져오는거로군
            score_port = self.__peer_port + conf.PORT_DIFF_SCORE_CONTAINER + conf.PORT_DIFF_BETWEEN_SCORE_CONTAINER * i

            args = ['python3', '-m', 'loopchain', 'channel']
            args += ['-p', str(score_port)]
            args += ['--channel', str(channel_name)] # 이 채널 명으로 채널을 띄우눈군.
            args += command_arguments.get_raw_commands_by_filter(
                command_arguments.Type.Develop,
                command_arguments.Type.AMQPTarget,
                command_arguments.Type.AMQPKey,
                command_arguments.Type.ConfigurationFilePath,
                command_arguments.Type.RadioStationTarget
            )

            print("\n\n\n 채널을 띄울 준비가 됨! \n\n\n ")
            # todo: 얘는 대체 왜 여러개가 뜨는거야~~ 그리고는 이 이후에 아이콘 스코어 스텁이 뜨는 것 같은데..
            service = CommonSubprocess(args)
            # service.set_proctitle(f"이거슨 채널{i} 띄우다가 뜬 커먼서브프로세스임다.") # 이 자체만으로도 실행하는 걸텐데.. 일단은 아래의 표준출력이 바로 찍히고, 채널이 또다른 프로세스에서 돌아가기 시작하는 것 같다.
            print("\n\n\n 채널에서 커먼 써브프로세스 코드 건넘! \n\n\n ")

            channel_stub = StubCollection().channel_stubs[channel_name] # ready_tasks에서 만들었던 stub들을 꺼내와서 각자 매핑.
            await channel_stub.async_task().hello() # 응답 오는지 확인하는 듯 - 각 채널마다.

            self.__channel_services[channel_name] = service # 각 피어마다 관리하는 채널이 dict식으로 되어있나봄. 그 프로세스를 매핑하네
            print("\n\n써어브 채널쓰 끝. \n\n\n")

    async def ready_tasks(self):
        await StubCollection().create_peer_stub()  # for getting status info. 얘는 내부에서  통신하기 위해 mq에 접속할 대리인을 만드는 것 같음.내부 통신 대리자

        # 채널이 여러개면 (멀티채널이면) 채널 갯수만큼 여기서 뜨는 것 같다.
        for channel_name, channel_info in self.__channel_infos.items():
            print("\n\n\n채널네임: ", channel_name, )

            # 여러 스텁을 만듦으로서, 외부와 통신할 준비를 하는 것 같군.
            await StubCollection().create_channel_stub(channel_name) # 멀티채널용 채널 간 통신을 위한 대리자. 채널 명을 따서 큐를 만드는 듯 하다.
            await StubCollection().create_channel_tx_receiver_stub(channel_name) # 채널 간의 tx를 주고 받기 위한 통로같음. todo TxCreator는 어디서 만들어지는거지?
            await StubCollection().create_icon_score_stub(channel_name) # 근본적으로 create.. 이것들은 MQ의 큐를 선언하는 행위같음. 실제로 rabbitMQ의 큐가 만들어짐
