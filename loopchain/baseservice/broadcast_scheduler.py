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
"""gRPC broadcast thread"""

import logging
import queue
import threading
import signal
import abc
import time
import os
import multiprocessing as mp
from concurrent import futures
from enum import Enum
from functools import partial

import grpc
from grpc._channel import _Rendezvous

from loopchain import configure as conf, utils as util
from loopchain.baseservice import StubManager, ObjectManager, CommonThread, BroadcastCommand, \
    TimerService, Timer
from loopchain.baseservice.tx_item_helper import *
from loopchain.protos import loopchain_pb2_grpc
from loopchain.baseservice.module_process import ModuleProcess, ModuleProcessProperties


class PeerThreadStatus(Enum):
    normal = 0
    leader_complained = 1


class _Broadcaster:
    """broadcast class for each channel"""
    # 얘가 실질적으로 "보내는" 역할을 수행하는 애구나. 스케쥴러는 얘에게 던져놓기만 하는 것 같아...

    THREAD_INFO_KEY = "thread_info"
    THREAD_VARIABLE_STUB_TO_SELF_PEER = "stub_to_self_peer"
    THREAD_VARIABLE_PEER_STATUS = "peer_status"

    SELF_PEER_TARGET_KEY = "self_peer_target"
    LEADER_PEER_TARGET_KEY = "leader_peer_target"

    def __init__(self, channel: str, self_target: str=None):
        self.__channel = channel
        self.__self_target = self_target

        self.__audience = {}  # self.__audience[peer_target] = stub_manager
        self.__thread_variables = dict()
        self.__thread_variables[self.THREAD_VARIABLE_PEER_STATUS] = PeerThreadStatus.normal

        if conf.IS_BROADCAST_ASYNC:
            self.__broadcast_run = self.__broadcast_run_async
        else:
            self.__broadcast_run = self.__broadcast_run_sync

        self.__handler_map = {
            BroadcastCommand.CREATE_TX: self.__handler_create_tx,
            BroadcastCommand.CONNECT_TO_LEADER: self.__handler_connect_to_leader,
            BroadcastCommand.SUBSCRIBE: self.__handler_subscribe,
            BroadcastCommand.UNSUBSCRIBE: self.__handler_unsubscribe,
            BroadcastCommand.BROADCAST: self.__handler_broadcast,
            BroadcastCommand.MAKE_SELF_PEER_CONNECTION: self.__handler_connect_to_self_peer,
        }

        self.__broadcast_with_self_target_methods = {
            "AddTx",
            "AddTxList",
            "BroadcastVote"
        }

        self.stored_tx = queue.Queue()

        self.__timer_service = TimerService() # 와 이게 뭔지 설명이 안되어있어서 모호한데, 초기 코드때부터 있었네...

    @property
    def is_running(self):
        return self.__timer_service.is_run()

    def start(self):
        self.__timer_service.start()

    def stop(self):
        if self.__timer_service.is_run():
            self.__timer_service.stop()
            self.__timer_service.wait()

    def handle_command(self, command, params):
        func = self.__handler_map[command]
        func(params)

    def __keep_grpc_connection(self, result, timeout, stub_manager: StubManager):
        return isinstance(result, _Rendezvous) \
               and result.code() in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE) \
               and stub_manager.elapsed_last_succeed_time() < timeout

    def __broadcast_retry_async(self, peer_target, method_name, method_param, retry_times, timeout, stub, result):
        if isinstance(result, _Rendezvous) and result.code() == grpc.StatusCode.OK:
            return
        if isinstance(result, futures.Future) and not result.exception():
            return

        logging.debug(f"try retry to : peer_target({peer_target})\n")
        if retry_times > 0:
            try:
                stub_manager: StubManager = self.__audience[peer_target]
                if stub_manager is None:
                    logging.warning(f"broadcast_thread:__broadcast_retry_async Failed to connect to ({peer_target}).")
                    return
                retry_times -= 1
                is_stub_reuse = stub_manager.stub != stub or self.__keep_grpc_connection(result, timeout, stub_manager)
                self.__call_async_to_target(peer_target, method_name, method_param, is_stub_reuse, retry_times, timeout)
            except KeyError as e:
                logging.debug(f"broadcast_thread:__broadcast_retry_async ({peer_target}) not in audience. ({e})")
        else:
            if isinstance(result, _Rendezvous):
                exception = result.details()
            elif isinstance(result, futures.Future):
                exception = result.exception()

            logging.warning(f"__broadcast_run_async fail({result})\n"
                            f"cause by: {exception}\n"
                            f"peer_target({peer_target})\n"
                            f"method_name({method_name})\n"
                            f"retry_remains({retry_times})\n"
                            f"timeout({timeout})")

    def __call_async_to_target(self, peer_target, method_name, method_param, is_stub_reuse, retry_times, timeout):
        print(f"콜 어씽크 투 타겟 {(peer_target, method_name, method_param, is_stub_reuse, retry_times, timeout)}")
        try:
            stub_manager: StubManager = self.__audience[peer_target]
            print("스텁 매니저. 누구에게 보내는건가: ", stub_manager) # 이래서 씽글턴 ㅡㅡ. 스텁매니저 메모리 주소만 나오네.
            if stub_manager is None:
                logging.debug(f"broadcast_thread:__call_async_to_target Failed to connect to ({peer_target}).")
                return
            call_back_partial = partial(self.__broadcast_retry_async,
                                        peer_target,
                                        method_name,
                                        method_param,
                                        retry_times,
                                        timeout,
                                        stub_manager.stub)
            stub_manager.call_async(method_name=method_name,
                                    message=method_param,
                                    is_stub_reuse=is_stub_reuse,
                                    call_back=call_back_partial,
                                    timeout=timeout)
        except KeyError as e:
            logging.debug(f"broadcast_thread:__call_async_to_target ({peer_target}) not in audience. ({e})")

    def __broadcast_run_async(self, method_name, method_param, retry_times=None, timeout=None):
        """call gRPC interface of audience

        :param method_name: gRPC interface
        :param method_param: gRPC message
        """
        print("브로드캐스트 런 - 어씽크", "method_name:", method_name, "method_param:", method_param)

        if timeout is None:
            timeout = conf.GRPC_TIMEOUT_BROADCAST_RETRY

        retry_times = conf.BROADCAST_RETRY_TIMES if retry_times is None else retry_times
        # logging.debug(f"broadcast({method_name}) async... ({len(self.__audience)})")

        for target in self.__get_broadcast_targets(method_name):
            print(f"target: {target}") # 보낼 피어의 주소가 적혀있다. (7100대 피어.) 피어 갯수대로 반복
            # util.logger.debug(f"method_name({method_name}), peer_target({target})")
            self.__call_async_to_target(target, method_name, method_param, True, retry_times, timeout)

    def __broadcast_run_sync(self, method_name, method_param, retry_times=None, timeout=None):
        """call gRPC interface of audience

        :param method_name: gRPC interface
        :param method_param: gRPC message
        """
        print("브로드캐스트 런 - 씽크")
        # logging.debug(f"broadcast({method_name}) sync... ({len(self.__audience)})")

        if timeout is None:
            timeout = conf.GRPC_TIMEOUT_BROADCAST_RETRY

        retry_times = conf.BROADCAST_RETRY_TIMES if retry_times is None else retry_times

        for target in self.__get_broadcast_targets(method_name):
            try:
                stub_manager: StubManager = self.__audience[target]
                if stub_manager is None:
                    logging.debug(f"broadcast_thread:__broadcast_run_sync Failed to connect to ({target}).")
                    continue

                response = stub_manager.call_in_times(method_name=method_name,
                                                      message=method_param,
                                                      timeout=timeout,
                                                      retry_times=retry_times)
                if response is None:
                    logging.warning(f"broadcast_thread:__broadcast_run_sync fail ({method_name}) "
                                    f"target({target}) ")
            except KeyError as e:
                logging.debug(f"broadcast_thread:__broadcast_run_sync ({target}) not in audience. ({e})")

    def __handler_subscribe(self, audience_target):
        logging.debug("BroadcastThread received subscribe command peer_target: " + str(audience_target))
        if audience_target not in self.__audience:
            stub_manager = StubManager.get_stub_manager_to_server(
                audience_target, loopchain_pb2_grpc.PeerServiceStub,
                time_out_seconds=conf.CONNECTION_RETRY_TIMEOUT_WHEN_INITIAL,
                is_allow_null_stub=True,
                ssl_auth_type=conf.GRPC_SSL_TYPE
            )
            self.__audience[audience_target] = stub_manager

    def __handler_unsubscribe(self, audience_target):
        # logging.debug(f"BroadcastThread received unsubscribe command peer_target({unsubscribe_peer_target})")
        try:
            del self.__audience[audience_target]
        except KeyError:
            logging.warning(f"Already deleted peer: {audience_target}")

    def __handler_broadcast(self, broadcast_param):
        """ 스케쥴 브로드캐스트하면 여기로 오는거지. ㅇㅇ """
        # 근데 왜 굳이 까는거지
        # logging.debug("BroadcastThread received broadcast command")
        broadcast_method_name = broadcast_param[0]
        broadcast_method_param = broadcast_param[1]
        broadcast_method_kwparam = broadcast_param[2]
        # logging.debug("BroadcastThread method name: " + broadcast_method_name)
        # logging.debug("BroadcastThread method param: " + str(broadcast_method_param))
        self.__broadcast_run(broadcast_method_name, broadcast_method_param, **broadcast_method_kwparam)

    def __make_tx_list_message(self):
        print("tx list 메시지 만들기")
        tx_list = []
        tx_list_size = 0
        tx_list_count = 0
        remains = False
        # 이 지점에서 큐에 쌓인 tx가 있으면 브로드캐스트 스케쥴러가 전송하려는 것 같이 보인다. 다 비우는 것이 목적.
        # 용량이 넘치거나, 갯수가 넘치거나 하면  보내고, 아니면 계속 빌 때까지 tx_list에 담아서 몽땅 리턴.
        while not self.stored_tx.empty():
            stored_tx_item = self.stored_tx.get() # tx 큐에서 하나 꺼내오고 - send_tx_IN_timer에서 put 했었음. - TxItem 객체
            tx_list_size += len(stored_tx_item) # 그 tx의 사이즈..? 바이트 길이로 용량을 재는건가
            tx_list_count += 1  # 갯수
            # tx
            if tx_list_size >= conf.MAX_TX_SIZE_IN_BLOCK or tx_list_count >= conf.MAX_TX_COUNT_IN_ADDTX_LIST:
                self.stored_tx.put(stored_tx_item) # 빼 온 것을 다시 넣음
                remains = True
                break # 음.. 뭐지
            tx_list.append(stored_tx_item.get_tx_message())
        # 얘가 보내는 것인가봄. GRPC가..
        message = loopchain_pb2.TxSendList(
            channel=self.__channel,
            tx_list=tx_list
        )

        print(f"메시지 리턴 값: {remains}, {message}") # todo: remains 플래그는 왜 필요한 것인가?
        return remains, message

    def __send_tx_by_timer(self, **kwargs):
        print("샌드 tx BY 타이머 실행됨")
        # util.logger.spam(f"broadcast_scheduler:__send_tx_by_timer")
        if self.__thread_variables[self.THREAD_VARIABLE_PEER_STATUS] == PeerThreadStatus.leader_complained:
            print("샌드 tx BY 타이머 조건: 참")
            logging.warning("Leader is complained your tx just stored in queue by temporally: " # 무슨..말이지?
                            + str(self.stored_tx.qsize()))
        else:
            print("샌드 tx BY 타이머 조건: 거짓")
            # Send single tx for test
            # stored_tx_item = self.stored_tx.get()
            # self.__broadcast_run("AddTx", stored_tx_item.get_tx_message())

            # Send multiple tx
            remains, message = self.__make_tx_list_message()
            self.__broadcast_run("AddTxList", message)
            if remains:
                self.__send_tx_in_timer()

    def __send_tx_in_timer(self, tx_item=None):
        """ in_timer와 by_timer...-_-?... """
        print("샌드 tx IN 타이머 실행됨 ")
        # 명령을 내려놓고, 콜백을 만드는 역할을 하는 것 같고, 실질적으로는 send_tx_by_timer가 보내는 것 같은 느낌이 든다.
        # 그리고는 스케쥴러에 의해 콜백이 호출되는 것 같고.

        # util.logger.spam(f"broadcast_scheduler:__send_tx_in_timer")
        duration = 0
        # create_tx_handler를 거쳐 오면 tx_item이 항상 있는 것 같으네. stored_tx로 처리되는듯.
        if tx_item: # 없을 수도 있는건가..?
            print("tx_item 있음")
            print(self.stored_tx)
            self.stored_tx.put(tx_item) # 생성된 tx(객체)를 큐에 밀어 넣고 ..응? 알아서 돌아가면서 처리하는 애가 따로 있나?;
            print(self.stored_tx)
            duration = conf.SEND_TX_LIST_DURATION

        # 이게 무엇인가?...
        print("조건문")
        print(TimerService.TIMER_KEY_ADD_TX)
        print(self.__timer_service.timer_list)
        if TimerService.TIMER_KEY_ADD_TX not in self.__timer_service.timer_list:
            print("조건문 실행됨. 타이머 서비스에 타이머 등록-_")
            self.__timer_service.add_timer(
                TimerService.TIMER_KEY_ADD_TX,
                Timer(
                    target=TimerService.TIMER_KEY_ADD_TX,
                    duration=duration,
                    callback=self.__send_tx_by_timer,
                    callback_kwargs={}
                )
            )
        else:
            pass
        print("샌드 tx IN 타이머 끝났어")

    def __handler_create_tx(self, create_tx_param):
        """ schedule_job 커맨드에 CREATE_TX하면 여기로 오는건가..? """
        # createTx 명령이 브로드캐스트 스케쥴러에게 들어오면, 여기로 매핑되는 듯 하다. (perform 어쩌구로 인해)
        # logging.debug(f"Broadcast create_tx....")
        print("create_tx 핸들러 동작", create_tx_param)
        try:
            tx_item = TxItem.create_tx_item(create_tx_param, self.__channel)
        except Exception as e:
            logging.warning(f"tx in channel({self.__channel})")
            logging.warning(f"__handler_create_tx: meta({create_tx_param})")
            logging.warning(f"tx dumps fail ({e})")
            return

        # 보내지는 Tx는 객체화되어 다음으로
        print("샌드 tx IN 타이머", tx_item)
        self.__send_tx_in_timer(tx_item)

    def __handler_connect_to_leader(self, connect_to_leader_param):
        # logging.debug("(tx thread) try... connect to leader: " + str(connect_to_leader_param))
        self.__thread_variables[self.LEADER_PEER_TARGET_KEY] = connect_to_leader_param

        # stub_to_self_peer = __thread_variables[self.THREAD_VARIABLE_STUB_TO_SELF_PEER]

        self.__thread_variables[self.THREAD_VARIABLE_PEER_STATUS] = PeerThreadStatus.normal

    def __handler_connect_to_self_peer(self, connect_param):
        # 자신을 생성한 부모 Peer 에 접속하기 위한 stub 을 만든다.
        # pipe 를 통한 return 은 pipe send 와 쌍이 맞지 않은 경우 오류를 발생시킬 수 있다.
        # 안전한 연결을 위하여 부모 프로세스와도 gRPC stub 을 이용하여 통신한다.
        logging.debug("try connect to self peer: " + str(connect_param))

        stub_to_self_peer = StubManager.get_stub_manager_to_server(
            connect_param, loopchain_pb2_grpc.InnerServiceStub,
            time_out_seconds=conf.CONNECTION_RETRY_TIMEOUT_WHEN_INITIAL,
            is_allow_null_stub=True,
            ssl_auth_type=conf.SSLAuthType.none
        )
        self.__thread_variables[self.SELF_PEER_TARGET_KEY] = connect_param
        self.__thread_variables[self.THREAD_VARIABLE_STUB_TO_SELF_PEER] = stub_to_self_peer

    def __get_broadcast_targets(self, method_name):

        peer_targets = list(self.__audience)
        if ObjectManager().rs_service:
            return peer_targets
        else:
            if self.__self_target is not None and method_name not in self.__broadcast_with_self_target_methods:
                peer_targets.remove(self.__self_target)
            return peer_targets


class BroadcastScheduler(metaclass=abc.ABCMeta):
    def __init__(self):
        self.__schedule_listeners = dict()

    @abc.abstractmethod
    def start(self):
        raise NotImplementedError("start function is interface method")

    @abc.abstractmethod
    def stop(self):
        raise NotImplementedError("stop function is interface method")

    @abc.abstractmethod
    def wait(self):
        raise NotImplementedError("stop function is interface method")

    @abc.abstractmethod
    def _put_command(self, command, params, block=False, block_timeout=None):
        raise NotImplementedError("_put_command function is interface method")

    def add_schedule_listener(self, callback, commands: tuple):
        """ """
        print(f"*----add_schedule_listener called")
        print(f"self.__schedule_listners: {self.__schedule_listeners}")
        print(f"  callback_input: {callback}")
        print(f"  commands_input: {commands}")
        if not commands:
            raise ValueError("commands parameter is required")

        # 제일 처음에 섭스, 언섭스가 커맨드쓰로 들어오겠음.
        for cmd in commands:
            callbacks = self.__schedule_listeners.get(cmd) # 각 커맨드마다 스케쥴 리스너: dict 에 []로 키를 호출하면, 없으면 에러가 난다고. get하면 None이 반환되고.
            print(f"cmd: {cmd}, corr_callbacks: {callbacks}")
            if callbacks is None: # 키 값이 없으면...
                callbacks = [] # ??...
                self.__schedule_listeners[cmd] = callbacks # 없으면 걍 아무것도 없이 걍 매핑? subscribe -> []?
            elif callback in callbacks: # 입력받은 콜백값이 커맨드에 대한 값에 이미 매핑되어 있을 경우
                raise ValueError("callback is already in callbacks")
            callbacks.append(callback) # 커맨드에 대한 리스트에 콜백을 집어넣음.
        print(f"after all stuffs-schedule_listeners: {self.__schedule_listeners}") # 콜백 함수는 결국 '큐에 넣기' 함수 그 자체를 등록한거네.
        # 그래서 이게 뭐 한걸까..
        # 섭과 언섭 등록 후, 뭔가 무수히 이 메서드가 쓰이는데, .. 왜 계속 subscribe 하는거지? 그러다가 또 안해요.

    def remove_schedule_listener(self, callback):
        removed = False
        for cmd in list(self.__schedule_listeners):
            callbacks = self.__schedule_listeners[cmd]
            try:
                callbacks.remove(callback)
                removed = True
                if len(callbacks):
                    del self.__schedule_listeners[cmd]
            except ValueError:
                pass
        if not removed:
            raise ValueError("callback is not in overserver callbacks")

    def __perform_schedule_listener(self, command, params):
        """ 이게 실질적인 콜백 루프인가..? 그래야 할 것 같은데.. """

        print(f"퍼폼 스케쥴 리스너에서 - self.__schedule_listeners: {self.__schedule_listeners}") # put_command에서 넣은 (createTx, (직렬화tx 및 그 버저너)) 는 여기에 없어요..
        print(f"command_input: {command}, params_input: {params}") #(createTx, (직렬화tx 및 그 버저너)) 는 잘 들어왔고. (그대로 인자에 건네줬으니까..)
        callbacks = self.__schedule_listeners.get(command) # 리스너에 등록된 것이 없으니 그냥 패스.. 으으으음.....
        print(f"callbacks: {callbacks}")
        if callbacks: # 해당하는 콜백이 없으면 그냥 빠져나가네.
            for cb in callbacks: # 아마도 등록한 큐에 넣기 함수에 이걸 집어넣겠지.
                print(f"cb: {cb}")
                cb(command, params)
        print(f"퍼폼 스케쥴 리스너 나가기 전 - self.__schedule_listeners: {self.__schedule_listeners}") # 여기도 비어있는디. 그럼 왜 한거야!

    def schedule_job(self, command, params, block=False, block_timeout=None):
        """ 얘를 파악해야만, 만들어진 tx가 어떻게 다른 피어에게 전송될 지 알 것 같다. """
        # todo: 현재로서는 얘가 다른 피어에게 tx를 날리는 것을 포착하지 못했음.. 흠;
        print("---schedule_job called. put_command calling") # tx 생성 후 얘를 또 호출함?.. 근데 어디선가 브로드캐스트 라는 커맨드를 주었어!!!!!
        print(f"self.__schedule_listeners: {self.__schedule_listeners}") # 제일 처음에는 비어있는게 당연하고.. 준비 다 되고 tx 받았을 경우에도 텅 비어있고.. (어쩌면 이미 타임아웃을 짧게 걸어서 다 비운 것일지도)
        print(f"command_input: {command}, params_input: {params}") # tx를 보내면, 파라미터로 (tx전체가 담긴 Transaction 객체 (시리얼라이즈된 것으로 보임), tx버저너 객체) 를 준다.
        self._put_command(command, params, block=block, block_timeout=block_timeout) # 이건 프로세스.큐에다가 (커맨드, 파라미터) 튜플째로 넣어버리는 것 같음.
        # 이 지점에서 큐에 잘 넣은.. 게 아니라, 큐에다가만 넣고.. 리스너에 등록은 안한거같은데
        print(f"perform_schedule_listener calling")
        self.__perform_schedule_listener(command, params) # 이게 정확히.. 어떤 거지?;
        print(f"스케쥴 잡 나가기 전의 self.__scHedule_listeners: {self.__schedule_listeners}") # 제일 처음에는 비어있는게 당연하고..

    def schedule_broadcast(self, method_name, method_param, *, retry_times=None, timeout=None):
        # tx를 받은 시점인지, 만든 이후인지 언젠진 모르겠으나, 얘가 브로드캐스트 스케쥴러에게 브로드캐스트 명령을 준다.
        print("브로드캐스트 스케쥴러의  방송 스케쥴. schdule_broadcast 호출됨") # 누가 여기를 호출하는지 봐야 한다!
        print(f"method_name: {method_name}, method_param: {method_param}, retry_times & timeout: {(retry_times, timeout)}")
        kwargs = {}
        if retry_times is not None:
            kwargs['retry_times'] = retry_times
        if timeout is not None:
            kwargs['timeout'] = timeout
        print(f"kwargs: {kwargs}")

        # 이것으로 인해 브로드캐스트 명령이 떨어진다.
        self.schedule_job(BroadcastCommand.BROADCAST, (method_name, method_param, kwargs))


class _BroadcastThread(CommonThread):
    def __init__(self, channel: str, self_target: str=None):
        self.broadcast_queue = queue.PriorityQueue()
        self.__broadcast_pool = futures.ThreadPoolExecutor(conf.MAX_BROADCAST_WORKERS, "BroadcastThread")
        self.__broadcaster = _Broadcaster(channel, self_target)

    def stop(self):
        super().stop()
        self.broadcast_queue.put((None, None, None, None))
        self.__broadcast_pool.shutdown(False)

    def run(self, event: threading.Event):
        event.set()
        self.__broadcaster.start()

        def _callback(curr_future: futures.Future, executor_future: futures.Future):
            if executor_future.exception():
                curr_future.set_exception(executor_future.exception())
                logging.error(executor_future.exception())
            else:
                curr_future.set_result(executor_future.result())

        while self.is_run():
            priority, command, params, future = self.broadcast_queue.get()
            if command is None:
                break

            return_future = self.__broadcast_pool.submit(self.__broadcaster.handle_command, command, params)
            if future is not None:
                return_future.add_done_callback(partial(_callback, future))


class _BroadcastSchedulerThread(BroadcastScheduler):
    def __init__(self, channel: str, self_target: str=None):
        super().__init__()

        self.__broadcast_thread = _BroadcastThread(channel, self_target=self_target)

    def start(self):
        self.__broadcast_thread.start()

    def stop(self):
        self.__broadcast_thread.stop()

    def wait(self):
        self.__broadcast_thread.wait()

    def _put_command(self, command, params, block=False, block_timeout=None):
        """ 뭔가 콜백을 만드는듯한 냄새가 나는데 """
        if command == BroadcastCommand.CREATE_TX:
            priority = (10, time.time())
        elif isinstance(params, tuple) and params[0] == "AddTx":
            priority = (10, time.time())
        else:
            priority = (0, time.time())

        # 그럼 tx가 오면, 한번도 Block 파라미터를 안준 거 보면 모두 false겠군.======= 아.. 이게 블락이 막힌다는 의미의 블락이었음 --.. 아닌가..;
        # 싱크로 하겠냐 아니냐를 묻는 질문이었네.
        future = futures.Future() if block else None    # 블락이 True면 future를 만들고, 아니면 None..??;;
        self.__broadcast_thread.broadcast_queue.put((priority, command, params, future))
        if future is not None:
            future.result(block_timeout)


class _BroadcastSchedulerMp(BroadcastScheduler):
    """ 얘가 상속받아서 돌아가는거였네. """

    def __init__(self, channel: str, self_target: str=None):
        super().__init__()

        self.__channel = channel
        self.__self_target = self_target

        self.__process = ModuleProcess()

        self.__broadcast_queue = self.__process.Queue()
        self.__broadcast_queue.cancel_join_thread()

    @staticmethod
    def _main(broadcast_queue: mp.Queue, channel: str, self_target: str, properties: ModuleProcessProperties=None):
        if properties is not None:
            ModuleProcess.load_properties(properties, f"{channel}_broadcast")

        logging.info(f"BroadcastScheduler process({channel}) start")

        broadcast_queue.cancel_join_thread()

        broadcaster = _Broadcaster(channel, self_target)
        broadcaster.start()

        original_sigterm_handler = signal.getsignal(signal.SIGTERM)
        original_sigint_handler = signal.getsignal(signal.SIGINT)

        def _signal_handler(signal_num, frame):
            signal.signal(signal.SIGTERM, original_sigterm_handler)
            signal.signal(signal.SIGINT, original_sigint_handler)
            logging.error(f"BroadcastScheduler process({channel}) has been received signal({signal_num})")
            broadcast_queue.put((None, None))
            broadcaster.stop()

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

        while True:
            command, params = broadcast_queue.get()
            if not broadcaster.is_running or command is None:
                break
            broadcaster.handle_command(command, params)

        while not broadcast_queue.empty():
            broadcast_queue.get()

        logging.info(f"BroadcastScheduler process({channel}) end")

    def start(self):
        def crash_callback_in_join_thread(process: ModuleProcess):
            os.kill(os.getpid(), signal.SIGTERM)

        args = (self.__broadcast_queue, self.__channel, self.__self_target)
        self.__process.start(target=_BroadcastSchedulerMp._main,
                             args=args,
                             crash_callback_in_join_thread=crash_callback_in_join_thread)

    def stop(self):
        logging.info(f"Terminate BroadcastScheduler process({self})")
        self.__process.terminate()

    def wait(self):
        self.__process.join()

    def _put_command(self, command, params, block=False, block_timeout=None):
        print(f"scheduler -MP: put_command called") # tx를 받으면, 그러니까 서브 프로세서의 풋 커맨드는 이제서야 실행되네
        print(f"inputs (command, params): {(command, params)}")  # tx를 보내면, 파라미터로 (tx전체가 담긴 Transaction 객체 (시리얼라이즈된 것으로 보임), tx버저너 객체) 를 준다.
        # print(f"self.__schedule_listeners: {self.__schedule_listeners}") # 이 시점에서 한번 봐 보자. 무엇이 있나 - 이거 보면 프로세스가 꼬이나..?
        # print(f"self.__broadcast_queue: {self.__broadcast_queue}")
        self.__broadcast_queue.put((command, params)) # 받은 커맨드 (createTx, (직렬화tx 및 그 버저너))를 큐에..넣어.?


class BroadcastSchedulerFactory:
    @staticmethod
    def new(channel: str, self_target: str=None, is_multiprocessing: bool=None) -> BroadcastScheduler:
        if is_multiprocessing is None:
            is_multiprocessing = conf.IS_BROADCAST_MULTIPROCESSING

        if is_multiprocessing:
            # print(f" 멀티 프로세스 분기! - 브로드캐스트 스케쥴러")
            return _BroadcastSchedulerMp(channel, self_target=self_target)
        else:
            return _BroadcastSchedulerThread(channel, self_target=self_target)
