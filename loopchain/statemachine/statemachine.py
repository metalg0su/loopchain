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
from transitions import Machine

import loopchain.utils as util


class ExceptionNullStatesInMachine(Exception):
    pass


class ExceptionNullInitState(Exception):
    pass


# 채널 SM의 전신
class StateMachine(object):
    # 왜 이게 시작하자마자 init이 되어버리냐..?;
    def __init__(self, arg):
        print("StateMachine 이닛")
        self.arg = arg # 이게 'Channel State Machine'이 되는건가
        util.logger.spam(f"arg is {self.arg}")

    # 채널 스테이트 머신을 호출하면 여기의 영향을 받나봄
    def __call__(self, cls):
        print("StateMachine 콜")
        class Wrapped(cls):
            attributes = self.arg # 스테이트 머신을 여러개 만들려고 했었나..?? 아니면 실제로 여러개인가?...

            def __init__(self, *cls_args): # 호출할 때 사용된 args로...
                print("StateMachine 래퍼 클래스 이닛")
                # 기본적으로 변신 가능한 상태를 정의해주고 있는가? 이게 없으면 퇴짜를 놓는군.
                if not hasattr(cls, 'states') or not cls.states: # hasattr랑 cls.states랑 뭐가 다른거지?
                    raise ExceptionNullStatesInMachine

                # 초기 상태를 정의하고 있는가?
                if not hasattr(cls, 'init_state') or not cls.init_state:
                    raise ExceptionNullInitState

                util.logger.spam(f"Wrapped __init__ called")
                util.logger.spam(f"cls_args is {cls_args}")
                # self.name = "superman"

                cls.machine = Machine(model=self, states=cls.states, initial=cls.init_state,
                                      ignore_invalid_triggers=True)

                print("래퍼 안에서 이닛 시작 전")
                cls.__init__(cls, *cls_args) # 원래 클래스의 init을 시작하는 것 같은데...
                print("래퍼 안에서 이닛 시작 후")

                # 이제 채널스테이트머신이 가지고 있는 모든 멤버들을 조사하는 것 같군.
                for attr_name in dir(cls):
                    print(attr_name)
                    attr = getattr(cls, attr_name, None)
                    print(attr)
                    if not attr:
                        continue

                    info_dict = getattr(attr, "_info_dict_", None)
                    print(info_dict)
                    if not info_dict:
                        continue

                    self.machine.add_transition(attr.__name__, **info_dict)
                    print(f"================ 매드 트랜지션: {info_dict} ")
        print("리턴 래퍼 클래스 ")
        return Wrapped


def transition(**kwargs_):
    # 이걸로 추가해 줄 키워드만 골라내나보군. _info_dict_ 라는 걸 이용해서
    def _transaction(func):
        func._info_dict_ = kwargs_
        return func

    return _transaction
