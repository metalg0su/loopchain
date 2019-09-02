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
"""Test timer service"""

import asyncio
import datetime
import functools
import time

import pytest
from freezegun import freeze_time

from loopchain.baseservice.timer_new import Timer as TimerNew
from loopchain.baseservice.timer_service import Timer, TimerService, OffType

attr_start_time = "_Timer__start_time"
timer_key = "test_timer_key"
duration = 0.1
test_args = {"arg1": "test_value1", "arg2": "test_value2"}


@pytest.fixture
def timer_service():
    ts = TimerService()
    ts.start()
    yield ts

    ts.stop()
    # time.sleep(0.5)  # How to wait for closing TimerService loop thread?


class TestTimer:
    def test_init(self):
        timer = Timer()
        assert timer

    def test_is_timeout(self):
        timer = Timer(duration=duration)
        assert not timer.is_timeout()

        with freeze_time(datetime.datetime.now() + datetime.timedelta(seconds=duration)):
            assert timer.is_timeout()

    def test_reset_timer(self):
        timer = Timer()
        start_time = getattr(timer, attr_start_time)
        timer.reset()
        new_start_time = getattr(timer, attr_start_time)

        assert new_start_time > start_time

    def test_remain_time(self):
        timer = Timer(duration=duration)

        remain_time = timer.remain_time()
        assert remain_time < duration

        with freeze_time(datetime.datetime.now() + datetime.timedelta(seconds=duration)):
            remain_time = timer.remain_time()

            assert remain_time == 0

    def test_timer_on(self):
        """Timer.on do nothing!!!"""
        timer = Timer()
        timer.on()

    def test_timer_off_invokes_blocking_callback(self, mocker):
        mock_timer_callback = mocker.MagicMock()
        # Will raise BaseException, due to the very top event loop catches Exception.
        mock_timer_callback.side_effect = BaseException("Call is back!!")
        timer = Timer(callback=mock_timer_callback, callback_kwargs=test_args)
        assert not mock_timer_callback.called

        with pytest.raises(BaseException):
            timer.off(OffType.time_out)

        assert mock_timer_callback.called

    def test_timer_off_invokes_coro_callback(self):
        async def coro_callback(**kwargs):
            print("Inside coro -- Called!", kwargs)
            raise BaseException(f"Call is back with: {kwargs}")

        loop = asyncio.get_event_loop()
        timer = Timer(callback=coro_callback, callback_kwargs=test_args)

        with pytest.raises(BaseException):
            timer.off(OffType.time_out)
            loop.run_until_complete(asyncio.sleep(0))  # Give a control chance to coro_callback

    def test_timer_off_and_exception_in_blocking_callback_does_not_break_process(self, mocker):
        mock_timer_callback = mocker.MagicMock()
        mock_timer_callback.side_effect = RuntimeError("Call is back!!")
        timer = Timer(callback=mock_timer_callback, callback_kwargs=test_args)
        assert not mock_timer_callback.called

        timer.off(OffType.time_out)

        assert mock_timer_callback.called

    def test_timer_off_and_exception_in_coro_callback_does_not_break_process(self):
        async def coro_callback(**kwargs):
            print("Inside coro -- Called!", kwargs)
            raise RuntimeError(f"Call is back with: {kwargs}")

        loop = asyncio.get_event_loop()
        timer = Timer(callback=coro_callback, callback_kwargs=test_args)

        timer.off(OffType.time_out)
        loop.run_until_complete(asyncio.sleep(0))  # Give a control chance to coro_callback


class TestTimerService:
    def test_add_and_remove_timer(self, timer_service: TimerService):
        """Add and remove timer to TimerService"""
        timer = Timer(duration=duration)

        # add_timer
        timer_service.add_timer(timer_key, timer)
        assert len(timer_service.timer_list) == 1
        assert timer_service.get_timer(timer_key)

        # remove_timer
        timer_service.remove_timer(timer_key)
        assert len(timer_service.timer_list) == 0
        assert not timer_service.get_timer(timer_key)

    def test_add_timer_convenient(self, timer_service: TimerService):
        assert len(timer_service.timer_list) == 0
        timer_service.add_timer_convenient(timer_key, duration)

        assert len(timer_service.timer_list) == 1
        assert timer_service.get_timer(timer_key)

    def test_get_timer_with_invalid_key(self, timer_service: TimerService):
        """Check with invalid key to TimerService.get_timer, Expected None"""
        timer = Timer()
        timer_service.add_timer("supplied_this_key", timer)
        assert not timer_service.get_timer("not_found_key")

    @pytest.mark.skip
    def test_reset_timer(self, timer_service: TimerService):
        timer = Timer(duration=duration)
        timer_service.add_timer(timer_key, timer)
        initial_time: datetime.datetime = datetime.datetime.now()
        assert not timer.is_timeout()

        # TIMER TICKS: After half of duration_sec passed
        reset_after_sec = int(duration)/2
        with freeze_time(initial_time + datetime.timedelta(seconds=reset_after_sec)):
            print("FIRST!!")
            print("time.time(): ", time.time())
            assert not timer.is_timeout()
            assert reset_after_sec < timer.remain_time() <= duration

            timer_service.reset_timer(timer_key)

        print("!!SECOND!!")
        # RESET TIMER: After duration_sec passed
        with freeze_time(initial_time + datetime.timedelta(seconds=duration)):
            print("SECOND!!")
            print("time.time(): ", time.time())
            assert 0 < timer.remain_time() <= reset_after_sec
            assert not timer.is_timeout()

        # TIMER OUT: After duration_sec and its half passed
        with freeze_time(initial_time + datetime.timedelta(seconds=reset_after_sec) + datetime.timedelta(seconds=duration)):
            print("THIRD!!")
            print("time.time(): ", time.time())
            assert timer.is_timeout()

    def test_stop_timer_forced_and_no_timer_in_queue(self, timer_service):
        timer = Timer(duration=duration)
        timer_service.add_timer(timer_key, timer)
        assert timer_service.get_timer(timer_key)

        timer_service.stop_timer(timer_key, OffType.normal)
        assert not timer_service.get_timer(timer_key)


# ----------
def blocking_callback(**kwargs):
    out = f"blocking callback called with: {kwargs}"

    if kwargs.get("exc"):
        raise RuntimeError(out)
    else:
        print(out)


async def coro_callback(**kwargs):
    out = f"coroutine_callback called with: {kwargs}"

    if kwargs.get("exc"):
        raise RuntimeError(out)
    else:
        print(out)


@pytest.fixture
def mock_maker(callback_func, mocker, monkeypatch):
    def _mock_maker(cb, mocker_fixture, monkeypatch_fixture):
        callback_func_name = f"{__name__}.{cb.__name__}"
        print("callback_func_name: ", callback_func_name, "is coro?: ", asyncio.iscoroutinefunction(cb))

        callback = mocker_fixture.MagicMock(callback_func_name, wraps=cb)
        if asyncio.iscoroutinefunction(cb):
            def mock_iscoroutinefunction(placeholder):
                return True
            monkeypatch_fixture.setattr(asyncio, "iscoroutinefunction", mock_iscoroutinefunction)

        return callback

    return functools.partial(_mock_maker, mocker_fixture=mocker, monkeypatch_fixture=monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("callback_func", [blocking_callback, coro_callback])
class TestTimerNew:
    async def test_multiple_timer_runs_in_one_process(self, callback_func):
        print()
        print("============")
        timer1 = TimerNew(callback=callback_func, timeout=2, callback_key="timer1")
        print("timer1 init")
        timer2 = TimerNew(callback=callback_func, timeout=3, callback_key="timer2")
        print("timer2 init")

        start_time = time.time()
        for s in range(4):
            curr_time = time.time() - start_time
            print(">>", s, "초 경과====", curr_time if curr_time > 1 else 0)
            if s == 1:
                print("time to kill...")
                await timer1.stop()
            await asyncio.sleep(1)
        print("sleep complete")
        print(">>", time.time() - start_time, "초 경과====")
        print("============")

    async def test_start(self, callback_func, mock_maker):
        callback_func_kwargs = {"callback_kwarg1": "test_value"}
        mocked_callback = mock_maker(callback_func)
        timer = TimerNew(callback=mocked_callback, timeout=duration, **callback_func_kwargs)

        assert timer.is_running()
        assert not mocked_callback.called

        await asyncio.sleep(duration + 0.1)
        assert not timer.is_running()

    async def test_reset(self, callback_func, mock_maker):
        timeout = 0.5
        callback_func_kwargs = {"callback_kwarg1": "test_value"}
        mocked_callback = mock_maker(callback_func)

        print("========================")
        print("> initial")
        timer = TimerNew(callback=mocked_callback, timeout=timeout, **callback_func_kwargs)
        assert timer.is_running()
        assert not mocked_callback.called

        print("> before timeout")
        await asyncio.sleep(timeout - 0.1)
        assert timer.is_running()
        assert not mocked_callback.called

        print("> reset timer")
        await timer.reset()

        print("> before timeout (timeout - 0.1)")
        await asyncio.sleep(timeout - 0.1)
        assert timer.is_running()
        assert not mocked_callback.called

        print("> timeout")
        await asyncio.sleep(0.2)
        assert not timer.is_running()
        assert mocked_callback.called
        print("========================")
