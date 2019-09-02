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
import time

import pytest
from freezegun import freeze_time

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
