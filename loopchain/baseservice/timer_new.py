import asyncio
from functools import partial


# NOTE that asyncio.create_task method is added at python 3.7!
class Timer:
    def __init__(self, callback, timeout, **kwargs):
        self._loop = asyncio.get_event_loop()
        self._callback = partial(callback, **kwargs)
        self._timeout = timeout
        self._task = None

        self.start()

    def start(self):
        print("Timer started!")
        self._task = asyncio.create_task(self._run())

    async def _run(self):
        """Manage callback func like coroutine, even if blocking func"""
        await asyncio.sleep(self._timeout)

        if asyncio.iscoroutinefunction(self._callback.func):
            print("callback is coro")
            await self._callback()
        else:
            print("callback is func")
            await self._loop.run_in_executor(None, self._callback)  # TODO: 이게 실행되고 있을 땐 어떻게 취소하지??

    async def stop(self):
        """Stop timer"""
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            print("Timer stopped!")

    def is_running(self) -> bool:
        print("self_task: ", self._task)
        return not self._task.done()

    async def reset(self):
        if self.is_running():
            await self.stop()
        self.start()
