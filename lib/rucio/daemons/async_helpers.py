# -*- coding: utf-8 -*-
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Union
import asyncio


def run_in_new_event_loop(coro):
    # TODO: remove this function and use asyncio.run directly after deprecating python < 3.9

    # For python 3.7+:
    if hasattr(asyncio, 'run'):
        return asyncio.run(coro)

    # For python 3.6:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(coro)
    finally:
        asyncio.set_event_loop(None)
        loop.close()


class MultithreadedAsyncQueue(asyncio.Queue):
    """
    Wrap asyncio.Queue to allow it being used in multiple threads.
    All operations which modify the queues' internal data structure will
    be performed in the thread that created the queue.
    """

    def __init__(self, *args, **kwargs):
        self._main_loop = asyncio.get_event_loop()
        super().__init__(*args, **kwargs)

    @property
    def unfinished_tasks(self):
        return getattr(self, '_unfinished_tasks')

    def get_nowait_threadsafe(self):
        self._main_loop.call_soon_threadsafe(self.task_done)

    async def put_threadsafe(self, item: Any):
        return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(self.put(item), loop=self._main_loop))

    async def get_threadsafe(self) -> Any:
        return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(self.get(), loop=self._main_loop))

    async def join_threadsafe(self):
        return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(self.join(), loop=self._main_loop))

    def task_done_threadsafe(self):
        self._main_loop.call_soon_threadsafe(self.task_done)


class MultithreadedAsyncEvent(asyncio.Event):
    """
    Wrap asyncio.Event to allow waiting on the event from multiple threads.
    """

    def __init__(self, *args, **kwargs):
        self._main_loop = asyncio.get_event_loop()
        super().__init__(*args, **kwargs)

    async def wait_threadsafe(self):
        return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(self.wait(), loop=self._main_loop))

    def set_threadsafe(self):
        self._main_loop.call_soon_threadsafe(self.set)


async def cancel_and_wait(fut: Union[asyncio.Future, None]) -> None:
    """
    Cancel a future/task and await for it to cancel.
    If the fut is already done() this is a no-op
    If everything goes well this returns None.

    If this coroutine is cancelled, we wait for the passed in argument to cancel
    but we will raise the CancelledError as per Cancellation Contract, Unless the task
    doesn't cancel correctly then we could raise other exceptions.

    If the task raises an exception during cancellation we re-raise it
    """
    if fut is None:
        return

    if not fut.done():
        fut.cancel()

    exc = None
    while True:
        shielded = asyncio.shield(fut)
        try:
            await asyncio.wait([shielded])
        except asyncio.CancelledError as ex:
            exc = ex
        finally:
            # Insure we handle the exception/value that may exist on the shielded task
            # This will prevent errors logged to the asyncio logger
            if shielded.done() and not shielded.cancelled() and not shielded.exception():
                shielded.result()
        if shielded.done():
            break

    if fut.cancelled():
        if exc is None:
            return
        # we were cancelled also so honor the contract
        raise exc from None

    # Some exception thrown during cancellation
    ex = fut.exception()
    if ex is not None:
        raise ex from None
