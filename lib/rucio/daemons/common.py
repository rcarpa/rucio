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

import asyncio
import contextlib
import datetime
import functools
import hashlib
import inspect
import logging
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, AsyncIterator, Awaitable, Callable, Generator, Generic, Iterator, List, Optional, Tuple, TypeVar, Union

from rucio.common.logging import formatted_logger
from rucio.common.utils import PriorityQueue
from rucio.core import heartbeat as heartbeat_core
from rucio.core.monitor import MetricManager
from rucio.daemons.async_helpers import MultithreadedAsyncEvent, MultithreadedAsyncQueue, run_in_new_event_loop, cancel_and_wait

T = TypeVar('T')
METRICS = MetricManager(module=__name__)


class HeartbeatHandler:
    """
    Simple contextmanager which sets a heartbeat and associated logger on entry and cleans up the heartbeat on exit.
    """

    def __init__(self, executable: str, renewal_interval: int, logger_prefix: Optional[str] = None):
        """
        :param executable: the executable name which will be set in heartbeats
        :param renewal_interval: the interval at which the heartbeat will be renewed in the database.
        Calls to live() in-between intervals will re-use the locally cached heartbeat.
        :param logger_prefix: the prefix to be prepended to all log messages
        """
        self.executable = executable
        self.renewal_interval = renewal_interval
        self.older_than = renewal_interval * 10 if renewal_interval and renewal_interval > 0 else None  # 10 was chosen without any particular reason
        self.logger_prefix = logger_prefix or executable

        self.hostname = socket.getfqdn()
        self.pid = os.getpid()
        self.hb_thread = threading.current_thread()
        self.logger_id = hashlib.sha1(f'{self.hostname}:{self.pid}:{self.hb_thread}'.encode('utf-8')).hexdigest()[:7]

        self.logger = logging.log
        self.last_heart_beat = None
        self.last_time = None
        self.last_payload = None

    def __enter__(self):
        heartbeat_core.sanity_check(executable=self.executable, hostname=self.hostname)
        self.live()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.last_heart_beat:
            heartbeat_core.die(self.executable, self.hostname, self.pid, self.hb_thread)
            if self.logger:
                self.logger(logging.INFO, 'Heartbeat cleaned up')

    def live(self, force_renew: bool = False, payload: Optional[str] = None):
        """
        :return: a tuple: <the number of the current worker>, <total number of workers>, <decorated logger>
        """
        if force_renew \
                or not self.last_time \
                or not self.last_heart_beat \
                or self.last_time < datetime.datetime.now() - datetime.timedelta(seconds=self.renewal_interval) \
                or self.last_payload != payload:
            if self.older_than:
                self.last_heart_beat = heartbeat_core.live(self.executable, self.hostname, self.pid, self.hb_thread, payload=payload, older_than=self.older_than)
            else:
                self.last_heart_beat = heartbeat_core.live(self.executable, self.hostname, self.pid, self.hb_thread, payload=payload)

            prefix = '%s[%s:%i/%i]: ' % (self.logger_prefix, self.logger_id, self.last_heart_beat['assign_thread'], self.last_heart_beat['nr_threads'])
            self.logger = formatted_logger(logging.log, prefix + '%s')

            if not self.last_time:
                self.logger(logging.DEBUG, 'First heartbeat set')
            else:
                self.logger(logging.DEBUG, 'Heartbeat renewed')
            self.last_time = datetime.datetime.now()
            self.last_payload = payload

        return self.last_heart_beat['assign_thread'], self.last_heart_beat['nr_threads'], self.logger


def _activity_looper(
        once: bool,
        sleep_time: int,
        activities: Optional[List[str]],
        logger,
) -> Generator[Tuple[str, float], Tuple[float, bool], None]:
    """
    Generator which loops (either once, or indefinitely) over all activities while ensuring that `sleep_time`
    passes between handling twice the same activity.

    Returns an activity and how much time the calling context must sleep before handling that activity
    and expects to get in return the time when the activity started to be executed and whether next
    execution must be immediate.
    """

    # For each activity, the priority queue will keep the next absolute time when that
    # activity must be handled.
    activity_next_exe_time = PriorityQueue()

    # On startup, we schedule to immediately handle all activities.
    now = time.time()
    for activity in activities or [None]:
        activity_next_exe_time[activity] = now

    while activity_next_exe_time:
        activity = activity_next_exe_time.top()
        desired_exe_time = activity_next_exe_time[activity]

        if once:
            time_to_sleep = 0
            activity_next_exe_time.pop()
        else:
            time_to_sleep = desired_exe_time - time.time()

        if time_to_sleep > 0:
            if activity:
                logger(logging.DEBUG, 'Switching to activity %s and sleeping %s seconds', activity, time_to_sleep)
            else:
                logger(logging.DEBUG, 'Sleeping %s seconds', time_to_sleep)
        else:
            if activity:
                logger(logging.DEBUG, 'Switching to activity %s', activity)
            else:
                logger(logging.DEBUG, 'Starting next iteration')

        # The calling context notifies us when the activity actually got handled. And if sleeping is desired.
        actual_exe_time, must_sleep = yield activity, time_to_sleep

        if not once:
            if must_sleep:
                time_diff = time.time() - actual_exe_time
                time_to_sleep = max(1, sleep_time - time_diff)
                activity_next_exe_time[activity] = time.time() + time_to_sleep
            else:
                activity_next_exe_time[activity] = time.time() + 1


def db_workqueue(
        once: bool,
        graceful_stop: threading.Event,
        executable: str,
        logger_prefix: str,
        partition_wait_time: int,
        sleep_time: int,
        activities: Optional[List[str]] = None,
):
    """
    Used to wrap a classical or an async function for interacting with the database as a work queue: i.e. to select
    a set of rows and perform some work on those rows while ensuring that two instances running in parallel don't
    work on the same set of rows. The last condition is ensured by using heartbeats to keep track of currently
    active workers.

    :param once: Whether to stop after one iteration
    :param graceful_stop: the threading.Event() object used for graceful stop of the daemon
    :param executable: the name of the executable used for hearbeats
    :param logger_prefix: the prefix to be prepended to all log messages
    :param partition_wait_time: time to wait for database partition rebalancing before starting the actual daemon loop
    :param sleep_time: time to sleep between the iterations of the daemon
    :param activities: optional list of activities on which to work. The run_once_fnc will be called on activities one by one.
    """

    def _decorate_async(run_once_fnc: Callable[..., Awaitable[Tuple[bool, T]]]) -> Callable[[], AsyncIterator[T]]:

        @functools.wraps(run_once_fnc)
        async def _generator():
            with HeartbeatHandler(executable=executable, renewal_interval=sleep_time - 1, logger_prefix=logger_prefix) as heartbeat_handler:
                logger = heartbeat_handler.logger
                logger(logging.INFO, 'started')

                if partition_wait_time:
                    await asyncio.sleep(partition_wait_time)
                    _, _, logger = heartbeat_handler.live(force_renew=True)

                activity_loop = _activity_looper(once=once, sleep_time=sleep_time, activities=activities, logger=logger)
                activity, time_to_sleep = next(activity_loop, (None, None))
                while time_to_sleep is not None:
                    _, _, logger = heartbeat_handler.live()

                    if time_to_sleep > 0:
                        await asyncio.sleep(time_to_sleep)

                    must_sleep = True
                    start_time = time.time()
                    try:
                        must_sleep, ret_value = await run_once_fnc(heartbeat_handler=heartbeat_handler, activity=activity)
                        yield ret_value
                    except Exception as e:
                        METRICS.counter('exceptions.{exception}').labels(exception=e.__class__.__name__).inc()
                        logger(logging.CRITICAL, "Exception", exc_info=True)
                        if once:
                            raise

                    try:
                        activity, time_to_sleep = activity_loop.send((start_time, must_sleep))
                    except StopIteration:
                        break

        return _generator

    def _decorate_sync(run_once_fnc: Callable[..., Union[None, bool, Tuple[bool, T]]]) -> Callable[[], Iterator[Union[T, None]]]:

        @functools.wraps(run_once_fnc)
        def _generator():

            with HeartbeatHandler(executable=executable, renewal_interval=sleep_time - 1, logger_prefix=logger_prefix) as heartbeat_handler:
                logger = heartbeat_handler.logger
                logger(logging.INFO, 'started')

                if partition_wait_time:
                    graceful_stop.wait(partition_wait_time)
                    _, _, logger = heartbeat_handler.live(force_renew=True)

                activity_loop = _activity_looper(once=once, sleep_time=sleep_time, activities=activities, logger=logger)
                activity, time_to_sleep = next(activity_loop, (None, None))
                while time_to_sleep is not None:
                    if graceful_stop.is_set():
                        break

                    if time_to_sleep > 0:
                        graceful_stop.wait(time_to_sleep)

                    _, _, logger = heartbeat_handler.live()

                    must_sleep = True
                    start_time = time.time()
                    try:
                        result = run_once_fnc(heartbeat_handler=heartbeat_handler, activity=activity)

                        # Handle return values already existing in the code
                        # TODO: update all existing daemons to always explicitly return (must_sleep, ret_value)
                        if result is None:
                            must_sleep = True
                            ret_value = None
                        elif isinstance(result, bool):
                            must_sleep = result
                            ret_value = None
                        else:
                            must_sleep, ret_value = result

                        yield ret_value
                    except Exception as e:
                        METRICS.counter('exceptions.{exception}').labels(exception=e.__class__.__name__).inc()
                        logger(logging.CRITICAL, "Exception", exc_info=True)
                        if once:
                            raise

                    try:
                        activity, time_to_sleep = activity_loop.send((start_time, must_sleep))
                    except StopIteration:
                        break

                if not once:
                    logger(logging.INFO, 'Graceful stop requested')

        return _generator

    def _decorate(run_once_fnc):
        if inspect.iscoroutinefunction(run_once_fnc):
            return _decorate_async(run_once_fnc)
        else:
            return _decorate_sync(run_once_fnc)

    return _decorate


def run_daemon(
        once: bool,
        graceful_stop: threading.Event,
        executable: str,
        logger_prefix: str,
        partition_wait_time: int,
        sleep_time: int,
        run_once_fnc: Callable[..., Union[None, bool, Tuple[bool, Any]]],
        activities: Optional[List[str]] = None):
    """
    Run the daemon loop and call the function run_once_fnc at each iteration
    """

    daemon = db_workqueue(
        once=once,
        graceful_stop=graceful_stop,
        executable=executable,
        logger_prefix=logger_prefix,
        partition_wait_time=partition_wait_time,
        sleep_time=sleep_time,
        activities=activities,
    )(run_once_fnc)

    for _ in daemon():
        pass


class ProducerConsumerDaemon(Generic[T]):
    """
    Daemon which connects N producers with M consumers via a queue.
    """

    def __init__(self, producers, consumers, multithreaded=False):
        self.producers = producers
        self.consumers = consumers
        self.multithreaded = multithreaded

        self.queue = MultithreadedAsyncQueue()
        self.lock = threading.Lock()
        self.exit_event = MultithreadedAsyncEvent()
        self.active_producers = 0
        self.producers_done_event = MultithreadedAsyncEvent()

    async def _produce(
            self,
            async_gen: Callable[[], AsyncIterator[T]],
            wait_for_consumers: bool = False
    ):
        """
        Iterate over the async generator function and put the extracted elements into the queue.

        Perform a graceful shutdown when the exit_event is set.
        """

        async def _anext(iterator):  # Make pyright happy. Wrap the awaitable into a coroutine
            # TODO: use `anext`(https://docs.python.org/3/library/functions.html#anext). Requires python 3.10
            return await iterator.__anext__()

        loop = asyncio.get_event_loop()
        it = async_gen()
        producer_task = None
        queue_put_task = None
        exit_event_task = None
        with self.lock:
            self.active_producers += 1
        try:
            while not self.exit_event.is_set():
                if not producer_task:
                    producer_task = loop.create_task(_anext(it))
                if not exit_event_task or exit_event_task.cancelled():
                    exit_event_task = loop.create_task(self.exit_event.wait_threadsafe())
                await asyncio.wait([producer_task, exit_event_task], return_when=asyncio.FIRST_COMPLETED)

                if producer_task.done():
                    try:
                        product = producer_task.result()
                    except StopAsyncIteration:
                        break
                    queue_put_task = loop.create_task(self.queue.put_threadsafe(product))
                    producer_task = None
        finally:
            with self.lock:
                self.active_producers -= 1
                if not self.active_producers > 0:
                    self.producers_done_event.set_threadsafe()

            wait_for_consumers_task = loop.create_task(self.queue.join_threadsafe())
            if wait_for_consumers:
                await asyncio.wait([wait_for_consumers_task], timeout=60)

            await cancel_and_wait(wait_for_consumers_task)
            await cancel_and_wait(exit_event_task)
            await cancel_and_wait(producer_task)
            await cancel_and_wait(queue_put_task)

    async def _consume(
            self,
            fnc: Callable[[T], Awaitable[None]]
    ):
        """
        Wait for elements to arrive via the queue and call the given async function on each element.

        If exit_event is set, handle all remaining elements from the queue and exit gracefully.
        """
        loop = asyncio.get_event_loop()
        queue_get_task = None
        producers_done_task = None
        try:
            while not self.producers_done_event.is_set() or self.queue.unfinished_tasks:
                if not queue_get_task:
                    queue_get_task = loop.create_task(self.queue.get_threadsafe())
                if not producers_done_task or producers_done_task.cancelled():
                    producers_done_task = loop.create_task(self.producers_done_event.wait_threadsafe())

                await asyncio.wait([queue_get_task, producers_done_task], return_when=asyncio.FIRST_COMPLETED)

                if queue_get_task.done():
                    if not queue_get_task.cancelled():
                        product = queue_get_task.result()
                        try:
                            await fnc(product)
                        finally:
                            self.queue.task_done_threadsafe()
                    queue_get_task = None
        finally:
            await cancel_and_wait(queue_get_task)
            await cancel_and_wait(producers_done_task)

    async def run(self):
        with contextlib.ExitStack() as stack:
            consumer_tasks = []
            producer_tasks = []
            loop = asyncio.get_event_loop()
            try:
                if self.multithreaded:
                    # Run each task in a separate thread
                    executor = ThreadPoolExecutor(max_workers=len(self.producers) + len(self.consumers))
                    stack.enter_context(executor)
                    for producer in self.producers:
                        task = loop.run_in_executor(executor, run_in_new_event_loop, self._produce(async_gen=producer, wait_for_consumers=True))
                        producer_tasks.append(task)
                    for consumer in self.consumers:
                        task = loop.run_in_executor(executor, run_in_new_event_loop, self._consume(fnc=consumer))
                        consumer_tasks.append(task)
                else:
                    # Run everything in the main thread
                    for producer in self.producers:
                        task = loop.create_task(self._produce(async_gen=producer, wait_for_consumers=True))
                        producer_tasks.append(task)
                    for consumer in self.consumers:
                        task = loop.create_task(self._consume(fnc=consumer))
                        consumer_tasks.append(task)

                await asyncio.wait(producer_tasks, return_when=asyncio.ALL_COMPLETED)
            finally:
                # We got here either due to an exception (which includes CancelledError generated by SIGINT),
                # or if all producers ended (probably because once == True). In both cases, we want to trigger
                # a graceful exit.
                self.exit_event.set()
                await asyncio.wait(producer_tasks, return_when=asyncio.ALL_COMPLETED)
                await asyncio.wait(consumer_tasks, return_when=asyncio.ALL_COMPLETED)
