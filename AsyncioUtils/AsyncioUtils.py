"""
Utility functions to help using asyncio features.

Some implementations used part of functions from the BuiltIn module from robot itself.
"""

import time
import inspect
import asyncio

from robot.libraries.BuiltIn import BuiltIn
from robot.utils.robottime import timestr_to_secs, secs_to_timestr
from robot.errors import DataError, ExecutionPassed, ExecutionFailed, ExecutionFailures


class AsyncioUtils:
    def __init__(self) -> None:
        try:
            self.builtIn = BuiltIn()
        except Exception:
            self.builtIn = None

    async def async_gather_keywords(self, *keywords):
        """Executes all the given keywords concurrently.

        This keyword is mainly useful to run multiple keywords concurrently
        just like calling asyncio.gather() in python.
        The keywords passed need to be async, else an error is raised.

        By default all arguments are expected to be keywords to be executed.

        Examples:
        | `Run Keywords` | `Initialize database` | `Start servers` | `Clear logs` |
        | `Run Keywords` | ${KW 1} | ${KW 2} |
        | `Run Keywords` | @{KEYWORDS} |

        Keywords can also be run with arguments using upper case ``AND`` as
        a separator between keywords. The keywords are executed so that the
        first argument is the first keyword and proceeding arguments until
        the first ``AND`` are arguments to it. First argument after the first
        ``AND`` is the second keyword and proceeding arguments until the next
        ``AND`` are its arguments. And so on.

        Examples:
        | `Run Keywords` | `Initialize database` | db1 | AND | `Start servers` | server1 | server2 |
        | `Run Keywords` | `Initialize database` | ${DB NAME} | AND | `Start servers` | @{SERVERS} | AND | `Clear logs` |
        | `Run Keywords` | ${KW} | AND | @{KW WITH ARGS} |

        Notice that the ``AND`` control argument must be used explicitly and
        cannot itself come from a variable. If you need to use literal ``AND``
        string as argument, you can either use variables or escape it with
        a backslash like ``\\AND``.
        """
        assert self.builtIn
        return await self._async_gather_keywords(
            self.builtIn._split_run_keywords(list(keywords))
        )

    async def _async_gather_keywords(self, iterable):
        assert self.builtIn
        tasks = []
        errors = []
        try:
            for kw, args in iterable:
                try:
                    task = self.builtIn.run_keyword(kw, *args)
                    if not inspect.iscoroutine(task):
                        raise DataError(f"Keyword '{kw}' is not async")
                    tasks.append(task)
                except ExecutionPassed as err:
                    err.set_earlier_failures(errors)
                    raise err
                except ExecutionFailed as err:
                    errors.extend(err.get_errors())
                    if not err.can_continue(self.builtIn._context):
                        break
            if errors:
                raise ExecutionFailures(errors)
        except Exception as err:
            for t in tasks:
                t.close()
            raise err
        return await asyncio.gather(*tasks)

    async def async_sleep(self, time_, reason=None):
        """
        Pauses the test executed for the given time asynchronously.
        This is useful for allowing other coroutines to continue running in the
        backgroud. Normal sleep blocks all execution.

        ``time`` may be either a number or a time string. Time strings are in
        a format such as ``1 day 2 hours 3 minutes 4 seconds 5milliseconds`` or
        ``1d 2h 3m 4s 5ms``, and they are fully explained in an appendix of
        Robot Framework User Guide. Providing a value without specifying minutes
        or seconds, defaults to seconds.
        Optional `reason` can be used to explain why
        sleeping is necessary. Both the time slept and the reason are logged.

        Examples:
        | Sleep | 42                   |
        | Sleep | 1.5                  |
        | Sleep | 2 minutes 10 seconds |
        | Sleep | 10s                  | Wait for a reply |
        """
        assert self.builtIn
        seconds = timestr_to_secs(time_)
        # Python hangs with negative values
        if seconds < 0:
            seconds = 0
        await self._async_sleep_in_parts(seconds)
        self.builtIn.log("Slept %s" % secs_to_timestr(seconds))
        if reason:
            self.builtIn.log(reason)

    async def _async_sleep_in_parts(self, seconds):
        """
        time.sleep can't be stopped in windows
        to ensure that we can signal stop (with timeout)
        split sleeping to small pieces
        """
        endtime = time.time() + float(seconds)
        while True:
            remaining = endtime - time.time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(remaining, 0.01))

    async def async_create_task(self, name, *args):
        """
        Schedules an async keyword to run in background.

        If keyword is not async, DataError will be raised.

        Because the name of the keyword to execute is given as an argument, it
        can be a variable and thus set dynamically, e.g. from a return value of
        another keyword or from the command line.
        """
        assert self.builtIn
        task = self.builtIn.run_keyword(name, *args)
        if not inspect.iscoroutine(task):
            raise DataError(f"Keyword '{name}' is not async")
        return asyncio.create_task(task)

    async def async_await_task(self, task: asyncio.Task):
        """
        Get the result from a scheduled async task.

        If obj is not a task, DataError will be raised.
        """
        if not inspect.isawaitable(task):
            raise DataError("Obj is not an async Task")
        return await task

    async def async_cancel_task(self, task: asyncio.Task) -> bool:
        """
        Attempt to cancel the task, return True if succeeded, False otherwise

        If obj is not a task, DataError will be raised.
        """
        if not inspect.isawaitable(task):
            raise DataError("Obj is not an async Task")
        return task.cancel()

    def async_is_task(self, obj) -> bool:
        """
        Return True if obj is coroutine, False otherwise
        """
        return asyncio.iscoroutine(obj)
