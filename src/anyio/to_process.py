from __future__ import annotations

__all__ = (
    "current_default_process_limiter",
    "process_worker",
    "run_sync",
)

import os
import pickle
import runpy
import subprocess
import sys
from collections import deque
from collections.abc import Callable
from types import ModuleType
from typing import Any, TypeVar, cast

from ._core._eventloop import current_time, get_async_backend, get_cancelled_exc_class
from ._core._exceptions import BrokenWorkerProcess
from ._core._subprocesses import open_process
from ._core._synchronization import CapacityLimiter
from ._core._tasks import CancelScope, fail_after
from .abc import ByteReceiveStream, ByteSendStream, Process
from .lowlevel import RunVar, checkpoint_if_cancelled
from .streams.buffered import BufferedByteReceiveStream

if sys.version_info >= (3, 11):
    from typing import TypeVarTuple, Unpack
else:
    from typing_extensions import TypeVarTuple, Unpack

WORKER_MAX_IDLE_TIME = 300  # 5 minutes

T_Retval = TypeVar("T_Retval")
PosArgsT = TypeVarTuple("PosArgsT")
InitArgsT = TypeVarTuple("InitArgsT")

_process_pool_workers: RunVar[set[Process]] = RunVar("_process_pool_workers")
_process_pool_idle_workers: RunVar[
    dict[
        tuple[Callable[..., Any] | None, tuple[Any, ...]],
        deque[tuple[Process, float]],
    ]
] = RunVar("_process_pool_idle_workers")
_default_process_limiter: RunVar[CapacityLimiter] = RunVar("_default_process_limiter")


async def run_sync(  # type: ignore[return]
    func: Callable[[Unpack[PosArgsT]], T_Retval],
    *args: Unpack[PosArgsT],
    cancellable: bool = False,
    limiter: CapacityLimiter | None = None,
    initializer: Callable[[Unpack[InitArgsT]], Any] | None = None,
    initargs: tuple[Unpack[InitArgsT]] = (),
) -> T_Retval:
    """
    Call the given function with the given arguments in a worker process.

    If the ``cancellable`` option is enabled and the task waiting for its completion is
    cancelled, the worker process running it will be abruptly terminated using SIGKILL
    (or ``terminateProcess()`` on Windows).

    If an initializer is given, it is called with ``initargs`` in each new worker
    process before it starts processing requests. This is useful for one-time setup
    such as loading a large data structure into memory. Worker processes are only
    shared between calls whose initializer and initargs are equal, so both must be
    hashable. If the initializer raises an exception, it is propagated to the caller
    and the worker process is discarded.

    :param func: a callable
    :param args: positional arguments for the callable
    :param cancellable: ``True`` to allow cancellation of the operation while it's
        running
    :param limiter: capacity limiter to use to limit the total amount of processes
        running (if omitted, the default limiter is used)
    :param initializer: a callable to run in each new worker process before it
        processes any requests
    :param initargs: positional arguments to pass to the initializer
    :raises NoEventLoopError: if no supported asynchronous event loop is running in the
        current thread
    :return: an awaitable that yields the return value of the function.

    """

    async def send_raw_command(pickled_cmd: bytes) -> object:
        try:
            await stdin.send(pickled_cmd)
            response = await buffered.receive_until(b"\n", 50)
            status, length = response.split(b" ")
            if status not in (b"RETURN", b"EXCEPTION"):
                raise RuntimeError(
                    f"Worker process returned unexpected response: {response!r}"
                )

            pickled_response = await buffered.receive_exactly(int(length))
        except BaseException as exc:
            workers.discard(process)
            try:
                process.kill()
                with CancelScope(shield=True):
                    await process.aclose()
            except ProcessLookupError:
                pass

            if isinstance(exc, get_cancelled_exc_class()):
                raise
            else:
                raise BrokenWorkerProcess from exc

        retval = pickle.loads(pickled_response)
        if status == b"EXCEPTION":
            assert isinstance(retval, BaseException)
            raise retval
        else:
            return retval

    # First pickle the request before trying to reserve a worker process
    await checkpoint_if_cancelled()
    request = pickle.dumps(("run", func, args), protocol=pickle.HIGHEST_PROTOCOL)
    if initializer is not None:
        init_request: bytes | None = pickle.dumps(
            ("run", initializer, initargs), protocol=pickle.HIGHEST_PROTOCOL
        )
    else:
        init_request = None

    # The initializer and its arguments determine which pool of worker processes can
    # serve this call, so they must be usable as a dictionary key
    pool_key: tuple[Callable[..., Any] | None, tuple[Any, ...]] = (
        initializer,
        initargs,
    )
    try:
        hash(pool_key)
    except TypeError:
        raise TypeError("the initializer and its arguments must be hashable") from None

    # If this is the first run in this event loop thread, set up the necessary variables
    try:
        workers = _process_pool_workers.get()
        idle_workers = _process_pool_idle_workers.get()
    except LookupError:
        workers = set()
        idle_workers = {}
        _process_pool_workers.set(workers)
        _process_pool_idle_workers.set(idle_workers)
        get_async_backend().setup_process_pool_exit_at_shutdown(workers)

    async with limiter or current_default_process_limiter():
        # Pop processes from the pool (starting from the most recently used) until we
        # find one that hasn't exited yet
        idle_worker_queue = idle_workers.setdefault(pool_key, deque())
        process: Process
        while idle_worker_queue:
            process, _idle_since = idle_worker_queue.pop()
            if process.returncode is None:
                stdin = cast(ByteSendStream, process.stdin)
                buffered = BufferedByteReceiveStream(
                    cast(ByteReceiveStream, process.stdout)
                )

                # Prune any other workers that have been idle for WORKER_MAX_IDLE_TIME
                # seconds or longer
                now = current_time()
                killed_processes: list[Process] = []
                for queue in idle_workers.values():
                    while queue:
                        if now - queue[0][1] < WORKER_MAX_IDLE_TIME:
                            break

                        process_to_kill, _idle_since = queue.popleft()
                        process_to_kill.kill()
                        workers.remove(process_to_kill)
                        killed_processes.append(process_to_kill)

                with CancelScope(shield=True):
                    for killed_process in killed_processes:
                        await killed_process.aclose()

                break

            workers.remove(process)
        else:
            command = [sys.executable, "-u", "-m", __name__]
            process = await open_process(
                command, stdin=subprocess.PIPE, stdout=subprocess.PIPE
            )
            try:
                stdin = cast(ByteSendStream, process.stdin)
                buffered = BufferedByteReceiveStream(
                    cast(ByteReceiveStream, process.stdout)
                )
                with fail_after(20):
                    message = await buffered.receive(6)

                if message != b"READY\n":
                    raise BrokenWorkerProcess(
                        f"Worker process returned unexpected response: {message!r}"
                    )

                main_module_path = getattr(sys.modules["__main__"], "__file__", None)
                pickled = pickle.dumps(
                    ("init", sys.path, main_module_path),
                    protocol=pickle.HIGHEST_PROTOCOL,
                )
                await send_raw_command(pickled)
            except (BrokenWorkerProcess, get_cancelled_exc_class()):
                raise
            except BaseException as exc:
                process.kill()
                raise BrokenWorkerProcess(
                    "Error during worker process initialization"
                ) from exc

            if init_request is not None:
                try:
                    await send_raw_command(init_request)
                except BaseException:
                    # A worker whose initializer failed (or was interrupted) must
                    # not be reused, so kill it and let the exception propagate to
                    # the caller
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass

                    with CancelScope(shield=True):
                        await process.aclose()

                    raise

            workers.add(process)

        with CancelScope(shield=not cancellable):
            try:
                return cast(T_Retval, await send_raw_command(request))
            finally:
                if process in workers:
                    idle_worker_queue.append((process, current_time()))


def current_default_process_limiter() -> CapacityLimiter:
    """
    Return the capacity limiter that is used by default to limit the number of worker
    processes.

    :return: a capacity limiter object

    """
    try:
        return _default_process_limiter.get()
    except LookupError:
        limiter = CapacityLimiter(os.cpu_count() or 2)
        _default_process_limiter.set(limiter)
        return limiter


def process_worker() -> None:
    # Redirect standard streams to os.devnull so that user code won't interfere with the
    # parent-worker communication
    stdin = sys.stdin
    stdout = sys.stdout
    sys.stdin = open(os.devnull)
    sys.stdout = open(os.devnull, "w")
    sys.stderr = open(os.devnull, "w")

    stdout.buffer.write(b"READY\n")
    while True:
        retval = exception = None
        try:
            command, *args = pickle.load(stdin.buffer)
        except EOFError:
            return
        except BaseException as exc:
            exception = exc
        else:
            if command == "run":
                func, args = args
                try:
                    retval = func(*args)
                except BaseException as exc:
                    exception = exc
            elif command == "init":
                main_module_path: str | None
                sys.path, main_module_path = args
                del sys.modules["__main__"]
                if main_module_path and os.path.isfile(main_module_path):
                    # Load the parent's main module but as __mp_main__ instead of
                    # __main__ (like multiprocessing does) to avoid infinite recursion
                    try:
                        main = ModuleType("__mp_main__")
                        main_content = runpy.run_path(
                            main_module_path, run_name="__mp_main__"
                        )
                        main.__dict__.update(main_content)
                        sys.modules["__main__"] = sys.modules["__mp_main__"] = main
                    except BaseException as exc:
                        exception = exc
        try:
            if exception is not None:
                status = b"EXCEPTION"
                pickled = pickle.dumps(exception, pickle.HIGHEST_PROTOCOL)
            else:
                status = b"RETURN"
                pickled = pickle.dumps(retval, pickle.HIGHEST_PROTOCOL)
        except BaseException as exc:
            exception = exc
            status = b"EXCEPTION"
            pickled = pickle.dumps(exc, pickle.HIGHEST_PROTOCOL)

        stdout.buffer.write(b"%s %d\n" % (status, len(pickled)))
        stdout.buffer.write(pickled)

        # Respect SIGTERM
        if isinstance(exception, SystemExit):
            raise exception


if __name__ == "__main__":
    process_worker()
