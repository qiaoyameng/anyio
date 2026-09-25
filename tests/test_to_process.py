from __future__ import annotations

import os
import sys
import time
from functools import partial
from pathlib import Path
from unittest.mock import Mock

import pytest
from pytest import MonkeyPatch

from anyio import (
    CancelScope,
    CapacityLimiter,
    create_task_group,
    fail_after,
    to_process,
    wait_all_tasks_blocked,
)
from anyio.abc import Process


async def test_run_sync_in_process_pool() -> None:
    """
    Test that the function runs in a different process, and the same process in both
    calls.

    """
    worker_pid = await to_process.run_sync(os.getpid)
    assert worker_pid != os.getpid()
    assert await to_process.run_sync(os.getpid) == worker_pid


async def test_identical_sys_path() -> None:
    """Test that partial() can be used to pass keyword arguments."""
    assert await to_process.run_sync(eval, "sys.path") == sys.path


async def test_partial() -> None:
    """Test that partial() can be used to pass keyword arguments."""
    assert await to_process.run_sync(partial(sorted, reverse=True), ["a", "b"]) == [
        "b",
        "a",
    ]


async def test_exception() -> None:
    """Test that exceptions are delivered properly."""
    with pytest.raises(ValueError, match="invalid literal for int"):
        assert await to_process.run_sync(int, "a")


async def test_print() -> None:
    """Test that print() won't interfere with parent-worker communication."""
    worker_pid = await to_process.run_sync(os.getpid)
    await to_process.run_sync(print, "hello")
    await to_process.run_sync(print, "world")
    assert await to_process.run_sync(os.getpid) == worker_pid


def _flood_stderr() -> str:
    """Helper that writes enough data to stderr to fill an undrained pipe buffer."""

    # A typical pipe buffer is 64 KiB; write well beyond that to ensure it would block
    # if stderr were still connected to the (undrained) parent pipe.
    payload = "x" * (1024 * 1024)
    for stream in sys.stdout, sys.stderr:
        stream.write(payload)
        stream.flush()

    return "completed"


async def test_stderr_flood() -> None:
    """
    Test that writing large amounts of data to stderr in the worker process won't
    deadlock the call. Regression test for the worker's stderr not being redirected to
    /dev/null, leaving it connected to an undrained pipe.
    """

    with fail_after(10):
        assert await to_process.run_sync(_flood_stderr) == "completed"


async def test_cancel_before() -> None:
    """
    Test that starting to_process.run_sync() in a cancelled scope does not cause a
    worker process to be reserved.

    """
    with CancelScope() as scope:
        scope.cancel()
        await to_process.run_sync(os.getpid)

    pytest.raises(LookupError, to_process._process_pool_workers.get)


@pytest.mark.usefixtures("deactivate_blockbuster")
async def test_cancel_during() -> None:
    """
    Test that cancelling an operation on the worker process causes the process to be
    killed.

    """
    worker_pid = await to_process.run_sync(os.getpid)
    with fail_after(4):
        async with create_task_group() as tg:
            tg.start_soon(partial(to_process.run_sync, cancellable=True), time.sleep, 5)
            await wait_all_tasks_blocked()
            tg.cancel_scope.cancel()

    # The previous worker was killed so we should get a new one now
    assert await to_process.run_sync(os.getpid) != worker_pid


async def test_exec_while_pruning() -> None:
    """
    Test that in the case when one or more idle workers are pruned, the originally
    selected idle worker is re-added to the queue of idle workers.
    """

    worker_pid1 = await to_process.run_sync(os.getpid)
    workers = to_process._process_pool_workers.get()
    idle_workers = to_process._process_pool_idle_workers.get()
    real_worker = next(iter(workers))
    idle = idle_workers[to_process._InitializerKey(None, ())]

    fake_idle_process = Mock(Process)
    workers.add(fake_idle_process)
    try:
        # Add a mock worker process that's guaranteed to be eligible for pruning
        idle.appendleft((fake_idle_process, -to_process.WORKER_MAX_IDLE_TIME - 1))

        worker_pid2 = await to_process.run_sync(os.getpid)
        assert worker_pid1 == worker_pid2
        fake_idle_process.kill.assert_called_once_with()
        assert idle[0][0] is real_worker
    finally:
        workers.discard(fake_idle_process)


async def test_nonexistent_main_module(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """
    Test that worker process creation won't fail if the detected path to the `__main__`
    module doesn't exist. Regression test for #696.
    """

    script_path = tmp_path / "badscript"
    script_path.touch()
    monkeypatch.setattr("__main__.__file__", str(script_path / "__main__.py"))
    await to_process.run_sync(os.getpid)


def _check_main_importable() -> int:
    """Helper that runs in the worker process to check main was imported correctly"""

    # python multiprocessing does it this way and there is probably code dependent on it.
    assert sys.modules["__main__"] == sys.modules["__mp_main__"]

    return sys.modules["__main__"].foo


async def test_entrypoint_main_module(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    """
    Test that worker process creation succeeds when __main__.__file__ points to an
    entry point script (no .py extension). Regression test for #1027.
    """

    script_path = tmp_path / "my-entrypoint"
    script_path.write_text("foo=3")
    monkeypatch.setattr("__main__.__file__", str(script_path))
    assert await to_process.run_sync(_check_main_importable) == 3


_init_call_count = 0
_init_arg: object = None


def _counting_initializer() -> None:
    global _init_call_count
    _init_call_count += 1


def _get_init_call_count() -> int:
    return _init_call_count


def _arg_storing_initializer(arg: object) -> None:
    global _init_arg
    _init_arg = arg


def _get_init_arg() -> object:
    return _init_arg


def _failing_initializer(counter_path: str) -> None:
    with open(counter_path, "a") as file:
        file.write("x")

    raise ValueError("initializer failed")


async def test_initializer() -> None:
    """
    Test that the initializer runs once in the worker process, and that subsequent
    calls with the same initializer reuse the initialized worker.
    """
    worker_pid = await to_process.run_sync(
        os.getpid, initializer=_counting_initializer
    )
    assert worker_pid != os.getpid()
    assert (
        await to_process.run_sync(os.getpid, initializer=_counting_initializer)
        == worker_pid
    )
    assert (
        await to_process.run_sync(
            _get_init_call_count, initializer=_counting_initializer
        )
        == 1
    )


async def test_initializer_initargs() -> None:
    """Test that initargs are passed to the initializer in the worker process."""
    initargs = ("config",)
    worker_pid = await to_process.run_sync(
        os.getpid, initializer=_arg_storing_initializer, initargs=initargs
    )
    assert (
        await to_process.run_sync(
            _get_init_arg, initializer=_arg_storing_initializer, initargs=initargs
        )
        == "config"
    )
    assert (
        await to_process.run_sync(
            os.getpid, initializer=_arg_storing_initializer, initargs=initargs
        )
        == worker_pid
    )


async def test_initializer_distinct_configurations() -> None:
    """
    Test that calls with different initializers or initargs never share worker
    processes, and that interleaved calls each reuse the worker matching their
    own configuration.
    """
    pid1 = await to_process.run_sync(
        os.getpid, initializer=_arg_storing_initializer, initargs=("config1",)
    )
    pid2 = await to_process.run_sync(
        os.getpid, initializer=_arg_storing_initializer, initargs=("config2",)
    )
    pid3 = await to_process.run_sync(os.getpid, initializer=_counting_initializer)
    assert len({pid1, pid2, pid3}) == 3

    # Interleaved calls reuse the worker initialized with the same configuration
    assert (
        await to_process.run_sync(os.getpid, initializer=_counting_initializer)
        == pid3
    )
    assert (
        await to_process.run_sync(
            os.getpid, initializer=_arg_storing_initializer, initargs=("config1",)
        )
        == pid1
    )
    assert (
        await to_process.run_sync(
            _get_init_arg,
            initializer=_arg_storing_initializer,
            initargs=("config2",),
        )
        == "config2"
    )


async def test_initializer_key_identity() -> None:
    """
    Test that initializer configurations are compared by the identity of the
    initializer and each of its arguments, not by equality.
    """
    shared_arg = ["config"]
    worker_pid = await to_process.run_sync(
        os.getpid, initializer=_arg_storing_initializer, initargs=(shared_arg,)
    )

    # The same argument object in a different tuple shares the worker pool
    assert (
        await to_process.run_sync(
            os.getpid, initializer=_arg_storing_initializer, initargs=(shared_arg,)
        )
        == worker_pid
    )

    # A distinct but equal argument object gets its own worker pool
    assert (
        await to_process.run_sync(
            os.getpid, initializer=_arg_storing_initializer, initargs=(["config"],)
        )
        != worker_pid
    )


async def test_initializer_exception(tmp_path: Path) -> None:
    """
    Test that an exception raised by the initializer is propagated to the caller,
    and that the broken worker process is discarded instead of being pooled or
    endlessly respawned.
    """
    counter_path = tmp_path / "counter"
    for _ in range(2):
        with pytest.raises(ValueError, match="initializer failed"):
            await to_process.run_sync(
                os.getpid,
                initializer=_failing_initializer,
                initargs=(str(counter_path),),
            )

    # Each call spawned a fresh worker whose initializer ran exactly once
    assert counter_path.read_text() == "xx"

    # No broken worker was left behind in the pools
    assert not to_process._process_pool_workers.get()
    assert not any(to_process._process_pool_idle_workers.get().values())


@pytest.mark.usefixtures("deactivate_blockbuster")
async def test_initializer_cancel_first_waiter() -> None:
    """
    Test that cancelling the first waiter on a worker process does not prevent a
    queued waiter from getting a new, properly initialized worker process.
    """
    limiter = CapacityLimiter(1)
    first_scope: CancelScope | None = None
    second_pid: int | None = None

    async def first_waiter() -> None:
        nonlocal first_scope
        with CancelScope() as first_scope:
            await to_process.run_sync(
                time.sleep,
                5,
                cancellable=True,
                limiter=limiter,
                initializer=_counting_initializer,
            )

    async def second_waiter() -> None:
        nonlocal second_pid
        second_pid = await to_process.run_sync(
            os.getpid, limiter=limiter, initializer=_counting_initializer
        )

    with fail_after(10):
        async with create_task_group() as tg:
            tg.start_soon(first_waiter)
            await wait_all_tasks_blocked()
            tg.start_soon(second_waiter)
            await wait_all_tasks_blocked()
            assert first_scope is not None
            first_scope.cancel()

    assert second_pid is not None

    # The replacement worker was initialized exactly once
    assert (
        await to_process.run_sync(
            _get_init_call_count, limiter=limiter, initializer=_counting_initializer
        )
        == 1
    )
