from __future__ import annotations

import os
import platform
import sys
from collections.abc import Callable
from pathlib import Path
from subprocess import DEVNULL, CalledProcessError
from textwrap import dedent
from typing import Any

import psutil
import pytest
from pytest_mock.plugin import MockerFixture

from anyio import (
    BrokenResourceError,
    CancelScope,
    ClosedResourceError,
    EndOfStream,
    ProcessPipelineError,
    create_task_group,
    fail_after,
    open_process,
    open_process_pipeline,
    run_process,
    sleep,
)
from anyio._core._eventloop import get_async_backend
from anyio.streams.buffered import BufferedByteReceiveStream


@pytest.mark.parametrize(
    "shell, command",
    [
        pytest.param(
            True,
            f'{sys.executable} -c "import sys; print(sys.stdin.read()[::-1])"',
            id="shell",
        ),
        pytest.param(
            False,
            [sys.executable, "-c", "import sys; print(sys.stdin.read()[::-1])"],
            id="exec",
        ),
    ],
)
async def test_run_process(
    shell: bool, command: str | list[str], event_loop_implementation_name: str | None
) -> None:
    if event_loop_implementation_name == "winloop":
        pytest.skip(reason="winloop produces a non-zero exit with status 1")

    process = await run_process(command, input=b"abc")
    assert process.returncode == 0
    assert process.stdout.rstrip() == b"cba"


async def test_run_process_checked() -> None:
    with pytest.raises(CalledProcessError) as exc:
        await run_process(
            [
                sys.executable,
                "-c",
                (
                    'import sys; print("stderr-text", file=sys.stderr); '
                    'print("stdout-text"); sys.exit(1)'
                ),
            ],
            check=True,
        )

    assert exc.value.returncode == 1
    assert exc.value.stdout.rstrip() == b"stdout-text"
    assert exc.value.stderr.rstrip() == b"stderr-text"


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="process.terminate() kills the process instantly on Windows",
)
async def test_terminate(tmp_path: Path) -> None:
    script_path = tmp_path / "script.py"
    script_path.write_text(
        dedent(
            """\
        import signal, sys, time

        def terminate(signum, frame):
            sys.exit(2)

        signal.signal(signal.SIGTERM, terminate)
        print('ready', flush=True)
        time.sleep(5)
    """
        )
    )
    async with await open_process([sys.executable, str(script_path)]) as process:
        stdout = process.stdout
        assert stdout is not None
        buffered_stdout = BufferedByteReceiveStream(stdout)
        line = await buffered_stdout.receive_until(b"\n", 100)
        assert line.rstrip() == b"ready"

        process.terminate()
        assert await process.wait() == 2


@pytest.mark.parametrize("max_bytes", [0, -1])
async def test_process_receive_invalid_max_bytes(max_bytes: int) -> None:
    async with await open_process(
        [sys.executable, "-c", "import sys; sys.stdout.write('x')"]
    ) as process:
        assert process.stdout is not None
        with pytest.raises(ValueError, match="max_bytes must be a positive integer"):
            await process.stdout.receive(max_bytes)

        await process.wait()


async def test_process_cwd(tmp_path: Path) -> None:
    """Test that `cwd` is successfully passed to the subprocess implementation"""
    cmd = [sys.executable, "-c", "import os; print(os.getcwd())"]
    result = await run_process(cmd, cwd=tmp_path)
    assert result.stdout.decode().strip() == str(tmp_path)


async def test_process_env() -> None:
    """Test that `env` is successfully passed to the subprocess implementation"""
    env = os.environ.copy()
    env.update({"foo": "bar"})
    cmd = [sys.executable, "-c", "import os; print(os.environ['foo'])"]
    result = await run_process(cmd, env=env)
    assert result.stdout.decode().strip() == env["foo"]


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Windows does not have os.getsid()"
)
async def test_process_new_session_sid() -> None:
    """
    Test that start_new_session is successfully passed to the subprocess implementation.

    """
    sid = os.getsid(os.getpid())
    cmd = [sys.executable, "-c", "import os; print(os.getsid(os.getpid()))"]

    result = await run_process(cmd)
    assert result.stdout.decode().strip() == str(sid)

    result = await run_process(cmd, start_new_session=True)
    assert result.stdout.decode().strip() != str(sid)


async def test_open_process_connect_to_file(tmp_path: Path) -> None:
    stdinfile = tmp_path / "stdin"
    stdinfile.write_text("Hello, process!\n")
    stdoutfile = tmp_path / "stdout"
    stderrfile = tmp_path / "stderr"
    with (
        stdinfile.open("rb") as fin,
        stdoutfile.open("wb") as fout,
        stderrfile.open("wb") as ferr,
    ):
        async with await open_process(
            [
                sys.executable,
                "-c",
                (
                    "import sys; txt = sys.stdin.read().strip(); "
                    'print("stdin says", repr(txt), "but stderr says NO!", '
                    "file=sys.stderr); "
                    'print("stdin says", repr(txt), "and stdout says YES!")'
                ),
            ],
            stdin=fin,
            stdout=fout,
            stderr=ferr,
        ) as p:
            assert await p.wait() == 0

    assert (
        stdoutfile.read_text() == "stdin says 'Hello, process!' and stdout says YES!\n"
    )
    assert (
        stderrfile.read_text() == "stdin says 'Hello, process!' but stderr says NO!\n"
    )


async def test_run_process_connect_to_file(tmp_path: Path) -> None:
    stdinfile = tmp_path / "stdin"
    stdinfile.write_text("Hello, process!\n")
    stdoutfile = tmp_path / "stdout"
    stderrfile = tmp_path / "stderr"
    with (
        stdinfile.open("rb") as fin,
        stdoutfile.open("wb") as fout,
        stderrfile.open("wb") as ferr,
    ):
        await run_process(
            [
                sys.executable,
                "-c",
                (
                    "import sys; txt = sys.stdin.read().strip(); "
                    'print("stdin says", repr(txt), "but stderr says NO!", '
                    "file=sys.stderr); "
                    'print("stdin says", repr(txt), "and stdout says YES!")'
                ),
            ],
            stdin=fin,
            stdout=fout,
            stderr=ferr,
        )

    assert (
        stdoutfile.read_text() == "stdin says 'Hello, process!' and stdout says YES!\n"
    )
    assert (
        stderrfile.read_text() == "stdin says 'Hello, process!' but stderr says NO!\n"
    )


async def test_stdin_input_both_passed(tmp_path: Path) -> None:
    stdinfile = tmp_path / "stdin"
    stdinfile.write_text("Hello, process!\n")
    with pytest.raises(ValueError, match="only one of"), stdinfile.open("rb") as fin:
        await run_process([sys.executable, "--version"], input=b"abc", stdin=fin)


async def test_run_process_inherit_stdout(capfd: pytest.CaptureFixture[str]) -> None:
    await run_process(
        [
            sys.executable,
            "-c",
            'import sys; print("stderr-text", file=sys.stderr); print("stdout-text")',
        ],
        check=True,
        stdout=None,
        stderr=None,
    )
    out, err = capfd.readouterr()
    assert out == "stdout-text" + os.linesep
    assert err == "stderr-text" + os.linesep


async def test_process_aexit_cancellation_doesnt_orphan_process() -> None:
    """
    Regression test for #669.

    Ensures that open_process.__aexit__() doesn't leave behind an orphan process when
    cancelled.

    """
    with CancelScope() as scope:
        async with await open_process(
            [sys.executable, "-c", "import time; time.sleep(1)"]
        ) as process:
            scope.cancel()

    assert process.returncode is not None
    assert process.returncode != 0


async def test_process_aexit_cancellation_closes_standard_streams() -> None:
    """
    Regression test for #669.

    Ensures that open_process.__aexit__() closes standard streams when cancelled. Also
    ensures that process.std{in.send,{out,err}.receive}() raise ClosedResourceError on a
    closed stream.

    """
    with CancelScope() as scope:
        async with await open_process(
            [sys.executable, "-c", "import time; time.sleep(1)"]
        ) as process:
            scope.cancel()

    assert process.stdin is not None

    with pytest.raises(ClosedResourceError):
        await process.stdin.send(b"foo")

    assert process.stdout is not None

    with pytest.raises(ClosedResourceError):
        await process.stdout.receive(1)

    assert process.stderr is not None

    with pytest.raises(ClosedResourceError):
        await process.stderr.receive(1)


async def test_exceptions_after_subprocess_closes_standard_streams() -> None:
    async with await open_process([sys.executable, "-c", ""]) as process:
        await process.wait()
        assert process.stdin is not None
        assert process.stderr is not None
        assert process.stdout is not None
        with pytest.raises(BrokenResourceError):
            # * On Trio, stdin.send() will always raise if the peer's end of the pipe is
            #   closed.
            # * On asyncio, even though process.wait() finished, some event loop
            #   implementations do not yet inform us that the peer's end of the pipe is
            #   closed; the event loop needs to run some rounds of callbacks before it
            #   will get that information to us. In particular:
            #   * On stdlib asyncio, stdin.send() will now always raise
            #     BrokenResourceError.
            #   * On uvloop, the second stdin.send() will raise BrokenResourceError.
            #   * On Winloop 0.3.0, the third stdin.send() will raise
            #     BrokenResourceError. The second also might, but it depends on
            #     scheduling order.
            #   * On Winloop 0.3.1, the second stdin() will raise BrokenResourceError.
            for _ in range(3):
                await process.stdin.send(b"foo")

        with pytest.raises(BrokenResourceError):
            await process.stdin.send(b"foo")

        with pytest.raises(EndOfStream):
            await process.stdout.receive(1)

        with pytest.raises(EndOfStream):
            await process.stderr.receive(1)

    with pytest.raises(ClosedResourceError):
        await process.stdin.send(b"foo")

    with pytest.raises(ClosedResourceError):
        await process.stdout.receive(1)

    with pytest.raises(ClosedResourceError):
        await process.stderr.receive(1)


@pytest.mark.parametrize(
    "argname, argvalue_factory",
    [
        pytest.param(
            "user",
            lambda: os.getuid(),
            id="user",
            marks=[
                pytest.mark.skipif(
                    platform.system() == "Windows",
                    reason="os.getuid() is not available on Windows",
                )
            ],
        ),
        pytest.param(
            "group",
            lambda: os.getgid(),
            id="user",
            marks=[
                pytest.mark.skipif(
                    platform.system() == "Windows",
                    reason="os.getgid() is not available on Windows",
                )
            ],
        ),
        pytest.param("umask", lambda: 0, id="umask"),
    ],
)
async def test_user_group_arguments(
    argname: str,
    argvalue_factory: Callable[[], Any],
    event_loop_implementation_name: str | None,
) -> None:
    try:
        await run_process(
            [sys.executable, "-c", "print('hello')"],
            **{argname: argvalue_factory()},
        )
    except ValueError as exc:
        if "unexpected kwargs" in str(exc) and event_loop_implementation_name in (
            "uvloop",
            "winloop",
        ):
            pytest.skip(
                f"the {argname!r} argument is not supported by "
                f"{event_loop_implementation_name} yet"
            )

        raise


async def test_arguments_passed_through(mocker: MockerFixture) -> None:
    """
    Regression test ensuring all arguments accepted by ``open_process()``
    are passed through to the backend's ``open_process()``.
    """

    command = [sys.executable, "-c", "pass"]
    kwargs: dict[str, Any] = {
        "stdin": DEVNULL,
        "stdout": DEVNULL,
        "stderr": DEVNULL,
        "cwd": "/tmp",
        "env": {"FOO": "bar"},
        "startupinfo": object(),
        "creationflags": 4,
        "start_new_session": True,
        "pass_fds": (5, 6),
        "user": "myuser",
        "group": "mygroup",
        "extra_groups": [1, 2, "foo"],
        "umask": 123,
    }
    mock_open_process = mocker.patch.object(get_async_backend(), "open_process")
    await open_process(command, **kwargs)
    mock_open_process.assert_called_once_with(command, **kwargs)


async def test_close_early() -> None:
    """Regression test for #490."""
    code = dedent("""\
    import sys
    for _ in range(100):
        sys.stdout.buffer.write(bytes(range(256)))
    """)

    async with await open_process([sys.executable, "-c", code]):
        pass


async def test_close_while_reading() -> None:
    code = dedent("""\
    import time

    time.sleep(3)
    """)

    async with (
        await open_process([sys.executable, "-c", code]) as process,
        create_task_group() as tg,
    ):
        assert process.stdout
        tg.start_soon(process.stdout.aclose)
        with pytest.raises(ClosedResourceError):
            await process.stdout.receive()

        process.terminate()


async def test_wait_returns_on_process_exit_with_open_stdout() -> None:
    """
    wait() should return once the process exits, rather than waiting for the piped
    stdout/stderr to close. Easiest triggered by a grandchild that inherits the
    stdout pipe and keeps it open after the immediate child has exited.
    """
    code = dedent("""\
    import subprocess, sys

    subprocess.Popen(
        [sys.executable, "-c", "import select, sys; select.select([sys.stdin], [], [], 10)"]
    )
    """)

    process = await open_process([sys.executable, "-c", code])
    assert process.stdin is not None
    assert process.stdout is not None
    assert process.stderr is not None

    # Closing stdin unblocks the grandchild
    async with process.stdin:
        with fail_after(5):
            assert await process.wait() == 0

    # Wait for grandchild to exit to avoid ResourceWarnings
    with fail_after(3, shield=True):
        async for _ in process.stdout:
            pass

        async for _ in process.stderr:
            pass


async def test_close_with_stdout_blocked_subprocess(anyio_backend_name: str) -> None:
    """
    Regression test for #1166.

    Test that closing a process unblocks a subprocess blocked on writing to stdout
    (because the pipe buffer is full and nobody is reading), instead of deadlocking
    while waiting for it to exit.
    """

    process = await open_process(
        [sys.executable, "-c", "import sys;sys.stdout.write('x' * 1024 * 1024)"]
    )
    try:
        with fail_after(5):
            await process.aclose()
    except TimeoutError:
        if anyio_backend_name == "asyncio":
            process._process._transport.close()  # type: ignore[attr-defined]

        pytest.fail("Process.aclose() deadlocked")


def pipeline_command(script: str) -> list[str]:
    return [sys.executable, "-c", dedent(script)]


def assert_no_child_processes() -> None:
    assert psutil.Process().children() == []


async def test_pipeline_basic() -> None:
    commands = [
        pipeline_command("import sys; sys.stdout.write(sys.stdin.read()[::-1])"),
        pipeline_command("import sys; sys.stdout.write(sys.stdin.read().upper())"),
    ]
    async with open_process_pipeline(commands) as pipeline:
        assert len(pipeline.processes) == 2
        assert pipeline.stdin is not None
        assert pipeline.stdout is not None
        await pipeline.stdin.send(b"hello")
        await pipeline.stdin.aclose()
        output = b""
        async for chunk in pipeline.stdout:
            output += chunk

    assert output == b"OLLEH"
    assert pipeline.returncodes == [0, 0]
    assert pipeline.stderrs == [b"", b""]
    assert_no_child_processes()


@pytest.mark.parametrize(
    "commands",
    [
        pytest.param([], id="empty"),
        pytest.param([["echo", "hello"]], id="single"),
    ],
)
async def test_pipeline_too_few_commands(commands: list[list[str]]) -> None:
    with pytest.raises(ValueError, match="at least two commands"):
        async with open_process_pipeline(commands):
            pass


async def test_pipeline_string_command_rejected() -> None:
    commands = ["echo hello", "cat"]
    with pytest.raises(TypeError, match="must be sequences of arguments"):
        async with open_process_pipeline(commands):
            pass


async def test_pipeline_empty_command_rejected() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        async with open_process_pipeline([["echo"], []]):
            pass


async def test_pipeline_large_output() -> None:
    """16 MiB of output must stream through the middle process without deadlocking."""
    commands = [
        pipeline_command(
            """
            import sys
            chunk = b"x" * 65536
            for _ in range(256):
                sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()
            """
        ),
        pipeline_command(
            "import shutil, sys; shutil.copyfileobj(sys.stdin.buffer, sys.stdout.buffer)"
        ),
    ]
    with fail_after(15):
        async with open_process_pipeline(commands) as pipeline:
            assert pipeline.stdout is not None
            received = 0
            async for chunk in pipeline.stdout:
                received += len(chunk)

    assert received == 256 * 65536
    assert pipeline.returncodes == [0, 0]
    assert_no_child_processes()


async def test_pipeline_streams_incrementally() -> None:
    """Data must be passed on as it is read, not buffered until the producer exits."""
    commands = [
        pipeline_command(
            """
            import sys
            import time
            sys.stdout.buffer.write(b"x" * 65536)
            sys.stdout.buffer.flush()
            time.sleep(2)
            """
        ),
        pipeline_command(
            "import shutil, sys; shutil.copyfileobj(sys.stdin.buffer, sys.stdout.buffer)"
        ),
    ]
    async with open_process_pipeline(commands, check=False) as pipeline:
        assert pipeline.stdout is not None
        with fail_after(1):
            # The producer is still sleeping, so this can only arrive if the data
            # is streamed through the pipeline instead of being buffered
            chunk = await pipeline.stdout.receive()

        assert chunk.startswith(b"x")

    assert_no_child_processes()


async def test_pipeline_stderr_captured_separately() -> None:
    commands = [
        pipeline_command("import sys; sys.stderr.write('error one\\n')"),
        pipeline_command("import sys; sys.stderr.write('error two\\n')"),
    ]
    async with open_process_pipeline(commands) as pipeline:
        pass

    assert pipeline.stderrs == [b"error one\n", b"error two\n"]
    assert_no_child_processes()


async def test_pipeline_large_stderr_no_deadlock() -> None:
    """A process flooding its stderr pipe must not deadlock the pipeline."""
    command = pipeline_command("import sys; sys.stderr.buffer.write(b'e' * 2_000_000)")
    with fail_after(15):
        async with open_process_pipeline([command, command], check=False) as pipeline:
            pass

    assert pipeline.stderrs == [b"e" * 2_000_000] * 2
    assert_no_child_processes()


async def test_pipeline_early_exit() -> None:
    """A process exiting early must not hang the rest of the pipeline."""
    commands = [
        pipeline_command(
            """
            import sys
            chunk = b"x" * 65536
            while True:
                sys.stdout.buffer.write(chunk)
            """
        ),
        pipeline_command("# exits immediately"),
    ]
    with fail_after(10):
        async with open_process_pipeline(commands, check=False) as pipeline:
            pass

    assert pipeline.returncodes[1] == 0
    assert pipeline.returncodes[0] != 0
    assert_no_child_processes()


async def test_pipeline_missing_middle_command() -> None:
    """
    If a command in the middle fails to start, the already started processes must be
    killed and reaped.
    """
    commands = [
        pipeline_command("import time; time.sleep(30)"),
        ["nonexistent_command_anyio_test"],
        pipeline_command("import sys; sys.stdin.read()"),
    ]
    # A nonexistent command raises FileNotFoundError, but some sandboxed
    # environments report it as PermissionError instead
    with pytest.raises((FileNotFoundError, PermissionError)):
        async with open_process_pipeline(commands):
            pass

    assert_no_child_processes()


async def test_pipeline_check() -> None:
    """The raised error must contain the exit codes of all the stages."""
    commands = [
        pipeline_command("import sys; sys.exit(3)"),
        pipeline_command("pass"),
        pipeline_command("import sys; sys.stderr.write('oops'); sys.exit(5)"),
    ]
    with pytest.raises(ProcessPipelineError) as exc_info:
        async with open_process_pipeline(commands):
            pass

    assert exc_info.value.returncodes == [3, 0, 5]
    assert exc_info.value.stderrs[2] == b"oops"
    assert "exited with code 3" in str(exc_info.value)
    assert "exited with code 5" in str(exc_info.value)
    assert_no_child_processes()


async def test_pipeline_cancelled() -> None:
    """Cancelling the enclosing task must kill and reap all the processes."""
    command = pipeline_command("import time; time.sleep(30)")
    with CancelScope() as scope:
        async with open_process_pipeline([command, command], check=False):
            scope.cancel()
            await sleep(1)

    assert_no_child_processes()


async def test_pipeline_body_exception_kills_processes() -> None:
    """An exception in the enclosed block must kill and reap all the processes."""
    command = pipeline_command("import time; time.sleep(30)")
    with pytest.raises(RuntimeError, match="boom"):
        async with open_process_pipeline([command, command], check=False):
            raise RuntimeError("boom")

    assert_no_child_processes()
