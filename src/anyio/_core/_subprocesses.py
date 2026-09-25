from __future__ import annotations

import os
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from io import BytesIO
from os import PathLike
from subprocess import PIPE, CalledProcessError, CompletedProcess
from typing import IO, Any, TypeAlias, cast

from ..abc import ByteReceiveStream, ByteSendStream, Process  # noqa: TC001
from ._eventloop import get_async_backend
from ._tasks import CancelScope, create_task_group

StrOrBytesPath: TypeAlias = str | bytes | PathLike[str] | PathLike[bytes]


async def run_process(
    command: StrOrBytesPath | Sequence[StrOrBytesPath],
    *,
    input: bytes | None = None,
    stdin: int | IO[Any] | None = None,
    stdout: int | IO[Any] | None = PIPE,
    stderr: int | IO[Any] | None = PIPE,
    check: bool = True,
    cwd: StrOrBytesPath | None = None,
    env: Mapping[str, str] | None = None,
    startupinfo: Any = None,
    creationflags: int = 0,
    start_new_session: bool = False,
    pass_fds: Sequence[int] = (),
    user: str | int | None = None,
    group: str | int | None = None,
    extra_groups: Iterable[str | int] | None = None,
    umask: int = -1,
) -> CompletedProcess[bytes]:
    """
    Run an external command in a subprocess and wait until it completes.

    .. seealso:: :func:`subprocess.run`

    :param command: either a string to pass to the shell, or an iterable of strings
        containing the executable name or path and its arguments
    :param input: bytes passed to the standard input of the subprocess
    :param stdin: one of :data:`subprocess.PIPE`, :data:`subprocess.DEVNULL`,
        a file-like object, or `None`; ``input`` overrides this
    :param stdout: one of :data:`subprocess.PIPE`, :data:`subprocess.DEVNULL`,
        a file-like object, or `None`
    :param stderr: one of :data:`subprocess.PIPE`, :data:`subprocess.DEVNULL`,
        :data:`subprocess.STDOUT`, a file-like object, or `None`
    :param check: if ``True``, raise :exc:`~subprocess.CalledProcessError` if the
        process terminates with a return code other than 0
    :param cwd: If not ``None``, change the working directory to this before running the
        command
    :param env: if not ``None``, this mapping replaces the inherited environment
        variables from the parent process
    :param startupinfo: an instance of :class:`subprocess.STARTUPINFO` that can be used
        to specify process startup parameters (Windows only)
    :param creationflags: flags that can be used to control the creation of the
        subprocess (see :class:`subprocess.Popen` for the specifics)
    :param start_new_session: if ``true`` the setsid() system call will be made in the
        child process prior to the execution of the subprocess. (POSIX only)
    :param pass_fds: sequence of file descriptors to keep open between the parent and
        child processes. (POSIX only)
    :param user: effective user to run the process as (Python >= 3.9, POSIX only)
    :param group: effective group to run the process as (Python >= 3.9, POSIX only)
    :param extra_groups: supplementary groups to set in the subprocess (Python >= 3.9,
        POSIX only)
    :param umask: if not negative, this umask is applied in the child process before
        running the given command (Python >= 3.9, POSIX only)
    :return: an object representing the completed process
    :raises ~subprocess.CalledProcessError: if ``check`` is ``True`` and the process
        exits with a nonzero return code

    """

    async def drain_stream(stream: AsyncIterable[bytes], index: int) -> None:
        buffer = BytesIO()
        async for chunk in stream:
            buffer.write(chunk)

        stream_contents[index] = buffer.getvalue()

    if stdin is not None and input is not None:
        raise ValueError("only one of stdin and input is allowed")

    async with await open_process(
        command,
        stdin=PIPE if input else stdin,
        stdout=stdout,
        stderr=stderr,
        cwd=cwd,
        env=env,
        startupinfo=startupinfo,
        creationflags=creationflags,
        start_new_session=start_new_session,
        pass_fds=pass_fds,
        user=user,
        group=group,
        extra_groups=extra_groups,
        umask=umask,
    ) as process:
        stream_contents: list[bytes | None] = [None, None]
        async with create_task_group() as tg:
            if process.stdout:
                tg.start_soon(drain_stream, process.stdout, 0)

            if process.stderr:
                tg.start_soon(drain_stream, process.stderr, 1)

            if process.stdin and input:
                await process.stdin.send(input)
                await process.stdin.aclose()

            await process.wait()

    output, errors = stream_contents
    if check and process.returncode != 0:
        raise CalledProcessError(cast(int, process.returncode), command, output, errors)

    return CompletedProcess(command, cast(int, process.returncode), output, errors)


async def open_process(
    command: StrOrBytesPath | Sequence[StrOrBytesPath],
    *,
    stdin: int | IO[Any] | None = PIPE,
    stdout: int | IO[Any] | None = PIPE,
    stderr: int | IO[Any] | None = PIPE,
    cwd: StrOrBytesPath | None = None,
    env: Mapping[str, str] | None = None,
    startupinfo: Any = None,
    creationflags: int = 0,
    start_new_session: bool = False,
    pass_fds: Sequence[int] = (),
    user: str | int | None = None,
    group: str | int | None = None,
    extra_groups: Iterable[str | int] | None = None,
    umask: int = -1,
) -> Process:
    """
    Start an external command in a subprocess.

    .. seealso:: :class:`subprocess.Popen`

    :param command: either a string to pass to the shell, or an iterable of strings
        containing the executable name or path and its arguments
    :param stdin: one of :data:`subprocess.PIPE`, :data:`subprocess.DEVNULL`, a
        file-like object, or ``None``
    :param stdout: one of :data:`subprocess.PIPE`, :data:`subprocess.DEVNULL`,
        a file-like object, or ``None``
    :param stderr: one of :data:`subprocess.PIPE`, :data:`subprocess.DEVNULL`,
        :data:`subprocess.STDOUT`, a file-like object, or ``None``
    :param cwd: If not ``None``, the working directory is changed before executing
    :param env: If env is not ``None``, it must be a mapping that defines the
        environment variables for the new process
    :param creationflags: flags that can be used to control the creation of the
        subprocess (see :class:`subprocess.Popen` for the specifics)
    :param startupinfo: an instance of :class:`subprocess.STARTUPINFO` that can be used
        to specify process startup parameters (Windows only)
    :param start_new_session: if ``true`` the setsid() system call will be made in the
        child process prior to the execution of the subprocess. (POSIX only)
    :param pass_fds: sequence of file descriptors to keep open between the parent and
        child processes. (POSIX only)
    :param user: effective user to run the process as (POSIX only)
    :param group: effective group to run the process as (POSIX only)
    :param extra_groups: supplementary groups to set in the subprocess (POSIX only)
    :param umask: if not negative, this umask is applied in the child process before
        running the given command (POSIX only)
    :return: an asynchronous process object

    """
    kwargs: dict[str, Any] = {}
    if user is not None:
        kwargs["user"] = user

    if group is not None:
        kwargs["group"] = group

    if extra_groups is not None:
        kwargs["extra_groups"] = extra_groups

    if umask >= 0:
        kwargs["umask"] = umask

    return await get_async_backend().open_process(
        command,
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        cwd=cwd,
        env=env,
        startupinfo=startupinfo,
        creationflags=creationflags,
        start_new_session=start_new_session,
        pass_fds=pass_fds,
        **kwargs,
    )


class ProcessPipelineError(Exception):
    """
    Raised by :func:`open_process_pipeline` when one or more processes in the pipeline
    exit with a non-zero exit code and ``check`` is ``True``.

    :ivar commands: the commands that were run in the pipeline
    :ivar returncodes: the exit codes of all the processes in the pipeline, in command
        order
    :ivar stderrs: the captured standard error output of each process, in command
        order (``None`` for processes whose standard error was not captured)
    """

    def __init__(
        self,
        commands: Sequence[Sequence[StrOrBytesPath]],
        returncodes: Sequence[int],
        stderrs: Sequence[bytes | None],
    ) -> None:
        self.commands: list[Sequence[StrOrBytesPath]] = list(commands)
        self.returncodes: list[int] = list(returncodes)
        self.stderrs: list[bytes | None] = list(stderrs)
        failures = ", ".join(
            f"command {index} ({command!r}) exited with code {returncode}"
            for index, (command, returncode) in enumerate(
                zip(self.commands, self.returncodes, strict=True)
            )
            if returncode != 0
        )
        super().__init__(f"process pipeline failed: {failures}")


@dataclass
class ProcessPipeline:
    """
    A chain of processes connected by pipes, as returned by
    :func:`open_process_pipeline`.

    :ivar processes: the processes in the pipeline, in command order
    :ivar stdin: the stream for the standard input of the first process, or ``None``
        if ``stdin`` was not :data:`subprocess.PIPE`
    :ivar stdout: the stream for the standard output of the last process, or ``None``
        if ``stdout`` was not :data:`subprocess.PIPE`
    :ivar returncodes: the exit codes of the processes, in command order (filled in
        when the context manager exits)
    :ivar stderrs: the captured standard error output of each process, in command
        order (filled in when the context manager exits)
    """

    processes: list[Process]
    stdin: ByteSendStream | None
    stdout: ByteReceiveStream | None
    returncodes: list[int] = field(default_factory=list)
    stderrs: list[bytes | None] = field(default_factory=list)


@asynccontextmanager
async def open_process_pipeline(
    commands: Sequence[Sequence[StrOrBytesPath]],
    *,
    stdin: int | IO[Any] | None = PIPE,
    stdout: int | IO[Any] | None = PIPE,
    stderr: int | IO[Any] | None = PIPE,
    check: bool = True,
    cwd: StrOrBytesPath | None = None,
    env: Mapping[str, str] | None = None,
    startupinfo: Any = None,
    creationflags: int = 0,
    start_new_session: bool = False,
    pass_fds: Sequence[int] = (),
    user: str | int | None = None,
    group: str | int | None = None,
    extra_groups: Iterable[str | int] | None = None,
    umask: int = -1,
) -> AsyncIterator[ProcessPipeline]:
    """
    Start a chain of processes where each process feeds its standard output to the
    standard input of the next one, like the ``|`` operator in a shell but without
    involving a shell.

    The processes are connected directly with operating system pipes, so the
    intermediate data streams from one process to the next without passing through
    the Python process. The standard error output of every process is captured
    separately (when ``stderr`` is :data:`subprocess.PIPE`) and drained in the
    background so that a process filling up its stderr pipe cannot deadlock the
    pipeline.

    If any process fails to start, all previously started processes are killed and
    reaped before the exception is raised. Likewise, if the enclosed code block
    raises an exception or the enclosing task is cancelled, all remaining processes
    are killed and reaped before the exception is propagated.

    Example::

        async with open_process_pipeline([["gunzip", "data.gz"], ["parser"]]) as pipe:
            async for chunk in pipe.stdout:
                ...

    :param commands: a sequence of at least two commands, each a sequence of strings
        (or path-like objects) containing the executable name or path and its
        arguments
    :param stdin: one of :data:`subprocess.PIPE`, :data:`subprocess.DEVNULL`, a
        file-like object, or ``None``; used as the standard input of the first
        process
    :param stdout: one of :data:`subprocess.PIPE`, :data:`subprocess.DEVNULL`, a
        file-like object, or ``None``; used as the standard output of the last
        process
    :param stderr: one of :data:`subprocess.PIPE`, :data:`subprocess.DEVNULL`,
        :data:`subprocess.STDOUT`, a file-like object, or ``None``; used as the
        standard error output of every process
    :param check: if ``True``, raise :exc:`ProcessPipelineError` on context manager
        exit if any of the processes exited with a non-zero exit code
    :param cwd: If not ``None``, the working directory is changed before executing
        the commands
    :param env: If env is not ``None``, it must be a mapping that defines the
        environment variables for the new processes
    :param startupinfo: an instance of :class:`subprocess.STARTUPINFO` that can be
        used to specify process startup parameters (Windows only)
    :param creationflags: flags that can be used to control the creation of the
        subprocess (see :class:`subprocess.Popen` for the specifics) (Windows only)
    :param start_new_session: if ``true`` the setsid() system call will be made in
        the child process prior to the execution of the subprocess. (POSIX only)
    :param pass_fds: sequence of file descriptors to keep open between the parent
        and child processes. (POSIX only)
    :param user: effective user to run the processes as (POSIX only)
    :param group: effective group to run the processes as (POSIX only)
    :param extra_groups: supplementary groups to set in the processes (POSIX only)
    :param umask: if not negative, this umask is applied in the child processes
        before running the given commands (POSIX only)
    :return: an asynchronous context manager yielding a :class:`ProcessPipeline`
    :raises ProcessPipelineError: if ``check`` is ``True`` and any of the processes
        exits with a non-zero exit code

    """
    if len(commands) < 2:
        raise ValueError("at least two commands are required to form a pipeline")

    for command in commands:
        if isinstance(command, (str, bytes)):
            raise TypeError(
                "commands in a pipeline must be sequences of arguments, not strings"
            )

        if not command:
            raise ValueError("commands in a pipeline must not be empty")

    async def drain_stderr(stream: ByteReceiveStream, index: int) -> None:
        buffer = BytesIO()
        async for chunk in stream:
            buffer.write(chunk)

        stderr_contents[index] = buffer.getvalue()

    # Create the pipes that connect adjacent processes
    pipes: list[tuple[int, int]] = []
    parent_fds: set[int] = set()
    processes: list[Process] = []
    try:
        for _ in range(len(commands) - 1):
            read_fd, write_fd = os.pipe()
            pipes.append((read_fd, write_fd))
            parent_fds.update((read_fd, write_fd))

        for index, command in enumerate(commands):
            processes.append(
                await open_process(
                    command,
                    stdin=stdin if index == 0 else pipes[index - 1][0],
                    stdout=stdout if index == len(commands) - 1 else pipes[index][1],
                    stderr=stderr,
                    cwd=cwd,
                    env=env,
                    startupinfo=startupinfo,
                    creationflags=creationflags,
                    start_new_session=start_new_session,
                    pass_fds=pass_fds,
                    user=user,
                    group=group,
                    extra_groups=extra_groups,
                    umask=umask,
                )
            )

            # Close the parent's copies of the pipe ends now owned by the children
            if index > 0:
                os.close(pipes[index - 1][0])
                parent_fds.discard(pipes[index - 1][0])

            if index < len(commands) - 1:
                os.close(pipes[index][1])
                parent_fds.discard(pipes[index][1])
    except BaseException:
        for fd in parent_fds:
            os.close(fd)

        for process in processes:
            if process.returncode is None:
                process.kill()

        with CancelScope(shield=True):
            for process in processes:
                await process.wait()

        raise

    pipeline = ProcessPipeline(
        processes=processes,
        stdin=processes[0].stdin,
        stdout=processes[-1].stdout,
    )
    stderr_contents: list[bytes | None] = [None] * len(processes)
    body_exception: BaseException | None = None
    async with create_task_group() as tg:
        for index, process in enumerate(processes):
            if process.stderr is not None:
                tg.start_soon(drain_stderr, process.stderr, index)

        try:
            try:
                yield pipeline
            except BaseException as exc:
                # Defer re-raising until the task group has exited, so that the
                # exception does not get wrapped in an ExceptionGroup
                body_exception = exc
                for process in processes:
                    if process.returncode is None:
                        process.kill()
        finally:
            with CancelScope(shield=True) as scope:
                if pipeline.stdin is not None:
                    await pipeline.stdin.aclose()

                if pipeline.stdout is not None:
                    await pipeline.stdout.aclose()

                scope.shield = False
                try:
                    for process in processes:
                        await process.wait()
                except BaseException:
                    scope.shield = True
                    for process in processes:
                        if process.returncode is None:
                            process.kill()

                    for process in processes:
                        await process.wait()

                    raise

    if body_exception is not None:
        raise body_exception

    pipeline.returncodes = [cast(int, process.returncode) for process in processes]
    pipeline.stderrs = stderr_contents
    if check and any(returncode != 0 for returncode in pipeline.returncodes):
        raise ProcessPipelineError(commands, pipeline.returncodes, pipeline.stderrs)
