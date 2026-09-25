from __future__ import annotations

import os
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from io import BytesIO
from os import PathLike
from subprocess import PIPE, CalledProcessError, CompletedProcess
from typing import IO, TYPE_CHECKING, Any, TypeAlias, cast

from ..abc import Process  # noqa: TC001
from ..streams.file import FileReadStream
from ._eventloop import get_async_backend
from ._exceptions import ProcessPipelineError
from ._resources import aclose_forcefully
from ._tasks import CancelScope, create_task_group

if TYPE_CHECKING:
    from ..abc import ByteReceiveStream, ByteSendStream

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


class ProcessPipeline:
    """
    A chain of processes connected by pipes, as created by
    :func:`create_process_pipeline`.

    :ivar processes: the processes in the pipeline, in the order their commands were
        given
    :ivar stdin: the stream for the standard input of the first process in the
        pipeline
    :ivar stdout: the stream for the standard output of the last process in the
        pipeline
    :ivar stderr: the captured standard error output of each process, in the same
        order as :attr:`processes`; entries are filled in as the processes terminate
    """

    def __init__(
        self,
        processes: list[Process],
        stdin: ByteSendStream | None,
        stdout: ByteReceiveStream,
        stderr: list[bytes],
    ) -> None:
        self.processes = processes
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

    @property
    def returncodes(self) -> list[int | None]:
        """
        The exit codes of the processes in the pipeline, or ``None`` for processes
        that have not exited yet.
        """
        return [process.returncode for process in self.processes]


@asynccontextmanager
async def create_process_pipeline(
    commands: Sequence[Sequence[StrOrBytesPath]],
    *,
    check: bool = True,
    cwd: StrOrBytesPath | None = None,
    env: Mapping[str, str] | None = None,
) -> AsyncIterator[ProcessPipeline]:
    """
    Start a chain of processes where each process's standard output is piped to the
    standard input of the next one, like ``cmd1 | cmd2 | ...`` in a shell, but without
    involving a shell.

    Data flows through the pipeline as it is produced; it is never buffered in the
    parent process. The standard error output of each process is captured separately
    (see :attr:`ProcessPipeline.stderr`) and drained in the background so that a
    process writing lots of output to stderr cannot deadlock the pipeline.

    When the context manager is exited, the standard input of the first process and
    the standard output of the last process are closed and all the processes are
    waited for. If the context is exited due to an exception (including cancellation),
    any processes still running are killed first. Either way, no processes are left
    running or unreaped. The same cleanup also happens if one of the commands fails
    to start.

    :param commands: the commands to run, each as a sequence of strings (or path-like
        objects) containing the executable name or path and its arguments; plain
        strings are not allowed as the commands are never passed to a shell
    :param check: if ``True``, raise :exc:`~anyio.ProcessPipelineError` on exit if
        any of the processes exited with a nonzero return code
    :param cwd: if not ``None``, the working directory is changed to this before
        running the commands
    :param env: if not ``None``, this mapping replaces the inherited environment
        variables from the parent process
    :return: an asynchronous context manager yielding a :class:`ProcessPipeline`
    :raises ProcessPipelineError: if ``check`` is ``True`` and any of the processes
        exited with a nonzero return code

    """

    async def drain_stderr(stream: ByteReceiveStream, index: int) -> None:
        buffer = BytesIO()
        async for chunk in stream:
            buffer.write(chunk)

        stderr[index] = buffer.getvalue()

    if len(commands) < 2:
        raise ValueError("at least two commands are required")

    for command in commands:
        if isinstance(command, (str, bytes)):
            raise TypeError(
                "each command must be a sequence of program arguments, not a string "
                "(commands in a pipeline are never passed to a shell)"
            )

        if not command:
            raise ValueError("commands must not be empty")

    processes: list[Process] = []
    try:
        read_fd: int | None = None
        for command in commands:
            pipe_read, pipe_write = os.pipe()
            try:
                process = await open_process(
                    command,
                    stdin=read_fd if read_fd is not None else PIPE,
                    stdout=pipe_write,
                    stderr=PIPE,
                    cwd=cwd,
                    env=env,
                )
            except BaseException:
                os.close(pipe_read)
                os.close(pipe_write)
                if read_fd is not None:
                    os.close(read_fd)

                raise

            # The child process holds its own copies of the pipe ends now, so close
            # the parent's copies to allow EOF to propagate through the pipeline
            os.close(pipe_write)
            if read_fd is not None:
                os.close(read_fd)

            read_fd = pipe_read
            processes.append(process)

        assert read_fd is not None
        # Wrap the read end of the last pipe in a stream of our own (rather than
        # using the last process's stdout stream) so that closing it closes the
        # underlying file descriptor, unblocking a last process that is stuck
        # writing to a full pipe
        stdout_stream: ByteReceiveStream = FileReadStream(os.fdopen(read_fd, "rb"))
    except BaseException:
        # One of the commands failed to start; kill and reap the processes that were
        # already started
        with CancelScope(shield=True):
            for process in processes:
                if process.returncode is None:
                    process.kill()

            for process in processes:
                await process.aclose()

        raise

    stderr: list[bytes] = [b""] * len(processes)
    pipeline = ProcessPipeline(processes, processes[0].stdin, stdout_stream, stderr)
    kill_processes = False
    async with create_task_group() as task_group:
        for index, process in enumerate(processes):
            assert process.stderr is not None
            task_group.start_soon(drain_stderr, process.stderr, index)

        try:
            yield pipeline
        except BaseException:
            kill_processes = True
            raise
        finally:
            with CancelScope(shield=True):
                if pipeline.stdin is not None:
                    await aclose_forcefully(pipeline.stdin)

                # Closing the read end of the last pipe lets any process blocked
                # on writing to a full pipe receive EPIPE/SIGPIPE and exit
                await pipeline.stdout.aclose()

                if kill_processes:
                    for process in processes:
                        if process.returncode is None:
                            process.kill()

                for process in processes:
                    await process.wait()

    # All the processes have exited and their stderr has been drained, so they
    # can now be safely closed to release any remaining resources
    with CancelScope(shield=True):
        for process in processes:
            await process.aclose()

    if check:
        returncodes = pipeline.returncodes
        if any(returncode != 0 for returncode in returncodes):
            raise ProcessPipelineError(commands, cast(list[int], returncodes))
