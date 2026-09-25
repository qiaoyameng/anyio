from __future__ import annotations

__all__ = (
    "BufferedByteReceiveStream",
    "BufferedByteStream",
    "BufferedConnectable",
    "BufferedReceiveCheckpoint",
)

import sys
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, SupportsIndex

from .. import (
    BufferFullError,
    ClosedResourceError,
    DelimiterNotFound,
    EndOfStream,
    IncompleteRead,
)
from ..abc import (
    AnyByteReceiveStream,
    AnyByteStream,
    AnyByteStreamConnectable,
    ByteReceiveStream,
    ByteStream,
    ByteStreamConnectable,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


@dataclass(eq=False)
class BufferedByteReceiveStream(ByteReceiveStream):
    """
    Wraps any bytes-based receive stream and uses a buffer to provide sophisticated
    receiving capabilities in the form of a byte stream.
    """

    receive_stream: AnyByteReceiveStream
    _buffer: bytearray = field(init=False, default_factory=bytearray)
    _closed: bool = field(init=False, default=False)
    _offset: int = field(init=False, default=0)
    _checkpoints: list[BufferedReceiveCheckpoint] = field(
        init=False, default_factory=list
    )

    async def aclose(self) -> None:
        await self.receive_stream.aclose()
        self._closed = True

    @property
    def buffer(self) -> bytes:
        """The bytes currently in the buffer."""
        return bytes(self._buffer[self._offset :])

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self.receive_stream.extra_attributes

    def feed_data(self, data: Iterable[SupportsIndex], /) -> None:
        """
        Append data directly into the buffer.

        Any data in the buffer will be consumed by receive operations before receiving
        anything from the wrapped stream.

        :param data: the data to append to the buffer (can be bytes or anything else
            that supports ``__index__()``)

        """
        self._buffer.extend(data)

    def checkpoint(self, max_bytes: int) -> BufferedReceiveCheckpoint:
        """
        Return a context manager that allows received bytes to be rolled back.

        While the context manager is active, all bytes consumed from the stream are
        retained in the buffer. If :meth:`~BufferedReceiveCheckpoint.commit` is
        called on it before the context is exited, the consumed bytes are discarded
        as usual. Otherwise — whether the context is exited normally, via an
        exception or via cancellation — the receive position is restored to where it
        was when the context was entered, as if none of the bytes had been consumed.

        Checkpoints can be nested. Committing an inner checkpoint does not prevent an
        outer checkpoint from rolling back the bytes received while the inner
        checkpoint was active.

        :param max_bytes: the maximum number of uncommitted bytes to retain in the
            buffer while the checkpoint is active (including any bytes already in
            the buffer when the checkpoint is entered); if a receive operation would
            exceed this limit, :exc:`~anyio.BufferFullError` is raised before any
            unrecoverable data is consumed
        :return: the checkpoint context manager

        """
        return BufferedReceiveCheckpoint(self, max_bytes)

    def _consume(self, nbytes: int) -> None:
        if self._checkpoints:
            self._offset += nbytes
        else:
            del self._buffer[:nbytes]

    def _check_checkpoint_limits(self) -> None:
        for checkpoint in self._checkpoints:
            if len(self._buffer) - checkpoint._mark > checkpoint.max_bytes:
                raise BufferFullError(checkpoint.max_bytes)

    async def _receive_more(self, max_bytes: int) -> None:
        if self._checkpoints:
            checkpoint = min(
                self._checkpoints,
                key=lambda cp: cp.max_bytes - (len(self._buffer) - cp._mark),
            )
            room = checkpoint.max_bytes - (len(self._buffer) - checkpoint._mark)
            if room < 1:
                raise BufferFullError(checkpoint.max_bytes)

            max_bytes = min(max_bytes, room)

        if isinstance(self.receive_stream, ByteReceiveStream):
            chunk = await self.receive_stream.receive(max_bytes)
        else:
            # Object streams can return chunks of arbitrary size, so the checkpoint
            # limits have to be checked after receiving from them
            chunk = await self.receive_stream.receive()

        self._buffer.extend(chunk)
        self._check_checkpoint_limits()

    async def receive(self, max_bytes: int = 65536) -> bytes:
        if max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")

        if self._closed:
            raise ClosedResourceError

        if self._offset == len(self._buffer):
            if not self._checkpoints and isinstance(
                self.receive_stream, ByteReceiveStream
            ):
                return await self.receive_stream.receive(max_bytes)

            await self._receive_more(max_bytes)

        chunk = bytes(self._buffer[self._offset : self._offset + max_bytes])
        self._consume(len(chunk))
        return chunk

    async def receive_exactly(self, nbytes: int) -> bytes:
        """
        Read exactly the given amount of bytes from the stream.

        :param nbytes: the number of bytes to read
        :return: the bytes read
        :raises ~anyio.IncompleteRead: if the stream was closed before the requested
            amount of bytes could be read from the stream

        """
        while True:
            remaining = nbytes - (len(self._buffer) - self._offset)
            if remaining <= 0:
                retval = self._buffer[self._offset : self._offset + nbytes]
                self._consume(nbytes)
                return bytes(retval)

            try:
                await self._receive_more(remaining)
            except EndOfStream as exc:
                raise IncompleteRead from exc

    async def receive_until(self, delimiter: bytes, max_bytes: int) -> bytes:
        """
        Read from the stream until the delimiter is found or max_bytes have been read.

        :param delimiter: the marker to look for in the stream
        :param max_bytes: maximum number of bytes that will be read before raising
            :exc:`~anyio.DelimiterNotFound`
        :return: the bytes read (not including the delimiter)
        :raises ~anyio.IncompleteRead: if the stream was closed before the delimiter
            was found
        :raises ~anyio.DelimiterNotFound: if the delimiter is not found within the
            bytes read up to the maximum allowed

        """
        delimiter_size = len(delimiter)
        offset = 0
        while True:
            # Check if the delimiter can be found in the current buffer
            index = self._buffer.find(delimiter, self._offset + offset)
            if index >= 0:
                found = self._buffer[self._offset : index]
                self._consume(index + delimiter_size - self._offset)
                return bytes(found)

            # Check if the buffer is already at or over the limit
            if len(self._buffer) - self._offset >= max_bytes:
                raise DelimiterNotFound(max_bytes)

            # Move the offset forward to the start of a potential partial match
            offset = max(len(self._buffer) - self._offset - delimiter_size + 1, 0)

            # Read more data into the buffer from the socket
            try:
                await self._receive_more(65536)
            except EndOfStream as exc:
                raise IncompleteRead from exc


class BufferedReceiveCheckpoint:
    """
    A context manager, returned by
    :meth:`BufferedByteReceiveStream.checkpoint`, that retains the bytes received
    while it is active so that they can be rolled back.

    Call :meth:`commit` before exiting the context to discard the retained bytes.
    If the context is exited without a commit — normally, via an exception or via
    cancellation — the receive position of the stream is restored to where it was
    when the context was entered.
    """

    def __init__(self, stream: BufferedByteReceiveStream, max_bytes: int) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")

        self._stream = stream
        self.max_bytes = max_bytes
        self._mark = 0
        self._entered = False
        self._active = False
        self._committed = False

    def __enter__(self) -> BufferedReceiveCheckpoint:
        if self._entered:
            raise RuntimeError("this checkpoint has already been entered")

        self._entered = True
        self._active = True
        self._mark = self._stream._offset
        self._stream._checkpoints.append(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._active = False
        checkpoints = self._stream._checkpoints
        if not checkpoints or checkpoints[-1] is not self:
            raise RuntimeError("checkpoints must be exited in reverse order of entry")

        checkpoints.pop()
        if not self._committed:
            self._stream._offset = self._mark

        if not checkpoints:
            # No checkpoints remain active, so the consumed bytes can be discarded
            del self._stream._buffer[: self._stream._offset]
            self._stream._offset = 0

    def commit(self) -> None:
        """
        Discard the bytes retained by this checkpoint.

        The bytes consumed from the stream while this checkpoint has been active
        will not be rolled back when the context is exited (unless an outer
        checkpoint is rolled back).

        """
        if not self._active:
            raise RuntimeError("this checkpoint is not active")

        if self._committed:
            raise RuntimeError("this checkpoint has already been committed")

        self._committed = True


class BufferedByteStream(BufferedByteReceiveStream, ByteStream):
    """
    A full-duplex variant of :class:`BufferedByteReceiveStream`. All writes are passed
    through to the wrapped stream as-is.
    """

    def __init__(self, stream: AnyByteStream):
        """
        :param stream: the stream to be wrapped

        """
        super().__init__(stream)
        self._stream = stream

    @override
    async def send_eof(self) -> None:
        await self._stream.send_eof()

    @override
    async def send(self, item: bytes) -> None:
        await self._stream.send(item)


class BufferedConnectable(ByteStreamConnectable):
    """
    Wraps a byte stream connectable to produce :class:`BufferedByteStream` connections.

    Use this when you want the streams returned by :meth:`connect` to have the buffered
    receive API (e.g. :meth:`~BufferedByteReceiveStream.receive_exactly` and
    :meth:`~BufferedByteReceiveStream.receive_until`).

    :param connectable: the byte stream connectable to wrap
    """

    def __init__(self, connectable: AnyByteStreamConnectable):
        """
        :param connectable: the connectable to wrap

        """
        self.connectable = connectable

    @override
    async def connect(self) -> BufferedByteStream:
        stream = await self.connectable.connect()
        return BufferedByteStream(stream)
