from __future__ import annotations

__all__ = (
    "BufferedByteReceiveStream",
    "BufferedByteStream",
    "BufferedConnectable",
    "Checkpoint",
)

import sys
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, SupportsIndex

from .. import ClosedResourceError, DelimiterNotFound, EndOfStream, IncompleteRead
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


class Checkpoint:
    """
    A handle to an active checkpoint on a :class:`BufferedByteReceiveStream`.

    Instances of this class are returned by
    :meth:`BufferedByteReceiveStream.checkpoint` and should not be instantiated
    directly.

    .. versionadded:: 4.16.0
    """

    def __init__(self, start: int, max_bytes: int) -> None:
        self.start = start
        self.max_bytes = max_bytes
        self.committed = False

    def commit(self) -> None:
        """
        Mark this checkpoint as committed.

        When the checkpoint block is exited, the bytes read since entering it are
        discarded from the buffer (unless an outer checkpoint still covers them)
        instead of the read position being rolled back.
        """
        self.committed = True


@dataclass(eq=False)
class BufferedByteReceiveStream(ByteReceiveStream):
    """
    Wraps any bytes-based receive stream and uses a buffer to provide sophisticated
    receiving capabilities in the form of a byte stream.
    """

    receive_stream: AnyByteReceiveStream
    _buffer: bytearray = field(init=False, default_factory=bytearray)
    _closed: bool = field(init=False, default=False)
    _read_pos: int = field(init=False, default=0)
    _checkpoints: list[Checkpoint] = field(init=False, default_factory=list)

    async def aclose(self) -> None:
        await self.receive_stream.aclose()
        self._closed = True

    @property
    def buffer(self) -> bytes:
        """The bytes currently in the buffer."""
        return bytes(self._buffer[self._read_pos :])

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

    @asynccontextmanager
    async def checkpoint(self, max_bytes: int = 65536) -> AsyncIterator[Checkpoint]:
        """
        Create a checkpoint that allows rolling back bytes read from the stream.

        While the checkpoint is active, all bytes read from the stream are retained in
        the buffer. If the enclosed block exits without the checkpoint having been
        committed – whether normally, via an exception or via cancellation – the read
        position is restored to where it was when the checkpoint was entered, so the
        bytes can be read again. Only calling :meth:`Checkpoint.commit` on the yielded
        checkpoint discards the retained bytes when the block is exited.

        Checkpoints can be nested. Committing an inner checkpoint does not discard any
        bytes still covered by an outer checkpoint, so the outer checkpoint can still
        be rolled back.

        :param max_bytes: the maximum number of bytes to retain in the buffer, counted
            from the position where the checkpoint was entered; if reading more data
            would exceed this limit, :exc:`BufferError` is raised before any
            unrecoverable data is consumed
        :return: an asynchronous context manager yielding a :class:`Checkpoint`

        .. versionadded:: 4.16.0
        """
        if max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")

        checkpoint = Checkpoint(self._read_pos, max_bytes)
        self._checkpoints.append(checkpoint)
        try:
            self._check_retention()
            yield checkpoint
        except BaseException:
            self._rollback(checkpoint)
            raise
        else:
            if checkpoint.committed:
                self._commit(checkpoint)
            else:
                self._rollback(checkpoint)

    def _commit(self, checkpoint: Checkpoint) -> None:
        self._checkpoints.remove(checkpoint)
        if not self._checkpoints:
            self._discard_read_bytes()

    def _rollback(self, checkpoint: Checkpoint) -> None:
        self._checkpoints.remove(checkpoint)
        self._read_pos = checkpoint.start
        if not self._checkpoints:
            self._discard_read_bytes()

    def _discard_read_bytes(self) -> None:
        if self._read_pos:
            del self._buffer[: self._read_pos]
            self._read_pos = 0

    def _check_retention(self) -> None:
        for checkpoint in self._checkpoints:
            if len(self._buffer) - checkpoint.start > checkpoint.max_bytes:
                raise BufferError(
                    f"the checkpoint would retain more than the allowed "
                    f"{checkpoint.max_bytes} bytes"
                )

    def _consume(self, nbytes: int) -> None:
        self._check_retention()
        self._read_pos += nbytes
        if not self._checkpoints:
            self._discard_read_bytes()

    async def receive(self, max_bytes: int = 65536) -> bytes:
        if max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")

        if self._closed:
            raise ClosedResourceError

        if self._read_pos == len(self._buffer):
            if isinstance(self.receive_stream, ByteReceiveStream):
                chunk = await self.receive_stream.receive(max_bytes)
            else:
                # With a bytes-oriented object stream, we need to handle any surplus
                # bytes we get from the receive() call
                chunk = await self.receive_stream.receive()

            if not self._checkpoints:
                # No checkpoints to honor, so no need to retain anything
                if len(chunk) > max_bytes:
                    # Save the surplus bytes in the buffer
                    self._buffer.extend(chunk[max_bytes:])
                    return chunk[:max_bytes]

                return chunk

            self._buffer.extend(chunk)
            self._check_retention()

        end = min(self._read_pos + max_bytes, len(self._buffer))
        chunk = bytes(self._buffer[self._read_pos : end])
        self._consume(end - self._read_pos)
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
            remaining = nbytes - (len(self._buffer) - self._read_pos)
            if remaining <= 0:
                retval = self._buffer[self._read_pos : self._read_pos + nbytes]
                self._consume(nbytes)
                return bytes(retval)

            try:
                if isinstance(self.receive_stream, ByteReceiveStream):
                    chunk = await self.receive_stream.receive(remaining)
                else:
                    chunk = await self.receive_stream.receive()
            except EndOfStream as exc:
                raise IncompleteRead from exc

            self._buffer.extend(chunk)
            self._check_retention()

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
        offset = self._read_pos
        while True:
            # Check if the delimiter can be found in the current buffer
            index = self._buffer.find(delimiter, offset)
            if index >= 0:
                found = self._buffer[self._read_pos : index]
                self._consume(index + delimiter_size - self._read_pos)
                return bytes(found)

            # Check if the buffer is already at or over the limit
            if len(self._buffer) - self._read_pos >= max_bytes:
                raise DelimiterNotFound(max_bytes)

            # Read more data into the buffer from the socket
            try:
                data = await self.receive_stream.receive()
            except EndOfStream as exc:
                raise IncompleteRead from exc

            # Move the offset forward and add the new data to the buffer
            offset = max(len(self._buffer) - delimiter_size + 1, self._read_pos)
            self._buffer.extend(data)
            self._check_retention()


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
