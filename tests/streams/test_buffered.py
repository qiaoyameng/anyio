from __future__ import annotations

import pytest

from anyio import (
    BufferFullError,
    ClosedResourceError,
    EndOfStream,
    IncompleteRead,
    create_memory_object_stream,
    move_on_after,
    sleep_forever,
)
from anyio.abc import ByteReceiveStream, ObjectStream, ObjectStreamConnectable
from anyio.streams.buffered import (
    BufferedByteReceiveStream,
    BufferedByteStream,
    BufferedConnectable,
)
from anyio.streams.stapled import StapledObjectStream


class FragmentedByteReceiveStream(ByteReceiveStream):
    """A byte receive stream that serves its data in fragments of a fixed size."""

    def __init__(self, data: bytes, fragment_size: int) -> None:
        self._buffer = bytearray(data)
        self._fragment_size = fragment_size

    async def receive(self, max_bytes: int = 65536) -> bytes:
        if not self._buffer:
            raise EndOfStream

        size = min(len(self._buffer), self._fragment_size, max_bytes)
        fragment = bytes(self._buffer[:size])
        del self._buffer[:size]
        return fragment

    async def aclose(self) -> None:
        pass


async def test_receive_exactly() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](2)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcd")
    await send_stream.send(b"efgh")
    result = await buffered_stream.receive_exactly(8)
    assert result == b"abcdefgh"
    assert isinstance(result, bytes)

    send_stream.close()
    receive_stream.close()


async def test_receive_exactly_incomplete() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcd")
    await send_stream.aclose()
    with pytest.raises(IncompleteRead):
        await buffered_stream.receive_exactly(8)

    send_stream.close()
    receive_stream.close()


async def test_receive_until() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](2)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcd")
    await send_stream.send(b"efgh")

    result = await buffered_stream.receive_until(b"de", 10)
    assert result == b"abc"
    assert isinstance(result, bytes)

    result = await buffered_stream.receive_until(b"h", 10)
    assert result == b"fg"
    assert isinstance(result, bytes)

    send_stream.close()
    receive_stream.close()


async def test_receive_until_incomplete() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcd")
    await send_stream.aclose()
    with pytest.raises(IncompleteRead):
        assert await buffered_stream.receive_until(b"de", 10)

    assert buffered_stream.buffer == b"abcd"

    send_stream.close()
    receive_stream.close()


async def test_buffered_stream() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteStream(
        StapledObjectStream(send_stream, receive_stream)
    )
    await send_stream.send(b"abcd")
    assert await buffered_stream.receive_exactly(2) == b"ab"
    assert await buffered_stream.receive_exactly(2) == b"cd"

    # send_eof() should close only the sending end
    await buffered_stream.send_eof()
    pytest.raises(ClosedResourceError, send_stream.send_nowait, b"abc")
    pytest.raises(EndOfStream, receive_stream.receive_nowait)

    # aclose() closes the receive stream too
    await buffered_stream.aclose()
    pytest.raises(ClosedResourceError, receive_stream.receive_nowait)


async def test_buffered_connectable() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    memory_stream = StapledObjectStream(send_stream, receive_stream)

    class MemoryObjectConnectable(ObjectStreamConnectable[bytes]):
        async def connect(self) -> ObjectStream[bytes]:
            return memory_stream

    connectable = BufferedConnectable(MemoryObjectConnectable())
    async with await connectable.connect() as stream:
        assert isinstance(stream, BufferedByteStream)
        await stream.send(b"abcd")
        assert await stream.receive_exactly(2) == b"ab"
        assert await stream.receive_exactly(2) == b"cd"


@pytest.mark.parametrize("max_bytes", [0, -1])
async def test_receive_invalid_max_bytes(max_bytes: int) -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    with pytest.raises(ValueError, match="max_bytes must be a positive integer"):
        await buffered_stream.receive(max_bytes)

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_rollback() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    with buffered_stream.checkpoint(100):
        assert await buffered_stream.receive_exactly(4) == b"abcd"
        assert buffered_stream.buffer == b"ef"

    # The checkpoint was not committed, so the bytes can be read again
    assert await buffered_stream.receive_exactly(6) == b"abcdef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_commit() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    with buffered_stream.checkpoint(100) as checkpoint:
        assert await buffered_stream.receive_exactly(2) == b"ab"
        checkpoint.commit()

    assert await buffered_stream.receive_exactly(4) == b"cdef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_rollback_on_exception() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcd")
    with pytest.raises(ValueError, match="bogus"):
        with buffered_stream.checkpoint(100):
            assert await buffered_stream.receive_exactly(2) == b"ab"
            raise ValueError("bogus")

    assert await buffered_stream.receive_exactly(4) == b"abcd"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_rollback_on_cancellation() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"ab")
    with move_on_after(0.1):
        with buffered_stream.checkpoint(100):
            assert await buffered_stream.receive_exactly(2) == b"ab"
            await sleep_forever()

    assert await buffered_stream.receive_exactly(2) == b"ab"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_nested_inner_commit() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    with buffered_stream.checkpoint(100) as outer:
        assert await buffered_stream.receive_exactly(2) == b"ab"
        with buffered_stream.checkpoint(100) as inner:
            assert await buffered_stream.receive_exactly(2) == b"cd"
            inner.commit()

        assert await buffered_stream.receive_exactly(2) == b"ef"

    # The inner commit must not prevent the outer checkpoint from rolling back
    assert await buffered_stream.receive_exactly(6) == b"abcdef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_nested_inner_rollback() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    with buffered_stream.checkpoint(100) as outer:
        assert await buffered_stream.receive_exactly(2) == b"ab"
        with buffered_stream.checkpoint(100):
            assert await buffered_stream.receive_exactly(2) == b"cd"

        # The inner checkpoint was rolled back, so the same bytes are read again
        assert await buffered_stream.receive_exactly(2) == b"cd"
        outer.commit()

    assert await buffered_stream.receive_exactly(2) == b"ef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_max_bytes_exceeded() -> None:
    buffered_stream = BufferedByteReceiveStream(
        FragmentedByteReceiveStream(b"abcdef", 2)
    )
    with pytest.raises(BufferFullError, match="cannot retain more than 3 bytes"):
        with buffered_stream.checkpoint(3):
            assert await buffered_stream.receive_exactly(2) == b"ab"
            await buffered_stream.receive_exactly(2)

    # The error must be raised before any unrecoverable data is consumed
    assert await buffered_stream.receive_exactly(6) == b"abcdef"


async def test_checkpoint_max_bytes_oversized_chunk() -> None:
    # Object streams can return chunks larger than what was requested, but the
    # checkpoint limit must still be enforced
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    with pytest.raises(BufferFullError, match="cannot retain more than 3 bytes"):
        with buffered_stream.checkpoint(3):
            await buffered_stream.receive_exactly(2)

    assert await buffered_stream.receive_exactly(6) == b"abcdef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_rollback_after_eof() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abc")
    await send_stream.aclose()
    with pytest.raises(IncompleteRead):
        with buffered_stream.checkpoint(100):
            await buffered_stream.receive_exactly(5)

    # The bytes received before the EOF can still be read after the rollback
    assert await buffered_stream.receive_exactly(3) == b"abc"
    with pytest.raises(IncompleteRead):
        await buffered_stream.receive_exactly(1)

    receive_stream.close()


async def test_checkpoint_closed_stream() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abc")
    await buffered_stream.aclose()
    with buffered_stream.checkpoint(100):
        with pytest.raises(ClosedResourceError):
            await buffered_stream.receive()

    # A closed stream cannot be used again, even after a rollback
    with pytest.raises(ClosedResourceError):
        await buffered_stream.receive()

    send_stream.close()


async def test_checkpoint_commit_after_exit() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    with buffered_stream.checkpoint(100) as checkpoint:
        pass

    with pytest.raises(RuntimeError, match="this checkpoint is not active"):
        checkpoint.commit()

    send_stream.close()
    receive_stream.close()


@pytest.mark.parametrize("max_bytes", [0, -1])
async def test_checkpoint_invalid_max_bytes(max_bytes: int) -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    with pytest.raises(ValueError, match="max_bytes must be a positive integer"):
        buffered_stream.checkpoint(max_bytes)

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_commit_mixed_receive_operations() -> None:
    data = b"GET /resource HTTP/1.1\r\nHost: example.org\r\n\r\n<request body>"
    buffered_stream = BufferedByteReceiveStream(FragmentedByteReceiveStream(data, 3))
    received = bytearray()
    with buffered_stream.checkpoint(len(data)) as checkpoint:
        received += await buffered_stream.receive_until(b"\r\n", 100) + b"\r\n"
        received += await buffered_stream.receive_exactly(6)
        received += await buffered_stream.receive(7)
        checkpoint.commit()

    while True:
        try:
            received += await buffered_stream.receive(5)
        except EndOfStream:
            break

    assert bytes(received) == data


async def test_checkpoint_rollback_mixed_receive_operations() -> None:
    data = b"GET /resource HTTP/1.1\r\nHost: example.org\r\n\r\n<request body>"
    buffered_stream = BufferedByteReceiveStream(FragmentedByteReceiveStream(data, 3))
    with buffered_stream.checkpoint(len(data)):
        await buffered_stream.receive_until(b"\r\n", 100)
        await buffered_stream.receive_exactly(6)
        await buffered_stream.receive(7)

    received = bytearray()
    while True:
        try:
            received += await buffered_stream.receive(5)
        except EndOfStream:
            break

    assert bytes(received) == data


@pytest.mark.parametrize("max_bytes", [0, -1])
async def test_buffered_stream_receive_invalid_max_bytes(max_bytes: int) -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteStream(
        StapledObjectStream(send_stream, receive_stream)
    )
    with pytest.raises(ValueError, match="max_bytes must be a positive integer"):
        await buffered_stream.receive(max_bytes)

    await buffered_stream.aclose()


async def test_feed_data() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteStream(
        StapledObjectStream(send_stream, receive_stream)
    )
    send_stream.send_nowait(b"abcd")

    # The stream has not received the sent data yet, so b"xxx" should come out of the
    # buffer first, despite this order of data input
    buffered_stream.feed_data(b"xxx")
    buffered_stream.feed_data(b"foo")
    assert await buffered_stream.receive_exactly(10) == b"xxxfooabcd"
    await buffered_stream.aclose()
