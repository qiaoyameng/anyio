from __future__ import annotations

import pytest

from anyio import (
    ClosedResourceError,
    EndOfStream,
    IncompleteRead,
    create_memory_object_stream,
    move_on_after,
    sleep,
)
from anyio.abc import ByteReceiveStream, ObjectStream, ObjectStreamConnectable
from anyio.streams.buffered import (
    BufferedByteReceiveStream,
    BufferedByteStream,
    BufferedConnectable,
)
from anyio.streams.stapled import StapledObjectStream


class FragmentedByteReceiveStream(ByteReceiveStream):
    """A byte receive stream that yields the given fragments, one at a time."""

    def __init__(self, fragments: list[bytes]) -> None:
        self._fragments = fragments
        self._closed = False

    async def receive(self, max_bytes: int = 65536) -> bytes:
        if self._closed:
            raise ClosedResourceError

        if not self._fragments:
            raise EndOfStream

        fragment = self._fragments.pop(0)
        if len(fragment) > max_bytes:
            self._fragments.insert(0, fragment[max_bytes:])
            fragment = fragment[:max_bytes]

        return fragment

    async def aclose(self) -> None:
        self._closed = True


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


async def test_checkpoint_commit() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    async with buffered_stream.checkpoint() as checkpoint:
        assert await buffered_stream.receive_exactly(3) == b"abc"
        assert buffered_stream.buffer == b"def"
        checkpoint.commit()

    assert buffered_stream.buffer == b"def"
    assert await buffered_stream.receive_exactly(3) == b"def"
    assert buffered_stream.buffer == b""

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_rollback_without_commit() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    async with buffered_stream.checkpoint():
        assert await buffered_stream.receive_exactly(3) == b"abc"

    # The checkpoint was not committed, so the bytes must be readable again
    assert await buffered_stream.receive_exactly(6) == b"abcdef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_rollback_on_exception() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    with pytest.raises(ValueError, match="boom"):
        async with buffered_stream.checkpoint():
            assert await buffered_stream.receive_exactly(3) == b"abc"
            raise ValueError("boom")

    assert await buffered_stream.receive_exactly(6) == b"abcdef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_rollback_on_cancellation() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    with move_on_after(0.1):
        async with buffered_stream.checkpoint():
            assert await buffered_stream.receive_exactly(3) == b"abc"
            await sleep(10)

    assert await buffered_stream.receive_exactly(6) == b"abcdef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_nested_rollback() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    async with buffered_stream.checkpoint():
        assert await buffered_stream.receive_exactly(2) == b"ab"
        async with buffered_stream.checkpoint() as inner:
            assert await buffered_stream.receive_exactly(2) == b"cd"
            inner.commit()

        # The inner commit must not prevent the outer checkpoint from rolling back

    assert await buffered_stream.receive_exactly(6) == b"abcdef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_nested_commit() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    async with buffered_stream.checkpoint() as outer:
        assert await buffered_stream.receive_exactly(2) == b"ab"
        async with buffered_stream.checkpoint() as inner:
            assert await buffered_stream.receive_exactly(2) == b"cd"
            inner.commit()

        outer.commit()

    assert await buffered_stream.receive_exactly(2) == b"ef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_max_bytes_exceeded() -> None:
    buffered_stream = BufferedByteReceiveStream(
        FragmentedByteReceiveStream([b"ab", b"cd", b"ef"])
    )
    with pytest.raises(BufferError, match="retain more than the allowed 3 bytes"):
        async with buffered_stream.checkpoint(max_bytes=3):
            assert await buffered_stream.receive_exactly(2) == b"ab"
            await buffered_stream.receive_exactly(2)

    # The read position was rolled back, so no data was lost
    assert await buffered_stream.receive_exactly(6) == b"abcdef"


async def test_checkpoint_max_bytes_oversized_chunk() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abcdef")
    with pytest.raises(BufferError, match="retain more than the allowed 3 bytes"):
        async with buffered_stream.checkpoint(max_bytes=3):
            await buffered_stream.receive(2)

    # The oversized chunk from the object stream was not lost
    assert await buffered_stream.receive_exactly(6) == b"abcdef"

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_rollback_after_eof() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abc")
    await send_stream.aclose()
    with pytest.raises(IncompleteRead):
        async with buffered_stream.checkpoint():
            await buffered_stream.receive_exactly(4)

    # The EOF did not prevent rolling back and re-reading the original bytes
    assert await buffered_stream.receive_exactly(3) == b"abc"
    with pytest.raises(IncompleteRead):
        await buffered_stream.receive_exactly(1)

    receive_stream.close()


async def test_checkpoint_rollback_after_close() -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    await send_stream.send(b"abc")
    async with buffered_stream.checkpoint():
        assert await buffered_stream.receive_exactly(1) == b"a"
        await buffered_stream.aclose()

    # The rollback happened, but the stream can no longer be used
    with pytest.raises(ClosedResourceError):
        await buffered_stream.receive()


@pytest.mark.parametrize("max_bytes", [0, -1])
async def test_checkpoint_invalid_max_bytes(max_bytes: int) -> None:
    send_stream, receive_stream = create_memory_object_stream[bytes](1)
    buffered_stream = BufferedByteReceiveStream(receive_stream)
    with pytest.raises(ValueError, match="max_bytes must be a positive integer"):
        async with buffered_stream.checkpoint(max_bytes):
            pass  # pragma: no cover

    send_stream.close()
    receive_stream.close()


async def test_checkpoint_rollback_mixed_reads_fragmented() -> None:
    fragments = [
        b"GET",
        b" /ind",
        b"ex HT",
        b"TP/1.1",
        b"\r\nHost",
        b": exam",
        b"ple",
        b"\r\n",
    ]
    expected = b"".join(fragments)
    buffered_stream = BufferedByteReceiveStream(
        FragmentedByteReceiveStream(fragments.copy())
    )
    async with buffered_stream.checkpoint():
        assert await buffered_stream.receive_exactly(4) == b"GET "
        assert await buffered_stream.receive(4) == b"/ind"
        assert await buffered_stream.receive_until(b"\r\n", 100) == b"ex HTTP/1.1"
        assert await buffered_stream.receive_exactly(5) == b"Host:"

    # Nothing was committed, so the full byte sequence must be readable again
    assert await buffered_stream.receive_exactly(len(expected)) == expected


async def test_checkpoint_commit_mixed_reads_fragmented() -> None:
    fragments = [
        b"GET",
        b" /ind",
        b"ex HT",
        b"TP/1.1",
        b"\r\nHost",
        b": exam",
        b"ple",
        b"\r\n",
    ]
    buffered_stream = BufferedByteReceiveStream(
        FragmentedByteReceiveStream(fragments.copy())
    )
    async with buffered_stream.checkpoint() as checkpoint:
        assert await buffered_stream.receive_exactly(4) == b"GET "
        assert await buffered_stream.receive(4) == b"/ind"
        assert await buffered_stream.receive_until(b"\r\n", 100) == b"ex HTTP/1.1"
        assert await buffered_stream.receive_exactly(5) == b"Host:"
        checkpoint.commit()

    # The committed bytes are gone; reading continues where the checkpoint left off
    assert await buffered_stream.receive_until(b"\r\n", 100) == b" example"
    with pytest.raises(EndOfStream):
        await buffered_stream.receive()
