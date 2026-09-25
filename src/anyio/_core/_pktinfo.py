from __future__ import annotations

import socket
import struct
import sys
from collections.abc import Iterable
from ipaddress import IPv4Address, IPv6Address
from typing import Protocol

#: ``True`` if the platform supports packet info (IP_PKTINFO/IPV6_PKTINFO) ancillary
#: data on UDP sockets
PACKET_INFO_SUPPORTED: bool = (
    sys.platform == "linux"
    and hasattr(socket, "IP_PKTINFO")
    and hasattr(socket, "IPV6_RECVPKTINFO")
)

# struct in_pktinfo { unsigned int ipi_ifindex; struct in_addr ipi_spec_dst;
#                     struct in_addr ipi_addr; }
_IN_PKTINFO_FORMAT = "@I4s4s"

# struct in6_pktinfo { struct in6_addr ipi6_addr; unsigned int ipi6_ifindex; }
_IN6_PKTINFO_FORMAT = "@16sI"

_MAX_PKTINFO_SIZE = 20


class _SocketLike(Protocol):
    @property
    def family(self) -> socket.AddressFamily: ...

    def setsockopt(self, __level: int, __optname: int, __value: int) -> None: ...


def enable_packet_info(sock: _SocketLike) -> None:
    """
    Enable generation of packet info ancillary data for datagrams received on the
    socket.

    """
    if sock.family == socket.AF_INET:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_PKTINFO, 1)
    elif sock.family == socket.AF_INET6:
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_RECVPKTINFO, 1)
        # IPv4 packets received on a dual-stack socket come with IP_PKTINFO instead
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_PKTINFO, 1)


def ancillary_buffer_size() -> int:
    """Return a buffer size large enough to hold the packet info ancillary data."""
    return socket.CMSG_SPACE(_MAX_PKTINFO_SIZE)


def build_packet_info(
    family: socket.AddressFamily,
    local_address: str | None,
    interface_index: int | None,
) -> list[tuple[int, int, bytes]]:
    """
    Build ancillary data for setting the source address and/or outgoing interface of a
    datagram to be sent with :meth:`~socket.socket.sendmsg`.

    """
    if family == socket.AF_INET:
        packed_addr = (
            IPv4Address(local_address).packed if local_address else b"\x00" * 4
        )
        data = struct.pack(
            _IN_PKTINFO_FORMAT, interface_index or 0, packed_addr, b"\x00" * 4
        )
        return [(socket.IPPROTO_IP, socket.IP_PKTINFO, data)]
    elif family == socket.AF_INET6:
        packed_addr = (
            IPv6Address(local_address).packed if local_address else b"\x00" * 16
        )
        data = struct.pack(_IN6_PKTINFO_FORMAT, packed_addr, interface_index or 0)
        return [(socket.IPPROTO_IPV6, socket.IPV6_PKTINFO, data)]
    else:
        raise ValueError(f"unsupported address family: {family!r}")


def parse_packet_info(
    ancdata: Iterable[tuple[int, int, bytes]], flags: int
) -> tuple[str | None, int | None, bool]:
    """
    Parse ancillary data received via :meth:`~socket.socket.recvmsg`.

    :return: a tuple of (local destination address, interface index, truncated flag)

    """
    if flags & getattr(socket, "MSG_CTRUNC", 0):
        raise RuntimeError(
            "ancillary data was truncated; the packet metadata is incomplete"
        )

    local_address: str | None = None
    interface_index: int | None = None
    for level, ctype, data in ancdata:
        if (
            level == socket.IPPROTO_IP
            and ctype == socket.IP_PKTINFO
            and len(data) >= struct.calcsize(_IN_PKTINFO_FORMAT)
        ):
            interface_index, _spec_dst, addr = struct.unpack_from(
                _IN_PKTINFO_FORMAT, data
            )
            local_address = str(IPv4Address(addr))
        elif (
            level == socket.IPPROTO_IPV6
            and ctype == socket.IPV6_PKTINFO
            and len(data) >= struct.calcsize(_IN6_PKTINFO_FORMAT)
        ):
            addr, interface_index = struct.unpack_from(_IN6_PKTINFO_FORMAT, data)
            local_address = str(IPv6Address(addr))

    return local_address, interface_index, bool(flags & socket.MSG_TRUNC)
