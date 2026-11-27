import asyncio
import json

import pytest

from dropzone.protocol import (HEADER, MAX_CONTROL_SIZE, ProtocolError, encode_frame, parse_beacon,
                               parse_offer, recv_msg, sanitize_filename, unique_path)


def reader_with(*pieces):
    r = asyncio.StreamReader()
    for p in pieces:
        r.feed_data(p)
    r.feed_eof()
    return r


@pytest.mark.asyncio
async def test_framing_round_trip_even_when_split():
    msgs = [{"type": "OFFER", "name": "ünïcode.txt", "size": 5, "sha256": "ab" * 32},
            {"type": "ACCEPT", "offset": 0}]
    stream = b"".join(encode_frame(m) for m in msgs)
    # Feed it 3 bytes at a time, like TCP segments arriving in pieces.
    r = reader_with(*(stream[i:i + 3] for i in range(0, len(stream), 3)))
    assert await recv_msg(r, 1) == msgs[0]
    assert await recv_msg(r, 1) == msgs[1]


@pytest.mark.asyncio
async def test_oversized_message_rejected_before_reading_body():
    r = reader_with(HEADER.pack(MAX_CONTROL_SIZE + 1))  # header only, no body sent
    with pytest.raises(ProtocolError, match="too large"):
        await recv_msg(r, 1)
    with pytest.raises(ProtocolError):
        encode_frame({"type": "X", "pad": "a" * MAX_CONTROL_SIZE})


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [
    HEADER.pack(10) + b"short",           # connection closed mid-message
    HEADER.pack(3) + b"\xff\xfe\x00",     # not UTF-8 / JSON
    HEADER.pack(2) + b"[]",               # JSON but not an object
    HEADER.pack(2) + b"{}",               # no type
])
async def test_malformed_frames(stream):
    with pytest.raises(ProtocolError):
        await recv_msg(reader_with(stream), 1)


@pytest.mark.asyncio
async def test_recv_times_out():
    r = asyncio.StreamReader()  # never receives data
    with pytest.raises(ProtocolError, match="timed out"):
        await recv_msg(r, 0.1)


@pytest.mark.parametrize("name,expected", [
    ("report.pdf", "report.pdf"),
    ("../../etc/passwd", "passwd"),
    ("/abs/path/x.txt", "x.txt"),
    ("..\\..\\Windows\\win.ini", "win.ini"),
    ("  spaced.txt ", "spaced.txt"),
])
def test_sanitize_ok(name, expected):
    assert sanitize_filename(name) == expected


@pytest.mark.parametrize("name", ["", ".", "..", "../", "a/..", "x\x00y", "bad\nname", "a" * 300, 42, None])
def test_sanitize_rejects(name):
    with pytest.raises(ProtocolError):
        sanitize_filename(name)


def test_parse_offer_validates_fields():
    good = {"type": "OFFER", "name": "a.txt", "size": 10, "sha256": "0" * 64}
    assert parse_offer(good, 100) == ("a.txt", 10, "0" * 64)
    for bad in [{"size": -1}, {"size": True}, {"size": "10"}, {"size": 101},
                {"sha256": "XYZ"}, {"sha256": "0" * 63}, {"name": ".."}]:
        with pytest.raises(ProtocolError):
            parse_offer({**good, **bad}, 100)


def test_parse_beacon_ignores_junk():
    hello = {"type": "HELLO", "id": "abc", "name": "n", "tcp_port": 50001, "ver": 1}
    assert parse_beacon(json.dumps(hello).encode())["tcp_port"] == 50001
    assert parse_beacon(b'{"type":"BYE","id":"abc"}') == {"type": "BYE", "id": "abc"}
    for junk in [b"", b"\xff", b"[]", b"null", b"x" * 5000,
                 json.dumps({**hello, "tcp_port": 0}).encode(),
                 json.dumps({**hello, "tcp_port": True}).encode(),
                 json.dumps({**hello, "ver": 2}).encode(),
                 json.dumps({**hello, "id": ""}).encode()]:
        assert parse_beacon(junk) is None


def test_unique_path_never_overwrites(tmp_path):
    (tmp_path / "a.txt").write_text("1")
    (tmp_path / "a (1).txt").write_text("2")
    assert unique_path(tmp_path, "a.txt").name == "a (2).txt"
    assert unique_path(tmp_path, "b.txt").name == "b.txt"
