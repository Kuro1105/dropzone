"""Real TCP transfers over localhost (port 0 = let the OS pick a free port)."""
import asyncio
import hashlib
import os

import pytest
import pytest_asyncio

from dropzone.protocol import HEADER, ProtocolError, encode_frame, recv_msg
from dropzone.transfer import TransferServer, send_file

pytestmark = pytest.mark.asyncio


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def src(tmp_path):
    p = tmp_path / "src" / "data.bin"
    p.parent.mkdir()
    p.write_bytes(os.urandom(1_000_003))  # odd size: last chunk is partial
    return p


async def start_server(dl, accept=True, **kw):
    async def on_offer(offer):
        return accept
    server = TransferServer(dl, on_offer, **kw)
    port = await server.start("127.0.0.1", 0)
    return server, port


async def test_full_transfer_with_hash_check(tmp_path, src):
    dl = tmp_path / "dl"
    server, port = await start_server(dl)
    try:
        assert await send_file("127.0.0.1", port, src) is True
    finally:
        await server.close()
    assert sha(dl / "data.bin") == sha(src)
    assert not list(dl.glob("*.part"))


async def test_resume_from_partial_file(tmp_path, src):
    dl = tmp_path / "dl"
    dl.mkdir()
    digest = sha(src)
    part = dl / f"data.bin.{digest[:12]}.part"
    part.write_bytes(src.read_bytes()[:300_000])  # an earlier, interrupted transfer

    events = []
    server, port = await start_server(dl)
    try:
        assert await send_file("127.0.0.1", port, src, emit=lambda k, d: events.append((k, d)))
    finally:
        await server.close()
    accept = next(d["msg"] for k, d in events if k == "log" and d["msg"]["type"] == "ACCEPT")
    assert accept["offset"] == 300_000
    sent = next(d["msg"]["bytes"] for k, d in events if k == "log" and d["msg"]["type"] == "(data)")
    assert sent == src.stat().st_size - 300_000  # only the missing tail went over the wire
    assert sha(dl / "data.bin") == digest
    assert not part.exists()


async def test_reject_writes_nothing(tmp_path, src):
    dl = tmp_path / "dl"
    server, port = await start_server(dl, accept=False)
    try:
        assert await send_file("127.0.0.1", port, src) is False
    finally:
        await server.close()
    assert list(dl.iterdir()) == []


async def test_too_large_is_rejected(tmp_path, src):
    dl = tmp_path / "dl"
    server, port = await start_server(dl, max_size=1000)
    try:
        assert await send_file("127.0.0.1", port, src) is False
    finally:
        await server.close()
    assert list(dl.iterdir()) == []


async def test_path_traversal_name_stays_in_folder(tmp_path, src):
    dl = tmp_path / "dl"
    server, port = await start_server(dl)
    try:
        assert await send_file("127.0.0.1", port, src, name="../../escape.bin")
    finally:
        await server.close()
    assert (dl / "escape.bin").exists()
    assert not (tmp_path / "escape.bin").exists()


async def test_existing_file_not_overwritten(tmp_path, src):
    dl = tmp_path / "dl"
    dl.mkdir()
    (dl / "data.bin").write_text("keep me")
    server, port = await start_server(dl)
    try:
        assert await send_file("127.0.0.1", port, src)
    finally:
        await server.close()
    assert (dl / "data.bin").read_text() == "keep me"
    assert sha(dl / "data (1).bin") == sha(src)


async def test_corrupt_partial_fails_hash_and_is_deleted(tmp_path, src):
    dl = tmp_path / "dl"
    dl.mkdir()
    part = dl / f"data.bin.{sha(src)[:12]}.part"
    part.write_bytes(b"\x00" * 1000)  # wrong bytes
    server, port = await start_server(dl)
    try:
        assert await send_file("127.0.0.1", port, src) is False
    finally:
        await server.close()
    assert list(dl.iterdir()) == []


async def test_oversized_control_message_closes_connection(tmp_path):
    server, port = await start_server(tmp_path / "dl")
    try:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(HEADER.pack(10_000_000))  # claims a 10 MB control message
        await w.drain()
        assert await asyncio.wait_for(r.read(), 2) == b""  # server hung up, sent nothing
        w.close()
    finally:
        await server.close()


async def test_stalled_sender_times_out_and_keeps_part(tmp_path, src):
    dl = tmp_path / "dl"
    server, port = await start_server(dl, read_timeout=0.3)
    try:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(encode_frame({"type": "OFFER", "name": "data.bin", "size": 1000, "sha256": "a" * 64}))
        assert (await recv_msg(r, 2))["type"] == "ACCEPT"
        w.write(b"x" * 400)  # send some bytes, then go silent
        await w.drain()
        assert await asyncio.wait_for(r.read(), 2) == b""  # server gave up
        w.close()
    finally:
        await server.close()
    parts = list(dl.glob("*.part"))
    assert len(parts) == 1 and parts[0].stat().st_size == 400  # kept for resume


async def test_receiver_must_send_valid_accept(tmp_path, src):
    # A malicious receiver answers with a bogus offset: the sender must refuse.
    async def evil(reader, writer):
        await recv_msg(reader, 2)
        writer.write(encode_frame({"type": "ACCEPT", "offset": 10**12}))
        await writer.drain()
    srv = await asyncio.start_server(evil, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    try:
        with pytest.raises(ProtocolError, match="offset"):
            await send_file("127.0.0.1", port, src)
    finally:
        srv.close()


# ---------------------------------------------------------------- host on/off + preview paths

def free_port(kind):
    import socket
    with socket.socket(socket.AF_INET, kind) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest_asyncio.fixture
async def node(tmp_path):
    import shutil
    import socket
    from dropzone.main import Node, parse_args
    cfg = parse_args(["--name", "Host T", "--downloads", str(tmp_path / "dl"), "--broadcast", "127.0.0.1",
                      "--udp-port", str(free_port(socket.SOCK_DGRAM)),
                      "--tcp-port", str(free_port(socket.SOCK_STREAM))])
    n = Node(cfg)
    yield n
    await n.set_online(False)
    shutil.rmtree(n.outbox, ignore_errors=True)


async def test_host_switch_closes_and_reopens_tcp(node):
    port = node.cfg.tcp_port
    await node.set_online(True)
    _, w = await asyncio.open_connection("127.0.0.1", port)
    w.close()

    await node.set_online(False)
    with pytest.raises(OSError):  # nobody listening any more
        await asyncio.open_connection("127.0.0.1", port)
    assert any(e["data"]["msg"]["type"] == "BYE" for e in node.history)

    await node.set_online(True)  # same port works again
    _, w = await asyncio.open_connection("127.0.0.1", port)
    w.close()


async def test_switching_off_declines_waiting_offer(node, src):
    await node.set_online(True)
    sending = asyncio.create_task(send_file("127.0.0.1", node.cfg.tcp_port, src))
    while not node.pending:          # wait until the offer is on screen
        await asyncio.sleep(0.01)
    await node.set_online(False)
    assert await sending is False    # sender got REJECT
    assert list(node.server.download_dir.iterdir()) == []


async def test_received_path_only_serves_finished_files(node):
    dl = node.server.download_dir
    (dl / "photo.jpg").write_bytes(b"x")
    (dl / "big.bin.0123456789ab.part").write_bytes(b"x")
    (dl / "sub").mkdir()
    assert node.received_path("photo.jpg") == dl / "photo.jpg"
    assert node.received_path("../../photo.jpg") == dl / "photo.jpg"  # reduced to the bare name
    for bad in ["big.bin.0123456789ab.part", "sub", "missing.txt", "..", ""]:
        assert node.received_path(bad) is None
