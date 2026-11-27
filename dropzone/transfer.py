"""File transfer over TCP.

Why TCP here: a file must arrive complete, in order and uncorrupted. TCP gives us
retransmission, ordering and flow control for free; with UDP we would have to
rebuild all of that ourselves.

Flow (sender -> receiver):
    OFFER {name,size,sha256}  ->
                              <-  ACCEPT {offset}  |  REJECT {reason}
    raw bytes [offset..size)  ->
                              <-  DONE {ok}
"""
import asyncio
import hashlib
import os
import time
import uuid
from pathlib import Path

from .protocol import (CHUNK_SIZE, ProtocolError, parse_accept, parse_done, parse_offer,
                       recv_msg, send_msg, sha256_file, unique_path)

READ_TIMEOUT = 30        # seconds a peer may stay silent mid-transfer
DECISION_TIMEOUT = 120   # seconds the human has to click Accept/Reject


def _noop(*_):
    pass


class Progress:
    """Emits throttled progress events (bytes done, MB/s) for the UI."""

    def __init__(self, emit, tid, direction, peer, name, size, done=0):
        self.emit, self.info = emit, {"id": tid, "dir": direction, "peer": peer, "name": name, "size": size}
        self.done, self.start_done = done, done
        self.t0 = self.last = time.monotonic()

    def add(self, n):
        self.done += n
        now = time.monotonic()
        if now - self.last >= 0.25 or self.done == self.info["size"]:
            self.last = now
            self.send("active")

    def send(self, status, error=None, saved=None):
        elapsed = max(time.monotonic() - self.t0, 1e-6)
        rate = (self.done - self.start_done) / elapsed / 1e6
        self.emit("progress", {**self.info, "done": self.done, "mbps": round(rate, 2),
                               "status": status, "error": error, "saved": saved})


class TransferServer:
    """Receives files. `on_offer(offer) -> awaitable[bool]` asks the human."""

    def __init__(self, download_dir, on_offer, max_size=2 * 1024**3, emit=None,
                 read_timeout=READ_TIMEOUT, decision_timeout=DECISION_TIMEOUT):
        self.download_dir = Path(download_dir).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.on_offer, self.max_size = on_offer, max_size
        self.emit = emit or _noop
        self.read_timeout, self.decision_timeout = read_timeout, decision_timeout
        self._active: set[Path] = set()   # .part files currently being written
        self.server = None

    async def start(self, host="0.0.0.0", port=50001):
        # listen() on a TCP socket; each incoming connect() gets its own _handle task.
        self.server = await asyncio.start_server(self._handle, host, port)
        return self.server.sockets[0].getsockname()[1]

    async def close(self, wait=True):
        """Stop listening. wait=False returns at once and lets running transfers finish."""
        if self.server:
            server, self.server = self.server, None
            server.close()
            if wait:
                await server.wait_closed()

    def _log(self, direction, peer, msg):
        self.emit("log", {"proto": "TCP", "dir": direction, "peer": peer, "msg": msg})

    async def _send(self, writer, peer, msg):
        self._log("out", peer, msg)
        await send_msg(writer, msg)

    async def _handle(self, reader, writer):
        ip, port = writer.get_extra_info("peername")[:2]
        peer = f"{ip}:{port}"
        self._log("in", peer, {"type": "(connected)"})
        try:
            await self._receive(reader, writer, peer)
        except (ProtocolError, OSError) as e:
            self._log("err", peer, {"error": str(e)})
        finally:
            writer.close()  # sends FIN
            try:
                await writer.wait_closed()
            except OSError:
                pass
            self._log("in", peer, {"type": "(closed)"})

    async def _receive(self, reader, writer, peer):
        msg = await recv_msg(reader, self.read_timeout)
        self._log("in", peer, msg)
        if msg["type"] != "OFFER":
            raise ProtocolError("expected OFFER")
        try:
            name, size, sha = parse_offer(msg, self.max_size)
        except ProtocolError as e:
            await self._send(writer, peer, {"type": "REJECT", "reason": str(e)})
            return

        # The partial file is keyed by content hash, so a retry of the *same*
        # file resumes, while a different file with the same name does not.
        part = self.download_dir / f"{name}.{sha[:12]}.part"
        if part in self._active:
            await self._send(writer, peer, {"type": "REJECT", "reason": "already receiving this file"})
            return
        self._active.add(part)
        try:
            offer = {"id": uuid.uuid4().hex, "peer": peer, "name": name, "size": size, "sha256": sha}
            # Ask the human. Nothing touches the disk until they say yes.
            try:
                accepted = await asyncio.wait_for(self.on_offer(offer), self.decision_timeout)
            except TimeoutError:
                accepted = False
            if not accepted:
                await self._send(writer, peer, {"type": "REJECT", "reason": "declined"})
                return
            await self._receive_body(reader, writer, peer, offer, part)
        finally:
            self._active.discard(part)

    async def _receive_body(self, reader, writer, peer, offer, part):
        name, size, sha = offer["name"], offer["size"], offer["sha256"]
        offset = part.stat().st_size if part.is_file() else 0
        if offset > size:
            part.unlink()
            offset = 0
        # Resume: hash the bytes we already have so the final check covers the whole file.
        hasher = await asyncio.to_thread(sha256_file, part) if offset else hashlib.sha256()
        await self._send(writer, peer, {"type": "ACCEPT", "offset": offset})

        prog = Progress(self.emit, offer["id"], "in", peer, name, size, offset)
        prog.send("active")
        try:
            with open(part, "ab") as f:
                remaining = size - offset
                while remaining:
                    # Timeout on every read so a stalled peer can't hang us forever.
                    try:
                        chunk = await asyncio.wait_for(
                            reader.read(min(CHUNK_SIZE, remaining)), self.read_timeout)
                    except TimeoutError:
                        raise ProtocolError("peer stalled") from None
                    if not chunk:  # EOF: sender closed early. Keep .part for resume.
                        raise ProtocolError(f"connection closed at {size - remaining}/{size} bytes")
                    f.write(chunk)
                    hasher.update(chunk)
                    remaining -= len(chunk)
                    prog.add(len(chunk))
        except (ProtocolError, OSError) as e:
            prog.send("failed", f"{e} (partial kept for resume)")
            raise

        self._log("in", peer, {"type": "(data)", "bytes": size - offset})
        if hasher.hexdigest() != sha:
            part.unlink()  # corrupt: throw it away
            prog.send("failed", "SHA-256 mismatch")
            await self._send(writer, peer, {"type": "DONE", "ok": False, "reason": "sha256 mismatch"})
            return
        final = unique_path(self.download_dir, name)  # never overwrite
        assert final.parent == self.download_dir       # never write outside the folder
        os.rename(part, final)                          # only after the hash checks out
        prog.send("done", saved=final.name)
        await self._send(writer, peer, {"type": "DONE", "ok": True, "saved_as": final.name})


async def send_file(host, port, path, name=None, emit=None, connect_timeout=10,
                    read_timeout=READ_TIMEOUT, tid=None) -> bool:
    """Offer `path` to host:port. Returns True if the receiver saved it intact."""
    emit = emit or _noop
    path = Path(path)
    name = name or path.name
    size = path.stat().st_size
    sha = (await asyncio.to_thread(sha256_file, path)).hexdigest()
    tid = tid or uuid.uuid4().hex

    peer = f"{host}:{port}"

    def log(direction, msg):
        emit("log", {"proto": "TCP", "dir": direction, "peer": peer, "msg": msg})

    prog = Progress(emit, tid, "out", peer, name, size)
    writer = None
    try:
        # open_connection() performs the TCP three-way handshake (SYN, SYN-ACK, ACK).
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), connect_timeout)
        offer = {"type": "OFFER", "name": name, "size": size, "sha256": sha}
        log("out", offer)
        await send_msg(writer, offer)
        prog.send("waiting")
        # Wait a little longer than the receiver's decision window.
        reply = await recv_msg(reader, DECISION_TIMEOUT + 10)
        log("in", reply)
        if reply["type"] == "REJECT":
            prog.send("rejected", str(reply.get("reason", ""))[:200])
            return False
        if reply["type"] != "ACCEPT":
            raise ProtocolError("expected ACCEPT or REJECT")
        offset = parse_accept(reply, size)
        if offset:
            log("in", {"type": "(resuming)", "offset": offset})
        prog.done = prog.start_done = offset

        with open(path, "rb") as f:
            f.seek(offset)
            remaining = size - offset
            while remaining:
                chunk = f.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    raise ProtocolError("file shrank while sending")
                writer.write(chunk)
                await asyncio.wait_for(writer.drain(), read_timeout)
                remaining -= len(chunk)
                prog.add(len(chunk))
        log("out", {"type": "(data)", "bytes": size - offset})

        done = await recv_msg(reader, read_timeout)
        log("in", done)
        ok = parse_done(done)
        prog.send("done" if ok else "failed", None if ok else str(done.get("reason", ""))[:200])
        return ok
    except (ProtocolError, OSError, TimeoutError) as e:
        prog.send("failed", str(e) or type(e).__name__)
        raise
    finally:
        if writer:
            writer.close()  # FIN
            try:
                await writer.wait_closed()
            except OSError:
                pass
