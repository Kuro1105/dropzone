"""Wire format shared by discovery (UDP) and transfer (TCP), plus input validation.

Everything that arrives from the network passes through a parse_* function here
before the rest of the program touches it.
"""
import asyncio
import hashlib
import json
import os
import re
import struct
from pathlib import Path

VERSION = 1
CHUNK_SIZE = 64 * 1024          # file bytes are streamed in 64 KB chunks
MAX_CONTROL_SIZE = 64 * 1024    # largest JSON control message we accept
MAX_BEACON_SIZE = 1024          # a HELLO beacon is ~100 bytes; anything bigger is junk
HEADER = struct.Struct("!I")    # 4-byte unsigned length, "!" = network (big-endian) byte order
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ProtocolError(Exception):
    """The peer sent something we refuse to process."""


# ---------------------------------------------------------------- TCP framing
# TCP is a byte *stream*: one send() on one side may arrive as several recv()s on
# the other, or two messages may arrive glued together. So we add our own
# message boundaries: [4-byte length][JSON body].

def encode_frame(msg: dict) -> bytes:
    body = json.dumps(msg, separators=(",", ":")).encode()
    if len(body) > MAX_CONTROL_SIZE:
        raise ProtocolError("control message too large")
    return HEADER.pack(len(body)) + body


def decode_body(body: bytes) -> dict:
    try:
        msg = json.loads(body)  # json only: never pickle/eval on network data
    except (UnicodeDecodeError, ValueError):
        raise ProtocolError("invalid JSON") from None
    if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
        raise ProtocolError("message has no type")
    return msg


async def send_msg(writer: asyncio.StreamWriter, msg: dict) -> None:
    writer.write(encode_frame(msg))
    await writer.drain()  # back-pressure: wait while the kernel send buffer is full


async def recv_msg(reader: asyncio.StreamReader, timeout: float) -> dict:
    try:
        # readexactly() keeps reading until it has exactly N bytes, which is how
        # we reassemble a frame that TCP split across several segments.
        header = await asyncio.wait_for(reader.readexactly(HEADER.size), timeout)
        (length,) = HEADER.unpack(header)
        # Check the length *before* reading the body, so a peer can't make us
        # allocate gigabytes by announcing a huge frame.
        if length > MAX_CONTROL_SIZE:
            raise ProtocolError(f"control message too large ({length} bytes)")
        body = await asyncio.wait_for(reader.readexactly(length), timeout)
    except asyncio.IncompleteReadError:
        raise ProtocolError("connection closed mid-message") from None
    except TimeoutError:
        raise ProtocolError("timed out waiting for peer") from None
    return decode_body(body)


# ---------------------------------------------------------------- validation

def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)  # bool is a subclass of int


def _short_str(v, max_len: int) -> bool:
    return isinstance(v, str) and 0 < len(v) <= max_len and v.isprintable()


def parse_beacon(data: bytes) -> dict | None:
    """Return a clean HELLO/BYE dict, or None for anything malformed (silently ignored)."""
    if len(data) > MAX_BEACON_SIZE:
        return None
    try:
        msg = json.loads(data)
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(msg, dict) or not _short_str(msg.get("id"), 64):
        return None
    if msg.get("type") == "BYE":
        return {"type": "BYE", "id": msg["id"]}
    if (msg.get("type") == "HELLO" and msg.get("ver") == VERSION
            and _short_str(msg.get("name"), 64)
            and _is_int(msg.get("tcp_port")) and 1 <= msg["tcp_port"] <= 65535):
        return {"type": "HELLO", "id": msg["id"], "name": msg["name"], "tcp_port": msg["tcp_port"]}
    return None


def sanitize_filename(name) -> str:
    """Reduce a peer-supplied name to a bare filename, or raise ProtocolError."""
    if not isinstance(name, str) or len(name) > 1024:
        raise ProtocolError("bad filename")
    # Treat backslashes as separators too, so "..\\..\\x" from a Windows peer is caught.
    clean = Path(name.replace("\\", "/")).name.strip()
    if clean in ("", ".", "..") or not clean.isprintable() or len(clean.encode()) > 255:
        raise ProtocolError("bad filename")
    return clean


def parse_offer(msg: dict, max_size: int) -> tuple[str, int, str]:
    name = sanitize_filename(msg.get("name"))
    size, sha = msg.get("size"), msg.get("sha256")
    if not _is_int(size) or size < 0:
        raise ProtocolError("bad size")
    if size > max_size:
        raise ProtocolError(f"file too large (limit {max_size} bytes)")
    if not isinstance(sha, str) or not SHA256_RE.match(sha):
        raise ProtocolError("bad sha256")
    return name, size, sha


def parse_accept(msg: dict, size: int) -> int:
    offset = msg.get("offset")
    if not _is_int(offset) or not 0 <= offset <= size:
        raise ProtocolError("bad offset")
    return offset


def parse_done(msg: dict) -> bool:
    if msg.get("type") != "DONE" or not isinstance(msg.get("ok"), bool):
        raise ProtocolError("expected DONE")
    return msg["ok"]


# ---------------------------------------------------------------- files

def unique_path(folder: Path, name: str) -> Path:
    """folder/name, or folder/'name (1).ext' etc. so we never overwrite a file."""
    path = folder / name
    stem, suffix = path.stem, path.suffix
    i = 1
    while os.path.lexists(path):
        path = folder / f"{stem} ({i}){suffix}"
        i += 1
    return path


def sha256_file(path: Path, hasher=None, limit: int | None = None):
    """Hash a file in chunks (never loads it all). Returns the hasher object."""
    hasher = hasher or hashlib.sha256()
    remaining = limit
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE if remaining is None else min(CHUNK_SIZE, remaining)):
            hasher.update(chunk)
            if remaining is not None:
                remaining -= len(chunk)
    return hasher
